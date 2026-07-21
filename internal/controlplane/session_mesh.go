package controlplane

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/placement"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/session"
)

const (
	sessionDeltaPath    = "/internal/session-directory/delta"
	sessionSnapshotPath = "/internal/session-directory/snapshot"
	sessionProbePath    = "/internal/session-holder/probe"
	sessionDispatchPath = "/internal/session-holder/dispatch"
	sessionKeyLeasePath = "/internal/session-holder/key-lease"
	sessionCommandPath  = "/internal/session-holder/command"
	sessionRecoveryPath = "/internal/session-holder/recovery-command"
	maximumSessionRPC   = 1 << 20
	sessionSnapshotPage = 256
)

type SessionPeer struct {
	MemberID string
	Endpoint string
	Client   *http.Client
}

type sessionSnapshotResponse struct {
	Records    []session.DirectoryRecord `json:"records"`
	NextNodeID string                    `json:"next_node_id,omitempty"`
}

type SessionMesh struct {
	self      string
	directory *session.Directory
	peers     map[string]SessionPeer
	log       *slog.Logger

	holderMu sync.RWMutex
	holder   *session.Holder
	deltas   chan session.DirectoryDelta

	healthMu  sync.RWMutex
	healthSeq atomic.Uint64
	health    map[string]peerHealth
}

type peerHealth struct {
	attempt   uint64
	available bool
}

func NewSessionMesh(self string, directory *session.Directory, peers []SessionPeer, log *slog.Logger) (*SessionMesh, error) {
	if self == "" || directory == nil {
		return nil, errors.New("controlplane: Session mesh requires local member and Directory")
	}
	if log == nil {
		log = slog.Default()
	}
	byID := make(map[string]SessionPeer, len(peers))
	for _, peer := range peers {
		peer.Endpoint = strings.TrimRight(peer.Endpoint, "/")
		if peer.MemberID == "" || peer.MemberID == self || peer.Endpoint == "" || peer.Client == nil {
			return nil, errors.New("controlplane: Session mesh peer is incomplete or local")
		}
		if _, duplicate := byID[peer.MemberID]; duplicate {
			return nil, errors.New("controlplane: duplicate Session mesh peer")
		}
		byID[peer.MemberID] = peer
	}
	health := make(map[string]peerHealth, len(byID))
	for memberID := range byID {
		health[memberID] = peerHealth{available: true}
	}
	return &SessionMesh{
		self: self, directory: directory, peers: byID, log: log,
		deltas: make(chan session.DirectoryDelta, 4096), health: health,
	}, nil
}

// MemberAvailable is a fail-fast hint for assigning a new or reconnecting
// node-link session. It never moves a live session or changes consensus state.
func (m *SessionMesh) MemberAvailable(memberID string) bool {
	if memberID == m.self {
		return true
	}
	m.healthMu.RLock()
	health, found := m.health[memberID]
	m.healthMu.RUnlock()
	return found && health.available
}

func (m *SessionMesh) recordPeerHealth(memberID string, attempt uint64, available bool) {
	m.healthMu.Lock()
	current, found := m.health[memberID]
	if found && attempt >= current.attempt {
		m.health[memberID] = peerHealth{attempt: attempt, available: available}
	}
	m.healthMu.Unlock()
}

func (m *SessionMesh) SetHolder(holder *session.Holder) {
	m.holderMu.Lock()
	m.holder = holder
	m.holderMu.Unlock()
}

func (m *SessionMesh) localHolder() (*session.Holder, error) {
	m.holderMu.RLock()
	holder := m.holder
	m.holderMu.RUnlock()
	if holder == nil {
		return nil, errors.New("controlplane: local Session Holder is unavailable")
	}
	return holder, nil
}

func (m *SessionMesh) PublishSessionDelta(delta session.DirectoryDelta) {
	m.directory.Apply(delta)
	select {
	case m.deltas <- delta:
	default:
		// Periodic full anti-entropy repairs a dropped hint.
	}
}

func (m *SessionMesh) Run(ctx context.Context, antiEntropyInterval time.Duration) {
	if antiEntropyInterval <= 0 {
		antiEntropyInterval = 2 * time.Second
	}
	ticker := time.NewTicker(antiEntropyInterval)
	defer ticker.Stop()
	m.pullSnapshots(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case delta := <-m.deltas:
			m.broadcastDelta(ctx, delta)
		case <-ticker.C:
			m.pullSnapshots(ctx)
		}
	}
}

func (m *SessionMesh) Mount(mux *http.ServeMux) {
	mux.HandleFunc(sessionDeltaPath, m.serveDelta)
	mux.HandleFunc(sessionSnapshotPath, m.serveSnapshot)
	mux.HandleFunc(sessionProbePath, m.serveProbe)
	mux.HandleFunc(sessionDispatchPath, m.serveDispatch)
	mux.HandleFunc(sessionKeyLeasePath, m.serveKeyLease)
	mux.HandleFunc(sessionCommandPath, m.serveCommand)
	mux.HandleFunc(sessionRecoveryPath, m.serveRecoveryCommand)
}

func (m *SessionMesh) InstallKeyLeaseAt(
	ctx context.Context,
	holderID string,
	identity session.ServeIdentity,
	nodeID string,
	nodeEpoch uint64,
	dataEndpoint string,
	lease routesync.NodeKeyLeaseV1,
) (routesync.NodeKeyLeaseRefV1, bool, error) {
	if holderID == m.self {
		holder, err := m.localHolder()
		if err != nil {
			return routesync.NodeKeyLeaseRefV1{}, false, err
		}
		return holder.InstallKeyLease(ctx, identity, nodeID, nodeEpoch, dataEndpoint, lease)
	}
	peer, ok := m.peers[holderID]
	if !ok {
		return routesync.NodeKeyLeaseRefV1{}, false, session.ErrSessionUnavailable
	}
	input := keyLeaseRPCRequest{
		Identity: identity, NodeID: nodeID, NodeEpoch: nodeEpoch,
		DataEndpoint: dataEndpoint, Lease: lease,
	}
	var response keyLeaseRPCResponse
	err := postSessionJSON(ctx, peer, sessionKeyLeasePath, input, &response)
	if err != nil {
		return response.Ref, true, err
	}
	if response.Error != "" {
		return response.Ref, response.Sent, decodeSessionRPCError(response.Code, response.Error)
	}
	return response.Ref, response.Sent, nil
}

func (m *SessionMesh) InstallKeyLease(
	ctx context.Context,
	identity session.ServeIdentity,
	nodeID string,
	nodeEpoch uint64,
	dataEndpoint string,
	lease routesync.NodeKeyLeaseV1,
) (routesync.NodeKeyLeaseRefV1, bool, error) {
	entry, found := m.directory.Lookup(nodeID)
	if !found {
		return routesync.NodeKeyLeaseRefV1{}, false, session.ErrSessionUnavailable
	}
	if entry.NodeEpoch > nodeEpoch {
		return routesync.NodeKeyLeaseRefV1{}, false, session.ErrStaleSession
	}
	if entry.NodeEpoch < nodeEpoch {
		return routesync.NodeKeyLeaseRefV1{}, false, session.ErrSessionUnavailable
	}
	return m.InstallKeyLeaseAt(
		ctx, entry.HolderMemberID, identity, nodeID, nodeEpoch, dataEndpoint, lease,
	)
}

func (m *SessionMesh) ProbePlacementBatch(
	ctx context.Context,
	holderID string,
	identity session.ServeIdentity,
	requests []placement.PlacementProbeRequest,
) ([]placement.PlacementProbeResponse, error) {
	if holderID == m.self {
		holder, err := m.localHolder()
		if err != nil {
			return nil, err
		}
		calls := make([]session.ProbeCall, len(requests))
		for index := range requests {
			calls[index] = session.ProbeCall{ServeIdentity: identity, Request: requests[index]}
		}
		return holder.ProbeBatch(ctx, calls), nil
	}
	peer, ok := m.peers[holderID]
	if !ok {
		return nil, session.ErrSessionUnavailable
	}
	var response probeRPCResponse
	err := postSessionJSON(ctx, peer, sessionProbePath, probeRPCRequest{Identity: identity, Requests: requests}, &response)
	if err != nil {
		return nil, err
	}
	if response.Error != "" {
		return nil, decodeSessionRPCError(response.Code, response.Error)
	}
	return response.Responses, nil
}

func (m *SessionMesh) AdmitAndDispatchAt(
	ctx context.Context,
	holderID string,
	command session.DispatchCommand,
) (session.DispatchReply, error) {
	if holderID == m.self {
		holder, err := m.localHolder()
		if err != nil {
			return session.DispatchReply{}, err
		}
		return holder.AdmitAndDispatch(ctx, command)
	}
	peer, ok := m.peers[holderID]
	if !ok {
		return session.DispatchReply{}, session.ErrSessionUnavailable
	}
	var response dispatchRPCResponse
	err := postSessionJSON(ctx, peer, sessionDispatchPath, dispatchRPCRequest{Command: command}, &response)
	if err != nil {
		return session.DispatchReply{}, err
	}
	if response.Error != "" {
		return session.DispatchReply{}, decodeSessionRPCError(response.Code, response.Error)
	}
	return response.Reply, nil
}

func (m *SessionMesh) SendNodeCommandAt(
	ctx context.Context,
	holderID string,
	identity session.ServeIdentity,
	nodeID string,
	nodeEpoch uint64,
	dataEndpoint string,
	command *routesync.Command,
) (routesync.CmdAck, bool, error) {
	request := commandRPCRequest{
		Identity: identity, NodeID: nodeID, NodeEpoch: nodeEpoch,
		DataEndpoint: dataEndpoint, Command: command,
	}
	if holderID == m.self {
		holder, err := m.localHolder()
		if err != nil {
			return routesync.CmdAck{}, false, err
		}
		return holder.SendNodeCommand(ctx, identity, nodeID, nodeEpoch, dataEndpoint, command)
	}
	peer, ok := m.peers[holderID]
	if !ok {
		return routesync.CmdAck{}, false, session.ErrSessionUnavailable
	}
	var response commandRPCResponse
	err := postSessionJSON(ctx, peer, sessionCommandPath, request, &response)
	if err != nil {
		return routesync.CmdAck{}, true, err
	}
	if response.Error != "" {
		return response.Ack, response.Sent, decodeSessionRPCError(response.Code, response.Error)
	}
	return response.Ack, response.Sent, nil
}

func (m *SessionMesh) SendNodeCommand(
	ctx context.Context,
	identity session.ServeIdentity,
	nodeID string,
	nodeEpoch uint64,
	dataEndpoint string,
	command *routesync.Command,
) (routesync.CmdAck, bool, error) {
	entry, found := m.directory.Lookup(nodeID)
	if !found {
		return routesync.CmdAck{}, false, session.ErrSessionUnavailable
	}
	if entry.NodeEpoch > nodeEpoch {
		return routesync.CmdAck{}, false, session.ErrStaleSession
	}
	if entry.NodeEpoch < nodeEpoch {
		return routesync.CmdAck{}, false, session.ErrSessionUnavailable
	}
	return m.SendNodeCommandAt(ctx, entry.HolderMemberID, identity, nodeID, nodeEpoch, dataEndpoint, command)
}

func (m *SessionMesh) SendRecoveryCommandAt(
	ctx context.Context,
	holderID string,
	identity session.ServeIdentity,
	nodeID string,
	nodeEpoch uint64,
	dataEndpoint string,
	command *routesync.Command,
) (routesync.CmdAck, bool, error) {
	request := commandRPCRequest{
		Identity: identity, NodeID: nodeID, NodeEpoch: nodeEpoch,
		DataEndpoint: dataEndpoint, Command: command,
	}
	if holderID == m.self {
		holder, err := m.localHolder()
		if err != nil {
			return routesync.CmdAck{}, false, err
		}
		return holder.SendRecoveryCommand(ctx, identity, nodeID, nodeEpoch, dataEndpoint, command)
	}
	peer, ok := m.peers[holderID]
	if !ok {
		return routesync.CmdAck{}, false, session.ErrSessionUnavailable
	}
	var response commandRPCResponse
	err := postSessionJSON(ctx, peer, sessionRecoveryPath, request, &response)
	if err != nil {
		return routesync.CmdAck{}, true, err
	}
	if response.Error != "" {
		return response.Ack, response.Sent, decodeSessionRPCError(response.Code, response.Error)
	}
	return response.Ack, response.Sent, nil
}

func (m *SessionMesh) SendRecoveryCommand(
	ctx context.Context,
	identity session.ServeIdentity,
	nodeID string,
	nodeEpoch uint64,
	dataEndpoint string,
	command *routesync.Command,
) (routesync.CmdAck, bool, error) {
	entry, found := m.directory.Lookup(nodeID)
	if !found {
		return routesync.CmdAck{}, false, session.ErrSessionUnavailable
	}
	if entry.NodeEpoch > nodeEpoch {
		return routesync.CmdAck{}, false, session.ErrStaleSession
	}
	if entry.NodeEpoch < nodeEpoch {
		return routesync.CmdAck{}, false, session.ErrSessionUnavailable
	}
	return m.SendRecoveryCommandAt(ctx, entry.HolderMemberID, identity, nodeID, nodeEpoch, dataEndpoint, command)
}

func (m *SessionMesh) serveDelta(w http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var delta session.DirectoryDelta
	if err := decodeSessionJSON(request.Body, &delta); err != nil || !m.directory.Apply(delta) {
		if err != nil {
			http.Error(w, "invalid Session Directory delta", http.StatusBadRequest)
			return
		}
	}
	w.WriteHeader(http.StatusNoContent)
}

func (m *SessionMesh) serveSnapshot(w http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	records, next := m.directory.SnapshotPage(request.URL.Query().Get("after_node_id"), sessionSnapshotPage)
	response, err := boundedSessionSnapshotResponse(records, next)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeSessionJSON(w, http.StatusOK, response)
}

func (m *SessionMesh) serveProbe(w http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var input probeRPCRequest
	if err := decodeSessionJSON(request.Body, &input); err != nil || len(input.Requests) == 0 || len(input.Requests) > 2 {
		writeSessionJSON(w, http.StatusBadRequest, probeRPCResponse{Error: "invalid Probe request", Code: "INVALID"})
		return
	}
	holder, err := m.localHolder()
	if err != nil {
		writeSessionJSON(w, http.StatusServiceUnavailable, probeRPCResponse{Error: err.Error(), Code: sessionErrorCode(err)})
		return
	}
	calls := make([]session.ProbeCall, len(input.Requests))
	for index := range input.Requests {
		calls[index] = session.ProbeCall{ServeIdentity: input.Identity, Request: input.Requests[index]}
	}
	writeSessionJSON(w, http.StatusOK, probeRPCResponse{Responses: holder.ProbeBatch(request.Context(), calls)})
}

func (m *SessionMesh) serveDispatch(w http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var input dispatchRPCRequest
	if err := decodeSessionJSON(request.Body, &input); err != nil {
		writeSessionJSON(w, http.StatusBadRequest, dispatchRPCResponse{Error: "invalid dispatch request", Code: "INVALID"})
		return
	}
	holder, err := m.localHolder()
	if err == nil {
		var reply session.DispatchReply
		reply, err = holder.AdmitAndDispatch(request.Context(), input.Command)
		if err == nil {
			writeSessionJSON(w, http.StatusOK, dispatchRPCResponse{Reply: reply})
			return
		}
	}
	writeSessionJSON(w, http.StatusConflict, dispatchRPCResponse{Error: err.Error(), Code: sessionErrorCode(err)})
}

func (m *SessionMesh) serveKeyLease(w http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var input keyLeaseRPCRequest
	if err := decodeSessionJSON(request.Body, &input); err != nil {
		writeSessionJSON(w, http.StatusBadRequest, keyLeaseRPCResponse{Error: "invalid key lease request", Code: "INVALID"})
		return
	}
	holder, err := m.localHolder()
	if err != nil {
		writeSessionJSON(w, http.StatusServiceUnavailable, keyLeaseRPCResponse{Error: err.Error(), Code: sessionErrorCode(err)})
		return
	}
	ref, sent, err := holder.InstallKeyLease(
		request.Context(), input.Identity, input.NodeID, input.NodeEpoch, input.DataEndpoint, input.Lease,
	)
	response := keyLeaseRPCResponse{Ref: ref, Sent: sent}
	if err != nil {
		response.Error, response.Code = err.Error(), sessionErrorCode(err)
	}
	writeSessionJSON(w, http.StatusOK, response)
}

func (m *SessionMesh) serveCommand(w http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var input commandRPCRequest
	if err := decodeSessionJSON(request.Body, &input); err != nil || input.Command == nil {
		writeSessionJSON(w, http.StatusBadRequest, commandRPCResponse{Error: "invalid node command", Code: "INVALID"})
		return
	}
	holder, err := m.localHolder()
	if err != nil {
		writeSessionJSON(w, http.StatusServiceUnavailable, commandRPCResponse{Error: err.Error(), Code: sessionErrorCode(err)})
		return
	}
	ack, sent, err := holder.SendNodeCommand(
		request.Context(), input.Identity, input.NodeID, input.NodeEpoch, input.DataEndpoint, input.Command,
	)
	response := commandRPCResponse{Ack: ack, Sent: sent}
	if err != nil {
		response.Error, response.Code = err.Error(), sessionErrorCode(err)
	}
	writeSessionJSON(w, http.StatusOK, response)
}

func (m *SessionMesh) serveRecoveryCommand(w http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var input commandRPCRequest
	if err := decodeSessionJSON(request.Body, &input); err != nil || input.Command == nil {
		writeSessionJSON(w, http.StatusBadRequest, commandRPCResponse{Error: "invalid recovery command", Code: "INVALID"})
		return
	}
	holder, err := m.localHolder()
	if err != nil {
		writeSessionJSON(w, http.StatusServiceUnavailable, commandRPCResponse{Error: err.Error(), Code: sessionErrorCode(err)})
		return
	}
	ack, sent, err := holder.SendRecoveryCommand(
		request.Context(), input.Identity, input.NodeID, input.NodeEpoch, input.DataEndpoint, input.Command,
	)
	response := commandRPCResponse{Ack: ack, Sent: sent}
	if err != nil {
		response.Error, response.Code = err.Error(), sessionErrorCode(err)
	}
	writeSessionJSON(w, http.StatusOK, response)
}

func (m *SessionMesh) broadcastDelta(ctx context.Context, delta session.DirectoryDelta) {
	for _, peer := range m.peers {
		peer := peer
		go func() {
			timeout, cancel := context.WithTimeout(ctx, time.Second)
			defer cancel()
			if err := postSessionJSON(timeout, peer, sessionDeltaPath, delta, nil); err != nil && ctx.Err() == nil {
				m.log.Debug("Session Directory delta delivery failed", "member", peer.MemberID, "err", err)
			}
		}()
	}
}

func (m *SessionMesh) pullSnapshots(ctx context.Context) {
	for _, peer := range m.peers {
		peer := peer
		attempt := m.healthSeq.Add(1)
		go func() {
			afterNodeID := ""
			for {
				timeout, cancel := context.WithTimeout(ctx, time.Second)
				endpoint := peer.Endpoint + sessionSnapshotPath
				if afterNodeID != "" {
					endpoint += "?after_node_id=" + url.QueryEscape(afterNodeID)
				}
				request, err := http.NewRequestWithContext(timeout, http.MethodGet, endpoint, nil)
				if err != nil {
					cancel()
					m.recordPeerHealth(peer.MemberID, attempt, false)
					return
				}
				response, err := peer.Client.Do(request)
				if err != nil {
					cancel()
					m.recordPeerHealth(peer.MemberID, attempt, false)
					return
				}
				var page sessionSnapshotResponse
				if response.StatusCode == http.StatusOK {
					err = decodeSessionJSON(response.Body, &page)
				} else {
					err = fmt.Errorf("Session snapshot returned %s", response.Status)
				}
				response.Body.Close()
				cancel()
				if err != nil || !validSessionSnapshotPage(page, afterNodeID) {
					m.recordPeerHealth(peer.MemberID, attempt, false)
					return
				}
				m.directory.MergeFull(page.Records)
				if page.NextNodeID == "" {
					break
				}
				afterNodeID = page.NextNodeID
			}
			m.recordPeerHealth(peer.MemberID, attempt, true)
		}()
	}
}

func validSessionSnapshotPage(page sessionSnapshotResponse, afterNodeID string) bool {
	if len(page.Records) > sessionSnapshotPage || page.NextNodeID != "" && len(page.Records) == 0 {
		return false
	}
	previous := afterNodeID
	for _, record := range page.Records {
		if record.Entry.NodeID <= previous {
			return false
		}
		previous = record.Entry.NodeID
	}
	if page.NextNodeID != "" && (len(page.Records) == 0 || page.NextNodeID != previous) {
		return false
	}
	return true
}

func boundedSessionSnapshotResponse(
	records []session.DirectoryRecord,
	nextNodeID string,
) (sessionSnapshotResponse, error) {
	response := sessionSnapshotResponse{Records: records, NextNodeID: nextNodeID}
	fits := func(candidate sessionSnapshotResponse) (bool, error) {
		encoded, err := json.Marshal(candidate)
		return len(encoded) <= maximumSessionRPC, err
	}
	if ok, err := fits(response); err != nil || ok {
		return response, err
	}

	best := 0
	for low, high := 1, len(records); low <= high; {
		middle := low + (high-low)/2
		candidate := sessionSnapshotResponse{
			Records: records[:middle], NextNodeID: records[middle-1].Entry.NodeID,
		}
		ok, err := fits(candidate)
		if err != nil {
			return sessionSnapshotResponse{}, err
		}
		if ok {
			best = middle
			low = middle + 1
		} else {
			high = middle - 1
		}
	}
	if best == 0 {
		return sessionSnapshotResponse{}, errors.New("controlplane: one Session Directory record exceeds the RPC size limit")
	}
	return sessionSnapshotResponse{
		Records: records[:best], NextNodeID: records[best-1].Entry.NodeID,
	}, nil
}

type probeRPCRequest struct {
	Identity session.ServeIdentity             `json:"identity"`
	Requests []placement.PlacementProbeRequest `json:"requests"`
}

type probeRPCResponse struct {
	Responses []placement.PlacementProbeResponse `json:"responses,omitempty"`
	Code      string                             `json:"code,omitempty"`
	Error     string                             `json:"error,omitempty"`
}

type dispatchRPCRequest struct {
	Command session.DispatchCommand `json:"command"`
}

type dispatchRPCResponse struct {
	Reply session.DispatchReply `json:"reply"`
	Code  string                `json:"code,omitempty"`
	Error string                `json:"error,omitempty"`
}

type keyLeaseRPCRequest struct {
	Identity     session.ServeIdentity    `json:"identity"`
	NodeID       string                   `json:"node_id"`
	NodeEpoch    uint64                   `json:"node_epoch"`
	DataEndpoint string                   `json:"data_endpoint"`
	Lease        routesync.NodeKeyLeaseV1 `json:"lease"`
}

type keyLeaseRPCResponse struct {
	Ref   routesync.NodeKeyLeaseRefV1 `json:"ref"`
	Sent  bool                        `json:"sent"`
	Code  string                      `json:"code,omitempty"`
	Error string                      `json:"error,omitempty"`
}

type commandRPCRequest struct {
	Identity     session.ServeIdentity `json:"identity"`
	NodeID       string                `json:"node_id"`
	NodeEpoch    uint64                `json:"node_epoch"`
	DataEndpoint string                `json:"data_endpoint"`
	Command      *routesync.Command    `json:"command"`
}

type commandRPCResponse struct {
	Ack   routesync.CmdAck `json:"ack"`
	Sent  bool             `json:"sent"`
	Code  string           `json:"code,omitempty"`
	Error string           `json:"error,omitempty"`
}

func postSessionJSON(ctx context.Context, peer SessionPeer, path string, input, output any) error {
	raw, err := json.Marshal(input)
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, peer.Endpoint+path, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := peer.Client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("controlplane: Session RPC returned %s", response.Status)
	}
	if output == nil || response.StatusCode == http.StatusNoContent {
		return nil
	}
	return decodeSessionJSON(response.Body, output)
}

func decodeSessionJSON(reader io.Reader, target any) error {
	decoder := json.NewDecoder(io.LimitReader(reader, maximumSessionRPC+1))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("controlplane: Session RPC has trailing JSON")
	}
	return nil
}

func writeSessionJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func sessionErrorCode(err error) string {
	switch {
	case errors.Is(err, session.ErrStaleSession):
		return "STALE_SESSION"
	case errors.Is(err, session.ErrSessionUnavailable):
		return "SESSION_UNAVAILABLE"
	case errors.Is(err, session.ErrPermitUnavailable):
		return "PERMIT_UNAVAILABLE"
	default:
		return "FAILED"
	}
}

func decodeSessionRPCError(code, message string) error {
	switch code {
	case "STALE_SESSION":
		return fmt.Errorf("%w: %s", session.ErrStaleSession, message)
	case "SESSION_UNAVAILABLE":
		return fmt.Errorf("%w: %s", session.ErrSessionUnavailable, message)
	case "PERMIT_UNAVAILABLE":
		return fmt.Errorf("%w: %s", session.ErrPermitUnavailable, message)
	default:
		return errors.New(message)
	}
}
