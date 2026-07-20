package controlplane

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/kuasar-sandbox/orchestrator/internal/raftstore"
)

const (
	recoveryApplyPath = "/internal/recovery/data/apply"
	recoveryReadPath  = "/internal/recovery/data/read"
)

// RecoveryMesh routes dedicated recovery operations only to the exact three
// replicas frozen in the signed registryLayout. It is Registry-to-Registry mTLS and
// remains separate from the normal Router Route/Build API.
type RecoveryMesh struct {
	self           string
	store          *RaftStore
	registryLayout raftstore.RegistryLayout
	peers          map[string]SessionPeer
}

func NewRecoveryMesh(self string, store *RaftStore, peers []SessionPeer) (*RecoveryMesh, error) {
	if self == "" || store == nil {
		return nil, errors.New("controlplane: recovery mesh requires local member and consensus store")
	}
	registryLayout, _ := store.RegistryLayoutSnapshot()
	byID := make(map[string]SessionPeer, len(peers))
	for _, peer := range peers {
		peer.Endpoint = strings.TrimRight(peer.Endpoint, "/")
		member, found := recoveryRegistryLayoutMember(registryLayout, peer.MemberID)
		if !found || peer.MemberID == self || peer.Endpoint != member.InternalEndpoint || peer.Client == nil {
			return nil, errors.New("controlplane: recovery peer is not an exact authenticated registryLayout member")
		}
		if _, duplicate := byID[peer.MemberID]; duplicate {
			return nil, errors.New("controlplane: duplicate recovery peer")
		}
		byID[peer.MemberID] = peer
	}
	for _, member := range registryLayout.Members {
		if member.MemberID == self {
			continue
		}
		if _, found := byID[member.MemberID]; !found {
			return nil, errors.New("controlplane: recovery mesh requires every registryLayout peer")
		}
	}
	return &RecoveryMesh{self: self, store: store, registryLayout: registryLayout, peers: byID}, nil
}

func (m *RecoveryMesh) Mount(mux *http.ServeMux) {
	mux.HandleFunc("POST "+recoveryApplyPath, m.serveApply)
	mux.HandleFunc("POST "+recoveryReadPath, m.serveRead)
}

func (m *RecoveryMesh) Apply(ctx context.Context, command raftstore.DataCommand) (raftstore.DataApplyResult, error) {
	if command.Identity.ShardID >= uint32(len(m.registryLayout.DataShards)) {
		return raftstore.DataApplyResult{}, errors.New("controlplane: recovery command targets an unknown shard")
	}
	var joined error
	for _, replica := range m.registryLayout.DataShards[command.Identity.ShardID].Replicas {
		if replica.MemberID == m.self {
			result, err := m.store.ApplyRecoveryData(ctx, command)
			if err == nil {
				return result, nil
			}
			joined = errors.Join(joined, err)
			continue
		}
		peer, found := m.peers[replica.MemberID]
		if !found {
			joined = errors.Join(joined, fmt.Errorf("controlplane: recovery replica %s has no client", replica.MemberID))
			continue
		}
		var response recoveryApplyResponse
		if err := postSessionJSON(ctx, peer, recoveryApplyPath, recoveryApplyRequest{Command: command}, &response); err != nil {
			joined = errors.Join(joined, err)
			continue
		}
		if response.Error != "" {
			joined = errors.Join(joined, errors.New(response.Error))
			continue
		}
		return response.Result, nil
	}
	return raftstore.DataApplyResult{}, errors.Join(errors.New("controlplane: recovery data shard is unavailable"), joined)
}

func (m *RecoveryMesh) Read(ctx context.Context, query raftstore.RecoveryLookup) (raftstore.RecoveryLookupResult, error) {
	if query.Identity.ShardID >= uint32(len(m.registryLayout.DataShards)) {
		return raftstore.RecoveryLookupResult{}, errors.New("controlplane: recovery query targets an unknown shard")
	}
	var joined error
	for _, replica := range m.registryLayout.DataShards[query.Identity.ShardID].Replicas {
		if replica.MemberID == m.self {
			result, err := m.store.ReadRecoveryData(ctx, query)
			if err == nil {
				return result, nil
			}
			joined = errors.Join(joined, err)
			continue
		}
		peer, found := m.peers[replica.MemberID]
		if !found {
			joined = errors.Join(joined, fmt.Errorf("controlplane: recovery replica %s has no client", replica.MemberID))
			continue
		}
		var response recoveryReadResponse
		if err := postSessionJSON(ctx, peer, recoveryReadPath, recoveryReadRequest{Query: query}, &response); err != nil {
			joined = errors.Join(joined, err)
			continue
		}
		if response.Error != "" {
			joined = errors.Join(joined, errors.New(response.Error))
			continue
		}
		return response.Result, nil
	}
	return raftstore.RecoveryLookupResult{}, errors.Join(errors.New("controlplane: recovery data shard is unavailable"), joined)
}

func (m *RecoveryMesh) serveApply(w http.ResponseWriter, request *http.Request) {
	var input recoveryApplyRequest
	if err := decodeSessionJSON(request.Body, &input); err != nil {
		writeSessionJSON(w, http.StatusBadRequest, recoveryApplyResponse{Error: "invalid recovery mutation"})
		return
	}
	result, err := m.store.ApplyRecoveryData(request.Context(), input.Command)
	response := recoveryApplyResponse{Result: result}
	if err != nil {
		response.Error = err.Error()
	}
	writeSessionJSON(w, http.StatusOK, response)
}

func (m *RecoveryMesh) serveRead(w http.ResponseWriter, request *http.Request) {
	var input recoveryReadRequest
	if err := decodeSessionJSON(request.Body, &input); err != nil {
		writeSessionJSON(w, http.StatusBadRequest, recoveryReadResponse{Error: "invalid recovery query"})
		return
	}
	result, err := m.store.ReadRecoveryData(request.Context(), input.Query)
	response := recoveryReadResponse{Result: result}
	if err != nil {
		response.Error = err.Error()
	}
	writeSessionJSON(w, http.StatusOK, response)
}

type recoveryApplyRequest struct {
	Command raftstore.DataCommand `json:"command"`
}

type recoveryApplyResponse struct {
	Result raftstore.DataApplyResult `json:"result"`
	Error  string                    `json:"error,omitempty"`
}

type recoveryReadRequest struct {
	Query raftstore.RecoveryLookup `json:"query"`
}

type recoveryReadResponse struct {
	Result raftstore.RecoveryLookupResult `json:"result"`
	Error  string                         `json:"error,omitempty"`
}

func recoveryRegistryLayoutMember(registryLayout raftstore.RegistryLayout, memberID string) (raftstore.RegistryMember, bool) {
	for _, member := range registryLayout.Members {
		if member.MemberID == memberID {
			return member, true
		}
	}
	return raftstore.RegistryMember{}, false
}
