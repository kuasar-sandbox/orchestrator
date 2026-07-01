package registry

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/apikey"
	clusterstate "github.com/kuasar-sandbox/sandbox-orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/cluster/shardkv"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/routesync"
)

// ErrNoNode is returned when no eligible node can host a sandbox.
var ErrNoNode = errors.New("registry: no eligible node")

// ErrNodeGone is returned when the node owner cannot reach the target node.
var ErrNodeGone = errors.New("registry: node owner cannot reach node")

var (
	errMissingImportSourceLease = errors.New("registry: import source lease fields are required")
	errStaleImportSourceLease   = errors.New("registry: stale import source lease")
)

const lifecycleAckTimeout = 5 * time.Second

// nodeConn is the registry's handle to one connected node's channel — it sends
// commands toward the node. channel.go implements it over the wire; tests fake it.
type nodeConn interface {
	id() string
	send(*routesync.Command) error
}

// Placer suggests a node for a new sandbox: the scaler suggests, the registry
// commits by CAS.
// PlaceRequest is a placement ask. Build marks a build placement (resource-aware,
// cluster-scaler.md §4.5); TargetRuntimeDigest lets the scaler prefer compatible
// node runtimes when the caller knows the required runtime identity.
type PlaceRequest struct {
	Group               string
	RouteKey            string
	SandboxID           string
	Config              map[string]string
	Build               bool
	TargetRuntimeDigest string
}

// Placement is a scaler answer plus the group-derived material the registry must
// place on node commands. Registry route owners persist AccessToken in route_link
// and never call a sandbox-group provider on the hot/read path.
type Placement struct {
	NodeID         string
	TemplateRef    string
	Config         map[string]string
	KeyFingerprint string
	AccessToken    string
	ImageRepo      string
	RegistryAuth   string
}

// Placer suggests a node and group-derived create/build material. Production
// placement calls scaler over scale_link; the builtin placer is the size-1/test
// fallback and only fills NodeID.
type Placer interface {
	Place(ctx context.Context, req PlaceRequest) (*Placement, error)
}

// Registry is the cluster control plane's state authority + node_link hub.
type Registry struct {
	stores      *Stores
	placer      Placer
	parkTimeout time.Duration
	log         *slog.Logger

	mu       sync.Mutex
	nodes    map[string]nodeConn               // node_id -> channel
	inflight map[string]*reserveCall           // single-flight ReserveSandbox per (group,route_key)
	sidKeys  map[string][2]string              // sid -> {group, route_key} (delete-by-sid from the route stream)
	acks     map[string]chan *routesync.CmdAck // cmd_id -> ack waiter (synchronous key commands)

	localNodeOwner     NodeOwner
	nodeOwner          NodeOwner
	deadAfter          time.Duration
	reaperCtx          context.Context
	nodeLinkRelayMu    sync.RWMutex
	nodeLinkRelayPeers map[string]NodeLinkRelayPeer

	scalerMu         sync.Mutex
	scaleReadyLabel  string
	scalerLabel      string
	scalePeerSource  ScalerPeerSource
	scalerSeedJoiner ScalerSeedJoiner
	scaleReplicas    int
	minReadyScalers  int
	scaleTimeout     time.Duration
}

type ScalerPeer struct {
	ID         string
	Advertise  string
	ReadyLabel string
}

type ScalerPeerSource func(readyLabel string) []ScalerPeer

type ScalerSeedJoiner func(ctx context.Context, id, label, advertise string) error

type ImportSourceLease struct {
	SourceID      string `json:"source_id"`
	OwnerID       string `json:"owner_id"`
	RunID         string `json:"run_id"`
	Term          uint64 `json:"term"`
	Cursor        string `json:"cursor,omitempty"`
	Round         uint64 `json:"round,omitempty"`
	NextRunUnixMs int64  `json:"next_run_unix_ms,omitempty"`
	LastError     string `json:"last_error,omitempty"`
	ExpiresUnixMs int64  `json:"expires_unix_ms"`
}

// reserveCall is one in-flight ReserveSandbox; joiners wait on done, the channel
// reader fills result + closes done when the sandbox goes running.
type reserveCall struct {
	done   chan struct{}
	result *ReserveResult
	err    error
	// re-Place context (§7.4): a rejected create re-places once on another node
	// before failing the Reserve. orig is the pre-reserve record to restore on
	// timeout/reject while the row is still RESERVED.
	group, routeKey string
	orig            *SandboxRecord
	found           bool
	createConfig    map[string]string
}

// ReserveResult is what a satisfied ReserveSandbox returns (cluster.md §7.2). The
// router injects AccessToken and forwards to the node's DataEndpoint.
type ReserveResult struct {
	NodeID       string `json:"node_id"`
	SID          string `json:"sid"`
	AccessToken  string `json:"access_token"`
	DataEndpoint string `json:"data_endpoint"`
}

// New builds a Registry. Production callers set a scale_link placer explicitly.
func New(stores *Stores, placer Placer, parkTimeout time.Duration, log *slog.Logger) *Registry {
	if placer == nil {
		placer = noPlacer{}
	}
	if parkTimeout <= 0 {
		parkTimeout = 30 * time.Second
	}
	if log == nil {
		log = slog.Default()
	}
	r := &Registry{
		stores:          stores,
		placer:          placer,
		parkTimeout:     parkTimeout,
		log:             log,
		nodes:           make(map[string]nodeConn),
		inflight:        make(map[string]*reserveCall),
		sidKeys:         make(map[string][2]string),
		acks:            make(map[string]chan *routesync.CmdAck),
		scaleReplicas:   1,
		minReadyScalers: 1,
		scaleTimeout:    2 * time.Second,
	}
	localOwner := newLocalNodeOwner(r)
	r.localNodeOwner = localOwner
	r.nodeOwner = localOwner
	return r
}

// applySelectorPatch renews the scaler-selected node manifest-key cache. The
// scaler owns selector/shuffle decisions; registry only writes the current key
// lease into node_link for the explicit nodes in the patch. Old keys are not
// actively deleted: node_link and node side TTLs expire entries that stop being
// renewed.
func (r *Registry) applySelectorPatch(ctx context.Context, p *routesync.SelectorPatch) error {
	if err := r.checkSelectorPatchLease(ctx, p); err != nil {
		return err
	}
	if p.KeyFingerprint == "" || len(p.NodeIDs) == 0 {
		return nil
	}
	keyType, keyValue := p.ManifestKeyType, p.ManifestKey
	if keyType == clusterstate.SecretRef {
		keyValue = p.ManifestKeyRef
	}
	if keyType == "" && keyValue != "" {
		keyType = clusterstate.SecretInline
	}
	if keyValue == "" {
		return nil
	}
	expiresUnix := time.Now().Add(keyLeaseTTL).Unix()
	for _, nodeID := range p.NodeIDs {
		if nodeID == "" || r.nodeOwner == nil {
			continue
		}
		if err := r.nodeOwner.PutManifestKey(ctx, nodeID, p.KeyFingerprint, keyType, keyValue, expiresUnix); err != nil && !errors.Is(err, ErrNodeGone) {
			return err
		}
	}
	return nil
}

func (r *Registry) checkSelectorPatchLease(ctx context.Context, p *routesync.SelectorPatch) error {
	if p == nil || p.ImportSourceID == "" || p.ImportOwnerID == "" || p.ImportRunID == "" || p.ImportTerm == 0 {
		return errMissingImportSourceLease
	}
	if !r.stores.CheckScaleLinkSourceLease(ctx, p.ImportSourceID, p.ImportOwnerID, p.ImportRunID, p.ImportTerm) {
		return errStaleImportSourceLease
	}
	return nil
}

// SetScaleReadyLabel sets the registry membership label a scaler must advertise
// before it is used for Place requests.
func (r *Registry) SetScaleReadyLabel(label string) {
	r.scalerMu.Lock()
	r.scaleReadyLabel = label
	r.scalerMu.Unlock()
}

func (r *Registry) SetScalerMemberlistLabel(label string) {
	r.scalerMu.Lock()
	r.scalerLabel = label
	r.scalerMu.Unlock()
}

func (r *Registry) scalerMemberlistLabel() string {
	r.scalerMu.Lock()
	defer r.scalerMu.Unlock()
	return r.scalerLabel
}

func (r *Registry) SetScalerPeerSource(source ScalerPeerSource) {
	r.scalerMu.Lock()
	r.scalePeerSource = source
	r.scalerMu.Unlock()
}

func (r *Registry) SetScalerSeedJoiner(joiner ScalerSeedJoiner) {
	r.scalerMu.Lock()
	r.scalerSeedJoiner = joiner
	r.scalerMu.Unlock()
}

func (r *Registry) joinScalerSeed(ctx context.Context, id, label, advertise string) error {
	r.scalerMu.Lock()
	joiner := r.scalerSeedJoiner
	r.scalerMu.Unlock()
	if joiner == nil {
		return nil
	}
	return joiner(ctx, id, label, advertise)
}

func (r *Registry) SetScalePolicy(replicaCount, minReady int, timeout time.Duration) {
	if replicaCount <= 0 {
		replicaCount = 1
	}
	if minReady <= 0 {
		minReady = 1
	}
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	r.scalerMu.Lock()
	r.scaleReplicas = replicaCount
	r.minReadyScalers = minReady
	r.scaleTimeout = timeout
	r.scalerMu.Unlock()
}

func (r *Registry) scalePolicy() (replicaCount, minReady int, timeout time.Duration) {
	r.scalerMu.Lock()
	defer r.scalerMu.Unlock()
	return r.scaleReplicas, r.minReadyScalers, r.scaleTimeout
}

func (r *Registry) readyScalerPeers(time.Duration) []ScalerPeer {
	r.scalerMu.Lock()
	source := r.scalePeerSource
	readyLabel := r.scaleReadyLabel
	r.scalerMu.Unlock()
	if source == nil {
		return nil
	}
	src := source(readyLabel)
	out := make([]ScalerPeer, 0, len(src))
	for _, peer := range src {
		if peer.ID == "" || peer.Advertise == "" {
			continue
		}
		if readyLabel != "" && peer.ReadyLabel != "" && peer.ReadyLabel != readyLabel {
			continue
		}
		out = append(out, peer)
	}
	return out
}

// SetPlacer wires the placer implementation.
func (r *Registry) SetPlacer(p Placer) { r.placer = p }

// SetNodeOwner replaces the local node-owner adapter. Reserve/build/key/orphan
// flows stay the same; execution-state authority moves behind this interface.
func (r *Registry) SetNodeOwner(owner NodeOwner) {
	r.nodeOwner = owner
}

func (r *Registry) SetRemoteNodeOwners(remotes map[string]NodeOwner) {
	r.nodeOwner = newRoutingNodeOwner(r, r.localNodeOwner, remotes)
}

func (r *Registry) SetNodeLinkRelayPeers(peers map[string]NodeLinkRelayPeer) {
	cp := make(map[string]NodeLinkRelayPeer, len(peers))
	for id, peer := range peers {
		if id == "" || peer.Endpoint == "" {
			continue
		}
		if peer.Client == nil {
			peer.Client = http.DefaultClient
		}
		peer.Endpoint = strings.TrimRight(peer.Endpoint, "/")
		peer.RedirectEndpoint = strings.TrimRight(peer.RedirectEndpoint, "/")
		cp[id] = peer
	}
	r.nodeLinkRelayMu.Lock()
	r.nodeLinkRelayPeers = cp
	r.nodeLinkRelayMu.Unlock()
}

func (r *Registry) LocalNodeOwner() NodeOwner { return r.localNodeOwner }

// Stores exposes the typed store layer used by registry route/node/build state.
func (r *Registry) Stores() *Stores { return r.stores }

func flightKey(group, routeKey string) string { return group + "\x00" + routeKey }

// ReserveSandbox resolves (group, route_key) to a running sandbox, placing +
// creating (or resuming a PAUSED sandbox) on a node and waiting for the node to
// report it running, single-flight per key (cluster.md §7.2).
func (r *Registry) ReserveSandbox(ctx context.Context, group, routeKey string, createConfig map[string]string) (*ReserveResult, error) {
	if group == "" || routeKey == "" {
		return nil, fmt.Errorf("registry: group and route_key are required")
	}
	rec, rev, found, err := r.getSandboxForReserve(ctx, group, routeKey)
	if err != nil {
		return nil, err
	}
	if found && rec.State == StateReady {
		if res, live := r.readyResultFromRecord(ctx, rec); live {
			return res, nil
		}
		// Missing node runtime: fall through to re-place (dead-node sweep also resets it).
	}

	key := flightKey(group, routeKey)
	r.mu.Lock()
	if call, ok := r.inflight[key]; ok {
		r.mu.Unlock()
		return waitCall(ctx, call)
	}
	call := &reserveCall{done: make(chan struct{}), group: group, routeKey: routeKey, orig: rec, found: found, createConfig: createConfig}
	r.inflight[key] = call
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		delete(r.inflight, key)
		r.mu.Unlock()
	}()

	if err := r.startReserve(ctx, group, routeKey, rec, rev, found, createConfig); err != nil {
		r.finish(key, nil, err)
		r.rollbackReserve(group, routeKey, rec, found)
		return waitCall(ctx, call)
	}
	wctx, cancel := context.WithTimeout(ctx, r.parkTimeout)
	defer cancel()
	res, rerr := r.waitReserveCall(wctx, call)
	if rerr != nil {
		// Park timeout / caller cancel: undo a RESERVED that never reached READY, so
		// it doesn't strand on a live node (the sweep only clears dead-node rows).
		r.rollbackReserve(group, routeKey, rec, found)
		r.finish(key, nil, rerr)
		return nil, rerr
	}
	r.finish(key, res, nil)
	return res, nil
}

func (r *Registry) getSandboxForReserve(ctx context.Context, group, routeKey string) (*SandboxRecord, int64, bool, error) {
	for {
		rec, rev, found, err := r.stores.GetSandbox(ctx, group, routeKey)
		if !transientRouteRead(err) {
			return rec, rev, found, err
		}
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, 0, false, ctx.Err()
		case <-timer.C:
		}
	}
}

// rollbackReserve restores a (group, route_key) to its pre-reserve state when a
// Reserve fails without reaching READY (cluster.md §7.4): a pre-existing PAUSED
// row is put back and a fresh one is deleted, but only while the row is still
// RESERVED (a late running route may have won).
func (r *Registry) rollbackReserve(group, routeKey string, orig *SandboxRecord, found bool) {
	ctx := context.Background() // must complete even if the caller's ctx is done
	cur, _, curFound, err := r.stores.GetSandbox(ctx, group, routeKey)
	if err != nil || !curFound || cur.State != StateReserved {
		return // already resolved (READY) / gone — nothing to roll back
	}
	if found {
		_, _ = r.stores.PutSandbox(ctx, orig)
	} else {
		_ = r.stores.DeleteSandbox(ctx, group, routeKey)
		if cur.NodeID != "" {
			_ = r.stores.RemoveNodeSandboxRef(ctx, cur.NodeID, group, routeKey, cur.SID)
		}
		r.dropSID(cur.SID)
	}
}

// startReserve drives the placement + command for the leader of a single-flight.
// It does not wait — the channel reader signals completion when the node reports
// the sandbox running (finish, via applyRoute).
func (r *Registry) startReserve(ctx context.Context, group, routeKey string, rec *SandboxRecord, rev int64, found bool, createConfig map[string]string) error {
	// PAUSED: resume on the same node (no placement).
	if found && rec.State == StatePaused && rec.NodeID != "" {
		if r.nodeOwner == nil {
			return ErrNodeGone
		}
		reserved := *rec
		reserved.State = StateReserved
		if _, ok, err := r.stores.CASSandbox(ctx, &reserved, rev); err != nil || !ok {
			return cas(err, ok)
		}
		_ = r.stores.AddNodeSandboxRef(ctx, rec.NodeID, clusterstate.NodeSandboxRef{Group: group, RouteKey: routeKey, SandboxID: rec.SID})
		ccmd := &routesync.Command{CmdID: newID(), Kind: routesync.CmdConnect, SID: rec.SID}
		ack, err := r.nodeOwner.SendCommandAndWait(ctx, rec.NodeID, ccmd, lifecycleAckTimeout)
		if err != nil {
			_, _ = r.stores.PutSandbox(ctx, rec) // roll back RESERVED -> PAUSED (connect never reached the node)
			return err
		}
		if ack == nil || ack.Status != routesync.AckAccepted {
			_, _ = r.stores.PutSandbox(ctx, rec)
			reason := ""
			if ack != nil {
				reason = ack.Reason
			}
			return fmt.Errorf("registry: connect rejected: %s", reason)
		}
		return nil
	}

	// NONE: place + create.
	return r.placeAndCreate(ctx, group, routeKey, rec, found, createConfig)
}

// placeAndCreate places a node and sends the create command, CASing the RESERVED
// record. It re-reads + re-asks the placer once on a dead-node suggestion OR a CAS
// conflict (a lagging view / concurrent mutation, cluster.md §4.3/§5). It is
// re-drivable: a rejected create re-invokes it (re-Place once, §7.4), which
// re-reads the current rev and places afresh.
func (r *Registry) placeAndCreate(ctx context.Context, group, routeKey string, orig *SandboxRecord, found bool, createConfig map[string]string) error {
	for attempt := 0; attempt < 2; attempt++ {
		cur, curRev, curFound, err := r.stores.GetSandbox(ctx, group, routeKey)
		if err != nil {
			return err
		}
		if curFound && cur.State == StateReady {
			return nil // a concurrent attempt already won; the running route finishes the Reserve
		}
		sid := "sb-" + newID()
		placement, perr := r.placer.Place(ctx, PlaceRequest{Group: group, RouteKey: routeKey, SandboxID: sid, Config: createConfig})
		if perr != nil {
			return perr
		}
		if placement == nil || placement.NodeID == "" {
			return ErrNoNode
		}
		nodeID := placement.NodeID
		expect := int64(0)
		if curFound {
			expect = curRev
		}
		reserved := &SandboxRecord{
			Group: group, RouteKey: routeKey, SID: sid, State: StateReserved, NodeID: nodeID,
			TemplateID: placement.TemplateRef, AccessToken: placement.AccessToken,
		}
		if _, ok, cerr := r.stores.CASSandbox(ctx, reserved, expect); cerr != nil {
			return cerr
		} else if !ok {
			if attempt == 0 {
				continue // CAS conflict: re-read + re-ask once (§4.3)
			}
			return ErrNoNode
		}
		_ = r.stores.AddNodeSandboxRef(ctx, nodeID, clusterstate.NodeSandboxRef{Group: group, RouteKey: routeKey, SandboxID: sid})

		cmd := &routesync.Command{
			CmdID: newID(), Kind: routesync.CmdCreate, SID: sid, Group: group, RouteKey: routeKey,
			TemplateRef: placement.TemplateRef, Config: placement.Config,
			KeyFingerprint: placement.KeyFingerprint, AccessToken: placement.AccessToken,
		}
		if r.nodeOwner == nil {
			_ = r.stores.DeleteSandbox(ctx, group, routeKey)
			_ = r.stores.RemoveNodeSandboxRef(ctx, nodeID, group, routeKey, sid)
			return ErrNodeGone
		}
		ack, err := r.nodeOwner.SendCommandAndWait(ctx, nodeID, cmd, lifecycleAckTimeout)
		if err != nil {
			_ = r.stores.DeleteSandbox(ctx, group, routeKey)
			_ = r.stores.RemoveNodeSandboxRef(ctx, nodeID, group, routeKey, sid)
			if attempt == 0 && errors.Is(err, ErrNodeGone) {
				continue
			}
			return err
		}
		if ack == nil || ack.Status != routesync.AckAccepted {
			_ = r.stores.DeleteSandbox(ctx, group, routeKey)
			_ = r.stores.RemoveNodeSandboxRef(ctx, nodeID, group, routeKey, sid)
			if attempt == 0 {
				continue
			}
			reason := ""
			if ack != nil {
				reason = ack.Reason
			}
			return fmt.Errorf("registry: create rejected: %s", reason)
		}
		return nil
	}
	return ErrNoNode
}

// sendAndWait sends a lifecycle/build command and blocks until the node acks it
// (or timeout). key_put refreshes are best-effort heartbeat maintenance and do
// not use this path.
func (r *Registry) sendAndWait(ctx context.Context, conn nodeConn, cmd *routesync.Command, timeout time.Duration) (*routesync.CmdAck, error) {
	ch := make(chan *routesync.CmdAck, 1)
	r.mu.Lock()
	r.acks[cmd.CmdID] = ch
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		delete(r.acks, cmd.CmdID)
		r.mu.Unlock()
	}()
	if err := conn.send(cmd); err != nil {
		return nil, err
	}
	wctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	select {
	case ack := <-ch:
		return ack, nil
	case <-wctx.Done():
		return nil, wctx.Err()
	}
}

// ackCommand handles a node's CmdAck and wakes the matching SendCommandAndWait
// waiter. Lifecycle rejection/retry is handled synchronously by the node owner
// command caller.
func (r *Registry) ackCommand(ack *routesync.CmdAck) {
	if ack == nil {
		return
	}
	r.mu.Lock()
	ch, isWait := r.acks[ack.CmdID]
	r.mu.Unlock()
	if isWait {
		select {
		case ch <- ack:
		default:
		}
	}
}

// applyRoute is called by the channel reader for each sandbox route the node
// streams up. It converges SandboxStore and, on a running route, satisfies a
// waiting ReserveSandbox (cluster.md §5.1 / §7.2).
func (r *Registry) applyRoute(ctx context.Context, nodeID string, e *routesync.RouteEntry) {
	if e.Group == "" || e.RouteKey == "" {
		return // not a cluster-scoped sandbox route
	}
	if e.State == routesync.StateDead {
		r.applyDelete(ctx, e.Group, e.RouteKey)
		_ = r.stores.RemoveNodeSandboxRef(ctx, nodeID, e.Group, e.RouteKey, e.SandboxID)
		r.dropSID(e.SandboxID)
		return
	}
	rec := &SandboxRecord{
		Group: e.Group, RouteKey: e.RouteKey, SID: e.SandboxID, NodeID: nodeID,
		SnapLoc: e.SnapshotLocation, TemplateID: e.TemplateID, AccessToken: e.AccessToken,
		LastActive: time.Now().Unix(),
	}
	switch e.State {
	case routesync.StateRunning:
		rec.State = StateReady
	case routesync.StatePaused:
		rec.State = StatePaused
	default:
		r.log.Warn("registry: ignored unknown route state", "group", e.Group, "route_key", e.RouteKey, "state", e.State)
		return
	}
	r.applyLiveRoute(ctx, nodeID, e, rec)
}

func (r *Registry) applyLiveRoute(ctx context.Context, nodeID string, e *routesync.RouteEntry, rec *SandboxRecord) {
	const attempts = 5
	for attempt := 0; attempt < attempts; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Duration(attempt) * 10 * time.Millisecond):
			}
		}
		if r.tryApplyLiveRoute(ctx, nodeID, e, rec) {
			return
		}
	}
	r.log.Warn("registry: route report did not converge", "group", e.Group, "route_key", e.RouteKey, "sid", e.SandboxID)
}

func (r *Registry) tryApplyLiveRoute(ctx context.Context, nodeID string, e *routesync.RouteEntry, rec *SandboxRecord) bool {
	// Route owner fences orphan reports. A node reporting a sandbox whose
	// (group, route_key, sandbox_id) is absent or has been replaced means the
	// sandbox has already been deleted/replaced in control state; tell that node
	// to kill its local copy instead of resurrecting the route.
	cur, _, found, err := r.stores.GetSandbox(ctx, e.Group, e.RouteKey)
	if err != nil {
		if transientRouteRead(err) {
			return false
		}
		r.log.Warn("registry: read sandbox route", "group", e.Group, "err", err)
		return true
	}
	if !found || (cur.SID != "" && cur.SID != e.SandboxID) || (cur.NodeID != "" && cur.NodeID != nodeID) {
		r.deleteOrphanSandbox(ctx, nodeID, e.SandboxID, e.Group, e.RouteKey)
		_ = r.stores.RemoveNodeSandboxRef(ctx, nodeID, e.Group, e.RouteKey, e.SandboxID)
		return true
	}
	if rec.TemplateID == "" {
		rec.TemplateID = cur.TemplateID
	}
	if rec.AccessToken == "" {
		rec.AccessToken = cur.AccessToken
	}
	if _, err := r.stores.PutSandbox(ctx, rec); err != nil {
		if transientRouteRead(err) {
			return false
		}
		r.log.Warn("registry: put sandbox route", "group", e.Group, "err", err)
		return true
	}
	_ = r.stores.AddNodeSandboxRef(ctx, nodeID, clusterstate.NodeSandboxRef{Group: e.Group, RouteKey: e.RouteKey, SandboxID: e.SandboxID})
	r.indexSID(e.SandboxID, e.Group, e.RouteKey)
	if rec.State == StateReady {
		r.finish(flightKey(e.Group, e.RouteKey), &ReserveResult{NodeID: nodeID, SID: e.SandboxID, AccessToken: rec.AccessToken, DataEndpoint: r.nodeDataEndpoint(ctx, nodeID)}, nil)
	}
	return true
}

func (r *Registry) deleteOrphanSandbox(ctx context.Context, nodeID, sid, group, routeKey string) {
	if r.nodeOwner == nil || sid == "" {
		return
	}
	if err := r.nodeOwner.DeleteSandbox(ctx, nodeID, sid); err != nil && !errors.Is(err, ErrNodeGone) {
		r.log.Warn("registry: delete orphan sandbox", "node", nodeID, "sid", sid, "group", group, "route_key", routeKey, "err", err)
	}
}

// applyDelete converges a removed route (the node reports the sandbox gone).
func (r *Registry) applyDelete(ctx context.Context, group, routeKey string) {
	if group == "" || routeKey == "" {
		return
	}
	const attempts = 5
	for attempt := 0; attempt < attempts; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Duration(attempt) * 10 * time.Millisecond):
			}
		}
		if r.tryApplyDelete(ctx, group, routeKey) {
			return
		}
	}
	r.log.Warn("registry: delete route report did not converge", "group", group, "route_key", routeKey)
}

func (r *Registry) tryApplyDelete(ctx context.Context, group, routeKey string) bool {
	rec, _, found, err := r.stores.GetSandbox(ctx, group, routeKey)
	if err != nil {
		if transientRouteRead(err) {
			return false
		}
		return true
	}
	if !found {
		return true
	}
	if err := r.stores.DeleteSandbox(ctx, group, routeKey); err != nil {
		if transientRouteRead(err) {
			return false
		}
		return true
	}
	_ = r.stores.RemoveNodeSandboxRef(ctx, rec.NodeID, group, routeKey, rec.SID)
	return true
}

func (r *Registry) applyNodeFullSnapshot(ctx context.Context, nodeID string, seen map[string]string) {
	if nodeID == "" {
		return
	}
	rec, found, err := r.stores.GetNode(ctx, nodeID)
	if err != nil || !found {
		return
	}
	for _, ref := range append([]clusterstate.NodeSandboxRef(nil), rec.Sandboxes...) {
		if ref.Group == "" || ref.RouteKey == "" {
			continue
		}
		if sid, ok := seen[clusterstate.RouteKey(ref.Group, ref.RouteKey)]; ok && (sid == "" || ref.SandboxID == "" || sid == ref.SandboxID) {
			continue
		}
		cur, _, routeFound, err := r.stores.GetSandbox(ctx, ref.Group, ref.RouteKey)
		if err != nil {
			continue
		}
		if routeFound && cur.NodeID == nodeID && (ref.SandboxID == "" || cur.SID == ref.SandboxID) {
			_ = r.stores.DeleteSandbox(ctx, ref.Group, ref.RouteKey)
			r.dropSID(cur.SID)
		}
		_ = r.stores.RemoveNodeSandboxRef(ctx, nodeID, ref.Group, ref.RouteKey, ref.SandboxID)
	}
}

func (r *Registry) indexSID(sid, group, routeKey string) {
	r.mu.Lock()
	r.sidKeys[sid] = [2]string{group, routeKey}
	r.mu.Unlock()
}

// applyDeleteBySID converges a route delete (the route stream deletes by sid; the
// registry keys sandboxes by (group, route_key), so it resolves via the index).
func (r *Registry) applyDeleteBySID(ctx context.Context, nodeID, sid string) {
	r.mu.Lock()
	kp, ok := r.sidKeys[sid]
	r.mu.Unlock()
	if ok {
		r.mu.Lock()
		delete(r.sidKeys, sid)
		r.mu.Unlock()
		r.applyDelete(ctx, kp[0], kp[1])
		return
	}
	if rec, found, err := r.stores.GetNode(ctx, nodeID); err == nil && found {
		for _, ref := range rec.Sandboxes {
			if ref.SandboxID == sid {
				r.applyDelete(ctx, ref.Group, ref.RouteKey)
				return
			}
		}
	}
}

// finish satisfies the single-flight call for key (idempotent — first wins). It
func (r *Registry) finish(key string, res *ReserveResult, err error) {
	r.mu.Lock()
	call := r.inflight[key]
	r.mu.Unlock()
	if call == nil {
		return
	}
	select {
	case <-call.done:
	default:
		call.result, call.err = res, err
		close(call.done)
	}
}

func waitCall(ctx context.Context, call *reserveCall) (*ReserveResult, error) {
	select {
	case <-call.done:
		return call.result, call.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (r *Registry) waitReserveCall(ctx context.Context, call *reserveCall) (*ReserveResult, error) {
	rev, _ := r.stores.RouteGroupRev(ctx, call.group)
	watchCtx, cancelWatch := context.WithCancel(ctx)
	defer cancelWatch()
	ch, _ := r.stores.WatchRouteGroup(watchCtx, call.group, rev)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-call.done:
			return call.result, call.err
		default:
		}
		if res, ready, err := r.reserveReadyResult(ctx, call.group, call.routeKey); transientRouteRead(err) {
			// A route owner may be accepting the node's READY update concurrently;
			// keep the parked reserve until the event/read settles or times out.
		} else if err != nil {
			return nil, err
		} else if ready {
			return res, nil
		}
		select {
		case <-call.done:
			return call.result, call.err
		case <-ch:
		case <-ticker.C:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func transientRouteRead(err error) bool {
	return errors.Is(err, clusterstate.ErrQuorum) || errors.Is(err, clusterstate.ErrConflict)
}

func (r *Registry) reserveReadyResult(ctx context.Context, group, routeKey string) (*ReserveResult, bool, error) {
	rec, _, found, err := r.stores.GetSandbox(ctx, group, routeKey)
	if err != nil || !found || rec.State != StateReady {
		return nil, false, err
	}
	res, live := r.readyResultFromRecord(ctx, rec)
	return res, live, nil
}

func (r *Registry) readyResultFromRecord(ctx context.Context, rec *SandboxRecord) (*ReserveResult, bool) {
	if rec == nil || rec.NodeID == "" || r.nodeOwner == nil {
		return nil, false
	}
	node, found, err := r.nodeOwner.Runtime(ctx, rec.NodeID)
	if err != nil || !found || node == nil {
		return nil, false
	}
	return &ReserveResult{
		NodeID: rec.NodeID, SID: rec.SID, AccessToken: rec.AccessToken,
		DataEndpoint: node.DataEndpoint,
	}, true
}

// --- node channel registry ---

func (r *Registry) node(id string) (nodeConn, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	c, ok := r.nodes[id]
	return c, ok
}

// nodeDataEndpoint returns a node's data-plane endpoint (the router forwards to
// it), or "" if the node is unknown.
func (r *Registry) nodeDataEndpoint(ctx context.Context, nodeID string) string {
	if r.nodeOwner == nil {
		return ""
	}
	if n, found, _ := r.nodeOwner.Runtime(ctx, nodeID); found && n != nil {
		return n.DataEndpoint
	}
	return ""
}

func (r *Registry) addNode(c nodeConn) {
	r.mu.Lock()
	// A second connection for the same node id replaces the first (the old
	// channel's writer then fails and tears itself down).
	r.nodes[c.id()] = c
	r.mu.Unlock()
}

func (r *Registry) removeNode(c nodeConn) {
	removed := false
	r.mu.Lock()
	if r.nodes[c.id()] == c {
		delete(r.nodes, c.id())
		removed = true
	}
	r.mu.Unlock()
	if removed {
		r.scheduleNodeReap(c.id())
	}
}

// updateNodeRegister upserts the node table row from a node's register frame.
func (r *Registry) updateNodeRegister(ctx context.Context, nr *routesync.NodeRegister) error {
	rec, found, err := r.getNodeForLinkUpdate(ctx, nr.NodeID)
	if err != nil {
		return err
	}
	if !found {
		rec = &NodeRecord{}
	}
	rec.NodeID = nr.NodeID
	rec.Labels = nr.Labels
	rec.Capacity = nr.Capacity
	rec.BuildCapacity = nr.BuildCapacity
	rec.DataEndpoint = nr.DataEndpoint
	rec.RuntimeDigest = nr.RuntimeDigest
	rec.LastHeartbeatUnix = time.Now().Unix()
	rec.LinkOwner = r.stores.WriterID()
	return r.stores.PutNode(ctx, rec)
}

// updateHeartbeat folds a node's water level into its record + stamps liveness.
func (r *Registry) updateHeartbeat(ctx context.Context, nodeID string, hb *routesync.Heartbeat) {
	rec, found, err := r.getNodeForLinkUpdate(ctx, nodeID)
	if err != nil || !found {
		return
	}
	oldDraining := rec.Draining
	oldHeartbeat := rec.LastHeartbeatUnix
	rec.Zone, rec.Allocated, rec.Pool = hb.Zone, hb.Allocated, hb.Pool
	rec.BuildAlloc, rec.Counts, rec.Draining = hb.BuildAlloc, hb.Counts, hb.Draining
	rec.LastHeartbeatUnix = time.Now().Unix()
	_ = r.stores.PutNodeRuntime(ctx, rec)
	r.refreshNodeManifestKeys(ctx, rec)
	if rec.Draining != oldDraining || oldHeartbeat <= 0 || rec.LastHeartbeatUnix-oldHeartbeat >= r.stores.NodeListHeartbeatRefreshSec() {
		_ = r.stores.PutNodeList(ctx, rec)
	}
}

func (r *Registry) refreshNodeManifestKeys(ctx context.Context, rec *NodeRecord) {
	if rec == nil || rec.NodeID == "" || len(rec.ManifestKeys) == 0 {
		return
	}
	if owner, ok := r.localNodeOwner.(*localNodeOwner); ok {
		owner.RefreshManifestKeys(ctx, rec.NodeID, rec.ManifestKeys)
	}
	_ = r.stores.PruneExpiredNodeManifestKeys(ctx, rec.NodeID, time.Now().Unix())
}

func (r *Registry) updateNodeResume(ctx context.Context, nodeID, token string) {
	rec, found, err := r.getNodeForLinkUpdate(ctx, nodeID)
	if err != nil || !found {
		return
	}
	rec.ResumeToken = token
	_ = r.stores.PutNodeRuntime(ctx, rec)
}

func (r *Registry) getNodeForLinkUpdate(ctx context.Context, nodeID string) (*NodeRecord, bool, error) {
	rec, found, err := r.stores.GetNode(ctx, nodeID)
	if errors.Is(err, shardkv.ErrInvalidView) {
		return r.stores.GetNodeProfile(ctx, nodeID)
	}
	return rec, found, err
}

// RunReaper configures event-driven dead-node cleanup. A node-link disconnect
// schedules cleanup for that node only; registry never scans node shards globally.
func (r *Registry) RunReaper(ctx context.Context, deadAfter time.Duration) {
	if deadAfter <= 0 {
		deadAfter = 30 * time.Second
	}
	r.mu.Lock()
	r.reaperCtx = ctx
	r.deadAfter = deadAfter
	r.mu.Unlock()
	<-ctx.Done()
}

func (r *Registry) RunCompactor(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = 5 * time.Minute
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			stats, err := r.stores.Compact(ctx, now)
			if err != nil {
				r.log.Warn("registry: shardkv compaction failed", "err", err)
				continue
			}
			if stats.Tombstones > 0 || stats.Shards > 0 {
				r.log.Debug("registry: shardkv compacted", "tombstones", stats.Tombstones, "shards", stats.Shards)
			}
		}
	}
}

func (r *Registry) scheduleNodeReap(nodeID string) {
	r.mu.Lock()
	ctx := r.reaperCtx
	deadAfter := r.deadAfter
	r.mu.Unlock()
	if ctx == nil {
		ctx = context.Background()
	}
	if deadAfter <= 0 {
		deadAfter = 30 * time.Second
	}
	go func() {
		timer := time.NewTimer(deadAfter)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			r.sweepNode(ctx, nodeID, deadAfter)
		}
	}()
}

// sweepNode resets one disconnected node's route/build refs from that node shard.
func (r *Registry) sweepNode(ctx context.Context, nodeID string, deadAfter time.Duration) {
	if nodeID == "" {
		return
	}
	if _, connected := r.node(nodeID); connected {
		return
	}
	rec, found, err := r.stores.GetNode(ctx, nodeID)
	if err != nil || !found {
		return
	}
	cutoff := time.Now().Add(-deadAfter).Unix()
	if rec.LastHeartbeatUnix <= 0 || rec.LastHeartbeatUnix >= cutoff {
		return
	}
	reset := 0
	for _, ref := range append([]clusterstate.NodeSandboxRef(nil), rec.Sandboxes...) {
		s, _, found, err := r.stores.GetSandbox(ctx, ref.Group, ref.RouteKey)
		if err != nil || !found || s.NodeID != nodeID {
			continue
		}
		shouldReset := s.State == StateReady || s.State == StatePaused
		if s.State == StateReserved && !r.hasInflight(flightKey(s.Group, s.RouteKey)) {
			shouldReset = true
		}
		if !shouldReset {
			continue
		}
		_ = r.stores.DeleteSandbox(ctx, s.Group, s.RouteKey)
		_ = r.stores.RemoveNodeSandboxRef(ctx, nodeID, s.Group, s.RouteKey, s.SID)
		r.dropSID(s.SID)
		reset++
	}
	deadBuilds := 0
	for _, ref := range append([]clusterstate.NodeBuildRef(nil), rec.Builds...) {
		b, found, err := r.stores.GetBuildInGroup(ctx, ref.Group, ref.BuildID)
		if err != nil || !found || b.NodeID != nodeID || !b.occupies() {
			continue
		}
		b.State, b.Reason = BuildError, "node disconnected"
		_ = r.stores.PutBuild(ctx, b)
		_ = r.stores.RemoveNodeBuildRef(ctx, nodeID, b.Group, b.BuildID)
		r.releaseBuildAdmission(b.BuildID)
		deadBuilds++
	}
	_ = r.stores.DeleteNode(ctx, nodeID)
	r.log.Warn("registry: swept dead node", "node", nodeID, "sandboxes_reset", reset, "builds_errored", deadBuilds)
}

func (r *Registry) dropSID(sid string) {
	r.mu.Lock()
	delete(r.sidKeys, sid)
	r.mu.Unlock()
}

func (r *Registry) hasInflight(key string) bool {
	r.mu.Lock()
	_, ok := r.inflight[key]
	r.mu.Unlock()
	return ok
}

// mergeConfig folds a group's sandbox_config defaults under the create config
// (create overrides group); returns create unchanged when there are no defaults.
func mergeConfig(group, create map[string]string) map[string]string {
	if len(group) == 0 {
		return create
	}
	merged := make(map[string]string, len(group)+len(create))
	for k, v := range group {
		merged[k] = v
	}
	for k, v := range create {
		merged[k] = v
	}
	return merged
}

func cas(err error, ok bool) error {
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("registry: reserve CAS conflict")
	}
	return nil
}

func newID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// keyFingerprint is the fingerprint the node matches against its manifest-key
// allowlist (= store.ManifestKeyHash: hex(apikey.Fingerprint(rawKey))).
func keyFingerprint(manifestKeyHex string) string {
	raw, err := hex.DecodeString(manifestKeyHex)
	if err != nil {
		return ""
	}
	return hex.EncodeToString(apikey.Fingerprint(raw))
}
