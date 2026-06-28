package registry

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/apikey"
	clusterstate "github.com/kuasar-sandbox/sandbox-orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/routesync"
)

// ErrNoNode is returned when no eligible node can host a sandbox.
var ErrNoNode = errors.New("registry: no eligible node")

// ErrNodeGone is returned when the placed node's channel disappeared mid-reserve.
var ErrNodeGone = errors.New("registry: placed node not connected")

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

	mu        sync.Mutex
	nodes     map[string]nodeConn               // node_id -> channel
	inflight  map[string]*reserveCall           // single-flight ReserveSandbox per (group,route_key)
	sidKeys   map[string][2]string              // sid -> {group, route_key} (delete-by-sid from the route stream)
	acks      map[string]chan *routesync.CmdAck // cmd_id -> ack waiter (synchronous key commands)
	cmdFlight map[string]string                 // create/connect cmd_id -> flightKey (ack-reject fast-fails Reserve)

	keyMu     sync.Mutex
	keyLeased map[string]keyLeaseState // group -> nodes currently holding the predistributed key

	reconcileTrigger chan struct{} // coalesced key-reconcile wakeups (a node connecting)

	localNodeOwner NodeOwner
	nodeOwner      NodeOwner

	scalerMu        sync.Mutex
	scaleReadyLabel string
	scalerPeers     map[string]scalerPeer
	keyAlloc        map[string]keyAllocationState // group -> scaler-owned manifest-key allocation set
}

type scalerPeer struct {
	ID         string
	Advertise  string
	ReadyLabel string
	LastSeen   time.Time
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
	retried         bool
}

// ReserveResult is what a satisfied ReserveSandbox returns (cluster.md §7.2). The
// router injects AccessToken and forwards to the node's DataEndpoint.
type ReserveResult struct {
	NodeID       string `json:"node_id"`
	SID          string `json:"sid"`
	AccessToken  string `json:"access_token"`
	DataEndpoint string `json:"data_endpoint"`
}

// New builds a Registry. If placer is nil a built-in least-loaded placer is used.
func New(stores *Stores, placer Placer, parkTimeout time.Duration, log *slog.Logger) *Registry {
	if placer == nil {
		placer = &builtinPlacer{stores: stores}
	}
	if parkTimeout <= 0 {
		parkTimeout = 30 * time.Second
	}
	r := &Registry{
		stores:           stores,
		placer:           placer,
		parkTimeout:      parkTimeout,
		log:              log,
		nodes:            make(map[string]nodeConn),
		inflight:         make(map[string]*reserveCall),
		sidKeys:          make(map[string][2]string),
		acks:             make(map[string]chan *routesync.CmdAck),
		cmdFlight:        make(map[string]string),
		keyLeased:        make(map[string]keyLeaseState),
		reconcileTrigger: make(chan struct{}, 1),
		scalerPeers:      make(map[string]scalerPeer),
		keyAlloc:         make(map[string]keyAllocationState),
	}
	localOwner := newLocalNodeOwner(r)
	r.localNodeOwner = localOwner
	r.nodeOwner = localOwner
	return r
}

// applySelectorPatch records the scaler-owned key allocation for a group. The
// registry/node owner executes key_put/key_drop to this explicit node set; local
// selector matching is only a bootstrap path before the scaler has pushed allocation.
func (r *Registry) applySelectorPatch(p *routesync.SelectorPatch) {
	r.scalerMu.Lock()
	if p.NodeAllocation {
		set := make(map[string]bool, len(p.NodeIDs))
		for _, id := range p.NodeIDs {
			if id != "" {
				set[id] = true
			}
		}
		keyType, keyValue := p.ManifestKeyType, p.ManifestKey
		if keyType == "ref" {
			keyValue = p.ManifestKeyRef
		}
		if keyType == "" && keyValue != "" {
			keyType = clusterstate.SecretInline
		}
		r.keyAlloc[p.Group] = keyAllocationState{fp: p.KeyFingerprint, keyType: keyType, keyValue: keyValue, nodes: set}
	}
	r.scalerMu.Unlock()
	r.onNodeConnected()
}

func (r *Registry) keyAllocations() map[string]keyAllocationState {
	r.scalerMu.Lock()
	defer r.scalerMu.Unlock()
	out := make(map[string]keyAllocationState, len(r.keyAlloc))
	for group, src := range r.keyAlloc {
		nodes := make(map[string]bool, len(src.nodes))
		for id := range src.nodes {
			nodes[id] = true
		}
		out[group] = keyAllocationState{fp: src.fp, keyType: src.keyType, keyValue: src.keyValue, nodes: nodes}
	}
	return out
}

// SetScaleReadyLabel sets the registry membership label a scaler must advertise
// before it is used for Place requests.
func (r *Registry) SetScaleReadyLabel(label string) {
	r.scalerMu.Lock()
	r.scaleReadyLabel = label
	r.scalerMu.Unlock()
}

func (r *Registry) setScalerPeer(peer scalerPeer) {
	r.scalerMu.Lock()
	r.scalerPeers[peer.ID] = peer
	r.scalerMu.Unlock()
}

func (r *Registry) readyScalerPeers(maxAge time.Duration) []scalerPeer {
	now := time.Now()
	r.scalerMu.Lock()
	defer r.scalerMu.Unlock()
	out := make([]scalerPeer, 0, len(r.scalerPeers))
	for id, peer := range r.scalerPeers {
		if maxAge > 0 && now.Sub(peer.LastSeen) > maxAge {
			delete(r.scalerPeers, id)
			continue
		}
		if r.scaleReadyLabel != "" && peer.ReadyLabel != r.scaleReadyLabel {
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

func (r *Registry) LocalNodeOwner() NodeOwner { return r.localNodeOwner }

// Stores exposes the typed store layer used by registry route/node/build state.
func (r *Registry) Stores() *Stores { return r.stores }

func flightKey(group, routeKey string) string { return group + "\x00" + routeKey }

// ReserveSandbox resolves (group, route_key) to a running sandbox, placing +
// creating (or resuming a PAUSED sandbox) on a node and waiting for the node to
// report it running, single-flight per key (cluster.md §7.2).
func (r *Registry) ReserveSandbox(ctx context.Context, group, routeKey string, createConfig map[string]string) (*ReserveResult, error) {
	if isBuildRouteKey(routeKey) {
		return nil, fmt.Errorf("registry: route_key prefix %q is reserved", buildRouteKeyPrefix)
	}
	rec, rev, found, err := r.stores.GetSandbox(ctx, group, routeKey)
	if err != nil {
		return nil, err
	}
	if found && rec.State == StateReady {
		if _, live := r.node(rec.NodeID); live {
			return &ReserveResult{NodeID: rec.NodeID, SID: rec.SID, AccessToken: rec.AccessToken, DataEndpoint: r.nodeDataEndpoint(ctx, rec.NodeID)}, nil
		}
		// node gone: fall through to re-place (dead-node sweep also resets it).
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
	res, rerr := waitCall(wctx, call)
	if rerr != nil {
		// Park timeout / caller cancel: undo a RESERVED that never reached READY, so
		// it doesn't strand on a live node (the sweep only clears dead-node rows).
		r.rollbackReserve(group, routeKey, rec, found)
	}
	return res, rerr
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
		r.dropSID(cur.SID)
	}
}

// startReserve drives the placement + command for the leader of a single-flight.
// It does not wait — the channel reader signals completion when the node reports
// the sandbox running (finish, via applyRoute).
func (r *Registry) startReserve(ctx context.Context, group, routeKey string, rec *SandboxRecord, rev int64, found bool, createConfig map[string]string) error {
	// PAUSED: resume on the same node (no placement).
	if found && rec.State == StatePaused && rec.NodeID != "" {
		conn, live := r.node(rec.NodeID)
		if !live {
			return ErrNodeGone
		}
		reserved := *rec
		reserved.State = StateReserved
		if _, ok, err := r.stores.CASSandbox(ctx, &reserved, rev); err != nil || !ok {
			return cas(err, ok)
		}
		ccmd := &routesync.Command{CmdID: newID(), Kind: routesync.CmdConnect, SID: rec.SID}
		r.trackCmd(ccmd.CmdID, flightKey(group, routeKey))
		if err := conn.send(ccmd); err != nil {
			_, _ = r.stores.PutSandbox(ctx, rec) // roll back RESERVED -> PAUSED (connect never reached the node)
			return err
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
		conn, live := r.node(nodeID)
		if !live {
			if attempt == 0 {
				continue // re-ask once (the suggested node just dropped / stale view)
			}
			return ErrNodeGone
		}
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

		cmd := &routesync.Command{
			CmdID: newID(), Kind: routesync.CmdCreate, SID: sid, Group: group, RouteKey: routeKey,
			TemplateRef: placement.TemplateRef, Config: placement.Config,
			KeyFingerprint: placement.KeyFingerprint, AccessToken: placement.AccessToken,
		}
		r.trackCmd(cmd.CmdID, flightKey(group, routeKey))
		if err := conn.send(cmd); err != nil {
			_ = r.stores.DeleteSandbox(ctx, group, routeKey)
			return err
		}
		return nil
	}
	return ErrNoNode
}

// sendAndWait sends a command and blocks until the node acks it (or timeout) — for
// synchronous commands (key_put/renew/drop), so a build forward gates on the key
// being installed (cluster.md §5.1) instead of racing it over the data plane.
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

// trackCmd registers a lifecycle command's cmd_id against its single-flight key so
// a rejected ack fast-fails the Reserve (instead of waiting out park_timeout).
func (r *Registry) trackCmd(cmdID, key string) {
	r.mu.Lock()
	r.cmdFlight[cmdID] = key
	r.mu.Unlock()
}

// ackCommand handles a node's CmdAck: wake a sendAndWait waiter, or fast-fail the
// Reserve a rejected lifecycle command belongs to.
func (r *Registry) ackCommand(ack *routesync.CmdAck) {
	if ack == nil {
		return
	}
	r.mu.Lock()
	ch, isWait := r.acks[ack.CmdID]
	fk, isFlight := r.cmdFlight[ack.CmdID]
	if isFlight {
		delete(r.cmdFlight, ack.CmdID)
	}
	r.mu.Unlock()
	if isWait {
		select {
		case ch <- ack:
		default:
		}
		return
	}
	if isFlight && ack.Status == routesync.AckRejected {
		// A rejected create re-places once on another node before failing the Reserve
		// (cluster.md §7.4): a node-specific reject (e.g. a missing key lease) often
		// succeeds elsewhere; a second reject (or non-create reject) fails it.
		if r.replaceOnReject(fk) {
			return
		}
		r.finish(fk, nil, fmt.Errorf("registry: node rejected command: %s", ack.Reason))
	}
}

// replaceOnReject re-drives placement once for a rejected flight (returns true if
// it re-placed, so the Reserve keeps waiting).
func (r *Registry) replaceOnReject(key string) bool {
	r.mu.Lock()
	call := r.inflight[key]
	if call == nil || call.retried {
		r.mu.Unlock()
		return false
	}
	call.retried = true
	g, rk, orig, found, cfg := call.group, call.routeKey, call.orig, call.found, call.createConfig
	r.mu.Unlock()
	go func() {
		// Background ctx: the re-Place must outlive the channel-reader callback; the
		// placer applies its own timeout. A failure fails the Reserve.
		if err := r.placeAndCreate(context.Background(), g, rk, orig, found, cfg); err != nil {
			r.finish(key, nil, fmt.Errorf("registry: re-place after reject failed: %w", err))
		}
	}()
	return true
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
	// Route owner fences orphan reports. A node reporting a sandbox whose
	// (group, route_key, sandbox_id) is absent or has been replaced means the
	// sandbox has already been deleted/replaced in control state; tell that node
	// to kill its local copy instead of resurrecting the route.
	cur, _, found, err := r.stores.GetSandbox(ctx, e.Group, e.RouteKey)
	if err != nil {
		r.log.Warn("registry: read sandbox route", "group", e.Group, "err", err)
		return
	}
	if !found || (cur.SID != "" && cur.SID != e.SandboxID) || (cur.NodeID != "" && cur.NodeID != nodeID) {
		r.deleteOrphanSandbox(ctx, nodeID, e.SandboxID, e.Group, e.RouteKey)
		return
	}
	if rec.TemplateID == "" {
		rec.TemplateID = cur.TemplateID
	}
	if rec.AccessToken == "" {
		rec.AccessToken = cur.AccessToken
	}
	if _, err := r.stores.PutSandbox(ctx, rec); err != nil {
		r.log.Warn("registry: put sandbox route", "group", e.Group, "err", err)
		return
	}
	r.indexSID(e.SandboxID, e.Group, e.RouteKey)
	if rec.State == StateReady {
		r.finish(flightKey(e.Group, e.RouteKey), &ReserveResult{NodeID: nodeID, SID: e.SandboxID, AccessToken: rec.AccessToken, DataEndpoint: r.nodeDataEndpoint(ctx, nodeID)}, nil)
	}
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
	_ = r.stores.DeleteSandbox(ctx, group, routeKey)
}

func (r *Registry) indexSID(sid, group, routeKey string) {
	r.mu.Lock()
	r.sidKeys[sid] = [2]string{group, routeKey}
	r.mu.Unlock()
}

// applyDeleteBySID converges a route delete (the route stream deletes by sid; the
// registry keys sandboxes by (group, route_key), so it resolves via the index).
func (r *Registry) applyDeleteBySID(ctx context.Context, sid string) {
	r.mu.Lock()
	kp, ok := r.sidKeys[sid]
	r.mu.Unlock()
	if !ok {
		return
	}
	r.mu.Lock()
	delete(r.sidKeys, sid)
	r.mu.Unlock()
	r.applyDelete(ctx, kp[0], kp[1])
}

// finish satisfies the single-flight call for key (idempotent — first wins). It
// also drops any in-flight command tracked for this key, so cmdFlight doesn't
// leak when the route event finishes the Reserve before/without the cmd_ack.
func (r *Registry) finish(key string, res *ReserveResult, err error) {
	r.mu.Lock()
	call := r.inflight[key]
	for id, fk := range r.cmdFlight {
		if fk == key {
			delete(r.cmdFlight, id)
		}
	}
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
	r.mu.Lock()
	if r.nodes[c.id()] == c {
		delete(r.nodes, c.id())
	}
	r.mu.Unlock()
}

// updateNodeRegister upserts the node table row from a node's register frame.
func (r *Registry) updateNodeRegister(ctx context.Context, nr *routesync.NodeRegister) error {
	rec, found, err := r.stores.GetNode(ctx, nr.NodeID)
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
	rec, found, err := r.stores.GetNode(ctx, nodeID)
	if err != nil || !found {
		return
	}
	oldDraining := rec.Draining
	oldHeartbeat := rec.LastHeartbeatUnix
	rec.Zone, rec.Allocated, rec.Pool = hb.Zone, hb.Allocated, hb.Pool
	rec.BuildAlloc, rec.Counts, rec.Draining = hb.BuildAlloc, hb.Counts, hb.Draining
	rec.LastHeartbeatUnix = time.Now().Unix()
	_ = r.stores.PutNodeRuntime(ctx, rec)
	if rec.Draining != oldDraining || oldHeartbeat <= 0 || rec.LastHeartbeatUnix-oldHeartbeat >= r.stores.NodeListHeartbeatRefreshSec() {
		_ = r.stores.PutNodeList(ctx, rec)
	}
}

func (r *Registry) updateNodeResume(ctx context.Context, nodeID, token string) {
	rec, found, err := r.stores.GetNode(ctx, nodeID)
	if err != nil || !found {
		return
	}
	rec.ResumeToken = token
	_ = r.stores.PutNodeRuntime(ctx, rec)
}

// RunReaper periodically sweeps dead nodes until ctx is cancelled (cluster.md
// §11); cluster-ctl registry runs it in the background.
func (r *Registry) RunReaper(ctx context.Context, deadAfter time.Duration) {
	if deadAfter <= 0 {
		deadAfter = 30 * time.Second
	}
	tick := deadAfter / 3
	if tick < time.Second {
		tick = time.Second
	}
	t := time.NewTicker(tick)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			r.sweepDeadNodes(ctx, deadAfter)
		}
	}
}

// sweepDeadNodes resets the sandboxes of every disconnected node whose last
// heartbeat predates node_dead_after, then removes the node record (cluster.md
// §11): READY/PAUSED (node-local snapshot, lost with the node) → reset so the
// next Reserve re-places; a RESERVED row is reset only when no in-flight Reserve
// owns it, so the sweep can't delete a reservation under a concurrent reserve on
// a reconnect blip.
func (r *Registry) sweepDeadNodes(ctx context.Context, deadAfter time.Duration) {
	cutoff := time.Now().Add(-deadAfter).Unix()
	var dead []string
	_ = r.stores.RangeNodes(ctx, func(n *NodeRecord) error {
		if _, connected := r.node(n.NodeID); connected {
			return nil // a live channel is not dead (a quiet water level is fine)
		}
		if n.LastHeartbeatUnix > 0 && n.LastHeartbeatUnix < cutoff {
			dead = append(dead, n.NodeID)
		}
		return nil
	})
	if len(dead) == 0 {
		return
	}
	deadSet := make(map[string]bool, len(dead))
	for _, id := range dead {
		deadSet[id] = true
	}
	// Collect first (the Range callback is read-only), then mutate.
	var reset []*SandboxRecord
	_ = r.stores.RangeAllSandboxes(ctx, func(s *SandboxRecord) error {
		if !deadSet[s.NodeID] {
			return nil
		}
		switch s.State {
		case StateReady, StatePaused:
			reset = append(reset, &SandboxRecord{Group: s.Group, RouteKey: s.RouteKey, SID: s.SID})
		case StateReserved:
			// Owned by an in-flight single-flight; only sweep a stale reservation
			// (no live reserve), never one a concurrent Reserve still holds.
			if !r.hasInflight(flightKey(s.Group, s.RouteKey)) {
				reset = append(reset, &SandboxRecord{Group: s.Group, RouteKey: s.RouteKey, SID: s.SID})
			}
		}
		return nil
	})
	for _, s := range reset {
		_ = r.stores.DeleteSandbox(ctx, s.Group, s.RouteKey)
		r.dropSID(s.SID)
	}
	// A dead node's in-flight builds go to error (§11) — their reservation releases
	// (the headroom sum counts only registered/building), and the e2b client sees
	// the failure via the router's build status.
	var deadBuilds []*BuildRecord
	_ = r.stores.RangeBuilds(ctx, func(b *BuildRecord) error {
		if deadSet[b.NodeID] && b.occupies() {
			cp := *b
			deadBuilds = append(deadBuilds, &cp)
		}
		return nil
	})
	for _, b := range deadBuilds {
		b.State, b.Reason = BuildError, "node disconnected"
		_ = r.stores.PutBuild(ctx, b)
		r.releaseBuildAdmission(b.BuildID)
	}
	for _, id := range dead {
		_ = r.stores.DeleteNode(ctx, id)
	}
	r.log.Warn("registry: swept dead nodes", "nodes", dead, "sandboxes_reset", len(reset), "builds_errored", len(deadBuilds))
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
