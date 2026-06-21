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

// Placer suggests a node for a new sandbox (cluster.md §4.3: the scaler suggests,
// the registry commits by CAS). Phase 2 ships a built-in single-node placer; the
// cluster-ctl scaler replaces it over the op channel in Phase 4.
type Placer interface {
	PlaceSandbox(ctx context.Context, group, routeKey string) (nodeID string, err error)
}

// Registry is the cluster control plane's state authority + node-link hub.
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
	keyLeased map[string]map[string]bool // group -> node_ids currently holding the predistributed key
}

// reserveCall is one in-flight ReserveSandbox; joiners wait on done, the channel
// reader fills result + closes done when the sandbox goes running.
type reserveCall struct {
	done   chan struct{}
	result *ReserveResult
	err    error
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
	return &Registry{
		stores:      stores,
		placer:      placer,
		parkTimeout: parkTimeout,
		log:         log,
		nodes:       make(map[string]nodeConn),
		inflight:    make(map[string]*reserveCall),
		sidKeys:     make(map[string][2]string),
		acks:        make(map[string]chan *routesync.CmdAck),
		cmdFlight:   make(map[string]string),
		keyLeased:   make(map[string]map[string]bool),
	}
}

// Stores exposes the typed store layer (cluster-ctl seeds group config; tests).
func (r *Registry) Stores() *Stores { return r.stores }

func flightKey(group, routeKey string) string { return group + "\x00" + routeKey }

// ReserveSandbox resolves (group, route_key) to a running sandbox, placing +
// creating (or resuming a PAUSED sandbox) on a node and waiting for the node to
// report it running, single-flight per key (cluster.md §7.2).
func (r *Registry) ReserveSandbox(ctx context.Context, group, routeKey string, createConfig map[string]string) (*ReserveResult, error) {
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
	call := &reserveCall{done: make(chan struct{})}
	r.inflight[key] = call
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		delete(r.inflight, key)
		r.mu.Unlock()
	}()

	if err := r.startReserve(ctx, group, routeKey, rec, rev, found, createConfig); err != nil {
		r.finish(key, nil, err)
		return waitCall(ctx, call)
	}
	wctx, cancel := context.WithTimeout(ctx, r.parkTimeout)
	defer cancel()
	return waitCall(wctx, call)
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

	// NONE / SAVED: place + create.
	nodeID, err := r.placer.PlaceSandbox(ctx, group, routeKey)
	if err != nil {
		return err
	}
	conn, live := r.node(nodeID)
	if !live {
		return ErrNodeGone
	}
	sid := "sb-" + newID()
	migrationToken := ""
	if found && rec.State == StateSaved {
		migrationToken = rec.MigrationToken
	}
	reserved := &SandboxRecord{Group: group, RouteKey: routeKey, SID: sid, State: StateReserved, NodeID: nodeID}
	expect := int64(0)
	if found {
		expect = rev
	}
	if _, ok, err := r.stores.CASSandbox(ctx, reserved, expect); err != nil || !ok {
		return cas(err, ok)
	}

	cmd := &routesync.Command{CmdID: newID(), Kind: routesync.CmdCreate, SID: sid, Group: group, RouteKey: routeKey, Config: createConfig, MigrationToken: migrationToken}
	if g, gok, _ := r.stores.GetGroupByID(ctx, group); gok {
		cmd.TemplateRef = g.TemplateRef
		if g.ManifestKey != "" {
			// The key is predistributed to the group's allocation set ahead of
			// placement (cluster.md §7.6; reconcileKeys); create only references it
			// by fingerprint. A node missing it fails precheck -> rejected ack.
			cmd.KeyFingerprint = keyFingerprint(g.ManifestKey)
		}
	}
	r.trackCmd(cmd.CmdID, flightKey(group, routeKey))
	if err := conn.send(cmd); err != nil {
		// Roll back the reservation that never reached the node, so it doesn't
		// strand a RESERVED tombstone on a live node (the sweep won't clear it).
		_ = r.stores.DeleteSandbox(ctx, group, routeKey)
		return err
	}
	return nil
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
		r.finish(fk, nil, fmt.Errorf("registry: node rejected command: %s", ack.Reason))
	}
}

// applyRoute is called by the channel reader for each sandbox route the node
// streams up. It converges SandboxStore and, on a running route, satisfies a
// waiting ReserveSandbox (cluster.md §5.1 / §7.2).
func (r *Registry) applyRoute(ctx context.Context, nodeID string, e *routesync.RouteEntry) {
	if e.Group == "" || e.RouteKey == "" {
		return // not a cluster-scoped sandbox route
	}
	rec := &SandboxRecord{
		Group: e.Group, RouteKey: e.RouteKey, SID: e.SandboxID, NodeID: nodeID,
		AccessToken: e.AccessToken, MigrationToken: e.MigrationToken, SnapLoc: e.SnapshotLocation,
		TemplateID: e.TemplateID, LastActive: time.Now().Unix(),
	}
	switch e.State {
	case routesync.StateRunning:
		rec.State = StateReady
	case routesync.StatePaused:
		rec.State = StatePaused
	case routesync.StateDead:
		rec.State = StateNone
	default:
		rec.State = StateReady
	}
	// Fence a stale upsert from a node that no longer owns this (group,route_key):
	// a re-placement CASes the record onto the new node, so the new owner's routes
	// apply (its record is RESERVED, not yet READY), while a former owner's late
	// re-stream (after a reconnect / false-positive sweep) can't resurrect a route
	// the registry already moved.
	if cur, _, found, _ := r.stores.GetSandbox(ctx, e.Group, e.RouteKey); found &&
		cur.NodeID != "" && cur.NodeID != nodeID && cur.State == StateReady {
		return
	}
	if _, err := r.stores.PutSandbox(ctx, rec); err != nil {
		r.log.Warn("registry: put sandbox route", "group", e.Group, "err", err)
		return
	}
	r.indexSID(e.SandboxID, e.Group, e.RouteKey)
	if rec.State == StateReady {
		r.finish(flightKey(e.Group, e.RouteKey), &ReserveResult{NodeID: nodeID, SID: e.SandboxID, AccessToken: e.AccessToken, DataEndpoint: r.nodeDataEndpoint(ctx, nodeID)}, nil)
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
	delete(r.sidKeys, sid)
	r.mu.Unlock()
	if ok {
		r.applyDelete(ctx, kp[0], kp[1])
	}
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
	if n, found, _ := r.stores.GetNode(ctx, nodeID); found && n != nil {
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
	return r.stores.PutNode(ctx, rec)
}

// updateHeartbeat folds a node's water level into its record + stamps liveness.
func (r *Registry) updateHeartbeat(ctx context.Context, nodeID string, hb *routesync.Heartbeat) {
	rec, found, err := r.stores.GetNode(ctx, nodeID)
	if err != nil || !found {
		return
	}
	rec.Zone, rec.Allocated, rec.Pool = hb.Zone, hb.Allocated, hb.Pool
	rec.BuildAlloc, rec.Counts, rec.Draining = hb.BuildAlloc, hb.Counts, hb.Draining
	rec.LastHeartbeatUnix = time.Now().Unix()
	_ = r.stores.PutNode(ctx, rec)
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
// owns it (so the sweep can't delete a reservation under a concurrent reserve on
// a reconnect blip); SAVED (remote, unbound) is left untouched.
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
	for _, id := range dead {
		_ = r.stores.DeleteNode(ctx, id)
	}
	r.log.Warn("registry: swept dead nodes", "nodes", dead, "sandboxes_reset", len(reset))
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
