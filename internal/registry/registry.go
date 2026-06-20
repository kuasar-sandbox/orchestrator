package registry

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
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

	mu       sync.Mutex
	nodes    map[string]nodeConn     // node_id -> channel
	inflight map[string]*reserveCall // single-flight ReserveSandbox per (group,route_key)
	sidKeys  map[string][2]string    // sid -> {group, route_key} (delete-by-sid from the route stream)
}

// reserveCall is one in-flight ReserveSandbox; joiners wait on done, the channel
// reader fills result + closes done when the sandbox goes running.
type reserveCall struct {
	done   chan struct{}
	result *ReserveResult
	err    error
}

// ReserveResult is what a satisfied ReserveSandbox returns (cluster.md §7.2).
type ReserveResult struct {
	NodeID      string
	SID         string
	AccessToken string
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
			return &ReserveResult{NodeID: rec.NodeID, SID: rec.SID, AccessToken: rec.AccessToken}, nil
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
		return conn.send(&routesync.Command{CmdID: newID(), Kind: routesync.CmdConnect, SID: rec.SID})
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
			cmd.KeyFingerprint = keyFingerprint(g.ManifestKey)
		}
	}
	return conn.send(cmd)
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
	if _, err := r.stores.PutSandbox(ctx, rec); err != nil {
		r.log.Warn("registry: put sandbox route", "group", e.Group, "err", err)
		return
	}
	r.indexSID(e.SandboxID, e.Group, e.RouteKey)
	if rec.State == StateReady {
		r.finish(flightKey(e.Group, e.RouteKey), &ReserveResult{NodeID: nodeID, SID: e.SandboxID, AccessToken: e.AccessToken}, nil)
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

// finish satisfies the single-flight call for key (idempotent — first wins).
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

// --- node channel registry ---

func (r *Registry) node(id string) (nodeConn, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	c, ok := r.nodes[id]
	return c, ok
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
	return r.stores.PutNode(ctx, rec)
}

// updateHeartbeat folds a node's water level into its record.
func (r *Registry) updateHeartbeat(ctx context.Context, nodeID string, hb *routesync.Heartbeat) {
	rec, found, err := r.stores.GetNode(ctx, nodeID)
	if err != nil || !found {
		return
	}
	rec.Zone, rec.Allocated, rec.Pool = hb.Zone, hb.Allocated, hb.Pool
	rec.BuildAlloc, rec.Counts, rec.Draining = hb.BuildAlloc, hb.Counts, hb.Draining
	_ = r.stores.PutNode(ctx, rec)
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
