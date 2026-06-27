// Package registry is the cluster control plane's route/node owner and node_link
// hub. route_link and node_link state go through the registry-owned quorum
// kernel; group/build configuration remains in local typed tables until those
// namespaces move behind the same member RPC boundary.
package registry

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	clusterstate "github.com/kuasar-sandbox/sandbox-orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/clusterstore"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/secretbox"
)

// Key prefixes for registry-local typed tables.
const (
	groupPrefix = "group/" // group/<group>               (config; keys sealed)
	buildPrefix = "build/" // build/<esc(group)>/<build_id> (group-sharded, §6.1)
)

// SandboxState mirrors cluster.md route state. A missing/dead sandbox is
// represented by no route row.
type SandboxState string

const (
	StateNone     SandboxState = "none"
	StateReserved SandboxState = "reserved"
	StateReady    SandboxState = "ready"
	StatePaused   SandboxState = "paused"
)

// NodeRecord is the registry-facing node_link view.
type NodeRecord struct {
	NodeID        string                    `json:"node_id"`
	Labels        map[string]string         `json:"labels,omitempty"`
	Capacity      int                       `json:"capacity,omitempty"`
	BuildCapacity *routesync.BuildResources `json:"build_capacity,omitempty"`
	DataEndpoint  string                    `json:"data_endpoint,omitempty"`
	RuntimeDigest string                    `json:"runtime_digest,omitempty"`
	Zone          string                    `json:"zone,omitempty"`
	Allocated     int64                     `json:"allocated,omitempty"`
	Pool          int64                     `json:"pool,omitempty"`
	BuildAlloc    *routesync.BuildResources `json:"build_alloc,omitempty"`
	Counts        int                       `json:"counts,omitempty"`
	Draining      bool                      `json:"draining,omitempty"`
	// LastHeartbeatUnix is the last sign of life (register or heartbeat); the
	// dead-node sweep (§11) resets a disconnected node whose last beat predates
	// node_dead_after.
	LastHeartbeatUnix int64  `json:"last_heartbeat_unix,omitempty"`
	ResumeToken       string `json:"resume_token,omitempty"`
}

// SandboxRecord is the registry-facing route_link view, keyed by
// (group, route_key).
type SandboxRecord struct {
	Group      string       `json:"group"`
	RouteKey   string       `json:"route_key"`
	SID        string       `json:"sid,omitempty"`
	State      SandboxState `json:"state"`
	NodeID     string       `json:"node_id,omitempty"`
	SnapLoc    string       `json:"snap_loc,omitempty"`
	TemplateID string       `json:"template_id,omitempty"`
	LastActive int64        `json:"last_active,omitempty"`
}

// GroupConfig is the sandbox-group config. Secret fields are sealed at rest by
// the registry's box.
type GroupConfig struct {
	Group         string              `json:"group"` // group path (the store key)
	ProjectID     string              `json:"project_id,omitempty"`
	ManifestKey   string              `json:"manifest_key,omitempty"`  // hex; sealed on store, plain in memory
	AuthKey       string              `json:"auth_key,omitempty"`      // hex; sealed on store, plain in memory
	RegistryAuth  string              `json:"registry_auth,omitempty"` // build image-pull creds (docker config.json); sealed on store
	SandboxConfig map[string]string   `json:"sandbox_config,omitempty"`
	ImageRepo     string              `json:"image_repo,omitempty"`
	TemplateRef   string              `json:"template_ref,omitempty"`
	NodeSelectors []map[string]string `json:"node_selectors,omitempty"`
	ShuffleLabels map[string]string   `json:"shuffle_labels,omitempty"`
}

// Stores wraps the shared KV with typed, per-table accessors. The box seals group
// secrets at rest.
type Stores struct {
	kv     clusterstore.Store
	box    *secretbox.Box
	routes *clusterstate.RouteQuorum
	nodes  *clusterstate.NodeQuorum

	nodeListMu   sync.Mutex
	nodeListRev  int64
	nodeListLog  []clusterstore.Event
	nodeListSubs map[int]chan clusterstore.Event
	nodeListSeq  int

	handoffMu        sync.RWMutex
	membershipVer    int64
	routeHandoffGate map[string]clusterstate.HandoffGate
	nodeHandoffGate  map[string]clusterstate.HandoffGate
}

const nodeListHeartbeatRefreshSec = 60

// NewStores builds the typed store layer over a clusterstore (box may be nil
// only if no group secret is ever stored).
func NewStores(kv clusterstore.Store, box *secretbox.Box) *Stores {
	return &Stores{
		kv:               kv,
		box:              box,
		routes:           clusterstate.NewRouteQuorum("registry", clusterstate.NewMemoryRouteReplica()),
		nodes:            clusterstate.NewNodeQuorum("registry", clusterstate.NewMemoryNodeReplica()),
		nodeListSubs:     map[int]chan clusterstore.Event{},
		membershipVer:    1,
		routeHandoffGate: map[string]clusterstate.HandoffGate{},
		nodeHandoffGate:  map[string]clusterstate.HandoffGate{},
	}
}

func groupKey(group string) string { return groupPrefix + group }

func (s *Stores) SetMembershipVersion(version int64) {
	s.handoffMu.Lock()
	s.membershipVer = version
	s.handoffMu.Unlock()
}

func (s *Stores) SetRouteHandoffGate(group, routeKey string, gate clusterstate.HandoffGate) {
	s.handoffMu.Lock()
	s.routeHandoffGate[clusterstate.RouteKey(group, routeKey)] = gate
	s.handoffMu.Unlock()
}

func (s *Stores) SetNodeHandoffGate(nodeID string, gate clusterstate.HandoffGate) {
	s.handoffMu.Lock()
	s.nodeHandoffGate[nodeID] = gate
	s.handoffMu.Unlock()
}

func (s *Stores) checkRouteHandoff(group, routeKey string) error {
	s.handoffMu.RLock()
	version := s.membershipVer
	gate, ok := s.routeHandoffGate[clusterstate.RouteKey(group, routeKey)]
	s.handoffMu.RUnlock()
	if !ok {
		return nil
	}
	return clusterstate.DecisionError(gate.DecideWrite(version, time.Now()))
}

func (s *Stores) checkNodeHandoff(nodeID string) error {
	s.handoffMu.RLock()
	version := s.membershipVer
	gate, ok := s.nodeHandoffGate[nodeID]
	s.handoffMu.RUnlock()
	if !ok {
		return nil
	}
	return clusterstate.DecisionError(gate.DecideWrite(version, time.Now()))
}

// --- node_link ---

func (s *Stores) PutNode(ctx context.Context, n *NodeRecord) error {
	if _, err := s.putNodeLink(ctx, n); err != nil {
		return err
	}
	return s.PutNodeList(ctx, n)
}

// PutNodeRuntime updates node_link high-frequency state only. Heartbeats must not
// write node_list, otherwise every water-level tick fans out to scaler WATCH_LIST.
func (s *Stores) PutNodeRuntime(ctx context.Context, n *NodeRecord) error {
	_, err := s.putNodeLink(ctx, n)
	return err
}

func (s *Stores) GetNode(ctx context.Context, id string) (*NodeRecord, bool, error) {
	rec, found, err := s.nodes.Get(ctx, id)
	if err != nil || !found {
		return nil, found, err
	}
	n := fromClusterNode(rec)
	return &n, true, nil
}

func (s *Stores) DeleteNode(ctx context.Context, id string) error {
	if err := s.checkNodeHandoff(id); err != nil {
		return err
	}
	rec, found, err := s.nodes.Get(ctx, id)
	if err != nil {
		return err
	}
	if found {
		_, err = s.nodes.CAS(ctx, id, rec.Meta.Rev, func(clusterstate.NodeRecord, bool) (clusterstate.NodeRecord, bool, error) {
			return clusterstate.NodeRecord{}, false, nil
		})
		if err != nil {
			return err
		}
	}
	return s.publishNodeListDelete(ctx, id)
}

// RangeNodes streams every node record (read-only callback).
func (s *Stores) RangeNodes(ctx context.Context, fn func(*NodeRecord) error) error {
	return s.nodes.List(ctx, func(rec clusterstate.NodeRecord) error {
		n := fromClusterNode(rec)
		return fn(&n)
	})
}

func (s *Stores) PutNodeList(ctx context.Context, n *NodeRecord) error {
	entry := projectNodeListRecord(n)
	return s.publishNodeListPut(ctx, entry)
}

func (s *Stores) putNodeLink(ctx context.Context, n *NodeRecord) (uint64, error) {
	if err := s.checkNodeHandoff(n.NodeID); err != nil {
		return 0, err
	}
	cur, found, err := s.nodes.Get(ctx, n.NodeID)
	if err != nil {
		return 0, err
	}
	expect := uint64(0)
	if found {
		expect = cur.Meta.Rev
	}
	rec, err := s.nodes.CAS(ctx, n.NodeID, expect, func(clusterstate.NodeRecord, bool) (clusterstate.NodeRecord, bool, error) {
		return toClusterNode(n), true, nil
	})
	if err != nil {
		return 0, err
	}
	return rec.Meta.Rev, nil
}

func projectNodeListRecord(n *NodeRecord) clusterstate.NodeListEntry {
	return clusterstate.ProjectNodeList(toClusterNode(n))
}

func (s *Stores) NodeListRev(ctx context.Context) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	s.nodeListMu.Lock()
	defer s.nodeListMu.Unlock()
	return s.nodeListRev, nil
}

func (s *Stores) RangeNodeList(ctx context.Context, fn func(clusterstate.NodeListEntry) error) error {
	return s.nodes.List(ctx, func(n clusterstate.NodeRecord) error {
		return fn(clusterstate.ProjectNodeList(n))
	})
}

func (s *Stores) WatchNodeList(ctx context.Context, fromRev int64) (<-chan clusterstore.Event, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.nodeListMu.Lock()
	defer s.nodeListMu.Unlock()
	if fromRev > 0 && len(s.nodeListLog) > 0 && fromRev < s.nodeListLog[0].Rev-1 {
		return nil, clusterstore.ErrCompacted
	}
	replay := make([]clusterstore.Event, 0)
	for _, ev := range s.nodeListLog {
		if ev.Rev > fromRev {
			replay = append(replay, ev)
		}
	}
	ch := make(chan clusterstore.Event, len(replay)+1024)
	for _, ev := range replay {
		ch <- ev
	}
	id := s.nodeListSeq
	s.nodeListSeq++
	s.nodeListSubs[id] = ch
	go func() {
		<-ctx.Done()
		s.nodeListMu.Lock()
		if cur, ok := s.nodeListSubs[id]; ok {
			delete(s.nodeListSubs, id)
			close(cur)
		}
		s.nodeListMu.Unlock()
	}()
	return ch, nil
}

func (s *Stores) publishNodeListPut(ctx context.Context, entry clusterstate.NodeListEntry) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	b, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	s.publishNodeListEvent(clusterstore.Event{Type: clusterstore.EventPut, Key: entry.NodeID, Value: b})
	return nil
}

func (s *Stores) publishNodeListDelete(ctx context.Context, nodeID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.publishNodeListEvent(clusterstore.Event{Type: clusterstore.EventDelete, Key: nodeID})
	return nil
}

func (s *Stores) publishNodeListEvent(ev clusterstore.Event) {
	s.nodeListMu.Lock()
	defer s.nodeListMu.Unlock()
	s.nodeListRev++
	ev.Rev = s.nodeListRev
	s.nodeListLog = append(s.nodeListLog, ev)
	if len(s.nodeListLog) > 10000 {
		copy(s.nodeListLog, s.nodeListLog[len(s.nodeListLog)-10000:])
		s.nodeListLog = s.nodeListLog[:10000]
	}
	for id, ch := range s.nodeListSubs {
		select {
		case ch <- ev:
		default:
			delete(s.nodeListSubs, id)
			close(ch)
		}
	}
}

// --- route_link ---

// GetSandbox returns the route_link record + its revision (for CAS).
func (s *Stores) GetSandbox(ctx context.Context, group, routeKey string) (rec *SandboxRecord, rev int64, found bool, err error) {
	rr, found, err := s.routes.Get(ctx, group, routeKey)
	if err != nil || !found {
		return nil, 0, found, err
	}
	r := fromClusterRoute(rr)
	return &r, int64(rr.Meta.Rev), true, nil
}

func (s *Stores) PutSandbox(ctx context.Context, r *SandboxRecord) (int64, error) {
	_, rev, found, err := s.GetSandbox(ctx, r.Group, r.RouteKey)
	if err != nil {
		return 0, err
	}
	expect := int64(0)
	if found {
		expect = rev
	}
	rev, ok, err := s.CASSandbox(ctx, r, expect)
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, clusterstate.ErrConflict
	}
	return rev, nil
}

// CASSandbox commits r only if the record's current revision is expectRev
// (expectRev 0 = create-only). Returns (newRev, committed).
func (s *Stores) CASSandbox(ctx context.Context, r *SandboxRecord, expectRev int64) (int64, bool, error) {
	if expectRev < 0 {
		return 0, false, fmt.Errorf("registry: negative route_link revision %d", expectRev)
	}
	if err := s.checkRouteHandoff(r.Group, r.RouteKey); err != nil {
		return 0, false, err
	}
	rec, err := s.routes.CAS(ctx, r.Group, r.RouteKey, uint64(expectRev), func(clusterstate.RouteRecord, bool) (clusterstate.RouteRecord, bool, error) {
		return toClusterRoute(r), true, nil
	})
	if err == clusterstate.ErrConflict {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return int64(rec.Meta.Rev), true, nil
}

func (s *Stores) DeleteSandbox(ctx context.Context, group, routeKey string) error {
	if err := s.checkRouteHandoff(group, routeKey); err != nil {
		return err
	}
	rec, found, err := s.routes.Get(ctx, group, routeKey)
	if err != nil || !found {
		return err
	}
	_, err = s.routes.CAS(ctx, group, routeKey, rec.Meta.Rev, func(clusterstate.RouteRecord, bool) (clusterstate.RouteRecord, bool, error) {
		return clusterstate.RouteRecord{}, false, nil
	})
	return err
}

// RangeSandboxes streams a group's route_link rows.
func (s *Stores) RangeSandboxes(ctx context.Context, group string, fn func(*SandboxRecord) error) error {
	return s.routes.List(ctx, group, func(rec clusterstate.RouteRecord) error {
		r := fromClusterRoute(rec)
		return fn(&r)
	})
}

// RangeAllSandboxes streams every route_link row across groups.
func (s *Stores) RangeAllSandboxes(ctx context.Context, fn func(*SandboxRecord) error) error {
	return s.routes.List(ctx, "", func(rec clusterstate.RouteRecord) error {
		r := fromClusterRoute(rec)
		return fn(&r)
	})
}

func toClusterRoute(r *SandboxRecord) clusterstate.RouteRecord {
	if r == nil {
		return clusterstate.RouteRecord{}
	}
	return clusterstate.RouteRecord{
		Group:      r.Group,
		RouteKey:   r.RouteKey,
		SandboxID:  r.SID,
		State:      toClusterRouteState(r.State),
		NodeID:     r.NodeID,
		TemplateID: r.TemplateID,
	}
}

func fromClusterRoute(r clusterstate.RouteRecord) SandboxRecord {
	return SandboxRecord{
		Group:      r.Group,
		RouteKey:   r.RouteKey,
		SID:        r.SandboxID,
		State:      fromClusterRouteState(r.State),
		NodeID:     r.NodeID,
		TemplateID: r.TemplateID,
		LastActive: r.Meta.UpdatedAt.Unix(),
	}
}

func toClusterRouteState(st SandboxState) clusterstate.RouteState {
	switch st {
	case StateReserved:
		return clusterstate.RouteReserved
	case StateReady:
		return clusterstate.RouteReady
	case StatePaused:
		return clusterstate.RoutePaused
	default:
		return clusterstate.RouteNone
	}
}

func fromClusterRouteState(st clusterstate.RouteState) SandboxState {
	switch st {
	case clusterstate.RouteReserved:
		return StateReserved
	case clusterstate.RouteReady:
		return StateReady
	case clusterstate.RoutePaused:
		return StatePaused
	default:
		return StateNone
	}
}

func toClusterNode(n *NodeRecord) clusterstate.NodeRecord {
	if n == nil {
		return clusterstate.NodeRecord{}
	}
	st := clusterstate.NodeLive
	if n.Draining {
		st = clusterstate.NodeDrained
	}
	return clusterstate.NodeRecord{
		NodeID:            n.NodeID,
		State:             st,
		Labels:            cloneStringMap(n.Labels),
		Capacity:          n.Capacity,
		BuildCapacity:     cloneBuildResources(n.BuildCapacity),
		DataEndpoint:      n.DataEndpoint,
		RuntimeDigest:     n.RuntimeDigest,
		Zone:              n.Zone,
		Allocated:         n.Allocated,
		Pool:              n.Pool,
		BuildAlloc:        cloneBuildResources(n.BuildAlloc),
		Counts:            n.Counts,
		Draining:          n.Draining,
		LastHeartbeatUnix: n.LastHeartbeatUnix,
		ResumeToken:       n.ResumeToken,
	}
}

func fromClusterNode(n clusterstate.NodeRecord) NodeRecord {
	return NodeRecord{
		NodeID:            n.NodeID,
		Labels:            cloneStringMap(n.Labels),
		Capacity:          n.Capacity,
		BuildCapacity:     cloneBuildResources(n.BuildCapacity),
		DataEndpoint:      n.DataEndpoint,
		RuntimeDigest:     n.RuntimeDigest,
		Zone:              n.Zone,
		Allocated:         n.Allocated,
		Pool:              n.Pool,
		BuildAlloc:        cloneBuildResources(n.BuildAlloc),
		Counts:            n.Counts,
		Draining:          n.Draining || n.State == clusterstate.NodeDrained,
		LastHeartbeatUnix: n.LastHeartbeatUnix,
		ResumeToken:       n.ResumeToken,
	}
}

func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

// --- group config (secret fields sealed at rest) ---

func (s *Stores) PutGroup(ctx context.Context, g *GroupConfig) error {
	stored := *g
	if g.ManifestKey != "" || g.AuthKey != "" || g.RegistryAuth != "" {
		if s.box == nil {
			return fmt.Errorf("registry: group %q has sealed secrets but no encryption box configured", g.Group)
		}
		if g.ManifestKey != "" {
			enc, err := s.box.EncryptString(g.ManifestKey)
			if err != nil {
				return err
			}
			stored.ManifestKey = enc
		}
		if g.AuthKey != "" {
			enc, err := s.box.EncryptString(g.AuthKey)
			if err != nil {
				return err
			}
			stored.AuthKey = enc
		}
		if g.RegistryAuth != "" {
			enc, err := s.box.EncryptString(g.RegistryAuth)
			if err != nil {
				return err
			}
			stored.RegistryAuth = enc
		}
	}
	b, err := json.Marshal(&stored)
	if err != nil {
		return err
	}
	_, err = s.kv.Put(ctx, groupKey(g.Group), b)
	return err
}

// unsealGroup decrypts a group's sealed secrets in place.
func (s *Stores) unsealGroup(g *GroupConfig) error {
	if s.box == nil {
		return nil
	}
	if g.ManifestKey != "" {
		dec, err := s.box.DecryptString(g.ManifestKey)
		if err != nil {
			return fmt.Errorf("registry: decrypt group manifest_key: %w", err)
		}
		g.ManifestKey = dec
	}
	if g.AuthKey != "" {
		dec, err := s.box.DecryptString(g.AuthKey)
		if err != nil {
			return fmt.Errorf("registry: decrypt group auth_key: %w", err)
		}
		g.AuthKey = dec
	}
	if g.RegistryAuth != "" {
		dec, err := s.box.DecryptString(g.RegistryAuth)
		if err != nil {
			return fmt.Errorf("registry: decrypt group registry_auth: %w", err)
		}
		g.RegistryAuth = dec
	}
	return nil
}

// RangeGroups streams every group config (secrets decrypted) — the key
// distributor reconciles predistribution leases over these (§7.6).
func (s *Stores) RangeGroups(ctx context.Context, fn func(*GroupConfig) error) error {
	return s.kv.Range(ctx, groupPrefix, func(kv clusterstore.KV) error {
		var g GroupConfig
		if err := json.Unmarshal(kv.Value, &g); err != nil {
			return err
		}
		if err := s.unsealGroup(&g); err != nil {
			return err
		}
		return fn(&g)
	})
}

// GetGroupByID returns the group config for an exact group id (secrets decrypted),
// or (nil,false) if absent.
func (s *Stores) GetGroupByID(ctx context.Context, group string) (*GroupConfig, bool, error) {
	kv, found, err := s.kv.Get(ctx, groupKey(group))
	if err != nil || !found {
		return nil, found, err
	}
	var g GroupConfig
	if err := json.Unmarshal(kv.Value, &g); err != nil {
		return nil, false, err
	}
	if err := s.unsealGroup(&g); err != nil {
		return nil, false, err
	}
	return &g, true, nil
}
