// Package registry is the cluster control plane's route/node owner and node_link
// hub. route_link and node_link state go through the registry-owned quorum
// kernel; sandbox-group provider state lives on scaler/provider side.
package registry

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	clusterstate "github.com/kuasar-sandbox/sandbox-orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/clusterstore"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/routesync"
)

const buildRouteKeyPrefix = "__kuasar_build__/"

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
	LinkOwner         string `json:"link_owner,omitempty"`
}

// SandboxRecord is the registry-facing route_link view, keyed by
// (group, route_key).
type SandboxRecord struct {
	Group          string                    `json:"group"`
	RouteKey       string                    `json:"route_key"`
	SID            string                    `json:"sid,omitempty"`
	State          SandboxState              `json:"state"`
	NodeID         string                    `json:"node_id,omitempty"`
	SnapLoc        string                    `json:"snap_loc,omitempty"`
	TemplateID     string                    `json:"template_id,omitempty"`
	AccessToken    string                    `json:"access_token,omitempty"`
	LastActive     int64                     `json:"last_active,omitempty"`
	BuildID        string                    `json:"build_id,omitempty"`
	BuildState     BuildState                `json:"build_state,omitempty"`
	BuildResources *routesync.BuildResources `json:"build_resources,omitempty"`
	BuildReason    string                    `json:"build_reason,omitempty"`
	CreatedU       int64                     `json:"created_unix,omitempty"`
}

// Stores wraps the shared KV with typed, per-table accessors.
type Stores struct {
	kv     clusterstore.Store
	routes *clusterstate.RouteQuorum
	nodes  *clusterstate.NodeQuorum

	writerID        string
	memberViews     []clusterstate.MemberView
	routeOwnerCount int
	nodeOwnerCount  int
	localRoute      *clusterstate.MemoryRouteReplica
	localNode       *clusterstate.MemoryNodeReplica
	routeReplicas   map[string]clusterstate.RouteReplica
	nodeReplicas    map[string]clusterstate.NodeReplica
	replicaMu       sync.RWMutex

	nodeListMu                  sync.Mutex
	nodeList                    map[string]clusterstate.NodeListEntry
	nodeListRev                 int64
	nodeListLog                 []clusterstore.Event
	nodeListSubs                map[int]chan clusterstore.Event
	nodeListSeq                 int
	nodeListRetention           int
	nodeListHeartbeatRefreshSec int64
	nodeListOwnerCount          int
	nodeListReplicas            map[string]NodeListReplica
	nodeListTopologySet         bool

	handoffMu        sync.RWMutex
	membershipVer    int64
	routeHandoffGate map[string]clusterstate.HandoffGate
	nodeHandoffGate  map[string]clusterstate.HandoffGate
}

const defaultNodeListHeartbeatRefreshSec = 60

// NewStores builds the typed store layer over a clusterstore.
func NewStores(kv clusterstore.Store) *Stores {
	return NewClusterStores(kv, "registry", clusterstate.MemberView{Version: 1, Members: []string{"registry"}}, 1, 1, nil, nil)
}

func NewClusterStores(
	kv clusterstore.Store,
	writerID string,
	view clusterstate.MemberView,
	routeOwnerCount, nodeOwnerCount int,
	routeReplicas map[string]clusterstate.RouteReplica,
	nodeReplicas map[string]clusterstate.NodeReplica,
) *Stores {
	if writerID == "" {
		writerID = "registry"
	}
	if len(view.Members) == 0 {
		view = clusterstate.MemberView{Version: 1, Members: []string{writerID}}
	}
	if routeOwnerCount <= 0 {
		routeOwnerCount = 1
	}
	if nodeOwnerCount <= 0 {
		nodeOwnerCount = 1
	}
	localRoute := clusterstate.NewMemoryRouteReplica()
	localNode := clusterstate.NewMemoryNodeReplica()
	rreps := make(map[string]clusterstate.RouteReplica, len(routeReplicas)+1)
	nreps := make(map[string]clusterstate.NodeReplica, len(nodeReplicas)+1)
	for id, rep := range routeReplicas {
		if id != "" && rep != nil && id != writerID {
			rreps[id] = rep
		}
	}
	for id, rep := range nodeReplicas {
		if id != "" && rep != nil && id != writerID {
			nreps[id] = rep
		}
	}
	rreps[writerID] = localRoute
	nreps[writerID] = localNode
	return &Stores{
		kv:                          kv,
		routes:                      clusterstate.NewRouteQuorum(writerID, localRoute),
		nodes:                       clusterstate.NewNodeQuorum(writerID, localNode),
		writerID:                    writerID,
		memberViews:                 []clusterstate.MemberView{view},
		routeOwnerCount:             routeOwnerCount,
		nodeOwnerCount:              nodeOwnerCount,
		localRoute:                  localRoute,
		localNode:                   localNode,
		routeReplicas:               rreps,
		nodeReplicas:                nreps,
		nodeList:                    map[string]clusterstate.NodeListEntry{},
		nodeListSubs:                map[int]chan clusterstore.Event{},
		nodeListRetention:           10000,
		nodeListHeartbeatRefreshSec: defaultNodeListHeartbeatRefreshSec,
		nodeListOwnerCount:          1,
		nodeListReplicas:            map[string]NodeListReplica{writerID: nil},
		membershipVer:               view.Version,
		routeHandoffGate:            map[string]clusterstate.HandoffGate{},
		nodeHandoffGate:             map[string]clusterstate.HandoffGate{},
	}
}

func NewClusterStoresWithViews(
	kv clusterstore.Store,
	writerID string,
	views []clusterstate.MemberView,
	routeOwnerCount, nodeOwnerCount int,
	routeReplicas map[string]clusterstate.RouteReplica,
	nodeReplicas map[string]clusterstate.NodeReplica,
) *Stores {
	if len(views) == 0 {
		views = []clusterstate.MemberView{{Version: 1, Members: []string{writerID}}}
	}
	stores := NewClusterStores(kv, writerID, views[0], routeOwnerCount, nodeOwnerCount, routeReplicas, nodeReplicas)
	stores.SetMemberViews(views)
	return stores
}

func (s *Stores) LocalRouteReplica() clusterstate.RouteReplica { return s.localRoute }

func (s *Stores) LocalNodeReplica() clusterstate.NodeReplica { return s.localNode }

func (s *Stores) WriterID() string { return s.writerID }

func (s *Stores) SetNodeListHeartbeatRefresh(d time.Duration) {
	sec := int64(d.Seconds())
	if sec < 1 {
		sec = 1
	}
	s.nodeListMu.Lock()
	s.nodeListHeartbeatRefreshSec = sec
	s.nodeListMu.Unlock()
}

func (s *Stores) SetNodeListWatchRetention(n int) {
	if n <= 0 {
		n = 10000
	}
	s.nodeListMu.Lock()
	s.nodeListRetention = n
	if len(s.nodeListLog) > n {
		copy(s.nodeListLog, s.nodeListLog[len(s.nodeListLog)-n:])
		s.nodeListLog = s.nodeListLog[:n]
	}
	s.nodeListMu.Unlock()
}

func (s *Stores) NodeListHeartbeatRefreshSec() int64 {
	s.nodeListMu.Lock()
	defer s.nodeListMu.Unlock()
	if s.nodeListHeartbeatRefreshSec <= 0 {
		return defaultNodeListHeartbeatRefreshSec
	}
	return s.nodeListHeartbeatRefreshSec
}

func (s *Stores) SetMembershipVersion(version int64) {
	s.handoffMu.Lock()
	s.membershipVer = version
	s.handoffMu.Unlock()
}

func (s *Stores) SetMemberViews(views []clusterstate.MemberView) {
	if len(views) == 0 {
		return
	}
	clean := make([]clusterstate.MemberView, 0, len(views))
	seen := map[int64]bool{}
	for _, view := range views {
		if len(view.Members) == 0 || seen[view.Version] {
			continue
		}
		seen[view.Version] = true
		clean = append(clean, view)
	}
	if len(clean) == 0 {
		return
	}
	s.replicaMu.Lock()
	s.memberViews = clean
	s.replicaMu.Unlock()
}

func (s *Stores) SetClusterTopology(
	views []clusterstate.MemberView,
	routeOwnerCount, nodeOwnerCount int,
	routeReplicas map[string]clusterstate.RouteReplica,
	nodeReplicas map[string]clusterstate.NodeReplica,
) {
	if len(views) == 0 {
		return
	}
	cleanViews := make([]clusterstate.MemberView, 0, len(views))
	seenViews := map[int64]bool{}
	for _, view := range views {
		if len(view.Members) == 0 || seenViews[view.Version] {
			continue
		}
		seenViews[view.Version] = true
		cleanViews = append(cleanViews, view)
	}
	if len(cleanViews) == 0 {
		return
	}
	if routeOwnerCount <= 0 {
		routeOwnerCount = 1
	}
	if nodeOwnerCount <= 0 {
		nodeOwnerCount = 1
	}
	rreps := make(map[string]clusterstate.RouteReplica, len(routeReplicas)+1)
	nreps := make(map[string]clusterstate.NodeReplica, len(nodeReplicas)+1)
	for id, rep := range routeReplicas {
		if id != "" && id != s.writerID && rep != nil {
			rreps[id] = rep
		}
	}
	for id, rep := range nodeReplicas {
		if id != "" && id != s.writerID && rep != nil {
			nreps[id] = rep
		}
	}
	rreps[s.writerID] = s.localRoute
	nreps[s.writerID] = s.localNode
	s.replicaMu.Lock()
	s.memberViews = cleanViews
	s.routeOwnerCount = routeOwnerCount
	s.nodeOwnerCount = nodeOwnerCount
	s.routeReplicas = rreps
	s.nodeReplicas = nreps
	s.replicaMu.Unlock()
}

func (s *Stores) SetNodeListTopology(views []clusterstate.MemberView, ownerCount int, replicas map[string]NodeListReplica) {
	cleanViews := make([]clusterstate.MemberView, 0, len(views))
	seenVersion := map[int64]bool{}
	for _, view := range views {
		if len(view.Members) == 0 || seenVersion[view.Version] {
			continue
		}
		seenVersion[view.Version] = true
		cleanViews = append(cleanViews, view)
	}
	if len(cleanViews) == 0 {
		return
	}
	if ownerCount <= 0 {
		ownerCount = 1
	}
	reps := make(map[string]NodeListReplica, len(replicas)+1)
	for id, rep := range replicas {
		if id != "" && id != s.writerID && rep != nil {
			reps[id] = rep
		}
	}
	reps[s.writerID] = s
	s.replicaMu.Lock()
	s.memberViews = cleanViews
	s.nodeListOwnerCount = ownerCount
	s.nodeListReplicas = reps
	s.nodeListTopologySet = true
	s.replicaMu.Unlock()
}

func (s *Stores) routeQuorum(group string) (*clusterstate.RouteQuorum, error) {
	s.replicaMu.RLock()
	defer s.replicaMu.RUnlock()
	ownerSets, ownerUnion, err := locatedOwnerSets(s.memberViews, group, s.routeOwnerCount)
	if err != nil {
		return nil, err
	}
	reps := make([]clusterstate.RouteReplicaSlot, 0, len(ownerUnion))
	for _, id := range ownerUnion {
		rep := s.routeReplicas[id]
		if rep == nil {
			rep = unavailableRouteReplica{id: id}
		}
		reps = append(reps, clusterstate.RouteReplicaSlot{ID: id, Replica: rep})
	}
	return clusterstate.NewRouteJointQuorum(s.writerID, reps, ownerSets), nil
}

func (s *Stores) nodeQuorum(nodeID string) (*clusterstate.NodeQuorum, error) {
	s.replicaMu.RLock()
	defer s.replicaMu.RUnlock()
	ownerSets, ownerUnion, err := locatedOwnerSets(s.memberViews, nodeID, s.nodeOwnerCount)
	if err != nil {
		return nil, err
	}
	reps := make([]clusterstate.NodeReplicaSlot, 0, len(ownerUnion))
	for _, id := range ownerUnion {
		rep := s.nodeReplicas[id]
		if rep == nil {
			rep = unavailableNodeReplica{id: id}
		}
		reps = append(reps, clusterstate.NodeReplicaSlot{ID: id, Replica: rep})
	}
	return clusterstate.NewNodeJointQuorum(s.writerID, reps, ownerSets), nil
}

func (s *Stores) nodeListOwners() ([][]string, []string, error) {
	s.replicaMu.RLock()
	if !s.nodeListTopologySet {
		writer := s.writerID
		s.replicaMu.RUnlock()
		return [][]string{{writer}}, []string{writer}, nil
	}
	defer s.replicaMu.RUnlock()
	return locatedOwnerSets(s.memberViews, clusterstate.NamespaceNodeList, s.nodeListOwnerCount)
}

func (s *Stores) nodeListReplica(id string) NodeListReplica {
	if id == s.writerID {
		return s
	}
	s.replicaMu.RLock()
	rep := s.nodeListReplicas[id]
	s.replicaMu.RUnlock()
	return rep
}

func locatedOwnerSets(views []clusterstate.MemberView, key string, ownerCount int) ([][]string, []string, error) {
	sets := make([][]string, 0, len(views))
	seen := map[string]bool{}
	var union []string
	for _, view := range views {
		owners, err := view.Owners(key, ownerCount)
		if err != nil {
			return nil, nil, err
		}
		if len(owners) == 0 {
			continue
		}
		sets = append(sets, owners)
		for _, id := range owners {
			if seen[id] {
				continue
			}
			seen[id] = true
			union = append(union, id)
		}
	}
	sort.Strings(union)
	return sets, union, nil
}

func (s *Stores) routeKeys(ctx context.Context) []string {
	s.replicaMu.RLock()
	reps := make([]clusterstate.RouteReplica, 0, len(s.routeReplicas))
	for _, rep := range s.routeReplicas {
		reps = append(reps, rep)
	}
	s.replicaMu.RUnlock()
	seen := map[string]bool{}
	for _, rep := range reps {
		for _, key := range rep.Keys(ctx) {
			seen[key] = true
		}
	}
	keys := make([]string, 0, len(seen))
	for key := range seen {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func (s *Stores) nodeKeys(ctx context.Context) []string {
	s.replicaMu.RLock()
	reps := make([]clusterstate.NodeReplica, 0, len(s.nodeReplicas))
	for _, rep := range s.nodeReplicas {
		reps = append(reps, rep)
	}
	s.replicaMu.RUnlock()
	seen := map[string]bool{}
	for _, rep := range reps {
		for _, key := range rep.Keys(ctx) {
			seen[key] = true
		}
	}
	keys := make([]string, 0, len(seen))
	for key := range seen {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// CatchUp promotes records visible in any configured owner replica back through
// quorum reads. A restarted registry member starts with empty in-memory local
// replicas; this scan lets the quorum protocol read-repair route_link/node_link
// records into the local replica without a separate migration step.
func (s *Stores) CatchUp(ctx context.Context) error {
	for _, key := range s.routeKeys(ctx) {
		group, routeKey := splitRouteStorageKey(key)
		if _, _, _, err := s.GetSandbox(ctx, group, routeKey); err != nil {
			return err
		}
	}
	for _, key := range s.nodeKeys(ctx) {
		if _, _, err := s.GetNode(ctx, key); err != nil {
			return err
		}
	}
	return nil
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
	q, err := s.nodeQuorum(id)
	if err != nil {
		return nil, false, err
	}
	rec, found, err := q.Get(ctx, id)
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
	q, err := s.nodeQuorum(id)
	if err != nil {
		return err
	}
	rec, found, err := q.Get(ctx, id)
	if err != nil {
		return err
	}
	if found {
		_, err = q.CAS(ctx, id, rec.Meta.Rev, func(clusterstate.NodeRecord, bool) (clusterstate.NodeRecord, bool, error) {
			return clusterstate.NodeRecord{}, false, nil
		})
		if err != nil {
			return err
		}
	}
	return s.DeleteNodeList(ctx, id)
}

// RangeNodes streams every node record (read-only callback).
func (s *Stores) RangeNodes(ctx context.Context, fn func(*NodeRecord) error) error {
	for _, key := range s.nodeKeys(ctx) {
		rec, found, err := s.GetNode(ctx, key)
		if err != nil {
			return err
		}
		if !found {
			continue
		}
		if err := fn(rec); err != nil {
			return err
		}
	}
	return nil
}

func (s *Stores) PutNodeList(ctx context.Context, n *NodeRecord) error {
	entry := projectNodeListRecord(n)
	return s.PutNodeListEntry(ctx, entry)
}

func (s *Stores) putNodeLink(ctx context.Context, n *NodeRecord) (uint64, error) {
	if err := s.checkNodeHandoff(n.NodeID); err != nil {
		return 0, err
	}
	q, err := s.nodeQuorum(n.NodeID)
	if err != nil {
		return 0, err
	}
	cur, found, err := q.Get(ctx, n.NodeID)
	if err != nil {
		return 0, err
	}
	expect := uint64(0)
	if found {
		expect = cur.Meta.Rev
	}
	rec, err := q.CAS(ctx, n.NodeID, expect, func(clusterstate.NodeRecord, bool) (clusterstate.NodeRecord, bool, error) {
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

func nodeListEntryNewer(next, cur clusterstate.NodeListEntry) bool {
	return next.LastHeartbeatUnix >= cur.LastHeartbeatUnix
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
	if err := ctx.Err(); err != nil {
		return err
	}
	s.nodeListMu.Lock()
	out := make([]clusterstate.NodeListEntry, 0, len(s.nodeList))
	for _, entry := range s.nodeList {
		out = append(out, entry)
	}
	s.nodeListMu.Unlock()
	sort.Slice(out, func(i, j int) bool { return out[i].NodeID < out[j].NodeID })
	for _, entry := range out {
		if err := fn(entry); err != nil {
			return err
		}
	}
	return nil
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

func (s *Stores) PutNodeListEntry(ctx context.Context, entry clusterstate.NodeListEntry) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if entry.NodeID == "" {
		return fmt.Errorf("registry: node_list node_id is required")
	}
	return s.replicateNodeList(ctx, func(rep NodeListReplica) error {
		return rep.ApplyNodeListPut(ctx, entry)
	})
}

func (s *Stores) DeleteNodeList(ctx context.Context, nodeID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if nodeID == "" {
		return nil
	}
	return s.replicateNodeList(ctx, func(rep NodeListReplica) error {
		return rep.ApplyNodeListDelete(ctx, nodeID)
	})
}

func (s *Stores) replicateNodeList(ctx context.Context, apply func(NodeListReplica) error) error {
	ownerSets, ownerUnion, err := s.nodeListOwners()
	if err != nil {
		return err
	}
	accepted := map[string]bool{}
	var last error
	for _, id := range ownerUnion {
		rep := s.nodeListReplica(id)
		if rep == nil {
			last = fmt.Errorf("registry: node_list replica %q unavailable", id)
			continue
		}
		if err := apply(rep); err != nil {
			last = err
			continue
		}
		accepted[id] = true
	}
	if !satisfiesOwnerQuorums(accepted, ownerSets) {
		if last != nil {
			return last
		}
		return clusterstate.ErrQuorum
	}
	return nil
}

func satisfiesOwnerQuorums(accepted map[string]bool, ownerSets [][]string) bool {
	for _, owners := range ownerSets {
		need := len(owners)/2 + 1
		got := 0
		for _, id := range owners {
			if accepted[id] {
				got++
			}
		}
		if got < need {
			return false
		}
	}
	return true
}

func (s *Stores) ApplyNodeListPut(ctx context.Context, entry clusterstate.NodeListEntry) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if entry.NodeID == "" {
		return fmt.Errorf("registry: node_list node_id is required")
	}
	b, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	return s.publishNodeListEvent(clusterstore.Event{Type: clusterstore.EventPut, Key: entry.NodeID, Value: b}, &entry)
}

func (s *Stores) ApplyNodeListDelete(ctx context.Context, nodeID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if nodeID == "" {
		return nil
	}
	return s.publishNodeListEvent(clusterstore.Event{Type: clusterstore.EventDelete, Key: nodeID}, nil)
}

func (s *Stores) publishNodeListEvent(ev clusterstore.Event, entry *clusterstate.NodeListEntry) error {
	s.nodeListMu.Lock()
	defer s.nodeListMu.Unlock()
	switch ev.Type {
	case clusterstore.EventPut:
		if entry == nil {
			return fmt.Errorf("registry: node_list put missing entry")
		}
		if cur, ok := s.nodeList[entry.NodeID]; ok && !nodeListEntryNewer(*entry, cur) {
			return nil
		}
		s.nodeList[entry.NodeID] = *entry
	case clusterstore.EventDelete:
		delete(s.nodeList, ev.Key)
	}
	s.nodeListRev++
	ev.Rev = s.nodeListRev
	s.nodeListLog = append(s.nodeListLog, ev)
	retention := s.nodeListRetention
	if retention <= 0 {
		retention = 10000
	}
	if len(s.nodeListLog) > retention {
		copy(s.nodeListLog, s.nodeListLog[len(s.nodeListLog)-retention:])
		s.nodeListLog = s.nodeListLog[:retention]
	}
	for id, ch := range s.nodeListSubs {
		select {
		case ch <- ev:
		default:
			delete(s.nodeListSubs, id)
			close(ch)
		}
	}
	return nil
}

// --- route_link ---

// GetSandbox returns the route_link record + its revision (for CAS).
func (s *Stores) GetSandbox(ctx context.Context, group, routeKey string) (rec *SandboxRecord, rev int64, found bool, err error) {
	q, err := s.routeQuorum(group)
	if err != nil {
		return nil, 0, false, err
	}
	rr, found, err := q.Get(ctx, group, routeKey)
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
	q, err := s.routeQuorum(r.Group)
	if err != nil {
		return 0, false, err
	}
	rec, err := q.CAS(ctx, r.Group, r.RouteKey, uint64(expectRev), func(clusterstate.RouteRecord, bool) (clusterstate.RouteRecord, bool, error) {
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
	q, err := s.routeQuorum(group)
	if err != nil {
		return err
	}
	rec, found, err := q.Get(ctx, group, routeKey)
	if err != nil || !found {
		return err
	}
	_, err = q.CAS(ctx, group, routeKey, rec.Meta.Rev, func(clusterstate.RouteRecord, bool) (clusterstate.RouteRecord, bool, error) {
		return clusterstate.RouteRecord{}, false, nil
	})
	return err
}

// RangeSandboxes streams a group's route_link rows.
func (s *Stores) RangeSandboxes(ctx context.Context, group string, fn func(*SandboxRecord) error) error {
	return s.rangeSandboxes(ctx, group, fn)
}

// RangeAllSandboxes streams every route_link row across groups.
func (s *Stores) RangeAllSandboxes(ctx context.Context, fn func(*SandboxRecord) error) error {
	return s.rangeSandboxes(ctx, "", fn)
}

func (s *Stores) rangeSandboxes(ctx context.Context, group string, fn func(*SandboxRecord) error) error {
	for _, key := range s.routeKeys(ctx) {
		g, rk := splitRouteStorageKey(key)
		if group != "" && g != group {
			continue
		}
		if isBuildRouteKey(rk) {
			continue
		}
		rec, _, found, err := s.GetSandbox(ctx, g, rk)
		if err != nil {
			return err
		}
		if !found {
			continue
		}
		if err := fn(rec); err != nil {
			return err
		}
	}
	return nil
}

func buildRouteKey(buildID string) string { return buildRouteKeyPrefix + buildID }

func buildIDFromRouteKey(routeKey string) string {
	return strings.TrimPrefix(routeKey, buildRouteKeyPrefix)
}

func isBuildRouteKey(routeKey string) bool {
	return strings.HasPrefix(routeKey, buildRouteKeyPrefix)
}

func splitRouteStorageKey(key string) (string, string) {
	for i := 0; i < len(key); i++ {
		if key[i] == 0 {
			return key[:i], key[i+1:]
		}
	}
	return "", key
}

func toClusterRoute(r *SandboxRecord) clusterstate.RouteRecord {
	if r == nil {
		return clusterstate.RouteRecord{}
	}
	return clusterstate.RouteRecord{
		Group:          r.Group,
		RouteKey:       r.RouteKey,
		SandboxID:      r.SID,
		State:          toClusterRouteState(r.State),
		NodeID:         r.NodeID,
		TemplateID:     r.TemplateID,
		AccessToken:    r.AccessToken,
		BuildID:        r.BuildID,
		BuildState:     string(r.BuildState),
		BuildResources: cloneBuildResources(r.BuildResources),
		BuildReason:    r.BuildReason,
		CreatedUnix:    r.CreatedU,
	}
}

func fromClusterRoute(r clusterstate.RouteRecord) SandboxRecord {
	return SandboxRecord{
		Group:          r.Group,
		RouteKey:       r.RouteKey,
		SID:            r.SandboxID,
		State:          fromClusterRouteState(r.State),
		NodeID:         r.NodeID,
		TemplateID:     r.TemplateID,
		AccessToken:    r.AccessToken,
		LastActive:     r.Meta.UpdatedAt.Unix(),
		BuildID:        r.BuildID,
		BuildState:     BuildState(r.BuildState),
		BuildResources: cloneBuildResources(r.BuildResources),
		BuildReason:    r.BuildReason,
		CreatedU:       r.CreatedUnix,
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
		LinkOwner:         n.LinkOwner,
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
		LinkOwner:         n.LinkOwner,
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

type unavailableRouteReplica struct{ id string }

func (r unavailableRouteReplica) err() error {
	return fmt.Errorf("registry: route replica %q unavailable", r.id)
}

func (r unavailableRouteReplica) Read(context.Context, string) (clusterstate.RouteRecord, bool, error) {
	return clusterstate.RouteRecord{}, false, r.err()
}

func (r unavailableRouteReplica) Prepare(context.Context, string, clusterstate.Ballot) (clusterstate.RouteRecord, bool, bool, error) {
	return clusterstate.RouteRecord{}, false, false, r.err()
}

func (r unavailableRouteReplica) Accept(context.Context, string, clusterstate.RouteRecord, clusterstate.Ballot) (bool, error) {
	return false, r.err()
}

func (r unavailableRouteReplica) Repair(context.Context, string, clusterstate.RouteRecord) error {
	return r.err()
}

func (r unavailableRouteReplica) MaxBallot(context.Context, string) (clusterstate.Ballot, error) {
	return clusterstate.Ballot{}, r.err()
}

func (r unavailableRouteReplica) Keys(context.Context) []string { return nil }

type unavailableNodeReplica struct{ id string }

func (r unavailableNodeReplica) err() error {
	return fmt.Errorf("registry: node replica %q unavailable", r.id)
}

func (r unavailableNodeReplica) Read(context.Context, string) (clusterstate.NodeRecord, bool, error) {
	return clusterstate.NodeRecord{}, false, r.err()
}

func (r unavailableNodeReplica) Prepare(context.Context, string, clusterstate.Ballot) (clusterstate.NodeRecord, bool, bool, error) {
	return clusterstate.NodeRecord{}, false, false, r.err()
}

func (r unavailableNodeReplica) Accept(context.Context, string, clusterstate.NodeRecord, clusterstate.Ballot) (bool, error) {
	return false, r.err()
}

func (r unavailableNodeReplica) Repair(context.Context, string, clusterstate.NodeRecord) error {
	return r.err()
}

func (r unavailableNodeReplica) MaxBallot(context.Context, string) (clusterstate.Ballot, error) {
	return clusterstate.Ballot{}, r.err()
}

func (r unavailableNodeReplica) Keys(context.Context) []string { return nil }
