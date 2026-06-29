package cluster

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
	"time"
)

var (
	ErrConflict = errors.New("cluster: record conflict")
	ErrStale    = errors.New("cluster: stale ballot")
)

type RouteLink interface {
	GetRoute(ctx context.Context, group, routeKey string) (RouteRecord, bool, error)
	PutRoute(ctx context.Context, rec RouteRecord, expectRev uint64, ballot Ballot) (RouteRecord, error)
	DeleteRoute(ctx context.Context, group, routeKey string, expectRev uint64, ballot Ballot) error
	ListRoutes(ctx context.Context, group string, fn func(RouteRecord) error) error
}

type NodeLink interface {
	GetNode(ctx context.Context, nodeID string) (NodeRecord, bool, error)
	PutNode(ctx context.Context, rec NodeRecord, expectRev uint64, ballot Ballot) (NodeRecord, error)
	DeleteNode(ctx context.Context, nodeID string, expectRev uint64, ballot Ballot) error
}

type NodeList interface {
	GetNodeList(ctx context.Context, nodeID string) (NodeListEntry, bool, error)
	ListNodeList(ctx context.Context, fn func(NodeListEntry) error) error
}

// MemoryKernel is the size-1 implementation of route_link/node_link/node_list.
// It is deliberately shaped like the replicated kernel: all writes carry an
// expected rev plus a unique ballot, and reads return complete owner-local views.
type MemoryKernel struct {
	mu     sync.RWMutex
	writer string
	now    func() time.Time

	routes   map[string]RouteRecord
	nodes    map[string]NodeRecord
	nodeList map[string]NodeListEntry
}

func NewMemoryKernel(writer string) *MemoryKernel {
	if writer == "" {
		writer = "local"
	}
	return &MemoryKernel{
		writer: writer, now: time.Now,
		routes: map[string]RouteRecord{}, nodes: map[string]NodeRecord{}, nodeList: map[string]NodeListEntry{},
	}
}

func (m *MemoryKernel) NextBallot(current Ballot) Ballot {
	return Ballot{Round: current.Round + 1, Writer: m.writer}
}

func (m *MemoryKernel) GetRoute(ctx context.Context, group, routeKey string) (RouteRecord, bool, error) {
	if err := ctx.Err(); err != nil {
		return RouteRecord{}, false, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	rec, ok := m.routes[RouteKey(group, routeKey)]
	return cloneRoute(rec), ok, nil
}

func (m *MemoryKernel) PutRoute(ctx context.Context, rec RouteRecord, expectRev uint64, ballot Ballot) (RouteRecord, error) {
	if err := ctx.Err(); err != nil {
		return RouteRecord{}, err
	}
	if rec.Group == "" || rec.RouteKey == "" {
		return RouteRecord{}, errors.New("cluster: route group and route_key are required")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	key := RouteKey(rec.Group, rec.RouteKey)
	cur, found := m.routes[key]
	if !matchRev(found, cur.Meta.Rev, expectRev) {
		return RouteRecord{}, ErrConflict
	}
	if found && ballot.Less(cur.Meta.Ballot) {
		return RouteRecord{}, ErrStale
	}
	rec.Meta = nextMeta(cur.Meta, found, ballot, m.now())
	rec.Config = cloneStringMap(rec.Config)
	m.routes[key] = rec
	return cloneRoute(rec), nil
}

func (m *MemoryKernel) DeleteRoute(ctx context.Context, group, routeKey string, expectRev uint64, ballot Ballot) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	key := RouteKey(group, routeKey)
	cur, found := m.routes[key]
	if !matchRev(found, cur.Meta.Rev, expectRev) {
		return ErrConflict
	}
	if found && ballot.Less(cur.Meta.Ballot) {
		return ErrStale
	}
	delete(m.routes, key)
	return nil
}

func (m *MemoryKernel) ListRoutes(ctx context.Context, group string, fn func(RouteRecord) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	prefix := group + "\x00"
	m.mu.RLock()
	out := make([]RouteRecord, 0)
	for key, rec := range m.routes {
		if strings.HasPrefix(key, prefix) {
			out = append(out, cloneRoute(rec))
		}
	}
	m.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i].RouteKey < out[j].RouteKey })
	for _, rec := range out {
		if err := fn(rec); err != nil {
			return err
		}
	}
	return nil
}

func (m *MemoryKernel) GetNode(ctx context.Context, nodeID string) (NodeRecord, bool, error) {
	if err := ctx.Err(); err != nil {
		return NodeRecord{}, false, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	rec, ok := m.nodes[nodeID]
	return cloneNode(rec), ok, nil
}

func (m *MemoryKernel) PutNode(ctx context.Context, rec NodeRecord, expectRev uint64, ballot Ballot) (NodeRecord, error) {
	if err := ctx.Err(); err != nil {
		return NodeRecord{}, err
	}
	if rec.NodeID == "" {
		return NodeRecord{}, errors.New("cluster: node_id is required")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	cur, found := m.nodes[rec.NodeID]
	if !matchRev(found, cur.Meta.Rev, expectRev) {
		return NodeRecord{}, ErrConflict
	}
	if found && ballot.Less(cur.Meta.Ballot) {
		return NodeRecord{}, ErrStale
	}
	rec.Meta = nextMeta(cur.Meta, found, ballot, m.now())
	rec.Labels = cloneStringMap(rec.Labels)
	m.nodes[rec.NodeID] = rec
	m.nodeList[rec.NodeID] = ProjectNodeList(rec)
	return cloneNode(rec), nil
}

func (m *MemoryKernel) DeleteNode(ctx context.Context, nodeID string, expectRev uint64, ballot Ballot) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	cur, found := m.nodes[nodeID]
	if !matchRev(found, cur.Meta.Rev, expectRev) {
		return ErrConflict
	}
	if found && ballot.Less(cur.Meta.Ballot) {
		return ErrStale
	}
	delete(m.nodes, nodeID)
	delete(m.nodeList, nodeID)
	return nil
}

func (m *MemoryKernel) GetNodeList(ctx context.Context, nodeID string) (NodeListEntry, bool, error) {
	if err := ctx.Err(); err != nil {
		return NodeListEntry{}, false, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	rec, ok := m.nodeList[nodeID]
	return cloneNodeList(rec), ok, nil
}

func (m *MemoryKernel) ListNodeList(ctx context.Context, fn func(NodeListEntry) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.RLock()
	out := make([]NodeListEntry, 0, len(m.nodeList))
	for _, rec := range m.nodeList {
		out = append(out, cloneNodeList(rec))
	}
	m.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i].NodeID < out[j].NodeID })
	for _, rec := range out {
		if err := fn(rec); err != nil {
			return err
		}
	}
	return nil
}

func matchRev(found bool, rev, expect uint64) bool {
	if !found {
		return expect == 0
	}
	return rev == expect
}

func nextMeta(cur RecordMeta, found bool, ballot Ballot, now time.Time) RecordMeta {
	if ballot.IsZero() {
		ballot = Ballot{Round: cur.Ballot.Round + 1, Writer: "local"}
	}
	if found && !cur.Ballot.Less(ballot) && cur.Ballot != ballot {
		ballot = Ballot{Round: cur.Ballot.Round + 1, Writer: ballot.Writer}
	}
	return RecordMeta{Ballot: ballot, Rev: cur.Rev + 1, UpdatedAt: now}
}

func cloneRoute(in RouteRecord) RouteRecord {
	in.Config = cloneStringMap(in.Config)
	return in
}

func cloneNode(in NodeRecord) NodeRecord {
	in.Labels = cloneStringMap(in.Labels)
	in.BuildCapacity = cloneBuildResources(in.BuildCapacity)
	in.BuildAlloc = cloneBuildResources(in.BuildAlloc)
	in.ManifestKeys = cloneNodeManifestKeys(in.ManifestKeys)
	in.Sandboxes = cloneNodeSandboxRefs(in.Sandboxes)
	in.Builds = cloneNodeBuildRefs(in.Builds)
	return in
}

func cloneNodeList(in NodeListEntry) NodeListEntry {
	in.Labels = cloneStringMap(in.Labels)
	in.BuildCapacity = cloneBuildResources(in.BuildCapacity)
	return in
}

func cloneNodeManifestKeys(in []NodeManifestKey) []NodeManifestKey {
	if len(in) == 0 {
		return nil
	}
	out := make([]NodeManifestKey, len(in))
	copy(out, in)
	return out
}

func cloneNodeSandboxRefs(in []NodeSandboxRef) []NodeSandboxRef {
	if len(in) == 0 {
		return nil
	}
	out := make([]NodeSandboxRef, len(in))
	copy(out, in)
	return out
}

func cloneNodeBuildRefs(in []NodeBuildRef) []NodeBuildRef {
	if len(in) == 0 {
		return nil
	}
	out := make([]NodeBuildRef, len(in))
	copy(out, in)
	return out
}
