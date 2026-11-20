// Package metadatastore provides an in-process typed fixture for consumer tests.
// Production binaries must construct the native driver; this fixture deliberately
// has no configuration, fallback, transport, persistence or general KV API.
package metadatastore

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/cluster/metadata"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

type Owners struct {
	ID      string
	Members []string
	Count   int
}

func (o Owners) LocalID() string {
	if o.ID == "" {
		return "registry"
	}
	return o.ID
}
func (o Owners) Candidates(ctx context.Context, id string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(o.Members) == 0 {
		return []string{o.LocalID()}, nil
	}
	n := o.Count
	if n <= 0 || n > len(o.Members) {
		n = len(o.Members)
	}
	sum := sha256.Sum256([]byte(id))
	start := int(binary.BigEndian.Uint32(sum[:4])) % len(o.Members)
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, o.Members[(start+i)%len(o.Members)])
	}
	return out, nil
}

// Version constructs a complete deterministic stamp only for test expectations.
func Version(n uint64) metadata.Version {
	var v metadata.Version
	binary.BigEndian.PutUint64(v[:8], n)
	return v
}

type Store struct {
	mu                             sync.Mutex
	seq                            uint64
	routes                         map[string]metadata.Versioned[metadata.SandboxRecord]
	builds                         map[string]metadata.Versioned[metadata.BuildRecord]
	nodes                          map[string]metadata.NodeSnapshot
	sandboxes                      map[string]metadata.Versioned[metadata.NodeSandboxRef]
	owners                         map[string]metadata.Versioned[metadata.NodeBuildRef]
	keys                           map[string]metadata.Versioned[metadata.NodeKeyPair]
	imports                        map[string]metadata.PlacerImportSourceState
	directory                      map[string]metadata.NodeListEntry
	token                          metadata.DirectoryToken
	routeWatches                   map[string]map[*invalidation]struct{}
	directoryWatches               map[*invalidation]struct{}
	observationLimit, reservations int
}

func New() *Store {
	return &Store{routes: map[string]metadata.Versioned[metadata.SandboxRecord]{}, builds: map[string]metadata.Versioned[metadata.BuildRecord]{}, nodes: map[string]metadata.NodeSnapshot{}, sandboxes: map[string]metadata.Versioned[metadata.NodeSandboxRef]{}, owners: map[string]metadata.Versioned[metadata.NodeBuildRef]{}, keys: map[string]metadata.Versioned[metadata.NodeKeyPair]{}, imports: map[string]metadata.PlacerImportSourceState{}, directory: map[string]metadata.NodeListEntry{}, routeWatches: map[string]map[*invalidation]struct{}{}, directoryWatches: map[*invalidation]struct{}{}, observationLimit: 4096}
}
func (s *Store) Repositories() metadata.Repositories {
	return metadata.Repositories{Routes: s, Builds: s, Nodes: s, Ownership: s, KeyPairs: s, Directory: s, Imports: s, Observations: s}
}
func (s *Store) SetObservationLimit(limit int) {
	s.mu.Lock()
	s.observationLimit = limit
	s.mu.Unlock()
}
func (s *Store) ObservationReservations() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reservations
}
func (s *Store) next() metadata.Version { s.seq++; return Version(s.seq) }
func clone[T any](v T) T {
	raw, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	var out T
	if err := json.Unmarshal(raw, &out); err != nil {
		panic(err)
	}
	return out
}
func routeKey(k metadata.RouteKey) string { return k.Group + "\x00" + k.RouteKey }
func buildKey(k metadata.BuildKey) string { return k.Group + "\x00" + k.BuildID }
func childKey(node, id string) string     { return node + "\x00" + id }
func (s *Store) live(node string) bool    { n := s.nodes[node]; return n.Exists && !n.Reaping }
func (s *Store) guardRegistration(reg metadata.Registration) error {
	n := s.nodes[reg.NodeID]
	if reg.NodeID == "" || reg.LinkOwner == "" || !reg.Fence.IsCommitted() || !n.Exists || n.Reaping || n.Registration != reg {
		return metadata.ErrConflict
	}
	return nil
}
func (s *Store) putRoute(k string, r metadata.SandboxRecord, v metadata.Version) {
	s.routes[k] = metadata.Versioned[metadata.SandboxRecord]{Exists: true, Version: v, Value: clone(r)}
	s.notifyRoute(k)
}
func (s *Store) putBuild(k string, b metadata.BuildRecord, v metadata.Version) {
	s.builds[k] = metadata.Versioned[metadata.BuildRecord]{Exists: true, Version: v, Value: clone(b)}
}
func (s *Store) notifyRoute(k string) {
	for w := range s.routeWatches[k] {
		w.fire(nil)
	}
}
func (s *Store) updateDirectory(n metadata.NodeSnapshot, v metadata.Version) {
	old, found := s.directory[n.Value.NodeID]
	if !n.Exists || n.Reaping {
		if !found {
			return
		}
		delete(s.directory, n.Value.NodeID)
	} else {
		p := n.Value
		entry := metadata.NodeListEntry{Version: v, NodeID: p.NodeID, Labels: clone(p.Labels), Capacity: p.Capacity, BuildRegistrationCapacity: clone(p.BuildRegistrationCapacity), BuildExecutionCapacity: clone(p.BuildExecutionCapacity), APIEndpoint: p.APIEndpoint, DataEndpoint: p.DataEndpoint, RuntimeDigest: p.RuntimeDigest, Draining: p.Draining}
		comparable := entry
		comparable.Version = old.Version
		if found && reflect.DeepEqual(old, comparable) {
			return
		}
		s.directory[p.NodeID] = entry
	}
	bucket := sha256.Sum256([]byte(n.Value.NodeID))
	s.token[bucket[0]] = v
	for w := range s.directoryWatches {
		w.fire(nil)
	}
}
func page[T any](m map[string]T, prefix string, after metadata.Cursor, limit int) metadata.Page[T] {
	if limit <= 0 {
		limit = 128
	}
	keys := make([]string, 0)
	for k := range m {
		if strings.HasPrefix(k, prefix) && k > string(after) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	out := metadata.Page[T]{}
	if len(keys) > limit {
		out.Next = metadata.Cursor(keys[limit-1])
		keys = keys[:limit]
	}
	for _, k := range keys {
		out.Items = append(out.Items, clone(m[k]))
	}
	return out
}
func (s *Store) ReadRoute(ctx context.Context, k metadata.RouteKey) (metadata.Versioned[metadata.SandboxRecord], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return clone(s.routes[routeKey(k)]), ctx.Err()
}
func (s *Store) ReadResolution(ctx context.Context, k metadata.RouteKey) (metadata.Resolution, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.routes[routeKey(k)]
	n := s.nodes[r.Value.NodeID]
	return metadata.Resolution{Route: clone(r), Node: metadata.Versioned[metadata.NodeProfileRecord]{Exists: n.Exists, Version: n.Version, Value: clone(n.Value)}}, ctx.Err()
}
func (s *Store) CompareAndPutRoute(ctx context.Context, k metadata.RouteKey, expected metadata.Version, r metadata.SandboxRecord) (metadata.Version, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return metadata.Version{}, err
	}
	if err := metadata.ValidateSandbox(r); err != nil {
		return metadata.Version{}, err
	}
	if s.routes[routeKey(k)].Version != expected {
		return metadata.Version{}, metadata.ErrConflict
	}
	v := s.next()
	s.putRoute(routeKey(k), r, v)
	return v, nil
}
func (s *Store) DeleteRoute(ctx context.Context, k metadata.RouteKey, expected metadata.Version) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	key := routeKey(k)
	if s.routes[key].Version != expected {
		return metadata.ErrConflict
	}
	delete(s.routes, key)
	s.next()
	s.notifyRoute(key)
	return nil
}
func (s *Store) CommitIntentAndOwnership(ctx context.Context, expected metadata.Version, r metadata.SandboxRecord, ref metadata.NodeSandboxRef) (metadata.Version, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return metadata.Version{}, err
	}
	if err := metadata.ValidateSandbox(r); err != nil {
		return metadata.Version{}, err
	}
	if err := metadata.ValidateSandboxRef(ref); err != nil {
		return metadata.Version{}, err
	}
	if metadata.SandboxRef(r) != ref || (r.State != metadata.StateReserved && r.State != metadata.StateReady && r.State != metadata.StatePausing) {
		return metadata.Version{}, metadata.ErrInvalidRecord
	}
	key := routeKey(metadata.RouteKey{Group: r.Group, RouteKey: r.RouteKey})
	if s.routes[key].Version != expected || !s.live(r.NodeID) {
		return metadata.Version{}, metadata.ErrConflict
	}
	child := childKey(r.NodeID, ref.NodeSandboxID)
	old := s.sandboxes[child]
	if old.Exists && old.Value != ref {
		return metadata.Version{}, metadata.ErrOwnershipConflict
	}
	v := s.next()
	s.putRoute(key, r, v)
	s.sandboxes[child] = metadata.Versioned[metadata.NodeSandboxRef]{Exists: true, Version: v, Value: ref}
	return v, nil
}
func (s *Store) RollbackIntent(ctx context.Context, k metadata.RouteKey, expected metadata.Version, restore *metadata.SandboxRecord, drop bool) (metadata.Version, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return metadata.Version{}, err
	}
	key := routeKey(k)
	cur := s.routes[key]
	if !cur.Exists || cur.Version != expected || cur.Value.State != metadata.StateReserved {
		return metadata.Version{}, metadata.ErrConflict
	}
	var next metadata.SandboxRecord
	if restore != nil {
		next = clone(*restore)
		if next.NextSandboxGeneration < cur.Value.NextSandboxGeneration {
			next.NextSandboxGeneration = cur.Value.NextSandboxGeneration
		}
		if next.NodeID != "" && s.live(next.NodeID) {
			ref := metadata.SandboxRef(next)
			old := s.sandboxes[childKey(next.NodeID, next.NodeSandboxID)]
			if old.Exists && old.Value != ref {
				return metadata.Version{}, metadata.ErrOwnershipConflict
			}
		}
	}
	v := s.next()
	if restore == nil {
		delete(s.routes, key)
		s.notifyRoute(key)
	} else {
		s.putRoute(key, next, v)
		child := childKey(next.NodeID, next.NodeSandboxID)
		if s.live(next.NodeID) {
			s.sandboxes[child] = metadata.Versioned[metadata.NodeSandboxRef]{Exists: true, Version: v, Value: metadata.SandboxRef(next)}
		}
	}
	if drop && (restore == nil || cur.Value.NodeID != next.NodeID || metadata.SandboxRef(cur.Value) != metadata.SandboxRef(next)) {
		child := childKey(cur.Value.NodeID, cur.Value.NodeSandboxID)
		if owner := s.sandboxes[child]; owner.Exists && owner.Value == metadata.SandboxRef(cur.Value) {
			delete(s.sandboxes, child)
		}
	}
	if restore == nil {
		return metadata.Version{}, nil
	}
	return v, nil
}
func (s *Store) ApplyRouteReport(ctx context.Context, registration metadata.Registration, ref metadata.NodeSandboxRef, event routesync.RouteEntry) (metadata.RouteReportResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return metadata.RouteReportResult{}, err
	}
	if err := s.guardRegistration(registration); err != nil {
		return metadata.RouteReportResult{}, err
	}
	key := routeKey(metadata.RouteKey{Group: ref.Group, RouteKey: ref.RouteKey})
	result, err := metadata.MergeRouteReport(clone(s.routes[key]), registration.NodeID, ref, event, time.Now().Unix())
	if err != nil {
		return result, err
	}
	if result.Disposition == metadata.ReportWritten {
		v := s.next()
		s.putRoute(key, result.Record.Value, v)
		result.Record.Version = v
	}
	return result, nil
}
func (s *Store) ApplyRouteDelete(ctx context.Context, registration metadata.Registration, expected metadata.Version, ref metadata.NodeSandboxRef, restore *metadata.SandboxRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.guardRegistration(registration); err != nil {
		return err
	}
	if err := metadata.ValidateSandboxRef(ref); err != nil {
		return err
	}
	key := routeKey(metadata.RouteKey{Group: ref.Group, RouteKey: ref.RouteKey})
	current := s.routes[key]
	if current.Version != expected {
		return metadata.ErrConflict
	}
	matched := current.Exists && current.Value.NodeID == registration.NodeID && metadata.SandboxRef(current.Value) == ref
	preserveOwner := false
	var next metadata.SandboxRecord
	if matched && restore != nil {
		if current.Value.State != metadata.StateReserved {
			return metadata.ErrConflict
		}
		next = clone(*restore)
		if next.Group != ref.Group || next.RouteKey != ref.RouteKey {
			return metadata.ErrInvalidRecord
		}
		if err := metadata.ValidateSandbox(next); err != nil {
			return err
		}
		if next.NextSandboxGeneration < current.Value.NextSandboxGeneration {
			next.NextSandboxGeneration = current.Value.NextSandboxGeneration
		}
		if next.NodeID != "" && s.live(next.NodeID) {
			nextRef := metadata.SandboxRef(next)
			owner := s.sandboxes[childKey(next.NodeID, next.NodeSandboxID)]
			if owner.Exists && owner.Value != nextRef {
				return metadata.ErrOwnershipConflict
			}
			preserveOwner = next.NodeID == registration.NodeID && nextRef == ref
		}
	}
	if matched {
		v := s.next()
		if restore == nil {
			delete(s.routes, key)
			s.notifyRoute(key)
		} else {
			s.putRoute(key, next, v)
			if next.NodeID != "" && s.live(next.NodeID) {
				child := childKey(next.NodeID, next.NodeSandboxID)
				s.sandboxes[child] = metadata.Versioned[metadata.NodeSandboxRef]{Exists: true, Version: v, Value: metadata.SandboxRef(next)}
			}
		}
	}
	if !preserveOwner {
		child := childKey(registration.NodeID, ref.NodeSandboxID)
		if owner := s.sandboxes[child]; owner.Exists && owner.Value == ref {
			delete(s.sandboxes, child)
		}
	}
	return nil
}
func (s *Store) ListRoutes(ctx context.Context, group string, cursor metadata.Cursor, limit int) (metadata.Page[metadata.Versioned[metadata.SandboxRecord]], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return page(s.routes, group+"\x00", cursor, limit), ctx.Err()
}

func (s *Store) ReadBuild(ctx context.Context, k metadata.BuildKey) (metadata.Versioned[metadata.BuildRecord], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return clone(s.builds[buildKey(k)]), ctx.Err()
}
func (s *Store) CompareAndPutBuild(ctx context.Context, k metadata.BuildKey, expected metadata.Version, b metadata.BuildRecord) (metadata.Version, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return metadata.Version{}, err
	}
	key := buildKey(k)
	if s.builds[key].Version != expected {
		return metadata.Version{}, metadata.ErrConflict
	}
	v := s.next()
	s.putBuild(key, b, v)
	return v, nil
}
func (s *Store) DeleteBuild(ctx context.Context, k metadata.BuildKey, expected metadata.Version) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	key := buildKey(k)
	if s.builds[key].Version != expected {
		return metadata.ErrConflict
	}
	delete(s.builds, key)
	s.next()
	return nil
}
func (s *Store) CommitRegistrationIntent(ctx context.Context, b metadata.BuildRecord, ref metadata.NodeBuildRef) (metadata.Version, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return metadata.Version{}, err
	}
	key := buildKey(metadata.BuildKey{Group: b.Group, BuildID: b.BuildID})
	if s.builds[key].Exists || !s.live(b.NodeID) {
		return metadata.Version{}, metadata.ErrConflict
	}
	child := childKey(b.NodeID, b.BuildID)
	old := s.owners[child]
	if old.Exists && (old.Value.Group != ref.Group || old.Value.BuildID != ref.BuildID || (old.Value.TemplateID != "" && old.Value.TemplateID != ref.TemplateID)) {
		return metadata.Version{}, metadata.ErrOwnershipConflict
	}
	v := s.next()
	s.putBuild(key, b, v)
	s.owners[child] = metadata.Versioned[metadata.NodeBuildRef]{Exists: true, Version: v, Value: ref}
	return v, nil
}
func (s *Store) AcceptBuildRegistration(ctx context.Context, k metadata.BuildKey, node, template string, target *types.BuildTarget) (metadata.Versioned[metadata.BuildRecord], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return metadata.Versioned[metadata.BuildRecord]{}, err
	}
	key := buildKey(k)
	current := s.builds[key]
	next, changed, err := metadata.MergeBuildAccepted(clone(current), node, template, target)
	if err != nil {
		return metadata.Versioned[metadata.BuildRecord]{}, err
	}
	if changed {
		s.putBuild(key, next, s.next())
	}
	return clone(s.builds[key]), nil
}
func (s *Store) EnsureRegistrationOwnership(ctx context.Context, k metadata.BuildKey, node, template string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	b := s.builds[buildKey(k)]
	if !b.Exists || b.Value.NodeID != node || b.Value.TemplateID != template || !s.live(node) {
		return metadata.ErrConflict
	}
	ref := metadata.NodeBuildRef{Group: k.Group, BuildID: k.BuildID, TemplateID: template}
	key := childKey(node, k.BuildID)
	old := s.owners[key]
	if old.Exists && old.Value != ref {
		return metadata.ErrOwnershipConflict
	}
	if !old.Exists {
		s.owners[key] = metadata.Versioned[metadata.NodeBuildRef]{Exists: true, Version: s.next(), Value: ref}
	}
	return nil
}
func (s *Store) RejectRegistrationIntent(ctx context.Context, k metadata.BuildKey, node, template string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	key := buildKey(k)
	b := s.builds[key]
	if !b.Exists || b.Value.NodeID != node || b.Value.TemplateID != template || b.Value.State != metadata.BuildStarting || b.Value.RegistrationTargetSet {
		return metadata.ErrConflict
	}
	ref := metadata.NodeBuildRef{Group: k.Group, BuildID: k.BuildID, TemplateID: template}
	child := childKey(node, k.BuildID)
	old := s.owners[child]
	if old.Exists && old.Value != ref {
		return metadata.ErrConflict
	}
	delete(s.builds, key)
	delete(s.owners, child)
	s.next()
	return nil
}
func (s *Store) ListBuilds(ctx context.Context, group string, cursor metadata.Cursor, limit int) (metadata.Page[metadata.Versioned[metadata.BuildRecord]], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return page(s.builds, group+"\x00", cursor, limit), ctx.Err()
}
func (s *Store) ListBuildsByTemplate(ctx context.Context, group, template string, cursor metadata.Cursor, limit int) (metadata.Page[metadata.Versioned[metadata.BuildRecord]], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	filtered := map[string]metadata.Versioned[metadata.BuildRecord]{}
	for k, b := range s.builds {
		if b.Value.Group == group && b.Value.TemplateID == template {
			filtered[k] = b
		}
	}
	return page(filtered, group+"\x00", cursor, limit), ctx.Err()
}

func (s *Store) ReadNodeProfile(ctx context.Context, node string) (metadata.NodeSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return clone(s.nodes[node]), ctx.Err()
}
func (s *Store) Register(ctx context.Context, expected metadata.Version, p metadata.NodeProfileRecord) (metadata.NodeSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return metadata.NodeSnapshot{}, err
	}
	if s.nodes[p.NodeID].Version != expected {
		return metadata.NodeSnapshot{}, metadata.ErrConflict
	}
	p.State = metadata.NodeLive
	v := s.next()
	n := metadata.NodeSnapshot{Exists: true, Version: v, Value: clone(p), Registration: metadata.Registration{NodeID: p.NodeID, LinkOwner: p.LinkOwner, Fence: v}}
	s.nodes[p.NodeID] = n
	s.updateDirectory(n, v)
	return clone(n), nil
}
func (s *Store) UpdateProfile(ctx context.Context, reg metadata.Registration, expected metadata.Version, p metadata.NodeProfileRecord) (metadata.Version, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return metadata.Version{}, err
	}
	n := s.nodes[p.NodeID]
	if !n.Exists || n.Reaping || n.Registration != reg || n.Version != expected || p.LinkOwner != reg.LinkOwner {
		return metadata.Version{}, metadata.ErrConflict
	}
	p.State = metadata.NodeLive
	v := s.next()
	n.Value = clone(p)
	n.Version = v
	s.nodes[p.NodeID] = n
	s.updateDirectory(n, v)
	return v, nil
}
func (s *Store) BeginReap(ctx context.Context, node string, expected metadata.Version) (metadata.ReapClaim, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return metadata.ReapClaim{}, err
	}
	n := s.nodes[node]
	if !n.Exists || n.Version != expected {
		return metadata.ReapClaim{}, metadata.ErrConflict
	}
	if !n.Reaping {
		v := s.next()
		n.Reaping = true
		n.Version = v
		n.Value.State = metadata.NodeDead
		s.nodes[node] = n
		s.updateDirectory(n, v)
	}
	return metadata.ReapClaim{NodeID: node, LinkOwner: n.Value.LinkOwner, ProfileVersion: n.Version, Fence: n.Registration.Fence}, nil
}
func (s *Store) validClaim(c metadata.ReapClaim) bool {
	n := s.nodes[c.NodeID]
	return n.Exists && n.Reaping && n.Value.LinkOwner == c.LinkOwner && n.Version == c.ProfileVersion && n.Registration.Fence == c.Fence
}
func (s *Store) CleanupSandbox(ctx context.Context, c metadata.ReapClaim, ref metadata.Versioned[metadata.NodeSandboxRef], expected metadata.Version, allowReserved bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if !s.validClaim(c) {
		return metadata.ErrConflict
	}
	child := childKey(c.NodeID, ref.Value.NodeSandboxID)
	old := s.sandboxes[child]
	if old.Version != ref.Version || old.Value != ref.Value {
		return metadata.ErrConflict
	}
	key := routeKey(metadata.RouteKey{Group: ref.Value.Group, RouteKey: ref.Value.RouteKey})
	r := s.routes[key]
	if r.Version != expected {
		return metadata.ErrConflict
	}
	if r.Exists && r.Value.NodeID == c.NodeID && metadata.SandboxRef(r.Value) == ref.Value {
		if r.Value.State == metadata.StateDead || r.Value.State == metadata.StateDeleting || (r.Value.State == metadata.StateReserved && !allowReserved) {
			return nil
		}
		delete(s.routes, key)
		s.notifyRoute(key)
	}
	delete(s.sandboxes, child)
	s.next()
	return nil
}
func (s *Store) CleanupBuild(ctx context.Context, c metadata.ReapClaim, ref metadata.Versioned[metadata.NodeBuildRef], expected metadata.Version) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if !s.validClaim(c) {
		return metadata.ErrConflict
	}
	child := childKey(c.NodeID, ref.Value.BuildID)
	old := s.owners[child]
	if old.Version != ref.Version || old.Value != ref.Value {
		return metadata.ErrConflict
	}
	key := buildKey(metadata.BuildKey{Group: ref.Value.Group, BuildID: ref.Value.BuildID})
	b := s.builds[key]
	if b.Version != expected {
		return metadata.ErrConflict
	}
	if b.Exists && b.Value.NodeID == c.NodeID {
		switch b.Value.State {
		case metadata.BuildStarting, metadata.BuildRegistered, metadata.BuildWaiting, metadata.BuildBuilding:
			b.Value.State = metadata.BuildError
			b.Value.Reason = "node disconnected"
			b.Value.RegistrationImageRepo = ""
			b.Value.RegistrationRegistryAuth = ""
			s.putBuild(key, b.Value, s.next())
		}
		return nil
	}
	delete(s.owners, child)
	s.next()
	return nil
}
func (s *Store) CleanupKeyPair(ctx context.Context, c metadata.ReapClaim, p metadata.Versioned[metadata.NodeKeyPair]) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if !s.validClaim(c) {
		return metadata.ErrConflict
	}
	key := childKey(c.NodeID, p.Value.APISecretFingerprint)
	if s.keys[key].Version != p.Version {
		return metadata.ErrConflict
	}
	delete(s.keys, key)
	s.next()
	return nil
}
func (s *Store) FinalizeReap(ctx context.Context, c metadata.ReapClaim) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if !s.validClaim(c) {
		return false, metadata.ErrConflict
	}
	prefix := c.NodeID + "\x00"
	for k := range s.sandboxes {
		if strings.HasPrefix(k, prefix) {
			return false, nil
		}
	}
	for k := range s.owners {
		if strings.HasPrefix(k, prefix) {
			return false, nil
		}
	}
	for k := range s.keys {
		if strings.HasPrefix(k, prefix) {
			return false, nil
		}
	}
	delete(s.nodes, c.NodeID)
	s.next()
	return true, nil
}
func (s *Store) ListReaping(ctx context.Context, cursor metadata.Cursor, limit int) (metadata.Page[metadata.ReapClaim], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := map[string]metadata.ReapClaim{}
	for id, n := range s.nodes {
		if n.Reaping {
			m[id] = metadata.ReapClaim{NodeID: id, LinkOwner: n.Value.LinkOwner, ProfileVersion: n.Version, Fence: n.Registration.Fence}
		}
	}
	return page(m, "", cursor, limit), ctx.Err()
}

func (s *Store) ListExpiredNodeProfiles(ctx context.Context, cutoffUnix int64, cursor metadata.Cursor, limit int) (metadata.Page[metadata.Versioned[metadata.NodeProfileRecord]], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	values := page(s.nodes, "", cursor, limit)
	out := metadata.Page[metadata.Versioned[metadata.NodeProfileRecord]]{Next: values.Next}
	for _, value := range values.Items {
		if value.Value.State == metadata.NodeLive && value.Value.LastHeartbeatUnix > 0 && value.Value.LastHeartbeatUnix <= cutoffUnix {
			out.Items = append(out.Items, metadata.Versioned[metadata.NodeProfileRecord]{Exists: value.Exists, Version: value.Version, Value: value.Value})
		}
	}
	return out, ctx.Err()
}

func (s *Store) ReadSandboxOwner(ctx context.Context, node, id string) (metadata.Versioned[metadata.NodeSandboxRef], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return clone(s.sandboxes[childKey(node, id)]), ctx.Err()
}
func (s *Store) ReadBuildOwner(ctx context.Context, node, id string) (metadata.Versioned[metadata.NodeBuildRef], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return clone(s.owners[childKey(node, id)]), ctx.Err()
}
func (s *Store) EnsureSandboxOwner(ctx context.Context, node string, ref metadata.NodeSandboxRef) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	key := childKey(node, ref.NodeSandboxID)
	old := s.sandboxes[key]
	if old.Exists {
		if old.Value != ref {
			return metadata.ErrOwnershipConflict
		}
	}
	s.sandboxes[key] = metadata.Versioned[metadata.NodeSandboxRef]{Exists: true, Version: s.next(), Value: ref}
	return nil
}
func (s *Store) EnsureBuildOwner(ctx context.Context, node string, ref metadata.NodeBuildRef) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	key := childKey(node, ref.BuildID)
	old := s.owners[key]
	if old.Exists && (old.Value.Group != ref.Group || old.Value.BuildID != ref.BuildID || (old.Value.TemplateID != "" && old.Value.TemplateID != ref.TemplateID)) {
		return metadata.ErrOwnershipConflict
	}
	s.owners[key] = metadata.Versioned[metadata.NodeBuildRef]{Exists: true, Version: s.next(), Value: ref}
	return nil
}
func (s *Store) RemoveSandboxOwner(ctx context.Context, node, id string, v metadata.Version) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	key := childKey(node, id)
	if s.sandboxes[key].Version != v {
		return metadata.ErrConflict
	}
	delete(s.sandboxes, key)
	s.next()
	return nil
}
func (s *Store) RemoveBuildOwner(ctx context.Context, node, id string, v metadata.Version) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	key := childKey(node, id)
	if s.owners[key].Version != v {
		return metadata.ErrConflict
	}
	delete(s.owners, key)
	s.next()
	return nil
}
func (s *Store) ListSandboxOwners(ctx context.Context, node string, cursor metadata.Cursor, limit int) (metadata.Page[metadata.Versioned[metadata.NodeSandboxRef]], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return page(s.sandboxes, node+"\x00", cursor, limit), ctx.Err()
}
func (s *Store) ListBuildOwners(ctx context.Context, node string, cursor metadata.Cursor, limit int) (metadata.Page[metadata.Versioned[metadata.NodeBuildRef]], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return page(s.owners, node+"\x00", cursor, limit), ctx.Err()
}
func (s *Store) PutKeyPair(ctx context.Context, node string, p metadata.NodeKeyPair) (metadata.Versioned[metadata.NodeKeyPair], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return metadata.Versioned[metadata.NodeKeyPair]{}, err
	}
	p, err := metadata.NormalizeKeyPair(p)
	if err != nil {
		return metadata.Versioned[metadata.NodeKeyPair]{}, err
	}
	key := childKey(node, p.APISecretFingerprint)
	old := s.keys[key]
	if old.Exists && !metadata.SameKeyPairMaterial(old.Value, p) {
		return metadata.Versioned[metadata.NodeKeyPair]{}, metadata.ErrOwnershipConflict
	}
	if old.Exists && metadata.SameKeyPairMaterial(old.Value, p) {
		if old.Value.ExpiresUnix > p.ExpiresUnix {
			p.ExpiresUnix = old.Value.ExpiresUnix
		}
		p.AckedExpiresUnix = old.Value.AckedExpiresUnix
	} else {
		p.AckedExpiresUnix = 0
	}
	if old.Exists && reflect.DeepEqual(old.Value, p) {
		return clone(old), nil
	}
	next := metadata.Versioned[metadata.NodeKeyPair]{Exists: true, Version: s.next(), Value: p}
	s.keys[key] = next
	return clone(next), nil
}
func (s *Store) AckKeyPair(ctx context.Context, node string, p metadata.NodeKeyPair) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return false, err
	}
	key := childKey(node, p.APISecretFingerprint)
	cur := s.keys[key]
	if !cur.Exists || !metadata.SameKeyPairMaterial(cur.Value, p) || cur.Value.ExpiresUnix != p.ExpiresUnix {
		return false, nil
	}
	if cur.Value.AckedExpiresUnix >= p.ExpiresUnix {
		return true, nil
	}
	cur.Value.AckedExpiresUnix = p.ExpiresUnix
	cur.Version = s.next()
	s.keys[key] = cur
	return true, nil
}
func (s *Store) DropKeyPair(ctx context.Context, node, id string, v metadata.Version) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	key := childKey(node, id)
	if s.keys[key].Version != v {
		return metadata.ErrConflict
	}
	delete(s.keys, key)
	s.next()
	return nil
}
func (s *Store) ListKeyPairs(ctx context.Context, node string, cursor metadata.Cursor, limit int) (metadata.Page[metadata.Versioned[metadata.NodeKeyPair]], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return page(s.keys, node+"\x00", cursor, limit), ctx.Err()
}
func (s *Store) Load(ctx context.Context) (metadata.DirectoryView, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v := metadata.DirectoryView{Token: s.token}
	for _, e := range s.directory {
		v.Nodes = append(v.Nodes, clone(e))
	}
	sort.Slice(v.Nodes, func(i, j int) bool { return v.Nodes[i].NodeID < v.Nodes[j].NodeID })
	return v, ctx.Err()
}

type reservationKey struct{}
type reservation struct{ store *Store }

func (s *Store) ReserveObservation(ctx context.Context) (context.Context, func(), error) {
	if existing, ok := ctx.Value(reservationKey{}).(*reservation); ok && existing.store == s {
		return ctx, func() {}, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if s.reservations >= s.observationLimit {
		return nil, nil, metadata.ErrCapacity
	}
	s.reservations++
	var once sync.Once
	release := func() { once.Do(func() { s.mu.Lock(); s.reservations--; s.mu.Unlock() }) }
	return context.WithValue(ctx, reservationKey{}, &reservation{store: s}), release, nil
}

type invalidation struct {
	mu         sync.Mutex
	done       chan struct{}
	cancelled  chan struct{}
	once       sync.Once
	cancelOnce sync.Once
	err        error
	cancel     func()
}

func (w *invalidation) Done() <-chan struct{} { return w.done }
func (w *invalidation) Err() error            { w.mu.Lock(); defer w.mu.Unlock(); return w.err }
func (w *invalidation) fire(err error) {
	w.once.Do(func() { w.mu.Lock(); w.err = err; w.mu.Unlock(); close(w.done) })
}
func (w *invalidation) Cancel() {
	w.cancelOnce.Do(func() {
		if w.cancel != nil {
			w.cancel()
		}
		close(w.cancelled)
	})
}
func (s *Store) newWatch(ctx context.Context, release func()) *invalidation {
	w := &invalidation{done: make(chan struct{}), cancelled: make(chan struct{})}
	go func() {
		select {
		case <-ctx.Done():
			w.fire(ctx.Err())
		case <-w.done:
		case <-w.cancelled:
		}
	}()
	w.cancel = release
	return w
}
func (s *Store) ReadAndObserveRoute(ctx context.Context, k metadata.RouteKey) (metadata.Versioned[metadata.SandboxRecord], metadata.Invalidation, error) {
	admitted, release, err := s.ReserveObservation(ctx)
	if err != nil {
		return metadata.Versioned[metadata.SandboxRecord]{}, nil, err
	}
	s.mu.Lock()
	key := routeKey(k)
	var w *invalidation
	w = s.newWatch(admitted, func() { s.mu.Lock(); delete(s.routeWatches[key], w); s.mu.Unlock(); release() })
	if s.routeWatches[key] == nil {
		s.routeWatches[key] = map[*invalidation]struct{}{}
	}
	s.routeWatches[key][w] = struct{}{}
	r := clone(s.routes[key])
	s.mu.Unlock()
	return r, w, nil
}
func (s *Store) Observe(ctx context.Context, token metadata.DirectoryToken) (metadata.Invalidation, error) {
	admitted, release, err := s.ReserveObservation(ctx)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	var w *invalidation
	w = s.newWatch(admitted, func() { s.mu.Lock(); delete(s.directoryWatches, w); s.mu.Unlock(); release() })
	s.directoryWatches[w] = struct{}{}
	if token != s.token {
		w.fire(nil)
	}
	s.mu.Unlock()
	return w, nil
}

func (s *Store) Acquire(ctx context.Context, source, owner, run string, ttl time.Duration) (metadata.PlacerImportSourceState, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return metadata.PlacerImportSourceState{}, false, err
	}
	if source == "" || owner == "" || run == "" {
		return metadata.PlacerImportSourceState{}, false, metadata.ErrInvalidRecord
	}
	if ttl <= 0 {
		ttl = 15 * time.Second
	}
	now := time.Now()
	cur := s.imports[source]
	if cur.SourceID != "" && cur.ExpiresUnixMs > now.UnixMilli() && (cur.OwnerID != owner || cur.RunID != run) {
		return cur, false, nil
	}
	if cur.SourceID == "" || cur.OwnerID != owner || cur.RunID != run {
		cur.Term++
		if cur.Term == 0 {
			cur.Term = 1
		}
	}
	cur.SourceID, cur.OwnerID, cur.RunID = source, owner, run
	cur.ExpiresUnixMs = now.Add(ttl).UnixMilli()
	s.imports[source] = cur
	return cur, true, nil
}
func validLease(cur metadata.PlacerImportSourceState, owner, run string, term uint64) bool {
	return owner != "" && run != "" && term > 0 && cur.OwnerID == owner && cur.RunID == run && cur.Term == term && (cur.ExpiresUnixMs <= 0 || cur.ExpiresUnixMs > time.Now().UnixMilli())
}
func (s *Store) Check(ctx context.Context, source, owner, run string, term uint64) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return validLease(s.imports[source], owner, run, term), ctx.Err()
}
func (s *Store) Checkpoint(ctx context.Context, source, owner, run string, term uint64, cursor string, complete bool, lastErr string) (metadata.PlacerImportSourceState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return metadata.PlacerImportSourceState{}, err
	}
	cur := s.imports[source]
	if !validLease(cur, owner, run, term) {
		return metadata.PlacerImportSourceState{}, metadata.ErrStaleLease
	}
	cur.LastError = lastErr
	if complete {
		cur.Cursor = ""
		cur.Round++
	} else {
		cur.Cursor = cursor
	}
	s.imports[source] = cur
	return cur, nil
}

var _ = errors.Is

func (s *Store) DeleteRouteAndOwnership(ctx context.Context, k metadata.RouteKey, expected metadata.Version, node string, ref metadata.NodeSandboxRef) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	key := routeKey(k)
	cur := s.routes[key]
	if cur.Version != expected {
		return metadata.ErrConflict
	}
	child := childKey(node, ref.NodeSandboxID)
	owner := s.sandboxes[child]
	if cur.Exists && cur.Value.NodeID == node && metadata.SandboxRef(cur.Value) == ref {
		delete(s.routes, key)
		s.notifyRoute(key)
	}
	if owner.Exists && owner.Value == ref {
		delete(s.sandboxes, child)
	}
	s.next()
	return nil
}
func (s *Store) ApplyBuildReport(ctx context.Context, registration metadata.Registration, ref metadata.NodeBuildRef, e routesync.BuildEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.guardRegistration(registration); err != nil {
		return err
	}
	node := registration.NodeID
	child := childKey(node, ref.BuildID)
	owner := s.owners[child]
	if !owner.Exists || owner.Value != ref {
		return nil
	}
	if ref.TemplateID != "" && ref.TemplateID != e.TemplateID {
		return nil
	}
	key := buildKey(metadata.BuildKey{Group: ref.Group, BuildID: ref.BuildID})
	cur := s.builds[key]
	if !cur.Exists || (cur.Value.NodeID != "" && cur.Value.NodeID != node) {
		return nil
	}
	next := clone(cur.Value)
	if e.TemplateID != "" && types.IsTransientID(next.TemplateID) && next.TemplateID != e.TemplateID {
		return nil
	}
	next.State = metadata.BuildState(e.State)
	if next.RegistrationTargetSet {
		next.RegistrationImageRepo = ""
		next.RegistrationRegistryAuth = ""
	}
	if types.IsTransientID(e.TemplateID) {
		next.TemplateID = e.TemplateID
	}
	if e.PersistID != "" {
		next.PersistID = e.PersistID
	}
	next.Reason = e.Reason
	s.putBuild(key, next, s.next())
	return nil
}
func (s *Store) DeleteBuildProjection(ctx context.Context, registration metadata.Registration, ref metadata.NodeBuildRef) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.guardRegistration(registration); err != nil {
		return err
	}
	node := registration.NodeID
	child := childKey(node, ref.BuildID)
	owner := s.owners[child]
	if !owner.Exists || owner.Value != ref {
		return nil
	}
	key := buildKey(metadata.BuildKey{Group: ref.Group, BuildID: ref.BuildID})
	cur := s.builds[key]
	if cur.Exists && ref.TemplateID != "" && cur.Value.TemplateID != ref.TemplateID {
		return nil
	}
	if cur.Exists && cur.Value.NodeID == node {
		if cur.Value.State == metadata.BuildStarting {
			return nil
		}
		delete(s.builds, key)
	}
	delete(s.owners, child)
	s.next()
	return nil
}
