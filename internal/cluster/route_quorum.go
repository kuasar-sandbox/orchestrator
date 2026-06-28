package cluster

import (
	"context"
	"errors"
	"sort"
	"strconv"
	"sync"
	"time"
)

var ErrQuorum = errors.New("cluster: quorum unavailable")

type RouteProposal func(current RouteRecord, found bool) (RouteRecord, bool, error)

type RouteReplica interface {
	Read(ctx context.Context, key string) (RouteRecord, bool, error)
	Prepare(ctx context.Context, key string, ballot Ballot) (RouteRecord, bool, bool, error)
	Accept(ctx context.Context, key string, rec RouteRecord, ballot Ballot) (bool, error)
	Repair(ctx context.Context, key string, rec RouteRecord) error
	MaxBallot(ctx context.Context, key string) (Ballot, error)
	Keys(ctx context.Context) []string
}

type RouteReplicaSlot struct {
	ID      string
	Replica RouteReplica
}

// RouteQuorum is the leaderless route_link write protocol over an owner set. It
// uses unique ballots plus prepare/accept; reads repair the highest accepted value
// back to lagging owners.
type RouteQuorum struct {
	writer   string
	replicas []routeReplicaSlot
	sets     []quorumSet
	mu       sync.Mutex
	rounds   map[string]uint64
}

func NewRouteQuorum(writer string, replicas ...RouteReplica) *RouteQuorum {
	if writer == "" {
		writer = "local"
	}
	slots := make([]RouteReplicaSlot, 0, len(replicas))
	owners := make([]string, 0, len(replicas))
	for i, rep := range replicas {
		id := replicaIndexID(i)
		slots = append(slots, RouteReplicaSlot{ID: id, Replica: rep})
		owners = append(owners, id)
	}
	return NewRouteJointQuorum(writer, slots, [][]string{owners})
}

func NewRouteJointQuorum(writer string, slots []RouteReplicaSlot, ownerSets [][]string) *RouteQuorum {
	if writer == "" {
		writer = "local"
	}
	reps, sets := buildRouteQuorumSets(slots, ownerSets)
	return &RouteQuorum{writer: writer, replicas: reps, sets: sets, rounds: map[string]uint64{}}
}

func (q *RouteQuorum) Get(ctx context.Context, group, routeKey string) (RouteRecord, bool, error) {
	key := RouteKey(group, routeKey)
	ballot := q.nextBallot(ctx, key)
	reads := make([]routeRead, 0, len(q.replicas))
	prepared := map[int]bool{}
	for i, r := range q.replicas {
		rec, found, ok, err := r.replica.Prepare(ctx, key, ballot)
		if err != nil || !ok {
			continue
		}
		prepared[i] = true
		reads = append(reads, routeRead{rec: rec, found: found})
	}
	if !satisfiesQuorumSets(prepared, q.sets) {
		return RouteRecord{}, false, ErrQuorum
	}
	best, found := highestRoute(reads)
	if found {
		best.Meta.Ballot = ballot
		accepted := map[int]bool{}
		for i := range prepared {
			if ok, _ := q.replicas[i].replica.Accept(ctx, key, best, ballot); ok {
				accepted[i] = true
			}
		}
		if !satisfiesQuorumSets(accepted, q.sets) {
			return RouteRecord{}, false, ErrQuorum
		}
		for _, r := range q.replicas {
			_ = r.replica.Repair(ctx, key, best)
		}
	}
	if found && best.State == RouteDead {
		return RouteRecord{}, false, nil
	}
	return best, found, nil
}

func (q *RouteQuorum) List(ctx context.Context, group string, fn func(RouteRecord) error) error {
	keys := map[string]bool{}
	for _, r := range q.replicas {
		for _, key := range r.replica.Keys(ctx) {
			g, _ := splitRouteKey(key)
			if group == "" || g == group {
				keys[key] = true
			}
		}
	}
	ordered := make([]string, 0, len(keys))
	for key := range keys {
		ordered = append(ordered, key)
	}
	sort.Strings(ordered)
	for _, key := range ordered {
		g, rk := splitRouteKey(key)
		rec, found, err := q.Get(ctx, g, rk)
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

func (q *RouteQuorum) CAS(ctx context.Context, group, routeKey string, expectRev uint64, propose RouteProposal) (RouteRecord, error) {
	key := RouteKey(group, routeKey)
	ballot := q.nextBallot(ctx, key)
	reads := make([]routeRead, 0, len(q.replicas))
	prepared := map[int]bool{}
	for i, r := range q.replicas {
		rec, found, ok, err := r.replica.Prepare(ctx, key, ballot)
		if err != nil || !ok {
			continue
		}
		prepared[i] = true
		reads = append(reads, routeRead{rec: rec, found: found})
	}
	if !satisfiesQuorumSets(prepared, q.sets) {
		return RouteRecord{}, ErrQuorum
	}
	cur, physicalFound := highestRoute(reads)
	logicalFound := physicalFound && cur.State != RouteDead
	proposalCur := cur
	if !logicalFound {
		proposalCur = RouteRecord{}
	}
	if !matchRev(logicalFound, cur.Meta.Rev, expectRev) {
		return RouteRecord{}, ErrConflict
	}
	next, keep, err := propose(proposalCur, logicalFound)
	if err != nil {
		return RouteRecord{}, err
	}
	if !keep {
		if !physicalFound {
			return RouteRecord{}, nil
		}
		group, rk := cur.Group, cur.RouteKey
		if group == "" || rk == "" {
			group, rk = splitRouteKey(key)
		}
		tombstone := RouteRecord{
			Meta:     RecordMeta{Ballot: ballot, Rev: cur.Meta.Rev + 1, UpdatedAt: time.Now()},
			Group:    group,
			RouteKey: rk,
			State:    RouteDead,
		}
		accepted := map[int]bool{}
		for i := range prepared {
			if ok, _ := q.replicas[i].replica.Accept(ctx, key, tombstone, ballot); ok {
				accepted[i] = true
			}
		}
		if !satisfiesQuorumSets(accepted, q.sets) {
			return RouteRecord{}, ErrQuorum
		}
		for _, r := range q.replicas {
			_ = r.replica.Repair(ctx, key, tombstone)
		}
		return RouteRecord{}, nil
	}
	next.Meta = RecordMeta{Ballot: ballot, Rev: cur.Meta.Rev + 1, UpdatedAt: time.Now()}
	accepted := map[int]bool{}
	for i := range prepared {
		if ok, _ := q.replicas[i].replica.Accept(ctx, key, next, ballot); ok {
			accepted[i] = true
		}
	}
	if !satisfiesQuorumSets(accepted, q.sets) {
		return RouteRecord{}, ErrQuorum
	}
	for _, r := range q.replicas {
		_ = r.replica.Repair(ctx, key, next)
	}
	return next, nil
}

func (q *RouteQuorum) nextBallot(ctx context.Context, key string) Ballot {
	var max Ballot
	for _, r := range q.replicas {
		if b, err := r.replica.MaxBallot(ctx, key); err == nil && max.Less(b) {
			max = b
		}
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if local := q.rounds[key]; max.Round < local {
		max.Round = local
	}
	next := max.Round + 1
	q.rounds[key] = next
	return Ballot{Round: next, Writer: q.writer}
}

type routeReplicaSlot struct {
	id      string
	replica RouteReplica
}

type quorumSet struct {
	indices []int
	quorum  int
}

func buildRouteQuorumSets(slots []RouteReplicaSlot, ownerSets [][]string) ([]routeReplicaSlot, []quorumSet) {
	reps := make([]routeReplicaSlot, 0, len(slots))
	byID := map[string]int{}
	for i, slot := range slots {
		if slot.Replica == nil {
			continue
		}
		id := slot.ID
		if id == "" {
			id = replicaIndexID(i)
		}
		if _, ok := byID[id]; ok {
			continue
		}
		byID[id] = len(reps)
		reps = append(reps, routeReplicaSlot{id: id, replica: slot.Replica})
	}
	sets := make([]quorumSet, 0, len(ownerSets))
	for _, owners := range ownerSets {
		seen := map[int]bool{}
		var indices []int
		for _, id := range owners {
			idx, ok := byID[id]
			if !ok || seen[idx] {
				continue
			}
			seen[idx] = true
			indices = append(indices, idx)
		}
		if len(indices) == 0 {
			continue
		}
		sets = append(sets, quorumSet{indices: indices, quorum: len(indices)/2 + 1})
	}
	if len(sets) == 0 && len(reps) != 0 {
		indices := make([]int, len(reps))
		for i := range reps {
			indices[i] = i
		}
		sets = append(sets, quorumSet{indices: indices, quorum: len(indices)/2 + 1})
	}
	return reps, sets
}

func satisfiesQuorumSets(votes map[int]bool, sets []quorumSet) bool {
	if len(sets) == 0 {
		return false
	}
	for _, set := range sets {
		n := 0
		for _, idx := range set.indices {
			if votes[idx] {
				n++
			}
		}
		if n < set.quorum {
			return false
		}
	}
	return true
}

func replicaIndexID(i int) string {
	return "#" + strconv.Itoa(i)
}

type routeRead struct {
	rec   RouteRecord
	found bool
}

func highestRoute(reads []routeRead) (RouteRecord, bool) {
	var best RouteRecord
	found := false
	for _, rr := range reads {
		if !rr.found {
			continue
		}
		if !found || best.Meta.Ballot.Less(rr.rec.Meta.Ballot) ||
			(best.Meta.Ballot == rr.rec.Meta.Ballot && best.Meta.Rev < rr.rec.Meta.Rev) {
			best, found = rr.rec, true
		}
	}
	return best, found
}

type MemoryRouteReplica struct {
	mu       sync.Mutex
	promised map[string]Ballot
	accepted map[string]RouteRecord
}

func NewMemoryRouteReplica() *MemoryRouteReplica {
	return &MemoryRouteReplica{promised: map[string]Ballot{}, accepted: map[string]RouteRecord{}}
}

func (r *MemoryRouteReplica) Read(ctx context.Context, key string) (RouteRecord, bool, error) {
	if err := ctx.Err(); err != nil {
		return RouteRecord{}, false, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	rec, found := r.accepted[key]
	return cloneRoute(rec), found, nil
}

func (r *MemoryRouteReplica) Prepare(ctx context.Context, key string, ballot Ballot) (RouteRecord, bool, bool, error) {
	if err := ctx.Err(); err != nil {
		return RouteRecord{}, false, false, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if ballot.Less(r.promised[key]) {
		return RouteRecord{}, false, false, nil
	}
	r.promised[key] = ballot
	rec, found := r.accepted[key]
	return cloneRoute(rec), found, true, nil
}

func (r *MemoryRouteReplica) Accept(ctx context.Context, key string, rec RouteRecord, ballot Ballot) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if ballot.Less(r.promised[key]) {
		return false, nil
	}
	r.promised[key] = ballot
	r.accepted[key] = cloneRoute(rec)
	return true, nil
}

func (r *MemoryRouteReplica) AcceptDelete(ctx context.Context, key string, ballot Ballot) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if ballot.Less(r.promised[key]) {
		return false, nil
	}
	r.promised[key] = ballot
	cur, found := r.accepted[key]
	group, routeKey := splitRouteKey(key)
	tombstone := RouteRecord{
		Meta:     RecordMeta{Ballot: ballot, Rev: cur.Meta.Rev + 1, UpdatedAt: time.Now()},
		Group:    group,
		RouteKey: routeKey,
		State:    RouteDead,
	}
	if !found {
		tombstone.Meta.Rev = 1
	}
	r.accepted[key] = tombstone
	return true, nil
}

func (r *MemoryRouteReplica) Repair(ctx context.Context, key string, rec RouteRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	cur, found := r.accepted[key]
	if !found || cur.Meta.Ballot.Less(rec.Meta.Ballot) ||
		(cur.Meta.Ballot == rec.Meta.Ballot && cur.Meta.Rev < rec.Meta.Rev) {
		r.accepted[key] = cloneRoute(rec)
	}
	return nil
}

func splitRouteKey(key string) (string, string) {
	for i := 0; i < len(key); i++ {
		if key[i] == 0 {
			return key[:i], key[i+1:]
		}
	}
	return "", key
}

func (r *MemoryRouteReplica) MaxBallot(ctx context.Context, key string) (Ballot, error) {
	if err := ctx.Err(); err != nil {
		return Ballot{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	max := r.promised[key]
	if rec, found := r.accepted[key]; found && max.Less(rec.Meta.Ballot) {
		max = rec.Meta.Ballot
	}
	return max, nil
}

func (r *MemoryRouteReplica) Keys(ctx context.Context) []string {
	if err := ctx.Err(); err != nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	keys := make([]string, 0, len(r.accepted))
	for key, rec := range r.accepted {
		if rec.State != RouteDead {
			keys = append(keys, key)
		}
	}
	return keys
}
