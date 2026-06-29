package cluster

import (
	"context"
	"errors"
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
	reads, prepared := q.prepare(ctx, key, ballot)
	if !satisfiesQuorumSets(prepared, q.sets) {
		return RouteRecord{}, false, ErrQuorum
	}
	best, found := highestRoute(reads)
	if found {
		best.Meta.Ballot = ballot
		accepted := q.accept(ctx, key, best, ballot, prepared)
		if !satisfiesQuorumSets(accepted, q.sets) {
			return RouteRecord{}, false, ErrQuorum
		}
		q.repairBestEffort(ctx, key, best)
	}
	if found && best.State == RouteDead {
		return RouteRecord{}, false, nil
	}
	return best, found, nil
}

func (q *RouteQuorum) CAS(ctx context.Context, group, routeKey string, expectRev uint64, propose RouteProposal) (RouteRecord, error) {
	key := RouteKey(group, routeKey)
	ballot := q.nextBallot(ctx, key)
	reads, prepared := q.prepare(ctx, key, ballot)
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
		accepted := q.accept(ctx, key, tombstone, ballot, prepared)
		if !satisfiesQuorumSets(accepted, q.sets) {
			return RouteRecord{}, ErrQuorum
		}
		q.repairBestEffort(ctx, key, tombstone)
		return RouteRecord{}, nil
	}
	next.Meta = RecordMeta{Ballot: ballot, Rev: cur.Meta.Rev + 1, UpdatedAt: time.Now()}
	accepted := q.accept(ctx, key, next, ballot, prepared)
	if !satisfiesQuorumSets(accepted, q.sets) {
		return RouteRecord{}, ErrQuorum
	}
	q.repairBestEffort(ctx, key, next)
	return next, nil
}

func (q *RouteQuorum) nextBallot(ctx context.Context, key string) Ballot {
	max := q.maxBallot(ctx, key)
	q.mu.Lock()
	defer q.mu.Unlock()
	if local := q.rounds[key]; max.Round < local {
		max.Round = local
	}
	next := max.Round + 1
	q.rounds[key] = next
	return Ballot{Round: next, Writer: q.writer}
}

func (q *RouteQuorum) maxBallot(ctx context.Context, key string) Ballot {
	type result struct {
		ballot Ballot
		err    error
	}
	ch := make(chan result, len(q.replicas))
	for _, r := range q.replicas {
		rep := r.replica
		go func() {
			b, err := rep.MaxBallot(ctx, key)
			ch <- result{ballot: b, err: err}
		}()
	}
	var max Ballot
	for range q.replicas {
		res := <-ch
		if res.err == nil && max.Less(res.ballot) {
			max = res.ballot
		}
	}
	return max
}

func (q *RouteQuorum) prepare(ctx context.Context, key string, ballot Ballot) ([]routeRead, map[int]bool) {
	type result struct {
		idx   int
		rec   RouteRecord
		found bool
		ok    bool
		err   error
	}
	ch := make(chan result, len(q.replicas))
	for i, r := range q.replicas {
		idx, rep := i, r.replica
		go func() {
			rec, found, ok, err := rep.Prepare(ctx, key, ballot)
			ch <- result{idx: idx, rec: rec, found: found, ok: ok, err: err}
		}()
	}
	reads := make([]routeRead, 0, len(q.replicas))
	prepared := map[int]bool{}
	for range q.replicas {
		res := <-ch
		if res.err != nil || !res.ok {
			continue
		}
		prepared[res.idx] = true
		reads = append(reads, routeRead{rec: res.rec, found: res.found})
	}
	return reads, prepared
}

func (q *RouteQuorum) accept(ctx context.Context, key string, rec RouteRecord, ballot Ballot, prepared map[int]bool) map[int]bool {
	type result struct {
		idx int
		ok  bool
	}
	ch := make(chan result, len(prepared))
	for i := range prepared {
		idx, rep := i, q.replicas[i].replica
		go func() {
			ok, _ := rep.Accept(ctx, key, rec, ballot)
			ch <- result{idx: idx, ok: ok}
		}()
	}
	accepted := map[int]bool{}
	for range prepared {
		res := <-ch
		if res.ok {
			accepted[res.idx] = true
		}
	}
	return accepted
}

func (q *RouteQuorum) repairBestEffort(ctx context.Context, key string, rec RouteRecord) {
	repairCtx := context.WithoutCancel(ctx)
	if _, ok := repairCtx.Deadline(); !ok {
		var cancel context.CancelFunc
		repairCtx, cancel = context.WithTimeout(repairCtx, 250*time.Millisecond)
		defer cancel()
	}
	var wg sync.WaitGroup
	for _, r := range q.replicas {
		rep := r.replica
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = rep.Repair(repairCtx, key, rec)
		}()
	}
	wg.Wait()
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

func (r *MemoryRouteReplica) ListGroup(ctx context.Context, group string) ([]RouteRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]RouteRecord, 0)
	for key, rec := range r.accepted {
		g, _ := splitRouteKey(key)
		if g == group && rec.State != RouteDead {
			out = append(out, cloneRoute(rec))
		}
	}
	return out, nil
}
