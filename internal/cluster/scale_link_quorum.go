package cluster

import (
	"context"
	"sync"
	"time"
)

type ScaleLinkProposal func(current ScaleLinkRecord, found bool) (ScaleLinkRecord, bool, error)

type ScaleLinkReplica interface {
	Read(ctx context.Context, taskID string) (ScaleLinkRecord, bool, error)
	Prepare(ctx context.Context, taskID string, ballot Ballot) (ScaleLinkRecord, bool, bool, error)
	Accept(ctx context.Context, taskID string, rec ScaleLinkRecord, ballot Ballot) (bool, error)
	Repair(ctx context.Context, taskID string, rec ScaleLinkRecord) error
	MaxBallot(ctx context.Context, taskID string) (Ballot, error)
}

type ScaleLinkReplicaSlot struct {
	ID      string
	Replica ScaleLinkReplica
}

type ScaleLinkQuorum struct {
	writer   string
	replicas []scaleLinkReplicaSlot
	sets     []quorumSet
	mu       sync.Mutex
	rounds   map[string]uint64
}

func NewScaleLinkJointQuorum(writer string, slots []ScaleLinkReplicaSlot, ownerSets [][]string) *ScaleLinkQuorum {
	if writer == "" {
		writer = "local"
	}
	reps, sets := buildScaleLinkQuorumSets(slots, ownerSets)
	return &ScaleLinkQuorum{writer: writer, replicas: reps, sets: sets, rounds: map[string]uint64{}}
}

func (q *ScaleLinkQuorum) Get(ctx context.Context, taskID string) (ScaleLinkRecord, bool, error) {
	rec, found, err := q.Read(ctx, taskID)
	if err != nil || !found {
		return ScaleLinkRecord{}, false, err
	}
	return rec, true, nil
}

func (q *ScaleLinkQuorum) Read(ctx context.Context, taskID string) (ScaleLinkRecord, bool, error) {
	ballot := q.nextBallot(ctx, taskID)
	reads, prepared := q.prepare(ctx, taskID, ballot)
	if !satisfiesQuorumSets(prepared, q.sets) {
		return ScaleLinkRecord{}, false, ErrQuorum
	}
	best, found := highestScaleLink(reads)
	if found {
		best.Meta.Ballot = ballot
		accepted := q.accept(ctx, taskID, best, ballot, prepared)
		if !satisfiesQuorumSets(accepted, q.sets) {
			return ScaleLinkRecord{}, false, ErrQuorum
		}
		q.repairBestEffort(ctx, taskID, best)
	}
	return best, found, nil
}

func (q *ScaleLinkQuorum) CAS(ctx context.Context, taskID string, expectRev uint64, propose ScaleLinkProposal) (ScaleLinkRecord, error) {
	ballot := q.nextBallot(ctx, taskID)
	reads, prepared := q.prepare(ctx, taskID, ballot)
	if !satisfiesQuorumSets(prepared, q.sets) {
		return ScaleLinkRecord{}, ErrQuorum
	}
	cur, found := highestScaleLink(reads)
	if !matchRev(found, cur.Meta.Rev, expectRev) {
		return ScaleLinkRecord{}, ErrConflict
	}
	next, keep, err := propose(cur, found)
	if err != nil {
		return ScaleLinkRecord{}, err
	}
	if !keep {
		return ScaleLinkRecord{}, nil
	}
	if next.Key == "" {
		next.Key = taskID
	}
	next.Meta = RecordMeta{Ballot: ballot, Rev: cur.Meta.Rev + 1, UpdatedAt: time.Now()}
	accepted := q.accept(ctx, taskID, next, ballot, prepared)
	if !satisfiesQuorumSets(accepted, q.sets) {
		return ScaleLinkRecord{}, ErrQuorum
	}
	q.repairBestEffort(ctx, taskID, next)
	return next, nil
}

func (q *ScaleLinkQuorum) nextBallot(ctx context.Context, taskID string) Ballot {
	max := q.maxBallot(ctx, taskID)
	q.mu.Lock()
	defer q.mu.Unlock()
	if local := q.rounds[taskID]; max.Round < local {
		max.Round = local
	}
	next := max.Round + 1
	q.rounds[taskID] = next
	return Ballot{Round: next, Writer: q.writer}
}

func (q *ScaleLinkQuorum) maxBallot(ctx context.Context, taskID string) Ballot {
	type result struct {
		ballot Ballot
		err    error
	}
	ch := make(chan result, len(q.replicas))
	for _, r := range q.replicas {
		rep := r.replica
		go func() {
			b, err := rep.MaxBallot(ctx, taskID)
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

type scaleLinkRead struct {
	rec   ScaleLinkRecord
	found bool
}

func (q *ScaleLinkQuorum) prepare(ctx context.Context, taskID string, ballot Ballot) ([]scaleLinkRead, map[int]bool) {
	type result struct {
		idx   int
		rec   ScaleLinkRecord
		found bool
		ok    bool
		err   error
	}
	ch := make(chan result, len(q.replicas))
	for i, r := range q.replicas {
		idx, rep := i, r.replica
		go func() {
			rec, found, ok, err := rep.Prepare(ctx, taskID, ballot)
			ch <- result{idx: idx, rec: rec, found: found, ok: ok, err: err}
		}()
	}
	reads := make([]scaleLinkRead, 0, len(q.replicas))
	prepared := map[int]bool{}
	for range q.replicas {
		res := <-ch
		if res.err != nil || !res.ok {
			continue
		}
		prepared[res.idx] = true
		reads = append(reads, scaleLinkRead{rec: res.rec, found: res.found})
	}
	return reads, prepared
}

func (q *ScaleLinkQuorum) accept(ctx context.Context, taskID string, rec ScaleLinkRecord, ballot Ballot, prepared map[int]bool) map[int]bool {
	type result struct {
		idx int
		ok  bool
	}
	ch := make(chan result, len(prepared))
	for i := range prepared {
		idx, rep := i, q.replicas[i].replica
		go func() {
			ok, _ := rep.Accept(ctx, taskID, rec, ballot)
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

func (q *ScaleLinkQuorum) repairBestEffort(ctx context.Context, taskID string, rec ScaleLinkRecord) {
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
			_ = rep.Repair(repairCtx, taskID, rec)
		}()
	}
	wg.Wait()
}

type scaleLinkReplicaSlot struct {
	id      string
	replica ScaleLinkReplica
}

func buildScaleLinkQuorumSets(slots []ScaleLinkReplicaSlot, ownerSets [][]string) ([]scaleLinkReplicaSlot, []quorumSet) {
	reps := make([]scaleLinkReplicaSlot, 0, len(slots))
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
		reps = append(reps, scaleLinkReplicaSlot{id: id, replica: slot.Replica})
	}
	sets := make([]quorumSet, 0, len(ownerSets))
	for _, owners := range ownerSets {
		idxs := map[int]struct{}{}
		for _, id := range owners {
			if idx, ok := byID[id]; ok {
				idxs[idx] = struct{}{}
			}
		}
		set := make([]int, 0, len(idxs))
		for idx := range idxs {
			set = append(set, idx)
		}
		if len(set) > 0 {
			sets = append(sets, quorumSet{indices: set, quorum: len(set)/2 + 1})
		}
	}
	if len(sets) == 0 && len(reps) > 0 {
		set := make([]int, len(reps))
		for i := range reps {
			set[i] = i
		}
		sets = append(sets, quorumSet{indices: set, quorum: len(set)/2 + 1})
	}
	return reps, sets
}

func highestScaleLink(reads []scaleLinkRead) (ScaleLinkRecord, bool) {
	var best ScaleLinkRecord
	found := false
	for _, r := range reads {
		if !r.found {
			continue
		}
		if !found || best.Meta.Ballot.Less(r.rec.Meta.Ballot) ||
			(best.Meta.Ballot == r.rec.Meta.Ballot && best.Meta.Rev < r.rec.Meta.Rev) {
			best = cloneScaleLink(r.rec)
			found = true
		}
	}
	return best, found
}

type MemoryScaleLinkReplica struct {
	mu       sync.Mutex
	promised map[string]Ballot
	accepted map[string]ScaleLinkRecord
	onApply  func(ScaleLinkRecord)
}

func NewMemoryScaleLinkReplica() *MemoryScaleLinkReplica {
	return &MemoryScaleLinkReplica{promised: map[string]Ballot{}, accepted: map[string]ScaleLinkRecord{}}
}

func (r *MemoryScaleLinkReplica) SetApplyHook(fn func(ScaleLinkRecord)) {
	r.mu.Lock()
	r.onApply = fn
	r.mu.Unlock()
}

func (r *MemoryScaleLinkReplica) Read(ctx context.Context, taskID string) (ScaleLinkRecord, bool, error) {
	if err := ctx.Err(); err != nil {
		return ScaleLinkRecord{}, false, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	rec, found := r.accepted[taskID]
	return cloneScaleLink(rec), found, nil
}

func (r *MemoryScaleLinkReplica) Prepare(ctx context.Context, taskID string, ballot Ballot) (ScaleLinkRecord, bool, bool, error) {
	if err := ctx.Err(); err != nil {
		return ScaleLinkRecord{}, false, false, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if ballot.Less(r.promised[taskID]) {
		return ScaleLinkRecord{}, false, false, nil
	}
	r.promised[taskID] = ballot
	rec, found := r.accepted[taskID]
	return cloneScaleLink(rec), found, true, nil
}

func (r *MemoryScaleLinkReplica) Accept(ctx context.Context, taskID string, rec ScaleLinkRecord, ballot Ballot) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	r.mu.Lock()
	if ballot.Less(r.promised[taskID]) {
		r.mu.Unlock()
		return false, nil
	}
	if cur, found := r.accepted[taskID]; found && ballot.Less(cur.Meta.Ballot) {
		r.mu.Unlock()
		return false, nil
	}
	if rec.Key == "" {
		rec.Key = taskID
	}
	r.promised[taskID] = ballot
	r.accepted[taskID] = cloneScaleLink(rec)
	hook := r.onApply
	applied := cloneScaleLink(rec)
	r.mu.Unlock()
	if hook != nil {
		hook(applied)
	}
	return true, nil
}

func (r *MemoryScaleLinkReplica) Repair(ctx context.Context, taskID string, rec ScaleLinkRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.Lock()
	cur, found := r.accepted[taskID]
	if !found || cur.Meta.Ballot.Less(rec.Meta.Ballot) ||
		(cur.Meta.Ballot == rec.Meta.Ballot && cur.Meta.Rev < rec.Meta.Rev) {
		if rec.Key == "" {
			rec.Key = taskID
		}
		if r.promised[taskID].Less(rec.Meta.Ballot) {
			r.promised[taskID] = rec.Meta.Ballot
		}
		r.accepted[taskID] = cloneScaleLink(rec)
		hook := r.onApply
		applied := cloneScaleLink(rec)
		r.mu.Unlock()
		if hook != nil {
			hook(applied)
		}
		return nil
	} else if r.promised[taskID].Less(cur.Meta.Ballot) {
		r.promised[taskID] = cur.Meta.Ballot
	}
	r.mu.Unlock()
	return nil
}

func (r *MemoryScaleLinkReplica) MaxBallot(ctx context.Context, taskID string) (Ballot, error) {
	if err := ctx.Err(); err != nil {
		return Ballot{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	max := r.promised[taskID]
	if rec, found := r.accepted[taskID]; found && max.Less(rec.Meta.Ballot) {
		max = rec.Meta.Ballot
	}
	return max, nil
}

func cloneScaleLink(in ScaleLinkRecord) ScaleLinkRecord {
	out := in
	out.NodeIDs = append([]string(nil), in.NodeIDs...)
	return out
}
