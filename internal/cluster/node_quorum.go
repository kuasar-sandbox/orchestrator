package cluster

import (
	"context"
	"sync"
	"time"
)

type NodeProposal func(current NodeRecord, found bool) (NodeRecord, bool, error)

type NodeReplica interface {
	Read(ctx context.Context, nodeID string) (NodeRecord, bool, error)
	Prepare(ctx context.Context, nodeID string, ballot Ballot) (NodeRecord, bool, bool, error)
	Accept(ctx context.Context, nodeID string, rec NodeRecord, ballot Ballot) (bool, error)
	Repair(ctx context.Context, nodeID string, rec NodeRecord) error
	MaxBallot(ctx context.Context, nodeID string) (Ballot, error)
}

type NodeReplicaSlot struct {
	ID      string
	Replica NodeReplica
}

// NodeQuorum is the leaderless node_link write protocol over a node owner set.
// It mirrors RouteQuorum: unique ballots, prepare/accept, read-repair, and
// tombstones for deletes so a lagging owner cannot resurrect a removed node.
type NodeQuorum struct {
	writer   string
	replicas []nodeReplicaSlot
	sets     []quorumSet
	mu       sync.Mutex
	rounds   map[string]uint64
}

func NewNodeQuorum(writer string, replicas ...NodeReplica) *NodeQuorum {
	if writer == "" {
		writer = "local"
	}
	slots := make([]NodeReplicaSlot, 0, len(replicas))
	owners := make([]string, 0, len(replicas))
	for i, rep := range replicas {
		id := replicaIndexID(i)
		slots = append(slots, NodeReplicaSlot{ID: id, Replica: rep})
		owners = append(owners, id)
	}
	return NewNodeJointQuorum(writer, slots, [][]string{owners})
}

func NewNodeJointQuorum(writer string, slots []NodeReplicaSlot, ownerSets [][]string) *NodeQuorum {
	if writer == "" {
		writer = "local"
	}
	reps, sets := buildNodeQuorumSets(slots, ownerSets)
	return &NodeQuorum{writer: writer, replicas: reps, sets: sets, rounds: map[string]uint64{}}
}

func (q *NodeQuorum) Get(ctx context.Context, nodeID string) (NodeRecord, bool, error) {
	ballot := q.nextBallot(ctx, nodeID)
	reads, prepared := q.prepare(ctx, nodeID, ballot)
	if !satisfiesQuorumSets(prepared, q.sets) {
		return NodeRecord{}, false, ErrQuorum
	}
	best, found := highestNode(reads)
	if found {
		best.Meta.Ballot = ballot
		accepted := q.accept(ctx, nodeID, best, ballot, prepared)
		if !satisfiesQuorumSets(accepted, q.sets) {
			return NodeRecord{}, false, ErrQuorum
		}
		q.repairBestEffort(ctx, nodeID, best)
	}
	if found && best.State == NodeDead {
		return NodeRecord{}, false, nil
	}
	return best, found, nil
}

func (q *NodeQuorum) CAS(ctx context.Context, nodeID string, expectRev uint64, propose NodeProposal) (NodeRecord, error) {
	ballot := q.nextBallot(ctx, nodeID)
	reads, prepared := q.prepare(ctx, nodeID, ballot)
	if !satisfiesQuorumSets(prepared, q.sets) {
		return NodeRecord{}, ErrQuorum
	}
	cur, physicalFound := highestNode(reads)
	logicalFound := physicalFound && cur.State != NodeDead
	proposalCur := cur
	if !logicalFound {
		proposalCur = NodeRecord{}
	}
	if !matchRev(logicalFound, cur.Meta.Rev, expectRev) {
		return NodeRecord{}, ErrConflict
	}
	next, keep, err := propose(proposalCur, logicalFound)
	if err != nil {
		return NodeRecord{}, err
	}
	if !keep {
		if !physicalFound {
			return NodeRecord{}, nil
		}
		tombstone := NodeRecord{
			Meta:   RecordMeta{Ballot: ballot, Rev: cur.Meta.Rev + 1, UpdatedAt: time.Now()},
			NodeID: nodeID,
			State:  NodeDead,
		}
		accepted := q.accept(ctx, nodeID, tombstone, ballot, prepared)
		if !satisfiesQuorumSets(accepted, q.sets) {
			return NodeRecord{}, ErrQuorum
		}
		q.repairBestEffort(ctx, nodeID, tombstone)
		return NodeRecord{}, nil
	}
	next.Meta = RecordMeta{Ballot: ballot, Rev: cur.Meta.Rev + 1, UpdatedAt: time.Now()}
	accepted := q.accept(ctx, nodeID, next, ballot, prepared)
	if !satisfiesQuorumSets(accepted, q.sets) {
		return NodeRecord{}, ErrQuorum
	}
	q.repairBestEffort(ctx, nodeID, next)
	return next, nil
}

func (q *NodeQuorum) nextBallot(ctx context.Context, nodeID string) Ballot {
	max := q.maxBallot(ctx, nodeID)
	q.mu.Lock()
	defer q.mu.Unlock()
	if local := q.rounds[nodeID]; max.Round < local {
		max.Round = local
	}
	next := max.Round + 1
	q.rounds[nodeID] = next
	return Ballot{Round: next, Writer: q.writer}
}

func (q *NodeQuorum) maxBallot(ctx context.Context, nodeID string) Ballot {
	type result struct {
		ballot Ballot
		err    error
	}
	ch := make(chan result, len(q.replicas))
	for _, r := range q.replicas {
		rep := r.replica
		go func() {
			b, err := rep.MaxBallot(ctx, nodeID)
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

func (q *NodeQuorum) prepare(ctx context.Context, nodeID string, ballot Ballot) ([]nodeRead, map[int]bool) {
	type result struct {
		idx   int
		rec   NodeRecord
		found bool
		ok    bool
		err   error
	}
	ch := make(chan result, len(q.replicas))
	for i, r := range q.replicas {
		idx, rep := i, r.replica
		go func() {
			rec, found, ok, err := rep.Prepare(ctx, nodeID, ballot)
			ch <- result{idx: idx, rec: rec, found: found, ok: ok, err: err}
		}()
	}
	reads := make([]nodeRead, 0, len(q.replicas))
	prepared := map[int]bool{}
	for range q.replicas {
		res := <-ch
		if res.err != nil || !res.ok {
			continue
		}
		prepared[res.idx] = true
		reads = append(reads, nodeRead{rec: res.rec, found: res.found})
	}
	return reads, prepared
}

func (q *NodeQuorum) accept(ctx context.Context, nodeID string, rec NodeRecord, ballot Ballot, prepared map[int]bool) map[int]bool {
	type result struct {
		idx int
		ok  bool
	}
	ch := make(chan result, len(prepared))
	for i := range prepared {
		idx, rep := i, q.replicas[i].replica
		go func() {
			ok, _ := rep.Accept(ctx, nodeID, rec, ballot)
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

func (q *NodeQuorum) repairBestEffort(ctx context.Context, nodeID string, rec NodeRecord) {
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
			_ = rep.Repair(repairCtx, nodeID, rec)
		}()
	}
	wg.Wait()
}

type nodeReplicaSlot struct {
	id      string
	replica NodeReplica
}

func buildNodeQuorumSets(slots []NodeReplicaSlot, ownerSets [][]string) ([]nodeReplicaSlot, []quorumSet) {
	reps := make([]nodeReplicaSlot, 0, len(slots))
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
		reps = append(reps, nodeReplicaSlot{id: id, replica: slot.Replica})
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

type nodeRead struct {
	rec   NodeRecord
	found bool
}

func highestNode(reads []nodeRead) (NodeRecord, bool) {
	var best NodeRecord
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

type MemoryNodeReplica struct {
	mu       sync.Mutex
	promised map[string]Ballot
	accepted map[string]NodeRecord
}

func NewMemoryNodeReplica() *MemoryNodeReplica {
	return &MemoryNodeReplica{promised: map[string]Ballot{}, accepted: map[string]NodeRecord{}}
}

func (r *MemoryNodeReplica) Read(ctx context.Context, nodeID string) (NodeRecord, bool, error) {
	if err := ctx.Err(); err != nil {
		return NodeRecord{}, false, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	rec, found := r.accepted[nodeID]
	return cloneNode(rec), found, nil
}

func (r *MemoryNodeReplica) Prepare(ctx context.Context, nodeID string, ballot Ballot) (NodeRecord, bool, bool, error) {
	if err := ctx.Err(); err != nil {
		return NodeRecord{}, false, false, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if ballot.Less(r.promised[nodeID]) {
		return NodeRecord{}, false, false, nil
	}
	r.promised[nodeID] = ballot
	rec, found := r.accepted[nodeID]
	return cloneNode(rec), found, true, nil
}

func (r *MemoryNodeReplica) Accept(ctx context.Context, nodeID string, rec NodeRecord, ballot Ballot) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if ballot.Less(r.promised[nodeID]) {
		return false, nil
	}
	r.promised[nodeID] = ballot
	r.accepted[nodeID] = cloneNode(rec)
	return true, nil
}

func (r *MemoryNodeReplica) AcceptDelete(ctx context.Context, nodeID string, ballot Ballot) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if ballot.Less(r.promised[nodeID]) {
		return false, nil
	}
	r.promised[nodeID] = ballot
	cur, found := r.accepted[nodeID]
	tombstone := NodeRecord{
		Meta:   RecordMeta{Ballot: ballot, Rev: cur.Meta.Rev + 1, UpdatedAt: time.Now()},
		NodeID: nodeID,
		State:  NodeDead,
	}
	if !found {
		tombstone.Meta.Rev = 1
	}
	r.accepted[nodeID] = tombstone
	return true, nil
}

func (r *MemoryNodeReplica) Repair(ctx context.Context, nodeID string, rec NodeRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	cur, found := r.accepted[nodeID]
	if !found || cur.Meta.Ballot.Less(rec.Meta.Ballot) ||
		(cur.Meta.Ballot == rec.Meta.Ballot && cur.Meta.Rev < rec.Meta.Rev) {
		r.accepted[nodeID] = cloneNode(rec)
	}
	return nil
}

func (r *MemoryNodeReplica) MaxBallot(ctx context.Context, nodeID string) (Ballot, error) {
	if err := ctx.Err(); err != nil {
		return Ballot{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	max := r.promised[nodeID]
	if rec, found := r.accepted[nodeID]; found && max.Less(rec.Meta.Ballot) {
		max = rec.Meta.Ballot
	}
	return max, nil
}
