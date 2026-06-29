package cluster

import (
	"context"
	"reflect"
	"sync"
	"time"
)

type NodeListProposal func(current NodeListEntry, found bool) (NodeListEntry, bool, error)

type NodeListReplica interface {
	Read(ctx context.Context, nodeID string) (NodeListEntry, bool, error)
	Prepare(ctx context.Context, nodeID string, ballot Ballot) (NodeListEntry, bool, bool, error)
	Accept(ctx context.Context, nodeID string, rec NodeListEntry, ballot Ballot) (bool, error)
	Repair(ctx context.Context, nodeID string, rec NodeListEntry) error
	MaxBallot(ctx context.Context, nodeID string) (Ballot, error)
	List(ctx context.Context) ([]NodeListEntry, error)
}

type NodeListReplicaSlot struct {
	ID      string
	Replica NodeListReplica
}

type NodeListQuorum struct {
	writer   string
	replicas []nodeListReplicaSlot
	sets     []quorumSet
	mu       sync.Mutex
	rounds   map[string]uint64
}

func NewNodeListQuorum(writer string, replicas ...NodeListReplica) *NodeListQuorum {
	if writer == "" {
		writer = "local"
	}
	slots := make([]NodeListReplicaSlot, 0, len(replicas))
	owners := make([]string, 0, len(replicas))
	for i, rep := range replicas {
		id := replicaIndexID(i)
		slots = append(slots, NodeListReplicaSlot{ID: id, Replica: rep})
		owners = append(owners, id)
	}
	return NewNodeListJointQuorum(writer, slots, [][]string{owners})
}

func NewNodeListJointQuorum(writer string, slots []NodeListReplicaSlot, ownerSets [][]string) *NodeListQuorum {
	if writer == "" {
		writer = "local"
	}
	reps, sets := buildNodeListQuorumSets(slots, ownerSets)
	return &NodeListQuorum{writer: writer, replicas: reps, sets: sets, rounds: map[string]uint64{}}
}

func (q *NodeListQuorum) Get(ctx context.Context, nodeID string) (NodeListEntry, bool, error) {
	rec, found, err := q.Read(ctx, nodeID)
	if err != nil || !found || rec.Deleted {
		return NodeListEntry{}, false, err
	}
	return rec, true, nil
}

func (q *NodeListQuorum) Read(ctx context.Context, nodeID string) (NodeListEntry, bool, error) {
	ballot := q.nextBallot(ctx, nodeID)
	reads, prepared := q.prepare(ctx, nodeID, ballot)
	if !satisfiesQuorumSets(prepared, q.sets) {
		return NodeListEntry{}, false, ErrQuorum
	}
	best, found := highestNodeList(reads)
	if found {
		best.Meta.Ballot = ballot
		accepted := q.accept(ctx, nodeID, best, ballot, prepared)
		if !satisfiesQuorumSets(accepted, q.sets) {
			return NodeListEntry{}, false, ErrQuorum
		}
		q.repairBestEffort(ctx, nodeID, best)
	}
	return best, found, nil
}

func (q *NodeListQuorum) CAS(ctx context.Context, nodeID string, expectRev uint64, propose NodeListProposal) (NodeListEntry, error) {
	ballot := q.nextBallot(ctx, nodeID)
	reads, prepared := q.prepare(ctx, nodeID, ballot)
	if !satisfiesQuorumSets(prepared, q.sets) {
		return NodeListEntry{}, ErrQuorum
	}
	cur, found := highestNodeList(reads)
	if !matchRev(found, cur.Meta.Rev, expectRev) {
		return NodeListEntry{}, ErrConflict
	}
	next, keep, err := propose(cur, found)
	if err != nil {
		return NodeListEntry{}, err
	}
	if !keep {
		next = NodeListEntry{NodeID: nodeID, Deleted: true}
		if found {
			next.SourceMeta = cur.SourceMeta
		}
	}
	if next.NodeID == "" {
		next.NodeID = nodeID
	}
	next.Meta = RecordMeta{Ballot: ballot, Rev: cur.Meta.Rev + 1, UpdatedAt: time.Now()}
	accepted := q.accept(ctx, nodeID, next, ballot, prepared)
	if !satisfiesQuorumSets(accepted, q.sets) {
		return NodeListEntry{}, ErrQuorum
	}
	q.repairBestEffort(ctx, nodeID, next)
	return next, nil
}

func (q *NodeListQuorum) nextBallot(ctx context.Context, nodeID string) Ballot {
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

func (q *NodeListQuorum) maxBallot(ctx context.Context, nodeID string) Ballot {
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

func (q *NodeListQuorum) prepare(ctx context.Context, nodeID string, ballot Ballot) ([]nodeListRead, map[int]bool) {
	type result struct {
		idx   int
		rec   NodeListEntry
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
	reads := make([]nodeListRead, 0, len(q.replicas))
	prepared := map[int]bool{}
	for range q.replicas {
		res := <-ch
		if res.err != nil || !res.ok {
			continue
		}
		prepared[res.idx] = true
		reads = append(reads, nodeListRead{rec: res.rec, found: res.found})
	}
	return reads, prepared
}

func (q *NodeListQuorum) accept(ctx context.Context, nodeID string, rec NodeListEntry, ballot Ballot, prepared map[int]bool) map[int]bool {
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

func (q *NodeListQuorum) repairBestEffort(ctx context.Context, nodeID string, rec NodeListEntry) {
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

type nodeListReplicaSlot struct {
	id      string
	replica NodeListReplica
}

func buildNodeListQuorumSets(slots []NodeListReplicaSlot, ownerSets [][]string) ([]nodeListReplicaSlot, []quorumSet) {
	reps := make([]nodeListReplicaSlot, 0, len(slots))
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
		reps = append(reps, nodeListReplicaSlot{id: id, replica: slot.Replica})
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

type nodeListRead struct {
	rec   NodeListEntry
	found bool
}

func highestNodeList(reads []nodeListRead) (NodeListEntry, bool) {
	var best NodeListEntry
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

type MemoryNodeListReplica struct {
	mu       sync.Mutex
	promised map[string]Ballot
	accepted map[string]NodeListEntry
	onChange func(nodeID string, rec NodeListEntry)
}

func NewMemoryNodeListReplica() *MemoryNodeListReplica {
	return &MemoryNodeListReplica{promised: map[string]Ballot{}, accepted: map[string]NodeListEntry{}}
}

func (r *MemoryNodeListReplica) SetOnChange(fn func(nodeID string, rec NodeListEntry)) {
	r.mu.Lock()
	r.onChange = fn
	r.mu.Unlock()
}

func (r *MemoryNodeListReplica) Read(ctx context.Context, nodeID string) (NodeListEntry, bool, error) {
	if err := ctx.Err(); err != nil {
		return NodeListEntry{}, false, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	rec, found := r.accepted[nodeID]
	return cloneNodeList(rec), found, nil
}

func (r *MemoryNodeListReplica) Prepare(ctx context.Context, nodeID string, ballot Ballot) (NodeListEntry, bool, bool, error) {
	if err := ctx.Err(); err != nil {
		return NodeListEntry{}, false, false, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if ballot.Less(r.promised[nodeID]) {
		return NodeListEntry{}, false, false, nil
	}
	r.promised[nodeID] = ballot
	rec, found := r.accepted[nodeID]
	return cloneNodeList(rec), found, true, nil
}

func (r *MemoryNodeListReplica) Accept(ctx context.Context, nodeID string, rec NodeListEntry, ballot Ballot) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	r.mu.Lock()
	if ballot.Less(r.promised[nodeID]) {
		r.mu.Unlock()
		return false, nil
	}
	cur, found := r.accepted[nodeID]
	if found && ballot.Less(cur.Meta.Ballot) {
		r.mu.Unlock()
		return false, nil
	}
	if rec.NodeID == "" {
		rec.NodeID = nodeID
	}
	changed := !found || nodeListLogicalChanged(cur, rec)
	r.promised[nodeID] = ballot
	r.accepted[nodeID] = cloneNodeList(rec)
	onChange := r.onChange
	out := cloneNodeList(rec)
	r.mu.Unlock()
	if changed && onChange != nil {
		onChange(nodeID, out)
	}
	return true, nil
}

func (r *MemoryNodeListReplica) Repair(ctx context.Context, nodeID string, rec NodeListEntry) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.Lock()
	cur, found := r.accepted[nodeID]
	if !found || cur.Meta.Ballot.Less(rec.Meta.Ballot) ||
		(cur.Meta.Ballot == rec.Meta.Ballot && cur.Meta.Rev < rec.Meta.Rev) {
		if rec.NodeID == "" {
			rec.NodeID = nodeID
		}
		changed := !found || nodeListLogicalChanged(cur, rec)
		if r.promised[nodeID].Less(rec.Meta.Ballot) {
			r.promised[nodeID] = rec.Meta.Ballot
		}
		r.accepted[nodeID] = cloneNodeList(rec)
		onChange := r.onChange
		out := cloneNodeList(rec)
		r.mu.Unlock()
		if changed && onChange != nil {
			onChange(nodeID, out)
		}
		return nil
	}
	if r.promised[nodeID].Less(cur.Meta.Ballot) {
		r.promised[nodeID] = cur.Meta.Ballot
	}
	r.mu.Unlock()
	return nil
}

func (r *MemoryNodeListReplica) MaxBallot(ctx context.Context, nodeID string) (Ballot, error) {
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

func (r *MemoryNodeListReplica) List(ctx context.Context) ([]NodeListEntry, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]NodeListEntry, 0, len(r.accepted))
	for _, rec := range r.accepted {
		out = append(out, cloneNodeList(rec))
	}
	return out, nil
}

func nodeListLogicalChanged(a, b NodeListEntry) bool {
	a.Meta = RecordMeta{}
	b.Meta = RecordMeta{}
	return !reflect.DeepEqual(a, b)
}
