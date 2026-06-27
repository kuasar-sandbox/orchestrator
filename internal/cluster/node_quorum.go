package cluster

import (
	"context"
	"sort"
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
	Keys(ctx context.Context) []string
}

// NodeQuorum is the leaderless node_link write protocol over a node owner set.
// It mirrors RouteQuorum: unique ballots, prepare/accept, read-repair, and
// tombstones for deletes so a lagging owner cannot resurrect a removed node.
type NodeQuorum struct {
	writer   string
	replicas []NodeReplica
	mu       sync.Mutex
	rounds   map[string]uint64
}

func NewNodeQuorum(writer string, replicas ...NodeReplica) *NodeQuorum {
	if writer == "" {
		writer = "local"
	}
	return &NodeQuorum{writer: writer, replicas: replicas, rounds: map[string]uint64{}}
}

func (q *NodeQuorum) quorum() int { return len(q.replicas)/2 + 1 }

func (q *NodeQuorum) Get(ctx context.Context, nodeID string) (NodeRecord, bool, error) {
	ballot := q.nextBallot(ctx, nodeID)
	reads := make([]nodeRead, 0, len(q.replicas))
	prepared := make([]NodeReplica, 0, len(q.replicas))
	for _, r := range q.replicas {
		rec, found, ok, err := r.Prepare(ctx, nodeID, ballot)
		if err != nil || !ok {
			continue
		}
		prepared = append(prepared, r)
		reads = append(reads, nodeRead{rec: rec, found: found})
	}
	if len(prepared) < q.quorum() {
		return NodeRecord{}, false, ErrQuorum
	}
	best, found := highestNode(reads)
	if found {
		best.Meta.Ballot = ballot
		accepted := 0
		for _, r := range prepared {
			if ok, _ := r.Accept(ctx, nodeID, best, ballot); ok {
				accepted++
			}
		}
		if accepted < q.quorum() {
			return NodeRecord{}, false, ErrQuorum
		}
		for _, r := range q.replicas {
			_ = r.Repair(ctx, nodeID, best)
		}
	}
	if found && best.State == NodeDead {
		return NodeRecord{}, false, nil
	}
	return best, found, nil
}

func (q *NodeQuorum) List(ctx context.Context, fn func(NodeRecord) error) error {
	keys := map[string]bool{}
	for _, r := range q.replicas {
		for _, key := range r.Keys(ctx) {
			keys[key] = true
		}
	}
	ordered := make([]string, 0, len(keys))
	for key := range keys {
		ordered = append(ordered, key)
	}
	sort.Strings(ordered)
	for _, key := range ordered {
		rec, found, err := q.Get(ctx, key)
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

func (q *NodeQuorum) CAS(ctx context.Context, nodeID string, expectRev uint64, propose NodeProposal) (NodeRecord, error) {
	ballot := q.nextBallot(ctx, nodeID)
	reads := make([]nodeRead, 0, len(q.replicas))
	prepared := make([]NodeReplica, 0, len(q.replicas))
	for _, r := range q.replicas {
		rec, found, ok, err := r.Prepare(ctx, nodeID, ballot)
		if err != nil || !ok {
			continue
		}
		prepared = append(prepared, r)
		reads = append(reads, nodeRead{rec: rec, found: found})
	}
	if len(prepared) < q.quorum() {
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
		accepted := 0
		for _, r := range prepared {
			if ok, _ := r.Accept(ctx, nodeID, tombstone, ballot); ok {
				accepted++
			}
		}
		if accepted < q.quorum() {
			return NodeRecord{}, ErrQuorum
		}
		for _, r := range q.replicas {
			_ = r.Repair(ctx, nodeID, tombstone)
		}
		return NodeRecord{}, nil
	}
	next.Meta = RecordMeta{Ballot: ballot, Rev: cur.Meta.Rev + 1, UpdatedAt: time.Now()}
	accepted := 0
	for _, r := range prepared {
		if ok, _ := r.Accept(ctx, nodeID, next, ballot); ok {
			accepted++
		}
	}
	if accepted < q.quorum() {
		return NodeRecord{}, ErrQuorum
	}
	for _, r := range q.replicas {
		_ = r.Repair(ctx, nodeID, next)
	}
	return next, nil
}

func (q *NodeQuorum) nextBallot(ctx context.Context, nodeID string) Ballot {
	var max Ballot
	for _, r := range q.replicas {
		if b, err := r.MaxBallot(ctx, nodeID); err == nil && max.Less(b) {
			max = b
		}
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if local := q.rounds[nodeID]; max.Round < local {
		max.Round = local
	}
	next := max.Round + 1
	q.rounds[nodeID] = next
	return Ballot{Round: next, Writer: q.writer}
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

func (r *MemoryNodeReplica) Keys(ctx context.Context) []string {
	if err := ctx.Err(); err != nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	keys := make([]string, 0, len(r.accepted))
	for key, rec := range r.accepted {
		if rec.State != NodeDead {
			keys = append(keys, key)
		}
	}
	return keys
}
