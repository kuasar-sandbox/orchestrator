package cluster

import (
	"context"
	"errors"
	"sync"
	"testing"
)

func nodeQuorumForTest(writer string, rs []*MemoryNodeReplica) *NodeQuorum {
	reps := make([]NodeReplica, len(rs))
	for i, r := range rs {
		reps[i] = r
	}
	return NewNodeQuorum(writer, reps...)
}

func TestNodeQuorumCASAndConflict(t *testing.T) {
	ctx := context.Background()
	rs := []*MemoryNodeReplica{NewMemoryNodeReplica(), NewMemoryNodeReplica(), NewMemoryNodeReplica()}
	q := nodeQuorumForTest("node-owner", rs)
	rec, err := q.CAS(ctx, "n1", 0, func(NodeRecord, bool) (NodeRecord, bool, error) {
		return NodeRecord{NodeID: "n1", State: NodeLive, Labels: map[string]string{"zone": "east"}}, true, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if rec.Meta.Rev != 1 || rec.State != NodeLive {
		t.Fatalf("first node write = %+v", rec)
	}
	_, err = q.CAS(ctx, "n1", rec.Meta.Rev-1, func(NodeRecord, bool) (NodeRecord, bool, error) {
		return NodeRecord{NodeID: "n1", State: NodeDrained}, true, nil
	})
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("CAS stale rev err=%v, want ErrConflict", err)
	}
}

type flakyNodeReplica struct {
	NodeReplica
	down bool
}

func (r *flakyNodeReplica) Read(ctx context.Context, key string) (NodeRecord, bool, error) {
	if r.down {
		return NodeRecord{}, false, context.Canceled
	}
	return r.NodeReplica.Read(ctx, key)
}

func (r *flakyNodeReplica) Prepare(ctx context.Context, key string, ballot Ballot) (NodeRecord, bool, bool, error) {
	if r.down {
		return NodeRecord{}, false, false, context.Canceled
	}
	return r.NodeReplica.Prepare(ctx, key, ballot)
}

func (r *flakyNodeReplica) Accept(ctx context.Context, key string, rec NodeRecord, ballot Ballot) (bool, error) {
	if r.down {
		return false, context.Canceled
	}
	return r.NodeReplica.Accept(ctx, key, rec, ballot)
}

func (r *flakyNodeReplica) Repair(ctx context.Context, key string, rec NodeRecord) error {
	if r.down {
		return context.Canceled
	}
	return r.NodeReplica.Repair(ctx, key, rec)
}

func (r *flakyNodeReplica) MaxBallot(ctx context.Context, key string) (Ballot, error) {
	if r.down {
		return Ballot{}, context.Canceled
	}
	return r.NodeReplica.MaxBallot(ctx, key)
}

func (r *flakyNodeReplica) Keys(ctx context.Context) []string {
	if r.down {
		return nil
	}
	return r.NodeReplica.Keys(ctx)
}

func TestNodeQuorumMemberFailurePolicy(t *testing.T) {
	ctx := context.Background()
	a := &flakyNodeReplica{NodeReplica: NewMemoryNodeReplica()}
	b := &flakyNodeReplica{NodeReplica: NewMemoryNodeReplica()}
	c := &flakyNodeReplica{NodeReplica: NewMemoryNodeReplica()}
	q := NewNodeQuorum("writer", a, b, c)

	a.down = true
	if _, err := q.CAS(ctx, "n1", 0, func(NodeRecord, bool) (NodeRecord, bool, error) {
		return NodeRecord{NodeID: "n1", State: NodeLive}, true, nil
	}); err != nil {
		t.Fatalf("single member down should keep quorum: %v", err)
	}
	b.down = true
	if _, err := q.CAS(ctx, "n2", 0, func(NodeRecord, bool) (NodeRecord, bool, error) {
		return NodeRecord{NodeID: "n2", State: NodeLive}, true, nil
	}); !errors.Is(err, ErrQuorum) {
		t.Fatalf("two members down err=%v, want ErrQuorum", err)
	}
}

func TestNodeQuorumReadRepair(t *testing.T) {
	ctx := context.Background()
	rs := []*MemoryNodeReplica{NewMemoryNodeReplica(), NewMemoryNodeReplica(), NewMemoryNodeReplica()}
	partial := NodeRecord{
		Meta:   RecordMeta{Ballot: Ballot{Round: 5, Writer: "old"}, Rev: 2},
		NodeID: "n1", State: NodeLive, RuntimeDigest: "rt1",
	}
	if ok, err := rs[0].Accept(ctx, "n1", partial, partial.Meta.Ballot); err != nil || !ok {
		t.Fatalf("partial accept ok=%v err=%v", ok, err)
	}

	q := nodeQuorumForTest("reader", rs)
	got, found, err := q.Get(ctx, "n1")
	if err != nil || !found {
		t.Fatalf("Get found=%v err=%v", found, err)
	}
	if got.RuntimeDigest != "rt1" || got.Meta.Ballot.Writer != "reader" {
		t.Fatalf("Get = %+v", got)
	}
	for i, r := range rs {
		rr, found, err := r.Read(ctx, "n1")
		if err != nil || !found || rr.RuntimeDigest != "rt1" || rr.Meta.Ballot != got.Meta.Ballot {
			t.Fatalf("replica %d after repair found=%v err=%v rec=%+v", i, found, err, rr)
		}
	}
}

func TestNodeQuorumDeleteTombstonePreventsResurrection(t *testing.T) {
	ctx := context.Background()
	rs := []*MemoryNodeReplica{NewMemoryNodeReplica(), NewMemoryNodeReplica(), NewMemoryNodeReplica()}
	old := NodeRecord{
		Meta:   RecordMeta{Ballot: Ballot{Round: 1, Writer: "old"}, Rev: 1},
		NodeID: "n1", State: NodeLive,
	}
	for _, r := range rs {
		if ok, err := r.Accept(ctx, "n1", old, old.Meta.Ballot); err != nil || !ok {
			t.Fatalf("seed accept ok=%v err=%v", ok, err)
		}
	}
	delBallot := Ballot{Round: 2, Writer: "deleter"}
	for _, r := range rs[:2] {
		if ok, err := r.AcceptDelete(ctx, "n1", delBallot); err != nil || !ok {
			t.Fatalf("delete accept ok=%v err=%v", ok, err)
		}
	}

	q := nodeQuorumForTest("reader", rs)
	got, found, err := q.Get(ctx, "n1")
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatalf("deleted node resurrected as %+v", got)
	}
	for i, r := range rs {
		rec, found, err := r.Read(ctx, "n1")
		if err != nil || !found || rec.State != NodeDead {
			t.Fatalf("replica %d after repair found=%v err=%v rec=%+v, want tombstone", i, found, err, rec)
		}
	}
}

func TestNodeQuorumCASDeleteWritesUniformTombstone(t *testing.T) {
	ctx := context.Background()
	rs := []*MemoryNodeReplica{NewMemoryNodeReplica(), NewMemoryNodeReplica(), NewMemoryNodeReplica()}
	q := nodeQuorumForTest("writer", rs)
	rec, err := q.CAS(ctx, "n1", 0, func(NodeRecord, bool) (NodeRecord, bool, error) {
		return NodeRecord{NodeID: "n1", State: NodeLive}, true, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := q.CAS(ctx, "n1", rec.Meta.Rev, func(NodeRecord, bool) (NodeRecord, bool, error) {
		return NodeRecord{}, false, nil
	}); err != nil {
		t.Fatal(err)
	}
	var want RecordMeta
	for i, r := range rs {
		got, found, err := r.Read(ctx, "n1")
		if err != nil || !found || got.State != NodeDead {
			t.Fatalf("replica %d found=%v err=%v got=%+v, want tombstone", i, found, err, got)
		}
		if i == 0 {
			want = got.Meta
			continue
		}
		if got.Meta != want {
			t.Fatalf("replica %d tombstone meta=%+v, want %+v", i, got.Meta, want)
		}
	}
}

func TestNodeQuorumConcurrentSameWriterNextBallotsAreUnique(t *testing.T) {
	ctx := context.Background()
	rs := []*MemoryNodeReplica{NewMemoryNodeReplica(), NewMemoryNodeReplica(), NewMemoryNodeReplica()}
	q := nodeQuorumForTest("same-writer", rs)
	if _, err := q.CAS(ctx, "n1", 0, func(NodeRecord, bool) (NodeRecord, bool, error) {
		return NodeRecord{NodeID: "n1", State: NodeLive}, true, nil
	}); err != nil {
		t.Fatal(err)
	}
	const workers = 128
	ballots := make(chan Ballot, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ballots <- q.nextBallot(ctx, "n1")
		}()
	}
	wg.Wait()
	close(ballots)
	seen := map[Ballot]bool{}
	for b := range ballots {
		if seen[b] {
			t.Fatalf("duplicate ballot %+v", b)
		}
		seen[b] = true
	}
	if len(seen) != workers {
		t.Fatalf("ballots=%d, want %d", len(seen), workers)
	}
}
