package cluster

import (
	"context"
	"errors"
	"testing"
)

func nodeListQuorumForTest(writer string, rs []*MemoryNodeListReplica) *NodeListQuorum {
	reps := make([]NodeListReplica, len(rs))
	for i, r := range rs {
		reps[i] = r
	}
	return NewNodeListQuorum(writer, reps...)
}

func TestNodeListQuorumCASReadRepairAndTombstone(t *testing.T) {
	ctx := context.Background()
	rs := []*MemoryNodeListReplica{NewMemoryNodeListReplica(), NewMemoryNodeListReplica(), NewMemoryNodeListReplica()}
	q := nodeListQuorumForTest("node-list", rs)
	rec, err := q.CAS(ctx, "n1", 0, func(NodeListEntry, bool) (NodeListEntry, bool, error) {
		return NodeListEntry{NodeID: "n1", Labels: map[string]string{"pool": "p"}}, true, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := q.CAS(ctx, "n1", rec.Meta.Rev, func(cur NodeListEntry, found bool) (NodeListEntry, bool, error) {
		if !found {
			t.Fatal("delete proposal did not see current node_list entry")
		}
		cur.Deleted = true
		return cur, true, nil
	}); err != nil {
		t.Fatal(err)
	}
	if got, found, err := q.Get(ctx, "n1"); err != nil || found {
		t.Fatalf("deleted node_list entry found=%v err=%v got=%+v", found, err, got)
	}
	for i, r := range rs {
		got, found, err := r.Read(ctx, "n1")
		if err != nil || !found || !got.Deleted {
			t.Fatalf("replica %d tombstone found=%v err=%v got=%+v", i, found, err, got)
		}
	}
}

func TestNodeListRepairRaisesPromiseAndRejectsLowerAccept(t *testing.T) {
	ctx := context.Background()
	rep := NewMemoryNodeListReplica()
	high := NodeListEntry{
		Meta:       RecordMeta{Ballot: Ballot{Round: 9, Writer: "repair"}, Rev: 3},
		SourceMeta: RecordMeta{Ballot: Ballot{Round: 5, Writer: "node"}, Rev: 5},
		NodeID:     "n1",
		Labels:     map[string]string{"pool": "high"},
	}
	if err := rep.Repair(ctx, "n1", high); err != nil {
		t.Fatal(err)
	}
	low := NodeListEntry{
		Meta:       RecordMeta{Ballot: Ballot{Round: 4, Writer: "late"}, Rev: 2},
		SourceMeta: RecordMeta{Ballot: Ballot{Round: 3, Writer: "node"}, Rev: 3},
		NodeID:     "n1",
		Labels:     map[string]string{"pool": "low"},
	}
	if ok, err := rep.Accept(ctx, "n1", low, low.Meta.Ballot); err != nil || ok {
		t.Fatalf("lower accept after repair ok=%v err=%v, want rejected", ok, err)
	}
	got, found, err := rep.Read(ctx, "n1")
	if err != nil || !found || got.Labels["pool"] != "high" || got.Meta.Ballot != high.Meta.Ballot {
		t.Fatalf("repair value was overwritten: found=%v err=%v got=%+v", found, err, got)
	}
}

func TestNodeListJointQuorumRequiresEveryOwnerSet(t *testing.T) {
	ctx := context.Background()
	a := NewMemoryNodeListReplica()
	b := NewMemoryNodeListReplica()
	c := NewMemoryNodeListReplica()
	d := unavailableNodeListReplica{}
	e := unavailableNodeListReplica{}
	q := NewNodeListJointQuorum("writer", []NodeListReplicaSlot{
		{ID: "a", Replica: a},
		{ID: "b", Replica: b},
		{ID: "c", Replica: c},
		{ID: "d", Replica: d},
		{ID: "e", Replica: e},
	}, [][]string{{"a", "b", "c"}, {"b", "d", "e"}})
	_, err := q.CAS(ctx, "n1", 0, func(NodeListEntry, bool) (NodeListEntry, bool, error) {
		return NodeListEntry{NodeID: "n1"}, true, nil
	})
	if !errors.Is(err, ErrQuorum) {
		t.Fatalf("joint write err=%v, want ErrQuorum when next set lacks quorum", err)
	}
}

type unavailableNodeListReplica struct{}

func (unavailableNodeListReplica) Read(context.Context, string) (NodeListEntry, bool, error) {
	return NodeListEntry{}, false, context.Canceled
}
func (unavailableNodeListReplica) Prepare(context.Context, string, Ballot) (NodeListEntry, bool, bool, error) {
	return NodeListEntry{}, false, false, context.Canceled
}
func (unavailableNodeListReplica) Accept(context.Context, string, NodeListEntry, Ballot) (bool, error) {
	return false, context.Canceled
}
func (unavailableNodeListReplica) Repair(context.Context, string, NodeListEntry) error {
	return context.Canceled
}
func (unavailableNodeListReplica) MaxBallot(context.Context, string) (Ballot, error) {
	return Ballot{}, context.Canceled
}
func (unavailableNodeListReplica) List(context.Context) ([]NodeListEntry, error) {
	return nil, context.Canceled
}
