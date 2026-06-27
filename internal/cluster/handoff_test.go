package cluster

import (
	"context"
	"testing"
)

func TestSyncRouteHandoffCopiesHighestRecordToAddedOwners(t *testing.T) {
	ctx := context.Background()
	reps := RouteReplicaMap{
		"r1": NewMemoryRouteReplica(),
		"r2": NewMemoryRouteReplica(),
		"r3": NewMemoryRouteReplica(),
		"r4": NewMemoryRouteReplica(),
	}
	key := RouteKey("/g", "rk")
	old := RouteRecord{
		Meta:  RecordMeta{Ballot: Ballot{Round: 1, Writer: "r1"}, Rev: 1},
		Group: "/g", RouteKey: "rk", SandboxID: "old", State: RouteReserved,
	}
	best := RouteRecord{
		Meta:  RecordMeta{Ballot: Ballot{Round: 2, Writer: "r2"}, Rev: 2},
		Group: "/g", RouteKey: "rk", SandboxID: "best", State: RouteReady,
	}
	if ok, err := reps["r1"].Accept(ctx, key, old, old.Meta.Ballot); err != nil || !ok {
		t.Fatalf("accept old ok=%v err=%v", ok, err)
	}
	if ok, err := reps["r2"].Accept(ctx, key, best, best.Meta.Ballot); err != nil || !ok {
		t.Fatalf("accept best ok=%v err=%v", ok, err)
	}
	plan := HandoffPlan{Moves: []KeyMove{{
		Key: key, From: []string{"r1", "r2", "r3"}, To: []string{"r2", "r3", "r4"},
		Added: []string{"r4"}, Removed: []string{"r1"}, Stable: []string{"r2", "r3"}, Affected: true,
	}}}

	if err := SyncRouteHandoff(ctx, plan, reps); err != nil {
		t.Fatal(err)
	}
	got, found, err := reps["r4"].Read(ctx, key)
	if err != nil || !found {
		t.Fatalf("r4 found=%v err=%v", found, err)
	}
	if got.SandboxID != "best" || got.Meta.Ballot != best.Meta.Ballot {
		t.Fatalf("r4 got %+v, want %+v", got, best)
	}
}
