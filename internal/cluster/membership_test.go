package cluster

import (
	"context"
	"testing"
	"time"
)

func TestPlanHandoffOnlyReportsAffectedKeys(t *testing.T) {
	ctx := context.Background()
	from := MemberView{Version: 1, Members: []string{"r1", "r2", "r3"}}
	to := MemberView{Version: 2, Members: []string{"r1", "r2", "r3", "r4"}}
	keys := []string{"/g/a", "/g/b", "/g/c", "/g/d", "/g/e"}
	plan, err := PlanHandoff(ctx, from, to, 3, keys)
	if err != nil {
		t.Fatal(err)
	}
	if plan.FromVersion != 1 || plan.ToVersion != 2 || plan.OwnerCount != 3 {
		t.Fatalf("plan header = %+v", plan)
	}
	reported := map[string]bool{}
	for _, move := range plan.Moves {
		reported[move.Key] = true
		if !move.Affected {
			t.Fatalf("reported unaffected move: %+v", move)
		}
		if len(move.From) != 3 || len(move.To) != 3 {
			t.Fatalf("owner count not capped to 3: %+v", move)
		}
		for _, added := range move.Added {
			if added == "" || contains(move.From, added) {
				t.Fatalf("bad added owner in %+v", move)
			}
		}
		for _, removed := range move.Removed {
			if removed == "" || contains(move.To, removed) {
				t.Fatalf("bad removed owner in %+v", move)
			}
		}
	}
	for _, key := range keys {
		if reported[key] {
			continue
		}
		oldOwners, _ := from.Owners(key, 3)
		newOwners, _ := to.Owners(key, 3)
		if !sameStrings(oldOwners, newOwners) {
			t.Fatalf("unreported affected key %q: old=%v new=%v", key, oldOwners, newOwners)
		}
	}
}

func TestPlanHandoffRemoveMember(t *testing.T) {
	ctx := context.Background()
	from := MemberView{Version: 1, Members: []string{"r1", "r2", "r3", "r4"}}
	to := MemberView{Version: 2, Members: []string{"r1", "r2", "r3"}}
	plan, err := PlanHandoff(ctx, from, to, 3, []string{"/g/a", "/g/b", "/g/c", "/g/d"})
	if err != nil {
		t.Fatal(err)
	}
	for _, move := range plan.Moves {
		for _, removed := range move.Removed {
			if removed == "r4" {
				return
			}
		}
	}
	t.Fatalf("remove-member plan did not include leaving member in removed owners: %+v", plan)
}

func TestHandoffGateMinimizesMembershipFlipImpact(t *testing.T) {
	now := time.Unix(100, 0)
	move := KeyMove{
		Key: RouteKey("/g", "rk"), From: []string{"r1", "r2", "r3"}, To: []string{"r2", "r3", "r4"},
		Added: []string{"r4"}, Removed: []string{"r1"}, Stable: []string{"r2", "r3"}, Affected: true,
	}
	gate := HandoffGate{Move: move, FromVersion: 1, ToVersion: 2, Phase: HandoffCatchup}
	if d := gate.DecideWrite(1, now); !d.Allow || d.Version != 1 {
		t.Fatalf("catchup old-version decision = %+v, want allow v1", d)
	}
	if d := gate.DecideWrite(2, now); !d.Retry || d.Allow {
		t.Fatalf("catchup new-version decision = %+v, want retry", d)
	}

	gate.Phase = HandoffSwitching
	if d := gate.DecideWrite(1, now); !d.Retry || d.Allow || d.Moved {
		t.Fatalf("switching old-version decision = %+v, want retry barrier", d)
	}
	if d := gate.DecideWrite(2, now); !d.Retry || d.Allow || d.Moved {
		t.Fatalf("switching new-version decision = %+v, want retry barrier", d)
	}

	gate.Phase = HandoffOldGrace
	gate.GraceUntil = now.Add(time.Minute)
	if d := gate.DecideWrite(2, now); !d.Allow || d.Version != 2 {
		t.Fatalf("old-grace new-version decision = %+v, want allow v2", d)
	}
	if d := gate.DecideWrite(1, now); !d.Moved || d.Version != 2 || !sameStrings(d.NewOwners, move.To) {
		t.Fatalf("old-grace old-version decision = %+v, want moved to v2 owners", d)
	}
}

func contains(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
