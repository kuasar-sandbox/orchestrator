package scaler

import (
	"testing"

	"github.com/kuasar-sandbox/sandbox-accelerator/pkg/maglev"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/clustercfg"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/registry"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/routesync"
)

// The placement algorithm is now a pure function over a node slice + a group's
// selectors (the standalone scaler runs it over its subscribed view).

func nodes(ns ...*registry.NodeRecord) []*registry.NodeRecord { return ns }

func TestPlaceRespectsSelectors(t *testing.T) {
	ns := nodes(
		&registry.NodeRecord{NodeID: "a", Labels: map[string]string{"zone": "east"}, Counts: 5},
		&registry.NodeRecord{NodeID: "b", Labels: map[string]string{"zone": "east"}, Counts: 1},
		&registry.NodeRecord{NodeID: "c", Labels: map[string]string{"zone": "west"}},
	)
	sel := []map[string]string{{"zone": "east"}}
	for i := 0; i < 30; i++ {
		node, err := placeSandbox(placeParams{group: "/g", nodes: ns, selectors: sel, candidates: 2})
		if err != nil || node == "c" {
			t.Fatalf("placed %q err=%v (must stay in east)", node, err)
		}
	}
}

func TestPlaceNoEligible(t *testing.T) {
	ns := nodes(&registry.NodeRecord{NodeID: "a", Labels: map[string]string{"zone": "west"}})
	if _, err := placeSandbox(placeParams{group: "/g", nodes: ns, selectors: []map[string]string{{"zone": "east"}}}); err != registry.ErrNoNode {
		t.Fatalf("want ErrNoNode, got %v", err)
	}
}

func TestPlaceDraining(t *testing.T) {
	ns := nodes(&registry.NodeRecord{NodeID: "a", Draining: true})
	if _, err := placeSandbox(placeParams{group: "/g", nodes: ns}); err != registry.ErrNoNode {
		t.Fatalf("want ErrNoNode (only node draining), got %v", err)
	}
}

func TestPlaceZoneAndAlive(t *testing.T) {
	// A red node is excluded when zone_admit_max=yellow; a stale node is excluded.
	ns := nodes(
		&registry.NodeRecord{NodeID: "red", Zone: "red", Counts: 0},
		&registry.NodeRecord{NodeID: "ok", Zone: "yellow", Counts: 9, LastHeartbeatUnix: 1000},
		&registry.NodeRecord{NodeID: "stale", Zone: "green", Counts: 0, LastHeartbeatUnix: 100},
	)
	for i := 0; i < 30; i++ {
		node, err := placeSandbox(placeParams{group: "/g", nodes: ns, candidates: 3, zoneAdmitMax: "yellow", deadAfter: 30, now: 1000})
		if err != nil || node != "ok" {
			t.Fatalf("placed %q err=%v (want ok: red zone + stale excluded)", node, err)
		}
	}
}

func TestPlaceWatermarkLoad(t *testing.T) {
	// Ranking prefers the lower allocated/pool water level over raw count.
	ns := nodes(
		&registry.NodeRecord{NodeID: "hot", Allocated: 9, Pool: 10, Counts: 0},
		&registry.NodeRecord{NodeID: "cool", Allocated: 1, Pool: 10, Counts: 100},
	)
	for i := 0; i < 500; i++ {
		node, err := placeSandbox(placeParams{group: "/g", nodes: ns, candidates: 2})
		if err != nil || node != "cool" {
			t.Fatalf("placed %q (want cool: lower water level despite higher count)", node)
		}
	}
}

func TestP2CNoReplacementAvoidsHot(t *testing.T) {
	ns := nodes(
		&registry.NodeRecord{NodeID: "hot", Counts: 100},
		&registry.NodeRecord{NodeID: "cold", Counts: 0},
	)
	for i := 0; i < 2000; i++ {
		node, err := placeSandbox(placeParams{group: "/g", nodes: ns, candidates: 2})
		if err != nil || node == "hot" {
			t.Fatalf("P2C(k=2) picked %q; sampling-with-replacement regression", node)
		}
	}
}

func TestShuffleSharding(t *testing.T) {
	var ns []*registry.NodeRecord
	for _, slot := range []string{"s1", "s2", "s3", "s4"} {
		ns = append(ns, &registry.NodeRecord{NodeID: "n-" + slot, Labels: map[string]string{"pool": "p1", "slot": slot}})
	}
	rules := []clustercfg.ShuffleRule{{Selector: map[string]string{"pool": "p1"}, ShardBy: "slot", N: 2}}
	got := map[string]bool{}
	for i := 0; i < 40; i++ {
		node, err := placeSandbox(placeParams{group: "/cell/proj/app/g1", nodes: ns, rules: rules, candidates: 2})
		if err != nil {
			t.Fatal(err)
		}
		got[node] = true
	}
	if len(got) != 2 {
		t.Fatalf("group landed on %d nodes, want 2 (shuffle pins to N slots): %v", len(got), got)
	}
}

func TestEffectiveSelectors(t *testing.T) {
	var ns []*registry.NodeRecord
	for _, slot := range []string{"s1", "s2", "s3", "s4"} {
		ns = append(ns, &registry.NodeRecord{NodeID: "n-" + slot, Labels: map[string]string{"pool": "p1", "slot": slot}})
	}
	rules := []clustercfg.ShuffleRule{{Selector: map[string]string{"pool": "p1"}, ShardBy: "slot", N: 2}}
	eff, ok := effectiveSelectors("/cell/g1", ns, []map[string]string{{"pool": "p1"}}, rules)
	if !ok || len(eff) != 2 {
		t.Fatalf("effective selectors: ok=%v %v (want 2 = N pinned slots)", ok, eff)
	}
	// Each effective selector carries the static label + a pinned slot.
	for _, sel := range eff {
		if sel["pool"] != "p1" || sel["slot"] == "" {
			t.Fatalf("effective selector %v missing pool/slot narrowing", sel)
		}
	}
	// No shuffle rule -> no overlay; selector patch uses the static selectors.
	if _, ok := effectiveSelectors("/g", ns, nil, nil); ok {
		t.Fatal("no shuffle rule should yield no overlay")
	}
}

func TestSelectorPatchTargets(t *testing.T) {
	ns := nodes(
		&registry.NodeRecord{NodeID: "n-s1", Labels: map[string]string{"pool": "p1", "slot": "s1"}},
		&registry.NodeRecord{NodeID: "n-s2", Labels: map[string]string{"pool": "p1", "slot": "s2"}},
		&registry.NodeRecord{NodeID: "n-s3", Labels: map[string]string{"pool": "p1", "slot": "s3"}, Draining: true},
		&registry.NodeRecord{NodeID: "other", Labels: map[string]string{"pool": "p2", "slot": "s4"}},
	)
	rules := []clustercfg.ShuffleRule{{Selector: map[string]string{"pool": "p1"}, ShardBy: "slot", N: 2}}
	selectors, ids := selectorPatchTargets("/cell/g1", ns, []map[string]string{{"pool": "p1"}}, rules)
	if len(selectors) != 2 {
		t.Fatalf("selectors=%v, want two shuffle-effective selectors", selectors)
	}
	for _, id := range ids {
		if id == "n-s3" || id == "other" {
			t.Fatalf("selector patch included ineligible node %q: %v", id, ids)
		}
	}
	if len(ids) == 0 {
		t.Fatal("selector patch target set unexpectedly empty")
	}
	_, ids = selectorPatchTargets("/g", ns, []map[string]string{{"pool": "p2"}}, nil)
	if len(ids) != 1 || ids[0] != "other" {
		t.Fatalf("static selector patch ids=%v, want [other]", ids)
	}
}

func TestPlaceBuildHeadroom(t *testing.T) {
	// A node with no build headroom (alloc==capacity CPU) is excluded.
	ns := nodes(
		&registry.NodeRecord{NodeID: "full", BuildCapacity: &routesync.BuildResources{CPU: 2000}, BuildAlloc: &routesync.BuildResources{CPU: 2000}},
		&registry.NodeRecord{NodeID: "free", BuildCapacity: &routesync.BuildResources{CPU: 2000}, BuildAlloc: &routesync.BuildResources{CPU: 0}},
	)
	for i := 0; i < 30; i++ {
		node, err := placeBuild(placeParams{group: "/g", nodes: ns, candidates: 2})
		if err != nil || node != "free" {
			t.Fatalf("placeBuild = %q err=%v (want free: full node has no build headroom)", node, err)
		}
	}
}

// guard against an accidental maglev import drop (shuffle relies on it).
var _ = maglev.LocateN
