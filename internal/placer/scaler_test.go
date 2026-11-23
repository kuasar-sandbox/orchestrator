package placer

import (
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/clustercfg"
	"github.com/kuasar-sandbox/orchestrator/internal/registry"
)

// The placement algorithm is now a pure function over a node slice + a group's
// selectors (the standalone placer runs it over its subscribed view).

func nodes(ns ...*registry.NodeRecord) []*registry.NodeRecord { return ns }

func TestShuffleSharding(t *testing.T) {
	var ns []*registry.NodeRecord
	for _, slot := range []string{"s1", "s2", "s3", "s4"} {
		ns = append(ns, &registry.NodeRecord{NodeID: "n-" + slot, Labels: map[string]string{"pool": "p1", "slot": slot}})
	}
	rules := []clustercfg.ShuffleRule{{Selector: map[string]string{"pool": "p1"}, ShardBy: "slot", N: 2}}
	_, got := importTargets("/cell/proj/app/g1", ns, nil, rules)
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

func TestImportTargets(t *testing.T) {
	ns := nodes(
		&registry.NodeRecord{NodeID: "n-s1", Labels: map[string]string{"pool": "p1", "slot": "s1"}},
		&registry.NodeRecord{NodeID: "n-s2", Labels: map[string]string{"pool": "p1", "slot": "s2"}},
		&registry.NodeRecord{NodeID: "n-s3", Labels: map[string]string{"pool": "p1", "slot": "s3"}, Draining: true},
		&registry.NodeRecord{NodeID: "other", Labels: map[string]string{"pool": "p2", "slot": "s4"}},
	)
	rules := []clustercfg.ShuffleRule{{Selector: map[string]string{"pool": "p1"}, ShardBy: "slot", N: 2}}
	selectors, ids := importTargets("/cell/g1", ns, []map[string]string{{"pool": "p1"}}, rules)
	if len(selectors) != 2 {
		t.Fatalf("selectors=%v, want two shuffle-effective selectors", selectors)
	}
	for _, id := range ids {
		if id == "other" {
			t.Fatalf("selector patch included ineligible node %q: %v", id, ids)
		}
	}
	if len(ids) == 0 {
		t.Fatal("selector patch target set unexpectedly empty")
	}
	_, ids = importTargets("/g", ns, []map[string]string{{"pool": "p2"}}, nil)
	if len(ids) != 1 || ids[0] != "other" {
		t.Fatalf("static selector patch ids=%v, want [other]", ids)
	}
}

func TestImportTargetsIntersectStaticShardAndShuffle(t *testing.T) {
	ns := nodes(
		&registry.NodeRecord{NodeID: "one", Labels: map[string]string{"slot": "s1"}},
		&registry.NodeRecord{NodeID: "two", Labels: map[string]string{"slot": "s2"}},
	)
	rules := []clustercfg.ShuffleRule{{ShardBy: "slot", N: 1}}
	slots, _ := shuffleSlots("g", ns, rules)
	rejected := "s1"
	if slots[rejected] {
		rejected = "s2"
	}
	_, targets := importTargets("g", ns, []map[string]string{{"slot": rejected}}, rules)
	if len(targets) != 0 {
		t.Fatalf("conflicting static selector widened the shuffle set: %v", targets)
	}
}
