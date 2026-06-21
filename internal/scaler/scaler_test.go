package scaler

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/clustercfg"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/clusterstore"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/registry"
)

func testStores(t *testing.T) *registry.Stores {
	t.Helper()
	kv, err := clusterstore.Open(filepath.Join(t.TempDir(), "s.db"), 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { kv.Close() })
	return registry.NewStores(kv, nil)
}

func TestPlaceRespectsSelectors(t *testing.T) {
	ctx := context.Background()
	s := testStores(t)
	s.PutNode(ctx, &registry.NodeRecord{NodeID: "a", Labels: map[string]string{"zone": "east"}, Counts: 5})
	s.PutNode(ctx, &registry.NodeRecord{NodeID: "b", Labels: map[string]string{"zone": "east"}, Counts: 1})
	s.PutNode(ctx, &registry.NodeRecord{NodeID: "c", Labels: map[string]string{"zone": "west"}})
	s.PutGroup(ctx, &registry.GroupConfig{Group: "/g", NodeSelectors: []map[string]string{{"zone": "east"}}})
	p := New(s, clustercfg.ScalerConfig{PlaceCandidates: 2})
	for i := 0; i < 30; i++ {
		node, err := p.PlaceSandbox(ctx, "/g", "rk")
		if err != nil {
			t.Fatal(err)
		}
		if node == "c" {
			t.Fatal("placed on west node despite the east-only selector")
		}
	}
}

func TestPlaceNoEligible(t *testing.T) {
	ctx := context.Background()
	s := testStores(t)
	s.PutNode(ctx, &registry.NodeRecord{NodeID: "a", Labels: map[string]string{"zone": "west"}})
	s.PutGroup(ctx, &registry.GroupConfig{Group: "/g", NodeSelectors: []map[string]string{{"zone": "east"}}})
	p := New(s, clustercfg.ScalerConfig{})
	if _, err := p.PlaceSandbox(ctx, "/g", "rk"); err == nil {
		t.Fatal("expected ErrNoNode for no matching node")
	}
}

func TestPlaceDraining(t *testing.T) {
	ctx := context.Background()
	s := testStores(t)
	s.PutNode(ctx, &registry.NodeRecord{NodeID: "a", Draining: true})
	s.PutGroup(ctx, &registry.GroupConfig{Group: "/g"})
	p := New(s, clustercfg.ScalerConfig{})
	if _, err := p.PlaceSandbox(ctx, "/g", "rk"); err == nil {
		t.Fatal("expected ErrNoNode: the only node is draining")
	}
}

func TestP2CNoReplacementAvoidsHot(t *testing.T) {
	ctx := context.Background()
	s := testStores(t)
	s.PutNode(ctx, &registry.NodeRecord{NodeID: "hot", Counts: 100})
	s.PutNode(ctx, &registry.NodeRecord{NodeID: "cold", Counts: 0})
	s.PutGroup(ctx, &registry.GroupConfig{Group: "/g"})
	p := New(s, clustercfg.ScalerConfig{PlaceCandidates: 2})
	// k=2 over exactly 2 eligible nodes, sampled WITHOUT replacement, must always
	// pick the colder node (with replacement it picks the hot one ~25% of the time).
	for i := 0; i < 2000; i++ {
		node, err := p.PlaceSandbox(ctx, "/g", "rk")
		if err != nil {
			t.Fatal(err)
		}
		if node == "hot" {
			t.Fatal("P2C(k=2) picked the hot node; sampling-with-replacement regression")
		}
	}
}

func TestShuffleSharding(t *testing.T) {
	ctx := context.Background()
	s := testStores(t)
	for _, slot := range []string{"s1", "s2", "s3", "s4"} {
		s.PutNode(ctx, &registry.NodeRecord{NodeID: "n-" + slot, Labels: map[string]string{"pool": "p1", "slot": slot}})
	}
	s.PutGroup(ctx, &registry.GroupConfig{Group: "/cell/proj/app/g1"})
	p := New(s, clustercfg.ScalerConfig{PlaceCandidates: 2, ShuffleSharding: []clustercfg.ShuffleRule{
		{Selector: map[string]string{"pool": "p1"}, ShardBy: "slot", N: 2},
	}})
	got := map[string]bool{}
	for i := 0; i < 40; i++ {
		node, err := p.PlaceSandbox(ctx, "/cell/proj/app/g1", "rk")
		if err != nil {
			t.Fatal(err)
		}
		got[node] = true
	}
	// The group is deterministically pinned to exactly N=2 of the 4 slots.
	if len(got) != 2 {
		t.Fatalf("group landed on %d nodes, want 2 (shuffle-sharding pins to N slots): %v", len(got), got)
	}
}
