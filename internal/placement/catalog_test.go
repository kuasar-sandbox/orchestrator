package placement

import (
	"strings"
	"testing"
)

func TestCatalogSnapshotIsCanonicalAndTamperEvident(t *testing.T) {
	nodes := []CatalogNode{
		{NodeID: "node-b", SandboxSlotCapacity: 2, Labels: map[string]string{"zone": "b", "pool": "cpu"}},
		{NodeID: "node-a", SandboxSlotCapacity: 1, Labels: map[string]string{"pool": "cpu", "zone": "a"}},
	}
	first, err := NewCatalogSnapshot("cluster-1", "generation-1", 3, strings.Repeat("a", 64), 9, nodes)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewCatalogSnapshot("cluster-1", "generation-1", 3, strings.Repeat("a", 64), 9,
		[]CatalogNode{nodes[1], nodes[0]})
	if err != nil {
		t.Fatal(err)
	}
	if first.Reference != second.Reference || first.Nodes[0].NodeID != "node-a" {
		t.Fatalf("canonical snapshots differ: %+v %+v", first.Reference, second.Reference)
	}
	tampered := first.Clone()
	tampered.Nodes[0].SandboxSlotCapacity++
	if err := tampered.Validate(); err == nil {
		t.Fatal("tampered Node Catalog passed digest validation")
	}
}
