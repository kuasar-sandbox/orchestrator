package placement

import (
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/cluster"
)

type sequenceSource struct {
	values []int
	index  int
}

func (s *sequenceSource) IntN(n int) (int, error) {
	if s.index >= len(s.values) {
		return 0, errors.New("random sequence exhausted")
	}
	value := s.values[s.index]
	s.index++
	return value, nil
}

func TestPlaceSandboxNUsesOnlyStaticPolicyAndUniformSample(t *testing.T) {
	nodes := []CatalogNode{
		{NodeID: "n1", Labels: map[string]string{"pool": "a"}, FailureDomain: "z1", RuntimeDigest: "r1", Capabilities: map[string]bool{"kvm": true}, SandboxSlotCapacity: 10},
		{NodeID: "n2", Labels: map[string]string{"pool": "a"}, FailureDomain: "z2", RuntimeDigest: "r1", Capabilities: map[string]bool{"kvm": true}, SandboxSlotCapacity: 10},
		{NodeID: "n3", Labels: map[string]string{"pool": "a"}, FailureDomain: "z3", RuntimeDigest: "r1", Capabilities: map[string]bool{"kvm": true}, SandboxSlotCapacity: 10},
		{NodeID: "draining", Labels: map[string]string{"pool": "a"}, RuntimeDigest: "r1", Capabilities: map[string]bool{"kvm": true}, Draining: true, SandboxSlotCapacity: 10},
		{NodeID: "wrong-runtime", Labels: map[string]string{"pool": "a"}, RuntimeDigest: "r2", Capabilities: map[string]bool{"kvm": true}, SandboxSlotCapacity: 10},
		{NodeID: "unknown-capacity", Labels: map[string]string{"pool": "a"}, RuntimeDigest: "r1", Capabilities: map[string]bool{"kvm": true}},
	}
	policy := StaticPolicy{
		Selectors: []map[string]string{{"pool": "a"}}, TargetRuntimeDigest: "r1",
		RequiredCapabilities: []string{"kvm"},
	}
	got, err := PlaceSandboxN(nodes, SandboxDemand{SlotUnits: 1}, policy, 2, &sequenceSource{values: []int{2, 0}})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].NodeID != "n3" || got[1].NodeID != "n2" {
		t.Fatalf("candidates = %+v", got)
	}
	if got[0].NodeID == got[1].NodeID {
		t.Fatal("RandomN sampled with replacement")
	}
}

func TestPlaceBuildNRequiresEveryDemandedStableCapacity(t *testing.T) {
	nodes := []CatalogNode{
		{NodeID: "fit", BuildSlotCapacity: 2, BuildCPUCapacity: 2000, BuildMemoryCapacity: 4 << 30, BuildStorageCapacity: 20 << 30},
		{NodeID: "missing-memory", BuildSlotCapacity: 2, BuildCPUCapacity: 2000, BuildStorageCapacity: 20 << 30},
		{NodeID: "too-small", BuildSlotCapacity: 2, BuildCPUCapacity: 500, BuildMemoryCapacity: 4 << 30, BuildStorageCapacity: 20 << 30},
	}
	got, err := PlaceBuildN(nodes, BuildDemand{Slots: 1, CPU: 1000, Memory: 1 << 30, Storage: 10 << 30}, StaticPolicy{}, 4, &sequenceSource{values: []int{0}})
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"fit"}; !reflect.DeepEqual(candidateIDs(got), want) {
		t.Fatalf("candidates = %v, want %v", candidateIDs(got), want)
	}
}

func TestPlaceNHonorsShuffleAndFailureDomainFilters(t *testing.T) {
	nodes := []CatalogNode{
		{NodeID: "n1", FailureDomain: "z1", SandboxSlotCapacity: 1},
		{NodeID: "n2", FailureDomain: "z2", SandboxSlotCapacity: 1},
		{NodeID: "n3", FailureDomain: "z1", SandboxSlotCapacity: 1},
	}
	policy := StaticPolicy{
		AllowedNodeIDs:        map[string]struct{}{"n1": {}, "n2": {}},
		AllowedFailureDomains: map[string]struct{}{"z2": {}},
	}
	got, err := PlaceSandboxN(nodes, SandboxDemand{SlotUnits: 1}, policy, DefaultCandidateCount, &sequenceSource{values: []int{0}})
	if err != nil {
		t.Fatal(err)
	}
	if ids := candidateIDs(got); !reflect.DeepEqual(ids, []string{"n2"}) {
		t.Fatalf("candidates = %v", ids)
	}
}

func TestPlaceNRequiresSelectorKeyPresence(t *testing.T) {
	nodes := []CatalogNode{
		{NodeID: "missing", Labels: map[string]string{}, SandboxSlotCapacity: 1},
		{NodeID: "present", Labels: map[string]string{"dedicated": ""}, SandboxSlotCapacity: 1},
	}
	got, err := PlaceSandboxN(nodes, SandboxDemand{SlotUnits: 1}, StaticPolicy{
		Selectors: []map[string]string{{"dedicated": ""}},
	}, DefaultCandidateCount, &sequenceSource{values: []int{0}})
	if err != nil {
		t.Fatal(err)
	}
	if ids := candidateIDs(got); !reflect.DeepEqual(ids, []string{"present"}) {
		t.Fatalf("candidates = %v", ids)
	}
}

func TestPlaceNPreservesUnconstrainedRuntimePolicy(t *testing.T) {
	nodes := []CatalogNode{{NodeID: "n1", RuntimeDigest: "sampled-runtime", SandboxSlotCapacity: 1}}
	got, err := PlaceSandboxN(nodes, SandboxDemand{SlotUnits: 1}, StaticPolicy{}, 1, &sequenceSource{values: []int{0}})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].RuntimeDigest != "" {
		t.Fatalf("unconstrained candidate = %+v", got)
	}
	got, err = PlaceSandboxN(nodes, SandboxDemand{SlotUnits: 1}, StaticPolicy{TargetRuntimeDigest: "sampled-runtime"}, 1, &sequenceSource{values: []int{0}})
	if err != nil || len(got) != 1 || got[0].RuntimeDigest != "sampled-runtime" {
		t.Fatalf("constrained candidate = %+v, %v", got, err)
	}
}

func TestPlaceNRejectsInvalidRandomSource(t *testing.T) {
	nodes := []CatalogNode{{NodeID: "n1", SandboxSlotCapacity: 1}}
	if _, err := PlaceSandboxN(nodes, SandboxDemand{SlotUnits: 1}, StaticPolicy{}, 1, &sequenceSource{values: []int{1}}); err == nil {
		t.Fatal("out-of-range source was accepted")
	}
}

func TestPlaceNCapsCandidatesAtWorkflowProtocolLimit(t *testing.T) {
	nodes := make([]CatalogNode, cluster.MaxPlacementCandidates+2)
	values := make([]int, cluster.MaxPlacementCandidates)
	for index := range nodes {
		nodes[index] = CatalogNode{NodeID: fmt.Sprintf("n%d", index), SandboxSlotCapacity: 1}
	}
	got, err := PlaceSandboxN(
		nodes, SandboxDemand{SlotUnits: 1}, StaticPolicy{}, len(nodes), &sequenceSource{values: values},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != cluster.MaxPlacementCandidates {
		t.Fatalf("candidate count = %d, want %d", len(got), cluster.MaxPlacementCandidates)
	}
}

func candidateIDs(candidates []cluster.PlacementCandidate) []string {
	ids := make([]string, len(candidates))
	for index, candidate := range candidates {
		ids[index] = candidate.NodeID
	}
	return ids
}
