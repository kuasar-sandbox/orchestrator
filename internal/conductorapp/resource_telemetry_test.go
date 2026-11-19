package conductorapp

import (
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/nodectl"
)

func TestResourceGroupsDoNotInventSandboxIdentityForNode(t *testing.T) {
	st := nodectl.NewState(8<<30, 8, 0, 0, nodectl.Watermarks{})
	groups := hostResourceGroups(st, time.Now())
	if len(groups) < 1 {
		t.Fatal("no node resource group")
	}
	if groups[0].Resource != nil {
		t.Fatalf("node metrics fabricated sandbox identity: %+v", groups[0].Resource)
	}
	if len(groups[0].Samples) == 0 {
		t.Fatal("no node resource samples")
	}
	for _, group := range groups {
		if group.Resource["sandbox.id"] != "" && group.Resource["sandbox.stable_id"] != "" {
			t.Fatal("stable id fabricated without domain identity")
		}
	}
}

func TestResourceGroupsPreserveGuestReservationWithHostCache(t *testing.T) {
	state := nodectl.NewState(8<<30, 8000, 0, 0, nodectl.Watermarks{})
	const guestMemory = uint64(256 << 20)
	const cacheMemory = uint64(64 << 20)
	observed := time.Now()
	err := state.InstallProvisional(nodectl.ProvisionalSpec{
		SandboxID: "cache-owner", RecoveryKey: "test-recovery",
		Capacity:          nodectl.Resources{MemoryBytes: 2 << 30, CPUMilli: 1000},
		ReservationMemory: guestMemory, InitialBudget: guestMemory,
		CacheReservation: cacheMemory, CacheMax: cacheMemory,
		ClientFeatures: []string{nodectl.FeatureCacheBudgetV1},
	})
	if err != nil {
		t.Fatal(err)
	}
	node, sandbox := map[string]float64{}, map[string]float64{}
	for _, group := range hostResourceGroups(state, observed) {
		target := node
		if group.Resource["sandbox.id"] == "cache-owner" {
			target = sandbox
		}
		for _, sample := range group.Samples {
			target[sample.Name] = sample.Value
			if !sample.Timestamp.Equal(observed) {
				t.Fatalf("observation timestamp changed: %v", sample.Timestamp)
			}
		}
	}
	if got := node["node.resource.memory.reserved"]; got != float64(guestMemory) {
		t.Fatalf("Guest pool includes host cache: got %v, want %v", got, guestMemory)
	}
	if node["node.cache.allocated"] != float64(cacheMemory) || node["node.cache.pool.allocated"] != float64(cacheMemory) || sandbox["sandbox.cache.allocated"] != float64(cacheMemory) {
		t.Fatal("independent cache observations missing")
	}
	if got := sandbox["sandbox.resource.memory.reserved"]; got != float64(guestMemory) {
		t.Fatalf("Guest reservation includes host cache: got %v, want %v", got, guestMemory)
	}
	if got := sandbox["sandbox.resource.cpu.reserved"]; got != 1 {
		t.Fatalf("startup CPU fallback: got %v, want 1 core", got)
	}
}
