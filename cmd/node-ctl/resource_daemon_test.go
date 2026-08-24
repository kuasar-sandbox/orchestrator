package main

import (
	"math"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/nodectl"
)

func TestResourceProbeSaturatesConservativeRecoveryCharge(t *testing.T) {
	state := nodectl.NewState(math.MaxInt64, 1000, 0, 0, nodectl.Watermarks{})
	for _, sid := range []string{"unknown-a", "unknown-b"} {
		if err := state.InstallProvisional(nodectl.ProvisionalSpec{
			SandboxID: sid,
			Capacity: nodectl.Resources{
				MemoryBytes: 1 << 62,
			},
			ReservationMemory: 1 << 62,
			InitialBudget:     1 << 62,
			RecoverySource:    nodectl.RecoveryUnknownLease,
			RecoveryKey:       "unknown:" + sid,
		}); err != nil {
			t.Fatal(err)
		}
	}
	probe := resourceProbe{
		state:     state,
		admission: nodectl.NewAdmissionController(nodectl.AdmissionPolicy{}),
	}
	snapshot := probe.Snapshot()
	if snapshot.Allocated != math.MaxInt64 || snapshot.Pool != math.MaxInt64 {
		t.Fatalf("resource probe did not saturate safely: %+v", snapshot)
	}
}
