package main

import (
	"math"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/conductorapp"
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
	probe := conductorapp.NewResourceProbe(state, nodectl.NewAdmissionController(nodectl.AdmissionPolicy{}))
	snapshot := probe.Snapshot()
	if snapshot.Allocated != math.MaxInt64 || snapshot.Pool != math.MaxInt64 {
		t.Fatalf("resource probe did not saturate safely: %+v", snapshot)
	}
}

// Existing heartbeat/API production wiring must export the effective state,
// even though no live reservation alone explains the outstanding obligation.
func TestResourceProbeExportsEffectiveCriticalWithoutInventingReservation(t *testing.T) {
	s := nodectl.NewState(1000, 1000, 0, 0, nodectl.Watermarks{LowFactor: .7, HighFactor: .85, EmergencyFactor: .05, StartupFactor: .5})
	if err := s.ConfigurePressure(nodectl.DefaultPressurePolicy(), nodectl.PressureRecord{Zone: nodectl.ZoneCritical, Version: 1}); err != nil {
		t.Fatal(err)
	}
	s.ObserveObligation("paused", 1, "paused", false)
	p := conductorapp.NewResourceProbe(s, nodectl.NewAdmissionController(nodectl.AdmissionPolicy{})).Snapshot()
	if p.Zone != "critical" || p.Allocated != 0 || p.Pool != 1000 {
		t.Fatalf("node production snapshot=%+v", p)
	}
	if s.ResourceSnapshot().RawZone != nodectl.ZoneGreen {
		t.Fatal("effective critical fabricated reservation pressure")
	}
}
