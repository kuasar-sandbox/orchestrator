package nodectl

import (
	"path/filepath"
	"testing"
	"time"
)

func TestActiveReclaimer_ShrinksOverAllocated(t *testing.T) {
	state := makeState(8<<30, 1<<30)
	persister := &Persister{Path: filepath.Join(t.TempDir(), "state.json")}

	// Pre-load a settled reservation with allocatable far above the
	// reported working set. The reclaimer should shrink it toward
	// rss * SafetyMargin = working set + 25%.
	installReservationForTest(t, state, Reservation{
		Token:             "a",
		SandboxID:         "sb-a",
		Stage:             StageSettled,
		StageEnteredAt:    time.Now().Add(-1 * time.Minute),
		LastReportedRSS:   200 << 20,
		AllocatableNowMem: 1 << 30,
		Floor:             Resources{MemoryBytes: 64 << 20},
		Capacity:          Resources{MemoryBytes: 4 << 30},
	})

	r := &ActiveReclaimer{
		State:        state,
		Persister:    persister,
		SafetyMargin: 1.25,
		Logf:         t.Logf,
	}
	r.sweep()

	got := reservationForTest(t, state, "sb-a").AllocatableNowMem
	want := uint64(float64(200<<20) * 1.25)
	if got != want {
		t.Errorf("after sweep alloc=%d, want %d", got, want)
	}
}

func TestActiveReclaimer_RespectsFloor(t *testing.T) {
	state := makeState(8<<30, 1<<30)
	persister := &Persister{Path: filepath.Join(t.TempDir(), "state.json")}

	installReservationForTest(t, state, Reservation{
		Token:             "b",
		SandboxID:         "sb-b",
		Stage:             StageSettled,
		LastReportedRSS:   1 << 20, // 1 MiB working set
		AllocatableNowMem: 1 << 30,
		Floor:             Resources{MemoryBytes: 128 << 20}, // 128 MiB floor
		Capacity:          Resources{MemoryBytes: 4 << 30},
	})

	r := &ActiveReclaimer{State: state, Persister: persister, SafetyMargin: 1.25, Logf: t.Logf}
	r.sweep()

	got := reservationForTest(t, state, "sb-b").AllocatableNowMem
	if got != 128<<20 {
		t.Errorf("alloc=%d, want floor 128 MiB", got)
	}
}

func TestActiveReclaimer_SkipsNonSettled(t *testing.T) {
	state := makeState(8<<30, 1<<30)
	persister := &Persister{Path: filepath.Join(t.TempDir(), "state.json")}

	installReservationForTest(t, state, Reservation{
		Token:             "c",
		SandboxID:         "sb-c",
		Stage:             StageStartup, // not settled yet
		LastReportedRSS:   100 << 20,
		AllocatableNowMem: 1 << 30,
		Floor:             Resources{MemoryBytes: 64 << 20},
		Capacity:          Resources{MemoryBytes: 4 << 30},
	})

	r := &ActiveReclaimer{State: state, Persister: persister, SafetyMargin: 1.25, Logf: t.Logf}
	r.sweep()

	got := reservationForTest(t, state, "sb-c").AllocatableNowMem
	if got != 1<<30 {
		t.Errorf("non-settled reservation should be untouched, got alloc=%d", got)
	}
}

func TestActiveReclaimer_TighterMarginInRedZone(t *testing.T) {
	state := makeState(2<<30, 256<<20) // small node so red zone is reachable
	persister := &Persister{Path: filepath.Join(t.TempDir(), "state.json")}

	pool := state.AllocatablePool.MemoryBytes
	// Two settled reservations totaling > 90% of pool to put node in red.
	installReservationForTest(t, state, Reservation{
		Token: "d", SandboxID: "sb-d", Stage: StageSettled,
		LastReportedRSS:   500 << 20,
		AllocatableNowMem: uint64(float64(pool) * 0.45),
		Floor:             Resources{MemoryBytes: 64 << 20},
		Capacity:          Resources{MemoryBytes: 2 << 30},
	})
	installReservationForTest(t, state, Reservation{
		Token: "e", SandboxID: "sb-e", Stage: StageSettled,
		LastReportedRSS:   500 << 20,
		AllocatableNowMem: uint64(float64(pool) * 0.50),
		Floor:             Resources{MemoryBytes: 64 << 20},
		Capacity:          Resources{MemoryBytes: 2 << 30},
	})

	r := &ActiveReclaimer{State: state, Persister: persister, SafetyMargin: 1.25, Logf: t.Logf}
	r.sweep()

	// In red zone the margin is 1.05 → target ≈ 525 MiB.
	for _, sid := range []string{"sb-d", "sb-e"} {
		got := reservationForTest(t, state, sid).AllocatableNowMem
		want := uint64(float64(500<<20) * 1.05)
		if got != want {
			t.Errorf("%s alloc=%d, want %d (red-zone tight margin)", sid, got, want)
		}
	}
}
