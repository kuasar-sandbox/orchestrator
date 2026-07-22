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
	state.Lock()
	state.Reservations["a"] = &Reservation{
		Token:             "a",
		SandboxID:         "sb-a",
		Stage:             StageSettled,
		StageEnteredAt:    time.Now().Add(-1 * time.Minute),
		LastReportedRSS:   200 << 20,
		AllocatableNowMem: 1 << 30,
		Floor:             Resources{MemoryBytes: 64 << 20},
		Capacity:          Resources{MemoryBytes: 4 << 30},
	}
	state.Unlock()

	r := &ActiveReclaimer{
		State:        state,
		Persister:    persister,
		SafetyMargin: 1.25,
		Logf:         t.Logf,
	}
	r.sweep()

	state.Lock()
	got := state.Reservations["a"].AllocatableNowMem
	state.Unlock()
	want := uint64(float64(200<<20) * 1.25)
	if got != want {
		t.Errorf("after sweep alloc=%d, want %d", got, want)
	}
}

func TestActiveReclaimerWakesPreparedAdmissionsAfterDurableReclaim(t *testing.T) {
	state := preparedTestState()
	controller := preparedTestController(t, state, filepath.Join(t.TempDir(), "state.json"), 8)
	state.Lock()
	state.Reservations["running"] = &Reservation{
		Token: "running", SandboxID: "running", Stage: StageSettled,
		LastReportedRSS: 256 << 20, AllocatableNowMem: 2 << 30,
		Floor: Resources{MemoryBytes: 128 << 20}, Capacity: Resources{MemoryBytes: 4 << 30},
	}
	state.Unlock()
	for len(controller.Wake()) > 0 {
		<-controller.Wake()
	}

	reclaimer := &ActiveReclaimer{
		State: state, Persister: controller.persister, PreparedAdmission: controller,
		SafetyMargin: 1.25, Logf: t.Logf,
	}
	reclaimer.sweep()
	select {
	case <-controller.Wake():
	default:
		t.Fatal("durable reclaim did not wake prepared admissions")
	}
}

func TestAdminReclaimWakesPreparedAdmissionsAfterDurableReclaim(t *testing.T) {
	state := preparedTestState()
	controller := preparedTestController(t, state, filepath.Join(t.TempDir(), "state.json"), 8)
	state.Lock()
	state.Reservations["running"] = &Reservation{
		Token: "running", SandboxID: "running", Stage: StageSettled,
		AllocatableNowMem: 2 << 30, Floor: Resources{MemoryBytes: 128 << 20},
		Capacity: Resources{MemoryBytes: 4 << 30},
	}
	state.Unlock()
	for len(controller.Wake()) > 0 {
		<-controller.Wake()
	}
	server := &Server{
		State: state, Persister: controller.persister, PreparedAdmission: controller, Logf: t.Logf,
	}
	response := server.handleAdminReclaim(&Message{SandboxID: "running", TargetAllocatable: 1 << 30})
	if response.Type != TypeAck {
		t.Fatalf("admin reclaim = %+v", response)
	}
	select {
	case <-controller.Wake():
	default:
		t.Fatal("durable admin reclaim did not wake prepared admissions")
	}
}

func TestActiveReclaimer_RespectsFloor(t *testing.T) {
	state := makeState(8<<30, 1<<30)
	persister := &Persister{Path: filepath.Join(t.TempDir(), "state.json")}

	state.Lock()
	state.Reservations["b"] = &Reservation{
		Token:             "b",
		SandboxID:         "sb-b",
		Stage:             StageSettled,
		LastReportedRSS:   1 << 20, // 1 MiB working set
		AllocatableNowMem: 1 << 30,
		Floor:             Resources{MemoryBytes: 128 << 20}, // 128 MiB floor
		Capacity:          Resources{MemoryBytes: 4 << 30},
	}
	state.Unlock()

	r := &ActiveReclaimer{State: state, Persister: persister, SafetyMargin: 1.25, Logf: t.Logf}
	r.sweep()

	state.Lock()
	got := state.Reservations["b"].AllocatableNowMem
	state.Unlock()
	if got != 128<<20 {
		t.Errorf("alloc=%d, want floor 128 MiB", got)
	}
}

func TestActiveReclaimer_SkipsNonSettled(t *testing.T) {
	state := makeState(8<<30, 1<<30)
	persister := &Persister{Path: filepath.Join(t.TempDir(), "state.json")}

	state.Lock()
	state.Reservations["c"] = &Reservation{
		Token:             "c",
		SandboxID:         "sb-c",
		Stage:             StageStartup, // not settled yet
		LastReportedRSS:   100 << 20,
		AllocatableNowMem: 1 << 30,
		Floor:             Resources{MemoryBytes: 64 << 20},
		Capacity:          Resources{MemoryBytes: 4 << 30},
	}
	state.Unlock()

	r := &ActiveReclaimer{State: state, Persister: persister, SafetyMargin: 1.25, Logf: t.Logf}
	r.sweep()

	state.Lock()
	got := state.Reservations["c"].AllocatableNowMem
	state.Unlock()
	if got != 1<<30 {
		t.Errorf("non-settled reservation should be untouched, got alloc=%d", got)
	}
}

func TestActiveReclaimer_TighterMarginInRedZone(t *testing.T) {
	state := makeState(2<<30, 256<<20) // small node so red zone is reachable
	persister := &Persister{Path: filepath.Join(t.TempDir(), "state.json")}

	pool := state.AllocatablePool.MemoryBytes
	state.Lock()
	// Two settled reservations totaling > 90% of pool to put node in red.
	state.Reservations["d"] = &Reservation{
		Token: "d", SandboxID: "sb-d", Stage: StageSettled,
		LastReportedRSS:   500 << 20,
		AllocatableNowMem: uint64(float64(pool) * 0.45),
		Floor:             Resources{MemoryBytes: 64 << 20},
		Capacity:          Resources{MemoryBytes: 2 << 30},
	}
	state.Reservations["e"] = &Reservation{
		Token: "e", SandboxID: "sb-e", Stage: StageSettled,
		LastReportedRSS:   500 << 20,
		AllocatableNowMem: uint64(float64(pool) * 0.50),
		Floor:             Resources{MemoryBytes: 64 << 20},
		Capacity:          Resources{MemoryBytes: 2 << 30},
	}
	state.Unlock()

	r := &ActiveReclaimer{State: state, Persister: persister, SafetyMargin: 1.25, Logf: t.Logf}
	r.sweep()

	// In red zone the margin is 1.05 → target ≈ 525 MiB.
	state.Lock()
	defer state.Unlock()
	for _, k := range []string{"d", "e"} {
		got := state.Reservations[k].AllocatableNowMem
		want := uint64(float64(500<<20) * 1.05)
		if got != want {
			t.Errorf("%s alloc=%d, want %d (red-zone tight margin)", k, got, want)
		}
	}
}
