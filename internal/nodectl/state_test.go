package nodectl

import (
	"testing"
	"time"
)

func makeState(physMem, hostMem uint64) *State {
	return NewState(physMem, 8000, hostMem, 1500, Resources{}, Watermarks{
		OperationalMarginFactor: 0.10,
		HighFactor:              0.85,
		LowFactor:               0.70,
		EmergencyFactor:         0.05,
	})
}

func TestNewState_DerivedPool(t *testing.T) {
	s := makeState(100<<30, 16<<30)
	// node_budget = 100 - 16 = 84 GiB
	if s.NodeBudget.MemoryBytes != 100<<30 {
		t.Errorf("NodeBudget.MemoryBytes = %d", s.NodeBudget.MemoryBytes)
	}
	wantBudgetAfterHost := uint64(84 << 30)
	gotBudget := s.NodeBudget.Sub(s.HostReserved).MemoryBytes
	if gotBudget != wantBudgetAfterHost {
		t.Errorf("budget after host = %d, want %d", gotBudget, wantBudgetAfterHost)
	}
	// margin = 10% of 84 GiB
	wantMargin := uint64(float64(wantBudgetAfterHost) * 0.10)
	if s.OperationalMargin.MemoryBytes != wantMargin {
		t.Errorf("OperationalMargin = %d, want %d", s.OperationalMargin.MemoryBytes, wantMargin)
	}
	// pool = budget - margin
	wantPool := wantBudgetAfterHost - wantMargin
	if s.AllocatablePool.MemoryBytes != wantPool {
		t.Errorf("AllocatablePool = %d, want %d", s.AllocatablePool.MemoryBytes, wantPool)
	}
}

func TestNewState_DeductsBuildReservationBeforeMargin(t *testing.T) {
	s := NewState(100<<30, 8000, 16<<30, 1500, Resources{MemoryBytes: 8 << 30}, Watermarks{
		OperationalMarginFactor: 0.10,
		HighFactor:              0.85,
		LowFactor:               0.70,
		EmergencyFactor:         0.05,
	})

	base := uint64(76 << 30)
	wantMargin := uint64(float64(base) * 0.10)
	if s.BuildReserved.MemoryBytes != 8<<30 {
		t.Fatalf("BuildReserved.MemoryBytes = %d, want %d", s.BuildReserved.MemoryBytes, uint64(8<<30))
	}
	if s.OperationalMargin.MemoryBytes != wantMargin {
		t.Fatalf("OperationalMargin.MemoryBytes = %d, want %d", s.OperationalMargin.MemoryBytes, wantMargin)
	}
	if want := base - wantMargin; s.AllocatablePool.MemoryBytes != want {
		t.Fatalf("AllocatablePool.MemoryBytes = %d, want %d", s.AllocatablePool.MemoryBytes, want)
	}
}

func TestMemoryZone(t *testing.T) {
	s := makeState(100<<30, 16<<30)
	pool := s.AllocatablePool.MemoryBytes

	// Empty: green
	s.Lock()
	defer s.Unlock()
	if z := s.MemoryZone(); z != ZoneGreen {
		t.Errorf("empty state zone = %s, want green", z)
	}

	// Add a reservation that pushes into yellow.
	s.Reservations["a"] = &Reservation{
		Token:             "a",
		AllocatableNowMem: uint64(float64(pool) * 0.75),
	}
	if z := s.MemoryZone(); z != ZoneYellow {
		t.Errorf("75%% allocated zone = %s, want yellow", z)
	}

	// Bump into red.
	s.Reservations["a"].AllocatableNowMem = uint64(float64(pool) * 0.90)
	if z := s.MemoryZone(); z != ZoneRed {
		t.Errorf("90%% allocated zone = %s, want red", z)
	}

	// Push into critical (within emergency_pool).
	s.Reservations["a"].AllocatableNowMem = uint64(float64(pool) * 0.97)
	if z := s.MemoryZone(); z != ZoneCritical {
		t.Errorf("97%% allocated zone = %s, want critical", z)
	}
}

func TestNodeAllocated_SumsReservations(t *testing.T) {
	s := makeState(100<<30, 16<<30)
	s.Lock()
	defer s.Unlock()

	s.Reservations["a"] = &Reservation{Token: "a", AllocatableNowMem: 1 << 30, Floor: Resources{CPUMilli: 100}}
	s.Reservations["b"] = &Reservation{Token: "b", AllocatableNowMem: 2 << 30, Floor: Resources{CPUMilli: 500}}

	got := s.NodeAllocated()
	if got.MemoryBytes != 3<<30 {
		t.Errorf("MemoryBytes = %d, want 3 GiB", got.MemoryBytes)
	}
	if got.CPUMilli != 600 {
		t.Errorf("CPUMilli = %d, want 600", got.CPUMilli)
	}
}

func TestInsertRemoveLookup(t *testing.T) {
	s := makeState(100<<30, 16<<30)
	s.Lock()
	defer s.Unlock()

	r := &Reservation{Token: "x", SandboxID: "sb-x", Stage: StageAdmitted, StageEnteredAt: time.Now()}
	if err := s.Insert(r); err != nil {
		t.Fatal(err)
	}
	if s.Lookup("x") != r {
		t.Error("Lookup did not return the inserted reservation")
	}
	if err := s.Insert(r); err == nil {
		t.Error("duplicate Insert should error")
	}
	s.Remove("x")
	if s.Lookup("x") != nil {
		t.Error("Lookup should return nil after Remove")
	}
}
