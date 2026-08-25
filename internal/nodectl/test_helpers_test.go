package nodectl

import (
	"os"
	"testing"
)

// shortTestDir avoids embedding long test and subtest names in Unix socket
// paths, whose Linux sockaddr_un pathname limit is 107 bytes.
func shortTestDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Errorf("remove short test directory: %v", err)
		}
	})
	return dir
}

func installReservationForTest(t *testing.T, state *State, r Reservation) {
	t.Helper()
	if r.SandboxID == "" {
		r.SandboxID = "sid-" + r.Token
	}
	minimumCapacity := r.ReservationMemory
	if r.InitialBudget > minimumCapacity {
		minimumCapacity = r.InitialBudget
	}
	if r.ConfiguredAllocatable.MemoryBytes > minimumCapacity {
		minimumCapacity = r.ConfiguredAllocatable.MemoryBytes
	}
	if r.Capacity.MemoryBytes < minimumCapacity {
		r.Capacity.MemoryBytes = minimumCapacity
	}
	if r.Capacity.CPUMilli < r.ConfiguredAllocatable.CPUMilli {
		r.Capacity.CPUMilli = r.ConfiguredAllocatable.CPUMilli
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	copy := cloneReservation(&r)
	if err := state.insertLocked(&copy); err != nil {
		t.Fatal(err)
	}
}

func reservationForTest(t *testing.T, state *State, sid string) Reservation {
	t.Helper()
	state.mu.Lock()
	defer state.mu.Unlock()
	r := state.bySID[sid]
	if r == nil {
		t.Fatalf("reservation %q not found", sid)
	}
	return cloneReservation(r)
}

func reservationByTokenForTest(t *testing.T, state *State, token string) Reservation {
	t.Helper()
	state.mu.Lock()
	defer state.mu.Unlock()
	r := state.byTokenLocked(token)
	if r == nil {
		t.Fatalf("reservation token %q not found", token)
	}
	return cloneReservation(r)
}

func mutateReservationForTest(t *testing.T, state *State, sid string, fn func(*Reservation)) {
	t.Helper()
	state.mu.Lock()
	defer state.mu.Unlock()
	r := state.bySID[sid]
	if r == nil {
		t.Fatalf("reservation %q not found", sid)
	}
	if err := state.removeAggregatesLocked(r); err != nil {
		t.Fatal(err)
	}
	fn(r)
	if err := state.addAggregatesLocked(r); err != nil {
		t.Fatal(err)
	}
}
