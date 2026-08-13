package nodectl

import "testing"

func installReservationForTest(t *testing.T, state *State, r Reservation) {
	t.Helper()
	if r.SandboxID == "" {
		r.SandboxID = "sid-" + r.Token
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
	state.removeAggregatesLocked(r)
	fn(r)
	state.addAggregatesLocked(r)
}
