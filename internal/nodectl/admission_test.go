package nodectl

import (
	"net"
	"testing"
	"time"
)

// helper: build a State with a single-host pool of size mem (bytes) and
// default factor values + a startup_factor of 0.5.
func newTestState(t *testing.T, mem uint64) *State {
	t.Helper()
	return NewState(mem, 8000, 0, 0, Watermarks{
		OperationalMarginFactor: 0,
		HighFactor:              0.85,
		LowFactor:               0.70,
		EmergencyFactor:         0.05,
		StartupFactor:           0.50,
	})
}

// helper: wire admission with no-op processFn (caller calls AnalyzeRequest
// directly; admit-builder used only by worker, which we don't drive in
// pure logic tests).
func newTestAdmission(t *testing.T, policy AdmissionPolicy, state *State) *AdmissionController {
	t.Helper()
	a := NewAdmissionController(policy)
	a.SetWiring(state, nil,
		func(string, ...any) {},
		func(p *PendingAdmit) (*Message, error) { return nil, nil },
	)
	return a
}

func TestAdmission_TokenBucket(t *testing.T) {
	state := newTestState(t, 16<<30) // 16 GiB, never the bottleneck
	a := newTestAdmission(t, AdmissionPolicy{
		Rate:          4,
		Burst:         2,
		QueueTTL:      time.Second,
		QueueMaxDepth: 16,
	}, state)

	req := &Message{
		SandboxID:           "sb-1",
		CapacityMemoryBytes: 256 << 20,
		FloorMemoryBytes:    64 << 20,
		StartupBudgetMemory: 64 << 20,
	}
	// Burst=2: first two AnalyzeRequest+ConsumeToken pairs admitted.
	for i := 0; i < 2; i++ {
		oc := a.AnalyzeRequest(req)
		if oc.Status != OutcomeAdmitted {
			t.Fatalf("admit %d: status=%d, want admitted", i+1, oc.Status)
		}
		if !a.ConsumeToken() {
			t.Fatalf("admit %d: ConsumeToken failed", i+1)
		}
	}
	// Third must block on token bucket.
	oc := a.AnalyzeRequest(req)
	if oc.Status != OutcomeShortTermBlock {
		t.Fatalf("admit 3: status=%d, want short-term-block", oc.Status)
	}
	if oc.Block != BlockedByTokenBucket {
		t.Errorf("admit 3: block reason=%d, want BlockedByTokenBucket", oc.Block)
	}
}

func TestAdmission_StartupPoolGate(t *testing.T) {
	// pool=10GiB, startup_factor=0.5 → startup_pool=5GiB. burst=3GiB per
	// sandbox → only one fits in startup_pool; second short-term-blocks.
	state := newTestState(t, 10<<30)
	a := newTestAdmission(t, AdmissionPolicy{
		Rate:          100,
		Burst:         100,
		QueueTTL:      time.Second,
		QueueMaxDepth: 16,
	}, state)

	req := &Message{
		SandboxID:           "sb-1",
		CapacityMemoryBytes: 4 << 30,
		FloorMemoryBytes:    1 << 30,
		StartupBudgetMemory: 3 << 30,
	}
	oc := a.AnalyzeRequest(req)
	if oc.Status != OutcomeAdmitted {
		t.Fatalf("admit 1: status=%d, want admitted", oc.Status)
	}
	a.ConsumeToken()

	// Simulate the State accepting the reservation so startup_in_flight
	// grows to reflect the just-admitted budget.
	installReservationForTest(t, state, Reservation{
		Token:                  "t1",
		SandboxID:              "sb-1",
		Capacity:               Resources{MemoryBytes: 4 << 30},
		Floor:                  Resources{MemoryBytes: 1 << 30},
		AllocatableNowMem:      3 << 30,
		EffectiveStartupBudget: 3 << 30,
		Stage:                  StageAdmitted,
	})

	// Second admit: startup_pool full (3GiB in flight, 5GiB cap, head=3GiB
	// fits 5-3=2GiB headroom → no fit).
	req2 := &Message{
		SandboxID:           "sb-2",
		CapacityMemoryBytes: 4 << 30,
		FloorMemoryBytes:    1 << 30,
		StartupBudgetMemory: 3 << 30,
	}
	oc = a.AnalyzeRequest(req2)
	if oc.Status != OutcomeShortTermBlock {
		t.Fatalf("admit 2: status=%d, want short-term-block", oc.Status)
	}
	if oc.Block != BlockedByStartupBudget {
		t.Errorf("admit 2: block reason=%d, want BlockedByStartupBudget", oc.Block)
	}

	// Transition res1 → settled releases startup_in_flight; second admit fits.
	mutateReservationForTest(t, state, "sb-1", func(r *Reservation) { r.Stage = StageSettled })
	oc = a.AnalyzeRequest(req2)
	if oc.Status != OutcomeAdmitted {
		t.Errorf("after settled, admit 2 status=%d, want admitted", oc.Status)
	}
}

func TestAdmission_DrainRejects(t *testing.T) {
	state := newTestState(t, 16<<30)
	a := newTestAdmission(t, AdmissionPolicy{
		Rate: 100, Burst: 100, QueueTTL: time.Second, QueueMaxDepth: 16,
	}, state)
	a.SetDrained(true)
	req := &Message{
		SandboxID: "sb-1", CapacityMemoryBytes: 256 << 20,
		FloorMemoryBytes: 64 << 20, StartupBudgetMemory: 64 << 20,
	}
	oc := a.AnalyzeRequest(req)
	if oc.Status != OutcomeLongTermReject {
		t.Errorf("drained: status=%d, want long-reject", oc.Status)
	}
	if oc.RejectCode != "drained" {
		t.Errorf("drained: reject code=%s, want drained", oc.RejectCode)
	}
}

func TestAdmission_PreCheckExceedsNode(t *testing.T) {
	state := newTestState(t, 1<<30) // 1 GiB pool
	a := newTestAdmission(t, AdmissionPolicy{
		Rate: 100, Burst: 100, QueueTTL: time.Second, QueueMaxDepth: 16,
	}, state)
	req := &Message{
		SandboxID:           "sb-huge",
		CapacityMemoryBytes: 8 << 30,
		FloorMemoryBytes:    8 << 30,
		StartupBudgetMemory: 8 << 30, // > pool
	}
	oc := a.AnalyzeRequest(req)
	if oc.Status != OutcomePreCheckReject {
		t.Errorf("exceeds_node: status=%d, want pre-check-reject", oc.Status)
	}
	if oc.RejectCode != "exceeds_node_capacity" {
		t.Errorf("exceeds_node: reject code=%s", oc.RejectCode)
	}
}

func TestAdmission_PreCheckExceedsStartupPool(t *testing.T) {
	state := newTestState(t, 10<<30) // pool=10G, startup_pool=5G
	a := newTestAdmission(t, AdmissionPolicy{
		Rate: 100, Burst: 100, QueueTTL: time.Second, QueueMaxDepth: 16,
	}, state)
	req := &Message{
		SandboxID:           "sb-big",
		CapacityMemoryBytes: 8 << 30,
		FloorMemoryBytes:    1 << 30,
		StartupBudgetMemory: 6 << 30, // > startup_pool=5G but < pool
	}
	oc := a.AnalyzeRequest(req)
	if oc.Status != OutcomePreCheckReject {
		t.Errorf("exceeds_startup: status=%d, want pre-check-reject", oc.Status)
	}
	if oc.RejectCode != "exceeds_startup_pool" {
		t.Errorf("exceeds_startup: reject code=%s", oc.RejectCode)
	}
}

func TestAdmission_EffectiveBudget_RespectsAllMaxes(t *testing.T) {
	cases := []struct {
		name                 string
		startup, floor, snap uint64
		want                 uint64
	}{
		{"cold burst dominates", 4 << 30, 1 << 30, 0, 4 << 30},
		{"cold floor dominates", 1 << 30, 4 << 30, 0, 4 << 30},
		{"restore snap dominates", 1 << 30, 1 << 30, 4 << 30, 4 << 30},
		{"restore burst over snap honored", 4 << 30, 1 << 30, 2 << 30, 4 << 30},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := computeEffectiveStartupBudget(&Message{
				StartupBudgetMemory:   c.startup,
				FloorMemoryBytes:      c.floor,
				AllocatableAtSnapshot: c.snap,
			})
			if got != c.want {
				t.Errorf("got %d, want %d", got, c.want)
			}
		})
	}
}

func TestAdmission_QueueEnqueueAndCancel(t *testing.T) {
	state := newTestState(t, 1<<30) // small pool — short-term will block
	a := newTestAdmission(t, AdmissionPolicy{
		Rate:          100,
		Burst:         100,
		QueueTTL:      200 * time.Millisecond,
		QueueMaxDepth: 4,
	}, state)
	// Force a short-term block by filling main pool with one big reservation.
	installReservationForTest(t, state, Reservation{
		Token: "filler", SandboxID: "filler",
		Capacity:               Resources{MemoryBytes: 1 << 30},
		Floor:                  Resources{MemoryBytes: 1 << 30},
		AllocatableNowMem:      900 << 20,
		EffectiveStartupBudget: 900 << 20,
		Stage:                  StageAdmitted,
	})

	req := &Message{
		SandboxID:           "sb-q",
		CapacityMemoryBytes: 256 << 20,
		FloorMemoryBytes:    64 << 20,
		StartupBudgetMemory: 200 << 20,
	}
	// pipe conn for client side disconnect simulation.
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	entry, ok := a.Enqueue(req, serverConn)
	if !ok {
		t.Fatal("Enqueue failed")
	}
	if a.QueueDepth() != 1 {
		t.Errorf("queue depth=%d, want 1", a.QueueDepth())
	}
	// Production cancels via TTL or worker WriteMessage failure; here
	// we invoke cancel() directly to drive the canceled-entry sweep.
	entry.cancel()
	select {
	case <-entry.cancelCh:
		// ok
	default:
		t.Error("cancelCh not closed after cancel()")
	}
}

func TestAdmission_QueueAtCap(t *testing.T) {
	state := newTestState(t, 1<<30)
	a := newTestAdmission(t, AdmissionPolicy{
		Rate: 100, Burst: 100, QueueTTL: time.Second, QueueMaxDepth: 2,
	}, state)
	c1, _ := net.Pipe()
	c2, _ := net.Pipe()
	c3, _ := net.Pipe()
	req := &Message{
		SandboxID:           "x",
		CapacityMemoryBytes: 256 << 20,
		FloorMemoryBytes:    64 << 20,
		StartupBudgetMemory: 64 << 20,
	}
	if _, ok := a.Enqueue(req, c1); !ok {
		t.Fatal("enqueue 1 failed")
	}
	if _, ok := a.Enqueue(req, c2); !ok {
		t.Fatal("enqueue 2 failed")
	}
	if _, ok := a.Enqueue(req, c3); ok {
		t.Error("enqueue 3 should have failed (queue at cap)")
	}
}

func TestNewToken_Format(t *testing.T) {
	t1 := NewToken()
	t2 := NewToken()
	if t1 == t2 {
		t.Error("two tokens should differ")
	}
	if len(t1) != 32 {
		t.Errorf("token length = %d, want 32 hex chars", len(t1))
	}
}
