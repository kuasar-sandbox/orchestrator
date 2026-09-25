package nodectl

import (
	"bytes"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"sync"
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
		Token:                 "t1",
		SandboxID:             "sb-1",
		Capacity:              Resources{MemoryBytes: 4 << 30},
		ConfiguredAllocatable: Resources{MemoryBytes: 1 << 30},
		ReservationMemory:     3 << 30,
		InitialBudget:         3 << 30,
		Stage:                 StageAdmitted,
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

func TestAdmission_SelectsColdOrRestoreInitialBudget(t *testing.T) {
	cases := []struct {
		name                 string
		startup, floor, snap uint64
		want                 uint64
	}{
		{"cold startup only", 4 << 30, 1 << 30, 0, 4 << 30},
		{"cold startup may be below headroom", 1 << 30, 4 << 30, 0, 1 << 30},
		{"restore snapshot only", 1 << 30, 4 << 30, 2 << 30, 2 << 30},
		{"restore ignores larger startup", 4 << 30, 1 << 30, 2 << 30, 2 << 30},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := initialReservationBudget(&Message{
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

func TestAdmissionRejectsInvalidMemoryContract(t *testing.T) {
	state := newTestState(t, 16<<30)
	a := newTestAdmission(t, AdmissionPolicy{
		Rate: 100, Burst: 100, QueueTTL: time.Second, QueueMaxDepth: 16,
	}, state)
	valid := Message{
		SandboxID: "valid", CapacityMemoryBytes: 1 << 30,
		FloorMemoryBytes: 256 << 20, StartupBudgetMemory: 1 << 30,
	}
	for name, mutate := range map[string]func(*Message){
		"zero capacity":           func(r *Message) { r.CapacityMemoryBytes = 0 },
		"zero headroom":           func(r *Message) { r.FloorMemoryBytes = 0 },
		"headroom above capacity": func(r *Message) { r.FloorMemoryBytes = 2 << 30 },
		"zero startup":            func(r *Message) { r.StartupBudgetMemory = 0 },
		"startup above capacity":  func(r *Message) { r.StartupBudgetMemory = 2 << 30 },
		"snapshot above capacity": func(r *Message) { r.AllocatableAtSnapshot = 2 << 30 },
	} {
		t.Run(name, func(t *testing.T) {
			req := valid
			mutate(&req)
			outcome := a.AnalyzeRequest(&req)
			if outcome.Status != OutcomePreCheckReject || outcome.RejectCode != "invalid_resource_contract" {
				t.Fatalf("outcome = %+v", outcome)
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
		Capacity:              Resources{MemoryBytes: 1 << 30},
		ConfiguredAllocatable: Resources{MemoryBytes: 1 << 30},
		ReservationMemory:     900 << 20,
		InitialBudget:         900 << 20,
		Stage:                 StageAdmitted,
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

	entry, ok := a.Enqueue(req, serverConn, 4242)
	if !ok {
		t.Fatal("Enqueue failed")
	}
	if a.QueueDepth() != 1 {
		t.Errorf("queue depth=%d, want 1", a.QueueDepth())
	}
	if entry.peerPID != 4242 {
		t.Fatalf("queued peer PID = %d, want 4242", entry.peerPID)
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
	if _, ok := a.Enqueue(req, c1, 0); !ok {
		t.Fatal("enqueue 1 failed")
	}
	if _, ok := a.Enqueue(req, c2, 0); !ok {
		t.Fatal("enqueue 2 failed")
	}
	if _, ok := a.Enqueue(req, c3, 0); ok {
		t.Error("enqueue 3 should have failed (queue at cap)")
	}
}

type queueLockCheckingWriter struct {
	queueMu          *sync.Mutex
	buf              bytes.Buffer
	wroteWhileLocked bool
}

func (w *queueLockCheckingWriter) Write(p []byte) (int, error) {
	if !w.queueMu.TryLock() {
		w.wroteWhileLocked = true
	} else {
		w.queueMu.Unlock()
	}
	return w.buf.Write(p)
}

func TestAdmission_QueuedBuildFailureLogsDiagnostic(t *testing.T) {
	state := newTestState(t, 8<<30)
	a := NewAdmissionController(AdmissionPolicy{
		Rate: 100, Burst: 100, QueueTTL: time.Second, QueueMaxDepth: 4,
	})
	logs := &queueLockCheckingWriter{queueMu: &a.queueMu}
	a.SetWiring(state, slog.New(slog.NewJSONHandler(logs, nil)), func(*PendingAdmit) (*Message, error) {
		return nil, errors.New("build failed")
	})
	client, server := net.Pipe()
	defer server.Close()
	req := &Message{
		SandboxID: "queued-build-failure", CapacityMemoryBytes: 512 << 20,
		FloorMemoryBytes: 128 << 20, StartupBudgetMemory: 256 << 20,
	}
	if _, ok := a.Enqueue(req, server, 4242); !ok {
		t.Fatal("enqueue failed")
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	a.processQueue()
	if logs.wroteWhileLocked {
		t.Fatal("queue diagnostic was written while queueMu was held")
	}
	var record map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(logs.buf.Bytes()), &record); err != nil {
		t.Fatalf("decode queue diagnostic: %v: %s", err, logs.buf.String())
	}
	if record["level"] != "WARN" || record["event"] != "admit_queue_build_failed" ||
		record["sandbox_id"] != req.SandboxID || record["error"] != "build failed" {
		t.Fatalf("queue diagnostic = %v", record)
	}
}

// TestAdmission_QueueFullLogsDiagnostic verifies that an enqueue rejected at
// queue_max_depth emits an explicit admit_queue_full diagnostic, after the
// queue critical section is released. Regression for the silently-vanishing
// admits observed at N>queue_max_depth (issue #265): the client received a
// queue_full rejection but the server-side trail had no trace of the drop.
func TestAdmission_QueueFullLogsDiagnostic(t *testing.T) {
	state := newTestState(t, 1<<30)
	a := NewAdmissionController(AdmissionPolicy{
		Rate: 100, Burst: 100, QueueTTL: time.Hour, QueueMaxDepth: 1,
	})
	logs := &queueLockCheckingWriter{queueMu: &a.queueMu}
	a.SetWiring(state, slog.New(slog.NewJSONHandler(logs, nil)), func(*PendingAdmit) (*Message, error) {
		return nil, nil
	})
	req := &Message{
		SandboxID:           "sb-overflow",
		CapacityMemoryBytes: 256 << 20,
		FloorMemoryBytes:    64 << 20,
		StartupBudgetMemory: 64 << 20,
	}
	c1Client, c1Server := net.Pipe()
	defer c1Client.Close()
	defer c1Server.Close()
	c2Client, c2Server := net.Pipe()
	defer c2Client.Close()
	defer c2Server.Close()

	entry1, ok := a.Enqueue(req, c1Server, 0)
	if !ok {
		t.Fatal("enqueue 1 failed")
	}
	defer entry1.ttlTimer.Stop()

	if _, ok := a.Enqueue(req, c2Server, 0); ok {
		t.Fatal("enqueue 2 should have failed (queue at cap)")
	}
	if logs.wroteWhileLocked {
		t.Fatal("queue diagnostic was written while queueMu was held")
	}
	var record map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(logs.buf.Bytes()), &record); err != nil {
		t.Fatalf("decode queue diagnostic: %v: %s", err, logs.buf.String())
	}
	if record["event"] != "admit_queue_full" || record["sandbox_id"] != "sb-overflow" ||
		record["queue_max_depth"] != float64(1) {
		t.Fatalf("queue diagnostic = %v", record)
	}
	if a.QueueDepth() != 1 {
		t.Fatalf("queue depth = %d, want 1", a.QueueDepth())
	}
}

// TestAdmission_QueueCanceledLogsDiagnostic verifies that a TTL-expired
// queued admit emits an admit_queue_canceled diagnostic with reason
// ttl_expired and delivers the queue_canceled rejection to a connected client.
func TestAdmission_QueueCanceledLogsDiagnostic(t *testing.T) {
	state := newTestState(t, 1<<30)
	a := NewAdmissionController(AdmissionPolicy{
		Rate: 100, Burst: 100, QueueTTL: 10 * time.Millisecond, QueueMaxDepth: 4,
	})
	logs := &queueLockCheckingWriter{queueMu: &a.queueMu}
	builderCalled := false
	a.SetWiring(state, slog.New(slog.NewJSONHandler(logs, nil)), func(*PendingAdmit) (*Message, error) {
		builderCalled = true
		return &Message{Type: TypeAdmitResponse, Status: StatusAdmitted}, nil
	})
	req := &Message{
		SandboxID:           "sb-ttl",
		CapacityMemoryBytes: 256 << 20,
		FloorMemoryBytes:    64 << 20,
		StartupBudgetMemory: 64 << 20,
	}
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	respCh := make(chan *Message, 1)
	go func() {
		msg, err := ReadMessage(client)
		if err != nil {
			t.Errorf("read rejection: %v", err)
			return
		}
		respCh <- msg
	}()
	entry, ok := a.Enqueue(req, server, 4242)
	if !ok {
		t.Fatal("enqueue failed")
	}
	defer entry.ttlTimer.Stop()

	// Wait deterministically for the TTL timer to close cancelCh.
	select {
	case <-entry.cancelCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for TTL cancelCh to close")
	}

	a.processQueue()

	if builderCalled {
		t.Fatal("builder was called for canceled admit")
	}
	if logs.wroteWhileLocked {
		t.Fatal("queue diagnostic was written while queueMu was held")
	}
	select {
	case msg := <-respCh:
		if msg.Status != StatusRejected || msg.Reason != "queue_canceled" {
			t.Fatalf("rejection = %+v", msg)
		}
		if msg.Msg != "queued admit canceled (TTL expiry)" {
			t.Fatalf("rejection Msg = %q, want %q", msg.Msg, "queued admit canceled (TTL expiry)")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for queue_canceled rejection")
	}
	if a.QueueDepth() != 0 {
		t.Fatalf("queue depth = %d, want 0 after TTL cancel", a.QueueDepth())
	}
	var record map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(logs.buf.Bytes()), &record); err != nil {
		t.Fatalf("decode queue diagnostic: %v: %s", err, logs.buf.String())
	}
	if record["event"] != "admit_queue_canceled" || record["reason"] != "ttl_expired" ||
		record["sandbox_id"] != "sb-ttl" {
		t.Fatalf("queue diagnostic = %v", record)
	}
}

// TestAdmission_QueueCanceledLogsDiagnostic_ClientDisconnected verifies that
// when a queued admit's TTL expires after the client has already disconnected,
// the server safely removes the entry from the queue and emits an
// admit_queue_canceled diagnostic (with reason=ttl_expired) outside queueMu,
// without panicking on the dead socket or expecting response delivery.
func TestAdmission_QueueCanceledLogsDiagnostic_ClientDisconnected(t *testing.T) {
	state := newTestState(t, 1<<30)
	a := NewAdmissionController(AdmissionPolicy{
		Rate: 100, Burst: 100, QueueTTL: 10 * time.Millisecond, QueueMaxDepth: 4,
	})
	logs := &queueLockCheckingWriter{queueMu: &a.queueMu}
	builderCalled := false
	a.SetWiring(state, slog.New(slog.NewJSONHandler(logs, nil)), func(*PendingAdmit) (*Message, error) {
		builderCalled = true
		return &Message{Type: TypeAdmitResponse, Status: StatusAdmitted}, nil
	})
	req := &Message{
		SandboxID:           "sb-ttl-disconnected",
		CapacityMemoryBytes: 256 << 20,
		FloorMemoryBytes:    64 << 20,
		StartupBudgetMemory: 64 << 20,
	}
	client, server := net.Pipe()
	defer server.Close()

	entry, ok := a.Enqueue(req, server, 4242)
	if !ok {
		t.Fatal("enqueue failed")
	}
	defer entry.ttlTimer.Stop()

	// Client closes the socket before the TTL fires and before processQueue runs.
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}

	select {
	case <-entry.cancelCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for TTL cancelCh to close")
	}

	a.processQueue()

	if builderCalled {
		t.Fatal("builder was called for canceled admit")
	}
	if logs.wroteWhileLocked {
		t.Fatal("queue diagnostic was written while queueMu was held")
	}
	if a.QueueDepth() != 0 {
		t.Fatalf("queue depth = %d, want 0 after TTL cleanup", a.QueueDepth())
	}
	var record map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(logs.buf.Bytes()), &record); err != nil {
		t.Fatalf("decode queue diagnostic: %v: %s", err, logs.buf.String())
	}
	if record["event"] != "admit_queue_canceled" || record["reason"] != "ttl_expired" ||
		record["sandbox_id"] != "sb-ttl-disconnected" {
		t.Fatalf("queue diagnostic = %v", record)
	}
}

func TestAdmission_QueuedWriteFailureClearsReservationConnection(t *testing.T) {
	state := newTestState(t, 8<<30)
	a := NewAdmissionController(AdmissionPolicy{
		Rate: 100, Burst: 100, QueueTTL: time.Second, QueueMaxDepth: 4,
	})
	logs := &queueLockCheckingWriter{queueMu: &a.queueMu}
	a.logger = slog.New(slog.NewJSONHandler(logs, nil))
	a.state = state
	a.processFn = func(p *PendingAdmit) (*Message, error) {
		token := "queued-token"
		_, _, err := state.Admit(AdmitSpec{
			Token: token, SandboxID: p.req.SandboxID, PeerPID: p.peerPID,
			Capacity:              Resources{MemoryBytes: 512 << 20, CPUMilli: 1000},
			ConfiguredAllocatable: Resources{MemoryBytes: 128 << 20, CPUMilli: 500},
			InitialBudget:         256 << 20,
			Conn:                  p.conn,
		})
		return &Message{Type: TypeAdmitResponse, Status: StatusAdmitted, Token: token}, err
	}
	client, server := net.Pipe()
	req := &Message{
		SandboxID: "queued-disconnect", CapacityMemoryBytes: 512 << 20, CapacityCPU: 1,
		FloorMemoryBytes: 128 << 20, FloorCPU: .5, StartupBudgetMemory: 256 << 20,
	}
	if _, ok := a.Enqueue(req, server, 4242); !ok {
		t.Fatal("enqueue failed")
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	a.processQueue()
	defer server.Close()
	got := reservationForTest(t, state, req.SandboxID)
	if got.Conn != nil {
		t.Fatal("failed queued response retained a closed connection as liveness evidence")
	}
	var record map[string]any
	if logs.wroteWhileLocked {
		t.Fatal("queue diagnostic was written while queueMu was held")
	}
	if err := json.Unmarshal(bytes.TrimSpace(logs.buf.Bytes()), &record); err != nil {
		t.Fatalf("decode queue diagnostic: %v: %s", err, logs.buf.String())
	}
	if record["level"] != "WARN" || record["event"] != "admit_queue_write_failed" ||
		record["sandbox_id"] != req.SandboxID || record["queue_position"] != float64(0) ||
		record["error"] == "" {
		t.Fatalf("queue diagnostic = %v", record)
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
