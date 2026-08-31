package nodectl

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// startTestServer spins up a controller daemon backed by a tmpdir UDS.
func startTestServer(t *testing.T, physMem uint64) (*Server, *Client, func()) {
	t.Helper()
	dir := shortTestDir(t)
	sock := filepath.Join(dir, "ctl.sock")

	state := NewState(physMem, 8000, 1<<30, 1500, Watermarks{
		OperationalMarginFactor: 0.10,
		HighFactor:              0.85,
		LowFactor:               0.70,
		EmergencyFactor:         0.05,
		StartupFactor:           0.50,
	})
	admission := NewAdmissionController(AdmissionPolicy{
		Rate:          100,
		Burst:         100,
		StartupTTL:    5 * time.Minute,
		QueueTTL:      60 * time.Second,
		QueueMaxDepth: 256,
	})
	admission.SetWiring(state, nil,
		func(p *PendingAdmit) (*Message, error) { return nil, nil })
	allocator := NewAllocator(AllocatorPolicy{
		MemoryGrantPerSecBytes: 1 << 30, // 1 GiB/s for tests
		MinGrantStep:           1 << 20,
		MaxGrantStep:           1 << 30,
	})
	srv := &Server{
		Path:      sock,
		State:     state,
		Admission: admission,
		Allocator: allocator,
		Logf:      t.Logf,
	}
	if err := srv.Listen(); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	srvDone := make(chan struct{})
	go func() {
		_ = srv.Serve(ctx)
		close(srvDone)
	}()

	c := &Client{SocketPath: sock}
	if err := c.Connect(); err != nil {
		cancel()
		t.Fatal(err)
	}

	cleanup := func() {
		_ = c.Close()
		cancel()
		<-srvDone
	}
	return srv, c, cleanup
}

func TestFeatureAdmitDoesNotOpenLifecycleLease(t *testing.T) {
	dir := t.TempDir()
	root := filepath.Join(dir, "cgroups")
	state := makeState(8<<30, 0)
	srv := &Server{
		State: state, Inventory: &Inventory{
			ControllerSocket: filepath.Join(dir, "missing-controller.sock"),
			CgroupScanPaths:  []string{root},
		},
		Admission: NewAdmissionController(AdmissionPolicy{}),
		Allocator: NewAllocator(AllocatorPolicy{}), Logf: t.Logf,
	}
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	token := ""
	resp, err := srv.buildAdmitOK(server, 4242, &Message{
		Type: TypeAdmit, SandboxID: "no-lease-read",
		CapacityMemoryBytes: 1 << 30, CapacityCPU: 1,
		FloorMemoryBytes: 128 << 20, FloorCPU: .5,
		StartupBudgetMemory: 256 << 20,
		CgroupPath:          filepath.Join(root, "target"),
		ClientFeatures:      []string{FeatureStateSyncV1},
	}, &token)
	if err != nil {
		t.Fatalf("feature Admit consulted absent lease: %v", err)
	}
	if resp.Status != StatusAdmitted || token == "" {
		t.Fatalf("Admit response = %+v token=%q", resp, token)
	}
}

func TestRecoveredAdmitReplayBypassesNewConsumerGates(t *testing.T) {
	dir := t.TempDir()
	root := filepath.Join(dir, "cgroups")
	cgroup := filepath.Join(root, "recovering")
	state := makeState(530<<20, 0)
	features := []string{FeatureStateSyncV1}
	if err := state.InstallProvisional(ProvisionalSpec{
		SandboxID: "recovering", PeerPID: 4242, CgroupPath: cgroup,
		Capacity:              Resources{MemoryBytes: 512 << 20, CPUMilli: 1000},
		ConfiguredAllocatable: Resources{MemoryBytes: 128 << 20, CPUMilli: 500},
		ReservationMemory:     512 << 20, InitialBudget: 512 << 20,
		RecoverySource: RecoveryLease, RecoveryKey: "lease:recovering", ClientFeatures: features,
	}); err != nil {
		t.Fatal(err)
	}
	admission := NewAdmissionController(AdmissionPolicy{Rate: 1, Burst: 1})
	admission.state = state
	admission.SetDrained(true)
	srv := &Server{
		State: state, Admission: admission, Logf: t.Logf,
		Inventory: &Inventory{ControllerSocket: filepath.Join(dir, "controller.sock"), CgroupScanPaths: []string{root}},
	}
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	token := ""
	resp := srv.handleAdmit(server, 4242, &Message{
		Type: TypeAdmit, SandboxID: "recovering", CgroupPath: cgroup,
		CapacityMemoryBytes: 512 << 20, CapacityCPU: 1,
		FloorMemoryBytes: 128 << 20, FloorCPU: .5,
		StartupBudgetMemory: 256 << 20, ClientFeatures: features,
	}, &token)
	if resp == nil || resp.Status != StatusAdmitted || token == "" {
		t.Fatalf("replayed Admit = %+v token=%q", resp, token)
	}
	snapshot := state.ResourceSnapshot()
	if snapshot.ReservationCount != 1 || snapshot.ProvisionalCount != 0 || snapshot.Reserved.MemoryBytes != 256<<20 {
		t.Fatalf("replayed Admit snapshot = %+v", snapshot)
	}
	select {
	case <-admission.wakeCh:
	default:
		t.Fatal("provisional replacement did not wake resource-blocked admission")
	}
}

func TestAdmitSpecRoundsPositiveCPUFloorUp(t *testing.T) {
	spec := admitSpecFromRequest(nil, 1, &Message{
		SandboxID: "sub-millicore", CapacityMemoryBytes: 512 << 20, CapacityCPU: 1,
		FloorMemoryBytes: 128 << 20, FloorCPU: 0.0005, StartupBudgetMemory: 256 << 20,
	})
	if spec.ConfiguredAllocatable.CPUMilli != 1 {
		t.Fatalf("Admit allocatable CPU = %d millicores, want 1", spec.ConfiguredAllocatable.CPUMilli)
	}
}

func TestConcurrentAdmitCheckAndInsertCannotOversubscribePool(t *testing.T) {
	state := NewState(1<<30, 8000, 0, 0, Watermarks{
		HighFactor: .85, LowFactor: .7, EmergencyFactor: .05, StartupFactor: 1,
	})
	admission := NewAdmissionController(AdmissionPolicy{
		Rate: 100, Burst: 100, QueueTTL: time.Second, QueueMaxDepth: 4,
	})
	admission.SetWiring(state, nil, func(*PendingAdmit) (*Message, error) { return nil, nil })
	srv := &Server{State: state, Admission: admission, Logf: t.Logf}

	firstChecked := make(chan struct{})
	releaseFirst := make(chan struct{})
	var checked int32
	srv.phaseHook = func(phase string) {
		if phase != "admit_checked" {
			return
		}
		if atomic.AddInt32(&checked, 1) == 1 {
			close(firstChecked)
			<-releaseFirst
		}
	}

	type result struct{ response *Message }
	request := func(sid string, conn net.Conn, done chan<- result) {
		token := ""
		done <- result{response: srv.handleAdmit(conn, 4242, &Message{
			Type: TypeAdmit, SandboxID: sid,
			CapacityMemoryBytes: 700 << 20, CapacityCPU: 1,
			FloorMemoryBytes: 128 << 20, FloorCPU: .5,
			StartupBudgetMemory: 600 << 20,
		}, &token)}
	}
	firstClient, firstServer := net.Pipe()
	secondClient, secondServer := net.Pipe()
	defer firstClient.Close()
	defer firstServer.Close()
	defer secondClient.Close()
	defer secondServer.Close()
	firstDone, secondDone := make(chan result, 1), make(chan result, 1)
	go request("first", firstServer, firstDone)
	<-firstChecked
	go request("second", secondServer, secondDone)

	select {
	case <-secondDone:
		close(releaseFirst)
		<-firstDone
		t.Fatal("second Admit passed the check/insert serialization boundary")
	case <-time.After(20 * time.Millisecond):
	}
	close(releaseFirst)
	firstResult := <-firstDone
	secondResult := <-secondDone
	if firstResult.response == nil || firstResult.response.Status != StatusAdmitted {
		t.Fatalf("first Admit = %+v", firstResult.response)
	}
	if secondResult.response != nil {
		t.Fatalf("second Admit = %+v, want queued", secondResult.response)
	}
	snapshot := state.ResourceSnapshot()
	if snapshot.ReservationCount != 1 || snapshot.Reserved.MemoryBytes != 600<<20 {
		t.Fatalf("concurrent Admit oversubscribed state: %+v", snapshot)
	}
}

func TestConcurrentAdmitAndBudgetGrowCannotOversubscribePool(t *testing.T) {
	state := NewState(1<<30, 8000, 0, 0, Watermarks{
		HighFactor: .85, LowFactor: .7, EmergencyFactor: .05, StartupFactor: 1,
	})
	installReservationForTest(t, state, Reservation{
		Token: "existing-token", SandboxID: "existing",
		Capacity: Resources{MemoryBytes: 800 << 20}, ConfiguredAllocatable: Resources{MemoryBytes: 128 << 20},
		ReservationMemory: 300 << 20, Stage: StageSettled,
	})
	admission := NewAdmissionController(AdmissionPolicy{
		Rate: 100, Burst: 100, QueueTTL: time.Second, QueueMaxDepth: 4,
	})
	admission.SetWiring(state, nil, func(*PendingAdmit) (*Message, error) { return nil, nil })
	srv := &Server{
		State: state, Admission: admission,
		Allocator: NewAllocator(AllocatorPolicy{
			MemoryGrantPerSecBytes: 1 << 30, MinGrantStep: 1 << 20, MaxGrantStep: 1 << 30,
		}),
		Logf: t.Logf,
	}

	admitChecked := make(chan struct{})
	releaseAdmit := make(chan struct{})
	srv.phaseHook = func(phase string) {
		if phase == "admit_checked" {
			close(admitChecked)
			<-releaseAdmit
		}
	}
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	token := ""
	admitDone := make(chan *Message, 1)
	go func() {
		admitDone <- srv.handleAdmit(server, 4242, &Message{
			Type: TypeAdmit, SandboxID: "new", CapacityMemoryBytes: 700 << 20, CapacityCPU: 1,
			FloorMemoryBytes: 128 << 20, FloorCPU: .5, StartupBudgetMemory: 600 << 20,
		}, &token)
	}()
	<-admitChecked

	growDone := make(chan *Message, 1)
	go func() {
		growDone <- srv.handleRequestBudget(&Message{
			CurrentAlloc: 300 << 20, RequestedDelta: 400 << 20, Urgency: UrgencyNormal,
		}, "existing-token")
	}()
	select {
	case response := <-growDone:
		close(releaseAdmit)
		<-admitDone
		t.Fatalf("Budget grow crossed Admit check/insert boundary: %+v", response)
	case <-time.After(20 * time.Millisecond):
	}
	close(releaseAdmit)
	if response := <-admitDone; response == nil || response.Status != StatusAdmitted {
		t.Fatalf("Admit response = %+v", response)
	}
	response := <-growDone
	if response.Type != TypeBudgetResponse || response.GrantedDelta != 0 || response.NewAllocatable != 300<<20 {
		t.Fatalf("post-Admit grow response = %+v", response)
	}
	if snapshot := state.ResourceSnapshot(); snapshot.Reserved.MemoryBytes != 900<<20 ||
		snapshot.Reserved.MemoryBytes > snapshot.AllocatablePool.MemoryBytes {
		t.Fatalf("Admit/grow oversubscribed state: %+v", snapshot)
	}
}

func TestQueuedAdmitDisconnectBeforeFirstTokenRequestClearsConnection(t *testing.T) {
	dir := shortTestDir(t)
	state := NewState(8<<30, 8000, 0, 0, Watermarks{
		HighFactor: .85, LowFactor: .7, EmergencyFactor: .05, StartupFactor: .5,
	})
	installReservationForTest(t, state, Reservation{
		Token: "filler-token", SandboxID: "filler",
		Capacity:              Resources{MemoryBytes: 4 << 30, CPUMilli: 1000},
		ConfiguredAllocatable: Resources{MemoryBytes: 128 << 20, CPUMilli: 500},
		ReservationMemory:     128 << 20, InitialBudget: 4 << 30,
		Stage: StageAdmitted, StageEnteredAt: time.Now(), LastHeartbeatAt: time.Now(),
	})
	admission := NewAdmissionController(AdmissionPolicy{
		Rate: 100, Burst: 100, QueueTTL: time.Minute, QueueMaxDepth: 4,
	})
	srv := &Server{
		Path: filepath.Join(dir, "controller.sock"), State: state, Admission: admission,
		Allocator: NewAllocator(AllocatorPolicy{}), Logf: t.Logf,
	}
	admission.SetWiring(state, nil, srv.BuildAdmitOKFromQueue)
	if err := srv.Listen(); err != nil {
		t.Fatal(err)
	}
	admission.Run()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = srv.Serve(ctx)
		close(done)
	}()
	defer func() {
		cancel()
		<-done
		admission.Stop()
	}()

	client := &Client{SocketPath: srv.Path}
	if err := client.Connect(); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		res, err := client.Admit(AdmitParams{
			SandboxID: "queued", CapacityMemoryBytes: 512 << 20, CapacityCPU: 1,
			FloorMemoryBytes: 128 << 20, FloorCPU: .5, StartupBudgetMemory: 256 << 20,
		})
		if err == nil && res.Status != StatusAdmitted {
			err = fmt.Errorf("queued Admit status=%s msg=%s", res.Status, res.Msg)
		}
		result <- err
	}()
	deadline := time.Now().Add(2 * time.Second)
	for admission.QueueDepth() != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if admission.QueueDepth() != 1 {
		t.Fatal("Admit did not enter queue")
	}
	if _, found, err := state.Release("filler-token"); err != nil || !found {
		t.Fatal("failed to release startup-pool filler")
	}
	admission.PushWake()
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if got := reservationForTest(t, state, "queued"); got.Conn == nil {
			return
		}
		time.Sleep(time.Millisecond)
	}
	if got := reservationForTest(t, state, "queued"); got.Conn != nil {
		t.Fatal("queued Admit EOF retained a closed connection as liveness evidence")
	}
}

func TestServer_AdmitSettledRelease(t *testing.T) {
	srv, c, cleanup := startTestServer(t, 8<<30)
	defer cleanup()

	res, err := c.Admit(AdmitParams{
		SandboxID:           "sb-1",
		CapacityMemoryBytes: 1 << 30,
		CapacityCPU:         1,
		FloorMemoryBytes:    128 << 20,
		FloorCPU:            0.5,
		StartupBudgetMemory: 256 << 20,
		CgroupPath:          "/sys/fs/cgroup/sandboxes/sb-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != StatusAdmitted {
		t.Fatalf("status=%s msg=%s, want admitted", res.Status, res.Msg)
	}
	if res.GrantedInitialAlloc != 256<<20 {
		t.Errorf("granted=%d, want 256 MiB", res.GrantedInitialAlloc)
	}
	if res.Token == "" {
		t.Error("token empty")
	}

	if err := c.Settled(120<<20, 0); err != nil {
		t.Fatal(err)
	}
	if snapshot, found := srv.State.SnapshotSandboxResource("sb-1"); !found ||
		snapshot.HostMemoryCurrent != 120<<20 || snapshot.LastReportAt.IsZero() {
		t.Fatalf("settled resource report = %+v found=%v", snapshot, found)
	}
	if got := reservationByTokenForTest(t, srv.State, c.Token()).ReservationMemory; got != 256<<20 {
		t.Fatalf("Settled rewrote reservation to host memory.current=%d, want unchanged %d", got, uint64(256<<20))
	}

	if err := c.Release("normal"); err != nil {
		t.Fatal(err)
	}
	// Token should be cleared.
	if c.Token() != "" {
		t.Errorf("token after release = %q, want empty", c.Token())
	}
}

func TestServer_ColdAndRestoreAdmissionUseExactSelectedBudget(t *testing.T) {
	for _, tc := range []struct {
		name              string
		startup, headroom uint64
		snapshot          uint64
		want              uint64
	}{
		{name: "cold startup below headroom", startup: 128 << 20, headroom: 512 << 20, want: 128 << 20},
		{name: "restore ignores startup and headroom", startup: 768 << 20, headroom: 512 << 20, snapshot: 192 << 20, want: 192 << 20},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, c, cleanup := startTestServer(t, 8<<30)
			defer cleanup()
			result, err := c.Admit(AdmitParams{
				SandboxID: tc.name, CapacityMemoryBytes: 1 << 30,
				FloorMemoryBytes: tc.headroom, StartupBudgetMemory: tc.startup,
				AllocatableAtSnapshot: tc.snapshot,
			})
			if err != nil {
				t.Fatal(err)
			}
			if result.Status != StatusAdmitted || result.GrantedInitialAlloc != tc.want {
				t.Fatalf("admission = %+v, want exact initial Budget %d", result, tc.want)
			}
		})
	}
}

func TestServer_RequestBudgetReconcilesAbsoluteBaseline(t *testing.T) {
	srv, c, cleanup := startTestServer(t, 8<<30)
	defer cleanup()
	result, err := c.Admit(AdmitParams{
		SandboxID: "baseline", CapacityMemoryBytes: 1 << 30,
		FloorMemoryBytes: 256 << 20, StartupBudgetMemory: 128 << 20,
	})
	if err != nil || result.Status != StatusAdmitted {
		t.Fatalf("Admit = %+v, %v", result, err)
	}
	if err := c.Settled(900<<20, 0); err != nil {
		t.Fatal(err)
	}
	granted, grown, _, err := c.RequestBudget(128<<20, 64<<20, UrgencyNormal, "grow")
	if err != nil || granted != 64<<20 || grown != 192<<20 {
		t.Fatalf("grow = granted %d reservation %d err %v", granted, grown, err)
	}
	// Discard the coalesced wake from Admit/Settled/grow so the shrink commit
	// below must produce its own admission re-evaluation signal.
	select {
	case <-srv.Admission.wakeCh:
	default:
	}
	granted, shrunk, _, err := c.RequestBudget(128<<20, 0, UrgencyLow, "shrink_commit")
	if err != nil || granted != 0 || shrunk != 128<<20 {
		t.Fatalf("shrink commit = granted %d reservation %d err %v", granted, shrunk, err)
	}
	reservation := reservationForTest(t, srv.State, "baseline")
	if reservation.ReservationMemory != 128<<20 || reservation.Stage != StageSettled {
		t.Fatalf("reconciled reservation = %+v", reservation)
	}
	if snapshot := srv.State.ResourceSnapshot(); snapshot.Reserved.MemoryBytes != 128<<20 {
		t.Fatalf("node aggregate after shrink commit = %d, want %d", snapshot.Reserved.MemoryBytes, uint64(128<<20))
	}
	select {
	case <-srv.Admission.wakeCh:
	default:
		t.Fatal("shrink commit did not wake resource-blocked admission")
	}
	if _, _, _, err := c.RequestBudget(192<<20, 0, UrgencyLow, "invalid_baseline"); err == nil {
		t.Fatal("controller accepted sandbox baseline above its node reservation")
	}
}

func TestServer_RequestBudgetGrowPreservesSettledLifecycle(t *testing.T) {
	srv, c, cleanup := startTestServer(t, 8<<30)
	defer cleanup()

	res, err := c.Admit(AdmitParams{
		SandboxID:           "sb-2",
		CapacityMemoryBytes: 1 << 30,
		FloorMemoryBytes:    128 << 20,
		StartupBudgetMemory: 128 << 20,
	})
	if err != nil || res.Status != StatusAdmitted {
		t.Fatalf("admit failed: %v / %s", err, res.Status)
	}
	if err := c.Settled(120<<20, 0); err != nil {
		t.Fatal(err)
	}
	// Grant 256 MiB more.
	g, newAlloc, _, err := c.RequestBudget(128<<20, 256<<20, UrgencyNormal, "high_event")
	if err != nil {
		t.Fatal(err)
	}
	if g == 0 {
		t.Fatal("expected non-zero grant")
	}
	if newAlloc != 128<<20+g {
		t.Errorf("new_alloc=%d, want 128MiB + granted (%d)", newAlloc, 128<<20+g)
	}

	// Server-side reservation should reflect the new allocatable.
	r := reservationByTokenForTest(t, srv.State, c.Token())
	if r.ReservationMemory != newAlloc {
		t.Errorf("reservation alloc=%v, want %d", r, newAlloc)
	}
	if r.Stage != StageSettled {
		t.Errorf("stage=%s, want settled lifecycle unchanged", r.Stage)
	}
}

func TestServer_RejectInRedZone(t *testing.T) {
	srv, c, cleanup := startTestServer(t, 1<<30)
	defer cleanup()

	// Pre-load reservations totalling > 85% of pool to push into red.
	pool := srv.State.AllocatablePool.MemoryBytes
	installReservationForTest(t, srv.State, Reservation{
		Token: "dummy", SandboxID: "dummy",
		ReservationMemory: uint64(float64(pool) * 0.90),
	})

	res, err := c.Admit(AdmitParams{
		SandboxID:           "sb-r",
		CapacityMemoryBytes: 100 << 20,
		FloorMemoryBytes:    50 << 20,
		StartupBudgetMemory: 50 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != StatusRejected {
		t.Errorf("status=%s, want rejected (red zone)", res.Status)
	}
}

func TestServer_OOMReportAndHeartbeat(t *testing.T) {
	srv, c, cleanup := startTestServer(t, 8<<30)
	defer cleanup()

	res, _ := c.Admit(AdmitParams{
		SandboxID:           "sb-h",
		CapacityMemoryBytes: 256 << 20,
		FloorMemoryBytes:    64 << 20,
		StartupBudgetMemory: 64 << 20,
	})
	if res.Status != StatusAdmitted {
		t.Fatal("admit failed")
	}

	if _, err := c.Heartbeat(50<<20, 1000, 0, 0); err != nil {
		t.Fatal(err)
	}
	if err := c.OOMReport(2, 12345, 60<<20); err != nil {
		t.Fatal(err)
	}
	r := reservationByTokenForTest(t, srv.State, c.Token())
	if r.OOMCount != 2 {
		t.Errorf("oom_count = %d, want 2", r.OOMCount)
	}
	snapshot, found := srv.State.SnapshotSandboxResource("sb-h")
	if !found || snapshot.HostMemoryCurrent != 50<<20 || snapshot.LastReportAt.IsZero() {
		t.Fatalf("heartbeat resource report = %+v found=%v", snapshot, found)
	}
}

func TestIdleSweeperRemovesSandboxIndex(t *testing.T) {
	state := NewState(8<<30, 8000, 1<<30, 1000, Watermarks{})
	installReservationForTest(t, state, Reservation{
		Token: "12345678", SandboxID: "expired", Stage: StageCreating,
		StageEnteredAt: time.Now().Add(-time.Hour), LastHeartbeatAt: time.Now(),
	})
	sweeper := &IdleSweeper{
		State: state, Admission: NewAdmissionController(AdmissionPolicy{}),
		Allocator:  NewAllocator(AllocatorPolicy{}),
		StartupTTL: time.Minute, Logf: t.Logf,
	}
	sweeper.sweep()
	if _, found := state.SnapshotSandboxResource("expired"); found {
		t.Fatal("sweeper retained expired SID index entry")
	}
}

// TestServer_AdmitQueueFullWireRejectionAndDiagnostic verifies that when a
// short-term blocked Admit request arrives while the admission queue is at
// capacity, Server.handleAdmit returns a wire queue_full rejection, the
// admission controller emits an admit_queue_full diagnostic after releasing
// queueMu, queue depth remains unchanged, and no reservation is added to State.
func TestServer_AdmitQueueFullWireRejectionAndDiagnostic(t *testing.T) {
	state := NewState(1000<<20, 8000, 0, 0, Watermarks{
		HighFactor: .90, LowFactor: .70, EmergencyFactor: .05, StartupFactor: 1,
	})
	admission := NewAdmissionController(AdmissionPolicy{
		Rate: 100, Burst: 100, QueueTTL: time.Hour, QueueMaxDepth: 1,
	})
	logs := &queueLockCheckingWriter{queueMu: &admission.queueMu}
	logger := slog.New(slog.NewJSONHandler(logs, nil))
	admission.SetWiring(state, logger, func(*PendingAdmit) (*Message, error) { return nil, nil })
	srv := &Server{
		State: state, Admission: admission, Logf: t.Logf,
	}

	// Consume main headroom so that incoming requests are short-term blocked
	// (800 MiB reserved leaves 150 MiB headroom after 50 MiB emergency buffer,
	// while remaining in zone yellow rather than zone red).
	installReservationForTest(t, state, Reservation{
		Token: "existing-token", SandboxID: "existing",
		Capacity: Resources{MemoryBytes: 800 << 20}, ConfiguredAllocatable: Resources{MemoryBytes: 128 << 20},
		ReservationMemory: 800 << 20, Stage: StageSettled,
	})

	c1Client, c1Server := net.Pipe()
	defer c1Client.Close()
	defer c1Server.Close()
	c2Client, c2Server := net.Pipe()
	defer c2Client.Close()
	defer c2Server.Close()

	req1 := &Message{
		Type: TypeAdmit, SandboxID: "sb-1",
		CapacityMemoryBytes: 256 << 20, FloorMemoryBytes: 64 << 20, StartupBudgetMemory: 200 << 20,
	}
	token1 := ""
	// First request is short-term blocked and enqueued; handleAdmit returns nil to signal async handling.
	resp1 := srv.handleAdmit(c1Server, 4242, req1, &token1)
	if resp1 != nil {
		t.Fatalf("first admit expected nil (queued), got %+v", resp1)
	}
	if admission.QueueDepth() != 1 {
		t.Fatalf("queue depth = %d, want 1", admission.QueueDepth())
	}

	// Second request arrives while queue is at capacity (QueueMaxDepth = 1).
	req2 := &Message{
		Type: TypeAdmit, SandboxID: "sb-2",
		CapacityMemoryBytes: 256 << 20, FloorMemoryBytes: 64 << 20, StartupBudgetMemory: 200 << 20,
	}
	token2 := ""
	resp2 := srv.handleAdmit(c2Server, 4243, req2, &token2)
	if resp2 == nil {
		t.Fatal("second admit expected wire rejection, got nil")
	}
	if resp2.Status != StatusRejected || resp2.Reason != "queue_full" {
		t.Fatalf("second admit response = %+v, want StatusRejected reason=queue_full", resp2)
	}
	if resp2.Msg != "admission queue at capacity" {
		t.Fatalf("second admit Msg = %q, want %q", resp2.Msg, "admission queue at capacity")
	}

	// Verify lock-free diagnostic emission.
	if logs.wroteWhileLocked {
		t.Fatal("queue diagnostic was written while queueMu was held")
	}
	var record map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(logs.buf.Bytes()), &record); err != nil {
		t.Fatalf("decode queue diagnostic: %v: %s", err, logs.buf.String())
	}
	if record["event"] != "admit_queue_full" || record["sandbox_id"] != "sb-2" ||
		record["queue_max_depth"] != float64(1) {
		t.Fatalf("queue diagnostic = %v", record)
	}

	// Verify queue depth is unchanged.
	if admission.QueueDepth() != 1 {
		t.Fatalf("queue depth = %d, want 1 (unchanged)", admission.QueueDepth())
	}

	// Verify state reservations are unchanged (only the initial existing reservation exists).
	snapshot := state.ResourceSnapshot()
	if snapshot.ReservationCount != 1 || snapshot.Reserved.MemoryBytes != 800<<20 {
		t.Fatalf("unexpected state reservations after queue_full rejection: %+v", snapshot)
	}
}

