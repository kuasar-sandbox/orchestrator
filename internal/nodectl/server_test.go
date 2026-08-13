package nodectl

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"
)

// startTestServer spins up a controller daemon backed by a tmpdir UDS.
func startTestServer(t *testing.T, physMem uint64) (*Server, *Client, func()) {
	t.Helper()
	dir := t.TempDir()
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
	admission.SetWiring(state, nil, t.Logf,
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
		Capacity:     Resources{MemoryBytes: 512 << 20, CPUMilli: 1000},
		Floor:        Resources{MemoryBytes: 128 << 20, CPUMilli: 500},
		MemoryCharge: 512 << 20, StartupCharge: 512 << 20,
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
	if snapshot.ReservationCount != 1 || snapshot.ProvisionalCount != 0 || snapshot.Allocated.MemoryBytes != 256<<20 {
		t.Fatalf("replayed Admit snapshot = %+v", snapshot)
	}
}

func TestAdmitSpecRoundsPositiveCPUFloorUp(t *testing.T) {
	spec := admitSpecFromRequest(nil, 1, &Message{
		SandboxID: "sub-millicore", CapacityMemoryBytes: 512 << 20, CapacityCPU: 1,
		FloorMemoryBytes: 128 << 20, FloorCPU: 0.0005, StartupBudgetMemory: 256 << 20,
	})
	if spec.Floor.CPUMilli != 1 {
		t.Fatalf("Admit floor CPU = %d millicores, want 1", spec.Floor.CPUMilli)
	}
}

func TestQueuedAdmitDisconnectBeforeFirstTokenRequestClearsConnection(t *testing.T) {
	dir := t.TempDir()
	state := NewState(8<<30, 8000, 0, 0, Watermarks{
		HighFactor: .85, LowFactor: .7, EmergencyFactor: .05, StartupFactor: .5,
	})
	installReservationForTest(t, state, Reservation{
		Token: "filler-token", SandboxID: "filler",
		Capacity:          Resources{MemoryBytes: 4 << 30, CPUMilli: 1000},
		Floor:             Resources{MemoryBytes: 128 << 20, CPUMilli: 500},
		AllocatableNowMem: 128 << 20, EffectiveStartupBudget: 4 << 30,
		Stage: StageAdmitted, StageEnteredAt: time.Now(), LastHeartbeatAt: time.Now(),
	})
	admission := NewAdmissionController(AdmissionPolicy{
		Rate: 100, Burst: 100, QueueTTL: time.Minute, QueueMaxDepth: 4,
	})
	srv := &Server{
		Path: filepath.Join(dir, "controller.sock"), State: state, Admission: admission,
		Allocator: NewAllocator(AllocatorPolicy{}), Logf: t.Logf,
	}
	admission.SetWiring(state, nil, t.Logf, srv.BuildAdmitOKFromQueue)
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
	if _, found := state.Release("filler-token"); !found {
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
		snapshot.LastReportedRSS != 120<<20 || snapshot.LastReportAt.IsZero() {
		t.Fatalf("settled resource report = %+v found=%v", snapshot, found)
	}

	if err := c.Release("normal"); err != nil {
		t.Fatal(err)
	}
	// Token should be cleared.
	if c.Token() != "" {
		t.Errorf("token after release = %q, want empty", c.Token())
	}
}

func TestServer_BurstGrantAndRecover(t *testing.T) {
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
	if r.AllocatableNowMem != newAlloc {
		t.Errorf("reservation alloc=%v, want %d", r, newAlloc)
	}
	if r.Stage != StageBurst {
		t.Errorf("stage=%s, want burst", r.Stage)
	}
}

func TestServer_RejectInRedZone(t *testing.T) {
	srv, c, cleanup := startTestServer(t, 1<<30)
	defer cleanup()

	// Pre-load reservations totalling > 85% of pool to push into red.
	pool := srv.State.AllocatablePool.MemoryBytes
	installReservationForTest(t, srv.State, Reservation{
		Token: "dummy", SandboxID: "dummy",
		AllocatableNowMem: uint64(float64(pool) * 0.90),
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
	if !found || snapshot.LastReportedRSS != 50<<20 || snapshot.LastReportAt.IsZero() {
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
