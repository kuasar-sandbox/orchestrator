package nodectl

import (
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"
)

// startTestServer spins up a controller daemon backed by a tmpdir-based
// UDS socket and persistence file. The returned cleanup stops the
// server and waits for goroutines.
func startTestServer(t *testing.T, physMem uint64) (*Server, *Client, func()) {
	t.Helper()
	dir := t.TempDir()
	sock := filepath.Join(dir, "ctl.sock")
	statePath := filepath.Join(dir, "state.json")

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
	persister := &Persister{Path: statePath}

	srv := &Server{
		Path:      sock,
		State:     state,
		Admission: admission,
		Allocator: allocator,
		Persister: persister,
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

func TestServer_PersistAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	sock := filepath.Join(dir, "ctl.sock")
	statePath := filepath.Join(dir, "state.json")

	build := func() (*Server, *Client, context.CancelFunc, chan struct{}) {
		state := NewState(8<<30, 8000, 1<<30, 1500, Watermarks{
			OperationalMarginFactor: 0.10,
			HighFactor:              0.85,
			LowFactor:               0.70,
			EmergencyFactor:         0.05,
			StartupFactor:           0.50,
		})
		// Try to load any prior state.
		persister := &Persister{Path: statePath}
		if prev, err := persister.Load(); err == nil && prev != nil {
			if err := state.RestoreReservations(prev.PersistenceReservations()); err != nil {
				t.Fatal(err)
			}
		}
		admission := NewAdmissionController(AdmissionPolicy{
			Rate: 100, Burst: 100, StartupTTL: time.Minute,
			QueueTTL: 60 * time.Second, QueueMaxDepth: 256,
		})
		admission.SetWiring(state, nil, t.Logf,
			func(p *PendingAdmit) (*Message, error) { return nil, nil })
		allocator := NewAllocator(AllocatorPolicy{MemoryGrantPerSecBytes: 1 << 30, MinGrantStep: 1 << 20, MaxGrantStep: 1 << 30})
		s := &Server{
			Path: sock, State: state,
			Admission: admission, Allocator: allocator, Persister: persister,
			Logf: t.Logf,
		}
		if err := s.Listen(); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() { _ = s.Serve(ctx); close(done) }()
		c := &Client{SocketPath: sock}
		if err := c.Connect(); err != nil {
			cancel()
			t.Fatal(err)
		}
		return s, c, cancel, done
	}

	s1, c1, cancel1, done1 := build()
	res, _ := c1.Admit(AdmitParams{
		SandboxID:           "sb-p",
		CapacityMemoryBytes: 256 << 20,
		FloorMemoryBytes:    64 << 20,
		StartupBudgetMemory: 64 << 20,
	})
	if res.Status != StatusAdmitted {
		t.Fatal("admit failed")
	}
	tok := c1.Token()
	_ = c1.Close()
	cancel1()
	<-done1
	_ = s1

	// "Restart": new server reads persisted state.
	s2, c2, cancel2, done2 := build()
	defer func() {
		_ = c2.Close()
		cancel2()
		<-done2
	}()
	_ = reservationByTokenForTest(t, s2.State, tok)
}

func TestIdleSweeperRemovesSandboxIndex(t *testing.T) {
	state := NewState(8<<30, 8000, 1<<30, 1000, Watermarks{})
	installReservationForTest(t, state, Reservation{
		Token: "12345678", SandboxID: "expired", Stage: StageCreating,
		StageEnteredAt: time.Now().Add(-time.Hour), LastHeartbeatAt: time.Now(),
	})
	sweeper := &IdleSweeper{
		State: state, Admission: NewAdmissionController(AdmissionPolicy{}),
		Allocator: NewAllocator(AllocatorPolicy{}), Persister: &Persister{},
		StartupTTL: time.Minute, Logf: t.Logf,
	}
	sweeper.sweep()
	if _, found := state.SnapshotSandboxResource("expired"); found {
		t.Fatal("sweeper retained expired SID index entry")
	}
}
