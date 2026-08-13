package nodectl

import (
	"net"
	"strconv"
	"testing"
	"time"
)

func makeState(physMem, hostMem uint64) *State {
	return NewState(physMem, 8000, hostMem, 1500, Watermarks{
		OperationalMarginFactor: 0.10,
		HighFactor:              0.85,
		LowFactor:               0.70,
		EmergencyFactor:         0.05,
	})
}

func TestSyncAtomicallyReplacesProvisionalAndPriorSession(t *testing.T) {
	s := makeState(8<<30, 0)
	if err := s.InstallProvisional(ProvisionalSpec{
		SandboxID: "sync", CgroupPath: "/cg/sync", Capacity: Resources{MemoryBytes: 2 << 30},
		Floor: Resources{MemoryBytes: 128 << 20}, MemoryCharge: 2 << 30,
		StartupCharge: 2 << 30, RecoverySource: RecoveryLease, RecoveryKey: "lease:sync",
	}); err != nil {
		t.Fatal(err)
	}
	firstClient, firstServer := net.Pipe()
	defer firstClient.Close()
	defer firstServer.Close()
	if _, old, err := s.Sync(SyncSpec{
		SandboxID: "sync", CgroupPath: "/cg/sync", Token: "token-1",
		Capacity: Resources{MemoryBytes: 2 << 30}, Floor: Resources{MemoryBytes: 128 << 20},
		StartupMemory: 512 << 20, AppliedMemory: 512 << 20, Conn: firstServer,
	}); err != nil || len(old) != 0 {
		t.Fatalf("first sync old=%d err=%v", len(old), err)
	}
	if got := s.ResourceSnapshot(); got.Allocated.MemoryBytes != 512<<20 || got.StartupInFlight != 512<<20 || got.ProvisionalCount != 0 {
		t.Fatalf("first sync snapshot = %+v", got)
	}
	secondClient, secondServer := net.Pipe()
	defer secondClient.Close()
	defer secondServer.Close()
	if _, old, err := s.Sync(SyncSpec{
		SandboxID: "sync", CgroupPath: "/cg/sync", Token: "token-2",
		Capacity: Resources{MemoryBytes: 2 << 30}, Floor: Resources{MemoryBytes: 128 << 20},
		StartupMemory: 512 << 20, AppliedMemory: 256 << 20, Settled: true, Conn: secondServer,
	}); err != nil || len(old) != 1 || old[0] != firstServer {
		t.Fatalf("second sync old=%v err=%v", old, err)
	}
	if got := s.ResourceSnapshot(); got.Allocated.MemoryBytes != 256<<20 || got.StartupInFlight != 0 || got.ReservationCount != 1 {
		t.Fatalf("second sync snapshot = %+v", got)
	}
	if _, found := s.Heartbeat("token-1", 0, time.Now()); found {
		t.Fatal("old token remained valid after sync")
	}
}

func TestSyncRejectsLiveCgroupCollisionWithoutDroppingEitherConsumer(t *testing.T) {
	s := makeState(8<<30, 0)
	if _, _, err := s.Admit(AdmitSpec{
		Token: "owner-token", SandboxID: "owner", PeerPID: 100,
		CgroupPath: "/cg/shared", Capacity: Resources{MemoryBytes: 2 << 30},
		Floor: Resources{MemoryBytes: 128 << 20}, InitialAllocatable: 512 << 20,
		EffectiveStartupBudget: 512 << 20,
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.InstallProvisional(ProvisionalSpec{
		SandboxID: "claimant", PeerPID: 200, Capacity: Resources{MemoryBytes: 1 << 30},
		Floor: Resources{MemoryBytes: 128 << 20}, MemoryCharge: 1 << 30,
		StartupCharge: 1 << 30, RecoverySource: RecoveryLease, RecoveryKey: "lease:claimant",
	}); err != nil {
		t.Fatal(err)
	}
	before := s.ResourceSnapshot()
	if _, _, err := s.Sync(SyncSpec{
		Token: "claimant-token", SandboxID: "claimant", PeerPID: 200,
		CgroupPath: "/cg/shared", Capacity: Resources{MemoryBytes: 1 << 30},
		Floor: Resources{MemoryBytes: 128 << 20}, StartupMemory: 256 << 20,
		AppliedMemory: 256 << 20,
	}); err == nil {
		t.Fatal("StateSync evicted a different live cgroup owner")
	}
	after := s.ResourceSnapshot()
	if after != before {
		t.Fatalf("failed StateSync changed aggregates: before=%+v after=%+v", before, after)
	}
	if _, found := s.Heartbeat("owner-token", 0, time.Now()); !found {
		t.Fatal("failed StateSync removed the original session")
	}
	if got := reservationForTest(t, s, "claimant"); !got.Provisional {
		t.Fatalf("failed StateSync replaced claimant provisional: %+v", got)
	}
}

func TestSyncMergesOrphanCgroupProvisional(t *testing.T) {
	s := makeState(8<<30, 0)
	if err := s.InstallProvisional(ProvisionalSpec{
		SandboxID: "orphan", CgroupPath: "/cg/orphan",
		Capacity: Resources{MemoryBytes: 2 << 30}, MemoryCharge: 2 << 30,
		StartupCharge: 2 << 30, RecoverySource: RecoveryCgroup, RecoveryKey: "cgroup:/cg/orphan",
	}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Sync(SyncSpec{
		Token: "synced-token", SandboxID: "real-sid", PeerPID: 200,
		CgroupPath: "/cg/orphan", Capacity: Resources{MemoryBytes: 2 << 30},
		Floor: Resources{MemoryBytes: 128 << 20}, StartupMemory: 256 << 20,
		AppliedMemory: 256 << 20, Settled: true,
	}); err != nil {
		t.Fatal(err)
	}
	snapshot := s.ResourceSnapshot()
	if snapshot.ReservationCount != 1 || snapshot.ProvisionalCount != 0 || snapshot.Allocated.MemoryBytes != 256<<20 {
		t.Fatalf("merged orphan snapshot = %+v", snapshot)
	}
	if _, found := s.SnapshotSandboxResource("orphan"); found {
		t.Fatal("orphan synthetic SID remained after StateSync")
	}
}

func TestSyncRequiresRecoveredConsumer(t *testing.T) {
	s := makeState(8<<30, 0)
	before := s.ResourceSnapshot()
	if _, _, err := s.Sync(SyncSpec{
		Token: "untracked-token", SandboxID: "untracked", PeerPID: 200,
		CgroupPath: "/cg/untracked", Capacity: Resources{MemoryBytes: 2 << 30},
		Floor: Resources{MemoryBytes: 128 << 20}, StartupMemory: 256 << 20,
		AppliedMemory: 256 << 20, Settled: true,
	}); err == nil {
		t.Fatal("StateSync created a reservation without recovered state")
	}
	if after := s.ResourceSnapshot(); after != before {
		t.Fatalf("rejected StateSync changed state: before=%+v after=%+v", before, after)
	}
}

func TestAdmitRetryRequiresSameUnadvancedContract(t *testing.T) {
	s := makeState(8<<30, 0)
	spec := AdmitSpec{
		Token: "first-token", SandboxID: "retry", PeerPID: 100,
		CgroupPath: "/cg/retry", Capacity: Resources{MemoryBytes: 2 << 30, CPUMilli: 1000},
		Floor:              Resources{MemoryBytes: 128 << 20, CPUMilli: 500},
		InitialAllocatable: 512 << 20, EffectiveStartupBudget: 512 << 20,
	}
	if _, _, err := s.Admit(spec); err != nil {
		t.Fatal(err)
	}
	if !s.CanReplayAdmit(spec) {
		t.Fatal("matching admitted session was not replayable")
	}
	wrongOwner := spec
	wrongOwner.PeerPID++
	if s.CanReplayAdmit(wrongOwner) {
		t.Fatal("different owner could replay Admit")
	}
	retry := spec
	retry.Token = "retry-token"
	if _, _, err := s.Admit(retry); err != nil {
		t.Fatalf("identical Admit ACK-loss retry failed: %v", err)
	}
	before := s.ResourceSnapshot()
	changed := retry
	changed.Token = "changed-token"
	changed.Capacity.MemoryBytes++
	if _, _, err := s.Admit(changed); err == nil {
		t.Fatal("Admit retry changed immutable capacity")
	}
	if after := s.ResourceSnapshot(); after != before {
		t.Fatalf("failed Admit retry changed aggregates: before=%+v after=%+v", before, after)
	}
	if _, ok := s.SetSettled(retry.Token, 256<<20, time.Now()); !ok {
		t.Fatal("settle failed")
	}
	if s.CanReplayAdmit(retry) {
		t.Fatal("advanced reservation remained replayable")
	}
	retry.Token = "late-token"
	if _, _, err := s.Admit(retry); err == nil {
		t.Fatal("Admit retry replaced an advanced reservation")
	}
}

func TestProvisionalAdmitReplayIsAlreadyAccounted(t *testing.T) {
	s := makeState(530<<20, 0)
	spec := AdmitSpec{
		SandboxID: "recovering", PeerPID: 100, CgroupPath: "/cg/recovering",
		Capacity:           Resources{MemoryBytes: 512 << 20, CPUMilli: 1000},
		Floor:              Resources{MemoryBytes: 128 << 20, CPUMilli: 500},
		InitialAllocatable: 256 << 20, EffectiveStartupBudget: 256 << 20,
	}
	if err := s.InstallProvisional(ProvisionalSpec{
		SandboxID: spec.SandboxID, PeerPID: spec.PeerPID, CgroupPath: spec.CgroupPath,
		Capacity: spec.Capacity, Floor: spec.Floor, MemoryCharge: spec.Capacity.MemoryBytes,
		StartupCharge: spec.Capacity.MemoryBytes, RecoverySource: RecoveryLease, RecoveryKey: "lease:recovering",
	}); err != nil {
		t.Fatal(err)
	}
	if zone := s.ResourceSnapshot().Zone; zone != ZoneCritical {
		t.Fatalf("provisional did not make test pool critical: %s", zone)
	}
	if !s.CanReplayAdmit(spec) {
		t.Fatal("critical provisional could not be replayed")
	}
}

func TestInstallProvisionalRejectsDifferentLeaseOnSameCgroup(t *testing.T) {
	s := makeState(8<<30, 0)
	first := ProvisionalSpec{
		SandboxID: "first", PeerPID: 100, CgroupPath: "/cg/shared",
		Capacity: Resources{MemoryBytes: 1 << 30}, Floor: Resources{MemoryBytes: 128 << 20},
		MemoryCharge: 1 << 30, StartupCharge: 1 << 30,
		RecoverySource: RecoveryLease, RecoveryKey: "lease:first",
	}
	if err := s.InstallProvisional(first); err != nil {
		t.Fatal(err)
	}
	second := first
	second.SandboxID, second.PeerPID, second.RecoveryKey = "second", 200, "lease:second"
	if err := s.InstallProvisional(second); err == nil {
		t.Fatal("two live leases sharing one cgroup were silently deduplicated")
	}
	snapshot := s.ResourceSnapshot()
	if snapshot.ReservationCount != 1 || snapshot.Allocated.MemoryBytes != 1<<30 {
		t.Fatalf("failed provisional collision changed state: %+v", snapshot)
	}
}

func TestStateAggregatesAcrossThousandReservations(t *testing.T) {
	s := makeState(64<<30, 0)
	const count = 1000
	for n := 0; n < count; n++ {
		token := "token-" + strconv.Itoa(n)
		if _, _, err := s.Admit(AdmitSpec{
			Token: token, SandboxID: "sandbox-" + strconv.Itoa(n),
			Capacity:           Resources{MemoryBytes: 64 << 20, CPUMilli: 1000},
			Floor:              Resources{MemoryBytes: 16 << 20, CPUMilli: 100},
			InitialAllocatable: 32 << 20, EffectiveStartupBudget: 32 << 20,
		}); err != nil {
			t.Fatal(err)
		}
	}
	snapshot := s.ResourceSnapshot()
	if snapshot.ReservationCount != count || snapshot.Allocated.MemoryBytes != count*(32<<20) || snapshot.Allocated.CPUMilli != count*100 || snapshot.StartupInFlight != count*(32<<20) {
		t.Fatalf("aggregate snapshot = %+v", snapshot)
	}
	for n := 0; n < count; n++ {
		if _, found := s.Release("token-" + strconv.Itoa(n)); !found {
			t.Fatalf("release %d failed", n)
		}
	}
	if snapshot := s.ResourceSnapshot(); snapshot.ReservationCount != 0 || snapshot.Allocated != (Resources{}) || snapshot.StartupInFlight != 0 {
		t.Fatalf("final aggregate = %+v", snapshot)
	}
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

func TestMemoryZone(t *testing.T) {
	s := makeState(100<<30, 16<<30)
	pool := s.AllocatablePool.MemoryBytes

	// Empty: green
	if z := s.ResourceSnapshot().Zone; z != ZoneGreen {
		t.Errorf("empty state zone = %s, want green", z)
	}

	// Add a reservation that pushes into yellow.
	installReservationForTest(t, s, Reservation{
		Token: "a", SandboxID: "a",
		AllocatableNowMem: uint64(float64(pool) * 0.75),
	})
	if z := s.ResourceSnapshot().Zone; z != ZoneYellow {
		t.Errorf("75%% allocated zone = %s, want yellow", z)
	}

	// Bump into red.
	mutateReservationForTest(t, s, "a", func(r *Reservation) { r.AllocatableNowMem = uint64(float64(pool) * 0.90) })
	if z := s.ResourceSnapshot().Zone; z != ZoneRed {
		t.Errorf("90%% allocated zone = %s, want red", z)
	}

	// Push into critical (within emergency_pool).
	mutateReservationForTest(t, s, "a", func(r *Reservation) { r.AllocatableNowMem = uint64(float64(pool) * 0.97) })
	if z := s.ResourceSnapshot().Zone; z != ZoneCritical {
		t.Errorf("97%% allocated zone = %s, want critical", z)
	}
}

func TestNodeAllocated_SumsReservations(t *testing.T) {
	s := makeState(100<<30, 16<<30)
	installReservationForTest(t, s, Reservation{Token: "a", SandboxID: "a", AllocatableNowMem: 1 << 30, Floor: Resources{CPUMilli: 100}})
	installReservationForTest(t, s, Reservation{Token: "b", SandboxID: "b", AllocatableNowMem: 2 << 30, Floor: Resources{CPUMilli: 500}})

	got := s.ResourceSnapshot().Allocated
	if got.MemoryBytes != 3<<30 {
		t.Errorf("MemoryBytes = %d, want 3 GiB", got.MemoryBytes)
	}
	if got.CPUMilli != 600 {
		t.Errorf("CPUMilli = %d, want 600", got.CPUMilli)
	}
}

func TestNamedAdmitReleaseAndSIDIndex(t *testing.T) {
	s := makeState(100<<30, 16<<30)
	res, _, err := s.Admit(AdmitSpec{
		Token: "x", SandboxID: "sb-x", InitialAllocatable: 1,
		Capacity: Resources{MemoryBytes: 2}, Floor: Resources{MemoryBytes: 1},
		EffectiveStartupBudget: 1, Now: time.Now(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Admit(AdmitSpec{Token: "y", SandboxID: "sb-x", InitialAllocatable: 1}); err == nil {
		t.Error("second reservation for the same sandbox should error")
	}
	snapshot, found := s.SnapshotSandboxResource("sb-x")
	if !found || snapshot.Capacity != res.Capacity {
		t.Fatalf("SnapshotSandboxResource = %+v found=%v", snapshot, found)
	}
	if _, found := s.Release("x"); !found {
		t.Fatal("Release did not find token")
	}
	if _, found := s.SnapshotSandboxResource("sb-x"); found {
		t.Error("SID index retained a removed reservation")
	}
}

func TestAdmitRejectsAllocationAboveCapacity(t *testing.T) {
	s := makeState(8<<30, 0)
	if _, _, err := s.Admit(AdmitSpec{
		Token: "over-cap", SandboxID: "over-cap",
		Capacity:           Resources{MemoryBytes: 128 << 20, CPUMilli: 1000},
		Floor:              Resources{MemoryBytes: 64 << 20, CPUMilli: 500},
		InitialAllocatable: 256 << 20, EffectiveStartupBudget: 256 << 20,
	}); err == nil {
		t.Fatal("Admit accepted allocation above capacity")
	}
	if got := s.ResourceSnapshot(); got.ReservationCount != 0 || got.Allocated.MemoryBytes != 0 {
		t.Fatalf("failed Admit changed aggregates: %+v", got)
	}
}
