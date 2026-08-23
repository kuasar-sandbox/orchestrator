package nodectl

import (
	"math"
	"net"
	"strconv"
	"strings"
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

func TestScaleUint64FloorKeepsIntegerPrecision(t *testing.T) {
	max := ^uint64(0)
	wantSevenEighths := max - max/8 - 1
	for _, tc := range []struct {
		name   string
		value  uint64
		factor float64
		want   uint64
	}{
		{name: "zero factor", value: max, factor: 0, want: 0},
		{name: "negative factor", value: max, factor: -1, want: 0},
		{name: "nan factor", value: max, factor: math.NaN(), want: 0},
		{name: "seven eighths at uint64 max", value: max, factor: 0.875, want: wantSevenEighths},
		{name: "one", value: max, factor: 1, want: max},
		{name: "clamp above one", value: max, factor: 2, want: max},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := scaleUint64Floor(tc.value, tc.factor); got != tc.want {
				t.Fatalf("scaleUint64Floor(%d, %g) = %d, want %d", tc.value, tc.factor, got, tc.want)
			}
		})
	}
}

func TestCPUMilliCeilPreservesPositiveFloors(t *testing.T) {
	for _, tc := range []struct {
		cpu  float64
		want uint64
	}{{0, 0}, {0.0005, 1}, {0.001, 1}, {0.0011, 2}, {0.5, 500}} {
		if got := cpuMilliCeil(tc.cpu); got != tc.want {
			t.Fatalf("cpuMilliCeil(%g) = %d, want %d", tc.cpu, got, tc.want)
		}
	}
}

func TestSyncAtomicallyReplacesProvisionalAndPriorSession(t *testing.T) {
	s := makeState(8<<30, 0)
	if err := s.InstallProvisional(ProvisionalSpec{
		SandboxID: "sync", CgroupPath: "/cg/sync", Capacity: Resources{MemoryBytes: 2 << 30},
		ConfiguredAllocatable: Resources{MemoryBytes: 128 << 20}, ReservationMemory: 2 << 30,
		InitialBudget: 2 << 30, RecoverySource: RecoveryLease, RecoveryKey: "lease:sync",
	}); err != nil {
		t.Fatal(err)
	}
	firstClient, firstServer := net.Pipe()
	defer firstClient.Close()
	defer firstServer.Close()
	if _, old, err := s.Sync(SyncSpec{
		SandboxID: "sync", CgroupPath: "/cg/sync", Token: "token-1",
		Capacity: Resources{MemoryBytes: 2 << 30}, ConfiguredAllocatable: Resources{MemoryBytes: 128 << 20},
		ReservationMemory: 512 << 20, Conn: firstServer,
	}); err != nil || len(old) != 0 {
		t.Fatalf("first sync old=%d err=%v", len(old), err)
	}
	if got := s.ResourceSnapshot(); got.Reserved.MemoryBytes != 512<<20 || got.StartupInFlight != 512<<20 || got.ProvisionalCount != 0 {
		t.Fatalf("first sync snapshot = %+v", got)
	}
	secondClient, secondServer := net.Pipe()
	defer secondClient.Close()
	defer secondServer.Close()
	if _, old, err := s.Sync(SyncSpec{
		SandboxID: "sync", CgroupPath: "/cg/sync", Token: "token-2",
		Capacity: Resources{MemoryBytes: 2 << 30}, ConfiguredAllocatable: Resources{MemoryBytes: 128 << 20},
		ReservationMemory: 256 << 20, Settled: true, Conn: secondServer,
	}); err != nil || len(old) != 1 || old[0] != firstServer {
		t.Fatalf("second sync old=%v err=%v", old, err)
	}
	if got := s.ResourceSnapshot(); got.Reserved.MemoryBytes != 256<<20 || got.StartupInFlight != 0 || got.ReservationCount != 1 {
		t.Fatalf("second sync snapshot = %+v", got)
	}
	if _, found := s.Heartbeat("token-1", 0, time.Now()); found {
		t.Fatal("old token remained valid after sync")
	}
}

func TestSyncCannotIncreaseExistingReservation(t *testing.T) {
	s := makeState(8<<30, 0)
	if _, _, err := s.Admit(AdmitSpec{
		Token: "old-token", SandboxID: "sync", CgroupPath: "/cg/sync",
		Capacity: Resources{MemoryBytes: 1 << 30}, ConfiguredAllocatable: Resources{MemoryBytes: 128 << 20},
		InitialBudget: 256 << 20,
	}); err != nil {
		t.Fatal(err)
	}
	before := s.ResourceSnapshot()
	if _, _, err := s.Sync(SyncSpec{
		Token: "new-token", SandboxID: "sync", CgroupPath: "/cg/sync",
		Capacity: Resources{MemoryBytes: 1 << 30}, ConfiguredAllocatable: Resources{MemoryBytes: 128 << 20},
		ReservationMemory: 512 << 20, Settled: true,
	}); err == nil {
		t.Fatal("StateSync increased an existing reservation without admission")
	}
	if after := s.ResourceSnapshot(); after != before {
		t.Fatalf("failed StateSync changed aggregates: before=%+v after=%+v", before, after)
	}
	if _, found := s.Heartbeat("old-token", 0, time.Now()); !found {
		t.Fatal("failed StateSync invalidated the existing session")
	}
}

func TestSyncRejectsLiveCgroupCollisionWithoutDroppingEitherConsumer(t *testing.T) {
	s := makeState(8<<30, 0)
	if _, _, err := s.Admit(AdmitSpec{
		Token: "owner-token", SandboxID: "owner", PeerPID: 100,
		CgroupPath: "/cg/shared", Capacity: Resources{MemoryBytes: 2 << 30},
		ConfiguredAllocatable: Resources{MemoryBytes: 128 << 20}, InitialBudget: 512 << 20,
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.InstallProvisional(ProvisionalSpec{
		SandboxID: "claimant", PeerPID: 200, Capacity: Resources{MemoryBytes: 1 << 30},
		ConfiguredAllocatable: Resources{MemoryBytes: 128 << 20}, ReservationMemory: 1 << 30,
		InitialBudget: 1 << 30, RecoverySource: RecoveryLease, RecoveryKey: "lease:claimant",
	}); err != nil {
		t.Fatal(err)
	}
	before := s.ResourceSnapshot()
	if _, _, err := s.Sync(SyncSpec{
		Token: "claimant-token", SandboxID: "claimant", PeerPID: 200,
		CgroupPath: "/cg/shared", Capacity: Resources{MemoryBytes: 1 << 30},
		ConfiguredAllocatable: Resources{MemoryBytes: 128 << 20},
		ReservationMemory:     256 << 20,
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
		Capacity: Resources{MemoryBytes: 2 << 30}, ReservationMemory: 2 << 30,
		InitialBudget: 2 << 30, RecoverySource: RecoveryCgroup, RecoveryKey: "cgroup:/cg/orphan",
	}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Sync(SyncSpec{
		Token: "synced-token", SandboxID: "real-sid", PeerPID: 200,
		CgroupPath: "/cg/orphan", Capacity: Resources{MemoryBytes: 2 << 30},
		ConfiguredAllocatable: Resources{MemoryBytes: 128 << 20},
		ReservationMemory:     256 << 20, Settled: true,
	}); err != nil {
		t.Fatal(err)
	}
	snapshot := s.ResourceSnapshot()
	if snapshot.ReservationCount != 1 || snapshot.ProvisionalCount != 0 || snapshot.Reserved.MemoryBytes != 256<<20 {
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
		ConfiguredAllocatable: Resources{MemoryBytes: 128 << 20},
		ReservationMemory:     256 << 20, Settled: true,
	}); err == nil {
		t.Fatal("StateSync created a reservation without recovered state")
	}
	if after := s.ResourceSnapshot(); after != before {
		t.Fatalf("rejected StateSync changed state: before=%+v after=%+v", before, after)
	}
}

func TestInvalidSyncDoesNotDropRecoveredCgroup(t *testing.T) {
	s := makeState(8<<30, 0)
	if err := s.InstallProvisional(ProvisionalSpec{
		SandboxID: "orphan", CgroupPath: "/cg/orphan",
		Capacity: Resources{MemoryBytes: 2 << 30}, ReservationMemory: 2 << 30,
		InitialBudget: 2 << 30, RecoverySource: RecoveryCgroup, RecoveryKey: "cgroup:/cg/orphan",
	}); err != nil {
		t.Fatal(err)
	}
	before := s.ResourceSnapshot()
	if _, _, err := s.Sync(SyncSpec{
		Token: "invalid-token", SandboxID: "real-sid", PeerPID: 200,
		CgroupPath: "/cg/orphan", Capacity: Resources{MemoryBytes: 2 << 30, CPUMilli: 1000},
		ConfiguredAllocatable: Resources{MemoryBytes: 128 << 20, CPUMilli: 2000},
		ReservationMemory:     256 << 20,
	}); err == nil {
		t.Fatal("StateSync accepted a CPU floor above capacity")
	}
	if after := s.ResourceSnapshot(); after != before {
		t.Fatalf("invalid StateSync changed recovered charge: before=%+v after=%+v", before, after)
	}
	if got := reservationForTest(t, s, "orphan"); !got.Provisional || got.RecoverySource != RecoveryCgroup {
		t.Fatalf("invalid StateSync replaced recovered cgroup: %+v", got)
	}
}

func TestAdmitRetryRequiresSameUnadvancedContract(t *testing.T) {
	s := makeState(8<<30, 0)
	spec := AdmitSpec{
		Token: "first-token", SandboxID: "retry", PeerPID: 100,
		CgroupPath: "/cg/retry", Capacity: Resources{MemoryBytes: 2 << 30, CPUMilli: 1000},
		ConfiguredAllocatable: Resources{MemoryBytes: 128 << 20, CPUMilli: 500},
		InitialBudget:         512 << 20,
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
	if _, ok, err := s.SetSettled(retry.Token, 256<<20, time.Now()); err != nil || !ok {
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
		Capacity:              Resources{MemoryBytes: 512 << 20, CPUMilli: 1000},
		ConfiguredAllocatable: Resources{MemoryBytes: 128 << 20, CPUMilli: 500},
		InitialBudget:         256 << 20,
	}
	if err := s.InstallProvisional(ProvisionalSpec{
		SandboxID: spec.SandboxID, PeerPID: spec.PeerPID, CgroupPath: spec.CgroupPath,
		Capacity: spec.Capacity, ConfiguredAllocatable: spec.ConfiguredAllocatable, ReservationMemory: spec.Capacity.MemoryBytes,
		InitialBudget: spec.Capacity.MemoryBytes, RecoverySource: RecoveryLease, RecoveryKey: "lease:recovering",
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

func TestProvisionalAdmitCannotIncreaseRecoveredCharge(t *testing.T) {
	s := makeState(8<<30, 0)
	spec := AdmitSpec{
		Token: "new-token", SandboxID: "recovering", PeerPID: 100, CgroupPath: "/cg/recovering",
		Capacity:              Resources{MemoryBytes: 512 << 20, CPUMilli: 1000},
		ConfiguredAllocatable: Resources{MemoryBytes: 64 << 20, CPUMilli: 500},
		InitialBudget:         256 << 20,
	}
	if err := s.InstallProvisional(ProvisionalSpec{
		SandboxID: spec.SandboxID, PeerPID: spec.PeerPID, CgroupPath: spec.CgroupPath,
		Capacity: spec.Capacity, ConfiguredAllocatable: spec.ConfiguredAllocatable,
		ReservationMemory: 128 << 20, InitialBudget: 128 << 20,
		RecoverySource: RecoveryLease, RecoveryKey: "lease:recovering",
	}); err != nil {
		t.Fatal(err)
	}
	before := s.ResourceSnapshot()
	if s.CanReplayAdmit(spec) {
		t.Fatal("undercharged provisional was treated as an already-accounted replay")
	}
	if _, _, err := s.Admit(spec); err == nil || !strings.Contains(err.Error(), "exceeds recovered charge") {
		t.Fatalf("Admit error = %v, want recovered-charge rejection", err)
	}
	if after := s.ResourceSnapshot(); after != before {
		t.Fatalf("failed provisional replacement changed aggregates: before=%+v after=%+v", before, after)
	}
	if got := reservationForTest(t, s, spec.SandboxID); got.ReservationMemory != 128<<20 || !got.Provisional {
		t.Fatalf("failed replacement changed provisional: %+v", got)
	}
}

func TestAdmitReplacementRequiresSameClientFeatures(t *testing.T) {
	s := makeState(8<<30, 0)
	features := []string{FeatureStateSyncV1}
	base := AdmitSpec{
		Token: "new-token", SandboxID: "recovering", PeerPID: 100, CgroupPath: "/cg/recovering",
		Capacity:              Resources{MemoryBytes: 512 << 20, CPUMilli: 1000},
		ConfiguredAllocatable: Resources{MemoryBytes: 128 << 20, CPUMilli: 500},
		InitialBudget:         256 << 20,
		ClientFeatures:        features,
	}
	if err := s.InstallProvisional(ProvisionalSpec{
		SandboxID: base.SandboxID, PeerPID: base.PeerPID, CgroupPath: base.CgroupPath,
		Capacity: base.Capacity, ConfiguredAllocatable: base.ConfiguredAllocatable, ReservationMemory: base.Capacity.MemoryBytes,
		InitialBudget: base.Capacity.MemoryBytes, RecoverySource: RecoveryLease,
		RecoveryKey: "lease:recovering", ClientFeatures: features,
	}); err != nil {
		t.Fatal(err)
	}
	changed := base
	changed.ClientFeatures = nil
	before := s.ResourceSnapshot()
	if _, _, err := s.Admit(changed); err == nil {
		t.Fatal("Admit replaced provisional with different client features")
	}
	if after := s.ResourceSnapshot(); after != before {
		t.Fatalf("feature-mismatched provisional replay changed state: before=%+v after=%+v", before, after)
	}
	if _, _, err := s.Admit(base); err != nil {
		t.Fatal(err)
	}
	changed.Token = "retry-token"
	if _, _, err := s.Admit(changed); err == nil {
		t.Fatal("Admit replaced ACK-lost session with different client features")
	}
	if _, found := s.Heartbeat("new-token", 0, time.Now()); !found {
		t.Fatal("feature-mismatched retry invalidated original session")
	}
}

func TestInvalidAdmitDoesNotDropRecoveredCgroup(t *testing.T) {
	s := makeState(8<<30, 0)
	if err := s.InstallProvisional(ProvisionalSpec{
		SandboxID: "orphan", CgroupPath: "/cg/orphan",
		Capacity: Resources{MemoryBytes: 2 << 30}, ReservationMemory: 2 << 30,
		InitialBudget: 2 << 30, RecoverySource: RecoveryCgroup, RecoveryKey: "cgroup:/cg/orphan",
	}); err != nil {
		t.Fatal(err)
	}
	before := s.ResourceSnapshot()
	if _, _, err := s.Admit(AdmitSpec{
		Token: "invalid-token", SandboxID: "real-sid", PeerPID: 200,
		CgroupPath: "/cg/orphan", Capacity: Resources{MemoryBytes: 128 << 20},
		InitialBudget: 256 << 20,
	}); err == nil {
		t.Fatal("Admit accepted allocation above capacity")
	}
	if after := s.ResourceSnapshot(); after != before {
		t.Fatalf("invalid Admit changed recovered charge: before=%+v after=%+v", before, after)
	}
	if got := reservationForTest(t, s, "orphan"); !got.Provisional || got.RecoverySource != RecoveryCgroup {
		t.Fatalf("invalid Admit replaced recovered cgroup: %+v", got)
	}
}

func TestInstallProvisionalRejectsDifferentLeaseOnSameCgroup(t *testing.T) {
	s := makeState(8<<30, 0)
	first := ProvisionalSpec{
		SandboxID: "first", PeerPID: 100, CgroupPath: "/cg/shared",
		Capacity: Resources{MemoryBytes: 1 << 30}, ConfiguredAllocatable: Resources{MemoryBytes: 128 << 20},
		ReservationMemory: 1 << 30, InitialBudget: 1 << 30,
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
	if snapshot.ReservationCount != 1 || snapshot.Reserved.MemoryBytes != 1<<30 {
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
			Capacity:              Resources{MemoryBytes: 64 << 20, CPUMilli: 1000},
			ConfiguredAllocatable: Resources{MemoryBytes: 16 << 20, CPUMilli: 100},
			InitialBudget:         32 << 20,
		}); err != nil {
			t.Fatal(err)
		}
	}
	snapshot := s.ResourceSnapshot()
	if snapshot.ReservationCount != count || snapshot.Reserved.MemoryBytes != count*(32<<20) || snapshot.Reserved.CPUMilli != count*100 || snapshot.StartupInFlight != count*(32<<20) {
		t.Fatalf("aggregate snapshot = %+v", snapshot)
	}
	for n := 0; n < count; n++ {
		if _, found, err := s.Release("token-" + strconv.Itoa(n)); err != nil || !found {
			t.Fatalf("release %d failed", n)
		}
	}
	if snapshot := s.ResourceSnapshot(); snapshot.ReservationCount != 0 || snapshot.Reserved != (Resources{}) || snapshot.StartupInFlight != 0 {
		t.Fatalf("final aggregate = %+v", snapshot)
	}
}

func TestReconcileAndGrantReusesExistingReservationBeforeRateLimit(t *testing.T) {
	s := makeState(8<<30, 0)
	if _, _, err := s.Admit(AdmitSpec{
		Token: "reuse-token", SandboxID: "reuse", Capacity: Resources{MemoryBytes: 1 << 30},
		ConfiguredAllocatable: Resources{MemoryBytes: 128 << 20}, InitialBudget: 512 << 20,
	}); err != nil {
		t.Fatal(err)
	}
	allocator := NewAllocator(AllocatorPolicy{
		MemoryGrantPerSecBytes: 64 << 20,
		MinGrantStep:           4 << 20,
		MaxGrantStep:           256 << 20,
	})
	allocator.mu.Lock()
	allocator.tokens = 0
	allocator.lastRefill = time.Now()
	allocator.mu.Unlock()

	// The complete request fits in oldReservation-current. It is an
	// idempotent reuse of already charged memory and must bypass rate limits.
	result, found, err := s.ReconcileAndGrant("reuse-token", 384<<20, 128<<20, UrgencyNormal, allocator)
	if err != nil || !found {
		t.Fatalf("complete reuse found=%v err=%v", found, err)
	}
	if result.Decision.GrantedDelta != 128<<20 || result.Reservation.ReservationMemory != 512<<20 {
		t.Fatalf("complete reuse result=%+v", result)
	}
	if score := allocator.FairnessScore("reuse-token"); score != 0 {
		t.Fatalf("reused reservation entered allocator history: %d", score)
	}

	// Only the 64 MiB tail exceeds the old reservation. With an empty token
	// bucket that tail is deferred, while the reusable 128 MiB is still returned
	// in full and the aggregate remains at the old safe reservation.
	result, found, err = s.ReconcileAndGrant("reuse-token", 384<<20, 192<<20, UrgencyNormal, allocator)
	if err != nil || !found {
		t.Fatalf("partial reuse found=%v err=%v", found, err)
	}
	if result.Decision.GrantedDelta != 128<<20 || result.Reservation.ReservationMemory != 512<<20 || result.Decision.CooldownMs == 0 {
		t.Fatalf("partial reuse result=%+v", result)
	}
	if snapshot := s.ResourceSnapshot(); snapshot.Reserved.MemoryBytes != 512<<20 {
		t.Fatalf("node aggregate=%d, want %d", snapshot.Reserved.MemoryBytes, uint64(512<<20))
	}
}

func TestNewState_DerivedPool(t *testing.T) {
	s := makeState(100<<30, 16<<30)
	// The existing NodeBudget status field is the 100 GiB physical total;
	// subtracting HostReserved yields the 84 GiB post-host budget.
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
		ReservationMemory: uint64(float64(pool) * 0.75),
	})
	if z := s.ResourceSnapshot().Zone; z != ZoneYellow {
		t.Errorf("75%% allocated zone = %s, want yellow", z)
	}

	// Bump into red.
	mutateReservationForTest(t, s, "a", func(r *Reservation) { r.ReservationMemory = uint64(float64(pool) * 0.90) })
	if z := s.ResourceSnapshot().Zone; z != ZoneRed {
		t.Errorf("90%% allocated zone = %s, want red", z)
	}

	// Push into critical (within emergency_pool).
	mutateReservationForTest(t, s, "a", func(r *Reservation) { r.ReservationMemory = uint64(float64(pool) * 0.97) })
	if z := s.ResourceSnapshot().Zone; z != ZoneCritical {
		t.Errorf("97%% allocated zone = %s, want critical", z)
	}
}

func TestReservedMemorySumsReservations(t *testing.T) {
	s := makeState(100<<30, 16<<30)
	installReservationForTest(t, s, Reservation{Token: "a", SandboxID: "a", ReservationMemory: 1 << 30, ConfiguredAllocatable: Resources{CPUMilli: 100}})
	installReservationForTest(t, s, Reservation{Token: "b", SandboxID: "b", ReservationMemory: 2 << 30, ConfiguredAllocatable: Resources{CPUMilli: 500}})

	got := s.ResourceSnapshot().Reserved
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
		Token: "x", SandboxID: "sb-x", InitialBudget: 1,
		Capacity: Resources{MemoryBytes: 2}, ConfiguredAllocatable: Resources{MemoryBytes: 1},
		Now: time.Now(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Admit(AdmitSpec{Token: "y", SandboxID: "sb-x", InitialBudget: 1}); err == nil {
		t.Error("second reservation for the same sandbox should error")
	}
	snapshot, found := s.SnapshotSandboxResource("sb-x")
	if !found || snapshot.Capacity != res.Capacity {
		t.Fatalf("SnapshotSandboxResource = %+v found=%v", snapshot, found)
	}
	if _, found, err := s.Release("x"); err != nil || !found {
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
		Capacity:              Resources{MemoryBytes: 128 << 20, CPUMilli: 1000},
		ConfiguredAllocatable: Resources{MemoryBytes: 64 << 20, CPUMilli: 500},
		InitialBudget:         256 << 20,
	}); err == nil {
		t.Fatal("Admit accepted allocation above capacity")
	}
	if got := s.ResourceSnapshot(); got.ReservationCount != 0 || got.Reserved.MemoryBytes != 0 {
		t.Fatalf("failed Admit changed aggregates: %+v", got)
	}
}

func TestReservationAggregateOverflowFailsWithoutMutation(t *testing.T) {
	s := makeState(^uint64(0), 0)
	first := &Reservation{
		Token: "first", SandboxID: "first",
		Capacity:          Resources{MemoryBytes: ^uint64(0)},
		ReservationMemory: ^uint64(0) - 16, Stage: StageSettled,
	}
	s.mu.Lock()
	if err := s.insertLocked(first); err != nil {
		s.mu.Unlock()
		t.Fatal(err)
	}
	before := s.resourceSnapshotLocked()
	second := &Reservation{
		Token: "second", SandboxID: "second",
		Capacity: Resources{MemoryBytes: 32}, ReservationMemory: 32,
		Stage: StageSettled,
	}
	err := s.insertLocked(second)
	after := s.resourceSnapshotLocked()
	s.mu.Unlock()
	if err == nil {
		t.Fatal("overflowing reservation aggregate was accepted")
	}
	if after != before {
		t.Fatalf("failed aggregate insertion mutated state: before=%+v after=%+v", before, after)
	}
}

func TestAggregateUnderflowFailsClosedWithoutMutation(t *testing.T) {
	t.Run("release", func(t *testing.T) {
		s := makeState(8<<30, 0)
		if _, _, err := s.Admit(AdmitSpec{
			Token: "release-token", SandboxID: "release", Capacity: Resources{MemoryBytes: 1 << 30},
			ConfiguredAllocatable: Resources{MemoryBytes: 128 << 20}, InitialBudget: 512 << 20,
		}); err != nil {
			t.Fatal(err)
		}
		s.mu.Lock()
		s.reservedMemory = 256 << 20
		before := s.resourceSnapshotLocked()
		s.mu.Unlock()

		if _, found, err := s.Release("release-token"); err == nil || !found {
			t.Fatalf("Release found=%v err=%v, want found underflow error", found, err)
		}
		if after := s.ResourceSnapshot(); after != before {
			t.Fatalf("failed Release mutated aggregates: before=%+v after=%+v", before, after)
		}
		if got := reservationForTest(t, s, "release"); got.Token != "release-token" {
			t.Fatalf("failed Release removed reservation: %+v", got)
		}
	})

	t.Run("settled", func(t *testing.T) {
		s := makeState(8<<30, 0)
		if _, _, err := s.Admit(AdmitSpec{
			Token: "settled-token", SandboxID: "settled", Capacity: Resources{MemoryBytes: 1 << 30},
			ConfiguredAllocatable: Resources{MemoryBytes: 128 << 20}, InitialBudget: 512 << 20,
		}); err != nil {
			t.Fatal(err)
		}
		s.mu.Lock()
		s.startupInFlight = 0
		before := s.resourceSnapshotLocked()
		s.mu.Unlock()

		if _, found, err := s.SetSettled("settled-token", 384<<20, time.Now()); err == nil || !found {
			t.Fatalf("SetSettled found=%v err=%v, want found underflow error", found, err)
		}
		if after := s.ResourceSnapshot(); after != before {
			t.Fatalf("failed SetSettled mutated aggregates: before=%+v after=%+v", before, after)
		}
		got := reservationForTest(t, s, "settled")
		if got.Stage != StageAdmitted || got.InitialBudget != 512<<20 || got.LastHostMemoryCurrent != 0 || !got.LastReportAt.IsZero() {
			t.Fatalf("failed SetSettled mutated reservation: %+v", got)
		}
	})

	t.Run("request budget", func(t *testing.T) {
		s := makeState(8<<30, 0)
		if _, _, err := s.Admit(AdmitSpec{
			Token: "budget-token", SandboxID: "budget", Capacity: Resources{MemoryBytes: 1 << 30},
			ConfiguredAllocatable: Resources{MemoryBytes: 128 << 20}, InitialBudget: 512 << 20,
		}); err != nil {
			t.Fatal(err)
		}
		s.mu.Lock()
		s.reservedMemory = 256 << 20
		before := s.resourceSnapshotLocked()
		s.mu.Unlock()
		allocator := NewAllocator(AllocatorPolicy{})

		if _, found, err := s.ReconcileAndGrant("budget-token", 384<<20, 64<<20, UrgencyNormal, allocator); err == nil || !found {
			t.Fatalf("ReconcileAndGrant found=%v err=%v, want found underflow error", found, err)
		}
		if after := s.ResourceSnapshot(); after != before {
			t.Fatalf("failed RequestBudget mutated aggregates: before=%+v after=%+v", before, after)
		}
		if got := reservationForTest(t, s, "budget"); got.ReservationMemory != 512<<20 {
			t.Fatalf("failed RequestBudget mutated reservation: %+v", got)
		}
	})
}
