package nodectl

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/kuasar-sandbox/sandboxer/pkg/resource"
)

const testCacheMiB = uint64(1 << 20)

func admitCacheTest(t *testing.T, s *State, sid string, maximum uint64) string {
	t.Helper()
	token := "token-" + sid
	client, server := net.Pipe()
	t.Cleanup(func() { client.Close(); server.Close() })
	_, _, err := s.Admit(AdmitSpec{
		SandboxID: sid, Token: token, CgroupPath: "/cg/" + sid, Conn: server,
		Capacity:              Resources{MemoryBytes: 2 << 30, CPUMilli: 1000},
		ConfiguredAllocatable: Resources{MemoryBytes: 128 << 20, CPUMilli: 1000},
		InitialBudget:         256 << 20, CacheMax: maximum,
		ClientFeatures: []string{resource.FeatureCacheBudgetV1},
	})
	if err != nil {
		t.Fatal(err)
	}
	return token
}

func allocatorCacheTest() *Allocator {
	return NewAllocator(AllocatorPolicy{
		MemoryGrantPerSecBytes: 1 << 30, MinGrantStep: 4 << 20, MaxGrantStep: 512 << 20,
	})
}

func mustCacheGrant(t *testing.T, s *State, token string, current, requested, maximum uint64, a *Allocator) CacheGrantResult {
	t.Helper()
	res, found, err := s.ReconcileCacheBudget(token, current, requested, maximum, a)
	if err != nil || !found {
		t.Fatalf("cache grant current=%d requested=%d found=%v err=%v", current, requested, found, err)
	}
	return res
}

func TestCacheGrantSharesPoolAndKeepsGuestReservationIndependent(t *testing.T) {
	s := makeState(4<<30, 0)
	tok := admitCacheTest(t, s, "a", 1<<30)
	a := allocatorCacheTest()
	baseline := s.ResourceSnapshot().Reserved.MemoryBytes
	grant := mustCacheGrant(t, s, tok, 0, 64*testCacheMiB, 1<<30, a)
	if grant.GrantedDelta != 64*testCacheMiB || grant.Reservation != 64*testCacheMiB {
		t.Fatalf("grant: %+v", grant)
	}
	r := reservationByTokenForTest(t, s, tok)
	if r.ReservationMemory != 256<<20 || r.CacheReservation != 64*testCacheMiB {
		t.Fatalf("cache polluted Guest reservation: %+v", r)
	}
	if got := s.ResourceSnapshot().Reserved.MemoryBytes; got != baseline+64*testCacheMiB {
		t.Fatalf("cache not in pool: got=%d want=%d", got, baseline+64*testCacheMiB)
	}
	// A physically completed shrink returns exactly that amount. Replaying
	// the lower baseline is idempotent even if the original ACK was lost.
	shrunk := mustCacheGrant(t, s, tok, 16*testCacheMiB, 0, 1<<30, a)
	replay := mustCacheGrant(t, s, tok, 16*testCacheMiB, 0, 1<<30, a)
	if shrunk.Reservation != 16*testCacheMiB || replay.Reservation != shrunk.Reservation ||
		s.ResourceSnapshot().Reserved.MemoryBytes != baseline+16*testCacheMiB {
		t.Fatalf("bad shrink / replay: %+v %+v", shrunk, replay)
	}
	if _, _, err := s.ReconcileCacheBudget(tok, 24*testCacheMiB, 0, 1<<30, a); err == nil {
		t.Fatal("cache baseline greater than granted allocation accepted")
	}
	if _, _, err := s.ReconcileCacheBudget(tok, 16*testCacheMiB, 0, 2<<30, a); err == nil {
		t.Fatal("client changed immutable maximum")
	}
	if _, found, err := s.Release(tok); err != nil || !found {
		t.Fatalf("release found=%v err=%v", found, err)
	}
	if got := s.ResourceSnapshot().Reserved.MemoryBytes; got != 0 {
		t.Fatalf("cache leaked after release: %d", got)
	}
}

func TestCacheGrantOptionalCapAndDonorReleaseBeforeRegrant(t *testing.T) {
	s := makeState(4<<30, 0)
	s.CachePoolCapacity = 64 * testCacheMiB
	a := allocatorCacheTest()
	first := admitCacheTest(t, s, "a", 1<<30)
	second := admitCacheTest(t, s, "b", 1<<30)
	if g := mustCacheGrant(t, s, first, 0, 48*testCacheMiB, 1<<30, a); g.GrantedDelta != 48*testCacheMiB {
		t.Fatalf("first grant: %+v", g)
	}
	if g := mustCacheGrant(t, s, second, 0, 32*testCacheMiB, 1<<30, a); g.GrantedDelta != 16*testCacheMiB {
		t.Fatalf("cap partial grant: %+v", g)
	}
	if g := mustCacheGrant(t, s, second, 16*testCacheMiB, 16*testCacheMiB, 1<<30, a); g.GrantedDelta != 0 {
		t.Fatalf("grant exceeded shared cap: %+v", g)
	}
	// New demand is recorded before the node preempts another sandbox.
	time.Sleep(450 * time.Millisecond)
	_ = mustCacheGrant(t, s, second, 16*testCacheMiB, 16*testCacheMiB, 1<<30, a)
	target, found := s.CacheTarget(first)
	if !found || target != 32*testCacheMiB {
		t.Fatalf("node failed to request reclaim: target=%d found=%v", target, found)
	}
	// The target alone must not decrease reserved pool memory.
	if s.reservedCache != 64*testCacheMiB {
		t.Fatal("reclaim command was prematurely counted as returned memory")
	}
	_ = mustCacheGrant(t, s, first, 32*testCacheMiB, 0, 1<<30, a)
	target, _ = s.CacheTarget(first)
	if target != 0 {
		t.Fatalf("node kept acknowledged reclaim target %d", target)
	}
	if g := mustCacheGrant(t, s, second, 16*testCacheMiB, 16*testCacheMiB, 1<<30, a); g.GrantedDelta != 16*testCacheMiB {
		t.Fatalf("regrant after physical return: %+v", g)
	}
	if s.reservedCache != 64*testCacheMiB {
		t.Fatalf("unbalanced cache charges: %d", s.reservedCache)
	}
}

func TestCacheProvisionalRecoveryIncludesMaximumAndSyncReconciles(t *testing.T) {
	s := makeState(4<<30, 0)
	feat := []string{resource.FeatureCacheBudgetV1}
	const capMem = 512 << 20
	const capCache = 512 << 20
	if err := s.InstallProvisional(ProvisionalSpec{
		SandboxID: "restore", CgroupPath: "/cg/restore",
		Capacity:              Resources{MemoryBytes: capMem, CPUMilli: 1000},
		ConfiguredAllocatable: Resources{MemoryBytes: 128 << 20, CPUMilli: 1000},
		ReservationMemory:     capMem, CacheReservation: capCache, CacheMax: capCache,
		InitialBudget: capMem, RecoverySource: RecoveryLease, RecoveryKey: "lease:restore",
		ClientFeatures: feat,
	}); err != nil {
		t.Fatal(err)
	}
	if got := s.ResourceSnapshot().Reserved.MemoryBytes; got != capMem+capCache {
		t.Fatalf("recovery failed to charge cached maximum: %d", got)
	}
	if _, _, err := s.Sync(SyncSpec{
		SandboxID: "restore", Token: "new-token", CgroupPath: "/cg/restore",
		Capacity:              Resources{MemoryBytes: capMem, CPUMilli: 1000},
		ConfiguredAllocatable: Resources{MemoryBytes: 128 << 20, CPUMilli: 1000},
		ReservationMemory:     256 << 20, CacheReservation: 128 << 20, CacheMax: capCache,
		Settled: true, ClientFeatures: feat,
	}); err != nil {
		t.Fatal(err)
	}
	if got := s.ResourceSnapshot().Reserved.MemoryBytes; got != 384<<20 {
		t.Fatalf("state sync total charge: %d", got)
	}
}

func TestCacheGrantResponseLostReconcilesWithoutDoubleCharge(t *testing.T) {
	s := makeState(4<<30, 0)
	tok := admitCacheTest(t, s, "lost-grant", 512<<20)
	a := allocatorCacheTest()
	first := mustCacheGrant(t, s, tok, 0, 32*testCacheMiB, 512<<20, a)
	if first.GrantedDelta != 32*testCacheMiB {
		t.Fatal(first)
	}
	// The original response is lost. No physical payloads were created
	// after the original local 0 baseline. Reuse the same absolute baseline
	// and grant no more than a single live reservation.
	retry := mustCacheGrant(t, s, tok, 0, 0, 512<<20, a)
	if retry.Reservation != 0 || s.reservedCache != 0 {
		t.Fatalf("ambiguous grow retained an unconsumed grant: result=%+v cache=%d", retry, s.reservedCache)
	}
	second := mustCacheGrant(t, s, tok, 0, 32*testCacheMiB, 512<<20, a)
	if second.Reservation != 32*testCacheMiB || s.reservedCache != 32*testCacheMiB {
		t.Fatalf("retry double charged cache: result=%+v cache=%d", second, s.reservedCache)
	}
	// After actual host page reclamation a lost shrink response is safely
	// reissued as an absolute shrink, never a new Guest budget mutation.
	once := mustCacheGrant(t, s, tok, 8*testCacheMiB, 0, 512<<20, a)
	twice := mustCacheGrant(t, s, tok, 8*testCacheMiB, 0, 512<<20, a)
	if once.Reservation != 8*testCacheMiB || twice.Reservation != 8*testCacheMiB ||
		s.ResourceSnapshot().Reserved.MemoryBytes != 256<<20+8*testCacheMiB {
		t.Fatalf("ambiguous shrink failed idempotence: first=%+v repeat=%+v", once, twice)
	}
}

func TestCacheInitialAdmitReplayIsIdempotent(t *testing.T) {
	s := makeState(4<<30, 0)
	spec := AdmitSpec{
		Token: "token-1", SandboxID: "replay-cache", PeerPID: 100, CgroupPath: "/cg/replay-cache",
		Capacity:              Resources{MemoryBytes: 512 << 20, CPUMilli: 1000},
		ConfiguredAllocatable: Resources{MemoryBytes: 128 << 20, CPUMilli: 1000},
		InitialBudget:         256 << 20, CacheMax: 512 << 20, InitialCache: 8 << 20,
		ClientFeatures: []string{resource.FeatureCacheBudgetV1},
	}
	if _, _, err := s.Admit(spec); err != nil {
		t.Fatal(err)
	}
	before := s.ResourceSnapshot().Reserved.MemoryBytes
	if !s.CanReplayAdmit(spec) {
		t.Fatal("original cache grant not replayable")
	}
	spec.Token = "token-2"
	if _, _, err := s.Admit(spec); err != nil {
		t.Fatal(err)
	}
	if got := s.ResourceSnapshot().Reserved.MemoryBytes; got != before || got != 264<<20 {
		t.Fatalf("replayed Admit double charged: %d", got)
	}
	if r := reservationForTest(t, s, "replay-cache"); r.CacheReservation != 8<<20 {
		t.Fatalf("cache grant changed after replay %+v", r)
	}
}

// Test the exact compact E2E admission budget after accounting for the
// additional 8MiB physical diff_cow grant per sandbox. The old 400MiB
// fixture cannot admit four; 432MiB admits four but still rejects a fifth.
func TestCacheCompactAdmissionFourButNotFive(t *testing.T) {
	const miB = uint64(1 << 20)
	for _, tc := range []struct {
		physicalMiB  int
		wantAdmitted int
	}{
		{400, 3}, {432, 4},
	} {
		t.Run(fmt.Sprintf("%dMiB", tc.physicalMiB), func(t *testing.T) {
			s := makeState(uint64(tc.physicalMiB)*miB, 80*miB)
			s.Wm.StartupFactor = 1
			admitted := 0
			for i := 1; i <= 5; i++ {
				sid := fmt.Sprintf("resource-admission-%d", i)
				launch := LaunchAdmission{Operation: OperationCreate}
				result := s.AnalyzeLaunchWithCache(sid, 64*miB, 8*miB, launch)
				if result.Status != OutcomeAdmitted {
					break
				}
				_, _, err := s.Admit(AdmitSpec{
					SandboxID: sid, Token: "token-" + sid, CgroupPath: "/cg/" + sid,
					Capacity:              Resources{MemoryBytes: 256 * miB, CPUMilli: 1000},
					ConfiguredAllocatable: Resources{MemoryBytes: 64 * miB, CPUMilli: 1000},
					InitialBudget:         64 * miB, InitialCache: 8 * miB, CacheMax: 32 * miB,
					ClientFeatures: []string{resource.FeatureCacheBudgetV1},
					Admission:      &launch,
				})
				if err != nil {
					t.Fatal(err)
				}
				admitted++
			}
			if admitted != tc.wantAdmitted {
				t.Fatalf("admitted %d, want %d under %dMiB physical memory", admitted, tc.wantAdmitted, tc.physicalMiB)
			}
			want := uint64(admitted) * 72 * miB
			if got := s.ResourceSnapshot().Reserved.MemoryBytes; got != want {
				t.Fatalf("reserved %d, want Guest+cache %d", got, want)
			}
		})
	}
}

func TestCacheGrantCannotSpendProtectedGuestHeadroom(t *testing.T) {
	const miB = uint64(1 << 20)
	s := makeState(4<<30, 0)
	token := admitCacheTest(t, s, "cache-borrower", 1<<30)
	a := allocatorCacheTest()
	baseline := s.ResourceSnapshot().Reserved.MemoryBytes
	pool := s.AllocatablePool.MemoryBytes
	emergency := scaleUint64Floor(pool, s.Wm.EmergencyFactor)
	available := pool - baseline - emergency
	if available < 32*miB {
		t.Fatalf("unexpected available memory %d", available)
	}
	s.mu.Lock()
	s.initPressureLocked()
	now := s.pressure.clock()
	s.failDemandLocked("grow:protected", "protected", "guest-protected", available, 0, false, now)
	s.mu.Unlock()
	deny := mustCacheGrant(t, s, token, 0, 8*miB, 1<<30, a)
	if deny.GrantedDelta != 0 || s.reservedCache != 0 {
		t.Fatalf("cache spent protected Guest reservation: %+v reservedCache=%d", deny, s.reservedCache)
	}
	s.ForgetDemand("grow:protected")
	grant := mustCacheGrant(t, s, token, 0, 8*miB, 1<<30, a)
	if grant.GrantedDelta != 8*miB {
		t.Fatalf("cache remained blocked after protected demand cleared: %+v", grant)
	}
}

func TestCacheBlockedInitialAdmissionRequestsDonorReclaim(t *testing.T) {
	const miB = uint64(1 << 20)
	s := makeState(4<<30, 0)
	s.Wm.StartupFactor = 0.50
	s.CachePoolCapacity = 16 * miB
	allocator := allocatorCacheTest()
	donor := admitCacheTest(t, s, "donor", 1<<30)
	if got := mustCacheGrant(t, s, donor, 0, 16*miB, 1<<30, allocator); got.GrantedDelta != 16*miB {
		t.Fatalf("donor grant: %+v", got)
	}
	wait := LaunchAdmission{Operation: OperationCreate, Identity: "admit-cache-attempt"}
	oc := s.AnalyzeLaunchWithCache("waiting", 64*miB, 8*miB, wait)
	if oc.Status != OutcomeShortTermBlock || oc.Block != BlockedByMainBudget {
		t.Fatalf("cache-cap-only admission did not block: %+v", oc)
	}
	s.RecordAdmissionWaitWithCache("waiting", wait, 64*miB, 8*miB)
	target, found := s.CacheTarget(donor)
	if !found || target != 8*miB {
		t.Fatalf("queued sandbox did not trigger donor reclaim: %d found=%v", target, found)
	}
	if got := s.ResourceSnapshot().Reserved.MemoryBytes; got != 272*miB {
		t.Fatalf("reclaim directive prematurely returned donor memory: %d", got)
	}
	if demand := s.PressureSnapshot().Demands; len(demand) != 0 {
		t.Fatalf("cache-only shortage fabricated Guest pressure demand: %+v", demand)
	}
	// Donor has actually released host pages, then reports a smaller
	// trusted baseline. Node reuses only that confirmed balance.
	if result := mustCacheGrant(t, s, donor, 8*miB, 0, 1<<30, allocator); result.Reservation != 8*miB {
		t.Fatalf("donor failed to return physical memory: %+v", result)
	}
	if got := s.AnalyzeLaunchWithCache("waiting", 64*miB, 8*miB, wait); got.Status != OutcomeAdmitted {
		t.Fatalf("queued admission still blocked after confirmed return: %+v", got)
	}
	_, _, err := s.Admit(AdmitSpec{
		SandboxID: "waiting", Token: "waiting-token", CgroupPath: "/cg/waiting",
		Capacity:              Resources{MemoryBytes: 256 * miB, CPUMilli: 1000},
		ConfiguredAllocatable: Resources{MemoryBytes: 64 * miB, CPUMilli: 1000},
		InitialBudget:         64 * miB, InitialCache: 8 * miB, CacheMax: 256 * miB,
		ClientFeatures: []string{resource.FeatureCacheBudgetV1}, Admission: &wait,
	})
	if err != nil {
		t.Fatalf("reclaimed bytes not available for waiting Admit: %v", err)
	}
	if s.reservedCache != 16*miB {
		t.Fatalf("cache accounting did not transfer between sandboxes: %d", s.reservedCache)
	}
}

// Guest resume can use an emergency memory reserve, but a discretionary
// cache grant must never use that exemption.
func TestSavedSourceInitialCacheCannotUseGuestEmergencyMemory(t *testing.T) {
	const miB = uint64(1 << 20)
	s := makeState(4<<30, 0)
	pool := s.AllocatablePool.MemoryBytes
	emergency := scaleUint64Floor(pool, s.Wm.EmergencyFactor)
	if emergency < 16*miB {
		t.Fatalf("test pool has insufficient emergency reserve %d", emergency)
	}
	s.mu.Lock()
	s.reservedMemory = pool - emergency
	s.mu.Unlock()
	resume := LaunchAdmission{Operation: OperationResume, SavedSource: true, Accepted: true, Identity: "resume-with-cache"}
	guest := emergency - 8*miB
	if got := s.AnalyzeLaunchWithCache("saved", guest, 0, resume); got.Status != OutcomeAdmitted {
		t.Fatalf("saved-source Guest lost existing emergency exemption: %+v", got)
	}
	if got := s.AnalyzeLaunchWithCache("saved", guest, 8*miB, resume); got.Status != OutcomeShortTermBlock || got.Block != BlockedByMainBudget {
		t.Fatalf("saved-source cache improperly consumed emergency memory: %+v", got)
	}
}

func TestSavedSourceInitialCacheShortageRequestsDonor(t *testing.T) {
	const miB = uint64(1 << 20)
	s := makeState(4<<30, 0)
	pool := s.AllocatablePool.MemoryBytes
	emergency := scaleUint64Floor(pool, s.Wm.EmergencyFactor)
	if emergency < 16*miB {
		t.Fatalf("insufficient emergency headroom: %d", emergency)
	}
	a := allocatorCacheTest()
	donor := admitCacheTest(t, s, "cache-donor", 1<<30)
	if got := mustCacheGrant(t, s, donor, 0, 16*miB, 1<<30, a); got.GrantedDelta != 16*miB {
		t.Fatalf("donor grant: %+v", got)
	}
	// Leave only emergency memory free. Guest resume is entitled to use it,
	// while its initial cache needs the donor to return physical memory.
	s.mu.Lock()
	s.reservedMemory = pool - emergency
	s.mu.Unlock()
	resume := LaunchAdmission{Operation: OperationResume, SavedSource: true, Accepted: true, Identity: "saved-cache-wait"}
	guest := emergency - 8*miB
	if got := s.AnalyzeLaunchWithCache("saved", guest, 8*miB, resume); got.Status != OutcomeShortTermBlock || got.Block != BlockedByMainBudget {
		t.Fatalf("expected cache-only shortage: %+v", got)
	}
	s.RecordAdmissionWaitWithCache("saved", resume, guest, 8*miB)
	target, found := s.CacheTarget(donor)
	if !found || target != 8*miB {
		t.Fatalf("cache-only saved-source wait did not request donor reclaim: target=%d found=%v", target, found)
	}
	if got := s.reservedCache; got != 16*miB {
		t.Fatalf("unacknowledged reclaim prematurely returned memory: %d", got)
	}
}

func TestPendingReclaimDoesNotEvictMultipleDonorsForSameShortage(t *testing.T) {
	const miB = uint64(1 << 20)
	s := makeState(4<<30, 0)
	s.Wm.StartupFactor = .5
	s.CachePoolCapacity = 72 * miB
	a := allocatorCacheTest()
	donors := []string{
		admitCacheTest(t, s, "donor-a", 1<<30),
		admitCacheTest(t, s, "donor-b", 1<<30),
		admitCacheTest(t, s, "donor-c", 1<<30),
	}
	for _, donor := range donors {
		if result := mustCacheGrant(t, s, donor, 0, 24*miB, 1<<30, a); result.GrantedDelta != 24*miB {
			t.Fatalf("grant donor %s: %+v", donor, result)
		}
	}
	wait := LaunchAdmission{Operation: OperationCreate, Identity: "admit-cache-pending"}
	for round := 0; round < 5; round++ {
		s.RecordAdmissionWaitWithCache("wait", wait, 64*miB, 8*miB)
	}
	targetCount := 0
	targetIndex := -1
	for i, donor := range donors {
		target, found := s.CacheTarget(donor)
		if !found {
			t.Fatalf("lost donor %s", donor)
		}
		if target != 0 {
			targetCount++
			targetIndex = i
			if target != 16*miB {
				t.Fatalf("unexpected reclaim amount for donor %s: %d", donor, target)
			}
		}
	}
	if targetCount != 1 || s.reservedCache != 72*miB {
		t.Fatalf("five identical retries overreclaimed %d donors or changed reservation %d", targetCount, s.reservedCache)
	}
	_ = mustCacheGrant(t, s, donors[targetIndex], 16*miB, 0, 1<<30, a)
	if got := s.AnalyzeLaunchWithCache("wait", 64*miB, 8*miB, wait); got.Status != OutcomeAdmitted {
		t.Fatalf("pending reclaim did not satisfy original admission: %+v", got)
	}
}

func TestGuestGrowthShortageTriggersCacheReclaimBeforePause(t *testing.T) {
	const miB = uint64(1 << 20)
	for _, source := range []string{"other sandbox", "own sandbox"} {
		t.Run(source, func(t *testing.T) {
			s := makeState(4<<30, 0)
			s.Wm.EmergencyFactor = 0
			a := allocatorCacheTest()
			growing := admitCacheTest(t, s, "guest-growing", 1<<30)
			donor := growing
			if source == "other sandbox" {
				donor = admitCacheTest(t, s, "cache-donor", 1<<30)
			}
			if grant := mustCacheGrant(t, s, donor, 0, 128*miB, 1<<30, a); grant.GrantedDelta != 128*miB {
				t.Fatalf("donor setup grant: %+v", grant)
			}
			// Set the pool to its current charge so only actual returns can fund growth.
			s.AllocatablePool.MemoryBytes = s.reservedMemory
			request := func() uint64 {
				t.Helper()
				result, found, err := s.ReconcileAndGrant(growing, 256*miB, 64*miB, UrgencyNormal, a)
				if err != nil || !found {
					t.Fatalf("Guest growth reconcile: %+v found=%v err=%v", result, found, err)
				}
				return result.Decision.GrantedDelta
			}
			if delta := request(); delta != 0 {
				t.Fatalf("Guest received %d bytes from a full pool", delta)
			}
			target, found := s.CacheTarget(donor)
			if !found || target == 0 || target >= 128*miB {
				t.Fatalf("Guest shortage failed to request donor cache shrink: %d found=%v", target, found)
			}
			if s.reservedCache != 128*miB {
				t.Fatalf("node freed pending donor target without physical return: %d", s.reservedCache)
			}
			mustCacheGrant(t, s, donor, target, 0, 1<<30, a)
			if delta := request(); delta != 128*miB-target {
				t.Fatalf("Guest received %d bytes after donor returned %d", delta, 128*miB-target)
			}
		})
	}
}
