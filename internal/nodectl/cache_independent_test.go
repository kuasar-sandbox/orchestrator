package nodectl

import (
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/config"
	"github.com/kuasar-sandbox/sandboxer/pkg/resource"
)

func TestCacheGrantDoesNotTouchGuestPolicy(t *testing.T) {
	s := makeState(4<<30, 0)
	token := admitCacheTest(t, s, "independent", 8<<30)
	a := allocatorCacheTest()
	a.tokens = 0                                     // Guest growth cannot currently spend a token.
	s.AllocatablePool.MemoryBytes = s.reservedMemory // Guest pool is full.
	s.mu.Lock()
	s.initPressureLocked()
	s.failDemandLocked("grow:waiting", "waiting", "identity", 64<<20, 0, false, s.pressure.clock())
	beforePressure := s.pressure
	beforeDemands := map[string]memoryDemand{}
	for key, demand := range s.pressure.demands {
		beforeDemands[key] = *demand
	}
	beforeMemory := s.reservedMemory
	beforeTokens, beforeRefill := a.tokens, a.lastRefill
	s.mu.Unlock()

	// No cache capacity was configured. Cache grants exceed the entire Guest
	// memory pool without borrowing its budget or changing its policy state.
	got := mustCacheGrant(t, s, token, 0, 1<<30, 8<<30, a)
	if got.GrantedDelta != 512<<20 || s.reservedMemory != beforeMemory {
		t.Fatalf("independent cache grant: %+v Guest=%d", got, s.reservedMemory)
	}
	if len(s.cacheDemandSince) != 0 {
		t.Fatal("finite grant batch manufactured shortage in an unlimited cache pool")
	}
	mustCacheGrant(t, s, token, 256<<20, 0, 8<<30, a)
	if s.reservedCache != 256<<20 || s.reservedMemory != beforeMemory {
		t.Fatal("cache return crossed ledgers")
	}
	if s.pressure.record != beforePressure.record || s.pressure.beneficiary != beforePressure.beneficiary ||
		s.pressure.lastReservationChange != beforePressure.lastReservationChange {
		t.Fatal("cache transaction changed Guest watermarks, protection or stability age")
	}
	for key, before := range beforeDemands {
		if after := s.pressure.demands[key]; after == nil || *after != before {
			t.Fatal("cache transaction changed Guest failure rounds or demand")
		}
	}
	if a.tokens != beforeTokens || a.lastRefill != beforeRefill || len(a.history) != 0 {
		t.Fatal("cache transaction consumed Guest allocator tokens or history")
	}
}

func TestGuestGrowthIgnoresLargeCacheAllocation(t *testing.T) {
	s := makeState(4<<30, 0)
	token := admitCacheTest(t, s, "large-cache", 2<<30)
	mustCacheGrant(t, s, token, 0, 512<<20, 2<<30, nil)
	s.AllocatablePool.MemoryBytes = s.reservedMemory + (64 << 20)
	s.Wm.EmergencyFactor = 0
	grant, found, err := s.ReconcileAndGrant(token, 256<<20, 64<<20, UrgencyNormal, allocatorCacheTest())
	if err != nil || !found || grant.Decision.GrantedDelta != 64<<20 {
		t.Fatalf("cache blocked Guest-only headroom: %+v found=%v err=%v", grant, found, err)
	}
	if s.reservedMemory != 320<<20 || s.reservedCache != 512<<20 {
		t.Fatal("Guest grow changed cache ledger or included cache in memory")
	}
}

func TestCacheSyncCannotExchangeGuestAndCacheAuthorization(t *testing.T) {
	for _, test := range []struct {
		name         string
		guest, cache uint64
		accept       bool
	}{
		{"cache cannot fund Guest", 320 << 20, 64 << 20, false},
		{"Guest cannot fund cache", 128 << 20, 256 << 20, false},
		{"both shrink", 128 << 20, 64 << 20, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			s := makeState(4<<30, 0)
			token := admitCacheTest(t, s, "restore", 512<<20)
			mustCacheGrant(t, s, token, 0, 128<<20, 512<<20, allocatorCacheTest())
			before := s.ResourceSnapshot()
			_, _, err := s.Sync(SyncSpec{
				SandboxID: "restore", Token: "replacement", CgroupPath: "/cg/restore",
				Capacity:              Resources{MemoryBytes: 2 << 30, CPUMilli: 1000},
				ConfiguredAllocatable: Resources{MemoryBytes: 128 << 20, CPUMilli: 1000},
				ReservationMemory:     test.guest, CacheReservation: test.cache, CacheMax: 512 << 20,
				Settled: true, ClientFeatures: []string{resource.FeatureCacheBudgetV1},
			})
			if (err == nil) != test.accept {
				t.Fatalf("Sync accept=%v err=%v", test.accept, err)
			}
			if !test.accept {
				if after := s.ResourceSnapshot(); after != before {
					t.Fatalf("rejected exchange mutated aggregate: before=%+v after=%+v", before, after)
				}
				if _, found := s.Heartbeat(token, 0, time.Now()); !found {
					t.Fatal("rejected exchange invalidated existing session")
				}
			}
		})
	}
}

func TestCacheRecoveryRetainsOverQuotaUntilConfirmedReturn(t *testing.T) {
	s := makeState(4<<30, 0)
	s.CachePoolCapacity = 32 << 20 // Lowered since this execution was admitted.
	features := []string{resource.FeatureCacheBudgetV1}
	if err := s.InstallProvisional(ProvisionalSpec{
		SandboxID: "recovered", CgroupPath: "/cg/recovered", RecoverySource: RecoveryLease, RecoveryKey: "lease:recovered",
		Capacity: Resources{MemoryBytes: 1 << 30}, ReservationMemory: 512 << 20, InitialBudget: 512 << 20,
		CacheReservation: 256 << 20, CacheMax: 256 << 20, ClientFeatures: features,
	}); err != nil {
		t.Fatal(err)
	}
	if s.reservedCache != 256<<20 || s.reservedMemory != 512<<20 {
		t.Fatal("over-quota recovered cache was clipped or folded into Guest")
	}
	if _, _, err := s.Sync(SyncSpec{
		SandboxID: "recovered", Token: "recovered-token", CgroupPath: "/cg/recovered", Settled: true,
		Capacity: Resources{MemoryBytes: 1 << 30}, ReservationMemory: 256 << 20,
		CacheReservation: 128 << 20, CacheMax: 256 << 20, ClientFeatures: features,
	}); err != nil {
		t.Fatal(err)
	}
	if got := mustCacheGrant(t, s, "recovered-token", 128<<20, 4<<20, 256<<20, nil); got.GrantedDelta != 0 || s.reservedCache != 128<<20 {
		t.Fatalf("over-quota recovery granted growth or lost charge: %+v", got)
	}
	if got := mustCacheGrant(t, s, "recovered-token", 16<<20, 4<<20, 256<<20, nil); got.GrantedDelta != 4<<20 || s.reservedCache != 20<<20 {
		t.Fatalf("confirmed return did not restore independent cache headroom: %+v", got)
	}
}

func TestCacheAggregateFailurePreservesReservations(t *testing.T) {
	for _, underflow := range []bool{false, true} {
		t.Run(map[bool]string{false: "overflow", true: "underflow"}[underflow], func(t *testing.T) {
			s := makeState(4<<30, 0)
			token := admitCacheTest(t, s, "stable", 512<<20)
			mustCacheGrant(t, s, token, 0, 128<<20, 512<<20, nil)
			if underflow {
				s.reservedCache = 64 << 20
			} else {
				s.reservedCache = math.MaxUint64 - (4 << 20)
			}
			before, beforeReservation := s.ResourceSnapshot(), reservationByTokenForTest(t, s, token)
			var err error
			if underflow {
				_, _, err = s.Sync(SyncSpec{
					SandboxID: "stable", Token: "replacement", CgroupPath: "/cg/stable", Settled: true,
					Capacity: beforeReservation.Capacity, ConfiguredAllocatable: beforeReservation.ConfiguredAllocatable,
					ReservationMemory: beforeReservation.ReservationMemory, CacheReservation: 64 << 20,
					CacheMax: beforeReservation.CacheMax, ClientFeatures: beforeReservation.ClientFeatures,
				})
			} else {
				err = s.InstallProvisional(ProvisionalSpec{
					SandboxID: "overflow", CgroupPath: "/cg/overflow", RecoveryKey: "overflow",
					Capacity: Resources{MemoryBytes: 1 << 30}, ReservationMemory: 128 << 20,
					CacheReservation: 8 << 20, CacheMax: 8 << 20, ClientFeatures: []string{resource.FeatureCacheBudgetV1},
				})
			}
			if err == nil {
				t.Fatal("invalid cache aggregate accepted")
			}
			if s.ResourceSnapshot() != before || !reflect.DeepEqual(reservationByTokenForTest(t, s, token), beforeReservation) {
				t.Fatal("failed cache replacement partially mutated aggregate or session")
			}
		})
	}
}

func TestIndependentCacheConfigurationCombinations(t *testing.T) {
	capacity, threshold, zero := "8GiB", "128MiB", "0"
	for _, test := range []struct {
		name                string
		capacity, threshold *string
	}{
		{"default", nil, nil}, {"cap only", &capacity, nil},
		{"threshold only", nil, &threshold}, {"both", &capacity, &threshold},
		{"explicit zero", nil, &zero},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := &config.ResourceListenConfig{Pools: []config.ResourcePoolConfig{{Name: "default", CacheCapacity: test.capacity, CacheLowWatermark: test.threshold}}}
			cfg.Resources.PhysicalMemory, cfg.Resources.HostReserved.Memory = "4GiB", "1GiB"
			resolved, err := Resolve(cfg)
			if err != nil {
				t.Fatal(err)
			}
			s := NewPoolState(resolved)
			view := s.PoolSnapshots()[0]
			if (view.CacheCapacity == 0) != (test.capacity == nil) || (view.CacheLowWatermark == nil) != (test.threshold == nil) {
				t.Fatalf("configuration presence lost: %+v", view)
			}
			if test.capacity != nil && view.CacheCapacity != 8<<30 {
				t.Fatal("cache cap clamped to Guest memory")
			}
			if view.CacheLow != (test.threshold != nil) {
				t.Fatal("unconfigured gate enabled, or explicit zero disabled")
			}
			if test.threshold != nil {
				s.reservedCache = *s.CacheLowWatermark
				if !s.PoolSnapshots()[0].CacheLow {
					t.Fatal("threshold equality not low")
				}
				s.reservedCache++
				if s.PoolSnapshots()[0].CacheLow {
					t.Fatal("cache above threshold still low")
				}
			}
		})
	}
}

func TestCacheAdmissionWaitUsesIndependentReasonAndIdentity(t *testing.T) {
	s := makeState(4<<30, 0)
	s.Wm.StartupFactor = .5
	s.CachePoolCapacity = 8 << 20
	token := admitCacheTest(t, s, "holder", 64<<20)
	mustCacheGrant(t, s, token, 0, 8<<20, 64<<20, nil)
	wait := LaunchAdmission{Operation: OperationCreate, Identity: "cache-wait"}
	before := s.ResourceSnapshot()
	_, _, err := s.Admit(AdmitSpec{
		SandboxID: "waiting", Token: "waiting-token", CgroupPath: "/cg/waiting",
		Capacity: Resources{MemoryBytes: 256 << 20}, InitialBudget: 64 << 20,
		InitialCache: 4 << 20, CacheMax: 64 << 20, ClientFeatures: []string{resource.FeatureCacheBudgetV1}, Admission: &wait,
	})
	if err == nil || before != s.ResourceSnapshot() {
		t.Fatal("cache rejection half-committed Guest admission")
	}
	s.RecordAdmissionWaitWithCache("waiting", wait, 64<<20, 4<<20)
	if len(s.PressureSnapshot().Demands) != 0 || s.admissionCacheDemand["admit:waiting"].amount != 4<<20 {
		t.Fatal("cache shortage failed independent demand accounting")
	}
	s.ForgetAdmissionWait("waiting", "stale-identity")
	if len(s.admissionCacheDemand) != 1 {
		t.Fatal("stale cancellation removed successor demand")
	}
	s.ForgetAdmissionWait("waiting", wait.Identity)
	if len(s.admissionCacheDemand) != 0 {
		t.Fatal("cancelled cache demand retained")
	}
}
