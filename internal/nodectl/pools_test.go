package nodectl

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/config"
)

const poolMiB = uint64(1 << 20)

func twoPoolState(t *testing.T) *State {
	t.Helper()
	w := Watermarks{LowFactor: .7, HighFactor: .85, EmergencyFactor: .05, StartupFactor: .5}
	r := &Resolved{PhysicalMemory: 2000 * poolMiB, Watermarks: w, Allocator: AllocatorPolicy{MemoryGrantPerSecBytes: 2000 * poolMiB, MinGrantStep: 1, MaxGrantStep: 1000 * poolMiB}}
	for _, name := range []string{"numa0", "numa1"} {
		r.Pools = append(r.Pools, ResolvedPool{Name: name, MemoryGrantPerSecBytes: 1000 * poolMiB, Capacity: Resources{MemoryBytes: 4000 * poolMiB, CPUMilli: 8000}, Allocatable: Resources{MemoryBytes: 1000 * poolMiB, CPUMilli: 4000}, Watermarks: w, CPUWatermarks: w})
	}
	s := NewPoolState(r)
	if err := s.ConfigurePressure(DefaultPressurePolicy(), PressureRecord{}); err != nil {
		t.Fatal(err)
	}
	return s
}

func placePoolTest(t *testing.T, s *State, pool, sid string, capacity Resources) Placement {
	t.Helper()
	c, err := s.ReservePlacement(sid, "run-"+sid, capacity, OperationRecovery, "", func(name string) bool { return name == pool })
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func admitPoolTest(t *testing.T, s *State, pool, sid string, capacity, runtime Resources, budget uint64) {
	t.Helper()
	placePoolTest(t, s, pool, sid, capacity)
	if _, _, err := s.Admit(AdmitSpec{SandboxID: sid, Token: "token-" + sid, Capacity: capacity, ConfiguredAllocatable: runtime, InitialBudget: budget}); err != nil {
		t.Fatal(err)
	}
}

func TestResourcePoolLegacyFormulaAndExplicitNetMemory(t *testing.T) {
	c := &config.ResourceListenConfig{}
	c.Resources.PhysicalMemory, c.Resources.HostReserved.Memory = "1000MiB", "100MiB"
	c.Resources.PhysicalCPU, c.Resources.HostReserved.CPU = "8", 1
	r, err := Resolve(c)
	if err != nil {
		t.Fatal(err)
	}
	s := NewPoolState(r)
	legacy := NewState(r.PhysicalMemory, r.PhysicalCPU, r.HostReserved.MemoryBytes, r.HostReserved.CPUMilli, r.Watermarks)
	if s.AllocatablePool.MemoryBytes != legacy.AllocatablePool.MemoryBytes || s.startupPoolBytesLocked() != legacy.startupPoolBytesLocked() {
		t.Fatal("legacy memory formula changed")
	}
	if s.AllocatablePool.CPUMilli != 0 {
		t.Fatal("physical CPU became a runtime water limit")
	}
	mem := "500MiB"
	c.Pools = []config.ResourcePoolConfig{{Name: "default", Allocatable: config.ResourceAllocatable{Memory: &mem}}}
	r, err = Resolve(c)
	if err != nil {
		t.Fatal(err)
	}
	if NewPoolState(r).AllocatablePool.MemoryBytes != 500*poolMiB {
		t.Fatal("net memory was deducted twice")
	}
}

func TestResourcePoolUtilizationAndTargetAdmission(t *testing.T) {
	s := twoPoolState(t)
	capacity := Resources{MemoryBytes: 1000 * poolMiB, CPUMilli: 4000}
	admitPoolTest(t, s, "numa0", "a", capacity, Resources{CPUMilli: 4000}, 100*poolMiB)
	admitPoolTest(t, s, "numa1", "b", capacity, Resources{CPUMilli: 500}, 200*poolMiB)
	if s.ResourceSnapshot().Zone != ZoneCritical {
		t.Fatal("CPU critical missing from node summary")
	}
	if err := s.CheckOperation(OperationCreate); err != nil {
		t.Fatalf("critical source blocked healthy destination: %v", err)
	}
	claim, err := s.ReservePlacement("new", "new-run", Resources{MemoryBytes: 1000 * poolMiB, CPUMilli: 1000}, OperationCreate, "", nil)
	if err != nil || claim.Pool != "numa1" {
		t.Fatalf("placement=%+v err=%v", claim, err)
	}
	views := s.PoolSnapshots()
	if views[0].Utilization != 1 || views[0].RawZone != ZoneCritical || views[1].Utilization != .2 {
		t.Fatalf("pool usage=%+v", views)
	}
	if !s.ReleasePlacement("new", "new-run") {
		t.Fatal("cancelled claim not released")
	}
	// Scheduling capacity is declared Capacity, not the smaller runtime CPU.
	_, err = s.ReservePlacement("too-large", "large", Resources{CPUMilli: 5000}, OperationRecovery, "numa0", nil)
	if err == nil {
		t.Fatal("declared CPU scheduling capacity was overcommitted")
	}
}

func TestResourcePoolRuntimeCPUIsConstantAndUnbounded(t *testing.T) {
	for _, allocated := range []uint64{0, 2000} {
		s := twoPoolState(t)
		p := s.PoolStates()["numa0"]
		p.CapacityLimit = Resources{}
		p.AllocatablePool.CPUMilli = 1000
		admitPoolTest(t, s, "numa0", "cpu", Resources{MemoryBytes: 1000 * poolMiB, CPUMilli: 4000}, Resources{CPUMilli: allocated}, 100*poolMiB)
		want := allocated
		if want == 0 {
			want = 4000
		}
		if p.ResourceSnapshot().Reserved.CPUMilli != want {
			t.Fatal("wrong startup CPU charge")
		}
		if _, _, err := s.SetSettled("token-cpu", 0, time.Now()); err != nil {
			t.Fatal(err)
		}
		if p.ResourceSnapshot().Reserved.CPUMilli != want || p.ResourceSnapshot().StartupInFlight != 0 {
			t.Fatal("Settled switched non-memory resources")
		}
		if p.PoolSnapshots()[0].Utilization != float64(want)/1000 {
			t.Fatal("over-100% usage hidden")
		}
		if _, _, err := s.Release("token-cpu"); err != nil {
			t.Fatal(err)
		}
		if p.ResourceSnapshot().Reserved.CPUMilli != 0 {
			t.Fatal("CPU charge leaked")
		}
	}
}

func TestResourcePoolConcurrentCapacityAndExactRollback(t *testing.T) {
	s := twoPoolState(t)
	for _, p := range s.PoolStates() {
		p.CapacityLimit = Resources{MemoryBytes: 10}
	}
	var successes atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			sid := fmt.Sprint(i)
			if _, err := s.ReservePlacement(sid, sid, Resources{MemoryBytes: 1}, OperationCreate, "", nil); err == nil {
				successes.Add(1)
			}
		}(i)
	}
	wg.Wait()
	if successes.Load() != 20 {
		t.Fatalf("successful reservations=%d want20", successes.Load())
	}
	for _, p := range s.PoolSnapshots() {
		if p.Scheduled.MemoryBytes != 10 {
			t.Fatalf("capacity accounting=%+v", p)
		}
	}
	for i := 0; i < 100; i++ {
		sid := fmt.Sprint(i)
		if s.ReleasePlacement(sid, "stale") {
			t.Fatal("stale cancellation released capacity")
		}
		s.ReleasePlacement(sid, sid)
	}
	for _, p := range s.PoolSnapshots() {
		if p.Scheduled != (Resources{}) {
			t.Fatalf("rollback leaked %+v", p)
		}
	}
}

func TestResourcePoolGrowShrinkReleaseAndMove(t *testing.T) {
	s := twoPoolState(t)
	capacity := Resources{MemoryBytes: 1000 * poolMiB, CPUMilli: 1000}
	admitPoolTest(t, s, "numa0", "moving", capacity, Resources{}, 128*poolMiB)
	if err := s.BindResourcePool("moving", "numa1"); err == nil {
		t.Fatal("moved a running reservation")
	}
	if _, _, err := s.SetSettled("token-moving", 0, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.ReconcileAndGrant("token-moving", 128*poolMiB, 64*poolMiB, UrgencyNormal, nil); err != nil {
		t.Fatal(err)
	}
	if s.PoolStates()["numa0"].ResourceSnapshot().Reserved.MemoryBytes != 192*poolMiB || s.PoolStates()["numa1"].ResourceSnapshot().Reserved.MemoryBytes != 0 {
		t.Fatal("grow charged another pool")
	}
	if _, _, err := s.ReconcileAndGrant("token-moving", 64*poolMiB, 0, UrgencyNormal, nil); err != nil {
		t.Fatal(err)
	}
	if s.ReleasePlacement("moving", "run-moving") || s.PoolStates()["numa0"].PoolSnapshots()[0].Scheduled != capacity {
		t.Fatal("released live placement")
	}
	if _, _, err := s.Release("token-moving"); err != nil {
		t.Fatal(err)
	}
	if err := s.RetireExecution("moving", "run-moving"); err != nil {
		t.Fatal(err)
	}
	placePoolTest(t, s, "numa1", "moving", capacity)
	if _, _, err := s.Admit(AdmitSpec{SandboxID: "moving", Token: "successor", Capacity: capacity, InitialBudget: 128 * poolMiB}); err != nil {
		t.Fatal(err)
	}
	if _, found, err := s.Release("token-moving"); err != nil || found {
		t.Fatal("late release touched successor")
	}
	if s.PoolStates()["numa0"].ResourceSnapshot().Reserved.MemoryBytes != 0 || s.PoolStates()["numa1"].ResourceSnapshot().Reserved.MemoryBytes != 128*poolMiB {
		t.Fatal("cross-pool release corrupted ledgers")
	}
}

func TestResourcePoolRecoveryObligationAndCompetingDemandIsolation(t *testing.T) {
	s := twoPoolState(t)
	a, b := s.PoolStates()["numa0"], s.PoolStates()["numa1"]
	if err := a.ConfigurePressure(DefaultPressurePolicy(), PressureRecord{Zone: ZoneCritical, Version: 1}); err != nil {
		t.Fatal(err)
	}
	s.ObserveObligation("saved", 1, "paused", false)
	if a.PressureSnapshot().Zone != ZoneCritical || b.PressureSnapshot().Zone != ZoneGreen {
		t.Fatal("obligation promoted healthy pool")
	}
	a.mu.Lock()
	a.failDemandLocked("admit:other", "other", "run", 900*poolMiB, 0, false, time.Now())
	a.mu.Unlock()
	if !s.TryClaimBackgroundRecovery("saved") {
		t.Fatal("source demand blocked recovery to another pool")
	}
	defer s.ReleaseBackgroundRecovery("saved")
	launch := LaunchAdmission{Operation: OperationRecovery, SavedSource: true, Background: true}
	if s.CanPlace("numa0", "saved", Resources{}, 100*poolMiB, launch) {
		t.Fatal("competing target demand ignored")
	}
	if !s.CanPlace("numa1", "saved", Resources{}, 100*poolMiB, launch) {
		t.Fatal("healthy pool inherited other pool demand")
	}
	if err := b.ConfigurePressure(DefaultPressurePolicy(), PressureRecord{Zone: ZoneCritical, Version: 1}); err != nil {
		t.Fatal(err)
	}
	if !s.CanPlace("numa1", "saved", Resources{}, 1000*poolMiB, launch) {
		t.Fatal("all critical blocked complete-budget recovery")
	}
}

func TestResourcePoolBuilderPhaseReplacesSchedulingCharge(t *testing.T) {
	s := twoPoolState(t)
	capacity := Resources{MemoryBytes: 1000 * poolMiB, CPUMilli: 4000}
	placePoolTest(t, s, "numa0", "build", capacity)
	phaseCapacity := Resources{MemoryBytes: 500 * poolMiB, CPUMilli: 2000}
	if err := s.BeginPhase("numa0", "build", "phase-a", "run-build", phaseCapacity); err != nil {
		t.Fatal(err)
	}
	if got := s.PoolSnapshots()[0].Scheduled; got != phaseCapacity {
		t.Fatalf("phase double charged: %+v", got)
	}
	if _, _, err := s.Admit(AdmitSpec{SandboxID: "phase-a", Token: "phase-token", Capacity: phaseCapacity, InitialBudget: 128 * poolMiB}); err != nil {
		t.Fatal(err)
	}
	if err := s.BeginPhase("numa0", "phase-a", "phase-b", "run-build", capacity); err == nil {
		t.Fatal("next phase raced old live resources")
	}
	if _, _, err := s.Release("phase-token"); err != nil {
		t.Fatal(err)
	}
	if !s.ReleasePlacement("phase-a", "run-build") {
		t.Fatal("completed phase leaked placement")
	}
	if err := s.BeginPhase("numa0", "", "phase-b", "run-build", capacity); err != nil {
		t.Fatal(err)
	}
	if got := s.PoolSnapshots()[0].Scheduled; got != capacity {
		t.Fatalf("next phase scheduling %+v", got)
	}
	if err := s.BeginPhase("numa1", "", "phase-b", "run-build", capacity); err == nil {
		t.Fatal("running phase changed pool")
	}
	if err := s.RetireExecution("phase-b", "run-build"); err != nil {
		t.Fatal(err)
	}
	if s.PoolSnapshots()[0].Scheduled != (Resources{}) {
		t.Fatal("builder exit leaked scheduling charge")
	}
}

func TestResourcePoolIndependentThresholdsAndUnlimitedCPU(t *testing.T) {
	s := twoPoolState(t)
	a, b := s.PoolStates()["numa0"], s.PoolStates()["numa1"]
	a.CPUWatermarks.HighFactor = .4
	a.CPUWatermarks.LowFactor = .2
	b.AllocatablePool.CPUMilli = 0
	admitPoolTest(t, s, "numa0", "a", Resources{MemoryBytes: 1000 * poolMiB, CPUMilli: 2000}, Resources{}, 100*poolMiB)
	admitPoolTest(t, s, "numa1", "b", Resources{MemoryBytes: 1000 * poolMiB, CPUMilli: 4000}, Resources{}, 800*poolMiB)
	if a.ResourceSnapshot().RawZone != ZoneRed || b.ResourceSnapshot().RawZone != ZoneYellow {
		t.Fatal("dimension override or unlimited CPU changed unrelated thresholds")
	}
	if s.ResourceSnapshot().Zone != ZoneYellow {
		t.Fatal("node did not report lowest noncritical pool")
	}
	if b.PoolSnapshots()[0].CPUUtilization != 0 || b.ResourceSnapshot().Reserved.CPUMilli != 4000 {
		t.Fatal("unlimited CPU lost absolute usage")
	}
}

func TestResourcePoolFractionalCPUDoesNotRoundIdleToRed(t *testing.T) {
	s := twoPoolState(t)
	p := s.PoolStates()["numa0"]
	p.AllocatablePool.CPUMilli = 1
	if p.ResourceSnapshot().RawZone != ZoneGreen {
		t.Fatal("idle fractional CPU pool is not green")
	}
	admitPoolTest(t, s, "numa0", "tiny", Resources{MemoryBytes: 1000 * poolMiB, CPUMilli: 1000}, Resources{CPUMilli: 1}, 100*poolMiB)
	if p.ResourceSnapshot().RawZone != ZoneCritical {
		t.Fatal("full fractional CPU pool is not critical")
	}
}

func TestResourcePoolStartupBudgetAndFailedAdmitIsolation(t *testing.T) {
	s := twoPoolState(t)
	capacity := Resources{MemoryBytes: 1000 * poolMiB, CPUMilli: 1000}
	admitPoolTest(t, s, "numa0", "first", capacity, Resources{}, 450*poolMiB)
	launch := LaunchAdmission{Operation: OperationCreate}
	if s.CanPlace("numa0", "second", capacity, 100*poolMiB, launch) || !s.CanPlace("numa1", "second", capacity, 100*poolMiB, launch) {
		t.Fatal("startup concurrency escaped target pool")
	}
	placePoolTest(t, s, "numa1", "bad", capacity)
	if _, _, err := s.Admit(AdmitSpec{SandboxID: "bad", Token: "bad-token", Capacity: Resources{MemoryBytes: 2000 * poolMiB}, InitialBudget: 100 * poolMiB}); err == nil {
		t.Fatal("accepted a capacity different from the placement")
	}
	if s.PoolSnapshots()[1].Reserved != (Resources{}) || !s.ReleasePlacement("bad", "run-bad") {
		t.Fatal("failed admit leaked reservation or cancelled another identity")
	}
	if _, _, err := s.SetSettled("token-first", 0, time.Now()); err != nil {
		t.Fatal(err)
	}
	if !s.CanPlace("numa0", "second", capacity, 100*poolMiB, launch) {
		t.Fatal("settled memory remained in startup concurrency")
	}
}

func TestResourcePoolUnknownConsumerChargesFiniteSchedulingEnvelope(t *testing.T) {
	s := twoPoolState(t)
	if err := s.BindResourcePool("unknown", "numa0"); err != nil {
		t.Fatal(err)
	}
	if err := s.InstallProvisional(ProvisionalSpec{SandboxID: "unknown", RecoveryKey: "missing-lease", RecoverySource: RecoveryUnknownManaged}); err != nil {
		t.Fatal(err)
	}
	views := s.PoolSnapshots()
	if views[0].Scheduled != views[0].Capacity || views[0].Reserved.MemoryBytes != views[0].AllocatablePool.MemoryBytes || views[1].Scheduled != (Resources{}) {
		t.Fatalf("unknown consumer bounds=%+v", views)
	}
}

func TestResourcePoolLateAdmissionCancellationDoesNotProtectOldPool(t *testing.T) {
	s := twoPoolState(t)
	a, b := s.PoolStates()["numa0"], s.PoolStates()["numa1"]
	a.mu.Lock()
	a.failDemandLocked("admit:moving", "moving", "old-launch", 100*poolMiB, 0, true, time.Now())
	a.mu.Unlock()
	b.mu.Lock()
	b.failDemandLocked("admit:moving", "moving", "new-launch", 100*poolMiB, 0, true, time.Now())
	b.mu.Unlock()
	if err := s.BindResourcePool("moving", "numa1"); err != nil {
		t.Fatal(err)
	}
	s.ForgetAdmissionWait("moving", "old-launch")
	if len(a.PressureSnapshot().Demands) != 0 || len(b.PressureSnapshot().Demands) != 1 {
		t.Fatal("late cancellation leaked old protection or cancelled successor")
	}
}
