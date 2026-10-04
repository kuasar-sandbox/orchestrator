package nodectl

import (
	"math"
	"net"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/sandboxer/pkg/resource"
)

func TestPressureRoundedBoundariesAndConfiguration(t *testing.T) {
	for _, tc := range []struct {
		reserved uint64
		want     Zone
	}{{499, ZoneGreen}, {500, ZoneYellow}, {749, ZoneYellow}, {750, ZoneRed}, {874, ZoneRed}, {875, ZoneCritical}, {1001, ZoneCritical}} {
		s := NewState(1000, 1000, 0, 0, Watermarks{LowFactor: .5, HighFactor: .75, EmergencyFactor: .125, StartupFactor: .5})
		installReservationForTest(t, s, Reservation{SandboxID: "a", Token: "a", Capacity: Resources{MemoryBytes: 2000}, ReservationMemory: tc.reserved, Stage: StageSettled})
		if got := s.ResourceSnapshot(); got.RawZone != tc.want || got.Zone != tc.want {
			t.Fatalf("R=%d: %+v", tc.reserved, got)
		}
	}
	for _, tc := range []struct {
		name string
		edit func(*config.ResourceListenConfig)
	}{
		{"overlap", func(c *config.ResourceListenConfig) { c.Watermarks.HighFactor = .96 }},
		{"rounded", func(c *config.ResourceListenConfig) {
			c.Resources.PhysicalMemory = "2"
			c.Resources.HostReserved.Memory = "1"
		}},
		{"zero-pool", func(c *config.ResourceListenConfig) { c.Resources.PhysicalMemory = "1GiB" }},
		{"failure-interval", func(c *config.ResourceListenConfig) { c.Pressure.FailureInterval = "249ms" }},
		{"negative-rounds", func(c *config.ResourceListenConfig) { c.Pressure.PauseAfterRounds = -1 }},
		{"zero-hold", func(c *config.ResourceListenConfig) { c.Pressure.CriticalExitHold = "0s" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &config.ResourceListenConfig{Resources: config.ResourceHostConfig{PhysicalMemory: "4GiB", PhysicalCPU: "4", HostReserved: config.ResourceHostReserved{Memory: "1GiB"}}}
			c.ApplyDefaults()
			tc.edit(c)
			if _, err := Resolve(c); err == nil {
				t.Fatal("invalid pressure policy enabled")
			}
		})
	}
}

func TestResumeProtectionUsesNodeHeadroomBeforeSandboxCapacity(t *testing.T) {
	s, _ := pressureTestState(t)
	for _, r := range []Reservation{
		{SandboxID: "grow", Token: "grow", Capacity: Resources{MemoryBytes: 300}, ReservationMemory: 100, Stage: StageSettled},
		{SandboxID: "filler", Token: "filler", Capacity: Resources{MemoryBytes: 1000}, ReservationMemory: 500, Stage: StageSettled},
	} {
		installReservationForTest(t, s, r)
	}
	s.RecordAdmissionWait("resume", LaunchAdmission{Operation: OperationResume, SavedSource: true, Identity: "resume-1"}, 500)
	if _, _, err := s.Release("filler"); err != nil {
		t.Fatal(err)
	}
	a := NewAllocator(AllocatorPolicy{MemoryGrantPerSecBytes: 1000, MinGrantStep: 1, MaxGrantStep: 1000})
	r, _, err := s.ReconcileAndGrant("grow", 100, 100, UrgencyNormal, a)
	if err != nil || r.Decision.GrantedDelta != 100 {
		t.Fatalf("unprotected headroom unusable: %+v %v", r, err)
	}
	if p := s.PressureSnapshot(); p.Protected != 500 || p.ReservedMemory != 200 {
		t.Fatalf("protection double charged: %+v", p)
	}
}

func TestResumeSlotWaitRetainsFundsWithoutPressureRounds(t *testing.T) {
	s, now := pressureTestState(t)
	installReservationForTest(t, s, Reservation{SandboxID: "starting", Token: "starting", Capacity: Resources{MemoryBytes: 1000}, ReservationMemory: 600, InitialBudget: 600, Stage: StageStartup})
	a := LaunchAdmission{Operation: OperationRecovery, SavedSource: true, Identity: "saved-1"}
	for i := 0; i < 20; i++ {
		s.RecordAdmissionWait("saved", a, 400)
		*now = now.Add(time.Second)
	}
	p := s.PressureSnapshot()
	if p.Zone != ZoneGreen || p.Protected != 400 || p.PauseEligible || p.Blocked != "startup_or_rate" {
		t.Fatalf("slot wait became memory pressure: %+v", p)
	}
	s.RecordAdmissionWait("saved", LaunchAdmission{Operation: OperationRecovery, SavedSource: true, Identity: "saved-2"}, 400)
	s.ForgetAdmissionWait("saved", "saved-1")
	if s.PressureSnapshot().Protected != 400 {
		t.Fatal("old timeout released successor's protection")
	}
}

func TestSavedStartupReplayAndSyncRetainLaneAndCleanup(t *testing.T) {
	s, _ := pressureTestState(t)
	a := LaunchAdmission{Operation: OperationRecovery, SavedSource: true, Identity: "r1"}
	spec := AdmitSpec{SandboxID: "saved", Token: "t1", PeerPID: 42, CgroupPath: "/saved", Capacity: Resources{MemoryBytes: 1000}, InitialBudget: 400, Admission: &a}
	if _, _, err := s.Admit(spec); err != nil {
		t.Fatal(err)
	}
	s.ObserveResourceObligation("saved", 1, "", true, true)
	spec.Token = "t2"
	if _, _, err := s.Admit(spec); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Sync(SyncSpec{SandboxID: "saved", Token: "t3", PeerPID: 42, CgroupPath: "/saved", Capacity: spec.Capacity, ReservationMemory: 400}); err != nil {
		t.Fatal(err)
	}
	if p := s.PressureSnapshot(); p.ReservedMemory != 400 || p.Cleanup != 1 {
		t.Fatalf("replacement certified release: %+v", p)
	}
	if oc := s.AnalyzeLaunch("other", 10, LaunchAdmission{Accepted: true}); oc.Status != OutcomeShortTermBlock || oc.Block != BlockedByStartupBudget {
		t.Fatalf("replay lost exclusive startup: %+v", oc)
	}
	if _, _, err := s.Release("t3"); err != nil {
		t.Fatal(err)
	}
	if s.PressureSnapshot().Cleanup != 1 {
		t.Fatal("Release certified unfinished lifecycle cleanup")
	}
	s.ObserveResourceObligation("saved", 1, "", false, true)
	if s.PressureSnapshot().Cleanup != 0 {
		t.Fatal("confirmed cleanup did not converge")
	}
}

func TestPartialUnexecutableGrantDoesNotMaskPressure(t *testing.T) {
	const mib = uint64(1 << 20)
	s := NewState(1024*mib, 1000, 0, 0, Watermarks{LowFactor: .7, HighFactor: .85, EmergencyFactor: .05, StartupFactor: .5})
	now := time.Now()
	s.initPressureLocked()
	s.pressure.clock = func() time.Time { return now }
	installReservationForTest(t, s, Reservation{SandboxID: "grow", Token: "grow", Capacity: Resources{MemoryBytes: 1024 * mib}, ReservationMemory: 128 * mib, Stage: StageSettled})
	// Only 4MiB ordinary headroom remains; it is a valid reservation grant but
	// cannot execute the requested 64MiB Budget step.
	filler := s.AllocatablePool.MemoryBytes - scaleUint64Floor(s.AllocatablePool.MemoryBytes, s.Wm.EmergencyFactor) - 132*mib
	installReservationForTest(t, s, Reservation{SandboxID: "filler", Token: "filler", Capacity: Resources{MemoryBytes: 1024 * mib}, ReservationMemory: filler, Stage: StageSettled})
	a := NewAllocator(AllocatorPolicy{MemoryGrantPerSecBytes: 1024 * mib, MinGrantStep: 4 * mib, MaxGrantStep: 512 * mib})
	r, _, err := s.ReconcileAndGrant("grow", 128*mib, resource.MemoryStep, UrgencyNormal, a)
	if err != nil || r.Decision.GrantedDelta != 4*mib {
		t.Fatalf("partial grant=%+v %v", r, err)
	}
	for i := 0; i < 2; i++ {
		now = now.Add(time.Second)
		_, _, err = s.ReconcileAndGrant("grow", 132*mib, 60*mib, UrgencyNormal, a)
		if err != nil {
			t.Fatal(err)
		}
	}
	if p := s.PressureSnapshot(); p.Zone != ZoneCritical || !p.PauseEligible {
		t.Fatalf("partial progress masked failed demand or skipped rounds: %+v", p)
	}
}

func TestTokenOnlyGrowDoesNotCreateMemoryPressure(t *testing.T) {
	s, now := pressureTestState(t)
	installReservationForTest(t, s, Reservation{SandboxID: "grow", Token: "t", Capacity: Resources{MemoryBytes: 1000}, ReservationMemory: 100, Stage: StageSettled})
	a := NewAllocator(AllocatorPolicy{MemoryGrantPerSecBytes: 1, MinGrantStep: 1, MaxGrantStep: 500})
	for i := 0; i < 12; i++ {
		_, _, err := s.ReconcileAndGrant("t", 100, 400, UrgencyNormal, a)
		if err != nil {
			t.Fatal(err)
		}
		*now = now.Add(time.Second)
	}
	if p := s.PressureSnapshot(); p.Zone != ZoneGreen || p.PauseEligible {
		t.Fatalf("rate limit counted as memory shortage: %+v", p)
	}
}

func TestQueuedCreateDoesNotHideEligibleRecovery(t *testing.T) {
	s, _ := pressureTestState(t)
	s.ObserveObligation("recovery", 1, "paused", false)
	a := NewAdmissionController(AdmissionPolicy{Rate: 100, Burst: 100, QueueTTL: time.Minute, QueueMaxDepth: 4})
	a.LookupLaunch = func(sid string, _ int) (LaunchAdmission, error) {
		if sid == "recovery" {
			return LaunchAdmission{Operation: OperationRecovery, SavedSource: true, Identity: sid}, nil
		}
		return LaunchAdmission{Identity: sid}, nil
	}
	srv := &Server{State: s, Admission: a, Logf: t.Logf}
	a.SetWiring(s, nil, srv.BuildAdmitOKFromQueue)
	type reply struct {
		sid     string
		message *Message
	}
	results := make(chan reply, 2)
	for _, sid := range []string{"create", "recovery"} {
		client, server := net.Pipe()
		defer client.Close()
		defer server.Close()
		go func(sid string, c net.Conn) { m, _ := ReadMessage(c); results <- reply{sid, m} }(sid, client)
		req := &Message{SandboxID: sid, CapacityMemoryBytes: 1000, FloorMemoryBytes: 1, StartupBudgetMemory: 100}
		if sid == "recovery" {
			req.AllocatableAtSnapshot = 100
		}
		if _, ok := a.Enqueue(req, server, 42); !ok {
			t.Fatal("enqueue failed")
		}
	}
	a.processQueue()
	for range 2 {
		select {
		case r := <-results:
			want := StatusAdmitted
			if r.sid == "create" {
				want = StatusRejected
			}
			if r.message == nil || r.message.Status != want {
				t.Fatalf("%s queue reply=%+v want=%s", r.sid, r.message, want)
			}
		case <-time.After(time.Second):
			t.Fatal("recovery blocked behind create")
		}
	}
	if s.ResourceSnapshot().Reserved.MemoryBytes != 100 {
		t.Fatal("queue double charged or admitted create")
	}
}

func TestSavedSourceRequiresSnapshotBudgetAndAcceptedLaunchRetainsIdentity(t *testing.T) {
	s, _ := pressureTestState(t)
	a := NewAdmissionController(AdmissionPolicy{Rate: 100, Burst: 100})
	a.SetWiring(s, nil, nil)
	req := &Message{SandboxID: "saved", CapacityMemoryBytes: 1000, FloorMemoryBytes: 1, StartupBudgetMemory: 100}
	if out := a.analyzeLaunch(req, LaunchAdmission{Operation: OperationRecovery, SavedSource: true}); out.Status != OutcomePreCheckReject {
		t.Fatalf("missing Snapshot budget=%+v", out)
	}
	accepted := LaunchAdmission{Identity: "accepted-run", Accepted: true}
	s.ObserveObligation("pressure", 1, "paused", false)
	spec := AdmitSpec{SandboxID: "accepted", Token: "t", PeerPID: 42, Capacity: Resources{MemoryBytes: 1000}, InitialBudget: 100, Admission: &accepted}
	if _, _, err := s.Admit(spec); err != nil {
		t.Fatalf("accepted launch became false no-effect rejection: %v", err)
	}
	spec.Token = "retry"
	if _, _, err := s.Admit(spec); err != nil {
		t.Fatal(err)
	}
	if s.ResourceSnapshot().Reserved.MemoryBytes != 100 {
		t.Fatal("accepted replay double charged")
	}
}

// The balloon target is step-aligned, so a 1056 MiB Capacity has representable
// Budgets 32, 96, 160, 224, ... MiB.
func TestCapacityOffsetDoesNotInventProgress(t *testing.T) {
	const mib = uint64(1 << 20)
	const pool = 2048 * mib
	const capacity = 1056 * mib
	s := NewState(pool, 1000, 0, 0, Watermarks{LowFactor: .7, HighFactor: .85, EmergencyFactor: .05, StartupFactor: .5})
	now := time.Now()
	s.initPressureLocked()
	s.pressure.clock = func() time.Time { return now }
	installReservationForTest(t, s, Reservation{SandboxID: "grow", Token: "grow", Capacity: Resources{MemoryBytes: capacity}, ReservationMemory: 160 * mib, Stage: StageSettled})
	filler := pool - scaleUint64Floor(pool, s.Wm.EmergencyFactor) - 160*mib
	installReservationForTest(t, s, Reservation{SandboxID: "filler", Token: "filler", Capacity: Resources{MemoryBytes: pool}, ReservationMemory: filler, Stage: StageSettled})
	a := NewAllocator(AllocatorPolicy{MemoryGrantPerSecBytes: pool, MinGrantStep: 4 * mib, MaxGrantStep: 512 * mib})
	for i := 0; i < 3; i++ {
		if _, _, err := s.ReconcileAndGrant("grow", 160*mib, resource.MemoryStep, UrgencyNormal, a); err != nil {
			t.Fatal(err)
		}
		now = now.Add(time.Second)
	}
	if len(s.PressureSnapshot().Demands) != 1 {
		t.Fatal("fixture did not retain initial pressure")
	}
	if !s.HasPendingExecutableMemoryDemand() {
		t.Fatal("unexecutable grow wait did not block unattended recovery")
	}
	if _, _, err := s.ReconcileAndGrant("filler", filler-32*mib, 0, UrgencyNormal, a); err != nil {
		t.Fatal(err)
	}
	got, _, err := s.ReconcileAndGrant("grow", 160*mib, resource.MemoryStep, UrgencyNormal, a)
	if err != nil || got.Reservation.ReservationMemory != 192*mib {
		t.Fatalf("partial grant %+v %v", got, err)
	}
	p := s.PressureSnapshot()
	// Executable Budget is still 160 MiB. Its wait and the remaining 32 MiB
	// protection must not disappear merely because R crossed 192 MiB.
	if len(p.Demands) != 1 || p.Protected != 32*mib || p.Demands[0].Rounds < 3 {
		t.Fatalf("unexecutable partial grant erased pressure: %+v", p)
	}
	// Real progress clears the same established wait even when the caller
	// requests a larger objective that cannot be fulfilled in this round.
	now = now.Add(time.Second)
	if _, _, err := s.ReconcileAndGrant("filler", filler-64*mib, 0, UrgencyNormal, a); err != nil {
		t.Fatal(err)
	}
	got, _, err = s.ReconcileAndGrant("grow", 192*mib, 96*mib, UrgencyNormal, a)
	if err != nil || got.Reservation.ReservationMemory != 224*mib {
		t.Fatalf("executable partial grant %+v %v", got, err)
	}
	if p := s.PressureSnapshot(); len(p.Demands) != 0 || p.Protected != 0 {
		t.Fatalf("executable progress retained the established wait: %+v", p)
	}
	if s.HasPendingExecutableMemoryDemand() {
		t.Fatal("executable progress kept unattended recovery blocked")
	}
}

func TestPendingExecutableMemoryDemandFiltersRecoveryNonMemoryAndExpiredWaits(t *testing.T) {
	s := NewState(1<<30, 1000, 0, 0, Watermarks{LowFactor: .7, HighFactor: .85, EmergencyFactor: .05, StartupFactor: .5})
	now := time.Now()
	s.initPressureLocked()
	s.pressure.clock = func() time.Time { return now }

	// Sticky critical is coordination state, not an executable-demand gate.
	s.pressure.record = PressureRecord{Zone: ZoneCritical, Version: 1}
	if s.HasPendingExecutableMemoryDemand() {
		t.Fatal("sticky critical without demand blocked unattended recovery")
	}

	s.pressure.demands["admit:recovery"] = &memoryDemand{
		sid: "recovery", identity: "r", amount: 128 << 20, first: now, last: now, recovery: true, memoryBlocked: true,
	}
	s.pressure.demands["admit:rate"] = &memoryDemand{
		sid: "rate", identity: "q", amount: 128 << 20, first: now, last: now, recovery: true, memoryBlocked: false,
	}
	if s.HasPendingExecutableMemoryDemand() {
		t.Fatal("recovery or non-memory wait blocked unattended recovery")
	}

	s.pressure.demands["grow:running"] = &memoryDemand{
		sid: "running", identity: "g", amount: 64 << 20, first: now, last: now, memoryBlocked: true,
	}
	if !s.HasPendingExecutableMemoryDemand() {
		t.Fatal("fresh running memory wait was not reported")
	}

	now = now.Add(max(minimumDemandFreshness, 3*s.pressure.policy.FailureInterval, 2*s.pressure.policy.Interval) + time.Second)
	if s.HasPendingExecutableMemoryDemand() {
		t.Fatal("expired running memory wait kept unattended recovery blocked")
	}
	if _, ok := s.pressure.demands["grow:running"]; ok {
		t.Fatal("expired demand was not discarded")
	}
}

func TestCapacityOffsetRecognizesExecutableProgress(t *testing.T) {
	const mib = uint64(1 << 20)
	const pool = 2048 * mib
	s := NewState(pool, 1000, 0, 0, Watermarks{LowFactor: .7, HighFactor: .85, EmergencyFactor: .05, StartupFactor: .5})
	installReservationForTest(t, s, Reservation{SandboxID: "grow", Token: "grow", Capacity: Resources{MemoryBytes: 1056 * mib}, ReservationMemory: 192 * mib, Stage: StageSettled})
	filler := pool - scaleUint64Floor(pool, s.Wm.EmergencyFactor) - 224*mib
	installReservationForTest(t, s, Reservation{SandboxID: "filler", Token: "filler", Capacity: Resources{MemoryBytes: pool}, ReservationMemory: filler, Stage: StageSettled})
	a := NewAllocator(AllocatorPolicy{MemoryGrantPerSecBytes: pool, MinGrantStep: 4 * mib, MaxGrantStep: 512 * mib})
	got, _, err := s.ReconcileAndGrant("grow", 192*mib, 96*mib, UrgencyNormal, a)
	if err != nil || got.Reservation.ReservationMemory != 224*mib {
		t.Fatalf("partial grant %+v %v", got, err)
	}
	if p := s.PressureSnapshot(); len(p.Demands) != 0 {
		t.Fatalf("real 160->224 MiB executable progress counted as failure: %+v", p)
	}
}

func TestExecutableMemoryBudget(t *testing.T) {
	const step = resource.MemoryStep
	const capacity = uint64(1<<30 + 4096)
	for _, tc := range []struct {
		name                                        string
		capacity, reservation, wantBudget, wantNext uint64
	}{
		{"zero", 0, 0, 0, 0},
		{"zero-clamped", 0, math.MaxUint64, 0, 0},
		{"small-empty", 1, 0, 0, 1},
		{"small-full", 1, 1, 1, 0},
		{"below-step", step - 1, step - 2, 0, 1},
		{"aligned-empty", step, 0, 0, step},
		{"aligned-partial", step, 1, 0, step - 1},
		{"aligned-full", step, step, step, 0},
		{"offset-empty", capacity, 0, 0, 4096},
		{"offset-below-first", capacity, 4095, 0, 1},
		{"offset-first", capacity, 4096, 4096, step},
		{"offset-absolute-step", capacity, step, 4096, 4096},
		{"offset-next-budget", capacity, step + 4096, step + 4096, step},
		{"offset-near-full", capacity, capacity - 1, capacity - step, 1},
		{"offset-full", capacity, capacity, capacity, 0},
		{"offset-clamped", capacity, math.MaxUint64, capacity, 0},
		{"max-empty", math.MaxUint64, 0, 0, step - 1},
		{"max-below-first", math.MaxUint64, step - 2, 0, 1},
		{"max-first", math.MaxUint64, step - 1, step - 1, step},
		{"max-absolute-step", math.MaxUint64, step, step - 1, step - 1},
		{"max-near-full", math.MaxUint64, math.MaxUint64 - 1, math.MaxUint64 - step, 1},
		{"max-full", math.MaxUint64, math.MaxUint64, math.MaxUint64, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			budget, next := executableMemoryBudget(tc.capacity, tc.reservation)
			if budget != tc.wantBudget || next != tc.wantNext {
				t.Fatalf("budget=%d next=%d, want %d/%d", budget, next, tc.wantBudget, tc.wantNext)
			}
			if budget > min(tc.capacity, tc.reservation) || (budget > 0 && (tc.capacity-budget)%step != 0) {
				t.Fatalf("Budget exceeds reservation or has an unaligned target: %d", budget)
			}
			if tc.reservation < tc.capacity {
				if next == 0 || next > tc.capacity-tc.reservation {
					t.Fatalf("next step overflows Capacity: %d", next)
				}
				if (tc.capacity-(tc.reservation+next))%step != 0 {
					t.Fatalf("next Budget has an unaligned target: %d", tc.reservation+next)
				}
			}
		})
	}
}

func TestCapacityOffsetGrowGrantPolicy(t *testing.T) {
	const mib = uint64(1 << 20)
	const pool = 2048 * mib
	for _, tc := range []struct {
		name                                         string
		current, reserved, requested, headroom       uint64 // MiB
		minGrant, maxGrant, wantDelta, wantProtected uint64 // MiB
		rate                                         uint64 // bytes/second
	}{
		{"reuse-unexecutable", 160, 192, 64, 0, 4, 512, 32, 32, 1},
		{"reuse-executable", 192, 224, 96, 0, 4, 512, 32, 0, 1},
		{"token-only", 192, 192, 96, 32, 4, 512, 0, 0, 1},
		{"maximum-grant-only", 192, 192, 96, 32, 4, 16, 16, 0, pool},
		{"complete-small-tail", 208, 208, 16, 16, 32, 512, 16, 0, pool},
		{"partial-next-step", 192, 192, 96, 16, 4, 512, 16, 16, pool},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := NewState(pool, 1000, 0, 0, Watermarks{LowFactor: .7, HighFactor: .85, EmergencyFactor: .05, StartupFactor: .5})
			installReservationForTest(t, s, Reservation{SandboxID: "grow", Token: "grow", Capacity: Resources{MemoryBytes: 1056 * mib}, ReservationMemory: tc.reserved * mib, Stage: StageSettled})
			filler := pool - scaleUint64Floor(pool, s.Wm.EmergencyFactor) - (tc.reserved+tc.headroom)*mib
			installReservationForTest(t, s, Reservation{SandboxID: "filler", Token: "filler", Capacity: Resources{MemoryBytes: pool}, ReservationMemory: filler, Stage: StageSettled})
			a := NewAllocator(AllocatorPolicy{MemoryGrantPerSecBytes: tc.rate, MinGrantStep: tc.minGrant * mib, MaxGrantStep: tc.maxGrant * mib})
			got, found, err := s.ReconcileAndGrant("grow", tc.current*mib, tc.requested*mib, UrgencyNormal, a)
			wantReservation := (tc.current + tc.wantDelta) * mib
			if err != nil || !found || got.Decision.GrantedDelta != tc.wantDelta*mib || got.Reservation.ReservationMemory != wantReservation {
				t.Fatalf("grant=%+v found=%v err=%v", got, found, err)
			}
			p := s.PressureSnapshot()
			if p.ReservedMemory != filler+wantReservation || p.Protected != tc.wantProtected*mib {
				t.Fatalf("incorrect charge/protection: %+v", p)
			}
			if tc.wantProtected == 0 {
				if len(p.Demands) != 0 {
					t.Fatalf("executable progress or grant policy counted as memory failure: %+v", p)
				}
			} else if len(p.Demands) != 1 || p.Demands[0].Rounds != 1 {
				t.Fatalf("unexecutable partial grant lost blocked demand: %+v", p)
			}
		})
	}
}
