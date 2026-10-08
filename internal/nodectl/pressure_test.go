package nodectl

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

func pressureTestState(t *testing.T) (*State, *time.Time) {
	t.Helper()
	s := NewState(1000, 1000, 0, 0, Watermarks{LowFactor: .7, HighFactor: .85, EmergencyFactor: .05, StartupFactor: .5})
	now := time.Now()
	s.mu.Lock()
	s.initPressureLocked()
	s.pressure.clock = func() time.Time { return now }
	s.mu.Unlock()
	return s, &now
}

func TestPressureOperationMatrix(t *testing.T) {
	for _, zone := range []Zone{ZoneGreen, ZoneYellow, ZoneRed, ZoneCritical} {
		for _, op := range []Operation{OperationCreate, OperationResume, OperationRecovery} {
			s, _ := pressureTestState(t)
			if err := s.ConfigurePressure(DefaultPressurePolicy(), PressureRecord{Zone: zone, Version: 1}); err != nil {
				t.Fatal(err)
			}
			got := s.AnalyzeLaunch("launch", 100, LaunchAdmission{Operation: op})
			want := op == OperationRecovery || (op == OperationResume && zone != ZoneCritical) || (op == OperationCreate && (zone == ZoneGreen || zone == ZoneYellow))
			if (got.Status == OutcomeAdmitted) != want {
				t.Fatalf("zone=%s op=%d outcome=%+v", zone, op, got)
			}
		}
	}
	s, _ := pressureTestState(t)
	_ = s.ConfigurePressure(DefaultPressurePolicy(), PressureRecord{Zone: ZoneCritical, Version: 1})
	if oc := s.AnalyzeLaunch("adopted", 975, LaunchAdmission{Operation: OperationResume, SavedSource: true}); oc.Status != OutcomeLongTermReject {
		t.Fatalf("saved source acquired critical exemption: %+v", oc)
	}
}

func TestPressureObligationsAndStepwiseRelief(t *testing.T) {
	s, now := pressureTestState(t)
	s.ObserveObligation("a", 1, "capturing", false)
	s.ObserveObligation("a", 2, "paused", true)
	s.ObserveObligation("a", 2, "starting", false)
	*now = now.Add(time.Hour)
	if p := s.PressureSnapshot(); p.Zone != ZoneCritical || p.RawZone != ZoneGreen || p.Pending != 1 || p.Starting != 1 {
		t.Fatalf("starting cleared Q: %+v", p)
	}
	s.ObserveObligation("a", 3, "", true)
	*now = now.Add(time.Hour)
	if p := s.PressureSnapshot(); p.Zone != ZoneCritical || p.Pending != 0 || p.Cleanup != 1 {
		t.Fatalf("cleanup did not hold critical: %+v", p)
	}
	s.ObserveObligation("a", 3, "", false)
	*now = now.Add(5 * time.Second)
	if p := s.PressureSnapshot(); p.Zone != ZoneRed {
		t.Fatalf("critical exit=%+v", p)
	}
	*now = now.Add(time.Hour)
	if p := s.PressureSnapshot(); p.Zone != ZoneRed {
		t.Fatalf("downgrade skipped new hold: %+v", p)
	}
	*now = now.Add(30 * time.Second)
	if p := s.PressureSnapshot(); p.Zone != ZoneYellow {
		t.Fatalf("red exit=%+v", p)
	}
	_ = s.PressureSnapshot()
	*now = now.Add(30 * time.Second)
	if p := s.PressureSnapshot(); p.Zone != ZoneGreen {
		t.Fatalf("yellow exit=%+v", p)
	}
	s.ObserveObligation("a", 2, "paused", false)
	if p := s.PressureSnapshot(); p.Pending != 0 {
		t.Fatalf("stale obligation resurrected: %+v", p)
	}
}

func TestPressureDistinctFailureRounds(t *testing.T) {
	s, now := pressureTestState(t)
	installReservationForTest(t, s, Reservation{SandboxID: "existing", Token: "existing", Capacity: Resources{MemoryBytes: 1000}, ReservationMemory: 600, Stage: StageSettled})
	a := LaunchAdmission{Operation: OperationResume, Identity: "run-1", Accepted: true}
	for i := 0; i < 20; i++ {
		s.RecordAdmissionWait("resume", a, 500)
	}
	if p := s.PressureSnapshot(); p.Zone != ZoneGreen {
		t.Fatalf("duplicate requests counted: %+v", p)
	}
	for i := 0; i < 2; i++ {
		*now = now.Add(500 * time.Millisecond)
		s.RecordAdmissionWait("resume", a, 500)
	}
	if p := s.PressureSnapshot(); p.Zone != ZoneCritical || p.PauseEligible {
		t.Fatalf("critical must precede Pause: %+v", p)
	}
	for i := 0; i < 3; i++ {
		*now = now.Add(500 * time.Millisecond)
		s.RecordAdmissionWait("resume", a, 500)
	}
	if !s.BeginPressurePause("existing", 1) {
		t.Fatal("qualified Pause rejected")
	}
	if s.BeginPressurePause("second", 1) {
		t.Fatal("old rounds paused another object")
	}
	if p := s.PressureSnapshot(); p.Pending != 1 || p.Capturing != 1 {
		t.Fatalf("capture absent from Q: %+v", p)
	}
}

// Guest reports arrive every five seconds. A more frequent pressure scan (or
// admin observer) must not erase a live waiter's history between valid retries.
func TestPressureGuestReportCadenceSurvivesObservation(t *testing.T) {
	const mib = uint64(1 << 20)
	s := NewState(896*mib, 1000, 0, 0, Watermarks{LowFactor: .7, HighFactor: .85, EmergencyFactor: .05, StartupFactor: .4})
	now := time.Now()
	s.initPressureLocked()
	s.pressure.clock = func() time.Time { return now }
	policy := DefaultPressurePolicy()
	policy.Interval = 2 * time.Second
	if err := s.ConfigurePressure(policy, PressureRecord{}); err != nil {
		t.Fatal(err)
	}
	installReservationForTest(t, s, Reservation{SandboxID: "oldest", Token: "oldest", Capacity: Resources{MemoryBytes: 1024 * mib}, ReservationMemory: 512 * mib, Stage: StageSettled})
	installReservationForTest(t, s, Reservation{SandboxID: "waiter", Token: "waiter", Capacity: Resources{MemoryBytes: 1024 * mib}, ReservationMemory: 320 * mib, Stage: StageSettled})
	allocator := NewAllocator(AllocatorPolicy{MemoryGrantPerSecBytes: 1024 * mib, MinGrantStep: 64 * mib, MaxGrantStep: 64 * mib})
	for report := 0; report < policy.CriticalAfterRounds+policy.PauseAfterRounds; report++ {
		if report != 0 {
			for scan := 0; scan < 5; scan++ {
				now = now.Add(time.Second)
				_ = s.PressureSnapshot()
			}
		}
		got, found, err := s.ReconcileAndGrant("waiter", 320*mib, 64*mib, UrgencyNormal, allocator)
		if err != nil || !found || got.Decision.GrantedDelta != 0 {
			t.Fatalf("report %d: expected a valid headroom wait: %+v, %v", report, got, err)
		}
	}
	if p := s.PressureSnapshot(); p.Zone != ZoneCritical || !p.PauseEligible || p.Beneficiary != "waiter" {
		t.Fatalf("five-second retries lost their pressure history: %+v", p)
	}
	status := s.PressureSnapshot()
	if len(status.Demands) != 1 || status.Demands[0].Kind != "grow" || status.Demands[0].SandboxID != "waiter" || status.Demands[0].Rounds != 6 || status.Demands[0].CriticalRounds != 3 || status.Demands[0].LastAttemptAgo != 0 {
		t.Fatalf("pressure diagnostic lost the valid demand: %+v", status.Demands)
	}
	if !s.BeginPressurePause("oldest", 1) {
		t.Fatal("sustained valid guest demand did not authorize the settled victim")
	}
}

func TestPressureExpiredDemandCannotPauseOrCarryFailureRounds(t *testing.T) {
	s, now := pressureTestState(t)
	installReservationForTest(t, s, Reservation{SandboxID: "existing", Token: "existing", Capacity: Resources{MemoryBytes: 1000}, ReservationMemory: 600, Stage: StageSettled})
	a := LaunchAdmission{Operation: OperationResume, Identity: "run-1", Accepted: true}
	for i := 0; i < 6; i++ {
		s.RecordAdmissionWait("resume", a, 500)
		*now = now.Add(500 * time.Millisecond)
	}
	if !s.PressureSnapshot().PauseEligible {
		t.Fatal("fixture did not establish sustained demand")
	}
	*now = now.Add(31 * time.Second)
	// No snapshot/periodic scan intervenes: the operation itself must reject
	// stale Pause authority and restart the later real request's rounds.
	if s.BeginPressurePause("existing", 1) {
		t.Fatal("expired demand authorized a Pause")
	}
	s.RecordAdmissionWait("resume", a, 500)
	if p := s.PressureSnapshot(); p.PauseEligible || p.Pending != 0 || p.ReservedMemory != 600 {
		t.Fatalf("stale failure rounds survived the gap: %+v", p)
	}
}

func TestPressureExpiredProtectionDoesNotNeedObserver(t *testing.T) {
	s, now := pressureTestState(t)
	installReservationForTest(t, s, Reservation{SandboxID: "existing", Token: "existing", Capacity: Resources{MemoryBytes: 1000}, ReservationMemory: 600, Stage: StageSettled})
	s.RecordAdmissionWait("resume", LaunchAdmission{Operation: OperationResume, Identity: "run-1", Accepted: true}, 500)
	if got := s.AnalyzeLaunch("other", 1, LaunchAdmission{}); got.Status != OutcomeShortTermBlock {
		t.Fatalf("live resume funds were not protected: %+v", got)
	}
	*now = now.Add(31 * time.Second)
	if got := s.AnalyzeLaunch("other", 1, LaunchAdmission{}); got.Status != OutcomeAdmitted {
		t.Fatalf("unused expired protection blocked actual headroom: %+v", got)
	}
	if got := s.ResourceSnapshot().Reserved.MemoryBytes; got != 600 {
		t.Fatalf("expiration changed charged memory: %d", got)
	}
}

func TestSavedSourceCompleteBudgetAndSerialStartup(t *testing.T) {
	for _, budget := range []uint64{400, 975} {
		s, _ := pressureTestState(t)
		restore := LaunchAdmission{Operation: OperationResume, SavedSource: true}
		if budget == 975 {
			if oc := s.AnalyzeLaunch("template", budget, LaunchAdmission{}); oc.RejectCode != "exceeds_startup_pool" {
				t.Fatalf("template got recovery budget: %+v", oc)
			}
		}
		r, _, err := s.Admit(AdmitSpec{SandboxID: "resume", Token: "t", Capacity: Resources{MemoryBytes: 1000}, InitialBudget: budget, Admission: &restore})
		if err != nil || r.InitialBudget != budget || s.AdmissionSnapshot().StartupInFlight != budget {
			t.Fatalf("complete budget not charged: %+v %v", r, err)
		}
		if oc := s.AnalyzeLaunch("other", 1, LaunchAdmission{Accepted: true}); oc.Status == OutcomeAdmitted {
			t.Fatal("concurrent startup entered saved-source lane")
		}
		if _, _, err := s.SetSettled("t", 0, time.Now()); err != nil {
			t.Fatal(err)
		}
		if s.AdmissionSnapshot().StartupInFlight != 0 || s.ResourceSnapshot().Reserved.MemoryBytes != budget {
			t.Fatal("Settled changed live charge or retained startup budget")
		}
	}
}

func TestStickyCriticalRuntimeHeadroomAndFinalGate(t *testing.T) {
	s, _ := pressureTestState(t)
	installReservationForTest(t, s, Reservation{SandboxID: "running", Token: "t", Capacity: Resources{MemoryBytes: 1000}, ReservationMemory: 100, Stage: StageSettled})
	s.ObserveObligation("paused", 1, "paused", false)
	a := NewAllocator(AllocatorPolicy{MemoryGrantPerSecBytes: 1000, MinGrantStep: 1, MaxGrantStep: 1000})
	got, found, err := s.ReconcileAndGrant("t", 100, 100, UrgencyNormal, a)
	if err != nil || !found || got.Decision.GrantedDelta != 100 {
		t.Fatalf("sticky critical blocked grow: %+v %v", got, err)
	}
	ordinary := LaunchAdmission{}
	if _, _, err := s.Admit(AdmitSpec{SandboxID: "new", Token: "new", Capacity: Resources{MemoryBytes: 1000}, InitialBudget: 10, Admission: &ordinary}); err == nil {
		t.Fatal("final gate admitted new create in critical")
	}
	if s.ResourceSnapshot().Reserved.MemoryBytes != 200 {
		t.Fatal("failed admission changed accounting")
	}
}

func TestPressureRestartAndImpossibleDemand(t *testing.T) {
	s, now := pressureTestState(t)
	for i := 0; i < 10; i++ {
		s.RecordAdmissionWait("impossible", LaunchAdmission{Operation: OperationResume}, 1001)
		*now = now.Add(time.Second)
	}
	if p := s.PressureSnapshot(); p.Zone != ZoneGreen {
		t.Fatalf("impossible demand escalated: %+v", p)
	}
	_ = s.ConfigurePressure(DefaultPressurePolicy(), PressureRecord{Zone: ZoneCritical, Version: 10, SinceUnix: 1})
	if p := s.PressureSnapshot(); p.Zone != ZoneCritical {
		t.Fatalf("wall time skipped restart hold: %+v", p)
	}
	*now = now.Add(5 * time.Second)
	if p := s.PressureSnapshot(); p.Zone != ZoneRed {
		t.Fatalf("restart exit did not pass red: %+v", p)
	}
}

// Observation advances time and expiry, never a failed resource transaction.
// A full pool alone is not authority to interrupt an otherwise running VM.
func TestPressureScansWithoutDemandDoNotPause(t *testing.T) {
	for _, reserved := range []uint64{100, 950} {
		s, now := pressureTestState(t)
		installReservationForTest(t, s, Reservation{SandboxID: "running", Token: "running", Capacity: Resources{MemoryBytes: 1000}, ReservationMemory: reserved, Stage: StageSettled})
		wantZone := ZoneGreen
		if reserved == 950 {
			wantZone = ZoneCritical
		}
		for scan := 0; scan < 20; scan++ {
			*now = now.Add(s.pressure.policy.FailureInterval)
			if p := s.PressureSnapshot(); p.Zone != wantZone || p.RawZone != wantZone || p.PauseEligible || len(p.Demands) != 0 || p.ReservedMemory != reserved {
				t.Fatalf("R=%d scan=%d fabricated pressure or changed accounting: %+v", reserved, scan, p)
			}
			if s.BeginPressurePause("running", 1) {
				t.Fatalf("R=%d: a scan authorized Pause without a demand", reserved)
			}
		}
	}
}

func TestPressureSnapshotDoesNotClaimWholeHostSafety(t *testing.T) {
	s, _ := pressureTestState(t)
	body, err := json.Marshal(s.PressureSnapshot())
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(body, &fields); err != nil {
		t.Fatal(err)
	}
	if _, present := fields["host_safety_blocked"]; present {
		t.Fatal("pool diagnostics still claim to report whole-host memory safety")
	}
}

func TestHistoricalHostPressureReasonUsesNormalRelief(t *testing.T) {
	for _, barrier := range []string{"none", "obligation", "cleanup"} {
		t.Run(barrier, func(t *testing.T) {
			s, now := pressureTestState(t)
			installReservationForTest(t, s, Reservation{SandboxID: "running", Token: "running", Capacity: Resources{MemoryBytes: 1000}, ReservationMemory: 100, Stage: StageSettled})
			policy := DefaultPressurePolicy()
			if err := s.ConfigurePressure(policy, PressureRecord{Zone: ZoneCritical, Version: 12, Reason: "host_operational_margin", SinceUnix: 1}); err != nil {
				t.Fatal(err)
			}
			if p := s.PressureSnapshot(); p.Zone != ZoneCritical || p.Reason != "host_operational_margin" || p.PauseEligible || len(p.Demands) != 0 || p.ReservedMemory != 100 {
				t.Fatalf("historical reason became a demand or bypassed restart hold: %+v", p)
			}
			if barrier != "none" {
				phase, cleanup := "paused", false
				if barrier == "cleanup" {
					phase, cleanup = "", true
				}
				s.ObserveObligation("old-run", 1, phase, cleanup)
				*now = now.Add(time.Hour)
				p := s.PressureSnapshot()
				if p.Zone != ZoneCritical || p.PauseEligible || len(p.Demands) != 0 || p.ReservedMemory != 100 || (barrier == "obligation" && p.Pending != 1) || (barrier == "cleanup" && p.Cleanup != 1) {
					t.Fatalf("policy change discharged durable recovery/cleanup state: %+v", p)
				}
				s.ObserveObligation("old-run", 2, "", false)
			}
			*now = now.Add(policy.CriticalExitHold - time.Nanosecond)
			if p := s.PressureSnapshot(); p.Zone != ZoneCritical {
				t.Fatalf("critical exit hold was shortened: %+v", p)
			}
			*now = now.Add(time.Nanosecond)
			if p := s.PressureSnapshot(); p.Zone != ZoneRed || p.Reason != "stable_relief" || p.Pending != 0 || p.Cleanup != 0 || p.ReservedMemory != 100 {
				t.Fatalf("historical critical did not exit normally through red: %+v", p)
			}
			_ = s.PressureSnapshot() // Start the independent red hold.
			*now = now.Add(policy.RedToYellowHold)
			if p := s.PressureSnapshot(); p.Zone != ZoneYellow {
				t.Fatalf("red hold did not converge: %+v", p)
			}
			_ = s.PressureSnapshot() // Start the independent yellow hold.
			*now = now.Add(policy.YellowToGreenHold)
			if p := s.PressureSnapshot(); p.Zone != ZoneGreen || p.ReservedMemory != 100 || len(p.Demands) != 0 {
				t.Fatalf("historical episode failed to converge without changing charge: %+v", p)
			}
		})
	}
}

func TestPressureGrowWaitsForReleaseNotZoneRelief(t *testing.T) {
	const mib = uint64(1 << 20)
	s := NewState(1024*mib, 1000, 0, 0, Watermarks{LowFactor: .7, HighFactor: .85, EmergencyFactor: .05, StartupFactor: .5})
	now := time.Now()
	s.initPressureLocked()
	s.pressure.clock = func() time.Time { return now }
	pool := s.AllocatablePool.MemoryBytes
	emergency := scaleUint64Floor(pool, s.Wm.EmergencyFactor)
	initial := uint64(128 * mib)
	victim := pool - emergency - initial
	installReservationForTest(t, s, Reservation{SandboxID: "grow", Token: "grow", Capacity: Resources{MemoryBytes: pool}, ReservationMemory: initial, Stage: StageSettled})
	installReservationForTest(t, s, Reservation{SandboxID: "victim", Token: "victim", Capacity: Resources{MemoryBytes: pool}, ReservationMemory: victim, Stage: StageSettled})
	allocator := NewAllocator(AllocatorPolicy{MemoryGrantPerSecBytes: pool, MinGrantStep: 64 * mib, MaxGrantStep: 64 * mib})
	for attempt := 0; attempt < s.pressure.policy.CriticalAfterRounds+s.pressure.policy.PauseAfterRounds; attempt++ {
		got, found, err := s.ReconcileAndGrant("grow", initial, 64*mib, UrgencyNormal, allocator)
		if err != nil || !found || got.Decision.GrantedDelta != 0 {
			t.Fatalf("attempt %d consumed unavailable pool memory: %+v %v", attempt, got, err)
		}
		now = now.Add(s.pressure.policy.FailureInterval)
	}
	if !s.BeginPressurePause("victim", 1) {
		t.Fatal("valid sustained grow demand did not authorize Pause")
	}
	for _, phase := range []string{"capturing", "paused"} {
		s.ObserveResourceObligation("victim", 1, phase, false, true)
		got, found, err := s.ReconcileAndGrant("grow", initial, 64*mib, UrgencyNormal, allocator)
		if err != nil || !found || got.Decision.GrantedDelta != 0 {
			t.Fatalf("%s was mistaken for released funds: %+v %v", phase, got, err)
		}
		if p := s.PressureSnapshot(); p.ReservedMemory != initial+victim || p.Pending != 1 {
			t.Fatalf("%s changed charge or discharged Q: %+v", phase, p)
		}
	}
	if _, found, err := s.Release("victim"); err != nil || !found {
		t.Fatalf("confirmed victim Release failed: found=%v err=%v", found, err)
	}
	got, found, err := s.ReconcileAndGrant("grow", initial, 64*mib, UrgencyNormal, allocator)
	if err != nil || !found || got.Decision.GrantedDelta != 64*mib {
		t.Fatalf("released budget remained blocked by sticky critical: %+v %v", got, err)
	}
	if p := s.PressureSnapshot(); p.Zone != ZoneCritical || p.RawZone != ZoneGreen || p.Pending != 1 || p.ReservedMemory != initial+64*mib || len(p.Demands) != 0 {
		t.Fatalf("grant changed the recovery obligation or failed to consume actual headroom: %+v", p)
	}
}

func TestBackgroundRecoveryClaimSkipsOwnerDemandAndFencesNewCompetitor(t *testing.T) {
	s, now := pressureTestState(t)
	s.mu.Lock()
	s.failDemandLocked("admit:b", "b", "run-b", 400, 0, true, *now)
	s.mu.Unlock()
	if s.TryClaimBackgroundRecovery("a") {
		t.Fatal("older unrelated candidate ignored B's executable demand")
	}
	if !s.TryClaimBackgroundRecovery("b") {
		t.Fatal("demand owner could not claim its own background retry")
	}

	// A new ordinary grow after selection must win before B's final Admit.
	s.mu.Lock()
	s.failDemandLocked("grow:running", "running", "tok", 100, 0, false, *now)
	beneficiary := s.pressure.beneficiary
	s.mu.Unlock()
	if beneficiary != "grow:running" {
		t.Fatalf("background recovery displaced ordinary beneficiary: %q", beneficiary)
	}

	// An ordinary Resume/Wake demand has the same priority over claimed
	// background work; the historical recovery class must not hide it.
	s.mu.Lock()
	s.clearDemandLocked("grow:running")
	s.failDemandLocked("admit:wake", "wake", "run-wake", 100, 0, true, *now)
	beneficiary = s.pressure.beneficiary
	s.mu.Unlock()
	if beneficiary != "admit:wake" {
		t.Fatalf("background recovery displaced ordinary Wake beneficiary: %q", beneficiary)
	}
	launch := LaunchAdmission{Operation: OperationRecovery, SavedSource: true, Accepted: true, Identity: "run-b"}
	if got := s.AnalyzeLaunch("b", 100, launch); got.Status != OutcomeShortTermBlock || got.Block != BlockedByRecoveryPolicy {
		t.Fatalf("new demand did not fence final background admission: %+v", got)
	}

	s.ForgetDemand("admit:wake")
	if got := s.AnalyzeLaunch("b", 100, launch); got.Status != OutcomeAdmitted {
		t.Fatalf("cleared competitor did not release final fence: %+v", got)
	}
	s.ReleaseBackgroundRecovery("b")
}

func TestOrdinaryRecoveryAdmissionHasNoBackgroundClaimFence(t *testing.T) {
	s, now := pressureTestState(t)
	s.mu.Lock()
	s.failDemandLocked("grow:running", "running", "tok", 100, 0, false, *now)
	s.mu.Unlock()
	// A user/Proxy Wake has no coordinator claim. Its existing recovery
	// admission/protection semantics remain authoritative.
	launch := LaunchAdmission{Operation: OperationRecovery, SavedSource: true, Accepted: true, Identity: "wake"}
	if got := s.AnalyzeLaunch("wake", 100, launch); got.Status != OutcomeAdmitted {
		t.Fatalf("ordinary recovery changed admission semantics: %+v", got)
	}
}

func TestBackgroundRecoveryMustNotProjectRawRed(t *testing.T) {
	s, _ := pressureTestState(t)
	// Pool=1000, red starts at 850. Leave absolute room for the recovery while
	// making its exact saved Budget project the node into red.
	installReservationForTest(t, s, Reservation{SandboxID: "running", Token: "running", Capacity: Resources{MemoryBytes: 1000}, ReservationMemory: 700, Stage: StageSettled})
	if !s.TryClaimBackgroundRecovery("saved") {
		t.Fatal("background recovery claim unexpectedly blocked")
	}
	background := LaunchAdmission{Operation: OperationRecovery, SavedSource: true, Accepted: true, Identity: "saved:1"}
	if got := s.AnalyzeLaunch("saved", 200, background); got.Status != OutcomeShortTermBlock || got.Block != BlockedByRecoveryPolicy {
		t.Fatalf("background recovery projected raw red but was admitted: %+v", got)
	}

	// The same exact resource state must not change explicit/user Wake semantics.
	s.ReleaseBackgroundRecovery("saved")
	if got := s.AnalyzeLaunch("saved", 200, background); got.Status != OutcomeAdmitted {
		t.Fatalf("ordinary Wake inherited background stability gate: %+v", got)
	}

	// Once real managed-pool headroom makes the post-recovery state yellow,
	// unattended recovery can proceed without waiting for effective critical to clear.
	if _, found, err := s.ReconcileAndGrant("running", 600, 0, UrgencyNormal, NewAllocator(AllocatorPolicy{})); err != nil || !found {
		t.Fatalf("release running reservation: found=%v err=%v", found, err)
	}
	if !s.TryClaimBackgroundRecovery("saved") {
		t.Fatal("background recovery claim did not reopen after release")
	}
	if got := s.AnalyzeLaunch("saved", 200, background); got.Status != OutcomeAdmitted {
		t.Fatalf("post-recovery yellow state was not admitted: %+v", got)
	}
	s.ReleaseBackgroundRecovery("saved")
}

// A background policy refusal is not executable workload demand: waiting for
// a safer watermark must not evict another guest to manufacture that watermark.
func TestBackgroundPolicyWaitDoesNotBecomePressureDemand(t *testing.T) {
	for _, reserved := range []uint64{0, 700} {
		t.Run(fmt.Sprint(reserved), func(t *testing.T) {
			s, now := pressureTestState(t)
			if reserved != 0 {
				installReservationForTest(t, s, Reservation{SandboxID: "running", Token: "running", Capacity: Resources{MemoryBytes: 1000}, ReservationMemory: reserved, Stage: StageSettled})
			}
			if !s.TryClaimBackgroundRecovery("saved") {
				t.Fatal("claim")
			}
			launch := LaunchAdmission{Operation: OperationRecovery, SavedSource: true, Accepted: true, Identity: "saved:1"}
			for i := 0; i < 10; i++ {
				s.RecordAdmissionWait("saved", launch, 900)
				*now = now.Add(time.Second)
			}
			p := s.PressureSnapshot()
			if p.PauseEligible || len(p.Demands) != 0 || p.Protected != 0 || p.Beneficiary != "" {
				t.Fatalf("policy refusal manufactured pressure: %+v", p)
			}
			if p.ReservedMemory != reserved {
				t.Fatalf("changed actual reservation: %+v", p)
			}
			s.ReleaseBackgroundRecovery("saved")
			// An explicit recovery still has its ordinary memory-demand semantics.
			if reserved != 0 {
				s.RecordAdmissionWait("saved", launch, 900)
				if len(s.PressureSnapshot().Demands) != 1 {
					t.Fatal("ordinary recovery lost pressure demand")
				}
			}
		})
	}
}

func TestBackgroundPolicyClearsOnlyExactUnusedHold(t *testing.T) {
	for _, identity := range []string{"current", "successor"} {
		t.Run(identity, func(t *testing.T) {
			s, now := pressureTestState(t)
			s.ObserveObligation("saved", 1, "paused", false)
			s.mu.Lock()
			s.failDemandLocked("admit:saved", "saved", identity, 900, 0, true, *now)
			s.mu.Unlock()
			if !s.TryClaimBackgroundRecovery("saved") {
				t.Fatal("claim")
			}
			s.RecordAdmissionWait("saved", LaunchAdmission{Operation: OperationRecovery, SavedSource: true, Accepted: true, Identity: "current"}, 900)
			p := s.PressureSnapshot()
			want := 0
			if identity == "successor" {
				want = 1
			}
			if len(p.Demands) != want || p.Pending != 1 || p.Paused != 1 || p.ReservedMemory != 0 {
				t.Fatalf("exact attempt fence or durable obligation changed: %+v", p)
			}
		})
	}
}
