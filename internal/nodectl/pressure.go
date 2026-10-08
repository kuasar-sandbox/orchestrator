package nodectl

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// Operation is assigned by the node's authenticated launch owner, never by a
// resource RPC, template kind, urgency, metadata, or request origin.
type Operation uint8

const (
	OperationCreate Operation = iota
	OperationResume
	OperationRecovery
)

func OperationAllowed(zone Zone, op Operation) bool {
	switch op {
	case OperationCreate:
		return zone == ZoneGreen || zone == ZoneYellow
	case OperationResume:
		return zone == ZoneGreen || zone == ZoneYellow || zone == ZoneRed
	case OperationRecovery:
		return true
	default:
		return false
	}
}

// LaunchAdmission is in-process authority, not part of the resource protocol.
// Accepted preserves the asynchronous responsibility of an already durable
// starting operation. SavedSource permits a complete, serialized saved budget;
// it does not grant critical eligibility.
type LaunchAdmission struct {
	Operation   Operation
	SavedSource bool
	Accepted    bool
	Identity    string
}

type PressurePolicy struct {
	Interval            time.Duration
	FailureInterval     time.Duration
	CriticalAfterRounds int
	PauseAfterRounds    int
	CriticalExitHold    time.Duration
	RedToYellowHold     time.Duration
	YellowToGreenHold   time.Duration
	MinimumRunTime      time.Duration
}

func DefaultPressurePolicy() PressurePolicy {
	return PressurePolicy{
		Interval: time.Second, FailureInterval: 500 * time.Millisecond,
		CriticalAfterRounds: 3, PauseAfterRounds: 3,
		CriticalExitHold: 5 * time.Second, RedToYellowHold: 30 * time.Second,
		YellowToGreenHold: 30 * time.Second, MinimumRunTime: 30 * time.Second,
	}
}

func (p PressurePolicy) Validate() error {
	if p.Interval <= 0 || p.FailureInterval < 250*time.Millisecond ||
		p.CriticalAfterRounds < 1 || p.PauseAfterRounds < 1 ||
		p.CriticalExitHold <= 0 || p.RedToYellowHold <= 0 || p.YellowToGreenHold <= 0 || p.MinimumRunTime < 0 {
		return fmt.Errorf("invalid pressure timing or failure thresholds")
	}
	return nil
}

// PressureRecord contains only infrequent zone transitions. The existing node
// SQLite store persists it outside State.mu; monotonic hold clocks never survive
// restart. Sandbox rows separately own every recovery obligation.
type PressureRecord struct {
	Zone      Zone   `json:"zone"`
	Version   uint64 `json:"version"`
	Reason    string `json:"reason"`
	SinceUnix int64  `json:"since_unix"`
}

type PressureSnapshot struct {
	HeadroomStable  bool   `json:"headroom_stable"`
	PoolMemory      uint64 `json:"pool_memory"`
	ReservedMemory  uint64 `json:"reserved_memory"`
	EmergencyMemory uint64 `json:"emergency_memory"`
	PressureRecord
	RawZone       Zone                   `json:"raw_zone"`
	Pending       int                    `json:"pending_recoveries"`
	Capturing     int                    `json:"capturing"`
	Paused        int                    `json:"paused"`
	Starting      int                    `json:"starting"`
	Cleanup       int                    `json:"pending_cleanup"`
	Protected     uint64                 `json:"protected_memory"`
	Beneficiary   string                 `json:"beneficiary,omitempty"`
	Blocked       string                 `json:"blocked,omitempty"`
	OldestWait    time.Duration          `json:"oldest_wait"`
	HoldRemaining time.Duration          `json:"hold_remaining"`
	PauseEligible bool                   `json:"pause_eligible"`
	Demands       []PressureDemandStatus `json:"demands,omitempty"`
}

// PressureDemandStatus exposes only nonsecret wait facts, never the token or
// launch authority used to fence the demand. The underlying demand set is bounded.
type PressureDemandStatus struct {
	SandboxID       string        `json:"sandbox_id,omitempty"`
	Kind            string        `json:"kind"`
	RequestedMemory uint64        `json:"requested_memory"`
	MemoryBlocked   bool          `json:"memory_blocked"`
	Rounds          int           `json:"failure_rounds"`
	CriticalRounds  int           `json:"critical_rounds"`
	LastAttemptAgo  time.Duration `json:"last_attempt_ago"`
}

type pressureObligation struct {
	version       uint64
	phase         string
	cleanup       bool
	accountCharge bool
	cleanupOwner  bool
	retired       bool
	since         time.Time
}

type memoryDemand struct {
	sid           string
	identity      string
	amount        uint64
	baseline      uint64
	first         time.Time
	last          time.Time
	roundAt       time.Time
	rounds        int
	critical      int
	recovery      bool
	memoryBlocked bool
}

// Guest memory reports and RequestBudget retries have a cadence independent of
// the node's pressure scan. Keep a bounded freshness window that covers their
// five-second reporting/RPC budgets across several legitimate retries. A scan
// or an admin observer must not erase a valid waiter before its next report.
// Only another failed transaction advances its rounds.
const minimumDemandFreshness = 30 * time.Second

func (p *pressureState) demandFresh(d *memoryDemand, now time.Time) bool {
	return d != nil && now.Sub(d.last) <= max(minimumDemandFreshness, 3*p.policy.FailureInterval, 2*p.policy.Interval)
}

type pressureState struct {
	pending     int
	cleanup     int
	policy      PressurePolicy
	record      PressureRecord
	holdSince   time.Time
	obligations map[string]pressureObligation
	demands     map[string]*memoryDemand
	beneficiary string
	// One saved-source startup owns the complete-budget lane until Settled or
	// confirmed Release; a disconnected client does not relinquish it.
	exclusive             string
	backgroundRecovery    string
	clock                 func() time.Time
	lastReservationChange time.Time
}

func (s *State) initPressureLocked() {
	if s.pressure.clock != nil {
		return
	}
	s.pressure = pressureState{policy: DefaultPressurePolicy(), clock: time.Now,
		record:      PressureRecord{Zone: ZoneGreen},
		obligations: make(map[string]pressureObligation), demands: make(map[string]*memoryDemand)}
	if s.pressureWake == nil {
		s.pressureWake = make(chan struct{}, 1)
	}
}

func (s *State) ConfigurePressure(policy PressurePolicy, record PressureRecord) error {
	if err := policy.Validate(); err != nil {
		return err
	}
	if record.Zone != "" && zoneRank(record.Zone) < 0 {
		return fmt.Errorf("invalid persisted pressure zone %q", record.Zone)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.initPressureLocked()
	s.pressure.policy = policy
	if record.Zone != "" {
		s.pressure.record = record
	}
	s.pressure.holdSince = time.Time{}
	s.advancePressureLocked(s.pressure.clock())
	return nil
}

func (s *State) PressureWake() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.initPressureLocked()
	return s.pressureWake
}

func (s *State) signalPressureLocked() {
	select {
	case s.pressureWake <- struct{}{}:
	default:
	}
}

func zoneRank(z Zone) int {
	switch z {
	case ZoneGreen:
		return 0
	case ZoneYellow:
		return 1
	case ZoneRed:
		return 2
	case ZoneCritical:
		return 3
	default:
		return -1
	}
}

func (s *State) transitionPressureLocked(zone Zone, reason string, now time.Time) {
	p := &s.pressure
	if p.record.Zone == zone {
		return
	}
	p.record.Zone, p.record.Reason, p.record.SinceUnix = zone, reason, now.Unix()
	p.record.Version++
	p.holdSince = time.Time{}
	s.signalPressureLocked()
}

func (s *State) advancePressureLocked(now time.Time) {
	s.initPressureLocked()
	p := &s.pressure
	raw := s.memoryZoneForReservedLocked(s.reservedMemory)
	if s.AllocatablePool.MemoryBytes == 0 {
		s.transitionPressureLocked(ZoneCritical, "unavailable_pool", now)
		return
	}
	if zoneRank(raw) > zoneRank(p.record.Zone) {
		s.transitionPressureLocked(raw, "reservation", now)
	}
	if p.pending > 0 || p.cleanup > 0 {
		s.transitionPressureLocked(ZoneCritical, "recovery_obligation", now)
		p.holdSince = time.Time{}
		return
	}
	// Demand validity is refreshed by real resource transactions or queued
	// admission rechecks, not by the pressure timer fabricating failed RPCs.
	for _, d := range p.demands {
		if d.memoryBlocked && p.demandFresh(d, now) {
			p.holdSince = time.Time{}
			return
		}
	}
	var hold time.Duration
	var next Zone
	switch p.record.Zone {
	case ZoneCritical:
		if raw == ZoneCritical || s.unknownCount > 0 {
			p.holdSince = time.Time{}
			return
		}
		hold, next = p.policy.CriticalExitHold, ZoneRed
	case ZoneRed:
		if zoneRank(raw) >= zoneRank(ZoneRed) {
			p.holdSince = time.Time{}
			return
		}
		hold, next = p.policy.RedToYellowHold, ZoneYellow
	case ZoneYellow:
		if raw != ZoneGreen {
			p.holdSince = time.Time{}
			return
		}
		hold, next = p.policy.YellowToGreenHold, ZoneGreen
	default:
		return
	}
	if p.holdSince.IsZero() {
		p.holdSince = now
	} else if now.Sub(p.holdSince) >= hold {
		s.transitionPressureLocked(next, "stable_relief", now)
	}
}

func (s *State) CheckOperation(op Operation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.initPressureLocked()
	s.advancePressureLocked(s.pressure.clock())
	if !OperationAllowed(s.pressure.record.Zone, op) {
		return fmt.Errorf("node zone %s does not permit operation %d", s.pressure.record.Zone, op)
	}
	return nil
}

// ObserveObligation consumes a committed lifecycle fact (or a registered
// capture intent). Versions fence stale callbacks. Empty phase discharges Q but
// can retain a separate cleanup barrier; it never releases a reservation.
func (s *State) ObserveObligation(sid string, version uint64, phase string, cleanup bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.observeObligationLocked(sid, version, phase, cleanup, cleanup, false)
}

// ObserveResourceObligation distinguishes remaining lifecycle ownership from
// reservation charge, so a resource Release cannot certify network/run cleanup.
func (s *State) ObserveResourceObligation(sid string, version uint64, phase string, owner, accountCharge bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.observeObligationLocked(sid, version, phase, owner || (accountCharge && s.bySID[sid] != nil), owner, accountCharge)
}

func (s *State) observeObligationLocked(sid string, version uint64, phase string, cleanup, owner, accountCharge bool) {
	s.initPressureLocked()
	old, found := s.pressure.obligations[sid]
	if found && old.version > version {
		return
	}
	if found && old.version == version && old.phase == phase && old.cleanup == cleanup && old.cleanupOwner == owner && old.accountCharge == accountCharge {
		return
	}
	if !found && phase == "" && !cleanup && !accountCharge {
		return
	}
	if old.phase != "" {
		s.pressure.pending--
	}
	if old.cleanup {
		s.pressure.cleanup--
	}
	if phase != "" {
		s.pressure.pending++
	}
	if cleanup {
		s.pressure.cleanup++
	}
	now := s.pressure.clock()
	if old.since.IsZero() || (old.phase == "" && phase != "") {
		old.since = now
	}
	s.pressure.obligations[sid] = pressureObligation{version: version, phase: phase, cleanup: cleanup, cleanupOwner: owner, accountCharge: accountCharge, since: old.since}
	if phase == "" {
		for key, d := range s.pressure.demands {
			if d.sid == sid && d.recovery {
				s.clearDemandLocked(key)
			}
		}
	}
	s.advancePressureLocked(now)
	s.signalPressureLocked()
}

func (s *State) clearDemandLocked(key string) {
	delete(s.pressure.demands, key)
	if s.pressure.beneficiary == key {
		s.pressure.beneficiary = ""
	}
}

// failDemandLocked is called only after authentication, shape/capacity checks,
// policy checks and an actual memory-headroom failure. Its map is bounded.
func (s *State) failDemandLocked(key, sid, identity string, amount, baseline uint64, recovery bool, now time.Time) {
	s.initPressureLocked()
	p := &s.pressure
	d := p.demands[key]
	if d != nil && (d.identity != identity || !p.demandFresh(d, now)) {
		s.clearDemandLocked(key)
		d = nil
	}
	if d == nil {
		if len(p.demands) >= 1024 {
			return
		}
		d = &memoryDemand{sid: sid, identity: identity, amount: amount, baseline: baseline, first: now, recovery: recovery}
		p.demands[key] = d
	}
	d.last, d.amount, d.memoryBlocked = now, amount, true
	if baseline > d.baseline {
		d.baseline, d.rounds, d.critical = baseline, 0, 0
	}
	if !d.roundAt.IsZero() && now.Sub(d.roundAt) < p.policy.FailureInterval {
		return
	}
	d.roundAt = now
	d.rounds++
	if p.record.Zone == ZoneCritical {
		d.critical++
	} else if d.rounds >= p.policy.CriticalAfterRounds {
		s.transitionPressureLocked(ZoneCritical, "sustained_memory_demand", now)
		d.critical = 0
	}
	// Select one beneficiary. Recovery outranks grow; within a class the first
	// valid waiter keeps its funds until progress or cancellation.
	selected := p.demands[p.beneficiary]
	backgroundRecovery := recovery && p.backgroundRecovery == sid
	selectedBackground := selected != nil && p.backgroundRecovery != "" && selected.sid == p.backgroundRecovery
	if !p.demandFresh(selected, now) || (recovery && !backgroundRecovery && !selected.recovery) || (!backgroundRecovery && selectedBackground) {
		p.beneficiary = key
	}
	p.holdSince = time.Time{}
	s.signalPressureLocked()
}

func (s *State) protectedLocked(owner string) uint64 {
	s.initPressureLocked()
	if d := s.pressure.demands[s.pressure.beneficiary]; d != nil && s.pressure.beneficiary != owner {
		if !s.pressure.demandFresh(d, s.pressure.clock()) {
			s.clearDemandLocked(s.pressure.beneficiary)
			return 0
		}
		return d.amount
	}
	return 0
}

func (s *State) ForgetDemand(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.initPressureLocked()
	s.clearDemandLocked(key)
	s.signalPressureLocked()
}

// TryClaimBackgroundRecovery reserves the coordinator's single unattended
// recovery slot only when no fresh memory-blocked demand from another sandbox
// competes for managed-pool headroom. The candidate's own prior recovery wait
// is ignored; final admission rechecks the same condition under State.mu.
func (s *State) TryClaimBackgroundRecovery(sid string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.initPressureLocked()
	if sid == "" || (s.pressure.backgroundRecovery != "" && s.pressure.backgroundRecovery != sid) || s.competingExecutableDemandLocked(sid, s.pressure.clock()) {
		return false
	}
	s.pressure.backgroundRecovery = sid
	return true
}

func (s *State) ReleaseBackgroundRecovery(sid string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.initPressureLocked()
	if s.pressure.backgroundRecovery == sid {
		s.pressure.backgroundRecovery = ""
		s.signalPressureLocked()
	}
}

func (s *State) competingExecutableDemandLocked(sid string, now time.Time) bool {
	for key, d := range s.pressure.demands {
		if !s.pressure.demandFresh(d, now) {
			s.clearDemandLocked(key)
			continue
		}
		if d.sid != sid && d.memoryBlocked {
			return true
		}
	}
	return false
}

func (s *State) RecordAdmissionWait(sid string, admission LaunchAdmission, budget uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.initPressureLocked()
	if budget == 0 || budget > s.AllocatablePool.MemoryBytes ||
		(!admission.Accepted && !OperationAllowed(s.pressure.record.Zone, admission.Operation)) {
		return
	}
	oc := s.admissionBudgetLocked(sid, budget, admission)
	if oc.Status == OutcomeShortTermBlock && oc.Block == BlockedByRecoveryPolicy {
		// A policy fence is not executable memory demand. In particular it
		// must not trigger Pause to make its own projected watermark safe.
		// Cancel only this exact attempt's unused protection, never Q or a
		// charged reservation (or a successor's admission hold).
		key := "admit:" + sid
		if d := s.pressure.demands[key]; d != nil && d.identity == admission.Identity {
			s.clearDemandLocked(key)
		}
		return
	}
	if oc.Status == OutcomeShortTermBlock && oc.Block == BlockedByMainBudget {
		s.failDemandLocked("admit:"+sid, sid, admission.Identity, budget, 0, admission.Operation != OperationCreate, s.pressure.clock())
		return
	}
	// A saved-source resume waiting for a startup slot or rate token retains
	// its unused funds, but slot/rate delays never count as memory failure.
	if admission.Operation == OperationCreate || (oc.Status != OutcomeAdmitted && oc.Status != OutcomeShortTermBlock) {
		return
	}
	key := "admit:" + sid
	d := s.pressure.demands[key]
	if d != nil && d.identity != admission.Identity {
		s.clearDemandLocked(key)
		d = nil
	}
	now := s.pressure.clock()
	if d == nil {
		if len(s.pressure.demands) >= 1024 {
			return
		}
		d = &memoryDemand{sid: sid, identity: admission.Identity, first: now, recovery: true}
		s.pressure.demands[key] = d
	}
	d.last, d.amount, d.memoryBlocked, d.rounds, d.critical = now, budget, false, 0, 0
	selected := s.pressure.demands[s.pressure.beneficiary]
	if selected == nil || !selected.recovery {
		s.pressure.beneficiary = key
	}
}

func (s *State) PressureSnapshot() PressureSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.initPressureLocked()
	now := s.pressure.clock()
	// Only periodic/diagnostic work scans this bounded demand set. Expiration
	// drops an unused protection, never a reservation or durable obligation.
	for key, d := range s.pressure.demands {
		if !s.pressure.demandFresh(d, now) {
			s.clearDemandLocked(key)
		}
	}
	s.advancePressureLocked(now)
	p := &s.pressure
	out := PressureSnapshot{HeadroomStable: now.Sub(p.lastReservationChange) >= p.policy.Interval, PressureRecord: p.record, RawZone: s.memoryZoneForReservedLocked(s.reservedMemory), PoolMemory: s.AllocatablePool.MemoryBytes, ReservedMemory: s.reservedMemory, EmergencyMemory: scaleUint64Floor(s.AllocatablePool.MemoryBytes, s.Wm.EmergencyFactor)}
	for _, ob := range p.obligations {
		switch ob.phase {
		case "capturing":
			out.Capturing++
		case "paused":
			out.Paused++
		case "starting":
			out.Starting++
		}
		if ob.phase != "" {
			out.Pending++
			out.OldestWait = max(out.OldestWait, now.Sub(ob.since))
		}
		if ob.cleanup {
			out.Cleanup++
		}
	}
	for key, d := range p.demands {
		out.OldestWait = max(out.OldestWait, now.Sub(d.first))
		kind, _, _ := strings.Cut(key, ":")
		out.Demands = append(out.Demands, PressureDemandStatus{SandboxID: d.sid, Kind: kind, RequestedMemory: d.amount, MemoryBlocked: d.memoryBlocked, Rounds: d.rounds, CriticalRounds: d.critical, LastAttemptAgo: now.Sub(d.last)})
		if p.record.Zone == ZoneCritical && d.critical >= p.policy.PauseAfterRounds {
			out.PauseEligible = true
		}
	}
	sort.Slice(out.Demands, func(i, j int) bool {
		if out.Demands[i].SandboxID != out.Demands[j].SandboxID {
			return out.Demands[i].SandboxID < out.Demands[j].SandboxID
		}
		return out.Demands[i].Kind < out.Demands[j].Kind
	})
	if d := p.demands[p.beneficiary]; d != nil {
		out.Protected, out.Beneficiary, out.Blocked = d.amount, d.sid, "memory_headroom"
		if !d.memoryBlocked {
			out.Blocked = "startup_or_rate"
		}
	}
	if !p.holdSince.IsZero() {
		hold := p.policy.CriticalExitHold
		if p.record.Zone == ZoneRed {
			hold = p.policy.RedToYellowHold
		}
		if p.record.Zone == ZoneYellow {
			hold = p.policy.YellowToGreenHold
		}
		out.HoldRemaining = max(0, hold-now.Sub(p.holdSince))
	}
	return out
}

// AdmissionWaitError preserves a last-instant headroom race as a wait rather
// than inserting an overcommitted reservation.
type AdmissionWaitError struct{ Outcome Outcome }

func (e *AdmissionWaitError) Error() string {
	if e.Outcome.RejectMsg != "" {
		return e.Outcome.RejectMsg
	}
	return "node resource budget is temporarily unavailable"
}

func (s *State) AnalyzeLaunch(sid string, budget uint64, launch LaunchAdmission) Outcome {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.admissionBudgetLocked(sid, budget, launch)
}

func (s *State) admissionBudgetLocked(sid string, budget uint64, launch LaunchAdmission) Outcome {
	s.initPressureLocked()
	now := s.pressure.clock()
	s.advancePressureLocked(now)
	pool := s.AllocatablePool.MemoryBytes
	backgroundRecovery := launch.Operation == OperationRecovery && s.pressure.backgroundRecovery == sid
	if backgroundRecovery && s.competingExecutableDemandLocked(sid, now) {
		return Outcome{Status: OutcomeShortTermBlock, Block: BlockedByRecoveryPolicy}
	}
	startup := s.startupPoolBytesLocked()
	if budget == 0 || pool == 0 || budget > pool {
		return Outcome{Status: OutcomePreCheckReject, RejectCode: "exceeds_node_capacity", RejectMsg: "initial budget exceeds node pool"}
	}
	// Unattended recovery is deliberately more conservative than an explicit
	// Wake: it must not consume enough managed-pool headroom to recreate the
	// node's red/critical reservation pressure. The exact saved Budget is known
	// here, so this uses the existing raw watermarks without a new margin or
	// host-memory signal. Explicit/user recovery has no coordinator claim and
	// keeps the normal OperationRecovery admission semantics.
	if backgroundRecovery {
		if s.reservedMemory > pool || budget > pool-s.reservedMemory || zoneRank(s.memoryZoneForReservedLocked(s.reservedMemory+budget)) >= zoneRank(ZoneRed) {
			return Outcome{Status: OutcomeShortTermBlock, Block: BlockedByRecoveryPolicy}
		}
	}
	if !launch.SavedSource && budget > startup {
		return Outcome{Status: OutcomePreCheckReject, RejectCode: "exceeds_startup_pool", RejectMsg: "initial budget exceeds startup pool"}
	}
	if !launch.Accepted && !OperationAllowed(s.pressure.record.Zone, launch.Operation) {
		return Outcome{Status: OutcomeLongTermReject, RejectCode: "zone_critical", RejectMsg: "node water level does not permit this operation"}
	}
	available := uint64(0)
	if pool >= s.reservedMemory {
		available = pool - s.reservedMemory
	}
	if !launch.SavedSource {
		emergency := scaleUint64Floor(pool, s.Wm.EmergencyFactor)
		if available >= emergency {
			available -= emergency
		} else {
			available = 0
		}
	}
	protected := s.protectedLocked("admit:" + sid)
	if available >= protected {
		available -= protected
	} else {
		available = 0
	}
	if budget > available {
		return Outcome{Status: OutcomeShortTermBlock, Block: BlockedByMainBudget}
	}
	if s.pressure.exclusive != "" && s.pressure.exclusive != sid {
		return Outcome{Status: OutcomeShortTermBlock, Block: BlockedByStartupBudget}
	}
	if launch.SavedSource {
		if s.startupInFlight != 0 {
			return Outcome{Status: OutcomeShortTermBlock, Block: BlockedByStartupBudget}
		}
	} else if s.startupInFlight >= startup || budget > startup-s.startupInFlight {
		return Outcome{Status: OutcomeShortTermBlock, Block: BlockedByStartupBudget}
	}
	return Outcome{Status: OutcomeAdmitted}
}

func (s *State) CanPressurePause(sid string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.bySID[sid]
	return r != nil && !r.Provisional && r.Stage == StageSettled
}

// BeginPressurePause registers Q under the same mutex as zone decisions before
// the node commits intent or performs capture. It never calls a lifecycle owner.
func (s *State) BeginPressurePause(sid string, version uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.bySID[sid]
	if r == nil || r.Provisional || r.Stage != StageSettled {
		return false
	}
	s.initPressureLocked()
	s.advancePressureLocked(s.pressure.clock())
	if s.pressure.record.Zone != ZoneCritical {
		return false
	}
	if d := s.pressure.demands[s.pressure.beneficiary]; d != nil && d.sid == sid {
		return false
	}
	if old, found := s.pressure.obligations[sid]; found && (old.version > version || old.version == version && old.phase != "") {
		return false
	}
	eligible := false
	for _, d := range s.pressure.demands {
		if d.memoryBlocked && s.pressure.demandFresh(d, s.pressure.clock()) && d.critical >= s.pressure.policy.PauseAfterRounds {
			eligible = true
		}
	}
	if !eligible {
		return false
	}
	for _, d := range s.pressure.demands {
		d.critical = 0
	}
	old := s.pressure.obligations[sid]
	if old.phase == "" {
		s.pressure.pending++
	}
	if old.cleanup {
		s.pressure.cleanup--
	}
	s.pressure.obligations[sid] = pressureObligation{version: version, phase: "capturing", since: s.pressure.clock()}
	s.pressure.holdSince = time.Time{}
	s.signalPressureLocked()
	return true
}

// ForgetAdmissionWait only cancels the exact unused launch protection.
func (s *State) ForgetAdmissionWait(sid, identity string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.initPressureLocked()
	key := "admit:" + sid
	if d := s.pressure.demands[key]; d != nil && d.identity == identity {
		s.clearDemandLocked(key)
		s.signalPressureLocked()
	}
}

// ResetPressureIdentity is used only after authoritative publication of a new
// ordinary object with no pressure history. A charged or obligated predecessor
// cannot be reset. Runtime callbacks do not call this method.
func (s *State) ResetPressureIdentity(sid string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if old, found := s.pressure.obligations[sid]; found && old.phase == "" && !old.cleanup {
		delete(s.pressure.obligations, sid)
	}
}

// RetirePressureIdentity follows terminal publication under the lifecycle
// fence. Keep an unresolved reservation barrier until confirmed Release, then
// discard this deleted object's bookkeeping instead of retaining tombstones
// for every sandbox ever run by this node.
func (s *State) RetirePressureIdentity(sid string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ob, found := s.pressure.obligations[sid]
	if !found || ob.phase != "" || ob.cleanupOwner {
		return
	}
	if !ob.cleanup {
		delete(s.pressure.obligations, sid)
	} else {
		ob.retired = true
		s.pressure.obligations[sid] = ob
	}
}
