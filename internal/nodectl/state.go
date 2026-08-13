package nodectl

import (
	"fmt"
	"net"
	"sort"
	"sync"
	"time"
)

const (
	StageAdmitted  = "admitted"
	StageCreating  = "creating"
	StageStartup   = "startup"
	StageRestoring = "restoring"
	StageSettled   = "settled"
	StageBurst     = "burst"
	StageRecover   = "recover"
	StageReleased  = "released"
)

const (
	RecoveryAdmit          = "admit"
	RecoveryLease          = "lease"
	RecoveryManagedPIDFile = "managed-pidfile"
	RecoveryCgroup         = "cgroup"
	RecoveryUnknownLease   = "unknown-lease"
	RecoveryUnknownManaged = "unknown-managed"
	RecoverySynced         = "synced"
	RecoveryPersisted      = "persisted-compat"
)

type Resources struct {
	MemoryBytes uint64 `json:"memory_bytes"`
	CPUMilli    uint64 `json:"cpu_milli"`
}

func (r Resources) Sub(other Resources) Resources {
	out := r
	if other.MemoryBytes > out.MemoryBytes {
		out.MemoryBytes = 0
	} else {
		out.MemoryBytes -= other.MemoryBytes
	}
	if other.CPUMilli > out.CPUMilli {
		out.CPUMilli = 0
	} else {
		out.CPUMilli -= other.CPUMilli
	}
	return out
}

func (r Resources) Add(other Resources) Resources {
	return Resources{MemoryBytes: r.MemoryBytes + other.MemoryBytes, CPUMilli: r.CPUMilli + other.CPUMilli}
}

// Reservation is controller-owned mutable state. State methods return copies;
// no caller receives a pointer stored in the maps.
type Reservation struct {
	Token             string
	SandboxID         string
	PeerPID           int
	CgroupPath        string
	Capacity          Resources
	Floor             Resources
	AllocatableNowMem uint64

	EffectiveStartupBudget uint64
	Stage                  string
	StageEnteredAt         time.Time
	LastHeartbeatAt        time.Time
	OOMCount               uint64
	LastReportedRSS        uint64
	LastReportAt           time.Time

	Provisional    bool
	RecoverySource string
	RecoveryKey    string
	LeasePath      string
	StartupExpired bool
	ClientFeatures []string
	Conn           net.Conn
}

func (r Reservation) identity() string {
	if r.Token != "" {
		return "token:" + r.Token
	}
	return "recovery:" + r.RecoveryKey
}

type Watermarks struct {
	OperationalMarginFactor float64 `json:"operational_margin_factor"`
	HighFactor              float64 `json:"high_factor"`
	LowFactor               float64 `json:"low_factor"`
	EmergencyFactor         float64 `json:"emergency_factor"`
	StartupFactor           float64 `json:"startup_factor"`
}

type Zone string

const (
	ZoneGreen    Zone = "green"
	ZoneYellow   Zone = "yellow"
	ZoneRed      Zone = "red"
	ZoneCritical Zone = "critical"
)

// State is a rebuildable in-memory view. All aggregate fields are updated in
// the same critical section as their reservation transition.
type State struct {
	mu sync.Mutex

	bySID       map[string]*Reservation
	tokenToSID  map[string]string
	cgroupToSID map[string]string

	allocatedMem     uint64
	allocatedCPU     uint64
	startupInFlight  uint64
	provisionalCount int
	unknownCount     int

	NodeBudget        Resources
	HostReserved      Resources
	OperationalMargin Resources
	AllocatablePool   Resources
	Wm                Watermarks
}

func NewState(physicalMem uint64, physicalCPUMilli uint64,
	hostReservedMem uint64, hostReservedCPUMilli uint64, wm Watermarks) *State {
	node := Resources{MemoryBytes: physicalMem, CPUMilli: physicalCPUMilli}
	host := Resources{MemoryBytes: hostReservedMem, CPUMilli: hostReservedCPUMilli}
	budget := node.Sub(host)
	margin := Resources{
		MemoryBytes: uint64(float64(budget.MemoryBytes) * wm.OperationalMarginFactor),
		CPUMilli:    uint64(float64(budget.CPUMilli) * wm.OperationalMarginFactor),
	}
	return &State{
		NodeBudget: node, HostReserved: host, OperationalMargin: margin,
		AllocatablePool: budget.Sub(margin), Wm: wm,
		bySID: make(map[string]*Reservation), tokenToSID: make(map[string]string),
		cgroupToSID: make(map[string]string),
	}
}

func IsPreSettled(stage string) bool {
	switch stage {
	case StageAdmitted, StageCreating, StageStartup, StageRestoring:
		return true
	default:
		return false
	}
}

func (s *State) startupPoolBytesLocked() uint64 {
	return uint64(float64(s.AllocatablePool.MemoryBytes) * s.Wm.StartupFactor)
}

func (s *State) memoryZoneLocked() Zone {
	pool := s.AllocatablePool.MemoryBytes
	if pool == 0 {
		return ZoneRed
	}
	high := uint64(float64(pool) * s.Wm.HighFactor)
	low := uint64(float64(pool) * s.Wm.LowFactor)
	emerg := uint64(float64(pool) * s.Wm.EmergencyFactor)
	switch {
	case s.allocatedMem >= pool-emerg:
		return ZoneCritical
	case s.allocatedMem >= high:
		return ZoneRed
	case s.allocatedMem >= low:
		return ZoneYellow
	default:
		return ZoneGreen
	}
}

type AdmissionSnapshot struct {
	Pool            Resources
	Allocated       Resources
	StartupPool     uint64
	StartupInFlight uint64
	EmergencyMemory uint64
	Zone            Zone
}

func (s *State) AdmissionSnapshot() AdmissionSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return AdmissionSnapshot{
		Pool:        s.AllocatablePool,
		Allocated:   Resources{MemoryBytes: s.allocatedMem, CPUMilli: s.allocatedCPU},
		StartupPool: s.startupPoolBytesLocked(), StartupInFlight: s.startupInFlight,
		EmergencyMemory: uint64(float64(s.AllocatablePool.MemoryBytes) * s.Wm.EmergencyFactor),
		Zone:            s.memoryZoneLocked(),
	}
}

type ResourceSnapshot struct {
	NodeBudget        Resources
	HostReserved      Resources
	OperationalMargin Resources
	AllocatablePool   Resources
	Allocated         Resources
	StartupInFlight   uint64
	ReservationCount  int
	ProvisionalCount  int
	UnknownCount      int
	Zone              Zone
}

func (s *State) ResourceSnapshot() ResourceSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.resourceSnapshotLocked()
}

func (s *State) resourceSnapshotLocked() ResourceSnapshot {
	return ResourceSnapshot{
		NodeBudget: s.NodeBudget, HostReserved: s.HostReserved,
		OperationalMargin: s.OperationalMargin, AllocatablePool: s.AllocatablePool,
		Allocated:       Resources{MemoryBytes: s.allocatedMem, CPUMilli: s.allocatedCPU},
		StartupInFlight: s.startupInFlight, ReservationCount: len(s.bySID),
		ProvisionalCount: s.provisionalCount, UnknownCount: s.unknownCount,
		Zone: s.memoryZoneLocked(),
	}
}

func (s *State) addAggregatesLocked(r *Reservation) {
	s.allocatedMem += r.AllocatableNowMem
	s.allocatedCPU += r.Floor.CPUMilli
	if IsPreSettled(r.Stage) {
		s.startupInFlight += r.EffectiveStartupBudget
	}
	if r.Provisional {
		s.provisionalCount++
	}
	if r.RecoverySource == RecoveryUnknownLease || r.RecoverySource == RecoveryUnknownManaged {
		s.unknownCount++
	}
}

func subtract(value *uint64, delta uint64) {
	if delta >= *value {
		*value = 0
	} else {
		*value -= delta
	}
}

func (s *State) removeAggregatesLocked(r *Reservation) {
	subtract(&s.allocatedMem, r.AllocatableNowMem)
	subtract(&s.allocatedCPU, r.Floor.CPUMilli)
	if IsPreSettled(r.Stage) {
		subtract(&s.startupInFlight, r.EffectiveStartupBudget)
	}
	if r.Provisional && s.provisionalCount > 0 {
		s.provisionalCount--
	}
	if (r.RecoverySource == RecoveryUnknownLease || r.RecoverySource == RecoveryUnknownManaged) && s.unknownCount > 0 {
		s.unknownCount--
	}
}

func cloneReservation(r *Reservation) Reservation {
	out := *r
	out.ClientFeatures = append([]string(nil), r.ClientFeatures...)
	return out
}

func (s *State) deleteLocked(sid string) *Reservation {
	r := s.bySID[sid]
	if r == nil {
		return nil
	}
	s.removeAggregatesLocked(r)
	delete(s.bySID, sid)
	if r.Token != "" && s.tokenToSID[r.Token] == sid {
		delete(s.tokenToSID, r.Token)
	}
	if r.CgroupPath != "" && s.cgroupToSID[r.CgroupPath] == sid {
		delete(s.cgroupToSID, r.CgroupPath)
	}
	return r
}

func (s *State) insertLocked(r *Reservation) error {
	if r == nil || r.SandboxID == "" {
		return fmt.Errorf("reservation sandbox id is required")
	}
	if _, exists := s.bySID[r.SandboxID]; exists {
		return fmt.Errorf("sandbox %q already has a reservation", r.SandboxID)
	}
	if r.Token != "" {
		if _, exists := s.tokenToSID[r.Token]; exists {
			return fmt.Errorf("token already exists")
		}
	}
	if r.CgroupPath != "" {
		if sid, exists := s.cgroupToSID[r.CgroupPath]; exists && sid != r.SandboxID {
			return fmt.Errorf("cgroup %q already belongs to %q", r.CgroupPath, sid)
		}
	}
	if r.Token != "" {
		s.tokenToSID[r.Token] = r.SandboxID
	}
	if r.CgroupPath != "" {
		s.cgroupToSID[r.CgroupPath] = r.SandboxID
	}
	s.bySID[r.SandboxID] = r
	s.addAggregatesLocked(r)
	return nil
}

type ProvisionalSpec struct {
	SandboxID      string
	PeerPID        int
	CgroupPath     string
	Capacity       Resources
	Floor          Resources
	MemoryCharge   uint64
	StartupCharge  uint64
	RecoverySource string
	RecoveryKey    string
	LeasePath      string
	ClientFeatures []string
	Now            time.Time
}

func (s *State) InstallProvisional(spec ProvisionalSpec) error {
	if spec.SandboxID == "" || spec.RecoveryKey == "" || spec.MemoryCharge == 0 {
		return fmt.Errorf("invalid provisional reservation")
	}
	if spec.Now.IsZero() {
		spec.Now = time.Now()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing := s.bySID[spec.SandboxID]; existing != nil {
		if existing.Provisional && existing.RecoveryKey == spec.RecoveryKey {
			return nil
		}
		return fmt.Errorf("sandbox %q already reserved", spec.SandboxID)
	}
	if spec.CgroupPath != "" {
		if sid, exists := s.cgroupToSID[spec.CgroupPath]; exists {
			return fmt.Errorf("cgroup %q already belongs to %q", spec.CgroupPath, sid)
		}
	}
	r := &Reservation{
		SandboxID: spec.SandboxID, PeerPID: spec.PeerPID, CgroupPath: spec.CgroupPath,
		Capacity: spec.Capacity, Floor: spec.Floor, AllocatableNowMem: spec.MemoryCharge,
		EffectiveStartupBudget: spec.StartupCharge, Stage: StageStartup,
		StageEnteredAt: spec.Now, LastHeartbeatAt: spec.Now,
		Provisional: true, RecoverySource: spec.RecoverySource,
		RecoveryKey: spec.RecoveryKey, LeasePath: spec.LeasePath,
		ClientFeatures: append([]string(nil), spec.ClientFeatures...),
	}
	return s.insertLocked(r)
}

func (s *State) HasCgroup(path string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.cgroupToSID[path]
	return ok
}

type AdmitSpec struct {
	SandboxID              string
	PeerPID                int
	CgroupPath             string
	Capacity               Resources
	Floor                  Resources
	InitialAllocatable     uint64
	EffectiveStartupBudget uint64
	Token                  string
	LeasePath              string
	ClientFeatures         []string
	Conn                   net.Conn
	Now                    time.Time
}

// CanReplayAdmit reports whether an Admit is replacing an already-accounted
// recovery upper bound or replaying an ACK-lost, not-yet-advanced session. It
// never authorizes a new consumer: identity and immutable resource contract
// must match exactly, and Admit performs the same checks again atomically.
func (s *State) CanReplayAdmit(spec AdmitSpec) bool {
	if spec.SandboxID == "" || spec.PeerPID <= 0 || spec.InitialAllocatable < spec.Floor.MemoryBytes ||
		spec.InitialAllocatable > spec.Capacity.MemoryBytes {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.bySID[spec.SandboxID]
	if r == nil || r.PeerPID != spec.PeerPID || r.CgroupPath != spec.CgroupPath ||
		r.Capacity != spec.Capacity || r.Floor != spec.Floor {
		return false
	}
	if r.Provisional {
		return true
	}
	return r.Stage == StageAdmitted && r.AllocatableNowMem == spec.InitialAllocatable &&
		r.EffectiveStartupBudget == spec.EffectiveStartupBudget
}

// Admit atomically replaces a matching provisional reservation or creates a
// new precise reservation. A retry by the same lease owner is idempotent and
// receives a fresh session token without double charging.
func (s *State) Admit(spec AdmitSpec) (Reservation, net.Conn, error) {
	if spec.SandboxID == "" || spec.Token == "" || spec.InitialAllocatable == 0 {
		return Reservation{}, nil, fmt.Errorf("invalid admit reservation")
	}
	if spec.Now.IsZero() {
		spec.Now = time.Now()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var oldConn net.Conn
	if existing := s.bySID[spec.SandboxID]; existing != nil {
		if existing.Provisional {
			if existing.PeerPID > 0 && spec.PeerPID > 0 && existing.PeerPID != spec.PeerPID {
				return Reservation{}, nil, fmt.Errorf("sandbox %q provisional owner mismatch", spec.SandboxID)
			}
			if existing.CgroupPath != "" && existing.CgroupPath != spec.CgroupPath {
				return Reservation{}, nil, fmt.Errorf("sandbox %q provisional cgroup mismatch", spec.SandboxID)
			}
			if existing.Capacity != (Resources{}) && existing.Capacity != spec.Capacity {
				return Reservation{}, nil, fmt.Errorf("sandbox %q provisional capacity mismatch", spec.SandboxID)
			}
			if existing.Floor != (Resources{}) && existing.Floor != spec.Floor {
				return Reservation{}, nil, fmt.Errorf("sandbox %q provisional floor mismatch", spec.SandboxID)
			}
		} else if existing.Stage != StageAdmitted || existing.PeerPID == 0 ||
			existing.PeerPID != spec.PeerPID || existing.CgroupPath != spec.CgroupPath ||
			existing.Capacity != spec.Capacity || existing.Floor != spec.Floor ||
			existing.AllocatableNowMem != spec.InitialAllocatable ||
			existing.EffectiveStartupBudget != spec.EffectiveStartupBudget {
			return Reservation{}, nil, fmt.Errorf("sandbox %q already admitted with a different session contract", spec.SandboxID)
		}
		oldConn = existing.Conn
	}
	if sid := s.tokenToSID[spec.Token]; sid != "" && sid != spec.SandboxID {
		return Reservation{}, nil, fmt.Errorf("token already belongs to sandbox %q", sid)
	}
	otherSID := s.cgroupToSID[spec.CgroupPath]
	if otherSID != "" && otherSID != spec.SandboxID {
		other := s.bySID[otherSID]
		if other == nil || !other.Provisional || other.RecoverySource != RecoveryCgroup {
			return Reservation{}, nil, fmt.Errorf("cgroup %q already belongs to live sandbox %q", spec.CgroupPath, otherSID)
		}
	}
	if s.bySID[spec.SandboxID] != nil {
		s.deleteLocked(spec.SandboxID)
	}
	if otherSID != "" && otherSID != spec.SandboxID {
		s.deleteLocked(otherSID)
	}
	r := &Reservation{
		Token: spec.Token, SandboxID: spec.SandboxID, PeerPID: spec.PeerPID,
		CgroupPath: spec.CgroupPath, Capacity: spec.Capacity, Floor: spec.Floor,
		AllocatableNowMem:      spec.InitialAllocatable,
		EffectiveStartupBudget: spec.EffectiveStartupBudget,
		Stage:                  StageAdmitted, StageEnteredAt: spec.Now, LastHeartbeatAt: spec.Now,
		RecoverySource: RecoveryAdmit, LeasePath: spec.LeasePath,
		ClientFeatures: append([]string(nil), spec.ClientFeatures...), Conn: spec.Conn,
	}
	if err := s.insertLocked(r); err != nil {
		return Reservation{}, oldConn, err
	}
	return cloneReservation(r), oldConn, nil
}

type SyncSpec struct {
	SandboxID      string
	PeerPID        int
	CgroupPath     string
	Capacity       Resources
	Floor          Resources
	StartupMemory  uint64
	AppliedMemory  uint64
	Settled        bool
	CurrentRSS     uint64
	Token          string
	LeasePath      string
	ClientFeatures []string
	Conn           net.Conn
	Now            time.Time
}

func (s *State) Sync(spec SyncSpec) (Reservation, []net.Conn, error) {
	if spec.SandboxID == "" || spec.Token == "" || spec.AppliedMemory < spec.Floor.MemoryBytes || spec.AppliedMemory > spec.Capacity.MemoryBytes {
		return Reservation{}, nil, fmt.Errorf("invalid state sync")
	}
	if spec.Now.IsZero() {
		spec.Now = time.Now()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var oldConns []net.Conn
	if existing := s.bySID[spec.SandboxID]; existing != nil {
		if existing.PeerPID > 0 && spec.PeerPID > 0 && existing.PeerPID != spec.PeerPID {
			return Reservation{}, nil, fmt.Errorf("sandbox %q owner mismatch", spec.SandboxID)
		}
		if existing.Conn != nil && existing.Conn != spec.Conn {
			oldConns = append(oldConns, existing.Conn)
		}
	}
	if sid := s.tokenToSID[spec.Token]; sid != "" && sid != spec.SandboxID {
		return Reservation{}, nil, fmt.Errorf("token already belongs to sandbox %q", sid)
	}
	otherSID := s.cgroupToSID[spec.CgroupPath]
	if otherSID != "" && otherSID != spec.SandboxID {
		other := s.bySID[otherSID]
		if other == nil || !other.Provisional || other.RecoverySource != RecoveryCgroup {
			return Reservation{}, nil, fmt.Errorf("cgroup %q already belongs to live sandbox %q", spec.CgroupPath, otherSID)
		}
		if other.Conn != nil && other.Conn != spec.Conn {
			oldConns = append(oldConns, other.Conn)
		}
	}
	if s.bySID[spec.SandboxID] != nil {
		s.deleteLocked(spec.SandboxID)
	}
	if otherSID != "" && otherSID != spec.SandboxID {
		s.deleteLocked(otherSID)
	}
	stage := StageStartup
	startup := spec.StartupMemory
	if startup < spec.Floor.MemoryBytes {
		startup = spec.Floor.MemoryBytes
	}
	if startup < spec.AppliedMemory {
		startup = spec.AppliedMemory
	}
	if spec.Settled {
		stage = StageSettled
		startup = 0
	}
	r := &Reservation{
		Token: spec.Token, SandboxID: spec.SandboxID, PeerPID: spec.PeerPID,
		CgroupPath: spec.CgroupPath, Capacity: spec.Capacity, Floor: spec.Floor,
		AllocatableNowMem: spec.AppliedMemory, EffectiveStartupBudget: startup,
		Stage: stage, StageEnteredAt: spec.Now, LastHeartbeatAt: spec.Now,
		RecoverySource: RecoverySynced, LeasePath: spec.LeasePath,
		ClientFeatures: append([]string(nil), spec.ClientFeatures...), Conn: spec.Conn,
	}
	if spec.CurrentRSS > 0 {
		r.LastReportedRSS = spec.CurrentRSS
		r.LastReportAt = spec.Now
	}
	if err := s.insertLocked(r); err != nil {
		return Reservation{}, oldConns, err
	}
	return cloneReservation(r), oldConns, nil
}

func (s *State) byTokenLocked(token string) *Reservation {
	return s.bySID[s.tokenToSID[token]]
}

func (s *State) Reattach(token string, conn net.Conn) (Reservation, net.Conn, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.byTokenLocked(token)
	if r == nil {
		return Reservation{}, nil, false
	}
	old := r.Conn
	r.Conn = conn
	r.LastHeartbeatAt = time.Now()
	return cloneReservation(r), old, true
}

func (s *State) SetSettled(token string, rss uint64, now time.Time) (Reservation, bool) {
	if now.IsZero() {
		now = time.Now()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.byTokenLocked(token)
	if r == nil {
		return Reservation{}, false
	}
	s.removeAggregatesLocked(r)
	alloc := rss
	if alloc < r.Floor.MemoryBytes {
		alloc = r.Floor.MemoryBytes
	}
	if alloc > r.Capacity.MemoryBytes {
		alloc = r.Capacity.MemoryBytes
	}
	r.AllocatableNowMem = alloc
	r.Stage, r.StageEnteredAt, r.LastHeartbeatAt = StageSettled, now, now
	r.EffectiveStartupBudget = 0
	if rss > 0 {
		r.LastReportedRSS, r.LastReportAt = rss, now
	}
	s.addAggregatesLocked(r)
	return cloneReservation(r), true
}

type GrantResult struct {
	Reservation Reservation
	Decision    GrantDecision
	Zone        Zone
}

func (s *State) Grant(token string, requested uint64, urgency string, allocator *Allocator) (GrantResult, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.byTokenLocked(token)
	if r == nil || r.Provisional {
		return GrantResult{}, false
	}
	zone := s.memoryZoneLocked()
	if (zone == ZoneRed || zone == ZoneCritical) && urgency != UrgencyHigh {
		return GrantResult{Reservation: cloneReservation(r), Decision: GrantDecision{CooldownMs: 500}, Zone: zone}, true
	}
	pool := s.AllocatablePool.MemoryBytes
	headroom := uint64(0)
	if pool > s.allocatedMem {
		headroom = pool - s.allocatedMem
	}
	if urgency != UrgencyHigh {
		emerg := uint64(float64(pool) * s.Wm.EmergencyFactor)
		if pool > s.allocatedMem+emerg {
			headroom = pool - s.allocatedMem - emerg
		} else {
			headroom = 0
		}
	}
	if capRoom := r.Capacity.MemoryBytes - r.AllocatableNowMem; headroom > capRoom {
		headroom = capRoom
	}
	decision := allocator.Grant(token, requested, headroom, urgency)
	if decision.GrantedDelta > 0 {
		s.removeAggregatesLocked(r)
		r.AllocatableNowMem += decision.GrantedDelta
		if r.Stage != StageBurst {
			r.Stage, r.StageEnteredAt = StageBurst, time.Now()
		}
		s.addAggregatesLocked(r)
	}
	return GrantResult{Reservation: cloneReservation(r), Decision: decision, Zone: zone}, true
}

func (s *State) Heartbeat(token string, rss uint64, now time.Time) (Reservation, bool) {
	if now.IsZero() {
		now = time.Now()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.byTokenLocked(token)
	if r == nil {
		return Reservation{}, false
	}
	r.LastHeartbeatAt = now
	if rss > 0 {
		r.LastReportedRSS, r.LastReportAt = rss, now
	}
	return cloneReservation(r), true
}

func (s *State) RecordOOM(token string, count uint64) (Reservation, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.byTokenLocked(token)
	if r == nil {
		return Reservation{}, false
	}
	r.OOMCount += count
	return cloneReservation(r), true
}

func (s *State) Release(token string) (Reservation, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.byTokenLocked(token)
	if r == nil {
		return Reservation{}, false
	}
	out := cloneReservation(r)
	s.deleteLocked(r.SandboxID)
	return out, true
}

func (s *State) DropConnection(token string, conn net.Conn) (Reservation, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.byTokenLocked(token)
	if r == nil || r.Conn != conn {
		return Reservation{}, false
	}
	r.Conn = nil
	return cloneReservation(r), true
}

func (s *State) AdminGrant(sid string, delta uint64) (Reservation, uint64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.bySID[sid]
	if r == nil || r.Provisional {
		return Reservation{}, 0, false
	}
	newAlloc := r.AllocatableNowMem + delta
	if newAlloc < r.AllocatableNowMem || newAlloc > r.Capacity.MemoryBytes {
		newAlloc = r.Capacity.MemoryBytes
	}
	actual := newAlloc - r.AllocatableNowMem
	s.removeAggregatesLocked(r)
	r.AllocatableNowMem = newAlloc
	s.addAggregatesLocked(r)
	return cloneReservation(r), actual, true
}

func (s *State) AdminReclaim(sid string, target uint64) (Reservation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.bySID[sid]
	if r == nil || r.Provisional {
		return Reservation{}, fmt.Errorf("no precise reservation for sandbox %s", sid)
	}
	if target < r.Floor.MemoryBytes {
		target = r.Floor.MemoryBytes
	}
	if target > r.AllocatableNowMem {
		return Reservation{}, fmt.Errorf("target above current allocatable; use admin_grant to grow")
	}
	s.removeAggregatesLocked(r)
	r.AllocatableNowMem = target
	s.addAggregatesLocked(r)
	return cloneReservation(r), nil
}

type ReclaimEvent struct {
	Before Reservation
	After  Reservation
}

func (s *State) ReclaimSettled(defaultMargin float64) []ReclaimEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	margin := defaultMargin
	switch s.memoryZoneLocked() {
	case ZoneYellow:
		margin = 1.10
	case ZoneRed:
		margin = 1.05
	case ZoneCritical:
		margin = 1.00
	}
	var events []ReclaimEvent
	for _, r := range s.bySID {
		if r.Provisional || r.Stage != StageSettled {
			continue
		}
		ws := r.LastReportedRSS
		if ws == 0 {
			ws = r.Floor.MemoryBytes
		}
		target := uint64(float64(ws) * margin)
		if target < r.Floor.MemoryBytes {
			target = r.Floor.MemoryBytes
		}
		if target >= r.AllocatableNowMem {
			continue
		}
		before := cloneReservation(r)
		s.removeAggregatesLocked(r)
		r.AllocatableNowMem = target
		s.addAggregatesLocked(r)
		events = append(events, ReclaimEvent{Before: before, After: cloneReservation(r)})
	}
	return events
}

func (s *State) SweepSnapshot() []Reservation {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Reservation, 0, len(s.bySID))
	for _, r := range s.bySID {
		out = append(out, cloneReservation(r))
	}
	return out
}

func (s *State) MarkStartupExpired(sid, identity string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.bySID[sid]
	if r == nil || r.identity() != identity {
		return false
	}
	r.StartupExpired = true
	return true
}

func (s *State) RemoveIfIdentity(sid, identity string) (Reservation, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.bySID[sid]
	if r == nil || r.identity() != identity {
		return Reservation{}, false
	}
	out := cloneReservation(r)
	s.deleteLocked(sid)
	return out, true
}

type SandboxResourceSnapshot struct {
	Capacity        Resources
	CPUAllocatable  uint64
	MemAllocatable  uint64
	LastReportedRSS uint64
	LastReportAt    time.Time
}

func (s *State) SnapshotSandboxResource(sandboxID string) (SandboxResourceSnapshot, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.bySID[sandboxID]
	if r == nil {
		return SandboxResourceSnapshot{}, false
	}
	return SandboxResourceSnapshot{
		Capacity: r.Capacity, CPUAllocatable: r.Floor.CPUMilli,
		MemAllocatable: r.AllocatableNowMem, LastReportedRSS: r.LastReportedRSS,
		LastReportAt: r.LastReportAt,
	}, true
}

func (s *State) ReservationViews() []Reservation {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Reservation, 0, len(s.bySID))
	for _, r := range s.bySID {
		out = append(out, cloneReservation(r))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].SandboxID < out[j].SandboxID })
	return out
}

// RestoreReservations exists only for the temporary compatibility phase while
// the old Persister is still present. New recovery never calls it.
func (s *State) RestoreReservations(reservations map[string]*Reservation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.bySID = make(map[string]*Reservation)
	s.tokenToSID = make(map[string]string)
	s.cgroupToSID = make(map[string]string)
	s.allocatedMem, s.allocatedCPU, s.startupInFlight = 0, 0, 0
	s.provisionalCount, s.unknownCount = 0, 0
	for token, input := range reservations {
		if input == nil || token == "" || input.Token != token || input.SandboxID == "" {
			return fmt.Errorf("invalid persisted reservation %q", token)
		}
		r := cloneReservation(input)
		r.Conn = nil
		if err := s.insertLocked(&r); err != nil {
			return err
		}
	}
	return nil
}

func (s *State) PersistenceReservations() map[string]*Reservation {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]*Reservation)
	for _, r := range s.bySID {
		if r.Token == "" {
			continue
		}
		copy := cloneReservation(r)
		copy.Conn = nil
		out[r.Token] = &copy
	}
	return out
}

// MergePersistedReservations is temporary rolling-upgrade compatibility. Safe
// inventory wins: a persisted record is admitted only when neither its SID nor
// cgroup was discovered from a live lease/pidfile/cgroup.
func (s *State) MergePersistedReservations(reservations map[string]*Reservation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for token, input := range reservations {
		if input == nil || token == "" || input.Token != token || input.SandboxID == "" {
			return fmt.Errorf("invalid persisted reservation %q", token)
		}
		if s.bySID[input.SandboxID] != nil {
			continue
		}
		if input.CgroupPath != "" && s.cgroupToSID[input.CgroupPath] != "" {
			continue
		}
		copy := cloneReservation(input)
		copy.Conn = nil
		copy.RecoverySource = RecoveryPersisted
		copy.RecoveryKey = "persisted:" + token
		if err := s.insertLocked(&copy); err != nil {
			return err
		}
	}
	return nil
}
