package nodectl

import (
	"fmt"
	"math"
	"math/big"
	"math/bits"
	"net"
	"slices"
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
)

type Resources struct {
	MemoryBytes uint64 `json:"memory_bytes"`
	CPUMilli    uint64 `json:"cpu_milli"`
}

// cpuMilliCeil keeps resource accounting on the conservative side and maps
// every positive CPU floor to a non-zero protocol value. sandboxer's cgroup
// policy likewise maps sub-millicore allocations to the minimum CPU weight.
func cpuMilliCeil(cpu float64) uint64 {
	if cpu <= 0 {
		return 0
	}
	return uint64(math.Ceil(cpu * 1000))
}

// scaleUint64Floor applies a validated configuration factor without first
// converting the byte count to float64. That conversion loses integer
// precision above 2^53 and can make admission thresholds inconsistent with
// the checked uint64 reservation aggregate. Factors outside [0, 1] are
// conservatively clamped; ResolveConfig rejects them before normal use.
func scaleUint64Floor(value uint64, factor float64) uint64 {
	if value == 0 || factor <= 0 || math.IsNaN(factor) {
		return 0
	}
	if factor >= 1 {
		return value
	}
	ratio := new(big.Rat).SetFloat64(factor)
	if ratio == nil {
		return 0
	}
	scaled := new(big.Int).Mul(new(big.Int).SetUint64(value), ratio.Num())
	scaled.Quo(scaled, ratio.Denom())
	return scaled.Uint64()
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

// Reservation is controller-owned mutable state. State methods return copies;
// no caller receives a pointer stored in the maps.
type Reservation struct {
	Token      string    `json:"token"`
	SandboxID  string    `json:"sandbox_id"`
	PeerPID    int       `json:"sandbox_ctl_pid,omitempty"`
	CgroupPath string    `json:"cgroup_path,omitempty"`
	Capacity   Resources `json:"capacity"`
	// ConfiguredAllocatable.MemoryBytes is settled guest headroom;
	// ConfiguredAllocatable.CPUMilli retains the CPU guarantee/weight meaning.
	// ReservationMemory is node charge, not guest demand or a balloon/cgroup
	// target. InitialBudget is counted by the pre-settled concurrency pool and
	// becomes zero at Settled without changing ReservationMemory.
	ConfiguredAllocatable Resources `json:"configured_allocatable"`
	ReservationMemory     uint64    `json:"reservation_memory"`
	InitialBudget         uint64    `json:"initial_budget,omitempty"`

	Stage                 string    `json:"stage"`
	StageEnteredAt        time.Time `json:"stage_entered_at"`
	LastHeartbeatAt       time.Time `json:"last_heartbeat_at"`
	OOMCount              uint64    `json:"oom_count"`
	LastHostMemoryCurrent uint64    `json:"last_host_memory_current,omitempty"`
	LastReportAt          time.Time `json:"last_report_at,omitempty"`

	Provisional    bool     `json:"provisional,omitempty"`
	RecoverySource string   `json:"recovery_source,omitempty"`
	RecoveryKey    string   `json:"recovery_key,omitempty"`
	LeasePath      string   `json:"lease_path,omitempty"`
	StartupExpired bool     `json:"startup_expired,omitempty"`
	ClientFeatures []string `json:"client_features,omitempty"`
	Conn           net.Conn `json:"-"`
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

	reservedMemory   uint64
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
		MemoryBytes: scaleUint64Floor(budget.MemoryBytes, wm.OperationalMarginFactor),
		CPUMilli:    scaleUint64Floor(budget.CPUMilli, wm.OperationalMarginFactor),
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
	return scaleUint64Floor(s.AllocatablePool.MemoryBytes, s.Wm.StartupFactor)
}

func (s *State) memoryZoneLocked() Zone {
	return s.memoryZoneForReservedLocked(s.reservedMemory)
}

func (s *State) memoryZoneForReservedLocked(reserved uint64) Zone {
	pool := s.AllocatablePool.MemoryBytes
	if pool == 0 {
		return ZoneRed
	}
	high := scaleUint64Floor(pool, s.Wm.HighFactor)
	low := scaleUint64Floor(pool, s.Wm.LowFactor)
	emerg := scaleUint64Floor(pool, s.Wm.EmergencyFactor)
	criticalStart := uint64(0)
	if emerg < pool {
		criticalStart = pool - emerg
	}
	switch {
	case reserved >= criticalStart:
		return ZoneCritical
	case reserved >= high:
		return ZoneRed
	case reserved >= low:
		return ZoneYellow
	default:
		return ZoneGreen
	}
}

type AdmissionSnapshot struct {
	Pool            Resources
	Reserved        Resources
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
		Reserved:    Resources{MemoryBytes: s.reservedMemory, CPUMilli: s.allocatedCPU},
		StartupPool: s.startupPoolBytesLocked(), StartupInFlight: s.startupInFlight,
		EmergencyMemory: scaleUint64Floor(s.AllocatablePool.MemoryBytes, s.Wm.EmergencyFactor),
		Zone:            s.memoryZoneLocked(),
	}
}

type ResourceSnapshot struct {
	NodeBudget        Resources
	HostReserved      Resources
	OperationalMargin Resources
	AllocatablePool   Resources
	Reserved          Resources
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
		Reserved:        Resources{MemoryBytes: s.reservedMemory, CPUMilli: s.allocatedCPU},
		StartupInFlight: s.startupInFlight, ReservationCount: len(s.bySID),
		ProvisionalCount: s.provisionalCount, UnknownCount: s.unknownCount,
		Zone: s.memoryZoneLocked(),
	}
}

func (s *State) addAggregatesLocked(r *Reservation) error {
	mem, err := checkedAggregateAdd(s.reservedMemory, r.ReservationMemory, "memory reservation")
	if err != nil {
		return err
	}
	cpu, err := checkedAggregateAdd(s.allocatedCPU, r.ConfiguredAllocatable.CPUMilli, "CPU allocation")
	if err != nil {
		return err
	}
	startup, err := checkedAggregateAdd(s.startupInFlight, aggregateStartupCharge(r), "initial Budget")
	if err != nil {
		return err
	}
	s.reservedMemory, s.allocatedCPU, s.startupInFlight = mem, cpu, startup
	if r.Provisional {
		s.provisionalCount++
	}
	if r.RecoverySource == RecoveryUnknownLease || r.RecoverySource == RecoveryUnknownManaged {
		s.unknownCount++
	}
	return nil
}

func aggregateStartupCharge(r *Reservation) uint64 {
	if r != nil && IsPreSettled(r.Stage) {
		return r.InitialBudget
	}
	return 0
}

func checkedAggregateAdd(current, delta uint64, name string) (uint64, error) {
	next, carry := bits.Add64(current, delta, 0)
	if carry != 0 {
		return 0, fmt.Errorf("%s aggregate overflow", name)
	}
	return next, nil
}

func checkedAggregateSub(current, delta uint64, name string) (uint64, error) {
	if delta > current {
		return 0, fmt.Errorf("%s aggregate underflow", name)
	}
	return current - delta, nil
}

// validateAggregateReplacementLocked proves that removing the listed current
// reservations and inserting add cannot overflow any conservative aggregate.
// It runs before indexes/maps are changed so a failed Admit or StateSync keeps
// the previous charge intact.
func (s *State) validateAggregateReplacementLocked(remove []*Reservation, add *Reservation) error {
	mem, cpu, startup := s.reservedMemory, s.allocatedCPU, s.startupInFlight
	provisional, unknown := s.provisionalCount, s.unknownCount
	seen := make(map[*Reservation]struct{}, len(remove))
	for _, r := range remove {
		if r == nil {
			continue
		}
		if _, ok := seen[r]; ok {
			continue
		}
		seen[r] = struct{}{}
		var err error
		if mem, err = checkedAggregateSub(mem, r.ReservationMemory, "memory reservation"); err != nil {
			return err
		}
		if cpu, err = checkedAggregateSub(cpu, r.ConfiguredAllocatable.CPUMilli, "CPU allocation"); err != nil {
			return err
		}
		if startup, err = checkedAggregateSub(startup, aggregateStartupCharge(r), "initial Budget"); err != nil {
			return err
		}
		if r.Provisional {
			if provisional <= 0 {
				return fmt.Errorf("provisional reservation aggregate underflow")
			}
			provisional--
		}
		if r.RecoverySource == RecoveryUnknownLease || r.RecoverySource == RecoveryUnknownManaged {
			if unknown <= 0 {
				return fmt.Errorf("unknown reservation aggregate underflow")
			}
			unknown--
		}
	}
	var err error
	if add != nil {
		if mem, err = checkedAggregateAdd(mem, add.ReservationMemory, "memory reservation"); err != nil {
			return err
		}
		if cpu, err = checkedAggregateAdd(cpu, add.ConfiguredAllocatable.CPUMilli, "CPU allocation"); err != nil {
			return err
		}
		_, err = checkedAggregateAdd(startup, aggregateStartupCharge(add), "initial Budget")
	}
	return err
}

func (s *State) replaceReservationMemoryLocked(r *Reservation, memory uint64) error {
	if r.ReservationMemory == memory {
		return nil
	}
	next := cloneReservation(r)
	next.ReservationMemory = memory
	if err := s.validateAggregateReplacementLocked([]*Reservation{r}, &next); err != nil {
		return err
	}
	if err := s.removeAggregatesLocked(r); err != nil {
		return err
	}
	old := r.ReservationMemory
	r.ReservationMemory = memory
	if err := s.addAggregatesLocked(r); err != nil {
		r.ReservationMemory = old
		_ = s.addAggregatesLocked(r)
		return err
	}
	return nil
}

func (s *State) removeAggregatesLocked(r *Reservation) error {
	mem, err := checkedAggregateSub(s.reservedMemory, r.ReservationMemory, "memory reservation")
	if err != nil {
		return err
	}
	cpu, err := checkedAggregateSub(s.allocatedCPU, r.ConfiguredAllocatable.CPUMilli, "CPU allocation")
	if err != nil {
		return err
	}
	startup, err := checkedAggregateSub(s.startupInFlight, aggregateStartupCharge(r), "initial Budget")
	if err != nil {
		return err
	}
	if r.Provisional && s.provisionalCount <= 0 {
		return fmt.Errorf("provisional reservation aggregate underflow")
	}
	unknown := r.RecoverySource == RecoveryUnknownLease || r.RecoverySource == RecoveryUnknownManaged
	if unknown && s.unknownCount <= 0 {
		return fmt.Errorf("unknown reservation aggregate underflow")
	}
	s.reservedMemory, s.allocatedCPU, s.startupInFlight = mem, cpu, startup
	if r.Provisional {
		s.provisionalCount--
	}
	if unknown {
		s.unknownCount--
	}
	return nil
}

func cloneReservation(r *Reservation) Reservation {
	out := *r
	out.ClientFeatures = append([]string(nil), r.ClientFeatures...)
	return out
}

func (s *State) deleteLocked(sid string) (*Reservation, error) {
	r := s.bySID[sid]
	if r == nil {
		return nil, nil
	}
	if err := s.removeAggregatesLocked(r); err != nil {
		return nil, err
	}
	delete(s.bySID, sid)
	if r.Token != "" && s.tokenToSID[r.Token] == sid {
		delete(s.tokenToSID, r.Token)
	}
	if r.CgroupPath != "" && s.cgroupToSID[r.CgroupPath] == sid {
		delete(s.cgroupToSID, r.CgroupPath)
	}
	return r, nil
}

func (s *State) insertLocked(r *Reservation) error {
	if err := validateReservation(r); err != nil {
		return err
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
	if err := s.validateAggregateReplacementLocked(nil, r); err != nil {
		return err
	}
	if r.Token != "" {
		s.tokenToSID[r.Token] = r.SandboxID
	}
	if r.CgroupPath != "" {
		s.cgroupToSID[r.CgroupPath] = r.SandboxID
	}
	s.bySID[r.SandboxID] = r
	if err := s.addAggregatesLocked(r); err != nil {
		delete(s.bySID, r.SandboxID)
		if r.Token != "" && s.tokenToSID[r.Token] == r.SandboxID {
			delete(s.tokenToSID, r.Token)
		}
		if r.CgroupPath != "" && s.cgroupToSID[r.CgroupPath] == r.SandboxID {
			delete(s.cgroupToSID, r.CgroupPath)
		}
		return err
	}
	return nil
}

func validateReservation(r *Reservation) error {
	if r == nil || r.SandboxID == "" {
		return fmt.Errorf("reservation sandbox id is required")
	}
	if r.ReservationMemory > r.Capacity.MemoryBytes || r.ConfiguredAllocatable.MemoryBytes > r.Capacity.MemoryBytes ||
		r.ConfiguredAllocatable.CPUMilli > r.Capacity.CPUMilli || r.InitialBudget > r.Capacity.MemoryBytes {
		return fmt.Errorf("reservation resources exceed capacity")
	}
	return nil
}

type ProvisionalSpec struct {
	SandboxID             string
	PeerPID               int
	CgroupPath            string
	Capacity              Resources
	ConfiguredAllocatable Resources
	ReservationMemory     uint64
	InitialBudget         uint64
	RecoverySource        string
	RecoveryKey           string
	LeasePath             string
	ClientFeatures        []string
	Now                   time.Time
}

func (s *State) InstallProvisional(spec ProvisionalSpec) error {
	if spec.SandboxID == "" || spec.RecoveryKey == "" || spec.ReservationMemory == 0 {
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
		Capacity: spec.Capacity, ConfiguredAllocatable: spec.ConfiguredAllocatable,
		ReservationMemory: spec.ReservationMemory, InitialBudget: spec.InitialBudget, Stage: StageStartup,
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
	SandboxID             string
	PeerPID               int
	CgroupPath            string
	Capacity              Resources
	ConfiguredAllocatable Resources
	InitialBudget         uint64
	Token                 string
	LeasePath             string
	ClientFeatures        []string
	Conn                  net.Conn
	Now                   time.Time
}

// CanReplayAdmit reports whether an Admit is replacing an already-accounted
// recovery upper bound or replaying an ACK-lost, not-yet-advanced session. It
// never authorizes a new consumer: identity and immutable resource contract
// must match exactly, and Admit performs the same checks again atomically.
func (s *State) CanReplayAdmit(spec AdmitSpec) bool {
	if spec.SandboxID == "" || spec.PeerPID <= 0 || spec.InitialBudget == 0 ||
		spec.InitialBudget > spec.Capacity.MemoryBytes {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.bySID[spec.SandboxID]
	if r == nil || r.PeerPID != spec.PeerPID || r.CgroupPath != spec.CgroupPath ||
		r.Capacity != spec.Capacity || r.ConfiguredAllocatable != spec.ConfiguredAllocatable ||
		!slices.Equal(r.ClientFeatures, spec.ClientFeatures) {
		return false
	}
	if r.Provisional {
		return spec.InitialBudget <= r.ReservationMemory
	}
	return r.Stage == StageAdmitted && r.ReservationMemory == spec.InitialBudget &&
		r.InitialBudget == spec.InitialBudget
}

// Admit atomically replaces a matching provisional reservation or creates a
// new precise reservation. A retry by the same lease owner is idempotent and
// receives a fresh session token without double charging.
func (s *State) Admit(spec AdmitSpec) (Reservation, net.Conn, error) {
	if spec.SandboxID == "" || spec.Token == "" || spec.InitialBudget == 0 {
		return Reservation{}, nil, fmt.Errorf("invalid admit reservation")
	}
	if spec.Now.IsZero() {
		spec.Now = time.Now()
	}
	r := &Reservation{
		Token: spec.Token, SandboxID: spec.SandboxID, PeerPID: spec.PeerPID,
		CgroupPath: spec.CgroupPath, Capacity: spec.Capacity, ConfiguredAllocatable: spec.ConfiguredAllocatable,
		ReservationMemory: spec.InitialBudget,
		InitialBudget:     spec.InitialBudget,
		Stage:             StageAdmitted, StageEnteredAt: spec.Now, LastHeartbeatAt: spec.Now,
		RecoverySource: RecoveryAdmit, LeasePath: spec.LeasePath,
		ClientFeatures: append([]string(nil), spec.ClientFeatures...), Conn: spec.Conn,
	}
	if err := validateReservation(r); err != nil {
		return Reservation{}, nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var oldConn net.Conn
	if existing := s.bySID[spec.SandboxID]; existing != nil {
		if existing.Provisional {
			if spec.InitialBudget > existing.ReservationMemory {
				return Reservation{}, nil, fmt.Errorf("sandbox %q initial reservation %d exceeds recovered charge %d", spec.SandboxID, spec.InitialBudget, existing.ReservationMemory)
			}
			if existing.PeerPID > 0 && spec.PeerPID > 0 && existing.PeerPID != spec.PeerPID {
				return Reservation{}, nil, fmt.Errorf("sandbox %q provisional owner mismatch", spec.SandboxID)
			}
			if existing.CgroupPath != "" && existing.CgroupPath != spec.CgroupPath {
				return Reservation{}, nil, fmt.Errorf("sandbox %q provisional cgroup mismatch", spec.SandboxID)
			}
			if existing.Capacity != (Resources{}) && existing.Capacity != spec.Capacity {
				return Reservation{}, nil, fmt.Errorf("sandbox %q provisional capacity mismatch", spec.SandboxID)
			}
			if existing.ConfiguredAllocatable != (Resources{}) && existing.ConfiguredAllocatable != spec.ConfiguredAllocatable {
				return Reservation{}, nil, fmt.Errorf("sandbox %q provisional headroom mismatch", spec.SandboxID)
			}
			if !slices.Equal(existing.ClientFeatures, spec.ClientFeatures) {
				return Reservation{}, nil, fmt.Errorf("sandbox %q provisional client features mismatch", spec.SandboxID)
			}
		} else if existing.Stage != StageAdmitted || existing.PeerPID == 0 ||
			existing.PeerPID != spec.PeerPID || existing.CgroupPath != spec.CgroupPath ||
			existing.Capacity != spec.Capacity || existing.ConfiguredAllocatable != spec.ConfiguredAllocatable ||
			existing.ReservationMemory != spec.InitialBudget ||
			existing.InitialBudget != spec.InitialBudget ||
			!slices.Equal(existing.ClientFeatures, spec.ClientFeatures) {
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
	var replacements []*Reservation
	if existing := s.bySID[spec.SandboxID]; existing != nil {
		replacements = append(replacements, existing)
	}
	if otherSID != "" && otherSID != spec.SandboxID {
		replacements = append(replacements, s.bySID[otherSID])
	}
	if err := s.validateAggregateReplacementLocked(replacements, r); err != nil {
		return Reservation{}, oldConn, err
	}
	if s.bySID[spec.SandboxID] != nil {
		if _, err := s.deleteLocked(spec.SandboxID); err != nil {
			return Reservation{}, oldConn, err
		}
	}
	if otherSID != "" && otherSID != spec.SandboxID {
		if _, err := s.deleteLocked(otherSID); err != nil {
			return Reservation{}, oldConn, err
		}
	}
	if err := s.insertLocked(r); err != nil {
		return Reservation{}, oldConn, err
	}
	return cloneReservation(r), oldConn, nil
}

type SyncSpec struct {
	SandboxID             string
	PeerPID               int
	CgroupPath            string
	Capacity              Resources
	ConfiguredAllocatable Resources
	ReservationMemory     uint64
	Settled               bool
	HostMemoryCurrent     uint64
	Token                 string
	LeasePath             string
	ClientFeatures        []string
	Conn                  net.Conn
	Now                   time.Time
}

func (s *State) Sync(spec SyncSpec) (Reservation, []net.Conn, error) {
	if spec.SandboxID == "" || spec.Token == "" || spec.ReservationMemory == 0 || spec.ReservationMemory > spec.Capacity.MemoryBytes {
		return Reservation{}, nil, fmt.Errorf("invalid state sync")
	}
	if spec.Now.IsZero() {
		spec.Now = time.Now()
	}
	stage := StageStartup
	initialBudget := spec.ReservationMemory
	if spec.Settled {
		stage = StageSettled
		initialBudget = 0
	}
	r := &Reservation{
		Token: spec.Token, SandboxID: spec.SandboxID, PeerPID: spec.PeerPID,
		CgroupPath: spec.CgroupPath, Capacity: spec.Capacity, ConfiguredAllocatable: spec.ConfiguredAllocatable,
		ReservationMemory: spec.ReservationMemory, InitialBudget: initialBudget,
		Stage: stage, StageEnteredAt: spec.Now, LastHeartbeatAt: spec.Now,
		RecoverySource: RecoverySynced, LeasePath: spec.LeasePath,
		ClientFeatures: append([]string(nil), spec.ClientFeatures...), Conn: spec.Conn,
	}
	if spec.HostMemoryCurrent > 0 {
		r.LastHostMemoryCurrent = spec.HostMemoryCurrent
		r.LastReportAt = spec.Now
	}
	if err := validateReservation(r); err != nil {
		return Reservation{}, nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var oldConns []net.Conn
	existing := s.bySID[spec.SandboxID]
	if existing != nil {
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
	if existing == nil && otherSID == "" {
		return Reservation{}, nil, fmt.Errorf("state sync has no recovered reservation")
	}
	if otherSID != "" && otherSID != spec.SandboxID {
		other := s.bySID[otherSID]
		if other == nil || !other.Provisional || other.RecoverySource != RecoveryCgroup {
			return Reservation{}, nil, fmt.Errorf("cgroup %q already belongs to live sandbox %q", spec.CgroupPath, otherSID)
		}
		if other.Conn != nil && other.Conn != spec.Conn {
			oldConns = append(oldConns, other.Conn)
		}
	}
	var replacements []*Reservation
	if existing != nil {
		replacements = append(replacements, existing)
	}
	if otherSID != "" && otherSID != spec.SandboxID {
		replacements = append(replacements, s.bySID[otherSID])
	}
	// StateSync reconciles an already charged live/recovered consumer. Normal
	// paths replace Capacity-provisional or response-ambiguous state with an
	// equal or smaller sandbox baseline; allowing it to manufacture a larger
	// charge would bypass admission and could reuse pool headroom already
	// granted elsewhere.
	replacedMemory := uint64(0)
	seenReplacement := make(map[*Reservation]struct{}, len(replacements))
	for _, replaced := range replacements {
		if replaced == nil {
			continue
		}
		if _, seen := seenReplacement[replaced]; seen {
			continue
		}
		seenReplacement[replaced] = struct{}{}
		var addErr error
		replacedMemory, addErr = checkedAggregateAdd(replacedMemory, replaced.ReservationMemory, "state sync replacement")
		if addErr != nil {
			return Reservation{}, oldConns, addErr
		}
	}
	if r.ReservationMemory > replacedMemory {
		return Reservation{}, oldConns, fmt.Errorf("state sync reservation %d exceeds recovered charge %d", r.ReservationMemory, replacedMemory)
	}
	if err := s.validateAggregateReplacementLocked(replacements, r); err != nil {
		return Reservation{}, oldConns, err
	}
	if s.bySID[spec.SandboxID] != nil {
		if _, err := s.deleteLocked(spec.SandboxID); err != nil {
			return Reservation{}, oldConns, err
		}
	}
	if otherSID != "" && otherSID != spec.SandboxID {
		if _, err := s.deleteLocked(otherSID); err != nil {
			return Reservation{}, oldConns, err
		}
	}
	if err := s.insertLocked(r); err != nil {
		return Reservation{}, oldConns, err
	}
	return cloneReservation(r), oldConns, nil
}

func (s *State) byTokenLocked(token string) *Reservation {
	return s.bySID[s.tokenToSID[token]]
}

func (s *State) SetSettled(token string, hostMemoryCurrent uint64, now time.Time) (Reservation, bool, error) {
	if now.IsZero() {
		now = time.Now()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.byTokenLocked(token)
	if r == nil {
		return Reservation{}, false, nil
	}
	next := cloneReservation(r)
	next.Stage, next.InitialBudget = StageSettled, 0
	if err := s.validateAggregateReplacementLocked([]*Reservation{r}, &next); err != nil {
		return Reservation{}, true, err
	}
	if err := s.removeAggregatesLocked(r); err != nil {
		return Reservation{}, true, err
	}
	oldStage, oldStageEnteredAt, oldHeartbeatAt := r.Stage, r.StageEnteredAt, r.LastHeartbeatAt
	oldInitialBudget := r.InitialBudget
	oldHostMemoryCurrent, oldReportAt := r.LastHostMemoryCurrent, r.LastReportAt
	r.Stage, r.StageEnteredAt, r.LastHeartbeatAt = StageSettled, now, now
	r.InitialBudget = 0
	if hostMemoryCurrent > 0 {
		r.LastHostMemoryCurrent, r.LastReportAt = hostMemoryCurrent, now
	}
	if err := s.addAggregatesLocked(r); err != nil {
		r.Stage, r.StageEnteredAt, r.LastHeartbeatAt = oldStage, oldStageEnteredAt, oldHeartbeatAt
		r.InitialBudget = oldInitialBudget
		r.LastHostMemoryCurrent, r.LastReportAt = oldHostMemoryCurrent, oldReportAt
		_ = s.addAggregatesLocked(r)
		return Reservation{}, true, err
	}
	return cloneReservation(r), true, nil
}

type GrantResult struct {
	Reservation Reservation
	Decision    GrantDecision
	Zone        Zone
}

// ReconcileAndGrant applies one existing RequestBudget transaction atomically.
// current is the sandbox's absolute safe reservation baseline. It may reduce
// the controller's value after a locally completed shrink, but may never
// exceed it. requested==0 is therefore a pure shrink commit. Any grow grant is
// computed only after that reconciliation and the response always describes
// current+GrantedDelta.
func (s *State) ReconcileAndGrant(token string, current, requested uint64, urgency string, allocator *Allocator) (GrantResult, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.byTokenLocked(token)
	if r == nil || r.Provisional {
		return GrantResult{}, false, nil
	}
	if current == 0 || current > r.Capacity.MemoryBytes {
		return GrantResult{}, true, fmt.Errorf("current reservation %d outside (0, %d]", current, r.Capacity.MemoryBytes)
	}
	if current > r.ReservationMemory {
		return GrantResult{}, true, fmt.Errorf("current reservation %d exceeds controller reservation %d", current, r.ReservationMemory)
	}
	oldReservation := r.ReservationMemory
	reusable := oldReservation - current
	reused := requested
	if reused > reusable {
		reused = reusable
	}
	baseReservation, carry := bits.Add64(current, reused, 0)
	if carry != 0 || baseReservation > r.Capacity.MemoryBytes {
		return GrantResult{}, true, fmt.Errorf("reconciled reservation overflows capacity")
	}
	reservedWithoutCurrent, err := checkedAggregateSub(s.reservedMemory, oldReservation, "memory reservation")
	if err != nil {
		return GrantResult{}, true, err
	}
	baseReserved, err := checkedAggregateAdd(reservedWithoutCurrent, baseReservation, "memory reservation")
	if err != nil {
		return GrantResult{}, true, err
	}
	baseView := cloneReservation(r)
	baseView.ReservationMemory = baseReservation
	if err := s.validateAggregateReplacementLocked([]*Reservation{r}, &baseView); err != nil {
		return GrantResult{}, true, err
	}
	zone := s.memoryZoneForReservedLocked(baseReserved)
	decision := GrantDecision{GrantedDelta: reused}
	remaining := requested - reused
	if remaining == 0 {
		if err := s.replaceReservationMemoryLocked(r, baseReservation); err != nil {
			return GrantResult{}, true, err
		}
		return GrantResult{Reservation: cloneReservation(r), Decision: decision, Zone: zone}, true, nil
	}
	if (zone == ZoneRed || zone == ZoneCritical) && urgency != UrgencyHigh {
		decision.CooldownMs = 500
		if err := s.replaceReservationMemoryLocked(r, baseReservation); err != nil {
			return GrantResult{}, true, err
		}
		return GrantResult{Reservation: cloneReservation(r), Decision: decision, Zone: zone}, true, nil
	}
	pool := s.AllocatablePool.MemoryBytes
	headroom := uint64(0)
	if pool > baseReserved {
		headroom = pool - baseReserved
	}
	if urgency != UrgencyHigh {
		emerg := scaleUint64Floor(pool, s.Wm.EmergencyFactor)
		if baseReserved <= pool && emerg <= pool-baseReserved {
			headroom = pool - baseReserved - emerg
		} else {
			headroom = 0
		}
	}
	capRoom := uint64(0)
	if r.Capacity.MemoryBytes > baseReservation {
		capRoom = r.Capacity.MemoryBytes - baseReservation
	}
	if headroom > capRoom {
		headroom = capRoom
	}
	newGrant := allocator.Grant(token, remaining, headroom, urgency)
	decision.CooldownMs = newGrant.CooldownMs
	finalReservation := baseReservation
	if newGrant.GrantedDelta > 0 {
		next, carry := bits.Add64(baseReservation, newGrant.GrantedDelta, 0)
		if carry != 0 || next > r.Capacity.MemoryBytes {
			// Allocator is bounded by capRoom above. Keep this check local so a
			// future allocator change cannot corrupt node aggregates.
			return GrantResult{}, true, fmt.Errorf("allocator grant overflows reservation capacity")
		}
		totalGranted, carry := bits.Add64(decision.GrantedDelta, newGrant.GrantedDelta, 0)
		if carry != 0 {
			return GrantResult{}, true, fmt.Errorf("granted delta overflow")
		}
		finalReservation = next
		decision.GrantedDelta = totalGranted
	}
	if err := s.replaceReservationMemoryLocked(r, finalReservation); err != nil {
		return GrantResult{}, true, err
	}
	return GrantResult{Reservation: cloneReservation(r), Decision: decision, Zone: zone}, true, nil
}

func (s *State) Heartbeat(token string, hostMemoryCurrent uint64, now time.Time) (Reservation, bool) {
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
	if hostMemoryCurrent > 0 {
		r.LastHostMemoryCurrent, r.LastReportAt = hostMemoryCurrent, now
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

func (s *State) Release(token string) (Reservation, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.byTokenLocked(token)
	if r == nil {
		return Reservation{}, false, nil
	}
	out := cloneReservation(r)
	if _, err := s.deleteLocked(r.SandboxID); err != nil {
		return Reservation{}, true, err
	}
	return out, true, nil
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

// DropSandboxConnection clears a queued Admit connection before the client has
// had an opportunity to send its new token. SID lookup and the connection
// identity check keep this transition O(1) and prevent an old queue goroutine
// from detaching a replacement session.
func (s *State) DropSandboxConnection(sid string, conn net.Conn) (Reservation, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.bySID[sid]
	if r == nil || r.Conn != conn {
		return Reservation{}, false
	}
	r.Conn = nil
	return cloneReservation(r), true
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

func (s *State) RemoveIfIdentity(sid, identity string) (Reservation, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.bySID[sid]
	if r == nil || r.identity() != identity {
		return Reservation{}, false, nil
	}
	out := cloneReservation(r)
	if _, err := s.deleteLocked(sid); err != nil {
		return Reservation{}, true, err
	}
	return out, true, nil
}

type SandboxResourceSnapshot struct {
	Capacity          Resources
	CPUAllocatable    uint64
	ReservationMemory uint64
	HostMemoryCurrent uint64
	LastReportAt      time.Time
}

func (s *State) SnapshotSandboxResource(sandboxID string) (SandboxResourceSnapshot, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.bySID[sandboxID]
	if r == nil {
		return SandboxResourceSnapshot{}, false
	}
	return SandboxResourceSnapshot{
		Capacity: r.Capacity, CPUAllocatable: r.ConfiguredAllocatable.CPUMilli,
		ReservationMemory: r.ReservationMemory, HostMemoryCurrent: r.LastHostMemoryCurrent,
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
