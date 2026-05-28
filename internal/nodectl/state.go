package nodectl

import (
	"fmt"
	"net"
	"sync"
	"time"
)

// Stage names mirror the per-sandbox state machine in
// docs/sandbox.md §10.1.
const (
	StageAdmitted   = "admitted"
	StageCreating   = "creating"
	StageStartup    = "startup"
	StageRestoring  = "restoring"
	StageSettled    = "settled"
	StageBurst      = "burst"
	StageRecover    = "recover"
	StageReleased   = "released"
)

// Resources is a per-dimension headroom record. CPUMilli is allocatable.cpu * 1000.
type Resources struct {
	MemoryBytes uint64 `json:"memory_bytes"`
	CPUMilli    uint64 `json:"cpu_milli"`
}

// Sub returns r minus other (saturating at zero).
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

// Add returns r plus other.
func (r Resources) Add(other Resources) Resources {
	return Resources{
		MemoryBytes: r.MemoryBytes + other.MemoryBytes,
		CPUMilli:    r.CPUMilli + other.CPUMilli,
	}
}

// Reservation is the per-sandbox state held by the controller.
//
// Conn is nil when the connection is dropped (sandbox-ctl crashed or
// network jittered) — the reservation may still be valid pending
// reattach (§11.3).
type Reservation struct {
	Token             string    `json:"token"`
	SandboxID         string    `json:"sandbox_id"`
	SandboxCtlPID     int       `json:"sandbox_ctl_pid,omitempty"`
	CgroupPath        string    `json:"cgroup_path,omitempty"`
	Capacity          Resources `json:"capacity"`
	Floor             Resources `json:"floor"`
	AllocatableNowMem uint64    `json:"allocatable_now_mem"`
	Stage             string    `json:"stage"`
	StageEnteredAt    time.Time `json:"stage_entered_at"`
	LastHeartbeatAt   time.Time `json:"last_heartbeat_at"`
	OOMCount          uint64    `json:"oom_count"`

	// LastReportedRSS is the cgroup memory.current most recently reported
	// by sandbox-ctl in a Heartbeat / Settled message. Used by the active
	// reclaimer to size the working-set + safety-margin target.
	LastReportedRSS uint64 `json:"last_reported_rss,omitempty"`

	// Conn is the live RPC connection. Not persisted; restored as nil
	// after controller restart (sandbox-ctl reattaches on its own).
	Conn net.Conn `json:"-"`
}

// Watermarks captures the four water-mark fractions of allocatable_pool.
type Watermarks struct {
	OperationalMarginFactor float64 `json:"operational_margin_factor"`
	HighFactor              float64 `json:"high_factor"`
	LowFactor               float64 `json:"low_factor"`
	EmergencyFactor         float64 `json:"emergency_factor"`
}

// State is the in-memory snapshot mirrored to /run/node-ctl/state.json.
type State struct {
	mu sync.Mutex

	NodeBudget        Resources              `json:"node_budget"`
	HostReserved      Resources              `json:"host_reserved"`
	OperationalMargin Resources              `json:"operational_margin"`
	AllocatablePool   Resources              `json:"allocatable_pool"`
	Wm                Watermarks             `json:"watermarks"`
	Reservations      map[string]*Reservation `json:"reservations"`

	Version int `json:"version"`
}

// NewState constructs a State with derived watermarks.
func NewState(physicalMem uint64, physicalCPUMilli uint64,
	hostReservedMem uint64, hostReservedCPUMilli uint64,
	wm Watermarks) *State {
	node := Resources{MemoryBytes: physicalMem, CPUMilli: physicalCPUMilli}
	host := Resources{MemoryBytes: hostReservedMem, CPUMilli: hostReservedCPUMilli}
	budget := node.Sub(host)
	margin := Resources{
		MemoryBytes: uint64(float64(budget.MemoryBytes) * wm.OperationalMarginFactor),
		CPUMilli:    uint64(float64(budget.CPUMilli) * wm.OperationalMarginFactor),
	}
	pool := budget.Sub(margin)
	return &State{
		Version:           1,
		NodeBudget:        node,
		HostReserved:      host,
		OperationalMargin: margin,
		AllocatablePool:   pool,
		Wm:                wm,
		Reservations:      make(map[string]*Reservation),
	}
}

// Lock / Unlock allow callers to hold the state lock across multiple
// operations (admission + reservation insert, etc.).
func (s *State) Lock()   { s.mu.Lock() }
func (s *State) Unlock() { s.mu.Unlock() }

// NodeAllocated computes the current allocated resources by summing
// reservation budgets. Caller must hold the lock.
func (s *State) NodeAllocated() Resources {
	var out Resources
	for _, r := range s.Reservations {
		out.MemoryBytes += r.AllocatableNowMem
		out.CPUMilli += r.Floor.CPUMilli // CPU does not burst; floor is the budget
	}
	return out
}

// Watermark zone of the current allocation. Caller must hold the lock.
type Zone string

const (
	ZoneGreen     Zone = "green"
	ZoneYellow    Zone = "yellow"
	ZoneRed       Zone = "red"
	ZoneCritical  Zone = "critical"
)

// MemoryZone returns the current memory water-mark zone (drives
// admission and burst-grant decisions).
func (s *State) MemoryZone() Zone {
	alloc := s.NodeAllocated()
	pool := s.AllocatablePool.MemoryBytes
	if pool == 0 {
		return ZoneRed
	}
	high := uint64(float64(pool) * s.Wm.HighFactor)
	low := uint64(float64(pool) * s.Wm.LowFactor)
	emerg := uint64(float64(pool) * s.Wm.EmergencyFactor)
	switch {
	case alloc.MemoryBytes >= pool-emerg:
		return ZoneCritical
	case alloc.MemoryBytes >= high:
		return ZoneRed
	case alloc.MemoryBytes >= low:
		return ZoneYellow
	default:
		return ZoneGreen
	}
}

// Insert adds a reservation under its token. Caller must hold lock.
func (s *State) Insert(r *Reservation) error {
	if _, dup := s.Reservations[r.Token]; dup {
		return fmt.Errorf("token %q already exists", r.Token)
	}
	s.Reservations[r.Token] = r
	return nil
}

// Remove deletes a reservation. Caller must hold lock.
func (s *State) Remove(token string) {
	delete(s.Reservations, token)
}

// Lookup returns the reservation, or nil. Caller must hold lock.
func (s *State) Lookup(token string) *Reservation {
	return s.Reservations[token]
}
