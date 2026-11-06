package nodectl

import (
	"fmt"
	"math"
	"math/rand/v2"
	"sort"
)

// Placement is an exact execution's scheduling charge. Memory reservations
// remain owned exclusively by Admit/Sync/grow/shrink/Release.
type Placement struct {
	SandboxID string    `json:"sandbox_id"`
	Identity  string    `json:"identity"`
	Pool      string    `json:"resource_pool"`
	Capacity  Resources `json:"capacity"`
}

type PoolSnapshot struct {
	Name              string    `json:"name"`
	Capacity          Resources `json:"capacity"`
	Scheduled         Resources `json:"scheduled"`
	Available         Resources `json:"available"` // zero limit means unlimited; see Capacity
	StartupPool       uint64    `json:"startup_pool_memory_bytes"`
	Utilization       float64   `json:"utilization"`
	CPUUtilization    float64   `json:"cpu_utilization"`
	MemoryUtilization float64   `json:"memory_utilization"`
	ResourceSnapshot
	Pressure PressureSnapshot `json:"pressure"`
}

func NewPoolState(resolved *Resolved) *State {
	root := NewState(resolved.PhysicalMemory, resolved.PhysicalCPU, resolved.HostReserved.MemoryBytes, resolved.HostReserved.CPUMilli, resolved.Watermarks)
	if len(resolved.Pools) == 0 {
		return root
	}
	root.pools, root.bindings = map[string]*State{}, map[string]string{}
	root.initPressureLocked()
	for _, cfg := range resolved.Pools {
		p := NewState(cfg.Allocatable.MemoryBytes, 0, 0, 0, cfg.Watermarks)
		p.AllocatablePool = cfg.Allocatable
		p.PoolName, p.CapacityLimit, p.CPUWatermarks = cfg.Name, cfg.Capacity, cfg.CPUWatermarks
		p.placements = map[string]Placement{}
		p.initPressureLocked()
		p.pressureWake = root.pressureWake
		policy := resolved.Allocator
		policy.MemoryGrantPerSecBytes = cfg.MemoryGrantPerSecBytes
		p.poolAllocator = NewAllocator(policy)
		root.pools[cfg.Name] = p
	}
	if len(root.pools) == 1 {
		for _, p := range root.pools {
			p.NodeBudget, p.HostReserved, p.OperationalMargin = root.NodeBudget, root.HostReserved, root.OperationalMargin
			return p
		}
	}
	root.AllocatablePool = Resources{}
	for _, p := range root.pools {
		root.AllocatablePool = addResourcesSaturating(root.AllocatablePool, p.AllocatablePool)
	}
	root.NodeBudget, root.HostReserved, root.OperationalMargin = root.AllocatablePool, Resources{}, Resources{}
	return root
}

func (s *State) PoolStates() map[string]*State {
	out := map[string]*State{}
	if s.pools == nil {
		name := s.PoolName
		if name == "" {
			name = "default"
		}
		out[name] = s
		return out
	}
	for name, p := range s.pools {
		out[name] = p
	}
	return out
}

func (s *State) BindResourcePool(sid, name string) error {
	if s.pools == nil {
		expected := s.PoolName
		if expected == "" {
			expected = "default"
		}
		if name != "" && name != expected {
			return fmt.Errorf("resource pool mismatch")
		}
		return nil
	}
	if s.pools[name] == nil {
		return fmt.Errorf("unknown resource pool %q", name)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if old := s.bindings[sid]; old != "" && old != name {
		p := s.pools[old]
		p.mu.Lock()
		busy := p.bySID[sid] != nil
		_, placed := p.placements[sid]
		p.mu.Unlock()
		if busy || placed {
			return fmt.Errorf("running resource pool is immutable")
		}
	}
	s.bindings[sid] = name
	return nil
}

func (s *State) forSID(sid string) *State {
	if s.pools == nil {
		return s
	}
	s.mu.Lock()
	p := s.pools[s.bindings[sid]]
	s.mu.Unlock()
	if p != nil {
		return p
	}
	for _, p := range s.pools {
		p.mu.Lock()
		found := p.bySID[sid] != nil
		p.mu.Unlock()
		if found {
			return p
		}
	}
	return s // unknown consumers must fail closed, never guess a resource pool
}

func (s *State) forToken(token string) *State {
	for _, p := range s.pools {
		p.mu.Lock()
		found := p.tokenToSID[token] != ""
		p.mu.Unlock()
		if found {
			return p
		}
	}
	return s
}

func maxZone(a, b Zone) Zone {
	if zoneRank(b) > zoneRank(a) {
		return b
	}
	return a
}
func nodeZone(a, b Zone) Zone {
	if a == "" {
		return b
	}
	if a == ZoneCritical || b == ZoneCritical {
		return ZoneCritical
	}
	if zoneRank(b) < zoneRank(a) {
		return b
	}
	return a
}
func ratio(used, limit uint64) float64 {
	if limit == 0 {
		return 0
	}
	return float64(used) / float64(limit)
}
func addResourcesSaturating(a, b Resources) Resources {
	add := func(a, b uint64) uint64 {
		if b > math.MaxUint64-a {
			return math.MaxUint64
		}
		return a + b
	}
	return Resources{MemoryBytes: add(a.MemoryBytes, b.MemoryBytes), CPUMilli: add(a.CPUMilli, b.CPUMilli)}
}

func (s *State) scheduledLocked() Resources {
	var used Resources
	for sid, r := range s.bySID {
		if _, claimed := s.placements[sid]; !claimed {
			used = addResourcesSaturating(used, r.Capacity)
		}
	}
	for _, claim := range s.placements {
		used = addResourcesSaturating(used, claim.Capacity)
	}
	return used
}

func fits(used, request, limit Resources) bool {
	for _, d := range [][3]uint64{{used.MemoryBytes, request.MemoryBytes, limit.MemoryBytes}, {used.CPUMilli, request.CPUMilli, limit.CPUMilli}} {
		if d[1] > math.MaxUint64-d[0] || d[2] != 0 && (d[0] > d[2] || d[1] > d[2]-d[0]) {
			return false
		}
	}
	return true
}

func (s *State) PoolSnapshots() []PoolSnapshot {
	var out []PoolSnapshot
	for name, p := range s.PoolStates() {
		p.mu.Lock()
		r := p.resourceSnapshotLocked()
		cpu, memory := ratio(p.allocatedCPU, p.AllocatablePool.CPUMilli), ratio(p.reservedMemory, p.AllocatablePool.MemoryBytes)
		used := p.scheduledLocked()
		v := PoolSnapshot{Name: name, Capacity: p.CapacityLimit, Scheduled: used, Available: p.CapacityLimit.Sub(used), StartupPool: p.startupPoolBytesLocked(), Utilization: max(cpu, memory), CPUUtilization: cpu, MemoryUtilization: memory, ResourceSnapshot: r}
		p.mu.Unlock()
		v.Pressure = p.PressureSnapshot()
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// ReservePlacement serializes capacity acquisition across concurrent launches.
// supply is a read-only runner supply check; final runner acquisition can still
// fail and must roll back this exact identity.
func (s *State) ReservePlacement(sid, identity string, capacity Resources, op Operation, exclude string, supply func(string) bool) (Placement, error) {
	if sid == "" || identity == "" {
		return Placement{}, fmt.Errorf("missing placement identity")
	}
	if s.pools != nil {
		s.mu.Lock()
		defer s.mu.Unlock()
	}
	type candidate struct {
		p           *State
		name        string
		utilization float64
	}
	var candidates []candidate
	for name, p := range s.PoolStates() {
		p.mu.Lock()
		if old, exists := p.placements[sid]; exists {
			p.mu.Unlock()
			if old.Identity == identity && old.Capacity == capacity {
				return old, nil
			}
			return Placement{}, fmt.Errorf("sandbox %s already owns placement %s", sid, old.Identity)
		}
		if p.bySID[sid] != nil {
			p.mu.Unlock()
			return Placement{}, fmt.Errorf("sandbox %s still owns running resources", sid)
		}
		ok := name != exclude && OperationAllowed(p.memoryZoneLocked(), op) && fits(p.scheduledLocked(), capacity, p.CapacityLimit)
		use := max(ratio(p.reservedMemory, p.AllocatablePool.MemoryBytes), ratio(p.allocatedCPU, p.AllocatablePool.CPUMilli))
		p.mu.Unlock()
		if ok && (supply == nil || supply(name)) {
			candidates = append(candidates, candidate{p, name, use})
		}
	}
	rand.Shuffle(len(candidates), func(i, j int) { candidates[i], candidates[j] = candidates[j], candidates[i] })
	sort.SliceStable(candidates, func(i, j int) bool { return candidates[i].utilization < candidates[j].utilization })
	for _, c := range candidates {
		c.p.mu.Lock()
		if old, exists := c.p.placements[sid]; exists {
			c.p.mu.Unlock()
			if old.Identity == identity && old.Capacity == capacity {
				return old, nil
			}
			return Placement{}, fmt.Errorf("sandbox %s already owns placement", sid)
		}
		if c.p.bySID[sid] != nil {
			c.p.mu.Unlock()
			return Placement{}, fmt.Errorf("sandbox %s still owns running resources", sid)
		}
		if fits(c.p.scheduledLocked(), capacity, c.p.CapacityLimit) && OperationAllowed(c.p.memoryZoneLocked(), op) {
			if c.p.placements == nil {
				c.p.placements = map[string]Placement{}
			}
			claim := Placement{SandboxID: sid, Identity: identity, Pool: c.name, Capacity: capacity}
			c.p.placements[sid] = claim
			c.p.mu.Unlock()
			if s.pools != nil {
				s.bindings[sid] = c.name
			}
			return claim, nil
		}
		c.p.mu.Unlock()
	}
	return Placement{}, fmt.Errorf("no resource pool has scheduling capacity, operation permission and runner supply")
}

func (s *State) BindPlacement(sid, identity, runID string) error {
	p := s.forSID(sid)
	p.mu.Lock()
	defer p.mu.Unlock()
	c, ok := p.placements[sid]
	if !ok || c.Identity != identity {
		return fmt.Errorf("placement identity changed")
	}
	c.Identity = runID
	p.placements[sid] = c
	return nil
}

// BeginPhase replaces a builder's prelaunch/finished-phase charge with the
// next concrete VM's Capacity in the same pool. There must be no live previous
// reservation. Builder business quotas remain entirely outside this ledger.
func (s *State) BeginPhase(pool, previousSID, sid, runID string, capacity Resources) error {
	if sid == "" || runID == "" {
		return fmt.Errorf("missing phase identity")
	}
	p := s.PoolStates()[pool]
	if p == nil {
		return fmt.Errorf("unknown resource pool %s", pool)
	}
	if s.pools != nil {
		s.mu.Lock()
		defer s.mu.Unlock()
		if oldPool := s.bindings[sid]; oldPool != "" && oldPool != pool {
			return fmt.Errorf("phase is bound to another pool")
		}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if c, ok := p.placements[sid]; ok {
		if c.Identity == runID && c.Capacity == capacity {
			return nil
		}
		return fmt.Errorf("phase already has another placement")
	}
	if p.bySID[previousSID] != nil || p.bySID[sid] != nil {
		return fmt.Errorf("previous phase resources have not been released")
	}
	used := p.scheduledLocked()
	old, exists := p.placements[previousSID]
	if exists {
		if old.Identity != runID {
			return fmt.Errorf("previous phase identity changed")
		}
		used = used.Sub(old.Capacity)
	}
	if !OperationAllowed(p.memoryZoneLocked(), OperationCreate) || !fits(used, capacity, p.CapacityLimit) {
		return fmt.Errorf("phase does not fit resource pool %s", pool)
	}
	if p.placements == nil {
		p.placements = map[string]Placement{}
	}
	delete(p.placements, previousSID)
	p.placements[sid] = Placement{SandboxID: sid, Identity: runID, Pool: pool, Capacity: capacity}
	if s.pools != nil {
		s.bindings[sid] = pool
		if previousSID != sid {
			delete(s.bindings, previousSID)
		}
	}
	return nil
}

// RetireExecution is only called by the lifecycle owner after systemd confirms
// this run has exited. A stale run cannot remove its successor's reservation.
func (s *State) RetireExecution(sid, runID string) error {
	p := s.forSID(sid)
	if p != s {
		err := p.RetireExecution(sid, runID)
		s.pruneBinding(sid, p)
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	c, ok := p.placements[sid]
	if !ok || c.Identity != runID {
		return nil
	}
	if p.bySID[sid] != nil {
		if _, err := p.deleteLocked(sid); err != nil {
			return err
		}
	}
	delete(p.placements, sid)
	return nil
}

func (s *State) SandboxPool(sid string) string {
	p := s.forSID(sid)
	if p.PoolName == "" && p.pools == nil {
		return "default"
	}
	return p.PoolName
}

func (s *State) CanPlace(pool, sid string, capacity Resources, budget uint64, launch LaunchAdmission) bool {
	p := s.PoolStates()[pool]
	if p == nil {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return fits(p.scheduledLocked(), capacity, p.CapacityLimit) && p.admissionBudgetLocked(sid, budget, launch).Status == OutcomeAdmitted
}

func (s *State) IsBackgroundRecovery(sid string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return sid != "" && s.pressure.backgroundRecovery == sid
}

// RestorePlacement retains over-limit existing executions. It is used before
// admission opens and never converts an ambiguous legacy unit into a pool ID.
func (s *State) RestorePlacement(claim Placement) error {
	p := s.PoolStates()[claim.Pool]
	if p == nil {
		return fmt.Errorf("unknown persisted resource pool %q", claim.Pool)
	}
	if s.pools != nil {
		s.mu.Lock()
		defer s.mu.Unlock()
		if old := s.bindings[claim.SandboxID]; old != "" && old != claim.Pool {
			return fmt.Errorf("conflicting persisted resource pools for %s", claim.SandboxID)
		}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if old, ok := p.placements[claim.SandboxID]; ok && old != claim {
		return fmt.Errorf("conflicting placement for %s", claim.SandboxID)
	}
	if p.placements == nil {
		p.placements = map[string]Placement{}
	}
	p.placements[claim.SandboxID] = claim
	if s.pools != nil {
		s.bindings[claim.SandboxID] = claim.Pool
	}
	return nil
}

func (s *State) ReleasePlacement(sid, identity string) bool {
	if s.pools != nil {
		p := s.forSID(sid)
		if p == s {
			return false
		}
		ok := p.ReleasePlacement(sid, identity)
		s.pruneBinding(sid, p)
		return ok
	}
	p := s.forSID(sid)
	p.mu.Lock()
	defer p.mu.Unlock()
	claim, ok := p.placements[sid]
	if !ok || claim.Identity != identity || p.bySID[sid] != nil {
		return false
	}
	delete(p.placements, sid)
	return true
}

func (s *State) pruneBinding(sid string, p *State) {
	if s.pools == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pools[s.bindings[sid]] != p {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	_, placed := p.placements[sid]
	if p.bySID[sid] == nil && !placed {
		delete(s.bindings, sid)
	}
}

func (s *State) aggregateResourceSnapshot() ResourceSnapshot {
	out := ResourceSnapshot{NodeBudget: s.NodeBudget, HostReserved: s.HostReserved, OperationalMargin: s.OperationalMargin}
	for _, p := range s.pools {
		v := p.ResourceSnapshot()
		out.AllocatablePool = addResourcesSaturating(out.AllocatablePool, v.AllocatablePool)
		out.Reserved = addResourcesSaturating(out.Reserved, v.Reserved)
		out.StartupInFlight = addResourcesSaturating(Resources{MemoryBytes: out.StartupInFlight}, Resources{MemoryBytes: v.StartupInFlight}).MemoryBytes
		out.ReservationCount += v.ReservationCount
		out.ProvisionalCount += v.ProvisionalCount
		out.UnknownCount += v.UnknownCount
		out.Zone = nodeZone(out.Zone, v.Zone)
		out.RawZone = nodeZone(out.RawZone, v.RawZone)
	}
	return out
}

func (s *State) aggregatePressureSnapshot() PressureSnapshot {
	out := PressureSnapshot{HeadroomStable: true}
	for _, p := range s.pools {
		v := p.PressureSnapshot()
		out.Zone = nodeZone(out.Zone, v.Zone)
		out.RawZone = nodeZone(out.RawZone, v.RawZone)
		out.Version += v.Version
		out.PoolMemory = addResourcesSaturating(Resources{MemoryBytes: out.PoolMemory}, Resources{MemoryBytes: v.PoolMemory}).MemoryBytes
		out.ReservedMemory = addResourcesSaturating(Resources{MemoryBytes: out.ReservedMemory}, Resources{MemoryBytes: v.ReservedMemory}).MemoryBytes
		out.EmergencyMemory = addResourcesSaturating(Resources{MemoryBytes: out.EmergencyMemory}, Resources{MemoryBytes: v.EmergencyMemory}).MemoryBytes
		out.Protected = addResourcesSaturating(Resources{MemoryBytes: out.Protected}, Resources{MemoryBytes: v.Protected}).MemoryBytes
		out.HeadroomStable = out.HeadroomStable && v.HeadroomStable
		out.HoldRemaining = max(out.HoldRemaining, v.HoldRemaining)
		out.Pending = max(out.Pending, v.Pending)
		out.Capturing = max(out.Capturing, v.Capturing)
		out.Paused = max(out.Paused, v.Paused)
		out.Starting = max(out.Starting, v.Starting)
		out.Cleanup = max(out.Cleanup, v.Cleanup)
		out.PauseEligible = out.PauseEligible || v.PauseEligible
		out.Demands = append(out.Demands, v.Demands...)
		out.OldestWait = max(out.OldestWait, v.OldestWait)
	}
	return out
}
