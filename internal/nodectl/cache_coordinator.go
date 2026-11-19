package nodectl

import (
	"fmt"
	"math"
	"net"
	"sort"
	"sync"
	"time"

	"github.com/kuasar-sandbox/sandboxer/pkg/resource"
)

// Cache reclaim has its own node-wide budget. These are internal scheduling
// bounds, unrelated to the Guest allocator or a pool's optional cache quota.
const (
	cacheReclaimRate        = uint64(64 << 20)
	cacheReclaimBurst       = uint64(16 << 20)
	cacheReclaimBatch       = uint64(4 << 20)
	cacheReclaimMaxInFlight = 4
)

type cacheReclaimPool struct {
	name  string
	state *State
}

type cacheReclaimMember struct {
	runID, sid                 string
	pid                        int
	pool                       *State
	seq, ack                   uint64
	intent                     *resource.CacheReclaimIntent
	intentID                   uint64
	observation                uint64
	zeroObservation            uint64
	task                       *resource.CacheReclaimTask
	taskStart, taskObservation uint64
	lastIssued                 uint64
	lastAttempt                time.Time
	growthPending              bool
}

// CacheCoordinator serializes intent, authority and completion decisions for
// every resource pool on one node. The lock order is coordinator -> pool(s).
// Pool locks are acquired in name order only for short in-memory decisions;
// no network, token wait or physical reclaim runs under these locks.
// Ordinary accounting never calls back into the coordinator with a pool lock.
type CacheCoordinator struct {
	mu      sync.Mutex
	state   *State
	pools   []cacheReclaimPool
	members map[string]*cacheReclaimMember
	clock   func() time.Time
	tokens  uint64
	refill  time.Time
	order   uint64
}

func NewCacheCoordinator(state *State) *CacheCoordinator {
	c := &CacheCoordinator{state: state, members: make(map[string]*cacheReclaimMember), clock: time.Now, tokens: cacheReclaimBurst}
	for name, p := range state.PoolStates() {
		c.pools = append(c.pools, cacheReclaimPool{name, p})
	}
	sort.Slice(c.pools, func(i, j int) bool { return c.pools[i].name < c.pools[j].name })
	return c
}

func (c *CacheCoordinator) lockPools() {
	for _, p := range c.pools {
		p.state.mu.Lock()
	}
}

func (c *CacheCoordinator) unlockPools() {
	for i := len(c.pools) - 1; i >= 0; i-- {
		c.pools[i].state.mu.Unlock()
	}
}

func (c *CacheCoordinator) byTokenLocked(token string) (*State, *Reservation) {
	for _, p := range c.pools {
		if r := p.state.byTokenLocked(token); r != nil {
			return p.state, r
		}
	}
	return nil, nil
}

func validateCacheReclaimState(features []string, in *resource.CacheReclaimState, maximum uint64) error {
	runID, err := resource.CacheReclaimRunFromFeatures(features)
	if err != nil {
		return err
	}
	if in == nil {
		return nil
	} // A legacy-mode run does not switch on reconnect.
	if runID == "" || in.RunID != runID {
		return fmt.Errorf("cache reclaim run differs from immutable lease")
	}
	if in.Current > maximum || in.Current%4096 != 0 {
		return fmt.Errorf("invalid confirmed physical cache capacity")
	}
	if in.Task != nil && in.Done != nil {
		return fmt.Errorf("cache reclaim task and completion overlap")
	}
	checkIntent := func(i *resource.CacheReclaimIntent) error {
		if i != nil && (i.ID == 0 || i.Observation == 0 || i.Bytes == 0 || i.Bytes > maximum || i.Bytes%4096 != 0) {
			return fmt.Errorf("invalid finite cache reclaim intent")
		}
		return nil
	}
	if err := checkIntent(in.Intent); err != nil {
		return err
	}
	checkTask := func(t *resource.CacheReclaimTask) error {
		if t.RunID != runID || t.Seq == 0 || t.Seq > in.LastSeq || t.MaxBytes == 0 || t.MaxBytes > cacheReclaimBatch || t.MaxBytes > maximum || t.MaxBytes%4096 != 0 || t.Target == 0 || t.Target%4096 != 0 || t.Target > maximum ||
			(t.Source != resource.CacheReclaimAutonomous && t.Source != resource.CacheReclaimYield) ||
			(t.Source == resource.CacheReclaimAutonomous && t.IntentID == 0) ||
			(t.Source == resource.CacheReclaimYield && t.IntentID != 0) {
			return fmt.Errorf("invalid finite cache reclaim task")
		}
		if t.MaxBytes > maximum-t.Target {
			return fmt.Errorf("cache reclaim task exceeds maximum")
		}
		return nil
	}
	if in.Task != nil {
		if err := checkTask(in.Task); err != nil {
			return err
		}
	}
	if in.Done != nil {
		if err := checkTask(&in.Done.Task); err != nil {
			return err
		}
		if in.Done.Current > maximum || in.Done.Current%4096 != 0 {
			return fmt.Errorf("invalid cache reclaim completion baseline")
		}
		if err := checkIntent(in.Done.Intent); err != nil {
			return err
		}
	}
	return nil
}

func cloneCacheMember(m *cacheReclaimMember) *cacheReclaimMember {
	n := *m
	n.intent = resource.CloneCacheReclaimIntent(m.intent)
	if m.task != nil {
		t := *m.task
		n.task = &t
	}
	return &n
}

func (m *cacheReclaimMember) updateIntent(i *resource.CacheReclaimIntent) error {
	if i == nil {
		m.intent = nil
		return nil
	}
	if i.ID < m.intentID {
		return nil
	}
	if i.ID == m.intentID {
		if m.intent == nil {
			return nil
		} // Withdrawn/consumed observations stay closed.
		if i.Observation != m.intent.Observation || i.Probe != m.intent.Probe {
			return fmt.Errorf("cache intent identity changed")
		}
		m.intent.Bytes = min(m.intent.Bytes, i.Bytes)
		return nil
	}
	m.intentID = i.ID
	// A new ID alone is not new supply. A probe can revalidate a fruitless
	// observation, but is never counted as autonomous supply before issue.
	if !i.Probe && (i.Observation <= m.observation || i.Observation <= m.zeroObservation) {
		m.intent = nil
		return nil
	}
	m.observation = max(m.observation, i.Observation)
	m.intent = resource.CloneCacheReclaimIntent(i)
	return nil
}

// receive applies only to a copied member. A returned baseline is committed by
// the caller before publishing the copy. Duplicate done messages are checked
// first and cannot reapply their old baseline or old follow-on intent.
func (m *cacheReclaimMember) receive(in *resource.CacheReclaimState, current uint64, recover bool, now time.Time) (*uint64, error) {
	if in.RunID != m.runID {
		return nil, fmt.Errorf("cache reclaim run changed")
	}
	if !recover && in.LastSeq > m.seq {
		return nil, fmt.Errorf("cache reclaim sequence was not issued")
	}
	if in.Done != nil && in.Done.Task.Seq <= m.ack {
		return nil, nil
	}
	if recover {
		m.seq = max(m.seq, in.LastSeq)
		if m.task == nil && in.Task != nil {
			t := *in.Task
			m.task, m.taskStart = &t, t.Target+t.MaxBytes
			m.taskObservation = m.observation
		}
		if m.task == nil && in.Done != nil {
			t := in.Done.Task
			m.task, m.taskStart = &t, t.Target+t.MaxBytes
			m.taskObservation = m.observation
		}
	}
	if in.Task != nil && (m.task == nil || *in.Task != *m.task) {
		return nil, fmt.Errorf("cache reclaim task differs from issued authority")
	}
	if in.Done == nil {
		if m.task != nil && in.Task == nil && in.LastSeq >= m.task.Seq {
			return nil, fmt.Errorf("cache reclaim completion missing for accepted task")
		}
		return nil, m.updateIntent(in.Intent)
	}
	d := in.Done
	if m.task == nil || d.Task != *m.task {
		return nil, fmt.Errorf("cache reclaim completion differs from issued authority")
	}
	if d.Current > m.taskStart || m.taskStart-d.Current > m.task.MaxBytes {
		return nil, fmt.Errorf("cache reclaim completion exceeds finite authority")
	}
	if !recover && d.Current > current {
		return nil, fmt.Errorf("cache reclaim completion raises reservation")
	}
	returned := m.taskStart - d.Current
	if returned == 0 && m.task.Source == resource.CacheReclaimAutonomous {
		m.zeroObservation = max(m.zeroObservation, m.taskObservation)
		m.intent = nil
	}
	m.ack = m.task.Seq
	m.task = nil
	m.lastAttempt = now
	if err := m.updateIntent(d.Intent); err != nil {
		return nil, err
	}
	// StateSync's current baseline is authoritative, not historical Done.Current.
	// The live update path cannot have grown while completion is unacknowledged.
	if recover {
		return nil, nil
	}
	baseline := d.Current
	return &baseline, nil
}

func (c *CacheCoordinator) Admit(spec AdmitSpec, in *resource.CacheReclaimState) (Reservation, net.Conn, error) {
	if err := validateCacheReclaimState(spec.ClientFeatures, in, spec.CacheMax); err != nil {
		return Reservation{}, nil, err
	}
	if in != nil && (in.LastSeq != 0 || in.Task != nil || in.Done != nil || in.Intent != nil) {
		return Reservation{}, nil, fmt.Errorf("initial cache reclaim state is not empty")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	spec.CacheCoordinated = in != nil
	r, old, err := c.state.Admit(spec)
	if err != nil {
		return r, old, err
	}
	if in != nil {
		m := c.members[r.SandboxID]
		if m == nil || m.runID != in.RunID {
			c.members[r.SandboxID] = &cacheReclaimMember{runID: in.RunID, sid: r.SandboxID, pid: r.PeerPID, pool: c.state.forSID(r.SandboxID), growthPending: in.Current != r.CacheReservation}
		}
	} else {
		// State.Admit has already validated a new/replayed lifecycle. A new
		// legacy run must not inherit a prior run's authority after PID reuse.
		delete(c.members, r.SandboxID)
	}
	return r, old, nil
}

func (c *CacheCoordinator) Sync(spec SyncSpec, in *resource.CacheReclaimState) (Reservation, []net.Conn, resource.CacheReclaimResult, error) {
	if err := validateCacheReclaimState(spec.ClientFeatures, in, spec.CacheMax); err != nil {
		return Reservation{}, nil, resource.CacheReclaimResult{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	m := c.members[spec.SandboxID]
	if m != nil && in == nil {
		return Reservation{}, nil, resource.CacheReclaimResult{}, fmt.Errorf("coordinated cache run cannot discard recovery state")
	}
	var next *cacheReclaimMember
	if in != nil {
		if in.Current > spec.CacheReservation {
			return Reservation{}, nil, resource.CacheReclaimResult{}, fmt.Errorf("confirmed cache capacity exceeds StateSync reservation")
		}
		if m != nil && (m.runID != in.RunID || m.pid != spec.PeerPID) {
			return Reservation{}, nil, resource.CacheReclaimResult{}, fmt.Errorf("cache reclaim live identity changed")
		}
		if m == nil {
			next = &cacheReclaimMember{runID: in.RunID, sid: spec.SandboxID, pid: spec.PeerPID, pool: c.state.forSID(spec.SandboxID)}
		} else {
			next = cloneCacheMember(m)
		}
		if _, err := next.receive(in, spec.CacheReservation, m == nil, c.clock()); err != nil {
			return Reservation{}, nil, resource.CacheReclaimResult{}, err
		}
	}
	spec.CacheCoordinated = in != nil
	r, old, err := c.state.Sync(spec)
	if err != nil {
		return r, old, resource.CacheReclaimResult{}, err
	}
	if next != nil {
		next.pool = c.state.forSID(r.SandboxID)
		next.growthPending = in.Current != r.CacheReservation
		c.members[r.SandboxID] = next
	}
	c.lockPools()
	defer c.unlockPools()
	c.scheduleLocked(c.clock())
	return r, old, c.replyLocked(r.SandboxID, r.CacheReservation), nil
}

func (c *CacheCoordinator) Update(token string, in *resource.CacheReclaimState) (resource.CacheReclaimResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lockPools()
	defer c.unlockPools()
	p, r := c.byTokenLocked(token)
	if r == nil || r.Provisional || !r.CacheCoordinated {
		return resource.CacheReclaimResult{}, fmt.Errorf("no coordinated cache reservation")
	}
	if in == nil {
		return resource.CacheReclaimResult{}, fmt.Errorf("cache reclaim update is missing")
	}
	if err := validateCacheReclaimState(r.ClientFeatures, in, r.CacheMax); err != nil {
		return resource.CacheReclaimResult{}, err
	}
	m := c.members[r.SandboxID]
	if m == nil {
		return resource.CacheReclaimResult{}, fmt.Errorf("cache reclaim StateSync required")
	}
	next := cloneCacheMember(m)
	if in.Done == nil && in.Current != r.CacheReservation {
		return resource.CacheReclaimResult{}, fmt.Errorf("confirmed cache capacity requires budget reconciliation")
	}
	baseline, err := next.receive(in, r.CacheReservation, false, c.clock())
	if err != nil {
		return resource.CacheReclaimResult{}, err
	}
	if baseline != nil {
		if in.Current != *baseline {
			return resource.CacheReclaimResult{}, fmt.Errorf("cache completion and confirmed capacity disagree")
		}
		if err := p.changeCacheLocked(r, *baseline); err != nil {
			return resource.CacheReclaimResult{}, err
		}
	}
	if in.Done == nil || baseline != nil {
		next.growthPending = false
	}
	c.members[r.SandboxID] = next
	c.scheduleLocked(c.clock())
	return c.replyLocked(r.SandboxID, r.CacheReservation), nil
}

// Poll does not alter a reported intent. It can only replay or issue bounded
// authority; pure reporting never clears an independent cache growth demand.
func (c *CacheCoordinator) Poll(token string) resource.CacheReclaimResult {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lockPools()
	defer c.unlockPools()
	_, r := c.byTokenLocked(token)
	if r == nil {
		return resource.CacheReclaimResult{}
	}
	c.scheduleLocked(c.clock())
	return c.replyLocked(r.SandboxID, r.CacheReservation)
}

func (c *CacheCoordinator) Grant(token string, current, requested, maximum, done uint64, allocator *Allocator) (CacheGrantResult, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lockPools()
	_, r := c.byTokenLocked(token)
	if r != nil && r.CacheCoordinated {
		if m := c.members[r.SandboxID]; m == nil || m.task != nil {
			out := CacheGrantResult{Reservation: r.CacheReservation, CooldownMs: 200}
			c.unlockPools()
			return out, true, nil
		}
	}
	c.unlockPools()
	result, found, err := c.state.ReconcileCacheBudgetWithAck(token, current, requested, maximum, done, allocator)
	if err != nil || !found {
		return result, found, err
	}
	c.lockPools()
	if _, r := c.byTokenLocked(token); r != nil {
		if m := c.members[r.SandboxID]; m != nil && result.GrantedDelta > 0 {
			m.growthPending = true
		}
	}
	c.scheduleLocked(c.clock())
	c.unlockPools()
	return result, found, nil
}

func (c *CacheCoordinator) replyLocked(sid string, reservation uint64) resource.CacheReclaimResult {
	out := resource.CacheReclaimResult{Reservation: reservation}
	if m := c.members[sid]; m != nil {
		out.Ack = m.ack
		if m.task != nil {
			t := *m.task
			out.Task = &t
		}
	}
	return out
}

// shortage is based on still-valid demand and confirmed independent headroom.
// Neither Guest shortage, unlimited quota, nor the existence of a queue counts.
func cacheShortageLocked(p *State, now time.Time) (uint64, map[string]bool) {
	waiters := make(map[string]bool)
	if p.CachePoolCapacity == 0 || p.cacheRecoveryUnknownCount > 0 {
		return 0, waiters
	}
	headroom := p.cacheHeadroomLocked()
	var demand uint64
	for token, d := range p.cacheDemandSince {
		r := p.byTokenLocked(token)
		if r == nil || r.Provisional || now.Sub(d.last) > 2*time.Second {
			continue
		}
		amount := min(d.amount, r.CacheMax-r.CacheReservation)
		if amount > headroom {
			demand = max(demand, amount-headroom)
			waiters[r.SandboxID] = true
		}
	}
	for key, d := range p.admissionCacheDemand {
		if d.amount > headroom {
			demand = max(demand, d.amount-headroom)
			if len(key) > len("admit:") {
				waiters[key[len("admit:"):]] = true
			}
		}
	}
	return demand, waiters
}

func (c *CacheCoordinator) refreshMembersLocked() (int, int) {
	live := make(map[string]*Reservation)
	unknown, legacy := 0, 0
	for _, p := range c.pools {
		for sid, r := range p.state.bySID {
			live[sid] = r
			if r.CacheRecoveryUnknown || r.Provisional && (r.CacheMax > 0 || r.RecoverySource == RecoveryUnknownLease || r.RecoverySource == RecoveryUnknownManaged) {
				unknown++
				continue
			}
			if r.CacheCoordinated {
				m := c.members[sid]
				if m == nil || m.pid != r.PeerPID {
					unknown++
				}
			} else if r.CacheMax > 0 {
				legacy++
			}
		}
	}
	for sid, m := range c.members {
		r := live[sid]
		// The existing lifecycle has confirmed removal/replacement. A mere
		// disconnect, stale heartbeat or timeout does not enter this branch.
		if r == nil || r.PeerPID != m.pid {
			delete(c.members, sid)
			continue
		}
		runID, err := resource.CacheReclaimRunFromFeatures(r.ClientFeatures)
		if !r.Provisional && (!r.CacheCoordinated || err != nil || runID != m.runID) {
			delete(c.members, sid)
		}
	}
	return unknown, legacy
}

func (c *CacheCoordinator) refillLocked(now time.Time) {
	if c.refill.IsZero() {
		c.refill = now
		return
	}
	if !now.After(c.refill) {
		return
	}
	elapsed := now.Sub(c.refill)
	if elapsed >= time.Second {
		c.tokens = cacheReclaimBurst
	} else {
		c.tokens = min(cacheReclaimBurst, c.tokens+uint64(elapsed.Nanoseconds())*cacheReclaimRate/uint64(time.Second))
	}
	c.refill = now
}

func (c *CacheCoordinator) scheduleLocked(now time.Time) {
	unknown, _ := c.refreshMembersLocked()
	c.refillLocked(now)
	if unknown != 0 {
		return
	}
	shortages := make(map[*State]uint64)
	realShortages := make(map[*State]uint64)
	waiters := make(map[*State]map[string]bool)
	for _, p := range c.pools {
		shortages[p.state], waiters[p.state] = cacheShortageLocked(p.state, now)
		realShortages[p.state] = shortages[p.state]
	}
	var autos, probes, donors []*cacheReclaimMember
	var inBytes uint64
	inCount, inAuto, inYield := 0, false, false
	for _, m := range c.members {
		r := m.pool.bySID[m.sid]
		if r == nil || r.Provisional {
			continue
		}
		if m.task != nil {
			inCount++
			inBytes += m.task.MaxBytes
			inAuto = inAuto || m.task.Source == resource.CacheReclaimAutonomous
			inYield = inYield || m.task.Source == resource.CacheReclaimYield
			if m.task.Source == resource.CacheReclaimYield {
				shortages[m.pool] -= min(shortages[m.pool], m.task.MaxBytes)
			}
		}
		// Retain the intent until physical growth is confirmed, but do not
		// treat it as schedulable supply or a donor. Existing authority above
		// still participates in the global source and in-flight barriers.
		if m.growthPending {
			continue
		}
		available := uint64(0)
		if r.CacheReservation > cacheBudgetUnit {
			available = r.CacheReservation - cacheBudgetUnit
		}
		held := m.pool.CacheLowWatermark != nil && m.pool.reservedCache <= *m.pool.CacheLowWatermark && realShortages[m.pool] == 0
		if m.intent != nil && m.intent.Bytes > 0 && available > 0 && !held && r.Conn != nil {
			if m.intent.Probe {
				probes = append(probes, m)
			} else {
				autos = append(autos, m)
			}
		}
		if shortages[m.pool] > 0 && m.task == nil && r.Conn != nil && available > 0 && !waiters[m.pool][m.sid] && now.Sub(m.lastAttempt) >= cacheReclaimRetryDelay {
			donors = append(donors, m)
		}
	}
	// Recovery never discards a possible execution right, even if its source
	// contradicts another recovered record. Drain existing finite tasks first.
	if inAuto && inYield {
		return
	}
	if inCount >= cacheReclaimMaxInFlight || inBytes >= cacheReclaimBurst {
		return
	}
	sort.Slice(autos, func(i, j int) bool {
		if autos[i].lastIssued == autos[j].lastIssued {
			return autos[i].sid < autos[j].sid
		}
		return autos[i].lastIssued < autos[j].lastIssued
	})
	sort.Slice(probes, func(i, j int) bool {
		if probes[i].lastIssued == probes[j].lastIssued {
			return probes[i].sid < probes[j].sid
		}
		return probes[i].lastIssued < probes[j].lastIssued
	})
	issue := func(m *cacheReclaimMember, source string, bytes uint64) bool {
		r := m.pool.bySID[m.sid]
		if m.task != nil || m.growthPending || r.Conn == nil || m.seq == math.MaxUint64 || inCount >= cacheReclaimMaxInFlight {
			return false
		}
		bytes = min(bytes, cacheReclaimBatch, c.tokens, cacheReclaimBurst-inBytes, r.CacheReservation-cacheBudgetUnit)
		bytes -= bytes % 4096
		if bytes == 0 {
			return false
		}
		m.seq++
		m.task = &resource.CacheReclaimTask{RunID: m.runID, Seq: m.seq, Source: source, MaxBytes: bytes, Target: r.CacheReservation - bytes}
		m.taskStart, m.taskObservation = r.CacheReservation, 0
		if source == resource.CacheReclaimAutonomous {
			m.task.IntentID, m.taskObservation = m.intent.ID, m.intent.Observation
			m.intent.Bytes -= bytes
		}
		c.order++
		m.lastIssued = c.order
		c.tokens -= bytes
		inCount++
		inBytes += bytes
		return true
	}
	if len(autos) > 0 {
		if inYield {
			return
		}
		for _, m := range autos {
			issue(m, resource.CacheReclaimAutonomous, m.intent.Bytes)
		}
		return // Normal rate waiting remains primary supply.
	}
	if inAuto {
		return
	}
	sort.Slice(donors, func(i, j int) bool {
		a, b := donors[i].pool.bySID[donors[i].sid], donors[j].pool.bySID[donors[j].sid]
		if a.CacheReservation == b.CacheReservation {
			return donors[i].sid < donors[j].sid
		}
		return a.CacheReservation > b.CacheReservation
	})
	issuedYield := false
	for _, m := range donors {
		if shortage := shortages[m.pool]; shortage > 0 && issue(m, resource.CacheReclaimYield, shortage) {
			shortages[m.pool] -= min(shortage, m.task.MaxBytes)
			issuedYield = true
		}
	}
	if inYield || issuedYield {
		return
	}
	for _, m := range probes {
		issue(m, resource.CacheReclaimAutonomous, m.intent.Bytes)
	}
}

func (c *CacheCoordinator) Status() resource.CacheReclaimStatus {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lockPools()
	defer c.unlockPools()
	unknown, legacy := c.refreshMembersLocked()
	out := resource.CacheReclaimStatus{RateBytesPerSecond: cacheReclaimRate, BurstBytes: cacheReclaimBurst, RecoveryPending: unknown, LegacyParticipants: legacy}
	now := c.clock()
	for _, m := range c.members {
		if m.task != nil {
			out.InFlightTasks++
			out.InFlightBytes += m.task.MaxBytes
		}
		if m.intent != nil && m.intent.Bytes > 0 && !m.intent.Probe {
			out.AutonomousIntents++
			shortage, _ := cacheShortageLocked(m.pool, now)
			if m.pool.CacheLowWatermark != nil && m.pool.reservedCache <= *m.pool.CacheLowWatermark && shortage == 0 {
				out.HeldIntents++
			}
		}
	}
	return out
}
