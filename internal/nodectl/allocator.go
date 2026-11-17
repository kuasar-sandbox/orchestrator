package nodectl

import (
	"math"
	"sync"
	"time"
)

// AllocatorPolicy bounds runtime memory budget grants.
//
// MemoryGrantPerSecBytes is the replenishment rate. Normal/low grants share
// a bucket of max(rate, MinGrantStep), initially funded with one second's rate.
//
// EmergencyFactor (0..1) of allocatable_pool is reserved for urgency=high
// (oom) requests; normal/low requests cannot consume it.
//
// MinGrantStep / MaxGrantStep are sanity bounds on a single grant.
type AllocatorPolicy struct {
	MemoryGrantPerSecBytes uint64
	MinGrantStep           uint64
	MaxGrantStep           uint64
}

// Allocator processes RequestBudget calls under an in-memory token
// bucket. It does NOT keep its own queue — callers either get an
// immediate grant or a queued response with a cooldown hint, and the
// sandbox-ctl side retries.
type Allocator struct {
	mu         sync.Mutex
	policy     AllocatorPolicy
	tokens     float64
	lastRefill time.Time
	clock      func() time.Time
	// Activated by cache grants only: legacy Guest-only buckets retain
	// their existing capacity, but a shared cache/Guest bucket can retain
	// enough tokens to authorize a complete cache-budget unit.
	cacheUnit uint64

	// Per-sandbox grant history for fairness (last 60s grant total).
	history map[string]*grantHistory
}

type grantHistory struct {
	last60s []grantSample
}

type grantSample struct {
	at    time.Time
	bytes uint64
}

// NewAllocator starts with one second's rate and empty history. A low rate
// accumulates a complete MinGrantStep over time, without extra initial burst.
func NewAllocator(policy AllocatorPolicy) *Allocator {
	return &Allocator{
		policy:     policy,
		tokens:     float64(policy.MemoryGrantPerSecBytes), // start with 1s of headroom
		lastRefill: time.Now(),
		clock:      time.Now,
		history:    make(map[string]*grantHistory),
	}
}

// refillLocked refills the per-second bucket. Caller holds lock.
func (a *Allocator) refillLocked() {
	now := a.clock()
	elapsed := now.Sub(a.lastRefill).Seconds()
	a.tokens += elapsed * float64(a.policy.MemoryGrantPerSecBytes)
	if a.tokens > float64(a.bucketCapacity()) {
		a.tokens = float64(a.bucketCapacity())
	}
	a.lastRefill = now
}

func (a *Allocator) bucketCapacity() uint64 {
	return max(a.policy.MemoryGrantPerSecBytes, a.policy.MinGrantStep, a.cacheUnit)
}

// recordGrantLocked appends a grant to the per-sandbox history and
// trims expired samples. Caller holds lock.
func (a *Allocator) recordGrantLocked(token string, bytes uint64) {
	h, ok := a.history[token]
	if !ok {
		h = &grantHistory{}
		a.history[token] = h
	}
	now := a.clock()
	h.last60s = append(h.last60s, grantSample{at: now, bytes: bytes})
	cutoff := now.Add(-60 * time.Second)
	i := 0
	for i < len(h.last60s) && h.last60s[i].at.Before(cutoff) {
		i++
	}
	h.last60s = h.last60s[i:]
}

// FairnessScore returns the total grant bytes for the sandbox in the
// last 60 seconds. Higher = lower priority (drag to back of queue).
func (a *Allocator) FairnessScore(token string) uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	h := a.history[token]
	if h == nil {
		return 0
	}
	var total uint64
	for _, s := range h.last60s {
		total += s.bytes
	}
	return total
}

// GrantDecision is the outcome of a Grant call.
type GrantDecision struct {
	GrantedDelta uint64
	CooldownMs   int64
}

// Grant computes the newly chargeable delta for a RequestBudget.
// State.ReconcileAndGrant calls it while holding State's private mutex after
// reusing any reservation already charged to the sandbox and computing the
// remaining node headroom. External callers never lock or mutate State
// directly. This function only enforces the rate limiter.
//
// Returns a GrantDecision; GrantedDelta == 0 means "denied this round,
// retry after CooldownMs". ReservationMemory is updated by the caller only
// after the complete reconcile + grant result has been validated.
//
// headroom: the maximum delta the state-zone permits (e.g. capacity -
// reservation baseline, capped by emergency_pool exclusion).
func (a *Allocator) Grant(token string, requested, headroom uint64, urgency string) GrantDecision {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.refillLocked()

	if headroom == 0 {
		return GrantDecision{CooldownMs: 200}
	}

	// Cap the grant at headroom and at MaxGrantStep.
	delta := requested
	if delta > headroom {
		delta = headroom
	}
	if a.policy.MaxGrantStep > 0 && delta > a.policy.MaxGrantStep {
		delta = a.policy.MaxGrantStep
	}
	if urgency != UrgencyHigh {
		delta = min(delta, a.bucketCapacity())
	}
	// Never round a grant above RequestedDelta. A request whose complete tail is
	// smaller than MinGrantStep must still be grantable; otherwise an earlier
	// partial grant can strand the sandbox permanently just below its requested
	// reservation. Only a sub-minimum *partial* grant is deferred.
	if a.policy.MinGrantStep > 0 && delta < a.policy.MinGrantStep && delta != requested {
		return GrantDecision{CooldownMs: 200}
	}

	// Token bucket. urgency=high is unlimited (emergency path).
	if urgency != UrgencyHigh {
		// Resolve rejects zero effective rates. Keep direct internal policy
		// construction fail-closed without a division by zero.
		if a.policy.MemoryGrantPerSecBytes == 0 {
			return GrantDecision{CooldownMs: 200}
		}
		if a.tokens < float64(delta) {
			// Compute cooldown — when will we have enough tokens?
			need := float64(delta) - a.tokens
			eta := need / float64(a.policy.MemoryGrantPerSecBytes) * 1000
			if eta < 50 {
				eta = 50
			}
			return GrantDecision{CooldownMs: int64(math.Ceil(eta))}
		}
		a.tokens -= float64(delta)
	}

	a.recordGrantLocked(token, delta)
	return GrantDecision{
		GrantedDelta: delta,
		CooldownMs:   200,
	}
}

// CleanupHistory drops all history for a sandbox (called on Release).
func (a *Allocator) CleanupHistory(token string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.history, token)
}

// GrantUnits reserves cache in whole physical-budget units. It shares the
// existing nonemergency token bucket with Guest growth, but never consumes
// tokens for a sub-unit grant that the cache cannot use.
func (a *Allocator) GrantUnits(token string, requested, headroom, unit uint64) GrantDecision {
	if a == nil || unit == 0 || unit%4096 != 0 || requested%4096 != 0 {
		return GrantDecision{CooldownMs: 200}
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.cacheUnit = max(a.cacheUnit, unit)
	a.refillLocked()
	if requested == 0 || headroom == 0 || a.policy.MemoryGrantPerSecBytes == 0 {
		return GrantDecision{CooldownMs: 200}
	}
	limit := min(requested, headroom, a.bucketCapacity())
	if a.policy.MaxGrantStep > 0 {
		limit = min(limit, a.policy.MaxGrantStep)
	}
	// A final tail smaller than one accounting unit is all-or-nothing.
	// Partial grants cannot be reconciled safely after a lost response.
	required := unit
	if requested < unit {
		required = requested
		if limit < requested {
			return GrantDecision{CooldownMs: 200}
		}
		limit = requested
	} else {
		limit -= limit % unit
		if limit == 0 {
			return GrantDecision{CooldownMs: 200}
		}
	}
	if a.tokens < float64(required) {
		need := (float64(required) - a.tokens) / float64(a.policy.MemoryGrantPerSecBytes)
		return GrantDecision{CooldownMs: max(int64(200), int64(math.Ceil(need*1000)))}
	}
	if requested >= unit && a.tokens < float64(limit) {
		can := uint64(a.tokens)
		can -= can % unit
		limit = min(limit, can)
	}
	if limit == 0 {
		return GrantDecision{CooldownMs: 200}
	}
	a.tokens -= float64(limit)
	a.recordGrantLocked(token, limit)
	return GrantDecision{GrantedDelta: limit, CooldownMs: 200}
}
