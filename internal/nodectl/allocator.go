package nodectl

import (
	"sync"
	"time"
)

// AllocatorPolicy bounds runtime memory budget grants.
//
// MemoryGrantPerSecBytes is the FIFO drain rate — even with 100 sandboxes
// burst-requesting simultaneously, total grants per second cannot exceed
// this rate. This is the core anti-thundering-herd lever.
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

// NewAllocator builds an Allocator with full bucket and empty history.
func NewAllocator(policy AllocatorPolicy) *Allocator {
	return &Allocator{
		policy:     policy,
		tokens:     float64(policy.MemoryGrantPerSecBytes), // start with 1s of headroom
		lastRefill: time.Now(),
		history:    make(map[string]*grantHistory),
	}
}

// refillLocked refills the per-second bucket. Caller holds lock.
func (a *Allocator) refillLocked() {
	now := time.Now()
	elapsed := now.Sub(a.lastRefill).Seconds()
	a.tokens += elapsed * float64(a.policy.MemoryGrantPerSecBytes)
	if a.tokens > float64(a.policy.MemoryGrantPerSecBytes) {
		a.tokens = float64(a.policy.MemoryGrantPerSecBytes)
	}
	a.lastRefill = now
}

// recordGrantLocked appends a grant to the per-sandbox history and
// trims expired samples. Caller holds lock.
func (a *Allocator) recordGrantLocked(token string, bytes uint64) {
	h, ok := a.history[token]
	if !ok {
		h = &grantHistory{}
		a.history[token] = h
	}
	now := time.Now()
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
	// Never round a grant above RequestedDelta. A request whose complete tail is
	// smaller than MinGrantStep must still be grantable; otherwise an earlier
	// partial grant can strand the sandbox permanently just below its requested
	// reservation. Only a sub-minimum *partial* grant is deferred.
	if a.policy.MinGrantStep > 0 && delta < a.policy.MinGrantStep && delta != requested {
		return GrantDecision{CooldownMs: 200}
	}

	// Token bucket. urgency=high is unlimited (emergency path).
	if urgency != UrgencyHigh {
		if a.tokens < float64(delta) {
			// Compute cooldown — when will we have enough tokens?
			need := float64(delta) - a.tokens
			eta := need / float64(a.policy.MemoryGrantPerSecBytes) * 1000
			if eta < 50 {
				eta = 50
			}
			return GrantDecision{CooldownMs: int64(eta)}
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
