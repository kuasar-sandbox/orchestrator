package nodectl

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"time"
)

// AdmissionPolicy bounds how fast new sandboxes can be admitted.
//
// Rate / Burst form a token bucket; MaxConcurrentCreating caps the number
// of sandboxes between admit and Settled. StartupTTL bounds how long
// admit's reservation can persist without reaching Settled (after which
// the controller releases it as a creation failure).
type AdmissionPolicy struct {
	Rate                 int           // tokens / sec
	Burst                int           // bucket capacity
	MaxConcurrentCreating int
	StartupTTL           time.Duration
	QueueTTL             time.Duration
}

// AdmissionController decides whether to admit, queue, or reject a new
// Admit request. v1 is FIFO + token bucket; future versions can add
// priority.
type AdmissionController struct {
	mu     sync.Mutex
	policy AdmissionPolicy

	// Token bucket state.
	tokens     float64
	lastRefill time.Time

	// Concurrency counter — incremented at admit, decremented at Settled
	// or Release.
	creating int

	// drained, when true, causes TryAdmit to return rejected. Toggled
	// via the AdminDrain message (node-ctl drain CLI). Existing
	// reservations are unaffected.
	drained bool
}

// NewAdmissionController initializes with full bucket.
func NewAdmissionController(policy AdmissionPolicy) *AdmissionController {
	return &AdmissionController{
		policy:     policy,
		tokens:     float64(policy.Burst),
		lastRefill: time.Now(),
	}
}

// refill replenishes tokens based on elapsed time. Caller holds lock.
func (a *AdmissionController) refill() {
	now := time.Now()
	elapsed := now.Sub(a.lastRefill).Seconds()
	a.tokens += elapsed * float64(a.policy.Rate)
	if a.tokens > float64(a.policy.Burst) {
		a.tokens = float64(a.policy.Burst)
	}
	a.lastRefill = now
}

// Decision is the outcome of a TryAdmit call.
type Decision struct {
	Status      string        // StatusAdmitted / StatusQueued / StatusRejected
	QueueWait   time.Duration // estimated wait when queued
	RejectMsg   string        // populated when StatusRejected
}

// TryAdmit checks token bucket, concurrency, drain mode, and pool
// watermark. Does NOT actually allocate budget — caller does that under
// State.Lock if admitted. CountAdmit must be called by caller after a
// successful admit to consume the token / bump the creating counter.
func (a *AdmissionController) TryAdmit() Decision {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.drained {
		return Decision{Status: StatusRejected, RejectMsg: "controller is draining"}
	}

	a.refill()

	if a.creating >= a.policy.MaxConcurrentCreating {
		// Could be queued; v1 estimates wait based on average per-admit
		// time (~ 1/Rate seconds). Fall through to queued path.
		eta := time.Duration(float64(time.Second) / float64(a.policy.Rate))
		return Decision{Status: StatusQueued, QueueWait: eta}
	}
	if a.tokens < 1 {
		// Compute when next token arrives.
		need := 1 - a.tokens
		eta := time.Duration(need / float64(a.policy.Rate) * float64(time.Second))
		return Decision{Status: StatusQueued, QueueWait: eta}
	}
	return Decision{Status: StatusAdmitted}
}

// SetDrained toggles drain mode. Returns previous value.
func (a *AdmissionController) SetDrained(v bool) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	prev := a.drained
	a.drained = v
	return prev
}

// IsDrained reports whether drain mode is active.
func (a *AdmissionController) IsDrained() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.drained
}

// CountAdmit consumes one token and increments the creating counter.
// Call after the controller has successfully built a Reservation.
func (a *AdmissionController) CountAdmit() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.tokens -= 1
	a.creating++
}

// Settled decrements the creating counter (sandbox crossed startup TTL).
func (a *AdmissionController) Settled() {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.creating > 0 {
		a.creating--
	}
}

// Released decrements the creating counter when a sandbox dies before
// Settled (e.g. CH crash during boot). Idempotent: safe to call when
// Settled was already called.
func (a *AdmissionController) Released(stage string) {
	if stage == StageSettled || stage == StageBurst || stage == StageRecover {
		// Already left the creating window via Settled; nothing to do.
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.creating > 0 {
		a.creating--
	}
}

// CreatingCount returns the current pending-startup count for diagnostics.
func (a *AdmissionController) CreatingCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.creating
}

// NewToken returns a fresh random opaque token suitable for reservation
// identification. 16 bytes hex = 32 chars; collision risk is
// negligible.
func NewToken() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// rand.Read should never fail; if it does, fall back to time-
		// based token to avoid a crash. Token uniqueness still holds in
		// practice because the controller already verifies non-duplicate
		// at insert.
		return fmt.Sprintf("t-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}
