package mmdsrelay

import (
	"sync"
	"time"
)

// keyLimiter bounds one endpoint's ((sandbox_id,name)-scoped) relay traffic:
// a hand-rolled token bucket for requests/sec (avoiding a new dependency on
// golang.org/x/time/rate, matching this repo's low-dependency style — see
// e.g. internal/metrics's own from-scratch counter registry) plus a
// non-blocking semaphore for inflight concurrency.
type keyLimiter struct {
	mu       sync.Mutex
	tokens   float64
	lastFill time.Time
	rate     float64 // tokens/sec; <=0 disables rate limiting
	burst    float64

	sem chan struct{} // buffered to maxInflight; a full channel means "over the cap"
}

func newKeyLimiter(ratePerSecond float64, maxInflight int) *keyLimiter {
	if maxInflight <= 0 {
		maxInflight = 1
	}
	burst := ratePerSecond
	if burst < 1 {
		burst = 1
	}
	return &keyLimiter{
		tokens:   burst,
		lastFill: time.Now(),
		rate:     ratePerSecond,
		burst:    burst,
		sem:      make(chan struct{}, maxInflight),
	}
}

// allowRate reports whether one more request may proceed under the
// requests/second budget, consuming a token if so.
func (l *keyLimiter) allowRate() bool {
	if l.rate <= 0 {
		return true
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	elapsed := now.Sub(l.lastFill).Seconds()
	l.lastFill = now
	l.tokens += elapsed * l.rate
	if l.tokens > l.burst {
		l.tokens = l.burst
	}
	if l.tokens < 1 {
		return false
	}
	l.tokens--
	return true
}

// acquireInflight non-blockingly claims one inflight slot. release must be
// called exactly once iff ok is true.
func (l *keyLimiter) acquireInflight() (release func(), ok bool) {
	select {
	case l.sem <- struct{}{}:
		return func() { <-l.sem }, true
	default:
		return nil, false
	}
}
