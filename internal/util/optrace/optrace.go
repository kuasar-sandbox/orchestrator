// Package optrace is an opt-in, env-gated operation tracer for the
// store/cache daemons. With no per-op timeout by default, a wedged
// backend no longer fails fast — it just stalls. This makes the stall
// observable: every traced op logs its duration (WARN over a slow
// threshold) and a background reporter periodically dumps the ops that
// are still in flight and how long they have been stuck.
//
// Disabled (the default) it adds one atomic load per op and nothing
// else — Begin returns a shared no-op closure.
package optrace

import (
	"log"
	"os"
	"sync"
	"time"
)

// Tracer is safe for concurrent use. The zero value is a valid
// disabled tracer (Begin is a no-op).
type Tracer struct {
	enabled bool
	slow    time.Duration
	name    string

	mu       sync.Mutex
	seq      uint64
	inflight map[uint64]inflightOp
}

type inflightOp struct {
	op    string
	start time.Time
}

var noop = func() {}

// FromEnv builds a tracer for component `name` (e.g. "store-ctl").
// Enabled when <ENV>_DEBUG is set truthy, where ENV is the upper-cased
// name with '-' → '_'. <ENV>_SLOW (a Go duration, default 1s) is the
// threshold above which a completed op is logged at WARN and an
// in-flight op is reported as stuck. Off ⇒ near-zero overhead.
func FromEnv(name string) *Tracer {
	env := ""
	for _, r := range name {
		if r == '-' {
			r = '_'
		}
		if r >= 'a' && r <= 'z' {
			r -= 'a' - 'A'
		}
		env += string(r)
	}
	t := &Tracer{name: name, slow: time.Second}
	if v := os.Getenv(env + "_DEBUG"); v == "" || v == "0" || v == "false" {
		return t // disabled
	}
	t.enabled = true
	if d, err := time.ParseDuration(os.Getenv(env + "_SLOW")); err == nil && d > 0 {
		t.slow = d
	}
	t.inflight = make(map[uint64]inflightOp)
	log.Printf("[%s] optrace: enabled (slow=%s) — per-op timing + stuck-request reporting", name, t.slow)
	go t.reporter()
	return t
}

// Begin records the start of operation `op` and returns a closure to
// call (defer) when it completes. Disabled ⇒ returns a shared no-op.
func (t *Tracer) Begin(op string) func() {
	if t == nil || !t.enabled {
		return noop
	}
	start := time.Now()
	t.mu.Lock()
	t.seq++
	id := t.seq
	t.inflight[id] = inflightOp{op: op, start: start}
	t.mu.Unlock()
	return func() {
		d := time.Since(start)
		t.mu.Lock()
		delete(t.inflight, id)
		t.mu.Unlock()
		if d >= t.slow {
			log.Printf("[%s] optrace: SLOW op=%s dur=%s", t.name, op, d.Round(time.Millisecond))
		} else {
			log.Printf("[%s] optrace: op=%s dur=%s", t.name, op, d.Round(time.Millisecond))
		}
	}
}

// reporter periodically logs ops that have been in flight longer than
// the slow threshold so a stall on a wedged backend is visible.
func (t *Tracer) reporter() {
	tick := time.NewTicker(t.slow)
	defer tick.Stop()
	for range tick.C {
		now := time.Now()
		t.mu.Lock()
		for id, op := range t.inflight {
			if age := now.Sub(op.start); age >= t.slow {
				log.Printf("[%s] optrace: STUCK id=%d op=%s age=%s",
					t.name, id, op.op, age.Round(time.Millisecond))
			}
		}
		t.mu.Unlock()
	}
}

// InflightCount returns the number of ops currently in flight (0 when
// disabled). Exposed for health/stats surfaces.
func (t *Tracer) InflightCount() int {
	if t == nil || !t.enabled {
		return 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.inflight)
}
