package orch

import (
	"context"
	"sync"
)

// keyedLockGroup serializes distinct lifecycle mutations for one sandbox while
// allowing unrelated sandboxes to proceed independently. Entries are retained
// only while an owner or waiter references them.
type keyedLockGroup struct {
	mu    sync.Mutex
	locks map[string]*keyedLock
}

type keyedLock struct {
	mu   sync.Mutex
	refs int
}

func (g *keyedLockGroup) Lock(key string) func() {
	g.mu.Lock()
	if g.locks == nil {
		g.locks = make(map[string]*keyedLock)
	}
	lock := g.locks[key]
	if lock == nil {
		lock = &keyedLock{}
		g.locks[key] = lock
	}
	lock.refs++
	g.mu.Unlock()

	lock.mu.Lock()
	return func() {
		lock.mu.Unlock()
		g.mu.Lock()
		lock.refs--
		if lock.refs == 0 && g.locks[key] == lock {
			delete(g.locks, key)
		}
		g.mu.Unlock()
	}
}

func (o *Orchestrator) markDeadlineIntent(sid string) {
	o.deadlineIntentMu.Lock()
	o.deadlineIntents[sid] = struct{}{}
	o.deadlineIntentMu.Unlock()
}

func (o *Orchestrator) hasDeadlineIntent(sid string) bool {
	o.deadlineIntentMu.Lock()
	_, ok := o.deadlineIntents[sid]
	o.deadlineIntentMu.Unlock()
	return ok
}

func (o *Orchestrator) clearDeadlineIntent(sid string) {
	o.deadlineIntentMu.Lock()
	delete(o.deadlineIntents, sid)
	o.deadlineIntentMu.Unlock()
}

// resumeSandbox collapses concurrent wakeups, then runs the actual resume under
// the same per-sandbox lifecycle boundary used by delete and deadline updates.
// A deadline explicitly set while paused is preserved exactly once; ordinary
// data-plane wakeups continue to re-arm the node default TTL.
func (o *Orchestrator) resumeSandbox(ctx context.Context, sid string) error {
	return o.sf.Do(sid, func() error {
		unlock := o.lifecycle.Lock(sid)
		defer unlock()

		preserveDeadline := o.hasDeadlineIntent(sid)
		if err := o.resumeIfPaused(ctx, sid, preserveDeadline); err != nil {
			return err
		}
		o.clearDeadlineIntent(sid)
		return nil
	})
}
