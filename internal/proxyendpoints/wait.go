package proxyendpoints

import (
	"context"
	"time"
)

// This file mirrors internal/mmdsauth.Authority's wait/Notify/watch trio
// exactly (same channel-close-and-recreate broadcast idiom) — the two
// packages implement the same value-wait/cancellation contract for internal
// vs external mode and cannot share code without an import each other must
// not take (mmdsauth depends on internal/store; this package must not).

func waitKey(sandboxID, name string) string { return sandboxID + "\x00" + name }

// wait parks until Notify(sandboxID,name) fires or t.valueWaitTimeout
// elapses, whichever comes first — the bounded never-configured wait.
func (t *Table) wait(ctx context.Context, sandboxID, name string) bool {
	key := waitKey(sandboxID, name)
	t.waitersMu.Lock()
	ch, ok := t.waiters[key]
	if !ok {
		ch = make(chan struct{})
		t.waiters[key] = ch
	}
	t.waitersMu.Unlock()

	timer := time.NewTimer(t.valueWaitTimeout)
	defer timer.Stop()
	select {
	case <-ch:
		return true
	case <-timer.C:
		return false
	case <-ctx.Done():
		return false
	}
}

// watch derives a context cancelled the next time Notify(sandboxID,name)
// fires — used by ServeRelay to abort an in-flight upstream fetch when its
// auth value changes mid-flight (cancellation on auth revision changes).
func (t *Table) watch(parent context.Context, sandboxID, name string) (context.Context, context.CancelFunc) {
	key := waitKey(sandboxID, name)
	t.waitersMu.Lock()
	ch, ok := t.waiters[key]
	if !ok {
		ch = make(chan struct{})
		t.waiters[key] = ch
	}
	t.waitersMu.Unlock()

	cctx, cancel := context.WithCancel(parent)
	go func() {
		select {
		case <-ch:
			cancel()
		case <-cctx.Done():
		}
	}()
	return cctx, cancel
}

// notify wakes any goroutine parked in wait()/watch() for (sandboxID,name).
func (t *Table) notify(sandboxID, name string) {
	key := waitKey(sandboxID, name)
	t.waitersMu.Lock()
	defer t.waitersMu.Unlock()
	if ch, ok := t.waiters[key]; ok {
		close(ch)
		delete(t.waiters, key)
	}
}

// notifyAll wakes every currently-parked waiter — called after a
// bookmark, since the table transitioning available could unblock any
// number of pending requests at once.
func (t *Table) notifyAll() {
	t.waitersMu.Lock()
	defer t.waitersMu.Unlock()
	for key, ch := range t.waiters {
		close(ch)
		delete(t.waiters, key)
	}
}
