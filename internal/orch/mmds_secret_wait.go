package orch

import (
	"context"
	"strings"
	"sync"
	"time"
)

// mmdsSecretWaiter parks a bounded number of goroutines waiting for a
// not-yet-configured MMDS secret to arrive, waking them as soon as an admin
// PUT/DELETE lands for that exact (sandbox, name) pair -- or on timeout,
// whichever comes first. Ported from the superseded mmds-endpoints branch's
// internal/mmdsauth.Authority (wait/Notify/ForgetSandbox), narrowed to just
// this channel-bookkeeping primitive: the DB access lives in internal/store
// and the guest-serving decision lives in Orchestrator.MMDSRoute.
type mmdsSecretWaiter struct {
	timeout time.Duration

	mu      sync.Mutex
	waiters map[string]chan struct{} // sid+"\x00"+name -> close-to-broadcast
}

func newMMDSSecretWaiter(timeout time.Duration) *mmdsSecretWaiter {
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	return &mmdsSecretWaiter{timeout: timeout, waiters: map[string]chan struct{}{}}
}

func mmdsSecretWaitKey(sid, name string) string { return sid + "\x00" + name }

// register creates (or reuses) the waiter channel for (sid, name) and
// returns it immediately -- a fast, lock-only operation, deliberately split
// out from block so a caller can register *before* re-checking the store for
// the value. A closed Go channel stays observably closed forever, even for a
// receive that starts after the close already happened, so once register has
// returned, any Notify(sid, name) from that point on is guaranteed to close
// this exact channel -- whether or not the caller has started blocking on it
// yet. That's what closes the lost-wakeup window: checking the store first
// and only registering afterward (the old shape) left a gap where a PUT's
// Notify could run, find nobody registered, and be silently missed, so a
// guest that started waiting a moment later would sit out the full timeout
// even though the value had just landed.
func (w *mmdsSecretWaiter) register(sid, name string) <-chan struct{} {
	key := mmdsSecretWaitKey(sid, name)
	w.mu.Lock()
	defer w.mu.Unlock()
	ch, ok := w.waiters[key]
	if !ok {
		ch = make(chan struct{})
		w.waiters[key] = ch
	}
	return ch
}

// block waits on ch (from a prior register call) until it closes, the
// waiter's configured timeout elapses, or ctx is done, whichever comes
// first. Returns true only on an explicit close (worth re-checking the
// store); false on timeout/ctx-done (caller treats the value as still
// absent).
func (w *mmdsSecretWaiter) block(ctx context.Context, ch <-chan struct{}) bool {
	timer := time.NewTimer(w.timeout)
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

// wait registers and blocks in one call -- register(sid, name) followed by
// block. Kept for callers that have no need to re-check the store between
// registering and blocking (block on its own, given register's channel, is
// what closes the lost-wakeup race for callers that do).
func (w *mmdsSecretWaiter) wait(ctx context.Context, sid, name string) bool {
	return w.block(ctx, w.register(sid, name))
}

// Notify wakes every goroutine currently parked on (sid, name). A no-op if
// nobody's waiting.
func (w *mmdsSecretWaiter) Notify(sid, name string) {
	key := mmdsSecretWaitKey(sid, name)
	w.mu.Lock()
	defer w.mu.Unlock()
	if ch, ok := w.waiters[key]; ok {
		close(ch)
		delete(w.waiters, key) // the next waiter creates a fresh channel
	}
}

// ForgetSandbox force-wakes and discards every waiter for sid (any name), so
// a deleted sandbox's map entries don't leak for the process's lifetime.
func (w *mmdsSecretWaiter) ForgetSandbox(sid string) {
	prefix := sid + "\x00"
	w.mu.Lock()
	defer w.mu.Unlock()
	for key, ch := range w.waiters {
		if strings.HasPrefix(key, prefix) {
			close(ch)
			delete(w.waiters, key)
		}
	}
}
