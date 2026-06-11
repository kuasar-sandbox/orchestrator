// Package routetable is the proxy worker's local view of the orchestrator's route
// set, kept in sync over internal/routesync. It implements routesync.Sink (apply
// snapshot/upsert/delete) and routesync.WakeSource (yield sandboxes to resume),
// and adds the data-plane "park" primitive: a request for a missing/paused
// sandbox blocks (after prompting a Wake) until its route arrives running or the
// park timeout elapses.
package routetable

import (
	"context"
	"sync"
	"time"

	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/routesync"
)

// Table is concurrency-safe and shared by the proxy's data-plane handlers and its
// route-sync server.
type Table struct {
	mu       sync.Mutex
	routes   map[string]routesync.RouteEntry
	policy   routesync.Policy
	waiters  map[string][]chan struct{} // per-sid parking notifications
	synced   bool
	syncedCh chan struct{} // closed once the first snapshot is applied

	wakeCh  chan string
	pending map[string]bool // sids with a wake already queued (dedupe)

	defaultPark time.Duration
}

// New creates an empty table. defaultPark is the park timeout used until the
// orchestrator pushes a Policy with its own (and as a fallback if that is 0).
func New(defaultPark time.Duration) *Table {
	if defaultPark <= 0 {
		defaultPark = 30 * time.Second
	}
	return &Table{
		routes:      map[string]routesync.RouteEntry{},
		waiters:     map[string][]chan struct{}{},
		syncedCh:    make(chan struct{}),
		wakeCh:      make(chan string, 1024),
		pending:     map[string]bool{},
		defaultPark: defaultPark,
	}
}

// --- routesync.Sink ---

func (t *Table) ApplySnapshot(routes []routesync.RouteEntry) {
	t.mu.Lock()
	t.routes = make(map[string]routesync.RouteEntry, len(routes))
	for _, r := range routes {
		t.routes[r.SandboxID] = r
		t.notifyLocked(r.SandboxID)
	}
	if !t.synced {
		t.synced = true
		close(t.syncedCh)
	}
	t.mu.Unlock()
}

func (t *Table) ApplyUpsert(r routesync.RouteEntry) {
	t.mu.Lock()
	t.routes[r.SandboxID] = r
	t.notifyLocked(r.SandboxID) // wake parked requests (they re-check state)
	t.mu.Unlock()
}

func (t *Table) ApplyDelete(sid string) {
	t.mu.Lock()
	delete(t.routes, sid)
	t.notifyLocked(sid) // unblock parked requests -> they see "gone"
	t.mu.Unlock()
}

func (t *Table) SetPolicy(p routesync.Policy) {
	t.mu.Lock()
	t.policy = p
	t.mu.Unlock()
}

// --- routesync.WakeSource ---

// NextWake blocks until a wake is queued or ctx is done.
func (t *Table) NextWake(ctx context.Context) (string, bool) {
	select {
	case sid := <-t.wakeCh:
		t.mu.Lock()
		delete(t.pending, sid)
		t.mu.Unlock()
		return sid, true
	case <-ctx.Done():
		return "", false
	}
}

// --- proxy-side reads ---

// Lookup returns the current route for sid, if any.
func (t *Table) Lookup(sid string) (routesync.RouteEntry, bool) {
	t.mu.Lock()
	r, ok := t.routes[sid]
	t.mu.Unlock()
	return r, ok
}

// ByFloatingIP returns the running sandbox id whose floating IP matches ip — the lookup
// the MMDS service parks around at PUT to map a guest's (SNAT'd) source IP to its id.
// Implements mmds.Source (PUT stage) for the external proxy worker. Running only: a
// paused sandbox's floating IP may be reused by a running one, so paused entries would
// be ambiguous.
func (t *Table) ByFloatingIP(ip string) (sandboxID string, ok bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, r := range t.routes {
		if r.FloatingIP == ip && r.State == routesync.StateRunning {
			return r.SandboxID, true
		}
	}
	return "", false
}

// SandboxInfo returns sid's current template id + access token (mmds.Source, GET stage).
func (t *Table) SandboxInfo(sid string) (templateID, accessToken string, ok bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if r, ok := t.routes[sid]; ok && r.State == routesync.StateRunning {
		return r.TemplateID, r.AccessToken, true
	}
	return "", "", false
}

// Policy returns the last pushed policy.
func (t *Table) Policy() routesync.Policy {
	t.mu.Lock()
	p := t.policy
	t.mu.Unlock()
	return p
}

// Wake asks the orchestrator (over the sync stream) to resume sid. Deduped while a
// wake for the same sid is still queued; dropped if the queue is full (the next
// data-plane request retries).
func (t *Table) Wake(sid string) {
	t.mu.Lock()
	if t.pending[sid] {
		t.mu.Unlock()
		return
	}
	t.pending[sid] = true
	t.mu.Unlock()
	select {
	case t.wakeCh <- sid:
	default:
		t.mu.Lock()
		delete(t.pending, sid)
		t.mu.Unlock()
	}
}

// Resolve returns a running route for sid, waiting up to the park timeout for the
// route to sync / the sandbox to resume. It prompts a Wake when the sandbox is not
// already running. ok=false means it did not become running in time (missing,
// dead, or timed out) — the caller maps that to 404.
func (t *Table) Resolve(ctx context.Context, sid string) (routesync.RouteEntry, bool) {
	if !t.waitSynced(ctx) {
		return routesync.RouteEntry{}, false
	}
	if r, ok := t.Lookup(sid); ok && r.State == routesync.StateRunning {
		return r, true
	}
	t.Wake(sid)
	return t.waitRunning(ctx, sid)
}

// parkTimeout is the effective park budget (pushed policy, else default).
func (t *Table) parkTimeout() time.Duration {
	t.mu.Lock()
	ms := t.policy.ParkTimeoutMS
	t.mu.Unlock()
	if ms > 0 {
		return time.Duration(ms) * time.Millisecond
	}
	return t.defaultPark
}

// waitSynced blocks until the first snapshot is applied (or park timeout / ctx).
func (t *Table) waitSynced(ctx context.Context) bool {
	t.mu.Lock()
	if t.synced {
		t.mu.Unlock()
		return true
	}
	ch := t.syncedCh
	t.mu.Unlock()
	timer := time.NewTimer(t.parkTimeout())
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

// waitRunning parks until sid's route is running, the park timeout elapses, or ctx
// is cancelled.
func (t *Table) waitRunning(ctx context.Context, sid string) (routesync.RouteEntry, bool) {
	deadline := time.Now().Add(t.parkTimeout())
	for {
		t.mu.Lock()
		if r, ok := t.routes[sid]; ok && r.State == routesync.StateRunning {
			t.mu.Unlock()
			return r, true
		}
		w := make(chan struct{}, 1)
		t.waiters[sid] = append(t.waiters[sid], w)
		t.mu.Unlock()

		remaining := time.Until(deadline)
		if remaining <= 0 {
			t.removeWaiter(sid, w)
			return t.runningOrFalse(sid)
		}
		timer := time.NewTimer(remaining)
		select {
		case <-w:
			timer.Stop() // route changed; re-check on the next iteration
		case <-timer.C:
			t.removeWaiter(sid, w)
			return t.runningOrFalse(sid)
		case <-ctx.Done():
			timer.Stop()
			t.removeWaiter(sid, w)
			return routesync.RouteEntry{}, false
		}
	}
}

func (t *Table) runningOrFalse(sid string) (routesync.RouteEntry, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	r, ok := t.routes[sid]
	return r, ok && r.State == routesync.StateRunning
}

// notifyLocked signals (and clears) all parked waiters for sid. Caller holds mu.
func (t *Table) notifyLocked(sid string) {
	for _, w := range t.waiters[sid] {
		select {
		case w <- struct{}{}:
		default:
		}
	}
	delete(t.waiters, sid)
}

// removeWaiter drops a specific waiter (it timed out / ctx cancelled before being
// notified). A no-op if notifyLocked already cleared it.
func (t *Table) removeWaiter(sid string, w chan struct{}) {
	t.mu.Lock()
	defer t.mu.Unlock()
	ws := t.waiters[sid]
	for i, x := range ws {
		if x == w {
			t.waiters[sid] = append(ws[:i], ws[i+1:]...)
			break
		}
	}
	if len(t.waiters[sid]) == 0 {
		delete(t.waiters, sid)
	}
}
