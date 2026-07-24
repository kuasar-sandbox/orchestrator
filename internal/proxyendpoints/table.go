// Package proxyendpoints is the external-mode (proxy master process)
// counterpart to internal/mmdsauth: a capacity-bounded, in-process table of
// MMDS endpoints holding decrypted secret plaintext in bounded process
// memory. It is deliberately NOT part of internal/proxyshm — that table is
// a fixed-schema, fixed-capacity mmap structure (every field a fixed-size
// byte array) that cannot hold secrets or variable-length ciphertext/URLs;
// see internal/proxyshm/table.go's own design comments.
//
// Table implements routesync.MmdsSink (consuming the sync stream
// internal/orch publishes — see internal/routesync/serve.go's MmdsSource)
// and the same narrow Lookup/ServeStore/ServeRelay surface
// internal/mmdsauth.Authority gives internal/mmds.EndpointAuthority, so
// internal and external modes share one dispatch implementation in
// internal/mmds: internal and external modes expose the same endpoint and
// error semantics.
package proxyendpoints

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/mmdsrelay"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

// defaultMaxTotalEndpoints bounds Table's live in-memory footprint (the
// decrypted secret plaintext of every currently-declared endpoint across
// every sandbox on this node). Not operator-configurable — declaration/
// persistence policy, including any count limit, is conductor-only; this
// process only ever trusts and mirrors what the conductor already
// validated and decided to sync. A hardcoded ceiling still exists here
// purely as an implementation-level safety net against unbounded process
// memory growth, independent of any config surface.
const defaultMaxTotalEndpoints = 65536

// relayFetcher is the narrow relay surface Table needs — satisfied by
// *mmdsrelay.Client in production, stubbable in tests. Mirrors
// internal/mmdsauth's identically-shaped unexported interface (the two
// packages are not allowed to import each other, so this is duplicated
// rather than shared).
type relayFetcher interface {
	Fetch(ctx context.Context, key, rawURL, headerName, headerValue string) mmdsrelay.Result
}

// relayPublicConfig mirrors internal/orch's mmdsRelayPublicConfig JSON shape
// carried in MmdsEndpointEntry.PublicConfigJSON for a relay-backend
// endpoint: the immutable relay URL and auth header name.
type relayPublicConfig struct {
	URL            string `json:"url"`
	AuthHeaderName string `json:"auth_header_name"`
}

// Table is the external-mode MMDS endpoint store: staged-then-
// atomically-swapped per full-sync generation, gated unavailable (503 to
// every request) until the first generation of a session completes — the
// master marks its table unavailable, clears old plaintext, stages one
// bounded full generation, and atomically makes it available only at the
// matching bookmark.
type Table struct {
	relay            relayFetcher // nil = relay-backend endpoints are unavailable (503), mirrors mmdsauth.Authority
	maxTotal         int          // defaultMaxTotalEndpoints; not operator-configurable, see that const's doc
	valueWaitTimeout time.Duration

	mu         sync.RWMutex
	available  bool
	live       map[string]map[string]routesync.MmdsEndpointEntry // sandboxID -> name -> entry
	stagingGen string
	staging    map[string]map[string]routesync.MmdsEndpointEntry // non-nil only mid-generation

	waitersMu sync.Mutex
	waiters   map[string]chan struct{} // key = sid+"\x00"+name

	mx Counter
}

// Counter is the narrow metrics surface Table needs. Add (unlike
// internal/mmds.Counter's Inc-only surface) is safe here since Table only
// ever runs in the proxy-master process against a real *metrics.M, never
// through the worker-side metrics pipe.
type Counter interface {
	Inc(name string)
	Add(name string, n int64)
}

type noopCounter struct{}

func (noopCounter) Inc(string)        {}
func (noopCounter) Add(string, int64) {}

// New builds a Table. relay may be nil (relay endpoints unavailable).
// maxTotal<=0 uses defaultMaxTotalEndpoints — a Go-level constructor
// parameter (tests use a small value to exercise the capacity bound) that
// is deliberately NOT sourced from any YAML config surface in production;
// see defaultMaxTotalEndpoints's own doc. runtime is this process's own
// relay-serving policy (proxy.yaml's mmds.endpoints block — an
// independently configured value, not necessarily identical to the
// conductor's mmds.endpoints; see config.ProxyFileConfig's doc comment).
// runtime.ValueWaitTimeoutDur()<=0 defaults to 3s. mx may be nil (metrics
// off).
func New(relay relayFetcher, maxTotal int, runtime config.MMDSRuntimeConfig, mx Counter) *Table {
	if maxTotal <= 0 {
		maxTotal = defaultMaxTotalEndpoints
	}
	valueWaitTimeout := runtime.ValueWaitTimeoutDur()
	if valueWaitTimeout <= 0 {
		valueWaitTimeout = 3 * time.Second
	}
	if mx == nil {
		mx = noopCounter{}
	}
	return &Table{
		relay:            relay,
		maxTotal:         maxTotal,
		valueWaitTimeout: valueWaitTimeout,
		live:             map[string]map[string]routesync.MmdsEndpointEntry{},
		waiters:          map[string]chan struct{}{},
		mx:               mx,
	}
}

// --- routesync.MmdsSink ---

// BeginMmdsSync starts a fresh full-generation scan: the table is marked
// unavailable (every request 503s) and a new staging area opens. Entries
// applied before the matching MmdsBookmark land in staging, not live — the
// swap only happens atomically at the bookmark.
func (t *Table) BeginMmdsSync(generation string) {
	t.mu.Lock()
	t.available = false
	t.stagingGen = generation
	t.staging = map[string]map[string]routesync.MmdsEndpointEntry{}
	t.mu.Unlock()
	t.mx.Inc("mmds_sync_connected_total")
}

// Disconnected implements routesync.MmdsSink: called once the sync session
// ends, for any reason. Endpoint secrets must not linger and serve stale
// during a disconnected window, so every live/staged plaintext entry is
// cleared and the table marked unavailable (503) until a fresh
// BeginMmdsSync/MmdsBookmark pair completes — any request currently parked
// in wait()/watch() is woken immediately rather than left to time out.
func (t *Table) Disconnected() {
	t.mu.Lock()
	cleared := t.countLocked(t.live)
	oldSandboxes := sandboxIDSet(t.live)
	t.available = false
	t.live = map[string]map[string]routesync.MmdsEndpointEntry{}
	t.staging = nil
	t.stagingGen = ""
	t.mu.Unlock()
	t.notifyAll()
	t.forgetRelayForSandboxes(oldSandboxes, nil)
	t.mx.Inc("mmds_sync_disconnected_total")
	for i := 0; i < cleared; i++ {
		t.mx.Inc("mmds_cache_entries_completed_total")
	}
}

// mmdsEntryEqual reports whether a and b are identical. MmdsEndpointEntry is
// not comparable with == since SecretPlaintext is a []byte.
func mmdsEntryEqual(a, b routesync.MmdsEndpointEntry) bool {
	return a.SandboxID == b.SandboxID &&
		a.Name == b.Name &&
		a.Path == b.Path &&
		a.BackendType == b.BackendType &&
		a.PublicConfigJSON == b.PublicConfigJSON &&
		a.Revision == b.Revision &&
		a.ValuePresent == b.ValuePresent &&
		a.ContentType == b.ContentType &&
		a.ExpiresUnix == b.ExpiresUnix &&
		bytes.Equal(a.SecretPlaintext, b.SecretPlaintext)
}

// ApplyMmdsUpsert applies one endpoint's state to whichever table (staging
// mid-generation, live otherwise) is currently active. A lower revision
// than what's already recorded is ignored; an equal revision
// is idempotent only if the payload is byte-identical, otherwise it's a
// protocol error that forces a resync (the table is marked unavailable and
// cleared — the next BeginMmdsSync/Bookmark pair recovers it). Name/path
// declaration rules are NOT re-checked here: that is exclusively
// conductor-owned policy (see config.ProxyMMDSEndpointsConfig's doc
// comment) — this process trusts and mirrors whatever the conductor, which
// already validated it, decided to sync verbatim. Only capacity
// (max_total_endpoints, the actual local resource this process owns) is
// enforced locally, below — routine backpressure as the node fills up, so
// it drops only the marginal entry rather than forcing a resync.
func (t *Table) ApplyMmdsUpsert(e routesync.MmdsEndpointEntry) {
	t.mu.Lock()
	target := t.live
	if t.staging != nil {
		target = t.staging
	}
	byName := target[e.SandboxID]
	existing, exists := byName[e.Name]
	if exists {
		if e.Revision < existing.Revision {
			t.mu.Unlock()
			return // stale
		}
		if e.Revision == existing.Revision && !mmdsEntryEqual(existing, e) {
			cleared, oldSandboxes := t.forceResyncLocked()
			t.mu.Unlock()
			for i := 0; i < cleared; i++ {
				t.mx.Inc("mmds_cache_entries_completed_total")
			}
			t.forgetRelayForSandboxes(oldSandboxes, nil)
			return
		}
	} else if t.maxTotal > 0 && t.countLocked(target) >= t.maxTotal {
		// Capacity bound: this would be a brand-new (sandbox_id,name) key —
		// checked here (not just "sandbox is new") so a sandbox that
		// already has entries can't bypass the cap by adding more.
		t.mu.Unlock()
		return // the entry is simply dropped (bounded, not queued)
	}
	if byName == nil {
		byName = map[string]routesync.MmdsEndpointEntry{}
		target[e.SandboxID] = byName
	}
	byName[e.Name] = e
	live := t.staging == nil
	t.mu.Unlock()
	// Only a direct live-table insert (outside a full-generation swap) is
	// counted here — entries landing in staging aren't yet visible to
	// Lookup, so they aren't "cached" from an external observer's
	// perspective until MmdsBookmark's swap counts them.
	if live && !exists {
		t.mx.Inc("mmds_cache_entries_started_total")
	}
	if live {
		t.notify(e.SandboxID, e.Name)
	}
}

// relayForgetter is an optional extension of relayFetcher — implemented by
// *mmdsrelay.Client to evict the rate/inflight limiter state it otherwise
// keeps forever for every (sandbox_id,name) it has ever fetched. Asserted
// for rather than folded into relayFetcher itself so relayFetcher test
// stubs need not implement it.
type relayForgetter interface {
	ForgetSandbox(sandboxID string)
}

// ApplyMmdsDelete removes one endpoint from whichever table is active.
func (t *Table) ApplyMmdsDelete(k routesync.MmdsEndpointKey) {
	t.mu.Lock()
	target := t.live
	if t.staging != nil {
		target = t.staging
	}
	deleted := false
	sandboxEmptied := false
	if byName, ok := target[k.SandboxID]; ok {
		if _, ok := byName[k.Name]; ok {
			deleted = true
		}
		delete(byName, k.Name)
		if len(byName) == 0 {
			delete(target, k.SandboxID)
			sandboxEmptied = true
		}
	}
	live := t.staging == nil
	t.mu.Unlock()
	if live && deleted {
		t.mx.Inc("mmds_cache_entries_completed_total")
	}
	if live {
		t.notify(k.SandboxID, k.Name)
		// Only once the sandbox has no remaining live entries — deleting one
		// of several declared endpoints must not evict limiter state a
		// sibling endpoint on the same sandbox still needs.
		if sandboxEmptied {
			t.forgetRelayForOneSandbox(k.SandboxID)
		}
	}
}

// forgetRelayForOneSandbox evicts the shared relay client's rate/inflight
// limiter state for sandboxID, if the configured relay implements
// relayForgetter (only *mmdsrelay.Client does in production; test stubs need
// not). Safe to call with a nil t.relay (relay-backend endpoints disabled).
func (t *Table) forgetRelayForOneSandbox(sandboxID string) {
	if f, ok := t.relay.(relayForgetter); ok {
		f.ForgetSandbox(sandboxID)
	}
}

// sandboxIDSet snapshots m's sandbox_id keys into an independent set. The
// caller takes this snapshot while still holding t.mu and reads it only
// after releasing the lock (see forgetRelayForSandboxes) — it must never
// alias a live/staging map field itself, which a concurrent Table method
// can mutate the moment t.mu is released.
func sandboxIDSet(m map[string]map[string]routesync.MmdsEndpointEntry) map[string]struct{} {
	out := make(map[string]struct{}, len(m))
	for sid := range m {
		out[sid] = struct{}{}
	}
	return out
}

// forgetRelayForSandboxes calls forgetRelayForOneSandbox for every sandboxID
// in before but not in after — used wherever a full-generation swap or clear
// drops sandboxes in bulk without an individual ApplyMmdsDelete per endpoint
// (Disconnected, MmdsBookmark, forceResyncLocked's caller): those sandboxes
// can never generate that per-entry delete event, so without this their
// relay limiter state would persist in the shared, otherwise-never-pruned
// mmdsrelay.Client.limiters map for the rest of the process's lifetime.
// before/after must be sandboxIDSet snapshots, not the live/staging maps
// themselves — this always runs after t.mu has been released.
func (t *Table) forgetRelayForSandboxes(before, after map[string]struct{}) {
	for sid := range before {
		if _, stillLive := after[sid]; !stillLive {
			t.forgetRelayForOneSandbox(sid)
		}
	}
}

// MmdsBookmark completes generation's scan: if it matches the in-progress
// staging generation, the staged set atomically becomes live and the table
// becomes available. A stale/mismatched generation (e.g. a race with a
// forced resync) is dropped rather than applied.
func (t *Table) MmdsBookmark(generation string) {
	t.mu.Lock()
	var oldCount, newCount int
	var oldSandboxes, newSandboxes map[string]struct{}
	swapped := t.staging != nil && t.stagingGen == generation
	if swapped {
		oldSandboxes, newSandboxes = sandboxIDSet(t.live), sandboxIDSet(t.staging)
		oldCount, newCount = t.countLocked(t.live), t.countLocked(t.staging)
		t.live = t.staging
		t.staging = nil
		t.stagingGen = ""
		t.available = true
	}
	t.mu.Unlock()
	// A full-generation swap atomically replaces the entire live set, so —
	// for the started/completed counter-pair convention — every prior live
	// entry is counted as removed and every newly-live entry as added, even
	// ones byte-identical across generations. Counter is Inc-only (mirrors
	// internal/mmds.Counter, satisfied by the worker-side metrics pipe too,
	// which has no Add), so bounded counts are walked one Inc at a time.
	if swapped {
		for i := 0; i < oldCount; i++ {
			t.mx.Inc("mmds_cache_entries_completed_total")
		}
		for i := 0; i < newCount; i++ {
			t.mx.Inc("mmds_cache_entries_started_total")
		}
		// A sandbox declared in the old generation but not re-declared in
		// this one (e.g. deleted while this table was disconnected, so no
		// individual ApplyMmdsDelete for it was ever delivered) needs its
		// relay limiter state released here — this is the only place that
		// ever observes its absence.
		t.forgetRelayForSandboxes(oldSandboxes, newSandboxes)
	}
	t.notifyAll() // every waiter re-checks now that the table may be available
}

// forceResyncLocked handles an equal-revision-mismatch protocol error:
// clears both live and any in-progress staging and marks the table
// unavailable, so every request 503s until the next full generation
// completes. Caller must hold t.mu. Returns the live entry count cleared,
// for the caller to report as completed after unlocking, and an independent
// snapshot of the cleared live set's sandbox_id keys, for the caller to
// reconcile relay limiter state against (see forgetRelayForSandboxes) —
// also after unlocking.
func (t *Table) forceResyncLocked() (clearedLive int, oldSandboxes map[string]struct{}) {
	clearedLive = t.countLocked(t.live)
	oldSandboxes = sandboxIDSet(t.live)
	t.available = false
	t.live = map[string]map[string]routesync.MmdsEndpointEntry{}
	t.staging = nil
	t.stagingGen = ""
	return clearedLive, oldSandboxes
}

func (t *Table) countLocked(m map[string]map[string]routesync.MmdsEndpointEntry) int {
	n := 0
	for _, byName := range m {
		n += len(byName)
	}
	return n
}

// --- internal/mmds.EndpointAuthority-compatible dispatch (via
// internal/mmdsrpc.EndpointTable) ---

// ErrUnavailable is returned by Lookup/ServeStore/ServeRelay when the table
// itself is not currently usable (no full generation has completed yet, or
// a disconnect/protocol error cleared it) — distinct from a legitimate
// "this sandbox has no such endpoint" negative result. On a sync
// disconnect/protocol error the guest must get 503 until a valid full
// generation/bookmark arrives, never a silent fallthrough to the built-in
// envd response.
var ErrUnavailable = errors.New("proxyendpoints: table unavailable")

// Lookup resolves the exact (sandbox_id, path) to its declared name+backend
// type. found=false (err=nil) means no endpoint owns that path. A non-nil
// err (table unavailable) must never be treated the same as found=false.
func (t *Table) Lookup(_ context.Context, sandboxID, path string) (name, backendType string, found bool, err error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if !t.available {
		return "", "", false, ErrUnavailable
	}
	for n, e := range t.live[sandboxID] {
		if e.Path == path {
			return n, e.BackendType, true, nil
		}
	}
	return "", "", false, nil
}

// ServeStore answers a store-backend GET, applying the same bounded
// never-configured wait as internal/mmdsauth.Authority.ServeStore.
func (t *Table) ServeStore(ctx context.Context, sandboxID, name string) (value []byte, contentType string, revision int64, present bool, err error) {
	e, ok, unavailable := t.get(sandboxID, name)
	if unavailable {
		return nil, "", 0, false, ErrUnavailable
	}
	if !ok {
		return nil, "", 0, false, nil
	}
	if e.Revision == 0 && !e.ValuePresent {
		if !t.wait(ctx, sandboxID, name) {
			return nil, "", 0, false, nil
		}
		if e, ok, unavailable = t.get(sandboxID, name); unavailable {
			return nil, "", 0, false, ErrUnavailable
		} else if !ok {
			return nil, "", 0, false, nil
		}
	}
	if !e.ValuePresent {
		return nil, "", e.Revision, false, nil
	}
	if e.ExpiresUnix > 0 && time.Now().Unix() >= e.ExpiresUnix {
		return nil, "", e.Revision, false, nil
	}
	return e.SecretPlaintext, e.ContentType, e.Revision, true, nil
}

// ServeRelay answers a relay-backend GET, applying the same bounded
// never-configured wait and cancellation-on-change as
// internal/mmdsauth.Authority.ServeRelay.
func (t *Table) ServeRelay(ctx context.Context, sandboxID, name string) (status int, contentType string, body []byte, ok bool, err error) {
	if t.relay == nil {
		return http.StatusServiceUnavailable, "", nil, true, nil
	}
	e, found, unavailable := t.get(sandboxID, name)
	if unavailable {
		return 0, "", nil, false, ErrUnavailable
	}
	if !found {
		return 0, "", nil, false, nil
	}
	if e.Revision == 0 && !e.ValuePresent {
		if !t.wait(ctx, sandboxID, name) {
			return 0, "", nil, false, nil
		}
		if e, found, unavailable = t.get(sandboxID, name); unavailable {
			return 0, "", nil, false, ErrUnavailable
		} else if !found {
			return 0, "", nil, false, nil
		}
	}
	if !e.ValuePresent {
		return 0, "", nil, false, nil
	}

	var cfg relayPublicConfig
	if err := json.Unmarshal([]byte(e.PublicConfigJSON), &cfg); err != nil || cfg.URL == "" || cfg.AuthHeaderName == "" {
		return http.StatusBadGateway, "", nil, true, nil
	}

	watchCtx, cancel := t.watch(ctx, sandboxID, name)
	defer cancel()
	res := t.relay.Fetch(watchCtx, sandboxID+"\x00"+name, cfg.URL, cfg.AuthHeaderName, string(e.SecretPlaintext))
	return res.Status, res.ContentType, res.Body, true, nil
}

// get returns the current entry for (sandboxID,name). found=false means the
// table is available but has no such entry; unavailable=true means the
// table itself is not usable right now (found is meaningless in that case).
func (t *Table) get(sandboxID, name string) (entry routesync.MmdsEndpointEntry, found, unavailable bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if !t.available {
		return routesync.MmdsEndpointEntry{}, false, true
	}
	e, ok := t.live[sandboxID][name]
	return e, ok, false
}
