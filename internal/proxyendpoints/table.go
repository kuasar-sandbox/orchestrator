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
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/mmdsrelay"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

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
	maxTotal         int          // 0 = unbounded
	valueWaitTimeout time.Duration

	mu         sync.RWMutex
	available  bool
	live       map[string]map[string]routesync.MmdsEndpointEntry // sandboxID -> name -> entry
	stagingGen string
	staging    map[string]map[string]routesync.MmdsEndpointEntry // non-nil only mid-generation

	waitersMu sync.Mutex
	waiters   map[string]chan struct{} // key = sid+"\x00"+name
}

// New builds a Table. relay may be nil (relay endpoints unavailable).
// maxTotal<=0 means unbounded. valueWaitTimeout<=0 defaults to 3s.
func New(relay relayFetcher, maxTotal int, valueWaitTimeout time.Duration) *Table {
	if valueWaitTimeout <= 0 {
		valueWaitTimeout = 3 * time.Second
	}
	return &Table{
		relay:            relay,
		maxTotal:         maxTotal,
		valueWaitTimeout: valueWaitTimeout,
		live:             map[string]map[string]routesync.MmdsEndpointEntry{},
		waiters:          map[string]chan struct{}{},
	}
}

// --- routesync.MmdsSink ---

// BeginMmdsSync starts a fresh full-generation scan: the table is marked
// unavailable (every request 503s) and a new staging area opens. Entries
// applied before the matching MmdsBookmark land in staging, not live — the
// swap only happens atomically at the bookmark.
func (t *Table) BeginMmdsSync(generation string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.available = false
	t.stagingGen = generation
	t.staging = map[string]map[string]routesync.MmdsEndpointEntry{}
}

// ApplyMmdsUpsert applies one endpoint's state to whichever table (staging
// mid-generation, live otherwise) is currently active. A lower revision
// than what's already recorded is ignored; an equal revision
// is idempotent only if the payload is byte-identical, otherwise it's a
// protocol error that forces a resync (the table is marked unavailable and
// cleared — the next BeginMmdsSync/Bookmark pair recovers it).
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
		if e.Revision == existing.Revision && existing != e {
			t.forceResyncLocked()
			t.mu.Unlock()
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
	if live {
		t.notify(e.SandboxID, e.Name)
	}
}

// ApplyMmdsDelete removes one endpoint from whichever table is active.
func (t *Table) ApplyMmdsDelete(k routesync.MmdsEndpointKey) {
	t.mu.Lock()
	target := t.live
	if t.staging != nil {
		target = t.staging
	}
	if byName, ok := target[k.SandboxID]; ok {
		delete(byName, k.Name)
		if len(byName) == 0 {
			delete(target, k.SandboxID)
		}
	}
	live := t.staging == nil
	t.mu.Unlock()
	if live {
		t.notify(k.SandboxID, k.Name)
	}
}

// MmdsBookmark completes generation's scan: if it matches the in-progress
// staging generation, the staged set atomically becomes live and the table
// becomes available. A stale/mismatched generation (e.g. a race with a
// forced resync) is dropped rather than applied.
func (t *Table) MmdsBookmark(generation string) {
	t.mu.Lock()
	if t.staging != nil && t.stagingGen == generation {
		t.live = t.staging
		t.staging = nil
		t.stagingGen = ""
		t.available = true
	}
	t.mu.Unlock()
	t.notifyAll() // every waiter re-checks now that the table may be available
}

// forceResyncLocked handles an equal-revision-mismatch protocol error:
// clears both live and any in-progress staging and marks the table
// unavailable, so every request 503s until the next full generation
// completes. Caller must hold t.mu.
func (t *Table) forceResyncLocked() {
	t.available = false
	t.live = map[string]map[string]routesync.MmdsEndpointEntry{}
	t.staging = nil
	t.stagingGen = ""
}

func (t *Table) countLocked(m map[string]map[string]routesync.MmdsEndpointEntry) int {
	n := 0
	for _, byName := range m {
		n += len(byName)
	}
	return n
}

// --- internal/mmds.EndpointAuthority-compatible dispatch ---

// Lookup resolves the exact (sandbox_id, path) to its declared name+backend
// type. found=false when the table is unavailable or no endpoint owns that
// path.
func (t *Table) Lookup(_ context.Context, sandboxID, path string) (name, backendType string, found bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if !t.available {
		return "", "", false
	}
	for n, e := range t.live[sandboxID] {
		if e.Path == path {
			return n, e.BackendType, true
		}
	}
	return "", "", false
}

// ServeStore answers a store-backend GET, applying the same bounded
// never-configured wait as internal/mmdsauth.Authority.ServeStore.
func (t *Table) ServeStore(ctx context.Context, sandboxID, name string) (value []byte, contentType string, revision int64, present bool) {
	e, ok := t.get(sandboxID, name)
	if !ok {
		return nil, "", 0, false
	}
	if e.Revision == 0 && !e.ValuePresent {
		if !t.wait(ctx, sandboxID, name) {
			return nil, "", 0, false
		}
		if e, ok = t.get(sandboxID, name); !ok {
			return nil, "", 0, false
		}
	}
	if !e.ValuePresent {
		return nil, "", e.Revision, false
	}
	if e.ExpiresUnix > 0 && time.Now().Unix() >= e.ExpiresUnix {
		return nil, "", e.Revision, false
	}
	return []byte(e.SecretPlaintext), e.ContentType, e.Revision, true
}

// ServeRelay answers a relay-backend GET, applying the same bounded
// never-configured wait and cancellation-on-change as
// internal/mmdsauth.Authority.ServeRelay.
func (t *Table) ServeRelay(ctx context.Context, sandboxID, name string) (status int, contentType string, body []byte, ok bool) {
	if t.relay == nil {
		return http.StatusServiceUnavailable, "", nil, true
	}
	e, found := t.get(sandboxID, name)
	if !found {
		return 0, "", nil, false
	}
	if e.Revision == 0 && !e.ValuePresent {
		if !t.wait(ctx, sandboxID, name) {
			return 0, "", nil, false
		}
		if e, found = t.get(sandboxID, name); !found {
			return 0, "", nil, false
		}
	}
	if !e.ValuePresent {
		return 0, "", nil, false
	}

	var cfg relayPublicConfig
	if err := json.Unmarshal([]byte(e.PublicConfigJSON), &cfg); err != nil || cfg.URL == "" || cfg.AuthHeaderName == "" {
		return http.StatusBadGateway, "", nil, true
	}

	watchCtx, cancel := t.watch(ctx, sandboxID, name)
	defer cancel()
	res := t.relay.Fetch(watchCtx, sandboxID+"\x00"+name, cfg.URL, cfg.AuthHeaderName, e.SecretPlaintext)
	return res.Status, res.ContentType, res.Body, true
}

// get returns the current entry for (sandboxID,name), or ok=false if the
// table is unavailable or the endpoint is unknown.
func (t *Table) get(sandboxID, name string) (routesync.MmdsEndpointEntry, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if !t.available {
		return routesync.MmdsEndpointEntry{}, false
	}
	e, ok := t.live[sandboxID][name]
	return e, ok
}
