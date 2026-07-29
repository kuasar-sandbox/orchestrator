package proxyshm

import "sync"

// MMDSSecrets is the proxy master's sparse, ordinary-heap store of per-sandbox
// decrypted MMDS secret-value blobs (RouteEntry.MMDSSecrets -- see
// internal/store.GetMMDSSecretBlob for the JSON shape). Structurally similar
// to MMDSRoutes (same BeginSync/Bookmark mark-and-sweep generation
// discipline, same off-mmap-table rationale: bounded but variable-length,
// most sandboxes carry none), kept as a separate store rather than folded
// into MMDSRoutes because this field carries real secret plaintext and is
// only ever populated for a subscriber registered with the gated
// MMDSSecrets capability (see routesync.Register.MMDSSecrets) -- an
// ungated subscriber's entries simply never reach Upsert, so this store
// stays empty rather than needing its own per-entry access check.
//
// Unlike MMDSRoutes, this store fails closed across a disconnect: BeginSync
// (called once per fresh sync session -- see routesync.Subscriber.session)
// eagerly clears every held blob and marks the store unsynced, rather than
// only pruning stale entries once the *next* Bookmark completes. A
// disconnected proxy would otherwise keep serving whatever secret plaintext
// it last held for as long as reconnection takes (routesync.Subscriber
// backs off up to 5s per attempt, and a slow full resync of many sandboxes
// can itself take longer than that) -- for routes/static content that's an
// acceptable availability tradeoff, but for live secret plaintext the
// confirmed #42 design requires failing closed instead: cmd/node-ctl/proxy.go's
// mmdsRPCHandler checks Synced() and reports the whole secret view
// unavailable (503, not a stale value or a false "never configured" 404)
// until a fresh BeginSync->Bookmark cycle completes.
type MMDSSecrets struct {
	mu      sync.RWMutex
	byID    map[string]string
	syncGen map[string]uint64
	gen     uint64
	synced  bool // true only once a full BeginSync->Bookmark cycle has completed
}

// NewMMDSSecrets returns an empty, unsynced store.
func NewMMDSSecrets() *MMDSSecrets {
	return &MMDSSecrets{byID: map[string]string{}, syncGen: map[string]uint64{}}
}

// BeginSync starts a fresh sync generation, eagerly discarding every
// currently held blob (best-effort plaintext hygiene: don't sit on secret
// plaintext for the full duration of a resync just because Bookmark hasn't
// pruned it yet) and marking the store unsynced until the matching Bookmark
// completes.
func (m *MMDSSecrets) BeginSync() {
	m.mu.Lock()
	m.gen++
	m.synced = false
	m.byID = map[string]string{}
	m.syncGen = map[string]uint64{}
	m.mu.Unlock()
}

// Synced reports whether a full BeginSync->Bookmark cycle has completed
// since the store was created or last began a new sync generation. false
// means the store's view of every sandbox's secrets is stale or absent by
// construction (not merely "this one sandbox hasn't been resynced yet") --
// callers (mmdsRPCHandler) must treat every lookup as unavailable, not as
// "never configured" or "revoked", both of which are guest-visible outcomes
// that require a trustworthy, fully synced view to assert.
func (m *MMDSSecrets) Synced() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.synced
}

// Upsert records sid's decrypted secret blob, or clears it when blob is empty
// (the common case -- most RouteEntry upserts carry no MMDS secrets, either
// because the sandbox never configured any or because this subscriber isn't
// gated to receive them).
func (m *MMDSSecrets) Upsert(sid, blob string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if blob == "" {
		delete(m.byID, sid)
		delete(m.syncGen, sid)
		return
	}
	m.byID[sid] = blob
	m.syncGen[sid] = m.gen
}

// Delete clears sid's secret blob.
func (m *MMDSSecrets) Delete(sid string) {
	m.mu.Lock()
	delete(m.byID, sid)
	delete(m.syncGen, sid)
	m.mu.Unlock()
}

// Bookmark drops every entry not re-affirmed since the last BeginSync and
// marks the store synced -- the point at which a fresh, trustworthy full
// snapshot is in place and Get results become guest-servable again.
func (m *MMDSSecrets) Bookmark() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for sid, gen := range m.syncGen {
		if gen != m.gen {
			delete(m.byID, sid)
			delete(m.syncGen, sid)
		}
	}
	m.synced = true
}

// Get returns sid's decrypted secret blob JSON, if any.
func (m *MMDSSecrets) Get(sid string) (string, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	v, ok := m.byID[sid]
	return v, ok
}
