package proxyshm

import "sync"

// MMDSRoutes is the proxy master's sparse, ordinary-heap store of per-sandbox
// canonical kuasar-sandbox.mmds specifications (see internal/sandboxcfg.ExtractMMDS).
// Deliberately NOT part of the
// fixed-layout mmap Table: a specification is bounded but variable-length (up to
// mmds.routes.max_namespace_bytes, default 64KiB). This limit must leave
// headroom below the transport frame limit for the frame envelope and other
// protocol metadata. Most sandboxes specify
// none -- a fixed mmap field sized for the worst case would multiply by the
// table's record capacity regardless of use. Workers reach this store over
// internal/mmdsrpc (a separate inherited socketpair per worker), since they run
// as separate processes and cannot share this process's heap directly.
//
// Applies the same BeginSync/Bookmark mark-and-sweep generation discipline as
// Table (see Table.BeginSync/Bookmark): entries not re-affirmed by an Upsert
// since the last BeginSync are dropped at Bookmark, recovering deletions that
// happened while disconnected from the conductor.
//
// Unlike MMDSSecrets, BeginSync does NOT eagerly clear byID -- a stale
// declaration held here is tolerable for a "static" route (non-sensitive,
// immutable content) and Bookmark's own mark-and-sweep already recovers a
// stale entry's deletion once resync completes. synced instead exists so a
// caller resolving a "service" route (which, unlike static, drives a real
// dial to a local trusted service under the sandbox's identity) can choose to
// fail closed on a stale declaration without static routes losing
// availability across every proxy-master reconnect too -- see
// mmdsRPCHandler's per-route-type use of Synced().
type MMDSRoutes struct {
	mu      sync.RWMutex
	byID    map[string]string
	syncGen map[string]uint64
	gen     uint64
	synced  bool
}

// NewMMDSRoutes returns an empty, unsynced store.
func NewMMDSRoutes() *MMDSRoutes {
	return &MMDSRoutes{byID: map[string]string{}, syncGen: map[string]uint64{}}
}

// BeginSync starts a fresh sync generation and marks the store unsynced until
// the matching Bookmark completes.
func (m *MMDSRoutes) BeginSync() {
	m.mu.Lock()
	m.gen++
	m.synced = false
	m.mu.Unlock()
}

// Synced reports whether a full sync generation has completed since the most
// recent BeginSync (i.e. Bookmark has run and no new BeginSync has started
// since). False during the resync window following a master reconnect, or
// before the first sync ever completes.
func (m *MMDSRoutes) Synced() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.synced
}

// Upsert records sid's canonical specification, or clears it when canonical is
// empty (the common case -- most RouteEntry upserts carry no MMDS routes).
func (m *MMDSRoutes) Upsert(sid, canonical string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if canonical == "" {
		delete(m.byID, sid)
		delete(m.syncGen, sid)
		return
	}
	m.byID[sid] = canonical
	m.syncGen[sid] = m.gen
}

// Delete clears sid's specification.
func (m *MMDSRoutes) Delete(sid string) {
	m.mu.Lock()
	delete(m.byID, sid)
	delete(m.syncGen, sid)
	m.mu.Unlock()
}

// Bookmark drops every entry not re-affirmed since the last BeginSync, then
// marks the store synced.
func (m *MMDSRoutes) Bookmark() {
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

// Get returns sid's canonical specification JSON, if any.
func (m *MMDSRoutes) Get(sid string) (string, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	v, ok := m.byID[sid]
	return v, ok
}
