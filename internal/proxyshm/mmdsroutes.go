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
type MMDSRoutes struct {
	mu      sync.RWMutex
	byID    map[string]string
	syncGen map[string]uint64
	gen     uint64
}

// NewMMDSRoutes returns an empty store.
func NewMMDSRoutes() *MMDSRoutes {
	return &MMDSRoutes{byID: map[string]string{}, syncGen: map[string]uint64{}}
}

// BeginSync starts a fresh sync generation.
func (m *MMDSRoutes) BeginSync() {
	m.mu.Lock()
	m.gen++
	m.mu.Unlock()
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

// Bookmark drops every entry not re-affirmed since the last BeginSync.
func (m *MMDSRoutes) Bookmark() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for sid, gen := range m.syncGen {
		if gen != m.gen {
			delete(m.byID, sid)
			delete(m.syncGen, sid)
		}
	}
}

// Get returns sid's canonical specification JSON, if any.
func (m *MMDSRoutes) Get(sid string) (string, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	v, ok := m.byID[sid]
	return v, ok
}
