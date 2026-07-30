package proxyshm

import "testing"

func TestMMDSRoutesUpsertAndGet(t *testing.T) {
	m := NewMMDSRoutes()
	m.Upsert("sbx-1", `{"version":1}`)
	got, ok := m.Get("sbx-1")
	if !ok || got != `{"version":1}` {
		t.Fatalf("Get() = %q, %t", got, ok)
	}
	if _, ok := m.Get("sbx-2"); ok {
		t.Fatal("expected sbx-2 to be absent")
	}
}

func TestMMDSRoutesUpsertEmptyClears(t *testing.T) {
	m := NewMMDSRoutes()
	m.Upsert("sbx-1", `{"version":1}`)
	m.Upsert("sbx-1", "") // a route entry with no MMDS specification
	if _, ok := m.Get("sbx-1"); ok {
		t.Fatal("expected an empty upsert to clear the entry")
	}
}

func TestMMDSRoutesDelete(t *testing.T) {
	m := NewMMDSRoutes()
	m.Upsert("sbx-1", `{"version":1}`)
	m.Delete("sbx-1")
	if _, ok := m.Get("sbx-1"); ok {
		t.Fatal("expected the entry to be deleted")
	}
}

func TestMMDSRoutesBookmarkDropsUnaffirmedEntries(t *testing.T) {
	m := NewMMDSRoutes()
	m.BeginSync()
	m.Upsert("sbx-1", `{"version":1}`)
	m.Upsert("sbx-2", `{"version":1}`)
	m.Bookmark()

	// Reconnect: a fresh sync generation only re-affirms sbx-1 (sbx-2 was
	// deleted while disconnected).
	m.BeginSync()
	m.Upsert("sbx-1", `{"version":1}`)
	m.Bookmark()

	if _, ok := m.Get("sbx-1"); !ok {
		t.Fatal("expected sbx-1 (re-affirmed) to remain")
	}
	if _, ok := m.Get("sbx-2"); ok {
		t.Fatal("expected sbx-2 (not re-affirmed) to be dropped at Bookmark")
	}
}

func TestMMDSRoutesSyncedLifecycle(t *testing.T) {
	m := NewMMDSRoutes()
	if m.Synced() {
		t.Fatal("a freshly constructed store must start unsynced")
	}

	m.BeginSync()
	if m.Synced() {
		t.Fatal("BeginSync must not mark the store synced")
	}
	m.Upsert("sbx-1", `{"version":1}`)
	if m.Synced() {
		t.Fatal("an in-flight Upsert must not mark the store synced -- only a completed Bookmark does")
	}
	m.Bookmark()
	if !m.Synced() {
		t.Fatal("expected the store to be synced once Bookmark completes")
	}

	// A disconnect (BeginSync of a fresh generation) must flip back to
	// unsynced immediately, not only once the *next* Bookmark completes.
	m.BeginSync()
	if m.Synced() {
		t.Fatal("expected BeginSync to immediately mark the store unsynced again")
	}
	m.Bookmark()
	if !m.Synced() {
		t.Fatal("expected the store to be synced again once the new Bookmark completes")
	}
}

// TestMMDSRoutesBeginSyncDoesNotEagerlyClear proves BeginSync only flips
// Synced() to false and leaves byID intact -- unlike MMDSSecrets, a stale
// static-route declaration remains servable through a resync window (see
// MMDSRoutes's doc comment); only the Synced() signal itself changes
// immediately.
func TestMMDSRoutesBeginSyncDoesNotEagerlyClear(t *testing.T) {
	m := NewMMDSRoutes()
	m.BeginSync()
	m.Upsert("sbx-1", `{"version":1}`)
	m.Bookmark()

	m.BeginSync() // reconnect/resync begins; not yet re-affirmed
	if _, ok := m.Get("sbx-1"); !ok {
		t.Fatal("expected sbx-1's declaration to remain servable through BeginSync, unlike MMDSSecrets")
	}
}
