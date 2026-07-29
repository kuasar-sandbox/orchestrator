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
