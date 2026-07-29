package proxyshm

import "testing"

func TestMMDSSecretsUpsertAndGet(t *testing.T) {
	m := NewMMDSSecrets()
	m.Upsert("sbx-1", `{"version":1,"values":{}}`)
	got, ok := m.Get("sbx-1")
	if !ok || got != `{"version":1,"values":{}}` {
		t.Fatalf("Get() = %q, %t", got, ok)
	}
	if _, ok := m.Get("sbx-2"); ok {
		t.Fatal("expected sbx-2 to be absent")
	}
}

func TestMMDSSecretsUpsertEmptyClears(t *testing.T) {
	m := NewMMDSSecrets()
	m.Upsert("sbx-1", `{"version":1,"values":{}}`)
	m.Upsert("sbx-1", "") // a route entry with no MMDS secrets (or an ungated subscriber)
	if _, ok := m.Get("sbx-1"); ok {
		t.Fatal("expected an empty upsert to clear the entry")
	}
}

func TestMMDSSecretsDelete(t *testing.T) {
	m := NewMMDSSecrets()
	m.Upsert("sbx-1", `{"version":1,"values":{}}`)
	m.Delete("sbx-1")
	if _, ok := m.Get("sbx-1"); ok {
		t.Fatal("expected the entry to be deleted")
	}
}

func TestMMDSSecretsBookmarkDropsUnaffirmedEntries(t *testing.T) {
	m := NewMMDSSecrets()
	m.BeginSync()
	m.Upsert("sbx-1", `{"version":1,"values":{}}`)
	m.Upsert("sbx-2", `{"version":1,"values":{}}`)
	m.Bookmark()

	// Reconnect: a fresh sync generation only re-affirms sbx-1 (sbx-2 was
	// deleted, or this subscriber lost its MMDSSecrets gate, while disconnected).
	m.BeginSync()
	m.Upsert("sbx-1", `{"version":1,"values":{}}`)
	m.Bookmark()

	if _, ok := m.Get("sbx-1"); !ok {
		t.Fatal("expected sbx-1 (re-affirmed) to remain")
	}
	if _, ok := m.Get("sbx-2"); ok {
		t.Fatal("expected sbx-2 (not re-affirmed) to be dropped at Bookmark")
	}
}

func TestMMDSSecretsSyncedLifecycle(t *testing.T) {
	m := NewMMDSSecrets()
	if m.Synced() {
		t.Fatal("a freshly constructed store must start unsynced")
	}

	m.BeginSync()
	if m.Synced() {
		t.Fatal("BeginSync must not mark the store synced")
	}
	m.Upsert("sbx-1", `{"version":1,"values":{}}`)
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

// TestMMDSSecretsBeginSyncEagerlyClearsHeldPlaintext proves BeginSync doesn't
// merely mark existing entries stale for pruning at the *next* Bookmark (as
// MMDSRoutes does) -- it discards them immediately, so a slow or failed
// resync doesn't leave stale secret plaintext resident and Get-able for the
// whole resync duration.
func TestMMDSSecretsBeginSyncEagerlyClearsHeldPlaintext(t *testing.T) {
	m := NewMMDSSecrets()
	m.BeginSync()
	m.Upsert("sbx-1", `{"version":1,"values":{"key1":{"body_base64":"c2gtc2g="}}}`)
	m.Bookmark()
	if _, ok := m.Get("sbx-1"); !ok {
		t.Fatal("expected sbx-1 to be present after the first sync completes")
	}

	m.BeginSync() // reconnect/resync begins; not yet re-affirmed
	if _, ok := m.Get("sbx-1"); ok {
		t.Fatal("expected BeginSync to have already discarded sbx-1's plaintext, not just marked it stale")
	}
}
