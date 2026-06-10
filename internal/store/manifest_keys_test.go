package store

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/secretbox"
)

func testStore(t *testing.T) *Store {
	t.Helper()
	box, err := secretbox.NewFromColonHex(strings.Repeat("0", 64))
	if err != nil {
		t.Fatal(err)
	}
	st, err := Open(filepath.Join(t.TempDir(), "test.db"), box)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

// TestManifestKeyTTL exercises the expiry lifecycle: add (never) → allowed; re-add
// with a ttl refreshes (one row, expiry set); a past-expiry row is excluded from the
// allowlist but still listed until PruneExpiredManifestKeys deletes it.
func TestManifestKeyTTL(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	mk := strings.Repeat("1", 64)
	hash, err := ManifestKeyHash(mk)
	if err != nil {
		t.Fatal(err)
	}

	// add with no ttl => never expires, allowed.
	added, err := st.AddManifestKey(ctx, mk, "k", 0, "")
	if err != nil || !added {
		t.Fatalf("add: added=%v err=%v", added, err)
	}
	if ok, _ := st.HasManifestKey(ctx, mk); !ok {
		t.Fatal("key should be allowed right after add")
	}

	// re-add with a ttl => refresh (added=false), still one row, expiry now set.
	added, err = st.AddManifestKey(ctx, mk, "", 3600, "")
	if err != nil || added {
		t.Fatalf("re-add should refresh (added=false): added=%v err=%v", added, err)
	}
	infos, _ := st.ListManifestKeys(ctx)
	if len(infos) != 1 {
		t.Fatalf("want 1 row after re-add, got %d", len(infos))
	}
	if infos[0].ExpiresUnix == 0 {
		t.Fatal("re-add with ttl should set an expiry")
	}
	if infos[0].Label != "k" {
		t.Fatalf("re-add without --label should keep the old label, got %q", infos[0].Label)
	}

	// force the row into the past => excluded from the allowlist, still listed.
	if _, err := st.db.ExecContext(ctx, `UPDATE manifest_keys SET expires_unix=? WHERE key_hash=?`, time.Now().Unix()-10, hash); err != nil {
		t.Fatal(err)
	}
	if ok, _ := st.HasManifestKey(ctx, mk); ok {
		t.Fatal("expired key must not be allowed")
	}
	if allowed, _ := st.AllowedManifestKeysByHash(ctx, hash); len(allowed) != 0 {
		t.Fatal("expired key must be excluded from the allowlist")
	}
	if infos, _ := st.ListManifestKeys(ctx); len(infos) != 1 {
		t.Fatal("expired row should still be listed until pruned")
	}

	// prune deletes the expired row.
	n, err := st.PruneExpiredManifestKeys(ctx)
	if err != nil || n != 1 {
		t.Fatalf("prune: n=%d err=%v", n, err)
	}
	if infos, _ := st.ListManifestKeys(ctx); len(infos) != 0 {
		t.Fatal("pruned row should be gone")
	}
}

// TestManifestKeyRegistryAuth: tenant-default registry auth stores encrypted, is
// retrievable, and re-add updates it only when a new one is given.
func TestManifestKeyRegistryAuth(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	mk := strings.Repeat("2", 64)

	if a, _ := st.RegistryAuthForKey(ctx, mk); a != "" {
		t.Fatalf("expected no auth initially, got %q", a)
	}
	auth1 := `{"auths":{"*":{"username":"u","password":"p"}}}`
	if _, err := st.AddManifestKey(ctx, mk, "k", 0, auth1); err != nil {
		t.Fatal(err)
	}
	if a, _ := st.RegistryAuthForKey(ctx, mk); a != auth1 {
		t.Fatalf("auth1 not stored: %q", a)
	}
	// re-add with NO auth keeps the existing one (only refreshes expiry).
	if _, err := st.AddManifestKey(ctx, mk, "", 3600, ""); err != nil {
		t.Fatal(err)
	}
	if a, _ := st.RegistryAuthForKey(ctx, mk); a != auth1 {
		t.Fatalf("re-add without auth should keep it: %q", a)
	}
	// re-add with a new auth updates it.
	auth2 := `{"auths":{"*":{"token":"bearer"}}}`
	if _, err := st.AddManifestKey(ctx, mk, "", 0, auth2); err != nil {
		t.Fatal(err)
	}
	if a, _ := st.RegistryAuthForKey(ctx, mk); a != auth2 {
		t.Fatalf("re-add with auth should update: %q", a)
	}
}
