package store

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestKeyLeaseExactIdentityExpiryAndRotation(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	authKey := strings.Repeat("a", 64)
	manifestOne := strings.Repeat("b", 64)
	manifestTwo := strings.Repeat("c", 64)
	expires := time.Now().Add(time.Hour).Unix()

	added, err := st.PutKeyLease(ctx, KeyLease{
		Group: "/g", AuthKey: authKey, ManifestKey: manifestOne,
		RegistryAuth: `{"auths":{"*":{"token":"secret"}}}`, ExpiresUnix: expires,
	})
	if err != nil || !added {
		t.Fatalf("PutKeyLease = %t, %v", added, err)
	}
	authFP, _ := AuthKeyHash(authKey)
	manifestOneFP, _ := ManifestKeyHash(manifestOne)
	lease, found, err := st.KeyLeaseByFingerprints(ctx, "/g", authFP, manifestOneFP)
	if err != nil || !found || lease.AuthKey != authKey || lease.ManifestKey != manifestOne || lease.RegistryAuth == "" {
		t.Fatalf("resolved lease = %+v, %t, %v", lease, found, err)
	}

	if _, err := st.PutKeyLease(ctx, KeyLease{
		Group: "/g", AuthKey: authKey, ManifestKey: manifestTwo, ExpiresUnix: expires,
	}); err != nil {
		t.Fatal(err)
	}
	manifestTwoFP, _ := ManifestKeyHash(manifestTwo)
	if _, err := st.DropKeyLeaseRef(ctx, "/g", authFP, manifestOneFP); err != nil {
		t.Fatal(err)
	}
	if _, found, _ := st.KeyLeaseByFingerprints(ctx, "/g", authFP, manifestOneFP); found {
		t.Fatal("exact drop retained the old lease")
	}
	if _, found, _ := st.KeyLeaseByFingerprints(ctx, "/g", authFP, manifestTwoFP); !found {
		t.Fatal("stale exact drop removed the rotated lease")
	}

	if _, err := st.db.ExecContext(ctx, `UPDATE key_leases SET expires_unix=?`, time.Now().Unix()-1); err != nil {
		t.Fatal(err)
	}
	if _, found, _ := st.KeyLeaseByFingerprints(ctx, "/g", authFP, manifestTwoFP); found {
		t.Fatal("expired lease remained usable for new admission")
	}
	if count, err := st.PruneExpiredKeyLeases(ctx); err != nil || count != 1 {
		t.Fatalf("PruneExpiredKeyLeases = %d, %v", count, err)
	}
}

func TestKeyLeaseRequiresIndependentKeyDomains(t *testing.T) {
	st := testStore(t)
	key := strings.Repeat("d", 64)
	if _, err := st.PutKeyLease(context.Background(), KeyLease{
		Group: "/g", AuthKey: key, ManifestKey: key, ExpiresUnix: time.Now().Add(time.Hour).Unix(),
	}); err == nil {
		t.Fatal("shared AuthKey/ManifestKey material was accepted")
	}
}
