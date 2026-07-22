package store

import (
	"context"
	"errors"
	"strings"
	"sync"
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
		Group: "/g", AuthKey: authKey, ManifestKey: manifestOne, ExpiresUnix: expires,
	}); err != nil {
		t.Fatal(err)
	}
	lease, found, err = st.KeyLeaseByFingerprints(ctx, "/g", authFP, manifestOneFP)
	if err != nil || !found || lease.RegistryAuth == "" {
		t.Fatalf("refresh erased registry auth: %+v, %t, %v", lease, found, err)
	}

	if _, err := st.PutKeyLease(ctx, KeyLease{
		Group: "/g", AuthKey: authKey, ManifestKey: manifestTwo, ExpiresUnix: expires,
	}); err != nil {
		t.Fatal(err)
	}
	manifestTwoFP, _ := ManifestKeyHash(manifestTwo)
	if _, err := st.DropKeyLeaseRef(ctx, "/g", authFP, manifestOneFP, 0, ""); err != nil {
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

func TestKeyLeaseRefreshIsAtomicWithExpiryPruning(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	authKey := strings.Repeat("e", 64)
	manifestKey := strings.Repeat("f", 64)
	if _, err := st.PutKeyLease(ctx, KeyLease{
		Group: "/race", AuthKey: authKey, ManifestKey: manifestKey, ExpiresUnix: time.Now().Unix() - 1,
	}); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errCh := make(chan error, 2)
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, err := st.PutKeyLease(ctx, KeyLease{
			Group: "/race", AuthKey: authKey, ManifestKey: manifestKey, ExpiresUnix: time.Now().Add(time.Hour).Unix(),
		})
		errCh <- err
	}()
	go func() {
		defer wg.Done()
		_, err := st.PruneExpiredKeyLeases(ctx)
		errCh <- err
	}()
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
	authFP, _ := AuthKeyHash(authKey)
	manifestFP, _ := ManifestKeyHash(manifestKey)
	if _, found, err := st.KeyLeaseByFingerprints(ctx, "/race", authFP, manifestFP); err != nil || !found {
		t.Fatalf("atomic refresh lost the lease: found=%v err=%v", found, err)
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

func TestClusterKeyLeaseRevisionAndExactDropAreDurable(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	authKey := strings.Repeat("a", 64)
	manifestKey := strings.Repeat("b", 64)
	authFP, _ := AuthKeyHash(authKey)
	manifestFP, _ := ManifestKeyHash(manifestKey)
	now := time.Now().Unix()
	currentDigest := strings.Repeat("2", 64)
	current := KeyLease{
		Group: "/g", AuthKey: authKey, ManifestKey: manifestKey,
		KeyRevision: 2, RegistryAuth: "current-auth", RegistryAuthDigest: currentDigest,
		ExpiresUnix: now + 600,
	}
	if added, err := st.PutKeyLease(ctx, current); err != nil || !added {
		t.Fatalf("put current lease = %t, %v", added, err)
	}

	stale := current
	stale.KeyRevision = 1
	stale.RegistryAuth = "stale-auth"
	stale.RegistryAuthDigest = strings.Repeat("1", 64)
	stale.ExpiresUnix = now + 1200
	if _, err := st.PutKeyLease(ctx, stale); !errors.Is(err, ErrKeyLeaseRevisionRegression) {
		t.Fatalf("stale lease error = %v", err)
	}
	conflict := current
	conflict.RegistryAuth = "conflicting-auth"
	conflict.RegistryAuthDigest = strings.Repeat("3", 64)
	if _, err := st.PutKeyLease(ctx, conflict); !errors.Is(err, ErrKeyLeaseRevisionConflict) {
		t.Fatalf("equal-revision conflict error = %v", err)
	}
	shorter := current
	shorter.ExpiresUnix--
	if _, err := st.PutKeyLease(ctx, shorter); !errors.Is(err, ErrKeyLeaseExpiryRegression) {
		t.Fatalf("expiry regression error = %v", err)
	}
	renewed := current
	renewed.ExpiresUnix += 600
	if added, err := st.PutKeyLease(ctx, renewed); err != nil || added {
		t.Fatalf("renew exact lease = %t, %v", added, err)
	}
	current = renewed

	stored, found, err := st.KeyLeaseByFingerprints(ctx, "/g", authFP, manifestFP)
	if err != nil || !found || stored.KeyRevision != current.KeyRevision ||
		stored.RegistryAuthDigest != current.RegistryAuthDigest || stored.RegistryAuth != current.RegistryAuth ||
		stored.ExpiresUnix != current.ExpiresUnix {
		t.Fatalf("stored lease = %+v, found=%t err=%v", stored, found, err)
	}
	if removed, err := st.DropKeyLeaseRef(
		ctx, "/g", authFP, manifestFP, stale.KeyRevision, stale.RegistryAuthDigest,
	); err != nil || removed {
		t.Fatalf("stale exact drop = %t, %v", removed, err)
	}
	if _, found, err := st.KeyLeaseByFingerprints(ctx, "/g", authFP, manifestFP); err != nil || !found {
		t.Fatalf("stale drop removed current lease: found=%t err=%v", found, err)
	}
	if removed, err := st.DropKeyLeaseRef(
		ctx, "/g", authFP, manifestFP, current.KeyRevision, current.RegistryAuthDigest,
	); err != nil || !removed {
		t.Fatalf("current exact drop = %t, %v", removed, err)
	}
}
