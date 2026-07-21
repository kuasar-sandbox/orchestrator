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

func TestKeyLeaseRefreshCannotShortenDurableExpiry(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	authKey := strings.Repeat("a", 64)
	manifestKey := strings.Repeat("b", 64)
	longer := time.Now().Add(2 * time.Hour).Unix()
	lease := KeyLease{
		Group: "/g", AuthKey: authKey, ManifestKey: manifestKey, ExpiresUnix: longer,
	}
	if _, err := st.PutKeyLease(ctx, lease); err != nil {
		t.Fatal(err)
	}
	lease.ExpiresUnix = longer - int64(time.Hour/time.Second)
	if _, err := st.PutKeyLease(ctx, lease); !errors.Is(err, ErrKeyLeaseExpiryRegression) {
		t.Fatalf("shortening refresh error = %v", err)
	}
	authFP, _ := AuthKeyHash(authKey)
	manifestFP, _ := ManifestKeyHash(manifestKey)
	stored, found, err := st.KeyLeaseByFingerprints(ctx, "/g", authFP, manifestFP)
	if err != nil || !found || stored.ExpiresUnix != longer {
		t.Fatalf("durable lease = %+v found=%v err=%v", stored, found, err)
	}
	lease.ExpiresUnix = 0
	if _, err := st.PutKeyLease(ctx, lease); err != nil {
		t.Fatalf("permanent refresh: %v", err)
	}
	lease.ExpiresUnix = longer + int64(time.Hour/time.Second)
	if _, err := st.PutKeyLease(ctx, lease); !errors.Is(err, ErrKeyLeaseExpiryRegression) {
		t.Fatalf("permanent lease was shortened: %v", err)
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
