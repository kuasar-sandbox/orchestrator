package store

import (
	"context"
	"encoding/base64"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// testSandboxForMMDSSecrets inserts a minimal bare-profile sandbox row (the
// mmds_secrets table's FK requires an existing sandboxes row) and returns its ID.
func testSandboxForMMDSSecrets(t *testing.T, s *Store, id string) {
	t.Helper()
	sb := &types.Sandbox{
		ID:          id,
		Profile:     types.ProfileBare,
		TemplateID:  "bare-img-" + strings.Repeat("a", 64),
		State:       types.StateRunning,
		RunDir:      "/run/" + id,
		BaseDir:     "/base/" + id,
		APISecret:   strings.Repeat("1", 64),
		ManifestKey: strings.Repeat("2", 64),
		CreatedUnix: 1000,
	}
	setTestSandboxServiceCredentials(sb)
	if err := s.Put(context.Background(), sb); err != nil {
		t.Fatalf("seed sandbox: %v", err)
	}
}

func TestSetMMDSSecretRoundTrip(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	testSandboxForMMDSSecrets(t, s, "sbx-1")

	digest := "digest-v1"
	rev, err := s.SetMMDSSecret(ctx, "sbx-1", digest, "key1", []byte("hello"), "text/plain", 0)
	if err != nil {
		t.Fatal(err)
	}
	if rev != 1 {
		t.Fatalf("revision = %d, want 1", rev)
	}
	v, present, gotRev, err := s.GetMMDSSecretValue(ctx, "sbx-1", "key1")
	if err != nil {
		t.Fatal(err)
	}
	if !present || gotRev != 1 {
		t.Fatalf("present=%t revision=%d, want true/1", present, gotRev)
	}
	if v.ContentType != "text/plain" {
		t.Fatalf("content type = %q", v.ContentType)
	}
	decoded, err := base64.StdEncoding.DecodeString(v.BodyBase64)
	if err != nil || string(decoded) != "hello" {
		t.Fatalf("body = %q (err=%v), want %q", decoded, err, "hello")
	}
}

func TestSetMMDSSecretDefaultsContentType(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	testSandboxForMMDSSecrets(t, s, "sbx-1")

	if _, err := s.SetMMDSSecret(ctx, "sbx-1", "digest", "key1", []byte("x"), "", 0); err != nil {
		t.Fatal(err)
	}
	v, present, _, err := s.GetMMDSSecretValue(ctx, "sbx-1", "key1")
	if err != nil || !present {
		t.Fatalf("present=%t err=%v", present, err)
	}
	if v.ContentType != "application/octet-stream" {
		t.Fatalf("content type = %q, want default", v.ContentType)
	}
}

func TestClearMMDSSecretIncrementsRevisionKeepsOtherNames(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	testSandboxForMMDSSecrets(t, s, "sbx-1")

	if _, err := s.SetMMDSSecret(ctx, "sbx-1", "digest", "key1", []byte("a"), "", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SetMMDSSecret(ctx, "sbx-1", "digest", "key2", []byte("b"), "", 0); err != nil {
		t.Fatal(err)
	}
	rev, err := s.ClearMMDSSecret(ctx, "sbx-1", "digest", "key1")
	if err != nil {
		t.Fatal(err)
	}
	if rev != 3 { // 2 sets + 1 clear
		t.Fatalf("revision after clear = %d, want 3", rev)
	}
	if _, present, _, _ := s.GetMMDSSecretValue(ctx, "sbx-1", "key1"); present {
		t.Fatal("key1 should be cleared")
	}
	if _, present, _, _ := s.GetMMDSSecretValue(ctx, "sbx-1", "key2"); !present {
		t.Fatal("key2 should be untouched")
	}
}

func TestClearMMDSSecretIsIdempotentNoOp(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	testSandboxForMMDSSecrets(t, s, "sbx-1")

	// No row at all yet.
	rev, err := s.ClearMMDSSecret(ctx, "sbx-1", "digest", "key1")
	if err != nil {
		t.Fatal(err)
	}
	if rev != 0 {
		t.Fatalf("revision = %d, want 0 (no-op)", rev)
	}

	// A row exists (from a different name) but not this name.
	if _, err := s.SetMMDSSecret(ctx, "sbx-1", "digest", "key2", []byte("b"), "", 0); err != nil {
		t.Fatal(err)
	}
	rev, err = s.ClearMMDSSecret(ctx, "sbx-1", "digest", "key1")
	if err != nil {
		t.Fatal(err)
	}
	if rev != 1 { // unchanged from the one Set above, not bumped
		t.Fatalf("revision = %d, want 1 (unchanged no-op)", rev)
	}
}

func TestGetMMDSSecretValueDistinguishesNeverConfiguredFromRevoked(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	testSandboxForMMDSSecrets(t, s, "sbx-1")

	// Never configured: no row at all.
	_, present, rev, err := s.GetMMDSSecretValue(ctx, "sbx-1", "key1")
	if err != nil {
		t.Fatal(err)
	}
	if present || rev != 0 {
		t.Fatalf("never-configured: present=%t revision=%d, want false/0", present, rev)
	}

	// Configure then revoke: revision>0, present=false.
	if _, err := s.SetMMDSSecret(ctx, "sbx-1", "digest", "key1", []byte("a"), "", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ClearMMDSSecret(ctx, "sbx-1", "digest", "key1"); err != nil {
		t.Fatal(err)
	}
	_, present, rev, err = s.GetMMDSSecretValue(ctx, "sbx-1", "key1")
	if err != nil {
		t.Fatal(err)
	}
	if present {
		t.Fatal("expected the revoked secret to be absent")
	}
	if rev == 0 {
		t.Fatal("expected a nonzero revision distinguishing revoked from never-configured")
	}
}

func TestGetMMDSSecretValueExpiredTreatedAsAbsent(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	testSandboxForMMDSSecrets(t, s, "sbx-1")

	past := time.Now().Add(-time.Hour).Unix()
	rev, err := s.SetMMDSSecret(ctx, "sbx-1", "digest", "key1", []byte("stale"), "text/plain", past)
	if err != nil {
		t.Fatal(err)
	}

	v, present, gotRev, err := s.GetMMDSSecretValue(ctx, "sbx-1", "key1")
	if err != nil {
		t.Fatal(err)
	}
	if present {
		t.Fatalf("expected an expired secret to be reported absent, got value=%+v", v)
	}
	if gotRev != rev {
		t.Fatalf("revision = %d, want %d (the row's real revision, distinguishing this from never-configured)", gotRev, rev)
	}
}

func TestGetMMDSSecretValueNotYetExpiredIsPresent(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	testSandboxForMMDSSecrets(t, s, "sbx-1")

	future := time.Now().Add(time.Hour).Unix()
	if _, err := s.SetMMDSSecret(ctx, "sbx-1", "digest", "key1", []byte("fresh"), "text/plain", future); err != nil {
		t.Fatal(err)
	}

	v, present, _, err := s.GetMMDSSecretValue(ctx, "sbx-1", "key1")
	if err != nil {
		t.Fatal(err)
	}
	if !present {
		t.Fatal("expected a not-yet-expired secret to be present")
	}
	decoded, err := base64.StdEncoding.DecodeString(v.BodyBase64)
	if err != nil || string(decoded) != "fresh" {
		t.Fatalf("body = %q (err=%v), want %q", decoded, err, "fresh")
	}
}

func TestGetMMDSSecretBlobMatchesRouteEntryShape(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	testSandboxForMMDSSecrets(t, s, "sbx-1")

	if _, found, _ := mustGetBlob(t, s, "sbx-1"); found {
		t.Fatal("expected no blob before any secret is set")
	}
	if _, err := s.SetMMDSSecret(ctx, "sbx-1", "digest", "key1", []byte("hello"), "text/plain", 0); err != nil {
		t.Fatal(err)
	}
	blob, found, rev := mustGetBlob(t, s, "sbx-1")
	if !found || rev != 1 {
		t.Fatalf("found=%t rev=%d, want true/1", found, rev)
	}
	if !strings.Contains(blob, `"key1"`) || !strings.Contains(blob, `"version":1`) {
		t.Fatalf("unexpected blob shape: %s", blob)
	}
}

func mustGetBlob(t *testing.T, s *Store, sid string) (string, bool, int64) {
	t.Helper()
	blob, rev, found, err := s.GetMMDSSecretBlob(context.Background(), sid)
	if err != nil {
		t.Fatal(err)
	}
	return blob, found, rev
}

func TestSandboxDeleteCascadesMMDSSecretsRow(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	testSandboxForMMDSSecrets(t, s, "sbx-1")

	if _, err := s.SetMMDSSecret(ctx, "sbx-1", "digest", "key1", []byte("a"), "", 0); err != nil {
		t.Fatal(err)
	}
	if _, found, _ := mustGetBlob(t, s, "sbx-1"); !found {
		t.Fatal("expected the blob to exist before delete")
	}
	if err := s.Delete(ctx, "sbx-1"); err != nil {
		t.Fatal(err)
	}
	if _, found, _ := mustGetBlob(t, s, "sbx-1"); found {
		t.Fatal("expected ON DELETE CASCADE to remove the mmds_secrets row")
	}
}

func TestSetMMDSSecretCASRetriesUnderContention(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	testSandboxForMMDSSecrets(t, s, "sbx-1")

	const n = 16
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := s.SetMMDSSecret(ctx, "sbx-1", "digest", "key1", []byte{byte(i)}, "", 0)
			errs[i] = err
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("goroutine %d: %v", i, err)
		}
	}
	_, present, rev, err := s.GetMMDSSecretValue(ctx, "sbx-1", "key1")
	if err != nil {
		t.Fatal(err)
	}
	if !present || rev != n {
		t.Fatalf("present=%t revision=%d, want true/%d (every concurrent write landed)", present, rev, n)
	}
}
