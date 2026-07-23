package store

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/secretbox"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func testSandbox(id string) *types.Sandbox {
	return &types.Sandbox{
		ID:          id,
		TemplateID:  "e2b-img-" + strings.Repeat("1", 64),
		State:       types.StateRunning,
		RunDir:      "/run/sandbox/" + id,
		BaseDir:     "/var/lib/sandbox/" + id,
		ManifestKey: strings.Repeat("2", 64),
		CreatedUnix: time.Now().Unix(),
	}
}

func TestPutWithMMDSEndpointsAtomicInsert(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	sb := testSandbox("sb-atomic")

	err := st.PutWithMMDSEndpoints(ctx, sb, []MMDSEndpoint{
		{Name: "credentials", Path: "/latest/meta-data/credentials", BackendType: MMDSBackendRelay, PublicConfigJSON: `{"url":"https://example.com"}`},
		{Name: "user-data", Path: "/latest/user-data", BackendType: MMDSBackendStore},
	})
	if err != nil {
		t.Fatalf("PutWithMMDSEndpoints: %v", err)
	}

	got, err := st.Get(ctx, sb.ID)
	if err != nil || got == nil {
		t.Fatalf("Get sandbox: %v, got=%v", err, got)
	}
	statuses, err := st.ListMMDSEndpointStatus(ctx, sb.ID)
	if err != nil {
		t.Fatalf("ListMMDSEndpointStatus: %v", err)
	}
	if len(statuses) != 2 {
		t.Fatalf("got %d endpoint rows, want 2: %+v", len(statuses), statuses)
	}
	if statuses[0].Name != "credentials" || statuses[0].BackendType != MMDSBackendRelay || statuses[0].Configured {
		t.Fatalf("credentials status = %+v", statuses[0])
	}
	if statuses[1].Name != "user-data" || statuses[1].BackendType != MMDSBackendStore || statuses[1].Configured {
		t.Fatalf("user-data status = %+v", statuses[1])
	}
}

func TestPutWithMMDSEndpointsRollsBackOnFailure(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	sb := testSandbox("sb-rollback")

	// The second endpoint's path collides with the first (UNIQUE(sandbox_id,path)),
	// which must fail the whole transaction: neither the sandbox row nor the
	// first endpoint row may persist.
	err := st.PutWithMMDSEndpoints(ctx, sb, []MMDSEndpoint{
		{Name: "a", Path: "/latest/dup", BackendType: MMDSBackendStore},
		{Name: "b", Path: "/latest/dup", BackendType: MMDSBackendStore},
	})
	if err == nil {
		t.Fatal("PutWithMMDSEndpoints succeeded with a duplicate path, want an error")
	}

	if got, gerr := st.Get(ctx, sb.ID); gerr != nil || got != nil {
		t.Fatalf("sandbox row persisted after a rolled-back transaction: got=%+v err=%v", got, gerr)
	}
	statuses, lerr := st.ListMMDSEndpointStatus(ctx, sb.ID)
	if lerr != nil {
		t.Fatalf("ListMMDSEndpointStatus: %v", lerr)
	}
	if len(statuses) != 0 {
		t.Fatalf("endpoint rows persisted after a rolled-back transaction: %+v", statuses)
	}
}

func TestSandboxDeleteCascadesMMDSEndpoints(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	sb := testSandbox("sb-cascade")

	if err := st.PutWithMMDSEndpoints(ctx, sb, []MMDSEndpoint{
		{Name: "a", Path: "/latest/a", BackendType: MMDSBackendStore},
	}); err != nil {
		t.Fatalf("PutWithMMDSEndpoints: %v", err)
	}
	if err := st.Delete(ctx, sb.ID); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	statuses, err := st.ListMMDSEndpointStatus(ctx, sb.ID)
	if err != nil {
		t.Fatalf("ListMMDSEndpointStatus: %v", err)
	}
	if len(statuses) != 0 {
		t.Fatalf("endpoint rows survived sandbox deletion (FK cascade not enforced?): %+v", statuses)
	}
}

func TestPutWithMMDSEndpointsResumeLeavesExistingRowsUntouched(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	sb := testSandbox("sb-resume")

	if err := st.PutWithMMDSEndpoints(ctx, sb, []MMDSEndpoint{
		{Name: "a", Path: "/latest/a", BackendType: MMDSBackendStore},
	}); err != nil {
		t.Fatalf("initial PutWithMMDSEndpoints: %v", err)
	}
	if err := st.SetMMDSStoreValue(ctx, sb.ID, "a", []byte("v1"), "", 0); err != nil {
		t.Fatalf("SetMMDSStoreValue: %v", err)
	}

	// Resume: launch calls PutWithMMDSEndpoints again with no endpoints — the
	// existing row (now at revision 1, value_present=true) must be untouched,
	// not deleted-and-reinserted back to revision 0.
	sb.State = types.StatePaused // simulate a field changing on resume's upsert
	if err := st.PutWithMMDSEndpoints(ctx, sb, nil); err != nil {
		t.Fatalf("resume PutWithMMDSEndpoints: %v", err)
	}

	v, ok, err := st.GetMMDSStoreValue(ctx, sb.ID, "a")
	if err != nil || !ok {
		t.Fatalf("GetMMDSStoreValue after resume: ok=%t err=%v", ok, err)
	}
	if v.Revision != 1 || !v.Present || string(v.Value) != "v1" {
		t.Fatalf("endpoint value changed across resume: %+v", v)
	}
}

func TestSetAndClearMMDSStoreValue(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	sb := testSandbox("sb-store-value")
	if err := st.PutWithMMDSEndpoints(ctx, sb, []MMDSEndpoint{
		{Name: "a", Path: "/latest/a", BackendType: MMDSBackendStore},
	}); err != nil {
		t.Fatal(err)
	}

	if v, ok, err := st.GetMMDSStoreValue(ctx, sb.ID, "a"); err != nil || !ok || v.Present || v.Revision != 0 {
		t.Fatalf("never-configured state = %+v ok=%t err=%v", v, ok, err)
	}

	if err := st.SetMMDSStoreValue(ctx, sb.ID, "a", []byte("hello"), "text/plain", 0); err != nil {
		t.Fatalf("SetMMDSStoreValue: %v", err)
	}
	v, ok, err := st.GetMMDSStoreValue(ctx, sb.ID, "a")
	if err != nil || !ok || !v.Present || v.Revision != 1 || string(v.Value) != "hello" || v.ContentType != "text/plain" {
		t.Fatalf("configured state = %+v ok=%t err=%v", v, ok, err)
	}

	if err := st.ClearMMDSStoreValue(ctx, sb.ID, "a"); err != nil {
		t.Fatalf("ClearMMDSStoreValue: %v", err)
	}
	v, ok, err = st.GetMMDSStoreValue(ctx, sb.ID, "a")
	if err != nil || !ok || v.Present || v.Revision != 2 || len(v.Value) != 0 {
		t.Fatalf("deleted state = %+v ok=%t err=%v", v, ok, err)
	}
}

// TestSetMMDSStoreValuePersistenceFailureLeavesOldRevisionIntact covers the
// failure-semantics requirement that a store encryption/persistence failure
// fail the mutation and leave the old revision in place. mutateMMDSSecret only issues its
// UPDATE after successfully encrypting, and never applies a partial write,
// so any failure — simulated here by closing the store, the reliable way to
// force the UPDATE to fail — must leave the previously-committed
// value/revision completely unchanged.
func TestSetMMDSStoreValuePersistenceFailureLeavesOldRevisionIntact(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	box, err := secretbox.NewFromColonHex(strings.Repeat("0", 64))
	if err != nil {
		t.Fatal(err)
	}
	st, err := Open(path, box)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	sb := testSandbox("sb-persist-fail")
	if err := st.PutWithMMDSEndpoints(ctx, sb, []MMDSEndpoint{
		{Name: "a", Path: "/latest/a", BackendType: MMDSBackendStore},
	}); err != nil {
		t.Fatal(err)
	}
	if err := st.SetMMDSStoreValue(ctx, sb.ID, "a", []byte("hello"), "text/plain", 0); err != nil {
		t.Fatalf("SetMMDSStoreValue: %v", err)
	}

	st.Close() // force every subsequent DB operation on this handle to fail
	if err := st.SetMMDSStoreValue(ctx, sb.ID, "a", []byte("tampered"), "text/plain", 0); err == nil {
		t.Fatal("SetMMDSStoreValue on a closed store returned nil error, want a persistence failure")
	}

	// Reopen a fresh connection to the same on-disk file and confirm the
	// failed mutation left the previously-committed value/revision intact.
	st2, err := Open(path, box)
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	v, ok, err := st2.GetMMDSStoreValue(ctx, sb.ID, "a")
	if err != nil || !ok || !v.Present || v.Revision != 1 || string(v.Value) != "hello" {
		t.Fatalf("state after failed mutation = %+v ok=%t err=%v, want the original revision 1 value unchanged", v, ok, err)
	}
}

func TestSetMMDSStoreValueRejectsWrongBackendAndUnknownEndpoint(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	sb := testSandbox("sb-wrong-backend")
	if err := st.PutWithMMDSEndpoints(ctx, sb, []MMDSEndpoint{
		{Name: "relay-ep", Path: "/latest/relay", BackendType: MMDSBackendRelay},
	}); err != nil {
		t.Fatal(err)
	}

	if err := st.SetMMDSStoreValue(ctx, sb.ID, "relay-ep", []byte("x"), "", 0); !errors.Is(err, ErrMMDSEndpointWrongBackend) {
		t.Fatalf("SetMMDSStoreValue on a relay endpoint = %v, want ErrMMDSEndpointWrongBackend", err)
	}
	if err := st.SetMMDSRelayAuth(ctx, sb.ID, "relay-ep", []byte("secret")); err != nil {
		t.Fatalf("SetMMDSRelayAuth: %v", err)
	}
	if err := st.SetMMDSStoreValue(ctx, sb.ID, "does-not-exist", []byte("x"), "", 0); !errors.Is(err, ErrMMDSEndpointNotFound) {
		t.Fatalf("SetMMDSStoreValue on an unknown endpoint = %v, want ErrMMDSEndpointNotFound", err)
	}
}

func TestGetMMDSRelayAuthDecryptFailsUnderWrongAAD(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	sb := testSandbox("sb-aad-replay")
	if err := st.PutWithMMDSEndpoints(ctx, sb, []MMDSEndpoint{
		{Name: "a", Path: "/latest/a", BackendType: MMDSBackendRelay},
		{Name: "b", Path: "/latest/b", BackendType: MMDSBackendRelay},
	}); err != nil {
		t.Fatal(err)
	}
	if err := st.SetMMDSRelayAuth(ctx, sb.ID, "a", []byte("auth-a")); err != nil {
		t.Fatal(err)
	}
	if err := st.SetMMDSRelayAuth(ctx, sb.ID, "b", []byte("auth-b")); err != nil {
		t.Fatal(err)
	}

	// Swap the two rows' ciphertext directly (simulating a row-copy attack) and
	// confirm decryption fails rather than silently returning the wrong
	// endpoint's secret — this is what the AAD binding (record type + sandbox
	// id + name + backend type + revision) exists to prevent.
	var ctA, ctB string
	if err := st.db.QueryRowContext(ctx, `SELECT secret_ciphertext FROM sandbox_mmds_endpoints WHERE sandbox_id=? AND name='a'`, sb.ID).Scan(&ctA); err != nil {
		t.Fatal(err)
	}
	if err := st.db.QueryRowContext(ctx, `SELECT secret_ciphertext FROM sandbox_mmds_endpoints WHERE sandbox_id=? AND name='b'`, sb.ID).Scan(&ctB); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.ExecContext(ctx, `UPDATE sandbox_mmds_endpoints SET secret_ciphertext=? WHERE sandbox_id=? AND name='a'`, ctB, sb.ID); err != nil {
		t.Fatal(err)
	}

	if _, _, err := st.GetMMDSRelayAuth(ctx, sb.ID, "a"); err == nil {
		t.Fatal("GetMMDSRelayAuth decrypted a ciphertext swapped from a different endpoint, want an error")
	}
}

func TestListMMDSEndpointStatusReflectsExpiry(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	sb := testSandbox("sb-expiry")
	if err := st.PutWithMMDSEndpoints(ctx, sb, []MMDSEndpoint{
		{Name: "a", Path: "/latest/a", BackendType: MMDSBackendStore},
	}); err != nil {
		t.Fatal(err)
	}
	past := time.Now().Add(-time.Hour).Unix()
	if err := st.SetMMDSStoreValue(ctx, sb.ID, "a", []byte("v"), "", past); err != nil {
		t.Fatal(err)
	}
	statuses, err := st.ListMMDSEndpointStatus(ctx, sb.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(statuses) != 1 || !statuses[0].Expired || !statuses[0].Configured {
		t.Fatalf("status = %+v, want configured+expired", statuses)
	}
}

func TestGetMMDSEndpointPublicConfig(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	sb := testSandbox("sb-pubconfig")
	if err := st.PutWithMMDSEndpoints(ctx, sb, []MMDSEndpoint{
		{Name: "creds", Path: "/latest/meta-data/credentials", BackendType: MMDSBackendRelay, PublicConfigJSON: `{"url":"https://example.com","auth_header_name":"X-Auth"}`},
	}); err != nil {
		t.Fatal(err)
	}
	cfg, backend, found, err := st.GetMMDSEndpointPublicConfig(ctx, sb.ID, "creds")
	if err != nil || !found || backend != MMDSBackendRelay {
		t.Fatalf("GetMMDSEndpointPublicConfig = cfg=%q backend=%q found=%t err=%v", cfg, backend, found, err)
	}
	if !strings.Contains(cfg, "https://example.com") || !strings.Contains(cfg, "X-Auth") {
		t.Fatalf("public config = %q, want it to carry url + header name", cfg)
	}
	if _, _, found, err := st.GetMMDSEndpointPublicConfig(ctx, sb.ID, "nope"); err != nil || found {
		t.Fatalf("GetMMDSEndpointPublicConfig for an unknown endpoint: found=%t err=%v", found, err)
	}
}

func TestGetMMDSEndpointFull(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	sb := testSandbox("sb-full")
	if err := st.PutWithMMDSEndpoints(ctx, sb, []MMDSEndpoint{
		{Name: "creds", Path: "/latest/creds", BackendType: MMDSBackendRelay, PublicConfigJSON: `{"url":"https://example.com","auth_header_name":"X-Auth"}`},
	}); err != nil {
		t.Fatal(err)
	}

	e, found, err := st.GetMMDSEndpointFull(ctx, sb.ID, "creds")
	if err != nil || !found || e.ValuePresent || len(e.SecretPlaintext) != 0 {
		t.Fatalf("GetMMDSEndpointFull (never configured) = %+v found=%t err=%v", e, found, err)
	}
	if e.Path != "/latest/creds" || e.BackendType != MMDSBackendRelay || e.Revision != 0 {
		t.Fatalf("GetMMDSEndpointFull declaration fields = %+v", e)
	}

	if err := st.SetMMDSRelayAuth(ctx, sb.ID, "creds", []byte("secret-token")); err != nil {
		t.Fatal(err)
	}
	e, found, err = st.GetMMDSEndpointFull(ctx, sb.ID, "creds")
	if err != nil || !found || !e.ValuePresent || string(e.SecretPlaintext) != "secret-token" || e.Revision != 1 {
		t.Fatalf("GetMMDSEndpointFull (configured) = %+v found=%t err=%v", e, found, err)
	}

	if _, found, err := st.GetMMDSEndpointFull(ctx, sb.ID, "nope"); err != nil || found {
		t.Fatalf("GetMMDSEndpointFull for an unknown endpoint: found=%t err=%v", found, err)
	}
}

func TestRangeMMDSEndpoints(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	sb1 := testSandbox("sb-range-1")
	if err := st.PutWithMMDSEndpoints(ctx, sb1, []MMDSEndpoint{
		{Name: "a", Path: "/latest/a", BackendType: MMDSBackendStore},
	}); err != nil {
		t.Fatal(err)
	}
	sb2 := testSandbox("sb-range-2")
	if err := st.PutWithMMDSEndpoints(ctx, sb2, []MMDSEndpoint{
		{Name: "b", Path: "/latest/b", BackendType: MMDSBackendRelay},
	}); err != nil {
		t.Fatal(err)
	}
	if err := st.SetMMDSStoreValue(ctx, sb1.ID, "a", []byte("val-a"), "", 0); err != nil {
		t.Fatal(err)
	}

	var got []MMDSEndpointFull
	if err := st.RangeMMDSEndpoints(ctx, func(e MMDSEndpointFull) error {
		got = append(got, e)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d endpoints, want 2: %+v", len(got), got)
	}
	bySandbox := map[string]MMDSEndpointFull{}
	for _, e := range got {
		bySandbox[e.SandboxID] = e
	}
	if e := bySandbox[sb1.ID]; !e.ValuePresent || string(e.SecretPlaintext) != "val-a" {
		t.Fatalf("sb1 endpoint = %+v", e)
	}
	if e := bySandbox[sb2.ID]; e.ValuePresent || e.BackendType != MMDSBackendRelay {
		t.Fatalf("sb2 endpoint = %+v", e)
	}
}

func TestMMDSEndpointByPath(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	sb := testSandbox("sb-by-path")
	if err := st.PutWithMMDSEndpoints(ctx, sb, []MMDSEndpoint{
		{Name: "creds", Path: "/latest/meta-data/credentials", BackendType: MMDSBackendRelay},
	}); err != nil {
		t.Fatal(err)
	}
	name, backend, found, err := st.MMDSEndpointByPath(ctx, sb.ID, "/latest/meta-data/credentials")
	if err != nil || !found || name != "creds" || backend != MMDSBackendRelay {
		t.Fatalf("MMDSEndpointByPath = name=%q backend=%q found=%t err=%v", name, backend, found, err)
	}
	if _, _, found, err := st.MMDSEndpointByPath(ctx, sb.ID, "/latest/nope"); err != nil || found {
		t.Fatalf("MMDSEndpointByPath for an undeclared path: found=%t err=%v", found, err)
	}
}
