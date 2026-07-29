package orch

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

const testMMDSSecretSpec = `{"version":1,"secrets":[{"name":"key1"}]}`

func putTestSandboxForMMDSSecretAdmin(t *testing.T, o *Orchestrator, id, mmdsSpec string) *types.Sandbox {
	t.Helper()
	mk := strings.Repeat("a", 64)
	sb := &types.Sandbox{
		ID:          id,
		Profile:     types.ProfileBare,
		TemplateID:  "bare-img-" + strings.Repeat("b", 64),
		State:       types.StateRunning,
		APISecret:   deriveTestAPISecret(t, mk),
		ManifestKey: mk,
		RunDir:      filepath.Join(t.TempDir(), "run"),
		BaseDir:     filepath.Join(t.TempDir(), "lib"),
		CreatedUnix: 1,
		Metadata:    map[string]string{sandboxcfg.NsMMDS: mmdsSpec},
	}
	materializeTestSandboxCredentials(t, sb)
	if err := o.st.Put(context.Background(), sb); err != nil {
		t.Fatal(err)
	}
	return sb
}

func testMMDSSecretsOrch(t *testing.T, maxValueBytes int) *Orchestrator {
	t.Helper()
	cfg := &config.Config{}
	cfg.MMDS.Routes = testMMDSRoutesConfig()
	cfg.MMDS.Routes.Secret.MaxValueBytes = maxValueBytes
	return testOrchCfg(t, cfg)
}

func TestPutMMDSSecretRejectsUnknownSandbox(t *testing.T) {
	o := testMMDSSecretsOrch(t, 1024)
	_, err := o.PutMMDSSecret(context.Background(), "no-such-sandbox", "key1", []byte("v"), "", 0)
	if !errors.Is(err, api.ErrNotFound) {
		t.Fatalf("err = %v, want api.ErrNotFound", err)
	}
}

func TestPutMMDSSecretRejectsUnspecifiedName(t *testing.T) {
	o := testMMDSSecretsOrch(t, 1024)
	putTestSandboxForMMDSSecretAdmin(t, o, "sbx-1", testMMDSSecretSpec)
	_, err := o.PutMMDSSecret(context.Background(), "sbx-1", "not-specified", []byte("v"), "", 0)
	if !errors.Is(err, api.ErrBadRequest) {
		t.Fatalf("err = %v, want api.ErrBadRequest", err)
	}
}

func TestPutMMDSSecretRejectsTooLarge(t *testing.T) {
	o := testMMDSSecretsOrch(t, 4)
	putTestSandboxForMMDSSecretAdmin(t, o, "sbx-1", testMMDSSecretSpec)
	_, err := o.PutMMDSSecret(context.Background(), "sbx-1", "key1", []byte("too-large"), "", 0)
	if !errors.Is(err, api.ErrBadRequest) {
		t.Fatalf("err = %v, want api.ErrBadRequest", err)
	}
}

func TestPutMMDSSecretSucceedsAndWakesParkedWaiter(t *testing.T) {
	o := testMMDSSecretsOrch(t, 1024)
	putTestSandboxForMMDSSecretAdmin(t, o, "sbx-1", testMMDSSecretSpec)

	// register synchronously, before PutMMDSSecret runs -- registering only
	// after spawning the waiting goroutine would race PutMMDSSecret's Notify
	// against the goroutine's own registration (whichever runs first), which
	// is exactly the lost-wakeup shape this test exists to guard against, not
	// an artifact to route around here.
	ch := o.secretWait.register("sbx-1", "key1")
	woke := make(chan bool, 1)
	go func() { woke <- o.secretWait.block(context.Background(), ch) }()

	rev, err := o.PutMMDSSecret(context.Background(), "sbx-1", "key1", []byte("hello"), "text/plain", 0)
	if err != nil {
		t.Fatal(err)
	}
	if rev != 1 {
		t.Fatalf("revision = %d, want 1", rev)
	}
	if !<-woke {
		t.Fatal("PutMMDSSecret did not wake the parked waiter")
	}

	v, present, gotRev, err := o.st.GetMMDSSecretValue(context.Background(), "sbx-1", "key1")
	if err != nil {
		t.Fatal(err)
	}
	if !present || gotRev != 1 || v.ContentType != "text/plain" {
		t.Fatalf("stored value = %+v present=%t rev=%d", v, present, gotRev)
	}
}

func TestDeleteMMDSSecretRejectsUnknownSandbox(t *testing.T) {
	o := testMMDSSecretsOrch(t, 1024)
	_, err := o.DeleteMMDSSecret(context.Background(), "no-such-sandbox", "key1")
	if !errors.Is(err, api.ErrNotFound) {
		t.Fatalf("err = %v, want api.ErrNotFound", err)
	}
}

func TestDeleteMMDSSecretRejectsUnspecifiedName(t *testing.T) {
	o := testMMDSSecretsOrch(t, 1024)
	putTestSandboxForMMDSSecretAdmin(t, o, "sbx-1", testMMDSSecretSpec)
	_, err := o.DeleteMMDSSecret(context.Background(), "sbx-1", "not-specified")
	if !errors.Is(err, api.ErrBadRequest) {
		t.Fatalf("err = %v, want api.ErrBadRequest", err)
	}
}

func TestDeleteMMDSSecretIsIdempotentForNeverConfiguredName(t *testing.T) {
	o := testMMDSSecretsOrch(t, 1024)
	putTestSandboxForMMDSSecretAdmin(t, o, "sbx-1", testMMDSSecretSpec)
	rev, err := o.DeleteMMDSSecret(context.Background(), "sbx-1", "key1")
	if err != nil {
		t.Fatal(err)
	}
	if rev != 0 {
		t.Fatalf("revision = %d, want 0 (no-op)", rev)
	}
}

func TestDeleteMMDSSecretClearsConfiguredValue(t *testing.T) {
	o := testMMDSSecretsOrch(t, 1024)
	putTestSandboxForMMDSSecretAdmin(t, o, "sbx-1", testMMDSSecretSpec)
	if _, err := o.PutMMDSSecret(context.Background(), "sbx-1", "key1", []byte("hello"), "", 0); err != nil {
		t.Fatal(err)
	}
	rev, err := o.DeleteMMDSSecret(context.Background(), "sbx-1", "key1")
	if err != nil {
		t.Fatal(err)
	}
	if rev != 2 {
		t.Fatalf("revision = %d, want 2", rev)
	}
	_, present, _, err := o.st.GetMMDSSecretValue(context.Background(), "sbx-1", "key1")
	if err != nil {
		t.Fatal(err)
	}
	if present {
		t.Fatal("expected key1 to be cleared")
	}
}
