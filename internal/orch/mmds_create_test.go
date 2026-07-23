package orch

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/mmdscfg"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// validTemplateID is a syntactically valid <profile>-<kind>-<key> template id;
// Create's MMDS validation runs (and must fail fast) before any template/
// launch machinery is touched, so no matching template needs to actually exist.
var validTemplateID = "e2b-img-" + strings.Repeat("1", 64)

func TestCreateRejectsMMDSWhenEndpointsDisabled(t *testing.T) {
	o := testOrch(t) // default config: mmds.endpoints.enabled=false
	ctx := context.Background()
	apiKey, _, _ := allowlistedBuildIdentity(t, o)

	_, err := o.Create(ctx, api.CreateReq{
		APIKey:     apiKey,
		TemplateID: validTemplateID,
		Metadata: map[string]string{
			mmdscfg.Ns: "schema_version: 1\nendpoints:\n  - name: a\n    path: /latest/a\n    backend:\n      type: store\n",
		},
	})
	if !errors.Is(err, api.ErrBadRequest) {
		t.Fatalf("Create error = %v, want ErrBadRequest", err)
	}
	if !strings.Contains(err.Error(), "enabled=false") {
		t.Fatalf("Create error = %v, want it to mention endpoints disabled", err)
	}
}

func TestCreateRejectsMalformedMMDSBeforeLaunch(t *testing.T) {
	cfg := &config.Config{}
	cfg.MMDS.Endpoints.Enabled = true
	o := testOrchCfg(t, cfg) // launcher/vswitch are nil: a malformed-mmds Create
	// must fail before ever reaching launch, or this test would panic instead
	// of returning an error.
	ctx := context.Background()
	apiKey, _, _ := allowlistedBuildIdentity(t, o)

	_, err := o.Create(ctx, api.CreateReq{
		APIKey:     apiKey,
		TemplateID: validTemplateID,
		Metadata: map[string]string{
			mmdscfg.Ns: "schema_version: 2\nendpoints: []\n", // unsupported schema_version
		},
	})
	if !errors.Is(err, api.ErrBadRequest) {
		t.Fatalf("Create error = %v, want ErrBadRequest", err)
	}
	if !strings.Contains(err.Error(), "schema_version") {
		t.Fatalf("Create error = %v, want it to mention schema_version", err)
	}
}

// A valid mmds declaration is exercised at the mmdscfg.Extract level
// (internal/mmdscfg/mmdscfg_test.go) rather than through Orchestrator.Create:
// this package's test helper (testOrchCfg) wires a nil launcher/vswitch, so
// Create cannot run past validation into launch without panicking — the same
// limitation documented on internal/orch/orch.go's launch/Create path (no
// end-to-end harness exists here; see build_creds_test.go's testOrchCfg).

// TestRangeMmdsIgnoresPersistedEndpointsWhenDisabled covers the case where
// mmds.endpoints.enabled is toggled false after endpoints were already
// persisted (created while it was true): RangeMmds/SubscribeMmds must not
// expose those rows to an external proxy master's sync stream — a
// subscriber's own local inference of "enabled" (e.g. a proxy master that
// only checks whether mmds_listen is configured) must never be able to
// reactivate historical ciphertext the conductor itself now considers off.
func TestRangeMmdsIgnoresPersistedEndpointsWhenDisabled(t *testing.T) {
	cfg := &config.Config{}
	cfg.MMDS.Endpoints.Enabled = true
	o := testOrchCfg(t, cfg)
	ctx := context.Background()
	sb := &types.Sandbox{ID: "sb-gate", State: types.StateRunning, ManifestKey: strings.Repeat("7", 64), CreatedUnix: 1}
	if err := o.st.PutWithMMDSEndpoints(ctx, sb, []store.MMDSEndpoint{
		{Name: "creds", Path: "/latest/creds", BackendType: store.MMDSBackendStore},
	}); err != nil {
		t.Fatal(err)
	}

	// Sanity: with the flag on, the row is visible.
	var seen int
	if err := o.RangeMmds(ctx, func(routesync.MmdsEndpointEntry) error { seen++; return nil }); err != nil {
		t.Fatal(err)
	}
	if seen != 1 {
		t.Fatalf("RangeMmds with endpoints enabled saw %d entries, want 1", seen)
	}

	// Flip the flag off (as if the operator disabled it after Create) — the
	// row is still in the DB, but RangeMmds must now see nothing.
	o.cfg.MMDS.Endpoints.Enabled = false
	seen = 0
	if err := o.RangeMmds(ctx, func(routesync.MmdsEndpointEntry) error { seen++; return nil }); err != nil {
		t.Fatal(err)
	}
	if seen != 0 {
		t.Fatalf("RangeMmds with endpoints disabled saw %d entries, want 0 (must not reactivate historical ciphertext)", seen)
	}

	ch, cancel := o.SubscribeMmds()
	defer cancel()
	select {
	case ev := <-ch:
		t.Fatalf("SubscribeMmds with endpoints disabled delivered an event: %+v", ev)
	default:
	}
}

func TestCurrentRunIDReflectsCachedRunID(t *testing.T) {
	o := testOrch(t)
	sb := &types.Sandbox{ID: "sb-runid", State: types.StateRunning, RunID: "sr-run-1"}
	o.cache(sb)

	if got, ok := o.CurrentRunID("sb-runid"); !ok || got != "sr-run-1" {
		t.Fatalf("CurrentRunID = %q ok=%t, want sr-run-1", got, ok)
	}
	if _, ok := o.CurrentRunID("unknown"); ok {
		t.Fatal("CurrentRunID found an uncached sandbox")
	}

	// A resume republishes the cache entry with a new RunID (incarnation
	// binding relies on this becoming visible immediately, and
	// orch.go's own comment on launch/resume documents cache never being
	// mutated in place — this mirrors that by caching a fresh pointer).
	o.cache(&types.Sandbox{ID: "sb-runid", State: types.StateRunning, RunID: "sr-run-2"})
	if got, ok := o.CurrentRunID("sb-runid"); !ok || got != "sr-run-2" {
		t.Fatalf("CurrentRunID after resume = %q ok=%t, want sr-run-2", got, ok)
	}
}

func TestMmdsStoreEndpointsConversion(t *testing.T) {
	specs := []mmdscfg.EndpointSpec{
		{
			Name: "credentials",
			Path: "/latest/meta-data/credentials",
			Backend: mmdscfg.BackendSpec{
				Type: mmdscfg.BackendRelay,
				URL:  "https://identity.example.com/credentials",
				Auth: &mmdscfg.RelayAuthSpec{HeaderName: "X-Upstream-Assertion"},
			},
		},
		{
			Name:    "user-data",
			Path:    "/latest/user-data",
			Backend: mmdscfg.BackendSpec{Type: mmdscfg.BackendStore},
		},
	}
	got := mmdsStoreEndpoints(specs)
	if len(got) != 2 {
		t.Fatalf("got %d converted endpoints, want 2", len(got))
	}
	if got[0].Name != "credentials" || got[0].Path != "/latest/meta-data/credentials" || got[0].BackendType != store.MMDSBackendRelay {
		t.Fatalf("endpoint 0 = %+v", got[0])
	}
	if !strings.Contains(got[0].PublicConfigJSON, "https://identity.example.com/credentials") ||
		!strings.Contains(got[0].PublicConfigJSON, "X-Upstream-Assertion") {
		t.Fatalf("endpoint 0 public config = %q, want it to carry url + auth header name", got[0].PublicConfigJSON)
	}
	if got[1].Name != "user-data" || got[1].BackendType != store.MMDSBackendStore {
		t.Fatalf("endpoint 1 = %+v", got[1])
	}

	if out := mmdsStoreEndpoints(nil); out != nil {
		t.Fatalf("mmdsStoreEndpoints(nil) = %+v, want nil", out)
	}
}

func TestRegisterBuildRejectsMMDSNamespace(t *testing.T) {
	o := testOrch(t)
	ctx := context.Background()
	apiKey, _, _ := allowlistedBuildIdentity(t, o)

	_, err := o.RegisterBuild(ctx, apiKey, api.RegisterSpec{
		Profile: types.ProfileE2B,
		Metadata: map[string]string{
			mmdscfg.Ns: "schema_version: 1\nendpoints: []\n",
		},
	})
	if !errors.Is(err, api.ErrBadRequest) {
		t.Fatalf("RegisterBuild error = %v, want ErrBadRequest", err)
	}
	if !strings.Contains(err.Error(), mmdscfg.Ns) {
		t.Fatalf("RegisterBuild error = %v, want it to mention %s", err, mmdscfg.Ns)
	}
}

func TestTriggerBuildRejectsMMDSNamespace(t *testing.T) {
	o := testOrch(t)
	ctx := context.Background()
	apiKey, _, _ := allowlistedBuildIdentity(t, o)
	b, err := o.RegisterBuild(ctx, apiKey, api.RegisterSpec{Profile: types.ProfileE2B})
	if err != nil {
		t.Fatal(err)
	}

	err = o.TriggerBuild(ctx, apiKey, b.TemplateID, b.BuildID, api.TriggerSpec{
		FromImage: "registry.test/base:latest",
		Metadata: map[string]string{
			mmdscfg.Ns: "schema_version: 1\nendpoints: []\n",
		},
	}, api.BuildAuth{})
	if !errors.Is(err, api.ErrBadRequest) {
		t.Fatalf("TriggerBuild error = %v, want ErrBadRequest", err)
	}
	if !strings.Contains(err.Error(), mmdscfg.Ns) {
		t.Fatalf("TriggerBuild error = %v, want it to mention %s", err, mmdscfg.Ns)
	}
}
