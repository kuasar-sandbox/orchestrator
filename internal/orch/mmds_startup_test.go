package orch

import (
	"context"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestValidatePersistedMMDSEndpointsSkipsWhenDisabled(t *testing.T) {
	cfg := &config.Config{}
	cfg.MMDS.Endpoints.Enabled = true
	o := testOrchCfg(t, cfg)
	ctx := context.Background()
	sb := &types.Sandbox{ID: "sb-skip", State: types.StateRunning, ManifestKey: strings.Repeat("1", 64), CreatedUnix: 1}
	if err := o.st.PutWithMMDSEndpoints(ctx, sb, []store.MMDSEndpoint{
		// A path that will never validate (no leading slash) — proves the
		// disabled path really does skip validation entirely rather than
		// happening to pass.
		{Name: "a", Path: "not-absolute", BackendType: store.MMDSBackendStore},
	}); err != nil {
		t.Fatal(err)
	}

	o.cfg.MMDS.Endpoints.Enabled = false
	if err := o.ValidatePersistedMMDSEndpoints(ctx); err != nil {
		t.Fatalf("ValidatePersistedMMDSEndpoints with endpoints disabled = %v, want nil (skip entirely)", err)
	}
}

func TestValidatePersistedMMDSEndpointsPassesValidData(t *testing.T) {
	cfg := &config.Config{}
	cfg.MMDS.Endpoints.Enabled = true
	o := testOrchCfg(t, cfg)
	ctx := context.Background()
	sb := &types.Sandbox{ID: "sb-valid", State: types.StateRunning, ManifestKey: strings.Repeat("2", 64), CreatedUnix: 1}
	if err := o.st.PutWithMMDSEndpoints(ctx, sb, []store.MMDSEndpoint{
		{Name: "a", Path: "/latest/a", BackendType: store.MMDSBackendStore},
		{Name: "b", Path: "/latest/b", BackendType: store.MMDSBackendRelay},
	}); err != nil {
		t.Fatal(err)
	}

	if err := o.ValidatePersistedMMDSEndpoints(ctx); err != nil {
		t.Fatalf("ValidatePersistedMMDSEndpoints over valid data = %v, want nil", err)
	}
}

// TestValidatePersistedMMDSEndpointsRejectsReservedPrefixConflict covers an
// operator adding a new reserved_path_prefixes entry after a sandbox
// already declared a path that now falls under it — any such conflict must
// fail startup.
func TestValidatePersistedMMDSEndpointsRejectsReservedPrefixConflict(t *testing.T) {
	cfg := &config.Config{}
	cfg.MMDS.Endpoints.Enabled = true
	o := testOrchCfg(t, cfg)
	ctx := context.Background()
	sb := &types.Sandbox{ID: "sb-conflict", State: types.StateRunning, ManifestKey: strings.Repeat("3", 64), CreatedUnix: 1}
	if err := o.st.PutWithMMDSEndpoints(ctx, sb, []store.MMDSEndpoint{
		{Name: "creds", Path: "/newly-reserved/creds", BackendType: store.MMDSBackendStore},
	}); err != nil {
		t.Fatal(err)
	}

	// Simulate the operator reserving this prefix after the fact.
	o.cfg.MMDS.Endpoints.ReservedPathPrefixes = []string{"/latest/api/", "/internal/", "/newly-reserved/"}

	err := o.ValidatePersistedMMDSEndpoints(ctx)
	if err == nil {
		t.Fatal("ValidatePersistedMMDSEndpoints did not reject a path now under a reserved prefix")
	}
	if !strings.Contains(err.Error(), "sb-conflict") || !strings.Contains(err.Error(), "creds") {
		t.Fatalf("error %v does not name the offending sandbox/endpoint", err)
	}
}

// TestValidatePersistedMMDSEndpointsRejectsExceededPerSandboxCount covers an
// operator lowering max_endpoints_per_sandbox below what a sandbox already
// has declared.
func TestValidatePersistedMMDSEndpointsRejectsExceededPerSandboxCount(t *testing.T) {
	cfg := &config.Config{}
	cfg.MMDS.Endpoints.Enabled = true
	o := testOrchCfg(t, cfg)
	ctx := context.Background()
	sb := &types.Sandbox{ID: "sb-overcount", State: types.StateRunning, ManifestKey: strings.Repeat("4", 64), CreatedUnix: 1}
	if err := o.st.PutWithMMDSEndpoints(ctx, sb, []store.MMDSEndpoint{
		{Name: "a", Path: "/latest/a", BackendType: store.MMDSBackendStore},
		{Name: "b", Path: "/latest/b", BackendType: store.MMDSBackendStore},
		{Name: "c", Path: "/latest/c", BackendType: store.MMDSBackendStore},
	}); err != nil {
		t.Fatal(err)
	}

	// Simulate the operator lowering the per-sandbox cap after the fact.
	o.cfg.MMDS.Endpoints.MaxEndpointsPerSandbox = 2

	err := o.ValidatePersistedMMDSEndpoints(ctx)
	if err == nil {
		t.Fatal("ValidatePersistedMMDSEndpoints did not reject a sandbox now over max_endpoints_per_sandbox")
	}
	if !strings.Contains(err.Error(), "sb-overcount") {
		t.Fatalf("error %v does not name the offending sandbox", err)
	}
}
