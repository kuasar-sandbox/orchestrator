package orch

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/apikey"
	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/regcreds"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/secretbox"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func testOrch(t *testing.T) *Orchestrator { return testOrchCfg(t, &config.Config{}) }

type immediateRouteBarrierCoordinator struct{}

func (immediateRouteBarrierCoordinator) BeginProxyRouteBarrier() (routesync.RouteBarrier, error) {
	return immediateRouteBarrier{}, nil
}

type immediateRouteBarrier struct{}

func (immediateRouteBarrier) ID() string                 { return "test-immediate-route-barrier" }
func (immediateRouteBarrier) Wait(context.Context) error { return nil }
func (immediateRouteBarrier) Commit() error              { return nil }
func (immediateRouteBarrier) Cancel()                    {}

func testBuildResources() types.BuildResources {
	return types.BuildResources{CPU: 2000, Memory: 2 << 30}
}

func deriveTestAPISecret(t *testing.T, manifestKey string) string {
	t.Helper()
	raw, err := hex.DecodeString(manifestKey)
	if err != nil || len(raw) != 32 {
		t.Fatalf("invalid test ManifestKey %q: %v", manifestKey, err)
	}
	return hex.EncodeToString(apikey.DeriveAPISecret(raw))
}

func mintTestAPIKey(t *testing.T, apiSecret string) string {
	t.Helper()
	raw, err := hex.DecodeString(apiSecret)
	if err != nil || len(raw) != 32 {
		t.Fatalf("invalid test APISecret %q: %v", apiSecret, err)
	}
	apiKey, err := apikey.Mint(raw)
	if err != nil {
		t.Fatal(err)
	}
	return apiKey
}

func defaultTestCredentials(t *testing.T, manifestKey string) (apiSecret, apiKey string) {
	t.Helper()
	apiSecret = deriveTestAPISecret(t, manifestKey)
	return apiSecret, mintTestAPIKey(t, apiSecret)
}

func testOrchCfg(t *testing.T, cfg *config.Config) *Orchestrator {
	return testOrchCfgAt(t, cfg, filepath.Join(t.TempDir(), "t.db"))
}

func testOrchCfgAt(t *testing.T, cfg *config.Config, dbPath string) *Orchestrator {
	t.Helper()
	if cfg.Builder.Admission.Execution == nil {
		maxBuilds := int64(2)
		cfg.Builder.Admission.Execution = &config.BuildAdmissionLimitConfig{MaxBuilds: &maxBuilds}
	}
	if cfg.Builder.Admission.Registration == nil {
		maxBuilds := int64(2)
		cfg.Builder.Admission.Registration = &config.BuildAdmissionLimitConfig{MaxBuilds: &maxBuilds}
	}
	if cfg.Builder.RegistrationTTL == "" {
		cfg.Builder.RegistrationTTL = "1h"
	}
	if cfg.Builder.QueueTTL == "" {
		cfg.Builder.QueueTTL = "30m"
	}
	if cfg.Builder.TerminalTTL == "" {
		cfg.Builder.TerminalTTL = "24h"
	}
	if cfg.Sandbox.DeadTTL == "" {
		cfg.Sandbox.DeadTTL = "24h"
	}
	box, err := secretbox.NewFromColonHex(strings.Repeat("0", 64))
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(dbPath, box)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	o := New(cfg, st, nil, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	o.SetProxyRouteBarrierCoordinator(immediateRouteBarrierCoordinator{})
	// General unit fixtures model a controller after startup reconciliation.
	// Recovery-boundary tests construct Orchestrator directly and control this
	// gate explicitly.
	o.buildRecoveryReadyOnce.Do(func() { close(o.buildRecoveryReady) })
	return o
}

// TestResolveBuildCreds verifies the precedence: pull token > fromImageRegistry
// (cleartext) > tenant default (credential pair) > anonymous.
func TestResolveBuildCreds(t *testing.T) {
	o := testOrch(t)
	ctx := context.Background()
	mk := strings.Repeat("3", 64)
	apiSecret := deriveTestAPISecret(t, mk)
	b := &types.Build{APISecret: apiSecret, ManifestKey: mk, FromImage: "reg.example.com/app:tag"}

	user := func(js string) string {
		var c regcreds.Creds
		_ = json.Unmarshal([]byte(js), &c)
		return c.Username
	}

	// anonymous: nothing configured.
	if js, err := o.resolveBuildCreds(ctx, b, "", "", ""); err != nil || js != "" {
		t.Fatalf("anon: %q %v", js, err)
	}
	// fromImageRegistry (cleartext) when no token / tenant default.
	if js, err := o.resolveBuildCreds(ctx, b, "", "fu", "fp"); err != nil || user(js) != "fu" {
		t.Fatalf("fromImageRegistry: %q %v", js, err)
	}
	// tenant default (stored on the credential pair) when nothing per-build is given.
	auth, _ := regcreds.AssembleDockerAuth(regcreds.Creds{Username: "tu", Password: "tp"})
	if _, err := o.st.AddKeyPair(ctx, store.KeyPair{APISecret: apiSecret, ManifestKey: mk}, "", 0, auth); err != nil {
		t.Fatal(err)
	}
	if js, err := o.resolveBuildCreds(ctx, b, "", "", ""); err != nil || user(js) != "tu" {
		t.Fatalf("tenant default: %q %v", js, err)
	}
	// pull token wins over fromImageRegistry and the tenant default.
	tok, _ := regcreds.Seal(mk, regcreds.Creds{Username: "ku", Password: "kp"})
	if js, err := o.resolveBuildCreds(ctx, b, tok, "fu", "fp"); err != nil || user(js) != "ku" {
		t.Fatalf("token precedence: %q %v", js, err)
	}
	// a malformed / wrong-tenant token errors (fails the build loudly).
	if _, err := o.resolveBuildCreds(ctx, b, "kpt_garbage", "", ""); err == nil {
		t.Fatal("bad token should error")
	}
}

func TestValidateBuildOptionsCannotEnableNodeDisabledReferer(t *testing.T) {
	o := testOrch(t)
	enabled := true
	err := o.validateBuildOptions(types.BuildOptions{Referer: &types.BuildRefererOptions{Enabled: &enabled}}, false)
	if err == nil || !strings.Contains(err.Error(), api.ErrBadRequest.Error()) {
		t.Fatalf("expected bad request, got %v", err)
	}
}

func TestValidateBuildOptionsRestrictsRefererCapabilitySubset(t *testing.T) {
	enabled, disabled := true, false
	tests := []struct {
		name      string
		configure func(*config.Config)
		opts      types.BuildOptions
	}{
		{
			name: "writeback needs lookup",
			opts: types.BuildOptions{Referer: &types.BuildRefererOptions{
				Enabled: &disabled, Writeback: &enabled,
			}},
		},
		{
			name: "writeback cannot exceed node",
			configure: func(cfg *config.Config) {
				cfg.Builder.Referer.Enabled = true
				cfg.Builder.Referer.Writeback = &disabled
			},
			opts: types.BuildOptions{Referer: &types.BuildRefererOptions{Writeback: &enabled}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &config.Config{}
			if tt.configure != nil {
				tt.configure(cfg)
			}
			o := testOrchCfg(t, cfg)
			if err := o.validateBuildOptions(tt.opts, false); err == nil {
				t.Fatal("invalid referer capability escalation was accepted")
			}
		})
	}
}

func TestEffectiveImportRefererComputesOwnerToken(t *testing.T) {
	o := testOrchCfg(t, &config.Config{})
	o.cfg.Builder.Referer.Enabled = true
	o.cfg.Builder.Referer.Desc = "acme-prod"
	o.cfg.Builder.Referer.Key = "acme-prod"
	b := &types.Build{
		ManifestKey: strings.Repeat("4", 64),
		FromImage:   "reg.example.com/app:tag",
	}
	got, err := o.effectiveImportReferer(b)
	if err != nil {
		t.Fatalf("effectiveImportReferer: %v", err)
	}
	if !got.Enabled || !got.Fallback || !got.Writeback {
		t.Fatalf("unexpected referer defaults: %+v", got)
	}
	if !strings.HasSuffix(got.Owner, " acme-prod") {
		t.Fatalf("owner token missing descriptor: %q", got.Owner)
	}
	if strings.Contains(got.Owner, b.ManifestKey) {
		t.Fatalf("owner token leaked manifest key: %q", got.Owner)
	}
}

func TestEffectiveImportRefererHonorsPerBuildDisableSubset(t *testing.T) {
	o := testOrchCfg(t, &config.Config{})
	o.cfg.Builder.Referer.Enabled = true
	o.cfg.Builder.Referer.Desc = "acme-prod"
	o.cfg.Builder.Referer.Key = "acme-prod"
	disabled := false
	b := &types.Build{
		ManifestKey: strings.Repeat("4", 64),
		FromImage:   "reg.example.com/app:tag",
		Builder: types.BuildOptions{Referer: &types.BuildRefererOptions{
			Enabled: &disabled,
		}},
	}
	got, err := o.effectiveImportReferer(b)
	if err != nil {
		t.Fatal(err)
	}
	if got.Enabled || got.Writeback {
		t.Fatalf("per-build disable produced %+v", got)
	}

	b.Builder.Referer.Enabled = nil
	b.Builder.Referer.Writeback = &disabled
	got, err = o.effectiveImportReferer(b)
	if err != nil {
		t.Fatal(err)
	}
	if !got.Enabled || got.Writeback {
		t.Fatalf("writeback-only disable produced %+v", got)
	}
}
