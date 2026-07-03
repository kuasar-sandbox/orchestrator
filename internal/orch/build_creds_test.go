package orch

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/regcreds"
	"github.com/kuasar-sandbox/orchestrator/internal/secretbox"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func testOrch(t *testing.T) *Orchestrator { return testOrchCfg(t, &config.Config{}) }

func testOrchCfg(t *testing.T, cfg *config.Config) *Orchestrator {
	t.Helper()
	box, err := secretbox.NewFromColonHex(strings.Repeat("0", 64))
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(filepath.Join(t.TempDir(), "t.db"), box)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return New(cfg, st, nil, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

// TestResolveBuildCreds verifies the precedence: pull token > fromImageRegistry
// (cleartext) > tenant default (manifest_keys) > anonymous.
func TestResolveBuildCreds(t *testing.T) {
	o := testOrch(t)
	ctx := context.Background()
	mk := strings.Repeat("3", 64)
	b := &types.Build{ManifestKey: mk, FromImage: "reg.example.com/app:tag"}

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
	// tenant default (stored on the manifest key) when nothing per-build is given.
	auth, _ := regcreds.AssembleDockerAuth(regcreds.Creds{Username: "tu", Password: "tp"})
	if _, err := o.st.AddManifestKey(ctx, mk, "", 0, auth); err != nil {
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
