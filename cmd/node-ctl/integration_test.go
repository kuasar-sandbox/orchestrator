package main

// In-process integration of the local control socket's admin + api planes with a
// real store-backed orchestrator (no launcher/vswitch — those planes don't touch
// them), driving the actual CLI entry-points. Proves the full client path:
// manifestKeyCmd/exportSandboxCmd → udsDo → UDS → configsock → orch.Admin/api.Core →
// store. (The systemd-dependent `serve` path is covered by the root e2e instead.)

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/configresolve"
	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/orch"
	"github.com/kuasar-sandbox/orchestrator/internal/secretbox"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
)

func startCtlSocket(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	sock := filepath.Join(dir, "ctl.sock")
	for _, f := range []string{"k", "e", "b", "o", "manifest.yaml"} {
		if err := os.WriteFile(filepath.Join(dir, f), []byte("key: \"\"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	yaml := "" +
		"api: { domain: dev.local, listen: \":0\" }\n" +
		"proxy: { mode: internal }\n" +
		"encryption_key: \"0000000000000000000000000000000000000000000000000000000000000000\"\n" +
		"manifest_config: " + dir + "/manifest.yaml\n" +
		"paths: { run_root: " + dir + ", base_root: " + dir + ", config_socket: " + sock + " }\n" +
		"sandbox:\n" +
		"  boot: { kernel: " + dir + "/k, runtime: " + dir + "/rt, overlay_diff_template: " + dir + "/o }\n"
	cfgPath := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(cfgPath, []byte(yaml), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(cfgPath)
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}
	box, err := secretbox.NewFromColonHex(configresolve.EncryptionKeySpec(cfg))
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(cfg.Paths.DBPath, box)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	core := orch.New(cfg, st, nil, nil, log) // no launcher / vswitch — admin+export don't use them
	apiH := api.New(core, cfg.API.Domain, api.Resources{VCPU: configresolve.SandboxResources(cfg.Sandbox.Resources).Capacity.CPU, MemoryMB: cfg.Sandbox.Resources.MemoryMiB()}, log).Handler()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() {
		_ = configsock.New(sock, configsock.Deps{Provider: core, Admin: core, API: apiH}, log).Serve(ctx)
	}()
	for range 400 {
		if _, err := os.Stat(sock); err == nil {
			return sock
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("control socket did not bind")
	return ""
}

func captureStdout(t *testing.T, fn func() error) (string, error) {
	t.Helper()
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	err := fn()
	_ = w.Close()
	os.Stdout = old
	b, _ := io.ReadAll(r)
	return string(b), err
}

func parseKeyPairStatus(t *testing.T, out, wantStatus string) (string, string) {
	t.Helper()
	fields := strings.Fields(out)
	if len(fields) != 3 || fields[0] != wantStatus || !strings.HasPrefix(fields[1], "api=") || !strings.HasPrefix(fields[2], "manifest=") {
		t.Fatalf("manifest-key output=%q, want %s api=<fingerprint> manifest=<fingerprint>", out, wantStatus)
	}
	return strings.TrimPrefix(fields[1], "api="), strings.TrimPrefix(fields[2], "manifest=")
}

// TestManifestKeyCmdAdminPlane drives the real manifest-key CLI against a real
// store-backed admin plane over the control socket.
func TestManifestKeyCmdAdminPlane(t *testing.T) {
	sock := startCtlSocket(t)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	const mk = "1111111111111111111111111111111111111111111111111111111111111111"

	out, err := captureStdout(t, func() error {
		return manifestKeyCmd([]string{"add", "--label", "dev", "--socket", sock, mk}, log)
	})
	if err != nil {
		t.Fatalf("add: out=%q err=%v", out, err)
	}
	apiFP, manifestFP := parseKeyPairStatus(t, out, "added")
	if len(apiFP) != 64 || len(manifestFP) != 64 {
		t.Fatalf("add fingerprints api=%q manifest=%q, want full SHA-256 hex", apiFP, manifestFP)
	}

	out, err = captureStdout(t, func() error {
		return manifestKeyCmd([]string{"add", "--socket", sock, mk}, log)
	})
	if err != nil {
		t.Fatalf("re-add: out=%q err=%v", out, err)
	}
	refreshedAPI, refreshedManifest := parseKeyPairStatus(t, out, "refreshed")
	if refreshedAPI != apiFP || refreshedManifest != manifestFP {
		t.Fatalf("re-add fingerprints api=%q manifest=%q, want api=%q manifest=%q", refreshedAPI, refreshedManifest, apiFP, manifestFP)
	}

	out, err = captureStdout(t, func() error {
		return manifestKeyCmd([]string{"check", "--socket", sock, mk}, log)
	})
	if err != nil {
		t.Fatalf("check: out=%q err=%v", out, err)
	}
	checkedAPI, checkedManifest := parseKeyPairStatus(t, out, "present")
	if checkedAPI != apiFP || checkedManifest != manifestFP {
		t.Fatalf("check fingerprints api=%q manifest=%q, want api=%q manifest=%q", checkedAPI, checkedManifest, apiFP, manifestFP)
	}

	out, err = captureStdout(t, func() error {
		return manifestKeyCmd([]string{"list", "--socket", sock}, log)
	})
	if err != nil || !strings.Contains(out, "api="+apiFP) || !strings.Contains(out, "manifest="+manifestFP) {
		t.Fatalf("list: out=%q err=%v (want api=%s manifest=%s)", out, err, apiFP, manifestFP)
	}

	// Bad key is validated daemon-side (the CLI never sees key material logic).
	if _, err := captureStdout(t, func() error {
		return manifestKeyCmd([]string{"add", "--socket", sock, "nothex"}, log)
	}); err == nil {
		t.Fatal("add of a non-hex key should error")
	}
}

// TestExportImportCmdAPIPlane proves the export/import CLIs reach the api plane over
// the control socket and that api-key auth is enforced there.
func TestExportImportCmdAPIPlane(t *testing.T) {
	sock := startCtlSocket(t)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	t.Setenv("E2B_API_KEY", "") // no key -> CLI-side error before any request
	if err := exportSandboxCmd([]string{"sbx_x", "--socket", sock}, log); err == nil {
		t.Fatal("export with no E2B_API_KEY should error")
	}

	t.Setenv("E2B_API_KEY", "not-a-key") // reaches the api plane, rejected (401)
	if err := exportSandboxCmd([]string{"sbx_x", "--socket", sock}, log); err == nil {
		t.Fatal("export with a bad api key should error (api plane reachable + auth enforced)")
	}
	if err := importSandboxCmd([]string{"dG9rZW4=", "--socket", sock}, log); err == nil {
		t.Fatal("import with a bad api key should error")
	}
}
