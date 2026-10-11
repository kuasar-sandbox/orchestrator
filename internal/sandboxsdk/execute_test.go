package sandboxsdk

import (
	"context"
	"errors"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/sandboxer/pkg/config"
	runtimesdk "github.com/kuasar-sandbox/sandboxer/pkg/runtime"
	"github.com/kuasar-sandbox/sandboxer/pkg/stdio"
)

func cgroupFixture(t *testing.T) *os.File {
	t.Helper()
	f, err := os.Open("/sys/fs/cgroup")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })
	return f
}
func optionsFixture(t *testing.T) (*Input, *os.File) {
	t.Helper()
	spec := generatedSpec(t)
	if err := os.WriteFile(filepath.Join(spec.Workdir, "sandbox.yaml"), []byte("launch:\n  env: {}\n  ephemeral_env: {TOKEN: task}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	in, err := Parse(spec, types.ResumeSource{}, nil, map[string]string{"MANIFEST_KEY": "", "SANDBOX_CH_PATH": "/explicit/cloud-hypervisor"}, "sb-1")
	if err != nil {
		t.Fatal(err)
	}
	return in, cgroupFixture(t)
}
func TestOptionsBindingsAndPresence(t *testing.T) {
	in, cg := optionsFixture(t)
	in.Source = types.ResumeSource{Kind: types.ResumeSourceSandbox, Ref: "file:///source.sandbox"}
	in.Env["SANDBOX_PING_FATAL_THRESHOLD"] = "9"
	in.Env["SANDBOX_STATS_INTERVAL"] = "4s"
	opts, presence, storage, err := in.options(cg)
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()
	if !presence.Has("launch.env") || presence.Has("boot.root") {
		t.Fatal("explicit empty field presence lost")
	}
	if opts.Cfg.Resources.Control.CgroupFD != int(cg.Fd()) || opts.Cfg.Resources.Control.CgroupPath != "/sys/fs/cgroup" {
		t.Fatalf("cgroup=%+v", opts.Cfg.Resources.Control)
	}
	if opts.RuntimeRoot != in.RunRoot || opts.BaseRoot != in.BaseRoot || opts.SandboxID != in.SandboxID || opts.PathID != in.PathID || opts.CHBinary != "/explicit/cloud-hypervisor" {
		t.Fatal("SDK paths/identity changed")
	}
	if opts.StdioMode.Stdout.Kind != stdio.StreamJournald || opts.StdioMode.Stdout.Journal.Tag != "sandbox-runner" || opts.StdioMode.Console.Journal.Tag != "sandbox-console" {
		t.Fatal("log routing changed")
	}
	if opts.StatsInterval != 4*time.Second || opts.PingFatalThreshold != 9 {
		t.Fatal("explicit environment defaults lost")
	}
	if _, err := cg.Stat(); err != nil {
		t.Fatal("borrowed descriptor closed")
	}
}
func TestPerRunCredentialsAndLocalEncryption(t *testing.T) {
	in, cg := optionsFixture(t)
	t.Setenv("MANIFEST_KEY", strings.Repeat("ff", 32))
	t.Setenv("MANIFEST_CONFIG", "/must-not-load")
	path := filepath.Join(in.Workdir, "manifest.yaml")
	if err := os.WriteFile(path, []byte("manifest:\n  key: "+strings.Repeat("22", 32)+"\ncrypto:\n  local: required\n"), 0600); err != nil {
		t.Fatal(err)
	}
	in.ManifestConfig = path
	in.Env["MANIFEST_KEY"] = strings.Repeat("11", 32)
	opts, _, storage, err := in.options(cg)
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()
	in.Env["MANIFEST_KEY"] = strings.Repeat("33", 32)
	key, err := opts.CustomerKeyFn()
	if err != nil || key[0] != 0x11 || opts.LocalCodec == nil || !opts.LocalRequired {
		t.Fatalf("explicit storage isolation failed: %v", err)
	}
	second, _, storage2, err := in.options(cg)
	if err != nil {
		t.Fatal(err)
	}
	defer storage2.Close()
	key2, err := second.CustomerKeyFn()
	if err != nil || key2[0] != 0x33 {
		t.Fatal("second storage reused first credential")
	}
	in.Env["MANIFEST_KEY"] = ""
	fallback, _, storage3, err := in.options(cg)
	if err != nil {
		t.Fatal(err)
	}
	defer storage3.Close()
	key3, err := fallback.CustomerKeyFn()
	if err != nil || key3[0] != 0x22 {
		t.Fatal("empty authoritative key did not preserve YAML fallback")
	}
}
func TestNoAmbientManifestFallback(t *testing.T) {
	in, cg := optionsFixture(t)
	t.Setenv("MANIFEST_CONFIG", "/ambient-config-must-not-load")
	opts, _, storage, err := in.options(cg)
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()
	if opts.ManifestCfg != nil || opts.Fetcher != nil {
		t.Fatal("ambient manifest config leaked")
	}
}
func TestCgroupRejectsOrdinaryDirectory(t *testing.T) {
	f, err := os.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := cgroupPath(f); err == nil {
		t.Fatal("accepted non-cgroup descriptor")
	}
	if _, err := cgroupPath(nil); err == nil {
		t.Fatal("accepted missing descriptor")
	}
}
func TestLocateCHUsesOldExecutableBundle(t *testing.T) {
	in, _ := optionsFixture(t)
	delete(in.Env, "SANDBOX_CH_PATH")
	dir := t.TempDir()
	in.Exec = filepath.Join(dir, "sandbox-ctl")
	ch := filepath.Join(dir, "cloud-hypervisor")
	if err := os.WriteFile(ch, []byte("fixture"), 0700); err != nil {
		t.Fatal(err)
	}
	got, err := in.locateCH()
	if err != nil || got != ch {
		t.Fatalf("bundled CH=%q %v", got, err)
	}
}
func TestSnapshotSelection(t *testing.T) {
	in, _ := optionsFixture(t)
	in.Locations = config.RefLocations{}
	if err := in.Locations.Set("local=file:///prepared"); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ raw, path, ref, key string }{
		{raw: "relative.snapshot", path: filepath.Join(in.Workdir, "relative.snapshot")},
		{raw: "file:///prepared/source.snapshot", path: "/prepared/source.snapshot"},
		{raw: "manifest://" + strings.Repeat("a", 64), ref: "manifest://" + strings.Repeat("a", 64), key: strings.Repeat("a", 64)},
	} {
		in.Source.Ref = tc.raw
		spec := runtimesdk.RestoreSpec{ManifestCfg: &config.ManifestConfig{}}
		if err := in.snapshot(&spec); err != nil {
			t.Fatal(err)
		}
		if spec.SnapshotPath != tc.path || spec.SnapshotRef != tc.ref || spec.SnapshotManifestKey != tc.key {
			t.Fatalf("snapshot selection=%+v", spec)
		}
	}
}
func TestExecuteFailureClosesReadinessAndPreservesProcessState(t *testing.T) {
	in, cg := optionsFixture(t)
	in.Config = "missing.yaml"
	in.LogTo = "default"
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	before := os.Getenv("MANIFEST_KEY")
	w := &captureCloser{}
	code, err := Execute(context.Background(), in, cg, NewReadiness(w))
	if err == nil || !errors.Is(err, os.ErrNotExist) || code != 1 || w.closes != 1 {
		t.Fatalf("failure=%d %v closes=%d", code, err, w.closes)
	}
	after, _ := os.Getwd()
	if after != cwd || os.Getenv("MANIFEST_KEY") != before {
		t.Fatal("SDK changed process cwd/env")
	}
}

func TestInvalidEnvironmentUsesCLIUsageExit(t *testing.T) {
	for _, key := range []string{"SANDBOX_STATS_INTERVAL", "SANDBOX_PING_FATAL_THRESHOLD"} {
		t.Run(key, func(t *testing.T) {
			in, cg := optionsFixture(t)
			in.LogTo = "default"
			in.Env[key] = "invalid"
			code, err := Execute(context.Background(), in, cg, NewReadiness(&captureCloser{}))
			if err == nil || code != 2 {
				t.Fatalf("invalid env returned %d %v, want CLI usage exit 2", code, err)
			}
		})
	}
}

func TestCHEnvironmentBareNameUsesTaskPATH(t *testing.T) {
	in, _ := optionsFixture(t)
	dir := t.TempDir()
	binary := filepath.Join(dir, "alternate-ch")
	if err := os.WriteFile(binary, []byte("fixture"), 0700); err != nil {
		t.Fatal(err)
	}
	in.Env["SANDBOX_CH_PATH"] = "alternate-ch"
	in.Env["PATH"] = dir
	got, err := in.locateCH()
	if err != nil || got != binary {
		t.Fatalf("task PATH=%q %v", got, err)
	}
}

func TestDiagnosticsRestoredAfterSDKFailure(t *testing.T) {
	in, cg := optionsFixture(t)
	in.Config = "missing.yaml"
	in.LogTo = "journald=sandbox-ctl,KUASAR_RUN_ID=test-run"
	previous := log.Writer()
	if _, err := Execute(context.Background(), in, cg, NewReadiness(&captureCloser{})); err == nil {
		t.Fatal("expected config failure")
	}
	if log.Writer() != previous {
		t.Fatal("SDK retained diagnostic writer after cleanup")
	}
}

func TestCLILifecycleErrorStatus(t *testing.T) {
	failure := errors.New("runtime failure")
	for _, status := range []int{-1, 0, 2, 137} {
		code, err := cliRunResult(status, failure)
		if code != 1 || err != failure {
			t.Fatalf("SDK (%d, error) became (%d, %v)", status, code, err)
		}
		code, err = cliRunResult(status, nil)
		if code != status || err != nil {
			t.Fatalf("SDK (%d, nil) became (%d, %v)", status, code, err)
		}
	}
}

func TestUsageEnvironmentPrecedesConfigLoad(t *testing.T) {
	in, cg := optionsFixture(t)
	in.Config = "absent.yaml"
	in.Env["SANDBOX_STATS_INTERVAL"] = "invalid"
	_, _, storage, err := in.options(cg)
	var usage usageError
	if storage != nil || !errors.As(err, &usage) {
		t.Fatalf("usage validation order changed: %v", err)
	}
}
