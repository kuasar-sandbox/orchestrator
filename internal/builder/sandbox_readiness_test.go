package builder

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
)

func TestAttachReadinessPipeUsesNextExtraFilesFD(t *testing.T) {
	firstR, firstW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer firstR.Close()
	defer firstW.Close()
	cmd := exec.Command("/bin/true")
	cmd.ExtraFiles = []*os.File{firstW}
	readyR, readyW, childFD, err := attachReadinessPipe(cmd)
	if err != nil {
		t.Fatal(err)
	}
	defer readyR.Close()
	defer readyW.Close()
	if childFD != 4 {
		t.Fatalf("child fd = %d, want 4", childFD)
	}
	if got := cmd.Args[len(cmd.Args)-1]; got != "--ready-fd=4" {
		t.Fatalf("ready arg = %q", got)
	}
	if len(cmd.ExtraFiles) != 2 || cmd.ExtraFiles[1] != readyW {
		t.Fatalf("ExtraFiles = %#v", cmd.ExtraFiles)
	}
}

func TestPhaseSandboxRuntimeReadiness(t *testing.T) {
	tests := []struct {
		name    string
		wire    string
		wantErr bool
	}{
		{name: "exact", wire: "control_ready\nready\n"},
		{name: "EOF before control", wire: "", wantErr: true},
		{name: "EOF before ready", wire: "control_ready\n", wantErr: true},
		{name: "unknown", wire: "unknown\nready\n", wantErr: true},
		{name: "reverse", wire: "ready\ncontrol_ready\n", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sb, writer := pipePhaseSandbox(t)
			go func() {
				_, _ = io.WriteString(writer, tt.wire)
				_ = writer.Close()
			}()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			err := sb.waitRuntimeReady(ctx)
			if (err != nil) != tt.wantErr {
				t.Fatalf("waitRuntimeReady error = %v, want error=%t", err, tt.wantErr)
			}
		})
	}
}

func TestPhaseSandboxRuntimeReadinessTimeoutClosesReader(t *testing.T) {
	sb, writer := pipePhaseSandbox(t)
	defer writer.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := sb.waitRuntimeReady(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waitRuntimeReady error = %v, want deadline exceeded", err)
	}
	if _, err := writer.Write([]byte("control_ready\n")); err == nil {
		t.Fatal("readiness reader remained open after timeout")
	}
}

func TestPhaseSandboxRuntimeReadinessObservesChildExit(t *testing.T) {
	sb, writer := pipePhaseSandbox(t)
	defer writer.Close()
	wantErr := errors.New("child failed")
	sb.waitMu.Lock()
	sb.waitErr = wantErr
	sb.waitMu.Unlock()
	close(sb.done)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := sb.waitRuntimeReady(ctx); !errors.Is(err, wantErr) {
		t.Fatalf("waitRuntimeReady error = %v, want child error", err)
	}
}

func TestPhaseSandboxDoneBroadcastAndTeardownAfterExit(t *testing.T) {
	sb, writer := pipePhaseSandbox(t)
	_ = writer.Close()
	sb.cmd = &exec.Cmd{}
	wantErr := errors.New("wait result")
	sb.waitMu.Lock()
	sb.waitErr = wantErr
	sb.waitMu.Unlock()

	const observers = 4
	var wg sync.WaitGroup
	errs := make(chan error, observers)
	wg.Add(observers)
	for range observers {
		go func() {
			defer wg.Done()
			<-sb.done
			errs <- sb.waitError()
		}()
	}
	close(sb.done)
	wg.Wait()
	close(errs)
	for err := range errs {
		if !errors.Is(err, wantErr) {
			t.Fatalf("observer error = %v", err)
		}
	}

	returned := make(chan struct{})
	go func() {
		sb.teardown()
		close(returned)
	}()
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("teardown blocked after another observer saw child exit")
	}
}

func TestStartSandboxPassesReadinessFDAndClosesParentWriter(t *testing.T) {
	dir := t.TempDir()
	script := writeSandboxCtlTestScript(t, dir)
	p := &buildPipeline{
		spec: &configsock.BuildSpec{
			BuildID: "build-123456", RunID: "run-1", Workdir: dir,
			Paths:     configsock.BuildPaths{SandboxCtl: script},
			Resources: rtconfig.ResourcesConfig{Capacity: rtconfig.CapacityConfig{CPU: 2}},
		},
		log: testBuilderLogger(), vmmCgroup: testVMMCgroupFD(t),
	}
	sb, err := p.startSandbox("a", map[string]any{"launch": map[string]any{"placeholder": true}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer sb.teardown()

	var readyArg string
	for _, arg := range sb.cmd.Args {
		if strings.HasPrefix(arg, "--ready-fd=") {
			readyArg = arg
		}
	}
	if readyArg != "--ready-fd=4" {
		t.Fatalf("ready arg = %q, want --ready-fd=4", readyArg)
	}
	if len(sb.cmd.ExtraFiles) != 2 {
		t.Fatalf("ExtraFiles count = %d", len(sb.cmd.ExtraFiles))
	}
	if _, err := sb.cmd.ExtraFiles[1].Write([]byte("x")); err == nil {
		t.Fatal("parent readiness writer remained open after Start")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := sb.waitRuntimeReady(ctx); err != nil {
		t.Fatalf("waitRuntimeReady: %v", err)
	}
}

func TestStartSandboxFailureClosesReadinessPipe(t *testing.T) {
	vmmCgroup := testVMMCgroupFD(t)
	before, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Skipf("count open descriptors: %v", err)
	}
	p := &buildPipeline{
		spec: &configsock.BuildSpec{
			BuildID: "build-failure", Workdir: t.TempDir(),
			Paths: configsock.BuildPaths{SandboxCtl: "/definitely/missing/sandbox-ctl"},
		},
		log: testBuilderLogger(), vmmCgroup: vmmCgroup,
	}
	for range 10 {
		if _, err := p.startSandbox("a", map[string]any{}, nil); err == nil {
			t.Fatal("startSandbox unexpectedly succeeded")
		}
	}
	after, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != len(before) {
		t.Fatalf("open fd count after failed starts = %d, before = %d", len(after), len(before))
	}
}

func TestPhaseImportUsesRuntimeEventsWithoutExecProbe(t *testing.T) {
	dir := t.TempDir()
	script := writeSandboxCtlTestScript(t, dir)
	logPath := filepath.Join(dir, "sandbox-ctl.log")
	t.Setenv("BUILDER_TEST_SANDBOX_CTL_LOG", logPath)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	p := &buildPipeline{
		spec: &configsock.BuildSpec{
			BuildID: "build-123456", RunID: "run-1", Workdir: dir, FromImage: "example.invalid/image:latest",
			Paths:     configsock.BuildPaths{SandboxCtl: script},
			Timeouts:  configsock.BuildTimeouts{PullSec: 1},
			Resources: rtconfig.ResourcesConfig{Capacity: rtconfig.CapacityConfig{CPU: 2}},
		},
		ctx: ctx, log: testBuilderLogger(), vmmCgroup: testVMMCgroupFD(t),
	}
	if err := p.phaseImport(); err == nil {
		t.Fatal("phaseImport unexpectedly succeeded with failing fake exec")
	}
	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	got := string(data)
	if strings.Contains(got, "/.probe") || strings.Contains(got, " mountpoint ") {
		t.Fatalf("phase A still issued the old exec readiness probe: %s", got)
	}
	if !strings.Contains(got, guestFlatten) || !strings.Contains(got, " export ") {
		t.Fatalf("first phase A exec was not the import operation: %s", got)
	}
}

func testVMMCgroupFD(t *testing.T) *os.File {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "cgroup.events"), []byte("populated 0\nfrozen 0\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "cpu.max"), []byte("200000 100000\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	fd, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fd.Close() })
	return fd
}

func pipePhaseSandbox(t *testing.T) (*phaseSandbox, *os.File) {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	p := &buildPipeline{log: testBuilderLogger()}
	return &phaseSandbox{p: p, cmd: &exec.Cmd{}, done: make(chan struct{}), readyR: r}, w
}

func testBuilderLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func writeSandboxCtlTestScript(t *testing.T, dir string) string {
	t.Helper()
	path := filepath.Join(dir, "sandbox-ctl-test")
	script := `#!/bin/bash
set -eu
case "$1" in
run)
  ready_fd=""
  for arg in "$@"; do
    case "$arg" in --ready-fd=*) ready_fd="${arg#*=}";; esac
  done
  test -n "$ready_fd"
  eval "printf 'control_ready\\nready\\n' >&${ready_fd}"
  eval "exec ${ready_fd}>&-"
  exec sleep 30
  ;;
exec)
  printf '%s\n' "$*" >> "${BUILDER_TEST_SANDBOX_CTL_LOG:-/dev/null}"
  exit 42
  ;;
*)
  exit 43
  ;;
esac
`
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return path
}
