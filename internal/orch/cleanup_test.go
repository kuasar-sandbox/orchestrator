package orch

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/launcher"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/orchestrator/internal/vswitch"
)

type orderedCleanupLauncher struct {
	stopCalls  atomic.Int32
	resetCalls atomic.Int32
	stopErr    error
	resetErr   error
}

func (*orderedCleanupLauncher) Start(context.Context, string) error { return nil }
func (l *orderedCleanupLauncher) Stop(context.Context, string) error {
	if l.stopCalls.Add(1) == 1 {
		return l.stopErr
	}
	return nil
}
func (l *orderedCleanupLauncher) ResetFailed(context.Context, string) error {
	if l.resetCalls.Add(1) == 1 {
		return l.resetErr
	}
	return nil
}
func (*orderedCleanupLauncher) List(context.Context, string) ([]launcher.Unit, error) {
	return nil, nil
}
func (*orderedCleanupLauncher) Reload(context.Context) error { return nil }
func (*orderedCleanupLauncher) Close() error                 { return nil }

type orderedCleanupVS struct {
	detachCalls atomic.Int32
	detachErr   error
}

func (*orderedCleanupVS) Attach(context.Context, vswitch.AttachReq) (*vswitch.Port, error) {
	return nil, nil
}
func (v *orderedCleanupVS) Detach(context.Context, string) error {
	if v.detachCalls.Add(1) == 1 {
		return v.detachErr
	}
	return nil
}
func (*orderedCleanupVS) TapFD(string) vswitch.TapFD { return vswitch.TapFD{} }

func TestLaunchCleanupRetriesInOwnershipOrder(t *testing.T) {
	stopErr := errors.New("injected stop failure")
	resetErr := errors.New("injected reset failure")
	detachErr := errors.New("injected detach failure")
	lc := &orderedCleanupLauncher{stopErr: stopErr, resetErr: resetErr}
	vs := &orderedCleanupVS{detachErr: detachErr}
	cfg := &config.Config{}
	cfg.Units.Runner = "sandbox-runner@.service"
	o := &Orchestrator{cfg: cfg, lc: lc, vs: vs}

	root := t.TempDir()
	sb := &types.Sandbox{
		ID: "ordered-cleanup", RunID: "sr-00000000-0000-7000-8000-000000000001",
		VswitchPort: "ordered-port",
		RunDir:      filepath.Join(root, "run"), BaseDir: filepath.Join(root, "base"),
	}
	for _, dir := range []string{sb.RunDir, sb.BaseDir} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	attempt := &launchAttempt{sid: sb.ID, kind: launchCreate, runID: sb.RunID}

	if err := o.stepLaunchCleanup(context.Background(), attempt, sb, true); !errors.Is(err, stopErr) {
		t.Fatalf("first cleanup error = %v, want stop failure", err)
	}
	if lc.resetCalls.Load() != 0 || vs.detachCalls.Load() != 0 {
		t.Fatalf("cleanup advanced past failed Stop: reset=%d detach=%d", lc.resetCalls.Load(), vs.detachCalls.Load())
	}
	assertCleanupDirsExist(t, sb)

	if err := o.stepLaunchCleanup(context.Background(), attempt, sb, true); !errors.Is(err, resetErr) {
		t.Fatalf("second cleanup error = %v, want reset failure", err)
	}
	if vs.detachCalls.Load() != 0 {
		t.Fatalf("cleanup advanced past failed Reset: detach=%d", vs.detachCalls.Load())
	}
	assertCleanupDirsExist(t, sb)

	if err := o.stepLaunchCleanup(context.Background(), attempt, sb, true); !errors.Is(err, detachErr) {
		t.Fatalf("third cleanup error = %v, want detach failure", err)
	}
	assertCleanupDirsExist(t, sb)

	if err := o.stepLaunchCleanup(context.Background(), attempt, sb, true); err != nil {
		t.Fatalf("final cleanup: %v", err)
	}
	for _, dir := range []string{sb.RunDir, sb.BaseDir} {
		if _, err := os.Stat(dir); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("cleanup retained %s: %v", dir, err)
		}
	}
	if lc.stopCalls.Load() != 2 || lc.resetCalls.Load() != 2 || vs.detachCalls.Load() != 2 {
		t.Fatalf("cleanup calls stop=%d reset=%d detach=%d, want two each",
			lc.stopCalls.Load(), lc.resetCalls.Load(), vs.detachCalls.Load())
	}
}

func TestReconcileCleanupStopsBeforeLaterOwnership(t *testing.T) {
	stopErr := errors.New("injected reconcile stop failure")
	lc := &orderedCleanupLauncher{stopErr: stopErr}
	vs := &orderedCleanupVS{}
	cfg := &config.Config{}
	cfg.Units.Runner = "sandbox-runner@.service"
	o := &Orchestrator{cfg: cfg, lc: lc, vs: vs}

	root := t.TempDir()
	sb := &types.Sandbox{
		ID: "reconcile-ordered-cleanup", RunID: "sr-00000000-0000-7000-8000-000000000002",
		VswitchPort: "reconcile-ordered-port",
		RunDir:      filepath.Join(root, "run"),
	}
	if err := os.MkdirAll(sb.RunDir, 0o700); err != nil {
		t.Fatal(err)
	}

	if err := o.teardownPersistedOwnership(context.Background(), sb); !errors.Is(err, stopErr) {
		t.Fatalf("reconcile cleanup error = %v, want stop failure", err)
	}
	if lc.resetCalls.Load() != 0 || vs.detachCalls.Load() != 0 {
		t.Fatalf("reconcile cleanup advanced past failed Stop: reset=%d detach=%d",
			lc.resetCalls.Load(), vs.detachCalls.Load())
	}
	if _, err := os.Stat(sb.RunDir); err != nil {
		t.Fatalf("reconcile cleanup removed run dir before ownership release: %v", err)
	}
}

func assertCleanupDirsExist(t *testing.T, sb *types.Sandbox) {
	t.Helper()
	for _, dir := range []string{sb.RunDir, sb.BaseDir} {
		if _, err := os.Stat(dir); err != nil {
			t.Fatalf("cleanup removed %s before ownership release: %v", dir, err)
		}
	}
}
