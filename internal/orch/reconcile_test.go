package orch

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/launcher"
	"github.com/kuasar-sandbox/orchestrator/internal/secretbox"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/orchestrator/internal/vswitch"
)

type reconcileLauncher struct {
	units   []launcher.Unit
	stopped []string
	reset   []string
}

func (l *reconcileLauncher) Start(context.Context, string) error { return nil }
func (l *reconcileLauncher) Stop(_ context.Context, unit string) error {
	l.stopped = append(l.stopped, unit)
	return nil
}
func (l *reconcileLauncher) ResetFailed(_ context.Context, unit string) error {
	l.reset = append(l.reset, unit)
	return nil
}
func (l *reconcileLauncher) List(context.Context, string) ([]launcher.Unit, error) {
	return append([]launcher.Unit(nil), l.units...), nil
}
func (l *reconcileLauncher) Reload(context.Context) error { return nil }
func (l *reconcileLauncher) Close() error                 { return nil }

type reconcileVS struct {
	detached []string
	err      error
}

func (*reconcileVS) Attach(context.Context, vswitch.AttachReq) (*vswitch.Port, error) {
	return nil, nil
}
func (v *reconcileVS) Detach(_ context.Context, port string) error {
	v.detached = append(v.detached, port)
	return v.err
}
func (*reconcileVS) TapFD(string) vswitch.TapFD { return vswitch.TapFD{} }

func TestReconcileCleansOrphanPoolRunners(t *testing.T) {
	box, err := secretbox.NewFromColonHex(strings.Repeat("0", 64))
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(filepath.Join(t.TempDir(), "node.db"), box)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })

	cfg := &config.Config{}
	cfg.Paths.RunRoot = filepath.Join(t.TempDir(), "run")
	cfg.Paths.BaseRoot = filepath.Join(t.TempDir(), "base")
	cfg.Units.Runner = "sandbox-runner@.service"
	cfg.Units.Builder = "sandbox-builder@.service"
	knownRun := "sr-00000000-0000-7000-8000-000000000001"
	knownUnit := "sandbox-runner@" + knownRun + ".service"
	orphanUnit := "sandbox-runner@sr-00000000-0000-7000-8000-000000000002.service"
	failedUnit := "sandbox-runner@sr-00000000-0000-7000-8000-000000000003.service"
	createStartingRun := "sr-00000000-0000-7000-8000-000000000004"
	createStartingUnit := "sandbox-runner@" + createStartingRun + ".service"
	resumeStartingRun := "sr-00000000-0000-7000-8000-000000000005"
	resumeStartingUnit := "sandbox-runner@" + resumeStartingRun + ".service"
	manifestKey := strings.Repeat("a", 64)
	sb := &types.Sandbox{
		ID: "sandbox-1", TemplateID: types.TemplateID{Profile: types.ProfileBare, Kind: types.KindImg, Ref: "manifest://" + strings.Repeat("b", 64)}.String(),
		Profile: types.ProfileBare, State: types.StateRunning, RunID: knownRun,
		RunDir:    filepath.Join(cfg.Paths.RunRoot, "sandbox-1"),
		BaseDir:   filepath.Join(cfg.Paths.BaseRoot, "sandbox-1"),
		APISecret: deriveTestAPISecret(t, manifestKey), ManifestKey: manifestKey, CreatedUnix: 1,
	}
	materializeTestSandboxCredentials(t, sb)
	if err := st.Put(context.Background(), sb); err != nil {
		t.Fatal(err)
	}
	createStarting := *sb
	createStarting.ID = "create-starting"
	createStarting.State = types.StateStarting
	createStarting.RunID = createStartingRun
	createStarting.SnapshotRef = ""
	createStarting.RunDir = filepath.Join(cfg.Paths.RunRoot, createStarting.ID)
	createStarting.BaseDir = filepath.Join(cfg.Paths.BaseRoot, createStarting.ID)
	createStarting.VswitchPort = "create-assigned-port"
	createStarting.FloatingIP = "192.0.2.10"
	materializeTestSandboxCredentials(t, &createStarting)
	if err := st.Put(context.Background(), &createStarting); err != nil {
		t.Fatal(err)
	}
	resumeStarting := *sb
	resumeStarting.ID = "resume-starting"
	resumeStarting.State = types.StateStarting
	resumeStarting.RunID = resumeStartingRun
	resumeStarting.SnapshotRef = "manifest://" + strings.Repeat("c", 64)
	resumeStarting.RunDir = filepath.Join(cfg.Paths.RunRoot, resumeStarting.ID)
	resumeStarting.BaseDir = filepath.Join(cfg.Paths.BaseRoot, resumeStarting.ID)
	resumeStarting.VswitchPort = "resume-assigned-port"
	resumeStarting.FloatingIP = "192.0.2.11"
	materializeTestSandboxCredentials(t, &resumeStarting)
	if err := st.Put(context.Background(), &resumeStarting); err != nil {
		t.Fatal(err)
	}
	createEmpty := createStarting
	createEmpty.ID = "create-starting-empty-run"
	createEmpty.RunID = ""
	createEmpty.VswitchPort = "create-empty-port"
	createEmpty.FloatingIP = "192.0.2.12"
	createEmpty.RunDir = filepath.Join(cfg.Paths.RunRoot, createEmpty.ID)
	createEmpty.BaseDir = filepath.Join(cfg.Paths.BaseRoot, createEmpty.ID)
	materializeTestSandboxCredentials(t, &createEmpty)
	if err := st.Put(context.Background(), &createEmpty); err != nil {
		t.Fatal(err)
	}
	resumeEmpty := resumeStarting
	resumeEmpty.ID = "resume-starting-empty-run"
	resumeEmpty.RunID = ""
	resumeEmpty.VswitchPort = "resume-empty-port"
	resumeEmpty.FloatingIP = "192.0.2.13"
	resumeEmpty.RunDir = filepath.Join(cfg.Paths.RunRoot, resumeEmpty.ID)
	resumeEmpty.BaseDir = filepath.Join(cfg.Paths.BaseRoot, resumeEmpty.ID)
	materializeTestSandboxCredentials(t, &resumeEmpty)
	if err := st.Put(context.Background(), &resumeEmpty); err != nil {
		t.Fatal(err)
	}
	for _, starting := range []*types.Sandbox{&createStarting, &resumeStarting, &createEmpty, &resumeEmpty} {
		if err := os.MkdirAll(starting.RunDir, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(starting.RunDir, "ready.sock"), []byte("stale"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(starting.BaseDir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	lc := &reconcileLauncher{units: []launcher.Unit{
		{Name: knownUnit, ActiveState: "active"},
		{Name: orphanUnit, ActiveState: "active"},
		{Name: failedUnit, ActiveState: "failed"},
		{Name: createStartingUnit, ActiveState: "active"},
		{Name: resumeStartingUnit, ActiveState: "active"},
	}}
	vs := &reconcileVS{}
	o := New(cfg, st, lc, vs, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := o.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if o.lookup(sb.ID) == nil {
		t.Fatal("known running sandbox was not adopted")
	}
	for _, unit := range []string{orphanUnit, failedUnit, createStartingUnit, resumeStartingUnit} {
		if !containsString(lc.stopped, unit) || !containsString(lc.reset, unit) {
			t.Fatalf("orphan %s cleanup: stopped=%v reset=%v", unit, lc.stopped, lc.reset)
		}
	}
	if containsString(lc.stopped, knownUnit) || containsString(lc.reset, knownUnit) {
		t.Fatalf("known unit was cleaned: stopped=%v reset=%v", lc.stopped, lc.reset)
	}
	for _, tt := range []struct {
		id   string
		want types.State
	}{
		{id: createStarting.ID, want: types.StateDead},
		{id: resumeStarting.ID, want: types.StatePaused},
		{id: createEmpty.ID, want: types.StateDead},
		{id: resumeEmpty.ID, want: types.StatePaused},
	} {
		got, err := st.Get(context.Background(), tt.id)
		if err != nil || got == nil || got.State != tt.want || got.RunID != "" || got.VswitchPort != "" || got.FloatingIP != "" {
			t.Fatalf("reconciled %s = %+v, %v; want %s", tt.id, got, err, tt.want)
		}
		if o.lookup(tt.id) != nil {
			t.Fatalf("interrupted starting sandbox %s was adopted into cache", tt.id)
		}
	}
	for _, port := range []string{"create-assigned-port", "resume-assigned-port", "create-empty-port", "resume-empty-port"} {
		if !containsString(vs.detached, port) {
			t.Fatalf("persisted network %s was not detached: %v", port, vs.detached)
		}
	}
	for _, starting := range []*types.Sandbox{&createStarting, &resumeStarting, &createEmpty, &resumeEmpty} {
		if _, err := os.Stat(starting.RunDir); !os.IsNotExist(err) {
			t.Fatalf("stale run directory %s remains: %v", starting.RunDir, err)
		}
	}
	for _, fresh := range []*types.Sandbox{&createStarting, &createEmpty} {
		if _, err := os.Stat(fresh.BaseDir); !os.IsNotExist(err) {
			t.Fatalf("fresh-create base directory %s remains: %v", fresh.BaseDir, err)
		}
	}
	for _, resume := range []*types.Sandbox{&resumeStarting, &resumeEmpty} {
		if _, err := os.Stat(resume.BaseDir); err != nil {
			t.Fatalf("resume base directory %s was removed: %v", resume.BaseDir, err)
		}
	}
}

func TestReconcileCleanupFailurePreservesStartingOwnership(t *testing.T) {
	box, err := secretbox.NewFromColonHex(strings.Repeat("0", 64))
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(filepath.Join(t.TempDir(), "node.db"), box)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })

	cfg := &config.Config{}
	cfg.Paths.RunRoot = filepath.Join(t.TempDir(), "run")
	cfg.Paths.BaseRoot = filepath.Join(t.TempDir(), "base")
	cfg.Units.Runner = "sandbox-runner@.service"
	runID := "sr-00000000-0000-7000-8000-000000000099"
	sb := &types.Sandbox{
		ID: "cleanup-failure", Profile: types.ProfileBare,
		TemplateID: types.TemplateID{
			Profile: types.ProfileBare, Kind: types.KindImg,
			Ref: "manifest://" + strings.Repeat("b", 64),
		}.String(),
		State: types.StateStarting, RunID: runID,
		RunDir:      filepath.Join(cfg.Paths.RunRoot, "cleanup-failure"),
		BaseDir:     filepath.Join(cfg.Paths.BaseRoot, "cleanup-failure"),
		VswitchPort: "still-owned-port", FloatingIP: "192.0.2.99",
		APISecret: deriveTestAPISecret(t, strings.Repeat("a", 64)), ManifestKey: strings.Repeat("a", 64),
		CreatedUnix: 1,
	}
	materializeTestSandboxCredentials(t, sb)
	if err := st.Put(context.Background(), sb); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(sb.BaseDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(sb.RunDir, 0o700); err != nil {
		t.Fatal(err)
	}

	lc := &reconcileLauncher{units: []launcher.Unit{{
		Name: "sandbox-runner@" + runID + ".service", ActiveState: "active",
	}}}
	detachErr := errors.New("injected reconcile detach failure")
	vs := &reconcileVS{err: detachErr}
	o := New(cfg, st, lc, vs, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := o.Reconcile(context.Background()); !errors.Is(err, detachErr) {
		t.Fatalf("Reconcile error = %v, want detach failure", err)
	}
	stored, err := st.Get(context.Background(), sb.ID)
	if err != nil || stored == nil || stored.State != types.StateStarting ||
		stored.RunID != runID || stored.VswitchPort != sb.VswitchPort || stored.FloatingIP != sb.FloatingIP {
		t.Fatalf("starting ownership after failed reconcile cleanup = %+v, %v", stored, err)
	}
	if _, err := os.Stat(sb.BaseDir); err != nil {
		t.Fatalf("fresh base dir removed before cleanup completed: %v", err)
	}
	if _, err := os.Stat(sb.RunDir); err != nil {
		t.Fatalf("run dir removed before network ownership was released: %v", err)
	}
	if o.lookup(sb.ID) != nil {
		t.Fatal("failed reconcile adopted starting sandbox into cache")
	}
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
