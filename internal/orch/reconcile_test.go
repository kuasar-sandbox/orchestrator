package orch

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/launcher"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/secretbox"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/orchestrator/internal/vswitch"
)

type reconcileLauncher struct {
	mu        sync.Mutex
	units     []launcher.Unit
	stopped   []string
	reset     []string
	resources launcher.ResourceProperties
	stopErr   error
}

func (l *reconcileLauncher) Start(context.Context, string) error { return nil }
func (l *reconcileLauncher) Stop(_ context.Context, unit string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.stopped = append(l.stopped, unit)
	if l.stopErr != nil {
		return l.stopErr
	}
	for i := range l.units {
		if l.units[i].Name == unit {
			l.units[i].ActiveState = "inactive"
		}
	}
	return nil
}
func (l *reconcileLauncher) ResetFailed(_ context.Context, unit string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.reset = append(l.reset, unit)
	return nil
}
func (l *reconcileLauncher) List(_ context.Context, pattern string) ([]launcher.Unit, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	var matched []launcher.Unit
	for _, unit := range l.units {
		if ok, _ := filepath.Match(pattern, unit.Name); ok {
			matched = append(matched, unit)
		}
	}
	return matched, nil
}
func (l *reconcileLauncher) Reload(context.Context) error { return nil }
func (l *reconcileLauncher) SetResources(_ context.Context, _ string, p launcher.ResourceProperties) error {
	l.resources = p
	return nil
}
func (l *reconcileLauncher) Resources(context.Context, string, string) (launcher.ResourceProperties, error) {
	return l.resources, nil
}
func (l *reconcileLauncher) Close() error { return nil }

func (l *reconcileLauncher) setUnitState(name, state string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for i := range l.units {
		if l.units[i].Name == name {
			l.units[i].ActiveState = state
		}
	}
}

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
	cfg.Sandbox.TimeoutSec = 300
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
	resumeStarting.DeadlineUnix = 1_900_000_111
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
	resumeEmpty.DeadlineUnix = 1_900_000_222
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
		if tt.want == types.StatePaused {
			if !o.hasDeadlineIntent(tt.id) {
				t.Fatalf("interrupted resume %s lost durable deadline intent", tt.id)
			}
			if gotDeadline := o.resumeDeadline(got, nil); gotDeadline != got.DeadlineUnix {
				t.Fatalf("interrupted resume %s re-armed deadline=%d, want durable %d",
					tt.id, gotDeadline, got.DeadlineUnix)
			}
		} else if o.hasDeadlineIntent(tt.id) {
			t.Fatalf("interrupted fresh create %s gained resume deadline intent", tt.id)
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

func TestReconcileAdoptsLiveBuildAndCompletesWithoutReexecution(t *testing.T) {
	box, err := secretbox.NewFromColonHex(strings.Repeat("1", 64))
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(filepath.Join(t.TempDir(), "node.db"), box)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })

	runRoot := filepath.Join(t.TempDir(), "run")
	cfg := buildReconcileConfig(runRoot)
	runID := "br-00000000-0000-7000-8000-000000000001"
	unit := "sandbox-builder@" + runID + ".service"
	build := buildReconcileRow(t, runID)
	if err := os.MkdirAll(buildRuntimeDir(runRoot, build.BuildID), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := st.PutBuild(context.Background(), build); err != nil {
		t.Fatal(err)
	}
	lc := &reconcileLauncher{
		units: []launcher.Unit{{Name: unit, ActiveState: "active"}},
		resources: launcher.ResourceProperties{
			CPUQuotaPerSecUSec: 1_000_000,
			MemoryMax:          1 << 30,
		},
	}
	vs := &reconcileVS{}
	o := New(cfg, st, lc, vs, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := o.ReconcileSandboxes(ctx); err != nil {
		t.Fatal(err)
	}
	type buildSpecResult struct {
		ok  bool
		err error
	}
	specStarted := make(chan struct{})
	specDone := make(chan buildSpecResult, 1)
	go func() {
		close(specStarted)
		_, _, ok, err := o.BuildSpecFor(ctx, "build:"+build.BuildID)
		specDone <- buildSpecResult{ok: ok, err: err}
	}()
	<-specStarted
	imageRef := "manifest://" + strings.Repeat("b", 64)
	postStarted := make(chan struct{})
	postDone := make(chan error, 1)
	go func() {
		close(postStarted)
		postDone <- o.PostBuildResult(ctx, runID, build.BuildID, configsock.BuildResult{ImageRef: imageRef})
	}()
	<-postStarted
	select {
	case result := <-specDone:
		t.Fatalf("build spec crossed startup gate before adoption: %+v", result)
	case <-time.After(50 * time.Millisecond):
	}
	select {
	case err := <-postDone:
		t.Fatalf("build report crossed startup gate before adoption: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	if err := o.ReconcileBuilds(ctx); err != nil {
		t.Fatal(err)
	}
	if result := <-specDone; result.err != nil || !result.ok {
		t.Fatalf("recovered BuildSpec = ok %v err %v", result.ok, result.err)
	}

	// The recovered process has already run the pipeline. Posting its result must
	// finalize that same claim; no scheduler/assignment path is involved.
	lc.setUnitState(unit, "inactive")
	if err := <-postDone; err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		stored, err := st.GetBuild(ctx, build.BuildID)
		if err != nil {
			t.Fatal(err)
		}
		if stored.Status == types.BuildReady {
			if stored.ExecutionClaimed || stored.RuntimeVswitchPort != "" {
				t.Fatalf("terminal build retained ownership: %+v", stored)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("recovered build did not finish: %+v", stored)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := o.DrainBuilds(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(vs.detached) != 1 || vs.detached[0] != "17" {
		t.Fatalf("detached ports = %v", vs.detached)
	}
	if _, err := os.Stat(buildRuntimeDir(runRoot, build.BuildID)); !os.IsNotExist(err) {
		t.Fatalf("recovered workdir remains: %v", err)
	}
}

func TestReconcileRetainsExecutionClaimUntilInterruptedCleanupSucceeds(t *testing.T) {
	box, err := secretbox.NewFromColonHex(strings.Repeat("2", 64))
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(filepath.Join(t.TempDir(), "node.db"), box)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })

	cfg := buildReconcileConfig(filepath.Join(t.TempDir(), "run"))
	runID := "br-00000000-0000-7000-8000-000000000002"
	unit := "sandbox-builder@" + runID + ".service"
	build := buildReconcileRow(t, runID)
	if err := st.PutBuild(context.Background(), build); err != nil {
		t.Fatal(err)
	}
	lc := &reconcileLauncher{units: []launcher.Unit{{Name: unit, ActiveState: "failed"}}}
	vs := &reconcileVS{err: errors.New("connector unavailable")}
	o := New(cfg, st, lc, vs, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := o.Reconcile(context.Background()); err == nil {
		t.Fatal("cleanup failure did not fail closed")
	}
	stored, err := st.GetBuild(context.Background(), build.BuildID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Status != types.BuildBuilding || !stored.ExecutionClaimed || stored.RuntimeVswitchPort == "" {
		t.Fatalf("cleanup failure released durable usage: %+v", stored)
	}

	vs.err = nil
	if err := o.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	stored, err = st.GetBuild(context.Background(), build.BuildID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Status != types.BuildError || stored.ExecutionClaimed || stored.RuntimeVswitchPort != "" {
		t.Fatalf("successful retry did not terminally release: %+v", stored)
	}
}

func TestReconcileTreatsAlreadyDetachedBuildPortAsCompletedCleanup(t *testing.T) {
	box, err := secretbox.NewFromColonHex(strings.Repeat("7", 64))
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(filepath.Join(t.TempDir(), "node.db"), box)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })

	runID := "br-00000000-0000-7000-8000-000000000207"
	unit := "sandbox-builder@" + runID + ".service"
	build := buildReconcileRow(t, runID)
	if err := st.PutBuild(context.Background(), build); err != nil {
		t.Fatal(err)
	}
	o := New(
		buildReconcileConfig(filepath.Join(t.TempDir(), "run")),
		st,
		&reconcileLauncher{units: []launcher.Unit{{Name: unit, ActiveState: "failed"}}},
		&reconcileVS{err: vswitch.ErrPortNotAttached},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)

	if err := o.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile replay of completed detach: %v", err)
	}
	stored, err := st.GetBuild(context.Background(), build.BuildID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Status != types.BuildError || stored.ExecutionClaimed || stored.RuntimeVswitchPort != "" {
		t.Fatalf("already-detached cleanup did not release durable ownership: %+v", stored)
	}
}

func TestReplayClusterBuildTerminalStatesExceedsEventBuffer(t *testing.T) {
	box, err := secretbox.NewFromColonHex(strings.Repeat("8", 64))
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(filepath.Join(t.TempDir(), "node.db"), box)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })

	cfg := buildReconcileConfig(filepath.Join(t.TempDir(), "run"))
	o := New(cfg, st, &reconcileLauncher{}, &reconcileVS{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	total := cap(o.buildEvents) + 5
	for i := 0; i < total; i++ {
		build := buildReconcileRow(t, fmt.Sprintf("br-00000000-0000-7000-8000-%012d", i))
		build.BuildID = fmt.Sprintf("00000000-0000-7000-8000-%012d", i)
		build.TemplateID = fmt.Sprintf("transient-00000000-0000-7000-8000-%012d", i)
		build.ClusterGroup = "/recovery"
		build.RuntimeVswitchPort = ""
		build.RuntimeFloatingIP = ""
		build.RuntimePortMAC = ""
		if err := st.PutBuild(context.Background(), build); err != nil {
			t.Fatal(err)
		}
	}
	if err := o.ReconcileBuilds(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := len(o.buildEvents); got != cap(o.buildEvents) {
		t.Fatalf("recovery did not exercise the bounded event channel: len=%d cap=%d", got, cap(o.buildEvents))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- o.ReplayClusterBuildTerminalStates(ctx) }()
	seen := make(map[string]bool, total)
	doneCh := (<-chan error)(done)
	for len(seen) < total || doneCh != nil {
		select {
		case event := <-o.buildEvents:
			if event.State != string(types.BuildError) || event.Reason != "build unit was not live after controller restart" {
				t.Fatalf("replayed terminal event = %+v", event)
			}
			seen[event.BuildID] = true
		case err := <-doneCh:
			if err != nil {
				t.Fatal(err)
			}
			doneCh = nil
		case <-ctx.Done():
			t.Fatalf("terminal replay timed out after %d/%d unique builds: %v", len(seen), total, ctx.Err())
		}
	}
	if len(seen) != total {
		t.Fatalf("terminal replay delivered %d/%d unique builds", len(seen), total)
	}
}

func TestReconcileFailsClosedWhenLiveBuildResourcesDoNotMatch(t *testing.T) {
	box, err := secretbox.NewFromColonHex(strings.Repeat("3", 64))
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(filepath.Join(t.TempDir(), "node.db"), box)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })

	runID := "br-00000000-0000-7000-8000-000000000004"
	unit := "sandbox-builder@" + runID + ".service"
	build := buildReconcileRow(t, runID)
	if err := st.PutBuild(context.Background(), build); err != nil {
		t.Fatal(err)
	}
	lc := &reconcileLauncher{
		units: []launcher.Unit{{Name: unit, ActiveState: "active"}},
		resources: launcher.ResourceProperties{
			CPUQuotaPerSecUSec: 999_000,
			MemoryMax:          1 << 30,
		},
	}
	o := New(buildReconcileConfig(filepath.Join(t.TempDir(), "run")), st, lc, &reconcileVS{},
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := o.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	stored, err := st.GetBuild(context.Background(), build.BuildID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Status != types.BuildError || stored.ExecutionClaimed ||
		!strings.Contains(stored.Reason, "resource enforcement cannot be verified") {
		t.Fatalf("mismatched live build = %+v", stored)
	}
	if !containsString(lc.stopped, unit) {
		t.Fatalf("mismatched unit was not stopped: %v", lc.stopped)
	}
	if _, _, ok, err := o.BuildSpecFor(context.Background(), "build:"+build.BuildID); err != nil || ok {
		t.Fatalf("mismatched unit was adopted: ok=%t err=%v", ok, err)
	}
}

func TestReconcileRetainsClaimWhenLiveBuilderCannotBeStopped(t *testing.T) {
	box, err := secretbox.NewFromColonHex(strings.Repeat("4", 64))
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(filepath.Join(t.TempDir(), "node.db"), box)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })

	runID := "br-00000000-0000-7000-8000-000000000005"
	unit := "sandbox-builder@" + runID + ".service"
	build := buildReconcileRow(t, runID)
	if err := st.PutBuild(context.Background(), build); err != nil {
		t.Fatal(err)
	}
	stopErr := errors.New("systemd stop unavailable")
	lc := &reconcileLauncher{
		units:   []launcher.Unit{{Name: unit, ActiveState: "active"}},
		stopErr: stopErr,
		resources: launcher.ResourceProperties{
			CPUQuotaPerSecUSec: 999_000,
			MemoryMax:          1 << 30,
		},
	}
	vs := &reconcileVS{}
	o := New(buildReconcileConfig(filepath.Join(t.TempDir(), "run")), st, lc, vs,
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := o.Reconcile(context.Background()); !errors.Is(err, stopErr) {
		t.Fatalf("Reconcile error = %v, want stop failure", err)
	}
	stored, err := st.GetBuild(context.Background(), build.BuildID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Status != types.BuildBuilding || !stored.ExecutionClaimed || stored.RuntimeVswitchPort != "17" {
		t.Fatalf("failed stop released durable execution ownership: %+v", stored)
	}
	if len(vs.detached) != 0 {
		t.Fatalf("failed stop advanced to connector cleanup: %v", vs.detached)
	}
}

func TestRecoveredBuildPastDeadlineFailsClosedWhenUnitCannotStop(t *testing.T) {
	runID := "br-00000000-0000-7000-8000-000000000006"
	unit := "sandbox-builder@" + runID + ".service"
	stopErr := errors.New("systemd stop unavailable")
	lc := &reconcileLauncher{
		units:   []launcher.Unit{{Name: unit, ActiveState: "active"}},
		stopErr: stopErr,
	}
	o := &Orchestrator{
		cfg: buildReconcileConfig(t.TempDir()), lc: lc,
		log: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	build := buildReconcileRow(t, runID)
	build.ExecutionClaimedUnix = time.Now().Add(-2 * time.Hour).Unix()

	_, err := o.waitRecoveredBuild(context.Background(), build,
		&pendingBuild{result: make(chan configsock.BuildResult, 1)}, unit)
	if !errors.Is(err, errBuildCleanupPending) || !errors.Is(err, stopErr) {
		t.Fatalf("waitRecoveredBuild error = %v, want cleanup-pending stop failure", err)
	}
}

func buildReconcileConfig(runRoot string) *config.Config {
	policy := sandboxcfg.NodeResourcePolicy{}
	policy.ApplyDefaults()
	maxBuilds := int64(2)
	return &config.Config{
		Paths: config.PathsConfig{RunRoot: runRoot},
		Units: config.UnitsConfig{
			Runner: "sandbox-runner@.service", Builder: "sandbox-builder@.service", PoolWaitTimeout: "5s",
		},
		Sandbox: config.SandboxConfig{
			Resources: config.ResourcesConfig(policy),
			Network: config.NetworkConfig{
				Hostname: "sandbox", DNS: []string{"169.254.169.253"},
				Bare: config.ProfileNet{InnerIP: "169.254.1.1/31", Nexthop: "169.254.1.0"},
			},
		},
		Builder: config.BuilderConfig{
			Admission: config.BuilderAdmissionConfig{
				Registration: &config.BuildAdmissionLimitConfig{MaxBuilds: &maxBuilds},
				Execution:    &config.BuildAdmissionLimitConfig{MaxBuilds: &maxBuilds},
			},
			TotalTimeoutSec: 60,
		},
	}
}

func buildReconcileRow(t *testing.T, runID string) *types.Build {
	t.Helper()
	manifestKey := strings.Repeat("a", 64)
	return &types.Build{
		BuildID: "00000000-0000-7000-8000-000000000003", TemplateID: "transient-00000000-0000-7000-8000-000000000004",
		APISecret: deriveTestAPISecret(t, manifestKey), ManifestKey: manifestKey,
		Profile: types.ProfileBare, Kind: types.KindImg, Status: types.BuildBuilding,
		Resources:        types.BuildResources{CPU: 1000, Memory: 1 << 30},
		ExecutionClaimed: true, ExecutionClaimedUnix: time.Now().Unix(), RunID: runID,
		EnforcementStatus: "cpu,memory", RuntimeVswitchPort: "17", RuntimeFloatingIP: "192.0.2.17",
		RuntimePortMAC: "02:00:00:00:00:17", CreatedUnix: time.Now().Unix(),
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
