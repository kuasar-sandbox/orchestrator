package orch

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/configresolve"
	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/launcher"
	"github.com/kuasar-sandbox/orchestrator/internal/nodepath"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/secretbox"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/orchestrator/internal/vswitch"
	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
)

type reconcileLauncher struct {
	mu                 sync.Mutex
	units              []launcher.Unit
	stopped            []string
	reset              []string
	resources          launcher.ResourceProperties
	resourcesErr       error
	resourcesErrByUnit map[string]error
	stopErr            error
	inactiveAfterLists int
	listCalls          int
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
	l.listCalls++
	if l.inactiveAfterLists > 0 && l.listCalls >= l.inactiveAfterLists {
		for i := range l.units {
			l.units[i].ActiveState = "inactive"
		}
	}
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
func (l *reconcileLauncher) Resources(_ context.Context, unit, _ string) (launcher.ResourceProperties, error) {
	if err := l.resourcesErrByUnit[unit]; err != nil {
		return launcher.ResourceProperties{}, err
	}
	return l.resources, l.resourcesErr
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
	attach   *vswitch.Port
	attaches int
}

func (v *reconcileVS) Attach(context.Context, vswitch.AttachReq) (*vswitch.Port, error) {
	v.attaches++
	return v.attach, nil
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
		RunDir:    nodepath.SandboxRunDir(cfg.Paths.RunRoot, "sandbox-1"),
		BaseDir:   nodepath.SandboxBaseDir(cfg.Paths.BaseRoot, "sandbox-1"),
		APISecret: deriveTestAPISecret(t, manifestKey), ManifestKey: manifestKey, CreatedUnix: 1,
	}
	materializeTestSandboxCredentials(t, sb)
	if err := st.Put(context.Background(), sb); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{sb.RunDir, sb.BaseDir} {
		if err := os.MkdirAll(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	createStarting := *sb
	createStarting.ID = "create-starting"
	createStarting.State = types.StateStarting
	createStarting.RunID = createStartingRun
	createStarting.ResumeSource = types.ResumeSource{}
	createStarting.LaunchMode = types.LaunchImage
	createStarting.RunDir = nodepath.SandboxRunDir(cfg.Paths.RunRoot, createStarting.ID)
	createStarting.BaseDir = nodepath.SandboxBaseDir(cfg.Paths.BaseRoot, createStarting.ID)
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
	resumeStarting.ResumeSource = types.ResumeSource{Kind: types.ResumeSourceSnapshot, Ref: "manifest://" + strings.Repeat("c", 64)}
	resumeStarting.LaunchMode = types.LaunchMemory
	resumeStarting.DeadlineUnix = 1_900_000_111
	resumeStarting.RunDir = nodepath.SandboxRunDir(cfg.Paths.RunRoot, resumeStarting.ID)
	resumeStarting.BaseDir = nodepath.SandboxBaseDir(cfg.Paths.BaseRoot, resumeStarting.ID)
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
	createEmpty.RunDir = nodepath.SandboxRunDir(cfg.Paths.RunRoot, createEmpty.ID)
	createEmpty.BaseDir = nodepath.SandboxBaseDir(cfg.Paths.BaseRoot, createEmpty.ID)
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
	resumeEmpty.RunDir = nodepath.SandboxRunDir(cfg.Paths.RunRoot, resumeEmpty.ID)
	resumeEmpty.BaseDir = nodepath.SandboxBaseDir(cfg.Paths.BaseRoot, resumeEmpty.ID)
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
		id       string
		want     types.State
		wantMode types.LaunchMode
	}{
		{id: createStarting.ID, want: types.StateDead},
		{id: resumeStarting.ID, want: types.StateStarting, wantMode: types.LaunchMemory},
		{id: createEmpty.ID, want: types.StateDead},
		{id: resumeEmpty.ID, want: types.StateStarting, wantMode: types.LaunchMemory},
	} {
		got, err := st.Get(context.Background(), tt.id)
		if err != nil || got == nil || got.State != tt.want || got.LaunchMode != tt.wantMode ||
			got.RunID != "" || got.VswitchPort != "" || got.FloatingIP != "" {
			t.Fatalf("reconciled %s = %+v, %v; want %s", tt.id, got, err, tt.want)
		}
		if o.lookup(tt.id) != nil {
			t.Fatalf("interrupted starting sandbox %s was adopted into cache", tt.id)
		}
		if tt.want == types.StateStarting {
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

func TestReconcileCompletesPausedExactOwnershipCleanup(t *testing.T) {
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
	runID := "sr-00000000-0000-7000-8000-000000000066"
	manifestKey := strings.Repeat("a", 64)
	source := types.ResumeSource{Kind: types.ResumeSourceSandbox, Ref: "manifest://" + strings.Repeat("b", 64)}
	sb := &types.Sandbox{
		ID: "paused-interrupted-cleanup", Profile: types.ProfileBare,
		TemplateID: types.TemplateID{Profile: types.ProfileBare, Kind: types.KindImg, Ref: "manifest://" + strings.Repeat("c", 64)}.String(),
		State:      types.StatePaused, ResumeSource: source,
		RunID: runID, VswitchPort: "paused-old-port", FloatingIP: "192.0.2.66",
		InnerIP: "198.51.100.66/31", PortMAC: "02:00:00:00:00:66",
		RunDir: nodepath.SandboxRunDir(cfg.Paths.RunRoot, "paused-interrupted-cleanup"), BaseDir: nodepath.SandboxBaseDir(cfg.Paths.BaseRoot, "paused-interrupted-cleanup"),
		APISecret: deriveTestAPISecret(t, manifestKey), ManifestKey: manifestKey, CreatedUnix: 1, AutoPauseMemory: true,
	}
	materializeTestSandboxCredentials(t, sb)
	if err := st.Put(context.Background(), sb); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{sb.RunDir, sb.BaseDir} {
		if err := os.MkdirAll(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	unit := "sandbox-runner@" + runID + ".service"
	lc := &reconcileLauncher{units: []launcher.Unit{{Name: unit, ActiveState: "active"}}}
	vs := &reconcileVS{}
	o := New(cfg, st, lc, vs, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := o.ReconcileSandboxes(context.Background()); err != nil {
		t.Fatal(err)
	}

	got, err := st.Get(context.Background(), sb.ID)
	if err != nil || got == nil || got.State != types.StatePaused || got.ResumeSource != source ||
		got.RunID != "" || got.VswitchPort != "" || got.FloatingIP != "" || got.InnerIP != "" || got.PortMAC != "" ||
		got.RunDir != "" || got.EnvdUDS != "" || got.CiUDS != "" {
		t.Fatalf("reconciled paused row = %+v, %v", got, err)
	}
	if !containsString(lc.stopped, unit) || !containsString(lc.reset, unit) {
		t.Fatalf("paused runner cleanup stopped=%v reset=%v", lc.stopped, lc.reset)
	}
	if !containsString(vs.detached, "paused-old-port") {
		t.Fatalf("paused network cleanup = %v", vs.detached)
	}
	if cached := o.lookup(sb.ID); cached == nil || cached.RunID != "" || cached.VswitchPort != "" ||
		cached.RunDir != "" || cached.ResumeSource != source {
		t.Fatalf("reconciled paused cache = %+v", cached)
	}
	if _, err := os.Stat(sb.RunDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("paused reconcile retained RunDir: %v", err)
	}
	if _, err := os.Stat(sb.BaseDir); err != nil {
		t.Fatalf("paused reconcile removed BaseDir: %v", err)
	}
}

func TestReconcileRetriesSnapshotColdResumeWithDurableLaunchMode(t *testing.T) {
	dir := shortOrchestratorTestDir(t)
	box, err := secretbox.NewFromColonHex(strings.Repeat("0", 64))
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(filepath.Join(dir, "node.db"), box)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })

	cfg := &config.Config{}
	cfg.Paths.RunRoot = filepath.Join(dir, "run")
	cfg.Paths.BaseRoot = filepath.Join(dir, "base")
	cfg.Units.Runner = "sandbox-runner@.service"
	cfg.Units.Builder = "sandbox-builder@.service"
	cfg.Sandbox.Network.Bare.InnerIP = "169.254.1.1/31"
	oldRunID := "sr-00000000-0000-7000-8000-000000000077"
	manifestKey := strings.Repeat("a", 64)
	source := types.ResumeSource{
		Kind: types.ResumeSourceSnapshot,
		Ref:  "manifest://" + strings.Repeat("c", 64),
	}
	sb := &types.Sandbox{
		ID: "recover-snapshot-cold", Profile: types.ProfileBare,
		TemplateID: types.TemplateID{
			Profile: types.ProfileBare, Kind: types.KindImg,
			Ref: "manifest://" + strings.Repeat("b", 64),
		}.String(),
		State: types.StateStarting, ResumeSource: source, LaunchMode: types.LaunchCold,
		RunID: oldRunID, VswitchPort: "old-port", FloatingIP: "192.0.2.77",
		RunDir:    nodepath.SandboxRunDir(cfg.Paths.RunRoot, "recover-snapshot-cold"),
		BaseDir:   nodepath.SandboxBaseDir(cfg.Paths.BaseRoot, "recover-snapshot-cold"),
		APISecret: deriveTestAPISecret(t, manifestKey), ManifestKey: manifestKey,
		CreatedUnix: 1, DeadlineUnix: time.Now().Add(time.Hour).Unix(), AutoPauseMemory: true,
	}
	materializeTestSandboxCredentials(t, sb)
	if err := st.Put(context.Background(), sb); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{sb.RunDir, sb.BaseDir} {
		if err := os.MkdirAll(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}

	launchModes := make(chan string, 1)
	launcher := &countingLauncher{
		listedUnits: []launcher.Unit{{
			Name: "sandbox-runner@" + oldRunID + ".service", ActiveState: "active",
		}},
		artifactLaunchModes: launchModes,
	}
	vs := &checkpointVS{}
	o := New(cfg, st, launcher, vs, slog.New(slog.NewTextHandler(io.Discard, nil)))
	launcher.orch = o
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	o.SetClusterContext(ctx)
	if err := o.ReconcileSandboxes(ctx); err != nil {
		t.Fatal(err)
	}
	recovered, err := st.Get(ctx, sb.ID)
	if err != nil || recovered == nil || recovered.State != types.StateStarting || recovered.RunID != "" ||
		recovered.ResumeSource != source || recovered.LaunchMode != types.LaunchCold {
		t.Fatalf("reconciled cold resume = %+v, %v", recovered, err)
	}
	if err := o.StartRunPools(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case mode := <-launchModes:
		if mode != string(types.LaunchCold) {
			t.Fatalf("recovered task launch mode = %q, want cold", mode)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("recovered resume did not reach task-local artifact preparation")
	}
	running := waitForSandbox(t, o, ctx, sb.ID, func(current *types.Sandbox) bool {
		return current.State == types.StateRunning
	}, "running after recovered cold resume")
	if running.ResumeSource != source || running.LaunchMode != "" {
		t.Fatalf("recovered running source/mode = %+v/%q", running.ResumeSource, running.LaunchMode)
	}
	if vs.detaches.Load() != 1 {
		t.Fatalf("old network detaches = %d, want 1", vs.detaches.Load())
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
	for _, path := range []string{
		nodepath.BuildRunDir(cfg.Paths.RunRoot, build.BuildID),
		nodepath.BuildBaseDir(cfg.Paths.BaseRoot, build.BuildID),
	} {
		if err := os.MkdirAll(path, 0o700); err != nil {
			t.Fatal(err)
		}
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
		postDone <- o.PostBuildResult(ctx, runID, build.BuildID, configsock.BuildResult{
			Target: types.BuildTarget{Kind: types.BuildTargetImage}, ImageRef: imageRef,
		})
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
	for _, path := range []string{
		nodepath.BuildRunDir(cfg.Paths.RunRoot, build.BuildID),
		nodepath.BuildBaseDir(cfg.Paths.BaseRoot, build.BuildID),
	} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("recovered build directory %s remains: %v", path, err)
		}
	}
}

func TestReconcileCompletesDurablyAcceptedResultWithoutLiveUnit(t *testing.T) {
	box, err := secretbox.NewFromColonHex(strings.Repeat("9", 64))
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
	runID := "br-00000000-0000-7000-8000-000000000211"
	build := buildReconcileRow(t, runID)
	runDir := nodepath.BuildRunDir(cfg.Paths.RunRoot, build.BuildID)
	baseDir := nodepath.BuildBaseDir(cfg.Paths.BaseRoot, build.BuildID)
	for _, path := range []string{runDir, baseDir} {
		if err := os.MkdirAll(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.PutBuild(context.Background(), build); err != nil {
		t.Fatal(err)
	}
	imageRef := "manifest://" + strings.Repeat("f", 64)
	if accepted, err := st.AcceptBuildResult(context.Background(), build.BuildID, runID,
		configsock.BuildResult{Target: types.BuildTarget{Kind: types.BuildTargetImage}, ImageRef: imageRef}); err != nil || !accepted {
		t.Fatalf("persist accepted result = %v, %v", accepted, err)
	}

	vs := &reconcileVS{}
	o := New(cfg, st, &reconcileLauncher{}, vs,
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := o.ReconcileBuilds(context.Background()); err != nil {
		t.Fatal(err)
	}
	stored, err := st.GetBuild(context.Background(), build.BuildID)
	if err != nil || stored == nil {
		t.Fatalf("terminal build = %+v, %v", stored, err)
	}
	wantPersistID := types.TemplateID{Profile: build.Profile, Kind: types.KindImg, Ref: imageRef}.String()
	if stored.Status != types.BuildReady || stored.PersistID != wantPersistID ||
		stored.ExecutionClaimed || stored.ExecutionResult != nil || stored.RuntimeVswitchPort != "" {
		t.Fatalf("accepted result was not recovered exactly: %+v", stored)
	}
	if len(vs.detached) != 1 || vs.detached[0] != build.RuntimeVswitchPort {
		t.Fatalf("recovered accepted-result ports = %v", vs.detached)
	}
	for _, path := range []string{runDir, baseDir} {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("accepted-result build directory %s remains: %v", path, err)
		}
	}
}

func TestReconcileLiveBuildFinalizesAcceptedResultBeforePhaseRebuild(t *testing.T) {
	box, err := secretbox.NewFromColonHex(strings.Repeat("8", 64))
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
	runID := "br-00000000-0000-7000-8000-000000000215"
	unit := "sandbox-builder@" + runID + ".service"
	build := buildReconcileRow(t, runID)
	// This deliberately cannot be parsed if recovery tries to reconstruct a
	// completed pipeline. The already-acknowledged result must win first.
	build.FromTemplate = "not-a-template-id"
	runDir := nodepath.BuildRunDir(cfg.Paths.RunRoot, build.BuildID)
	baseDir := nodepath.BuildBaseDir(cfg.Paths.BaseRoot, build.BuildID)
	for _, path := range []string{runDir, baseDir} {
		if err := os.MkdirAll(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.PutBuild(context.Background(), build); err != nil {
		t.Fatal(err)
	}
	imageRef := "manifest://" + strings.Repeat("e", 64)
	if accepted, err := st.AcceptBuildResult(context.Background(), build.BuildID, runID,
		configsock.BuildResult{Target: types.BuildTarget{Kind: types.BuildTargetImage}, ImageRef: imageRef}); err != nil || !accepted {
		t.Fatalf("persist accepted result = %v, %v", accepted, err)
	}

	lc := &reconcileLauncher{
		units: []launcher.Unit{{Name: unit, ActiveState: "active"}},
		resources: launcher.ResourceProperties{
			CPUQuotaPerSecUSec: 1_000_000,
			MemoryMax:          1 << 30,
		},
	}
	vs := &reconcileVS{}
	o := New(cfg, st, lc, vs,
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := o.ReconcileBuilds(context.Background()); err != nil {
		t.Fatal(err)
	}
	stored, err := st.GetBuild(context.Background(), build.BuildID)
	if err != nil || stored == nil {
		t.Fatalf("terminal build = %+v, %v", stored, err)
	}
	wantPersistID := types.TemplateID{Profile: build.Profile, Kind: types.KindImg, Ref: imageRef}.String()
	if stored.Status != types.BuildReady || stored.PersistID != wantPersistID ||
		stored.ExecutionClaimed || stored.ExecutionResult != nil {
		t.Fatalf("accepted result was overwritten during live recovery: %+v", stored)
	}
	if len(lc.stopped) != 1 || lc.stopped[0] != unit {
		t.Fatalf("accepted-result unit fence = %v, want %s", lc.stopped, unit)
	}
	if len(vs.detached) != 1 || vs.detached[0] != build.RuntimeVswitchPort {
		t.Fatalf("accepted-result runtime detach = %v", vs.detached)
	}
	for _, path := range []string{runDir, baseDir} {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("accepted-result build directory %s remains: %v", path, err)
		}
	}
}

func TestPostBuildResultPersistsBeforeIdempotentNotification(t *testing.T) {
	o := testOrch(t)
	runID := "br-00000000-0000-7000-8000-000000000212"
	build := buildReconcileRow(t, runID)
	if err := o.st.PutBuild(context.Background(), build); err != nil {
		t.Fatal(err)
	}
	pend := &pendingBuild{build: build, result: make(chan configsock.BuildResult, 1)}
	o.pend[build.BuildID] = pend
	result := configsock.BuildResult{
		Target:   types.BuildTarget{Kind: types.BuildTargetImage},
		ImageRef: "manifest://" + strings.Repeat("a", 64),
	}
	if err := o.PostBuildResult(context.Background(), runID, build.BuildID, result); err != nil {
		t.Fatal(err)
	}
	stored, err := o.st.GetBuild(context.Background(), build.BuildID)
	if err != nil || stored.ExecutionResult == nil || *stored.ExecutionResult != result {
		t.Fatalf("acknowledged result was not durable: %+v, err=%v", stored, err)
	}
	if err := o.PostBuildResult(context.Background(), runID, build.BuildID, result); err != nil {
		t.Fatalf("identical result replay: %v", err)
	}
	conflict := result
	conflict.ImageRef = "manifest://" + strings.Repeat("b", 64)
	if err := o.PostBuildResult(context.Background(), runID, build.BuildID, conflict); !errors.Is(err, store.ErrBuildResultConflict) || !configsock.IsBuildReportRejection(err) {
		t.Fatalf("conflicting result replay = %v", err)
	}
	select {
	case got := <-pend.result:
		if got != result {
			t.Fatalf("result notification = %+v, want %+v", got, result)
		}
	default:
		t.Fatal("durable result did not notify the live monitor")
	}
	select {
	case duplicate := <-pend.result:
		t.Fatalf("idempotent replay queued a duplicate notification: %+v", duplicate)
	default:
	}
}

func TestClosedBuildResultGateRejectsLateReportBeforePersistence(t *testing.T) {
	o := testOrch(t)
	runID := "br-00000000-0000-7000-8000-000000000214"
	build := buildReconcileRow(t, runID)
	build.BuildID = "closed-result-gate"
	if err := o.st.PutBuild(context.Background(), build); err != nil {
		t.Fatal(err)
	}
	pend := &pendingBuild{build: build, result: make(chan configsock.BuildResult, 1)}
	o.pend[build.BuildID] = pend
	if result, found := closePendingBuildResultsAndTake(pend); found || result != nil {
		t.Fatalf("empty result gate returned %+v, found=%t", result, found)
	}

	err := o.PostBuildResult(context.Background(), runID, build.BuildID,
		configsock.BuildResult{
			Target:   types.BuildTarget{Kind: types.BuildTargetImage},
			ImageRef: "manifest://" + strings.Repeat("c", 64),
		})
	if err == nil || !configsock.IsBuildReportRejection(err) {
		t.Fatalf("late result was not definitively rejected: %v", err)
	}
	stored, getErr := o.st.GetBuild(context.Background(), build.BuildID)
	if getErr != nil || stored.ExecutionResult != nil {
		t.Fatalf("late result crossed closed gate: %+v, %v", stored, getErr)
	}
}

func TestWaitAssignmentReplaysDurableBuildRunBinding(t *testing.T) {
	o := testOrch(t)
	runID := "br-00000000-0000-7000-8000-000000000213"
	build := buildReconcileRow(t, runID)
	if err := o.st.PutBuild(context.Background(), build); err != nil {
		t.Fatal(err)
	}

	buildID, ok, err := o.WaitAssignment(context.Background(), runKindBuild, runID)
	if err != nil || !ok || buildID != build.BuildID {
		t.Fatalf("durable assignment replay = %q, %t, %v", buildID, ok, err)
	}
}

func TestRecoveredAcceptedResultWinsCanceledMonitor(t *testing.T) {
	runID := "br-00000000-0000-7000-8000-000000000214"
	unit := "sandbox-builder@" + runID + ".service"
	lc := &reconcileLauncher{
		units:              []launcher.Unit{{Name: unit, ActiveState: "active"}},
		inactiveAfterLists: 2,
	}
	o := &Orchestrator{
		cfg: buildReconcileConfig(t.TempDir()), lc: lc,
		log: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	build := buildReconcileRow(t, runID)
	accepted := configsock.BuildResult{
		Target:   types.BuildTarget{Kind: types.BuildTargetImage},
		ImageRef: "manifest://" + strings.Repeat("d", 64),
	}
	pend := &pendingBuild{
		handoff: newBuildTaskHandoff(false, fastBuildPrepareDigest(build.BuildID)),
		result:  make(chan configsock.BuildResult, 1),
	}
	pend.result <- accepted
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	result, err := o.waitRecoveredBuild(ctx, build, pend, unit)
	if err != nil || result == nil || *result != accepted {
		t.Fatalf("accepted result after canceled monitor = %+v, %v", result, err)
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

func TestReconcileRetriesBuildDirectoryCleanupAfterRestart(t *testing.T) {
	box, err := secretbox.NewFromColonHex(strings.Repeat("3", 64))
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(filepath.Join(t.TempDir(), "node.db"), box)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })

	cfg := buildReconcileConfig(filepath.Join(t.TempDir(), "run"))
	build := buildReconcileRow(t, "br-00000000-0000-7000-8000-000000000220")
	runDir := nodepath.BuildRunDir(cfg.Paths.RunRoot, build.BuildID)
	baseDir := nodepath.BuildBaseDir(cfg.Paths.BaseRoot, build.BuildID)
	for _, path := range []string{runDir, baseDir} {
		if err := os.MkdirAll(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.PutBuild(context.Background(), build); err != nil {
		t.Fatal(err)
	}

	removeErr := errors.New("injected BuildBaseDir removal failure")
	first := New(cfg, st, &reconcileLauncher{}, &reconcileVS{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	first.removeBuildBaseDir = func(path string) error {
		if path != baseDir {
			t.Fatalf("remove base path = %q, want %q", path, baseDir)
		}
		return removeErr
	}
	if err := first.ReconcileBuilds(context.Background()); !errors.Is(err, removeErr) {
		t.Fatalf("first reconcile error = %v, want BaseDir failure", err)
	}
	stored, err := st.GetBuild(context.Background(), build.BuildID)
	if err != nil || stored == nil || stored.Status != types.BuildBuilding || !stored.ExecutionClaimed {
		t.Fatalf("failed directory cleanup released execution claim: %+v, %v", stored, err)
	}
	if stored.RuntimeVswitchPort != "" {
		t.Fatalf("completed runtime cleanup was not durably recorded: %+v", stored)
	}
	if _, err := os.Stat(runDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("completed BuildRunDir cleanup = %v", err)
	}
	if _, err := os.Stat(baseDir); err != nil {
		t.Fatalf("failed BuildBaseDir cleanup lost retry owner: %v", err)
	}

	// A new controller has no process-local cleanup state. It must recover the
	// exact directories from BuildID plus the configured roots.
	restarted := New(cfg, st, &reconcileLauncher{}, &reconcileVS{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := restarted.ReconcileBuilds(context.Background()); err != nil {
		t.Fatal(err)
	}
	stored, err = st.GetBuild(context.Background(), build.BuildID)
	if err != nil || stored == nil || stored.Status != types.BuildError || stored.ExecutionClaimed {
		t.Fatalf("restart did not terminally release Build ownership: %+v, %v", stored, err)
	}
	for _, path := range []string{runDir, baseDir} {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("restart retained Build directory %s: %v", path, err)
		}
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

func TestRangeClusterBuildsAfterReconcileExceedsSubscriberBuffer(t *testing.T) {
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
	const total = 300
	for i := 0; i < total; i++ {
		build := buildReconcileRow(t, fmt.Sprintf("br-00000000-0000-7000-8000-%012d", i))
		build.BuildID = fmt.Sprintf("00000000-0000-7000-8000-%012d", i)
		build.TemplateID = fmt.Sprintf("transient-00000000-0000-7000-8000-%012d", i)
		build.ClusterGroup = "/recovery"
		build.RuntimeVswitchPort = ""
		build.RuntimeFloatingIP = ""
		build.RuntimePortMAC = ""
		build.RuntimePrepareJSON = ""
		if err := st.PutBuild(context.Background(), build); err != nil {
			t.Fatal(err)
		}
	}
	if err := o.ReconcileBuilds(context.Background()); err != nil {
		t.Fatal(err)
	}
	seen := make(map[string]bool, total)
	if err := o.RangeBuilds(context.Background(), func(event routesync.BuildEvent) error {
		if event.Kind != routesync.BuildUpsert || event.State != string(types.BuildError) || event.Reason != "build unit was not live after controller restart" {
			t.Fatalf("full-sync terminal event = %+v", event)
		}
		seen[event.BuildID] = true
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(seen) != total {
		t.Fatalf("Build full sync delivered %d/%d unique builds", len(seen), total)
	}
}

func TestRangeClusterBuildsAfterAcceptedResultReconcileExceedsSubscriberBuffer(t *testing.T) {
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
	const total = 300
	result := configsock.BuildResult{
		Target:   types.BuildTarget{Kind: types.BuildTargetImage},
		ImageRef: "manifest://" + strings.Repeat("a", 64),
	}
	wantTemplateID := types.TemplateID{Profile: types.ProfileBare, Kind: types.KindImg, Ref: result.ImageRef}.String()
	for i := 0; i < total; i++ {
		build := buildReconcileRow(t, fmt.Sprintf("br-00000000-0000-7000-8000-%012d", i))
		build.BuildID = fmt.Sprintf("00000000-0000-7000-8000-%012d", i)
		build.TemplateID = fmt.Sprintf("transient-00000000-0000-7000-8000-%012d", i)
		build.ClusterGroup = "/accepted-recovery"
		build.RuntimeVswitchPort = ""
		build.RuntimeFloatingIP = ""
		build.RuntimePortMAC = ""
		build.RuntimePrepareJSON = ""
		if err := st.PutBuild(context.Background(), build); err != nil {
			t.Fatal(err)
		}
		if accepted, err := st.AcceptBuildResult(context.Background(), build.BuildID, build.RunID, result); err != nil || !accepted {
			t.Fatalf("accept result %d = %t, %v", i, accepted, err)
		}
	}

	if err := o.ReconcileBuilds(context.Background()); err != nil {
		t.Fatal(err)
	}
	ready, err := st.BuildsByStatus(context.Background(), types.BuildReady)
	if err != nil {
		t.Fatal(err)
	}
	if len(ready) != total {
		t.Fatalf("durable ready builds = %d, want %d", len(ready), total)
	}
	for _, build := range ready {
		if build.PersistID != wantTemplateID || build.ExecutionClaimed || build.ExecutionResult != nil {
			t.Fatalf("accepted result did not finalize exactly: %+v", build)
		}
	}

	seen := make(map[string]bool, total)
	if err := o.RangeBuilds(context.Background(), func(event routesync.BuildEvent) error {
		if event.Kind != routesync.BuildUpsert || event.State != string(types.BuildReady) || !types.IsTransientID(event.TemplateID) || event.PersistID != wantTemplateID || event.Reason != "" {
			t.Fatalf("full-sync accepted-result event = %+v", event)
		}
		seen[event.BuildID] = true
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(seen) != total {
		t.Fatalf("Build full sync delivered %d/%d accepted results", len(seen), total)
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
		!strings.Contains(stored.Reason, "resource enforcement does not match") {
		t.Fatalf("mismatched live build = %+v", stored)
	}
	if !containsString(lc.stopped, unit) {
		t.Fatalf("mismatched unit was not stopped: %v", lc.stopped)
	}
	if _, _, ok, err := o.BuildSpecFor(context.Background(), "build:"+build.BuildID); err != nil || ok {
		t.Fatalf("mismatched unit was adopted: ok=%t err=%v", ok, err)
	}
}

func TestReconcilePreservesLiveBuildWhenResourceReadFails(t *testing.T) {
	box, err := secretbox.NewFromColonHex(strings.Repeat("3", 64))
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(filepath.Join(t.TempDir(), "node.db"), box)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })

	runID := "br-00000000-0000-7000-8000-000000000019"
	unit := "sandbox-builder@" + runID + ".service"
	build := buildReconcileRow(t, runID)
	if err := st.PutBuild(context.Background(), build); err != nil {
		t.Fatal(err)
	}
	readErr := errors.New("transient D-Bus read failure")
	lc := &reconcileLauncher{
		units:        []launcher.Unit{{Name: unit, ActiveState: "active"}},
		resourcesErr: readErr,
	}
	vs := &reconcileVS{}
	o := New(buildReconcileConfig(filepath.Join(t.TempDir(), "run")), st, lc, vs,
		slog.New(slog.NewTextHandler(io.Discard, nil)))

	err = o.Reconcile(context.Background())
	if !errors.Is(err, readErr) {
		t.Fatalf("Reconcile error = %v, want transient read failure", err)
	}
	stored, getErr := st.GetBuild(context.Background(), build.BuildID)
	if getErr != nil {
		t.Fatal(getErr)
	}
	if stored.Status != types.BuildBuilding || !stored.ExecutionClaimed || stored.RunID != runID {
		t.Fatalf("resource read failure changed durable live build: %+v", stored)
	}
	if len(lc.stopped) != 0 || len(lc.reset) != 0 {
		t.Fatalf("resource read failure touched live unit: stopped=%v reset=%v", lc.stopped, lc.reset)
	}
	if len(vs.detached) != 0 {
		t.Fatalf("resource read failure detached live network ownership: %v", vs.detached)
	}
}

func TestReconcileAdoptsSnapshotBuildStillPreparing(t *testing.T) {
	st := testOrch(t).st
	runRoot := filepath.Join(t.TempDir(), "run")
	runID := "br-00000000-0000-7000-8000-000000000221"
	unit := "sandbox-builder@" + runID + ".service"
	build := buildReconcileRow(t, runID)
	build.Profile = types.ProfileE2B
	build.FromTemplate = types.TemplateID{
		Profile: types.ProfileE2B, Kind: types.KindSnp,
		Ref: "manifest://" + strings.Repeat("e", 64),
	}.String()
	build.RuntimeVswitchPort, build.RuntimeFloatingIP, build.RuntimePortMAC = "", "", ""
	build.RuntimeEnvdAccessToken, build.RuntimePrepareJSON = "", ""
	if err := st.PutBuild(context.Background(), build); err != nil {
		t.Fatal(err)
	}
	lc := &reconcileLauncher{
		units: []launcher.Unit{{Name: unit, ActiveState: "active"}},
		resources: launcher.ResourceProperties{
			CPUQuotaPerSecUSec: 1_000_000, MemoryMax: 1 << 30,
		},
	}
	vs := &reconcileVS{attach: &vswitch.Port{
		Port: "27", FloatingIP: "192.0.2.27", MAC: "02:00:00:00:00:27", InnerIP: "10.0.0.5",
	}}
	o := New(buildReconcileConfig(runRoot), st, lc, vs, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx, cancel := context.WithCancel(context.Background())
	if err := o.ReconcileBuilds(ctx); err != nil {
		cancel()
		t.Fatal(err)
	}
	task, found, err := o.BuildTaskSpecFor(context.Background(), build.BuildID, runID)
	if err != nil || !found || task.Prepare == nil || task.Final != nil {
		cancel()
		t.Fatalf("recovered preparing bootstrap = %+v, %t, %v", task, found, err)
	}
	stored, err := st.GetBuild(context.Background(), build.BuildID)
	if err != nil || stored.RuntimeVswitchPort != "" || stored.RuntimePrepareJSON != "" {
		cancel()
		t.Fatalf("preparing build was prematurely committed: %+v, %v", stored, err)
	}
	final, err := o.CompleteBuildPrepare(context.Background(), build.BuildID, runID, validBuildPrepareSummary())
	if err != nil || final == nil || final.BuildID != build.BuildID {
		cancel()
		t.Fatalf("recovered preparing completion = %+v, %v", final, err)
	}
	stored, err = st.GetBuild(context.Background(), build.BuildID)
	if err != nil || stored.RuntimeVswitchPort != "27" || stored.RuntimePrepareJSON == "" || vs.attaches != 1 {
		cancel()
		t.Fatalf("recovered preparing durable final = %+v, attaches=%d, err=%v", stored, vs.attaches, err)
	}
	cancel()
	if err := o.DrainBuilds(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestReconcileFailsClosedForPortWithoutDurablePreparation(t *testing.T) {
	box, err := secretbox.NewFromColonHex(strings.Repeat("3", 64))
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(filepath.Join(t.TempDir(), "node.db"), box)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	runID := "br-00000000-0000-7000-8000-000000000222"
	unit := "sandbox-builder@" + runID + ".service"
	build := buildReconcileRow(t, runID)
	build.RuntimePrepareJSON = ""
	if err := st.PutBuild(context.Background(), build); err != nil {
		t.Fatal(err)
	}
	lc := &reconcileLauncher{
		units: []launcher.Unit{{Name: unit, ActiveState: "active"}},
		resources: launcher.ResourceProperties{
			CPUQuotaPerSecUSec: 1_000_000, MemoryMax: 1 << 30,
		},
	}
	vs := &reconcileVS{}
	o := New(buildReconcileConfig(t.TempDir()), st, lc, vs, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := o.ReconcileBuilds(context.Background()); err != nil {
		t.Fatal(err)
	}
	stored, err := st.GetBuild(context.Background(), build.BuildID)
	if err != nil || stored.Status != types.BuildError || stored.ExecutionClaimed || stored.RuntimeVswitchPort != "" ||
		!strings.Contains(stored.Reason, "durable runtime preparation is missing") {
		t.Fatalf("missing-preparation recovery = %+v, %v", stored, err)
	}
	if !containsString(lc.stopped, unit) || !containsString(vs.detached, "17") {
		t.Fatalf("missing-preparation cleanup: stopped=%v detached=%v", lc.stopped, vs.detached)
	}
}

func TestPreparedSnapshotRecoveryUsesDurableInputsAfterNodeDefaultsChange(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		t.Run(map[bool]string{false: "auto", true: "explicit"}[explicit], func(t *testing.T) {
			testPreparedSnapshotRecovery(t, explicit)
		})
	}
}

func testPreparedSnapshotRecovery(t *testing.T, explicit bool) {
	box, err := secretbox.NewFromColonHex(strings.Repeat("3", 64))
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(filepath.Join(t.TempDir(), "node.db"), box)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	runID := "br-00000000-0000-7000-8000-000000000223"
	unit := "sandbox-builder@" + runID + ".service"
	build := buildReconcileRow(t, runID)
	build.Profile = types.ProfileE2B
	build.FromTemplate = types.TemplateID{
		Profile: types.ProfileE2B, Kind: types.KindSnp,
		Ref: "manifest://" + strings.Repeat("f", 64),
	}.String()
	build.Metadata = map[string]string{
		sandboxcfg.NsMetadata: `{"registered":"preserved"}`,
	}
	build.Env = map[string]string{"REGISTERED_ENV": "preserved"}
	if explicit {
		build.Builder.Target = &types.BuildTarget{Kind: types.BuildTargetSandbox, Memory: true}
	}
	digest := strings.Repeat("9", 64)
	durableResources := rtconfig.ResourcesConfig{
		Capacity: rtconfig.CapacityConfig{CPU: 4, Memory: "8GiB"},
	}
	durableNetwork := sandboxcfg.NetworkSpec{Hostname: "frozen-build", InnerIP: "10.44.0.5/24", Nexthop: "10.44.0.1"}
	durableTemplateNetwork := durableNetwork
	durableTemplateNetwork.Hostname = "frozen-template"
	durableCheckpointPolicy := sandboxcfg.SnapshotPolicy{
		MergeRef: orchCheckpointBool(false), DropCaches: orchCheckpointBool(true),
	}
	build.RuntimePrepareJSON, err = encodeBuildRuntimePreparation(buildRuntimePreparation{
		SchemaVersion: buildRuntimePrepareSchemaVersion, PrepareDigest: digest,
		SourceHasBuildCommands: true,
		Network:                durableNetwork, TemplateNetwork: durableTemplateNetwork,
		Resources: durableResources, SandboxResources: durableResources,
		CheckpointPolicy: durableCheckpointPolicy,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := st.PutBuild(context.Background(), build); err != nil {
		t.Fatal(err)
	}
	cfg := buildReconcileConfig(t.TempDir())
	cfg.MMDS.Enabled = true
	// These current defaults deliberately disagree with the values atomically
	// committed alongside port 17 before the restart.
	cfg.Sandbox.Network.E2B.InnerIP = "192.0.2.5/24"
	cfg.Sandbox.Network.E2B.Nexthop = "192.0.2.1"
	cfg.Sandbox.Resources.Capacity.Memory = "invalid-current-policy"
	cfg.Checkpoint.MergeRef = orchCheckpointBool(true)
	cfg.Checkpoint.DropCaches = orchCheckpointBool(false)
	lc := &reconcileLauncher{
		units: []launcher.Unit{{Name: unit, ActiveState: "active"}},
		resources: launcher.ResourceProperties{
			CPUQuotaPerSecUSec: 1_000_000, MemoryMax: 1 << 30,
		},
	}
	vs := &reconcileVS{}
	o := New(cfg, st, lc, vs, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx, cancel := context.WithCancel(context.Background())
	if err := o.ReconcileBuilds(ctx); err != nil {
		cancel()
		t.Fatal(err)
	}
	summary := validBuildPrepareSummary()
	summary.ResolutionDigest = digest
	summary.HasBuildCommands = true
	final, err := o.CompleteBuildPrepare(context.Background(), build.BuildID, runID, summary)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	if final.Net.InnerIP != durableNetwork.InnerIP || final.Net.Hostname != durableNetwork.Hostname ||
		!reflect.DeepEqual(final.TemplateNetwork, durableTemplateNetwork) || final.Resources.Capacity != durableResources.Capacity {
		cancel()
		t.Fatalf("recovered final spec drifted: net=%+v template=%+v resources=%+v", final.Net, final.TemplateNetwork, final.Resources)
	}
	if final.SandboxSpec.Metadata["registered"] != "preserved" || final.SandboxEnv["REGISTERED_ENV"] != "preserved" ||
		!reflect.DeepEqual(final.RequestedTarget, build.Builder.Target) {
		cancel()
		t.Fatalf("recovered final spec lost immutable registration config: %+v", final)
	}
	if o.lookup(buildMMDSID(build.BuildID)) == nil {
		cancel()
		t.Fatal("recovery lost the memory target MMDS route")
	}
	if !reflect.DeepEqual(final.CheckpointPolicy, durableCheckpointPolicy) {
		cancel()
		t.Fatalf("recovered checkpoint policy drifted: got %+v, want %+v", final.CheckpointPolicy, durableCheckpointPolicy)
	}
	cancel()
	if err := o.DrainBuilds(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestReconcilePreflightsAllLiveBuildsBeforeAdoption(t *testing.T) {
	box, err := secretbox.NewFromColonHex(strings.Repeat("3", 64))
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(filepath.Join(t.TempDir(), "node.db"), box)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })

	runIDs := []string{
		"br-00000000-0000-7000-8000-000000000020",
		"br-00000000-0000-7000-8000-000000000021",
	}
	units := make([]launcher.Unit, 0, len(runIDs))
	builds := make([]*types.Build, 0, len(runIDs))
	for i, runID := range runIDs {
		build := buildReconcileRow(t, runID)
		build.BuildID = fmt.Sprintf("00000000-0000-7000-8000-%012d", 20+i)
		build.TemplateID = fmt.Sprintf("transient-00000000-0000-7000-8000-%012d", 20+i)
		if err := st.PutBuild(context.Background(), build); err != nil {
			t.Fatal(err)
		}
		builds = append(builds, build)
		units = append(units, launcher.Unit{Name: "sandbox-builder@" + runID + ".service", ActiveState: "active"})
	}
	readErr := errors.New("second unit D-Bus read failure")
	lc := &reconcileLauncher{
		units: units,
		resources: launcher.ResourceProperties{
			CPUQuotaPerSecUSec: 1_000_000,
			MemoryMax:          1 << 30,
		},
		resourcesErrByUnit: map[string]error{units[1].Name: readErr},
	}
	o := New(buildReconcileConfig(filepath.Join(t.TempDir(), "run")), st, lc, &reconcileVS{},
		slog.New(slog.NewTextHandler(io.Discard, nil)))

	if err := o.Reconcile(context.Background()); !errors.Is(err, readErr) {
		t.Fatalf("Reconcile error = %v, want later preflight failure", err)
	}
	o.pendMu.Lock()
	owners := len(o.pend)
	o.pendMu.Unlock()
	if owners != 0 {
		t.Fatalf("preflight failure started %d live monitors", owners)
	}
	if len(lc.stopped) != 0 || len(lc.reset) != 0 {
		t.Fatalf("preflight failure touched units: stopped=%v reset=%v", lc.stopped, lc.reset)
	}
	for _, build := range builds {
		stored, err := st.GetBuild(context.Background(), build.BuildID)
		if err != nil {
			t.Fatal(err)
		}
		if stored.Status != types.BuildBuilding || !stored.ExecutionClaimed {
			t.Fatalf("preflight failure changed build %s: %+v", build.BuildID, stored)
		}
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
		Paths: config.PathsConfig{RunRoot: runRoot, BaseRoot: filepath.Join(filepath.Dir(runRoot), "base")},
		Units: config.UnitsConfig{
			Runner: "sandbox-runner@.service", Builder: "sandbox-builder@.service", PoolWaitTimeout: "5s",
		},
		Sandbox: config.SandboxConfig{
			Resources: configresolve.PublicSandboxResources(policy),
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
		Checkpoint: config.CheckpointConfig{Mode: config.CheckpointBundle},
	}
}

func buildReconcileRow(t *testing.T, runID string) *types.Build {
	t.Helper()
	manifestKey := strings.Repeat("a", 64)
	runtimePrepare, err := encodeBuildRuntimePreparation(buildRuntimePreparation{
		SchemaVersion: buildRuntimePrepareSchemaVersion,
		PrepareDigest: strings.Repeat("b", 64),
		Network: sandboxcfg.NetworkSpec{
			Hostname: "build", InnerIP: "169.254.1.1/31", Nexthop: "169.254.1.0",
		},
		TemplateNetwork: sandboxcfg.NetworkSpec{
			Hostname: "sandbox", InnerIP: "169.254.1.1/31", Nexthop: "169.254.1.0",
		},
		Resources: rtconfig.ResourcesConfig{
			Capacity:    rtconfig.CapacityConfig{CPU: 1, Memory: "1GiB"},
			Allocatable: rtconfig.AllocatableConfig{CPU: 1, Memory: "1GiB"},
		},
		SandboxResources: rtconfig.ResourcesConfig{
			Capacity:    rtconfig.CapacityConfig{CPU: 1, Memory: "1GiB"},
			Allocatable: rtconfig.AllocatableConfig{CPU: 1, Memory: "1GiB"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return &types.Build{
		BuildID: "00000000-0000-7000-8000-000000000003", TemplateID: "transient-00000000-0000-7000-8000-000000000004",
		APISecret: deriveTestAPISecret(t, manifestKey), ManifestKey: manifestKey,
		Profile: types.ProfileBare, Status: types.BuildBuilding,
		Resources:        types.BuildResources{CPU: 1000, Memory: 1 << 30},
		ExecutionClaimed: true, ExecutionClaimedUnix: time.Now().Unix(), RunID: runID,
		EnforcementStatus: "cpu,memory", RuntimeVswitchPort: "17", RuntimeFloatingIP: "192.0.2.17",
		RuntimePortMAC: "02:00:00:00:00:17", RuntimePrepareJSON: runtimePrepare, CreatedUnix: time.Now().Unix(),
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
		LaunchMode:  types.LaunchImage,
		RunDir:      nodepath.SandboxRunDir(cfg.Paths.RunRoot, "cleanup-failure"),
		BaseDir:     nodepath.SandboxBaseDir(cfg.Paths.BaseRoot, "cleanup-failure"),
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
