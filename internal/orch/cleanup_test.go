package orch

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/launcher"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/orchestrator/internal/vswitch"
)

type orderedCleanupLauncher struct {
	stopCalls  atomic.Int32
	resetCalls atomic.Int32
	stopErr    error
	resetErr   error
	resources  launcher.ResourceProperties
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
func (l *orderedCleanupLauncher) SetResources(_ context.Context, _ string, p launcher.ResourceProperties) error {
	l.resources = p
	return nil
}
func (l *orderedCleanupLauncher) Resources(context.Context, string, string) (launcher.ResourceProperties, error) {
	return l.resources, nil
}
func (*orderedCleanupLauncher) Close() error { return nil }

type transientAcceptedResultLauncher struct {
	orderedCleanupLauncher
	mu       sync.Mutex
	unit     string
	active   bool
	firstErr error
}

func (l *transientAcceptedResultLauncher) Stop(context.Context, string) error {
	if l.stopCalls.Add(1) == 1 {
		return l.firstErr
	}
	l.mu.Lock()
	l.active = false
	l.mu.Unlock()
	return nil
}

func (l *transientAcceptedResultLauncher) List(context.Context, string) ([]launcher.Unit, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.active {
		return nil, nil
	}
	return []launcher.Unit{{Name: l.unit, ActiveState: "active"}}, nil
}

type orderedCleanupVS struct {
	detachCalls atomic.Int32
	detachErr   error
	mu          sync.Mutex
	ports       []string
}

func (*orderedCleanupVS) Attach(context.Context, vswitch.AttachReq) (*vswitch.Port, error) {
	return nil, nil
}
func (v *orderedCleanupVS) Detach(_ context.Context, port string) error {
	v.mu.Lock()
	v.ports = append(v.ports, port)
	v.mu.Unlock()
	if v.detachCalls.Add(1) == 1 {
		return v.detachErr
	}
	return nil
}

func (v *orderedCleanupVS) detachedPorts() []string {
	v.mu.Lock()
	defer v.mu.Unlock()
	return append([]string(nil), v.ports...)
}
func (*orderedCleanupVS) TapFD(string) vswitch.TapFD { return vswitch.TapFD{} }

type serializedBuildCleanupVS struct {
	detachStarted chan struct{}
	allowDetach   chan struct{}
	attachStarted chan struct{}
}

func (v *serializedBuildCleanupVS) Attach(context.Context, vswitch.AttachReq) (*vswitch.Port, error) {
	close(v.attachStarted)
	return &vswitch.Port{Port: "18"}, nil
}
func (v *serializedBuildCleanupVS) Detach(context.Context, string) error {
	close(v.detachStarted)
	<-v.allowDetach
	return nil
}
func (*serializedBuildCleanupVS) TapFD(string) vswitch.TapFD { return vswitch.TapFD{} }

func TestBuildPortDetachAndDurableClearFenceNewAllocation(t *testing.T) {
	o := testOrch(t)
	vs := &serializedBuildCleanupVS{
		detachStarted: make(chan struct{}),
		allowDetach:   make(chan struct{}),
		attachStarted: make(chan struct{}),
	}
	o.vs = vs
	build := buildReconcileRow(t, "br-00000000-0000-7000-8000-000000000208")
	if err := o.st.PutBuild(context.Background(), build); err != nil {
		t.Fatal(err)
	}

	cleanupDone := make(chan error, 1)
	go func() {
		cleanupDone <- o.cleanupBuildRuntime(build, build.RuntimeVswitchPort, t.TempDir(), true)
	}()
	<-vs.detachStarted
	attachDone := make(chan error, 1)
	go func() {
		_, err := o.attachNetwork(context.Background(), sandboxcfg.NetworkSpec{InnerIP: "169.254.1.1/31"})
		attachDone <- err
	}()

	select {
	case <-vs.attachStarted:
		t.Fatal("new connector allocation crossed detach-to-durable-clear fence")
	case <-time.After(50 * time.Millisecond):
	}
	close(vs.allowDetach)
	select {
	case err := <-cleanupDone:
		if err != nil {
			t.Fatalf("cleanupBuildRuntime: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("build cleanup did not finish")
	}
	select {
	case <-vs.attachStarted:
	case <-time.After(time.Second):
		t.Fatal("connector allocation did not resume after durable clear")
	}
	stored, err := o.st.GetBuild(context.Background(), build.BuildID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.RuntimeVswitchPort != "" {
		t.Fatalf("new allocation began before runtime ownership cleared: %+v", stored)
	}
	if err := <-attachDone; err != nil {
		t.Fatalf("attachNetwork: %v", err)
	}
}

func TestCompleteBuildRetriesTransientCleanupWithoutRestart(t *testing.T) {
	o := testOrch(t)
	o.cfg.Paths.RunRoot = t.TempDir()
	detachErr := errors.New("injected transient build detach failure")
	vs := &orderedCleanupVS{detachErr: detachErr}
	o.vs = vs
	o.lc = &orderedCleanupLauncher{}
	build := buildReconcileRow(t, "br-00000000-0000-7000-8000-000000000209")
	build.BuildID = "cleanup-retry-without-restart"
	workdir := buildRuntimeDir(o.cfg.Paths.RunRoot, build.BuildID)
	if err := os.MkdirAll(workdir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := o.st.PutBuild(context.Background(), build); err != nil {
		t.Fatal(err)
	}

	o.completeBuild(context.Background(), build, &buildResult{
		ImageRef: "manifest://" + strings.Repeat("d", 64),
	}, &buildCleanupPendingError{cleanup: errors.New("initial cleanup attempt failed")})

	stored, err := o.st.GetBuild(context.Background(), build.BuildID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Status != types.BuildReady || stored.ExecutionClaimed || stored.RuntimeVswitchPort != "" {
		t.Fatalf("cleanup retry did not converge terminal ownership: %+v", stored)
	}
	if got := vs.detachCalls.Load(); got != 2 {
		t.Fatalf("detach attempts = %d, want transient failure plus retry", got)
	}
	if _, err := os.Stat(workdir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("cleanup retry retained workdir: %v", err)
	}
}

func TestAcceptedBuildResultSurvivesTransientFenceFailure(t *testing.T) {
	o := testOrch(t)
	o.cfg.Paths.RunRoot = t.TempDir()
	runID := "br-00000000-0000-7000-8000-000000000215"
	unit := o.builderUnit(runID)
	stopErr := errors.New("injected transient accepted-result stop failure")
	lc := &transientAcceptedResultLauncher{unit: unit, active: true, firstErr: stopErr}
	o.lc = lc
	o.vs = &orderedCleanupVS{}
	build := buildReconcileRow(t, runID)
	build.BuildID = "accepted-result-cleanup-retry"
	if err := o.st.PutBuild(context.Background(), build); err != nil {
		t.Fatal(err)
	}

	want := buildResult{ImageRef: "manifest://" + strings.Repeat("f", 64)}
	accepted, err := o.fenceAcceptedBuildResultWithin(unit, want, time.Millisecond)
	var pending *buildCleanupPendingError
	if accepted == nil || *accepted != want || !errors.As(err, &pending) || pending.cause != nil || !errors.Is(err, stopErr) {
		t.Fatalf("initial accepted-result fence = %+v, %v", accepted, err)
	}

	// This is the same ownership enrichment runBuildUnit's defer performs.
	err = retainBuildCleanup(err, nil, build.RuntimeVswitchPort,
		buildRuntimeDir(o.cfg.Paths.RunRoot, build.BuildID), true)
	o.completeBuild(context.Background(), build, accepted, err)

	stored, err := o.st.GetBuild(context.Background(), build.BuildID)
	if err != nil {
		t.Fatal(err)
	}
	wantPersistID := types.TemplateID{Profile: build.Profile, Kind: types.KindImg, Ref: want.ImageRef}.String()
	if stored.Status != types.BuildReady || stored.PersistID != wantPersistID || stored.ExecutionClaimed {
		t.Fatalf("accepted result lost across cleanup retry: %+v", stored)
	}
	if got := lc.stopCalls.Load(); got != 2 {
		t.Fatalf("stop attempts = %d, want initial failure plus cleanup retry", got)
	}
}

func TestCompleteBuildRetainsUnpersistedPortAcrossCleanupRetries(t *testing.T) {
	o := testOrch(t)
	o.cfg.Paths.RunRoot = t.TempDir()
	vs := &orderedCleanupVS{detachErr: errors.New("injected first detach failure")}
	o.vs = vs
	o.lc = &orderedCleanupLauncher{}
	build := buildReconcileRow(t, "br-00000000-0000-7000-8000-000000000210")
	build.BuildID = "cleanup-retry-unpersisted-port"
	build.RuntimeVswitchPort = ""
	workdir := buildRuntimeDir(o.cfg.Paths.RunRoot, build.BuildID)
	if err := os.MkdirAll(workdir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := o.st.PutBuild(context.Background(), build); err != nil {
		t.Fatal(err)
	}

	o.completeBuild(context.Background(), build, &buildResult{
		ImageRef: "manifest://" + strings.Repeat("e", 64),
	}, &buildCleanupPendingError{
		cleanup: errors.New("ownership persistence and initial detach failed"),
		port:    "local-port-21", dir: workdir,
	})

	stored, err := o.st.GetBuild(context.Background(), build.BuildID)
	if err != nil || stored.Status != types.BuildReady || stored.ExecutionClaimed {
		t.Fatalf("cleanup retry terminal build = %+v, err=%v", stored, err)
	}
	if got := vs.detachedPorts(); len(got) != 2 || got[0] != "local-port-21" || got[1] != "local-port-21" {
		t.Fatalf("cleanup retries lost local port ownership: %v", got)
	}
}

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
