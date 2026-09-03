package orch

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/configresolve"
	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/launcher"
	"github.com/kuasar-sandbox/orchestrator/internal/nodepath"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

type assignmentOrderLauncher struct {
	events []string
	value  launcher.ResourceProperties
	setErr error
	onRead func()
	onList func()
}

func (l *assignmentOrderLauncher) Start(context.Context, string) error       { return nil }
func (l *assignmentOrderLauncher) Stop(context.Context, string) error        { return nil }
func (l *assignmentOrderLauncher) ResetFailed(context.Context, string) error { return nil }
func (l *assignmentOrderLauncher) List(context.Context, string) ([]launcher.Unit, error) {
	if l.onList != nil {
		l.onList()
	}
	return nil, nil
}
func (l *assignmentOrderLauncher) Reload(context.Context) error { return nil }
func (l *assignmentOrderLauncher) SetResources(_ context.Context, _ string, value launcher.ResourceProperties) error {
	l.events = append(l.events, "set")
	if l.setErr != nil {
		return l.setErr
	}
	l.value = value
	return nil
}
func (l *assignmentOrderLauncher) Resources(context.Context, string, string) (launcher.ResourceProperties, error) {
	l.events = append(l.events, "read")
	if l.onRead != nil {
		l.onRead()
	}
	return l.value, nil
}
func (l *assignmentOrderLauncher) Close() error { return nil }

func TestClaimWaitingBuildSurvivesRunIDPersistence(t *testing.T) {
	o := testOrch(t)
	ctx := context.Background()
	manifestKey := strings.Repeat("a", 64)
	b := &types.Build{
		BuildID:     "build-admission-test",
		TemplateID:  "transient-build-admission-test",
		APISecret:   deriveTestAPISecret(t, manifestKey),
		ManifestKey: manifestKey,
		Profile:     types.ProfileE2B,
		Status:      types.BuildWaiting,
		Resources:   types.BuildResources{CPU: 1000, Memory: 1 << 30},
		WaitingUnix: time.Now().Unix(),
	}
	if err := o.st.PutBuild(ctx, b); err != nil {
		t.Fatal(err)
	}
	limit, _ := configresolve.BuilderExecutionLimit(o.cfg.Builder)
	won, err := o.st.ClaimBuildExecution(ctx, b.BuildID, limit, time.Now())
	if err != nil || !won {
		t.Fatalf("ClaimBuildExecution: won=%t err=%v", won, err)
	}

	// runBuildUnit persists only the assigned run id. The claimed status must
	// remain building so a later pool scan cannot admit the build again.
	b.RunID = "br-00000000-0000-7000-8000-000000000001"
	if err := o.st.SetBuildRunID(ctx, b.BuildID, b.RunID); err != nil {
		t.Fatal(err)
	}
	got, err := o.st.GetBuild(ctx, b.BuildID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != types.BuildBuilding || got.RunID != b.RunID {
		t.Fatalf("persisted build = status %q run_id %q", got.Status, got.RunID)
	}
	waiting, err := o.st.BuildsByStatus(ctx, types.BuildWaiting)
	if err != nil {
		t.Fatal(err)
	}
	if len(waiting) != 0 {
		t.Fatalf("claimed build was requeued: %+v", waiting)
	}
}

func TestPrepareBuilderUnitAppliesAndVerifiesLimitsBeforeDurableAssignment(t *testing.T) {
	o := testOrch(t)
	o.cfg.Units.Builder = "sandbox-builder@.service"
	ctx := context.Background()
	manifestKey := strings.Repeat("b", 64)
	b := &types.Build{
		BuildID: "build-property-order", TemplateID: "transient-build-property-order",
		APISecret: deriveTestAPISecret(t, manifestKey), ManifestKey: manifestKey,
		Profile: types.ProfileE2B, Status: types.BuildBuilding,
		Resources: types.BuildResources{CPU: 2501, Memory: 3 << 30}, ExecutionClaimed: true,
	}
	if err := o.st.PutBuild(ctx, b); err != nil {
		t.Fatal(err)
	}
	lc := &assignmentOrderLauncher{}
	lc.onRead = func() {
		stored, err := o.st.GetBuild(ctx, b.BuildID)
		if err != nil {
			t.Error(err)
			return
		}
		if stored.RunID != "" {
			t.Errorf("run id became visible before effective properties were read: %q", stored.RunID)
		}
	}
	o.lc = lc
	runID := "br-00000000-0000-7000-8000-000000000205"
	unit, err := o.prepareBuilderUnit(ctx, b, runID)
	if err != nil {
		t.Fatal(err)
	}
	if unit != "sandbox-builder@"+runID+".service" || strings.Join(lc.events, ",") != "set,read" {
		t.Fatalf("assignment barrier unit=%q events=%v", unit, lc.events)
	}
	stored, err := o.st.GetBuild(ctx, b.BuildID)
	if err != nil || stored.RunID != runID || stored.EnforcementStatus != "cpu,memory" {
		t.Fatalf("durable assignment = %+v, %v", stored, err)
	}
}

func TestPrepareBuilderUnitPropertyFailureDoesNotBindRun(t *testing.T) {
	o := testOrch(t)
	o.cfg.Units.Builder = "sandbox-builder@.service"
	ctx := context.Background()
	manifestKey := strings.Repeat("c", 64)
	b := &types.Build{
		BuildID: "build-property-failure", TemplateID: "transient-build-property-failure",
		APISecret: deriveTestAPISecret(t, manifestKey), ManifestKey: manifestKey,
		Profile: types.ProfileE2B, Status: types.BuildBuilding,
		Resources: types.BuildResources{CPU: 1000, Memory: 1 << 30}, ExecutionClaimed: true,
	}
	if err := o.st.PutBuild(ctx, b); err != nil {
		t.Fatal(err)
	}
	wantErr := errors.New("set property failed")
	o.lc = &assignmentOrderLauncher{setErr: wantErr}
	if _, err := o.prepareBuilderUnit(ctx, b, "br-failed"); !errors.Is(err, wantErr) {
		t.Fatalf("prepare error = %v", err)
	}
	stored, err := o.st.GetBuild(ctx, b.BuildID)
	if err != nil || stored.RunID != "" || stored.EnforcementStatus != "" {
		t.Fatalf("failed property application bound run = %+v, %v", stored, err)
	}
}

func TestRunBuildUnitDeadlineCoversRunnerAssignment(t *testing.T) {
	o := testOrch(t)
	o.cfg.Paths.RunRoot = t.TempDir()
	o.cfg.Paths.BaseRoot = t.TempDir()
	o.cfg.Builder.TotalTimeoutSec = 1
	b := &types.Build{
		BuildID: "build-expired-before-assignment", Profile: types.ProfileBare,
		FromImage: "example.invalid/base:latest", Status: types.BuildBuilding,
		Resources:        types.BuildResources{CPU: 1000, Memory: 1 << 30},
		ExecutionClaimed: true, ExecutionClaimedUnix: time.Now().Add(-2 * time.Minute).Unix(),
	}
	if _, err := o.runBuildUnit(context.Background(), b); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expired assignment = %v, want deadline exceeded", err)
	}
	if b.RunID != "" || b.RuntimeVswitchPort != "" {
		t.Fatalf("expired build acquired runtime ownership: %+v", b)
	}
	for _, path := range []string{
		nodepath.BuildRunDir(o.cfg.Paths.RunRoot, b.BuildID),
		nodepath.BuildBaseDir(o.cfg.Paths.BaseRoot, b.BuildID),
	} {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("expired build directory %s remained: %v", path, err)
		}
	}
}

func TestBuildExecutionDeadlineExcludesCleanupHeadroom(t *testing.T) {
	o := testOrch(t)
	o.cfg.Builder.TotalTimeoutSec = 75
	claimed := time.Unix(1_800_000_000, 0)
	b := &types.Build{ExecutionClaimedUnix: claimed.Unix()}

	if got, want := o.buildExecutionDeadline(b), claimed.Add(75*time.Second); !got.Equal(want) {
		t.Fatalf("build execution deadline = %v, want %v", got, want)
	}
}

func TestWaitBuildResultRechecksAcceptedResultAfterInactiveReadback(t *testing.T) {
	o := testOrch(t)
	want := configsock.BuildResult{
		Target:   types.BuildTarget{Kind: types.BuildTargetImage},
		ImageRef: "manifest://" + strings.Repeat("a", 64),
	}
	pend := &pendingBuild{
		handoff: newBuildTaskHandoff(false, ""),
		result:  make(chan configsock.BuildResult, 1),
	}
	published := false
	o.lc = &assignmentOrderLauncher{onList: func() {
		if !published {
			published = true
			pend.result <- want
		}
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	got, err := o.waitBuildResult(ctx, pend, "sandbox-builder@test.service")
	if err != nil || got == nil || *got != want {
		t.Fatalf("accepted result after inactive readback = %+v, %v", got, err)
	}
}

func TestRunBuildUnitCleansDirectoriesWhenRequestResolutionFails(t *testing.T) {
	o := testOrch(t)
	o.cfg.Paths.RunRoot = t.TempDir()
	o.cfg.Paths.BaseRoot = t.TempDir()
	b := &types.Build{
		BuildID: "build-invalid-request-input", Profile: types.ProfileBare,
		Status: types.BuildBuilding, ExecutionClaimed: true,
		Metadata: map[string]string{sandboxcfg.NsResource: `{"capacity":`},
	}
	runDir := nodepath.BuildRunDir(o.cfg.Paths.RunRoot, b.BuildID)
	baseDir := nodepath.BuildBaseDir(o.cfg.Paths.BaseRoot, b.BuildID)
	seenRunDir, seenBaseDir := false, false
	o.removeBuildRunDir = func(path string) error {
		if path != runDir {
			t.Fatalf("BuildRunDir cleanup path = %q, want %q", path, runDir)
		}
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("execution claim did not create BuildRunDir: %v", err)
		}
		seenRunDir = true
		return os.RemoveAll(path)
	}
	o.removeBuildBaseDir = func(path string) error {
		if path != baseDir {
			t.Fatalf("BuildBaseDir cleanup path = %q, want %q", path, baseDir)
		}
		if _, err := os.Stat(nodepath.BuildCheckpointDir(o.cfg.Paths.BaseRoot, b.BuildID)); err != nil {
			t.Fatalf("execution claim did not create Build checkpoint directory: %v", err)
		}
		seenBaseDir = true
		return os.RemoveAll(path)
	}

	if _, err := o.runBuildUnit(context.Background(), b); err == nil {
		t.Fatal("malformed target Sandbox resource config was accepted")
	}
	if !seenRunDir || !seenBaseDir {
		t.Fatalf("execution claim directory observations: run=%t base=%t", seenRunDir, seenBaseDir)
	}
	for _, path := range []string{runDir, baseDir} {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("request-resolution failure retained %s: %v", path, err)
		}
	}
}

func TestRunBuildUnitFencesAssignedUnitBeforePreNetworkDirectoryCleanup(t *testing.T) {
	cfg := buildNetworkTestConfig()
	cfg.Paths.RunRoot = t.TempDir()
	cfg.Paths.BaseRoot = t.TempDir()
	cfg.Units.Builder = "sandbox-builder@.service"
	cfg.Units.PoolWaitTimeout = "1s"
	cfg.Builder.TotalTimeoutSec = 30
	o := testOrchCfg(t, cfg)
	lc := newRunPoolTestLauncher()
	var fenced atomic.Bool
	lc.stopFn = func(context.Context, string) error {
		fenced.Store(true)
		return nil
	}
	o.lc = lc
	o.builderRunPool = newRunPool(runKindBuild, 0, time.Second, cfg.Paths.RunRoot, lc,
		o.builderUnit, o.log.With("pool", "builder-test"))
	poolCtx, cancelPool := context.WithCancel(context.Background())
	t.Cleanup(cancelPool)
	if err := o.builderRunPool.Start(poolCtx); err != nil {
		t.Fatal(err)
	}
	wantAttachErr := errors.New("injected attach failure after assignment")
	o.vs = failingCreateVS{err: wantAttachErr}

	manifestKey := strings.Repeat("f", 64)
	build := &types.Build{
		BuildID: "build-assigned-fence-order", TemplateID: "transient-build-assigned-fence-order",
		APISecret: deriveTestAPISecret(t, manifestKey), ManifestKey: manifestKey,
		Profile: types.ProfileBare, Status: types.BuildBuilding,
		FromImage: "example.invalid/base:latest", Resources: testBuildResources(),
		ExecutionClaimed: true, ExecutionClaimedUnix: time.Now().Unix(), CreatedUnix: time.Now().Unix(),
	}
	if err := o.st.PutBuild(context.Background(), build); err != nil {
		t.Fatal(err)
	}
	runDir := nodepath.BuildRunDir(cfg.Paths.RunRoot, build.BuildID)
	baseDir := nodepath.BuildBaseDir(cfg.Paths.BaseRoot, build.BuildID)
	var removed atomic.Int32
	o.removeBuildRunDir = func(path string) error {
		if !fenced.Load() {
			t.Fatal("BuildRunDir removal preceded assigned-unit fence")
		}
		if path != runDir {
			t.Fatalf("BuildRunDir cleanup path = %q, want %q", path, runDir)
		}
		removed.Add(1)
		return os.RemoveAll(path)
	}
	o.removeBuildBaseDir = func(path string) error {
		if !fenced.Load() {
			t.Fatal("BuildBaseDir removal preceded assigned-unit fence")
		}
		if path != baseDir {
			t.Fatalf("BuildBaseDir cleanup path = %q, want %q", path, baseDir)
		}
		removed.Add(1)
		return os.RemoveAll(path)
	}

	done := make(chan error, 1)
	go func() {
		_, err := o.runBuildUnit(context.Background(), build)
		done <- err
	}()
	var unit string
	select {
	case unit = <-lc.started:
	case <-time.After(time.Second):
		t.Fatal("builder unit was not started")
	}
	runID := o.builderUnitToRunID(unit)
	assigned, ok, err := o.WaitAssignment(context.Background(), runKindBuild, runID)
	if err != nil || !ok || assigned != build.BuildID {
		t.Fatalf("builder assignment = %q, %t, %v", assigned, ok, err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, wantAttachErr) {
			t.Fatalf("runBuildUnit error = %v, want %v", err, wantAttachErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("runBuildUnit did not finish")
	}
	if !fenced.Load() || removed.Load() != 2 {
		t.Fatalf("cleanup observations: fenced=%t removed=%d", fenced.Load(), removed.Load())
	}
}

func TestBuildPoolTerminallyRejectsPermanentlyUnfitFIFOHead(t *testing.T) {
	maxBuilds := int64(1)
	executionCPU := config.CPUCores("1")
	o := testOrchCfg(t, &config.Config{Builder: config.BuilderConfig{
		Admission: config.BuilderAdmissionConfig{
			Registration: &config.BuildAdmissionLimitConfig{MaxBuilds: &maxBuilds},
			Execution: &config.BuildAdmissionLimitConfig{
				MaxBuilds: &maxBuilds,
				Resources: config.BuildAdmissionResourcesConfig{CPU: &executionCPU},
			},
		},
	}})
	o.vs = failingCreateVS{err: errors.New("stop after execution claim")}
	manifestKey := strings.Repeat("e", 64)
	apiSecret := deriveTestAPISecret(t, manifestKey)
	for i, build := range []*types.Build{
		{
			BuildID: "fifo-unfit", TemplateID: "transient-fifo-unfit",
			APISecret: apiSecret, ManifestKey: manifestKey, Profile: types.ProfileBare,
			Status:      types.BuildWaiting,
			Resources:   types.BuildResources{CPU: 2000, Memory: 1 << 30},
			WaitingUnix: time.Now().Unix(), WaitingSequence: 1, CreatedUnix: time.Now().Unix(),
		},
		{
			BuildID: "fifo-fit", TemplateID: "transient-fifo-fit",
			APISecret: apiSecret, ManifestKey: manifestKey, Profile: types.ProfileBare,
			Status:      types.BuildWaiting,
			Resources:   types.BuildResources{CPU: 1000, Memory: 1 << 30},
			WaitingUnix: time.Now().Unix(), WaitingSequence: 2, CreatedUnix: time.Now().Unix(),
		},
	} {
		if err := o.st.PutBuild(context.Background(), build); err != nil {
			t.Fatalf("put build %d: %v", i, err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		o.BuildPool(ctx, time.Hour)
		close(done)
	}()
	deadline := time.Now().Add(2 * time.Second)
	for {
		unfit, err := o.st.GetBuild(context.Background(), "fifo-unfit")
		if err != nil {
			t.Fatal(err)
		}
		fit, err := o.st.GetBuild(context.Background(), "fifo-fit")
		if err != nil {
			t.Fatal(err)
		}
		if unfit.Status == types.BuildError && fit.Status != types.BuildWaiting {
			if !strings.Contains(unfit.Reason, "no longer fit") {
				t.Fatalf("unfit terminal reason = %q", unfit.Reason)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("scheduler remained blocked: unfit=%+v fit=%+v", unfit, fit)
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("BuildPool did not stop")
	}
	if err := o.DrainBuilds(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestTerminalPersistenceFailureRetainsClaimAndRetriesSafely(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "terminal-failure.db")
	o := testOrchCfgAt(t, &config.Config{}, dbPath)
	ctx := context.Background()
	manifestKey := strings.Repeat("d", 64)
	b := &types.Build{
		BuildID: "build-terminal-persistence", TemplateID: "transient-build-terminal-persistence",
		APISecret: deriveTestAPISecret(t, manifestKey), ManifestKey: manifestKey,
		Profile: types.ProfileE2B, Status: types.BuildBuilding,
		Resources: types.BuildResources{CPU: 1000, Memory: 1 << 30}, ExecutionClaimed: true,
		ExecutionClaimedUnix: time.Now().Unix(), CreatedUnix: time.Now().Unix(),
	}
	if err := o.st.PutBuild(ctx, b); err != nil {
		t.Fatal(err)
	}

	rawDB, err := sql.Open("sqlite", "file:"+dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer rawDB.Close()
	if _, err := rawDB.Exec(`CREATE TRIGGER fail_build_terminal BEFORE UPDATE OF status ON builds
		WHEN OLD.build_id='build-terminal-persistence'
		BEGIN SELECT RAISE(ABORT, 'injected terminal failure'); END`); err != nil {
		t.Fatal(err)
	}
	b.Status, b.Reason = types.BuildError, "injected failure"
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if o.persistTerminalBuild(canceled, b) {
		t.Fatal("terminal persistence succeeded while failure trigger was active")
	}
	stored, err := o.st.GetBuild(ctx, b.BuildID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Status != types.BuildBuilding || !stored.ExecutionClaimed {
		t.Fatalf("failed terminal write released ownership: %+v", stored)
	}

	if _, err := rawDB.Exec(`DROP TRIGGER fail_build_terminal`); err != nil {
		t.Fatal(err)
	}
	if !o.persistTerminalBuild(ctx, b) {
		t.Fatal("terminal persistence did not converge after fault removal")
	}
	stored, err = o.st.GetBuild(ctx, b.BuildID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Status != types.BuildError || stored.ExecutionClaimed {
		t.Fatalf("terminal retry result = %+v", stored)
	}
}
