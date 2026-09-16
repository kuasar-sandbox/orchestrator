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
func (l *assignmentOrderLauncher) Close() error                 { return nil }

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

func TestPrepareBuilderUnitDurablyBindsClaimBeforeAssignment(t *testing.T) {
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
	o.lc = lc
	runID := "br-00000000-0000-7000-8000-000000000205"
	o.runs.restore(runID, instanceUnit(o.cfg.Units.Builder, runID))
	unit, err := o.prepareBuilderUnit(ctx, b, runID)
	if err != nil {
		t.Fatal(err)
	}
	if unit != "sandbox-builder@"+runID+".service" {
		t.Fatalf("assignment barrier unit=%q", unit)
	}
	stored, err := o.st.GetBuild(ctx, b.BuildID)
	if err != nil || stored.RunID != runID {
		t.Fatalf("durable assignment = %+v, %v", stored, err)
	}
}

func TestPrepareBuilderUnitBindFailureDoesNotPublishOwnership(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "bind.db")
	o := testOrchCfgAt(t, &config.Config{}, dbPath)
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
	raw, err := sql.Open("sqlite", "file:"+dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	if _, err := raw.Exec("CREATE TRIGGER fail_run_bind BEFORE UPDATE OF run_id ON builds BEGIN SELECT RAISE(ABORT, 'bind failed'); END"); err != nil {
		t.Fatal(err)
	}
	o.lc = &assignmentOrderLauncher{}
	if _, err := o.prepareBuilderUnit(ctx, b, "br-failed"); err == nil || !strings.Contains(err.Error(), "bind failed") {
		t.Fatalf("prepare error = %v", err)
	}
	stored, err := o.st.GetBuild(ctx, b.BuildID)
	if err != nil || stored.RunID != "" || !stored.ExecutionClaimed || b.RunID != "" {
		t.Fatalf("failed durable binding changed ownership = %+v, %v", stored, err)
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
	b.TemplateID = "transient-" + b.BuildID
	b.APISecret, b.ManifestKey = strings.Repeat("1", 64), strings.Repeat("2", 64)
	if err := o.st.PutBuild(context.Background(), b); err != nil {
		t.Fatal(err)
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

func TestRunBuildUnitReusesUnchangedImageWithoutRuntimeSideEffects(t *testing.T) {
	o := testOrch(t)
	vs := &resourcePreflightVS{}
	o.vs = vs
	ref := "manifest://" + strings.Repeat("d", 64)
	target := &types.BuildTarget{Kind: types.BuildTargetImage}
	b := &types.Build{
		BuildID: "build-phase-free-image", Profile: types.ProfileE2B,
		FromTemplate: types.TemplateID{Profile: types.ProfileE2B, Kind: types.KindImg, Ref: ref}.String(),
		Builder:      types.BuildOptions{Target: target},
	}

	result, err := o.runBuildUnit(context.Background(), b)
	if err != nil {
		t.Fatal(err)
	}
	if result == nil || result.Target != *target || result.ImageRef != ref {
		t.Fatalf("phase-free image result = %+v", result)
	}
	if vs.attaches.Load() != 0 || b.RunID != "" || b.RuntimeVswitchPort != "" {
		t.Fatalf("phase-free image acquired runtime state: attaches=%d build=%+v", vs.attaches.Load(), b)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := o.runBuildUnit(canceled, b); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled phase-free image = %v, want context canceled", err)
	}
}

func TestDirectImageBuildResultDoesNotBypassBundlePublication(t *testing.T) {
	ref := "manifest://" + strings.Repeat("d", 64)
	target := &types.BuildTarget{Kind: types.BuildTargetImage}
	build := &types.Build{
		BuildID: "build-bundle-image", Profile: types.ProfileE2B,
		FromTemplate: types.TemplateID{Profile: types.ProfileE2B, Kind: types.KindImg, Ref: ref}.String(),
		Builder:      types.BuildOptions{Target: target},
	}
	if result, handled, err := directImageBuildResult(build, true); err != nil || handled || result != nil {
		t.Fatalf("Bundle-policy direct image = result %+v handled=%t err=%v", result, handled, err)
	}
	if result, handled, err := directImageBuildResult(build, false); err != nil || !handled || result == nil || result.ImageRef != ref {
		t.Fatalf("Manifest-policy direct image = result %+v handled=%t err=%v", result, handled, err)
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
	b.TemplateID = "transient-" + b.BuildID
	b.APISecret, b.ManifestKey = strings.Repeat("1", 64), strings.Repeat("2", 64)
	if err := o.st.PutBuild(context.Background(), b); err != nil {
		t.Fatal(err)
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
	o.builderRunPool = newRunPools(runKindBuild, cfg.Units.BuilderPoolConfigs(), time.Second, cfg.Paths.RunRoot, lc,
		&o.runs, o.log.With("pool", "builder-test"))
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

func TestFiniteExecutionIdlePoolBindsBeforePublishingAssignment(t *testing.T) {
	o := testOrch(t)
	o.cfg.Units.Builder = "sandbox-builder@.service"
	cpu, memory := config.CPUCores("2"), "2GiB"
	o.cfg.Units.BuilderPoolSize = 1
	o.cfg.Builder.Admission.Execution = &config.BuildAdmissionLimitConfig{
		Resources: config.BuildAdmissionResourcesConfig{CPU: &cpu, Memory: &memory},
	}
	lc := newRunPoolTestLauncher()
	o.lc = lc
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool := newRunPools(runKindBuild, o.cfg.Units.BuilderPoolConfigs(), time.Second, t.TempDir(), lc, &o.runs, o.log).pools[0]
	if err := pool.Start(ctx); err != nil {
		t.Fatal(err)
	}
	var runID string
	select {
	case unit := <-lc.started:
		runID = o.builderUnitToRunID(unit)
	case <-time.After(3 * time.Second):
		t.Fatal("idle builder did not start")
	}
	if runID == "" {
		t.Fatal("invalid builder run-id")
	}
	waiter := &runWaitReq{runID: runID, ctx: ctx, resp: make(chan runWaitResp, 1)}
	select {
	case pool.waitCh <- waiter:
	case <-time.After(3 * time.Second):
		t.Fatal("idle builder did not wait")
	}
	usage, err := o.st.BuildUsage(ctx)
	if err != nil || usage.RegistrationBuilds != 0 || usage.ExecutionBuilds != 0 {
		t.Fatalf("idle pool charged Build ledger: %+v, %v", usage, err)
	}
	b := observerBuildingFixture(t, "pool-bind")
	b.Status, b.RunID, b.ExecutionClaimed, b.ExecutionClaimedUnix = types.BuildWaiting, "", false, 0
	b.Resources = types.BuildResources{CPU: 1000, Memory: 1 << 30}
	if err := o.st.PutBuild(ctx, b); err != nil {
		t.Fatal(err)
	}
	limit, err := configresolve.BuilderExecutionLimit(o.cfg.Builder)
	if err != nil {
		t.Fatal(err)
	}
	if won, err := o.st.ClaimBuildExecution(ctx, b.BuildID, limit, time.Now()); err != nil || !won {
		t.Fatalf("claim=%t, %v", won, err)
	}
	b, err = o.st.GetBuild(ctx, b.BuildID)
	if err != nil {
		t.Fatal(err)
	}
	assigned, err := pool.Assign(ctx, b.BuildID, func(exact string) error {
		if _, err := o.prepareBuilderUnit(ctx, b, exact); err != nil {
			return err
		}
		stored, err := o.st.GetBuild(ctx, b.BuildID)
		if err != nil {
			return err
		}
		if stored.RunID != exact || !stored.ExecutionClaimed {
			return errors.New("assignment crossed durable binding")
		}
		select {
		case <-waiter.resp:
			return errors.New("worker received assignment before commit returned")
		default:
		}
		return nil
	})
	if err != nil || assigned != runID {
		t.Fatalf("assignment=%q, %v", assigned, err)
	}
	select {
	case result := <-waiter.resp:
		if result.err != nil || !result.ok || result.taskID != b.BuildID {
			t.Fatalf("worker result=%+v", result)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("bound assignment not published")
	}
	usage, err = o.st.BuildUsage(ctx)
	if err != nil || usage.ExecutionBuilds != 1 || usage.Execution.CPU != 1000 {
		t.Fatalf("active ledger=%+v, %v", usage, err)
	}
}
