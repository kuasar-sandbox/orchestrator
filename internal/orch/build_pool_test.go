package orch

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/configresolve"
	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/launcher"
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
		Kind:        types.KindImg,
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
		Profile: types.ProfileE2B, Kind: types.KindImg, Status: types.BuildBuilding,
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
		Profile: types.ProfileE2B, Kind: types.KindImg, Status: types.BuildBuilding,
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
	o.cfg.Builder.TotalTimeoutSec = 1
	b := &types.Build{
		BuildID: "build-expired-before-assignment", Profile: types.ProfileBare,
		FromImage: "example.invalid/base:latest", Status: types.BuildBuilding,
		ExecutionClaimed: true, ExecutionClaimedUnix: time.Now().Add(-2 * time.Minute).Unix(),
	}
	if _, err := o.runBuildUnit(context.Background(), b); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expired assignment = %v, want deadline exceeded", err)
	}
	if b.RunID != "" || b.RuntimeVswitchPort != "" {
		t.Fatalf("expired build acquired runtime ownership: %+v", b)
	}
	if _, err := os.Stat(buildRuntimeDir(o.cfg.Paths.RunRoot, b.BuildID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expired build workdir remained: %v", err)
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
	want := configsock.BuildResult{ImageRef: "manifest://accepted-after-exit"}
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

func TestRunBuildUnitCleansWorkdirWhenRequestResolutionFails(t *testing.T) {
	o := testOrch(t)
	o.cfg.Paths.RunRoot = t.TempDir()
	b := &types.Build{
		BuildID: "build-invalid-request-input", Profile: types.ProfileBare,
		Status: types.BuildBuilding, PhaseResourcePatch: `{"capacity":`,
	}

	if _, err := o.runBuildUnit(context.Background(), b); err == nil {
		t.Fatal("malformed phase resource patch was accepted")
	}
	if _, err := os.Stat(buildRuntimeDir(o.cfg.Paths.RunRoot, b.BuildID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("request-resolution failure retained build workdir: %v", err)
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
			Kind: types.KindImg, Status: types.BuildWaiting,
			Resources:   types.BuildResources{CPU: 2000, Memory: 1 << 30},
			WaitingUnix: time.Now().Unix(), WaitingSequence: 1, CreatedUnix: time.Now().Unix(),
		},
		{
			BuildID: "fifo-fit", TemplateID: "transient-fifo-fit",
			APISecret: apiSecret, ManifestKey: manifestKey, Profile: types.ProfileBare,
			Kind: types.KindImg, Status: types.BuildWaiting,
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
		Profile: types.ProfileE2B, Kind: types.KindImg, Status: types.BuildBuilding,
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
