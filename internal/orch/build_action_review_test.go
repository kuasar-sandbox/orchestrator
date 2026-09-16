package orch

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/launcher"
	"github.com/kuasar-sandbox/orchestrator/internal/nodepath"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestCancelledRecoveryPrecedesUnrelatedAdoptionFailure(t *testing.T) {
	ctx := context.Background()
	cfg := buildReconcileConfig(filepath.Join(t.TempDir(), "run"))
	o := testOrchCfg(t, cfg)
	cancelled := buildReconcileRow(t, "br-review-cancel")
	if err := o.st.PutBuild(ctx, cancelled); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := o.st.RequestBuildAction(ctx, cancelled, true, true, time.Now()); err != nil {
		t.Fatal(err)
	}
	other := buildReconcileRow(t, "br-review-other")
	other.BuildID, other.TemplateID = "other-build", "transient-other-build"
	// Missing durable execution ownership is still a real adoption failure;
	// parent unit resource attributes no longer participate in recovery.
	other.ExecutionClaimed, other.ExecutionClaimedUnix = false, 0
	if err := o.st.PutBuild(ctx, other); err != nil {
		t.Fatal(err)
	}
	cancelUnit, otherUnit := instanceUnit(o.cfg.Units.BuilderPoolConfigs()[0].Unit, cancelled.RunID), instanceUnit(o.cfg.Units.BuilderPoolConfigs()[0].Unit, other.RunID)
	lc := &reconcileLauncher{units: []launcher.Unit{{Name: cancelUnit, ActiveState: "active"}, {Name: otherUnit, ActiveState: "active"}}}
	o.lc, o.vs = lc, &reconcileVS{}
	err := o.ReconcileBuilds(ctx)
	if err == nil || !strings.Contains(err.Error(), "building row has no execution claim") {
		t.Fatalf("expected other Build ownership failure, got %v", err)
	}
	for _, unit := range lc.stopped {
		if unit == cancelUnit {
			return
		}
	}
	t.Fatalf("durably cancelled unit was not stopped before unrelated ownership validation failed: stopped=%v err=%v", lc.stopped, err)
}

func TestPostTerminalDeleteFencesRegistrationReplay(t *testing.T) {
	ctx := context.Background()
	o := testOrch(t)
	_, _, fp := allowlistedBuildIdentity(t, o)
	cmd := clusterBuildRegisterCommand("review-late-replay", fp)
	if err := o.registerClusterBuild(ctx, cmd); err != nil {
		t.Fatal(err)
	}
	b, err := o.st.GetBuild(ctx, cmd.BuildID)
	if err != nil {
		t.Fatal(err)
	}
	b.Status, b.ExecutionClaimed = types.BuildBuilding, true
	if err := o.st.PutBuild(ctx, b); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := o.st.RequestBuildAction(ctx, b, true, true, time.Now()); err != nil {
		t.Fatal(err)
	}
	replayDone := make(chan error, 1)
	o.completeBuildWithPublisher(ctx, b, nil, context.Canceled, func(_, _, _, _ string) {
		// Terminal is durable and the completion owns the event fence. Replay can
		// read that terminal under buildRetention before waiting on this fence.
		go func() { replayDone <- o.registerClusterBuild(ctx, cmd) }()
		deadline := time.Now().Add(time.Second)
		for {
			o.buildEventFences.mu.Lock()
			lock := o.buildEventFences.locks[b.BuildID]
			waiting := lock != nil && lock.refs == 2
			o.buildEventFences.mu.Unlock()
			if waiting {
				return
			}
			if time.Now().After(deadline) {
				t.Fatal("replay never reached event fence")
			}
			runtime.Gosched()
		}
	})
	if err := <-replayDone; err != nil {
		t.Fatal(err)
	}
	row, err := o.st.GetBuild(ctx, b.BuildID)
	if err != nil {
		t.Fatal(err)
	}
	if row != nil {
		t.Fatalf("old registration replay resurrected deleted identity: status=%s transient=%s cancel=%d delete=%d", row.Status, row.TemplateID, row.CancelRequestedUnix, row.DeleteRequestedUnix)
	}
}

type cleanupPhaseReleaseBarrier struct {
	entered chan string
	release chan struct{}
}

func (p *cleanupPhaseReleaseBarrier) SandboxResourceStats(id string) (api.ResourceStats, bool) {
	select {
	case p.entered <- id:
	default:
	}
	select {
	case <-p.release:
		return api.ResourceStats{}, false
	default:
		return api.ResourceStats{}, true
	}
}

func TestCancelledOwnerCleanupWaitsForDurablePhaseReservation(t *testing.T) {
	ctx := context.Background()
	o := testOrchCfg(t, buildReconcileConfig(filepath.Join(t.TempDir(), "run")))
	b := buildReconcileRow(t, "br-cancel-phase-reservation")
	if err := o.st.PutBuild(ctx, b); err != nil {
		t.Fatal(err)
	}
	// The execution owner's pointer does not receive later SQLite phase reports.
	const phaseID = "bp-b-current-phase"
	if err := o.st.SetBuildPhase(ctx, b.BuildID, "b", phaseID, "starting"); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := o.st.RequestBuildAction(ctx, b, true, true, time.Now()); err != nil {
		t.Fatal(err)
	}
	provider := &cleanupPhaseReleaseBarrier{make(chan string, 1), make(chan struct{})}
	o.SetSandboxResourceProvider(provider)
	o.lc = &reconcileLauncher{units: []launcher.Unit{{Name: instanceUnit(o.cfg.Units.BuilderPoolConfigs()[0].Unit, b.RunID), ActiveState: "active"}}}
	o.vs = &reconcileVS{}
	done := make(chan error, 1)
	go func() {
		cause, err := o.retryBuildCleanup(ctx, b, &buildCleanupPendingError{cause: context.Canceled, unit: instanceUnit(o.cfg.Units.BuilderPoolConfigs()[0].Unit, b.RunID), port: b.RuntimeVswitchPort, persisted: true})
		if err == nil {
			o.completeBuild(ctx, b, nil, cause)
		}
		done <- err
	}()
	released := false
	t.Cleanup(func() {
		if !released {
			close(provider.release)
		}
	})
	select {
	case id := <-provider.entered:
		if id != phaseID {
			t.Fatalf("reservation id=%q", id)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cleanup did not inspect durable phase")
	}
	row, err := o.st.GetBuild(ctx, b.BuildID)
	if err != nil || row == nil || !row.ExecutionClaimed || row.PhaseSandboxID != phaseID {
		t.Fatalf("ownership released before phase: %+v %v", row, err)
	}
	usage, err := o.st.BuildUsage(ctx)
	if err != nil || usage.ExecutionBuilds != 1 || usage.RegistrationBuilds != 0 {
		t.Fatalf("usage during reservation: %+v %v", usage, err)
	}
	actionCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	if res, err := o.DeleteBuildAdmin(actionCtx, b.TemplateID, api.DeleteBuildOptions{}); err != nil || !res.Pending {
		t.Fatalf("repeat action blocked by cleanup fence: %+v %v", res, err)
	}
	select {
	case err := <-done:
		t.Fatalf("cleanup completed before release: %v", err)
	default:
	}
	close(provider.release)
	released = true
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cleanup did not finish after reservation release")
	}
	row, err = o.st.GetBuild(ctx, b.BuildID)
	if err != nil || row != nil {
		t.Fatalf("post-release delete: %+v %v", row, err)
	}
}

func TestRecoveryReleasesTerminalOwnershipWithoutChangingResult(t *testing.T) {
	for _, state := range []types.BuildState{types.BuildReady, types.BuildError} {
		for _, shape := range []string{"claim", "runtime-without-claim", "phase-without-run"} {
			for _, remove := range []bool{false, true} {
				t.Run(string(state)+"/"+shape+"/"+map[bool]string{false: "cancel", true: "delete"}[remove], func(t *testing.T) {
					ctx := context.Background()
					o := testOrchCfg(t, buildReconcileConfig(filepath.Join(t.TempDir(), "run")))
					b := buildReconcileRow(t, "br-terminal-owned")
					b.Status, b.Reason, b.FinishedUnix = state, "accepted diagnosis", 12345
					b.PersistID = types.TemplateID{Profile: types.ProfileE2B, Kind: types.KindImg, Ref: "manifest://accepted"}.String()
					if shape != "claim" {
						b.ExecutionClaimed, b.ExecutionClaimedUnix = false, 0
					}
					if shape == "phase-without-run" {
						b.RunID, b.RuntimeVswitchPort, b.RuntimeFloatingIP, b.RuntimePortMAC, b.RuntimePrepareJSON = "", "", "", "", ""
						b.Phase, b.PhaseSandboxID = "b", "bp-terminal-phase"
					}
					if err := o.st.PutBuild(ctx, b); err != nil {
						t.Fatal(err)
					}
					var res api.BuildActionResult
					var err error
					if remove {
						res, err = o.DeleteBuildAdmin(ctx, b.TemplateID, api.DeleteBuildOptions{Cancel: true})
					} else {
						res, err = o.CancelBuildAdmin(ctx, b.BuildID)
					}
					if err != nil || !res.Pending {
						t.Fatalf("owned terminal action: %+v %v", res, err)
					}
					lc := &reconcileLauncher{}
					if b.RunID != "" {
						lc.units = []launcher.Unit{{Name: instanceUnit(o.cfg.Units.BuilderPoolConfigs()[0].Unit, b.RunID), ActiveState: "active"}}
					}
					o.lc, o.vs = lc, &reconcileVS{}
					if err := o.ReconcileBuilds(ctx); err != nil {
						t.Fatal(err)
					}
					row, err := o.st.GetBuild(ctx, b.BuildID)
					if err != nil {
						t.Fatal(err)
					}
					if remove {
						if row != nil {
							t.Fatalf("terminal not deleted: %+v", row)
						}
					} else {
						if row == nil || row.Status != state || row.Reason != b.Reason || row.PersistID != b.PersistID || row.FinishedUnix != b.FinishedUnix || row.ExecutionClaimed || row.RunID != "" || row.PhaseSandboxID != "" || row.RuntimeVswitchPort != "" {
							t.Fatalf("terminal outcome/ownership: %+v", row)
						}
					}
					usage, err := o.st.BuildUsage(ctx)
					if err != nil || usage.ExecutionBuilds != 0 || usage.RegistrationBuilds != 0 {
						t.Fatalf("recovered usage: %+v %v", usage, err)
					}
				})
			}
		}
	}
}

// Exercise the complete claim/assignment/cancel/cleanup lifetime repeatedly.
// The existing barrier test joins both host preparation and the execution owner
// and drains accepted operations before its Store and launcher are closed.
func TestBuildCancellationDrainsExecutionGoroutinesAndFDs(t *testing.T) {
	countFDs := func() int {
		entries, err := os.ReadDir("/proc/self/fd")
		if err != nil {
			t.Fatal(err)
		}
		return len(entries)
	}
	t.Run("warm", TestBuildCancelBeforeRunBindingRetainsExactUnitFence)
	beforeG, beforeFD := runtime.NumGoroutine(), countFDs()
	for i := 0; i < 3; i++ {
		t.Run(fmt.Sprint(i), TestBuildCancelBeforeRunBindingRetainsExactUnitFence)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		afterG, afterFD := runtime.NumGoroutine(), countFDs()
		if afterG <= beforeG && afterFD <= beforeFD {
			t.Logf("joined cancellation lifetimes: goroutines %d -> %d, FDs %d -> %d", beforeG, afterG, beforeFD, afterFD)
			return
		}
		if time.Now().After(deadline) {
			entries, _ := os.ReadDir("/proc/self/fd")
			for _, entry := range entries {
				path, _ := os.Readlink("/proc/self/fd/" + entry.Name())
				t.Logf("fd %s: %s", entry.Name(), path)
			}
			t.Fatalf("resources remained after draining cancellation: goroutines %d -> %d, FDs %d -> %d", beforeG, afterG, beforeFD, afterFD)
		}
		runtime.Gosched()
	}
}

func TestBuildRuntimeOnlyOwnershipSurvivesDirectoryCleanup(t *testing.T) {
	ctx := context.Background()
	cfg := buildReconcileConfig(filepath.Join(t.TempDir(), "run"))
	o := testOrchCfg(t, cfg)
	b := buildReconcileRow(t, "")
	b.Status, b.ExecutionClaimed, b.ExecutionClaimedUnix = types.BuildError, false, 0
	if err := o.st.PutBuild(ctx, b); err != nil {
		t.Fatal(err)
	}
	res, err := o.DeleteBuildAdmin(ctx, b.TemplateID, api.DeleteBuildOptions{Cancel: true})
	if err != nil || !res.Pending {
		t.Fatalf("initial delete=%+v %v", res, err)
	}
	runDir, baseDir := nodepath.BuildRunDir(cfg.Paths.RunRoot, b.BuildID), nodepath.BuildBaseDir(cfg.Paths.BaseRoot, b.BuildID)
	for _, p := range []string{runDir, baseDir} {
		if err := os.MkdirAll(p, 0700); err != nil {
			t.Fatal(err)
		}
	}
	o.lc, o.vs = &reconcileLauncher{}, &reconcileVS{}
	entered, release := make(chan struct{}), make(chan struct{})
	o.removeBuildRunDir = func(p string) error { close(entered); <-release; return os.RemoveAll(p) }
	done := make(chan error, 1)
	go func() { done <- o.ReconcileBuilds(ctx) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("cleanup did not enter directory removal")
	}
	res, err = o.DeleteBuildAdmin(ctx, b.TemplateID, api.DeleteBuildOptions{})
	_, statErr := os.Stat(runDir)
	if err != nil || !res.Pending {
		t.Errorf("Delete reported completed before directory cleanup finished: result=%+v err=%v runDirStillPresent=%v", res, err, statErr == nil)
	}
	close(release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("recovery did not finish")
	}
}

func TestBuildRuntimeOnlyNonterminalIntentCompletes(t *testing.T) {
	for _, state := range []types.BuildState{types.BuildRegistered, types.BuildWaiting} {
		for _, remove := range []bool{false, true} {
			t.Run(string(state)+map[bool]string{false: "-cancel", true: "-delete"}[remove], func(t *testing.T) {
				ctx := context.Background()
				cfg := buildReconcileConfig(filepath.Join(t.TempDir(), "run"))
				o := testOrchCfg(t, cfg)
				b := buildReconcileRow(t, "")
				b.Status, b.ExecutionClaimed, b.ExecutionClaimedUnix = state, false, 0
				if err := o.st.PutBuild(ctx, b); err != nil {
					t.Fatal(err)
				}
				if _, _, pending, err := o.st.RequestBuildAction(ctx, b, remove, true, time.Now()); err != nil || !pending {
					t.Fatalf("pending=%v err=%v", pending, err)
				}
				o.lc, o.vs = &reconcileLauncher{}, &reconcileVS{}
				if err := o.ReconcileBuilds(ctx); err != nil {
					t.Fatal(err)
				}
				row, err := o.st.GetBuild(ctx, b.BuildID)
				if err != nil {
					t.Fatal(err)
				}
				if remove && row != nil {
					t.Fatalf("recovery silently stranded delete: status=%s cancel=%d delete=%d run=%q claim=%v port=%q", row.Status, row.CancelRequestedUnix, row.DeleteRequestedUnix, row.RunID, row.ExecutionClaimed, row.RuntimeVswitchPort)
				}
				if !remove && (row == nil || row.Status != types.BuildError) {
					t.Fatalf("recovery did not finish cancel; status=%s", row.Status)
				}
			})
		}
	}
}

func TestBuildTerminalBudgetWakeDoesNotWaitForRecordDeletion(t *testing.T) {
	ctx := context.Background()
	o := testOrch(t)
	b := buildReconcileRow(t, "")
	b.RuntimeVswitchPort, b.RuntimeFloatingIP, b.RuntimePortMAC, b.RuntimePrepareJSON = "", "", "", ""
	if err := o.st.PutBuild(ctx, b); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := o.st.RequestBuildAction(ctx, b, true, true, time.Now()); err != nil {
		t.Fatal(err)
	}
	retention := o.buildRetention.Lock(b.BuildID)
	committed, done := make(chan struct{}), make(chan struct{})
	go func() {
		o.completeBuildWithPublisher(ctx, b, nil, context.Canceled, func(_, _, _, _ string) { close(committed) })
		close(done)
	}()
	<-committed
	deadline := time.Now().Add(time.Second)
	for {
		o.buildRetention.mu.Lock()
		lock := o.buildRetention.locks[b.BuildID]
		blocked := lock != nil && lock.refs == 2
		o.buildRetention.mu.Unlock()
		if blocked {
			break
		}
		if time.Now().After(deadline) {
			retention()
			t.Fatal("completion did not reach deletion lock")
		}
		runtime.Gosched()
	}
	row, err := o.st.GetBuild(ctx, b.BuildID)
	if err != nil || row.ExecutionClaimed {
		retention()
		t.Fatalf("terminal not committed: %v", err)
	}
	select {
	case <-o.buildWake:
	default:
		t.Error("released execution budget did not wake scheduler while final record deletion waits")
	}
	select {
	case <-o.buildUsageWake:
	default:
		t.Error("released execution budget did not update node usage while final record deletion waits")
	}
	retention()
	<-done
}

func TestBuildOwnerFreeBuildingIntentCannotReportRecoverySuccess(t *testing.T) {
	ctx := context.Background()
	cfg := buildReconcileConfig(filepath.Join(t.TempDir(), "run"))
	o := testOrchCfg(t, cfg)
	b := buildReconcileRow(t, "")
	b.Status, b.ExecutionClaimed, b.ExecutionClaimedUnix = types.BuildBuilding, false, 0
	b.RuntimeVswitchPort, b.RuntimeFloatingIP, b.RuntimePortMAC, b.RuntimePrepareJSON = "", "", "", ""
	if err := o.st.PutBuild(ctx, b); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := o.st.RequestBuildAction(ctx, b, true, true, time.Now()); err != nil {
		t.Fatal(err)
	}
	o.lc, o.vs = &reconcileLauncher{}, &reconcileVS{}
	err := o.ReconcileBuilds(ctx)
	row, readErr := o.st.GetBuild(ctx, b.BuildID)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if err == nil && row != nil && row.Status == types.BuildBuilding {
		t.Fatal("recovery reported success although intent remains building with no owner and no recovery path")
	}
}
