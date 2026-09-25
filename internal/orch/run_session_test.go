package orch

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestRunSessionReplacementIgnoresOldClose(t *testing.T) {
	o := testOrch(t)
	runID := "sr-00000000-0000-7000-8000-000000000426"
	o.runs.restore(runID, testRunUnit(runID))

	first, ok, err := o.RegisterRunSession(context.Background(), runKindSandbox, runID)
	if err != nil || !ok {
		t.Fatalf("first RegisterRunSession = ok %v err %v", ok, err)
	}
	second, ok, err := o.RegisterRunSession(context.Background(), runKindSandbox, runID)
	if err != nil || !ok {
		t.Fatalf("second RegisterRunSession = ok %v err %v", ok, err)
	}
	first.Close(false)
	if !o.runSessionActive(runKindSandbox, runID) {
		t.Fatal("old session close removed its successor")
	}
	second.Close(true)
	if o.runSessionActive(runKindSandbox, runID) {
		t.Fatal("shutdown close retained current session registration")
	}
}

func TestRunSessionRejectsFormattedButUnknownRunID(t *testing.T) {
	o := testOrch(t)
	if _, ok, err := o.RegisterRunSession(context.Background(), runKindSandbox, "sr-00000000-0000-7000-8000-000000000427"); err != nil || ok {
		t.Fatalf("unknown formatted run admission = ok %v err %v", ok, err)
	}
}

func TestRunSessionDisconnectRetiresOnlyUnassignedPoolWorker(t *testing.T) {
	o := testOrch(t)
	p, lc, baseCtx, _ := startRunPoolTestWithIndex(t, 1, &o.runs)
	o.lc = lc
	runID, waiter := addIdleRunForTest(t, p, lc, baseCtx)
	session, ok, err := o.RegisterRunSession(context.Background(), runKindSandbox, runID)
	if err != nil || !ok {
		t.Fatalf("RegisterRunSession = ok %v err %v", ok, err)
	}
	session.Close(false)
	select {
	case got := <-waiter.resp:
		if got.err == nil || got.ok || got.taskID != "" {
			t.Fatalf("retired waiter = %+v, want error", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("unassigned disconnected waiter was not released")
	}
	select {
	case unit := <-lc.stopped:
		if unit != testRunUnit(runID) {
			t.Fatalf("stopped unit = %q", unit)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("unassigned disconnected run was not stopped")
	}
	select {
	case unit := <-lc.reset:
		if unit != testRunUnit(runID) {
			t.Fatalf("reset unit = %q", unit)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("unassigned disconnected run was not reset")
	}
	if unit := o.runs.unit(runID); unit != "" {
		t.Fatalf("retired unassigned run remained indexed as %q", unit)
	}
}

func TestRunSessionDisconnectRetirementLosesToSandboxAssignmentCommit(t *testing.T) {
	fixture := newSandboxFinalizerFixture(t, "session-assign-race")
	fixture.sb.State = types.StateStarting
	fixture.sb.RunID = ""
	fixture.sb.LaunchMode = types.LaunchImage
	fixture.sb.ExecutionResult = nil
	if err := fixture.o.st.Put(context.Background(), fixture.sb); err != nil {
		t.Fatal(err)
	}
	p, lc, baseCtx, _ := startRunPoolTestWithIndex(t, 1, &fixture.o.runs)
	fixture.o.lc = lc
	runID, waiter := addIdleRunForTest(t, p, lc, baseCtx)
	session, ok, err := fixture.o.RegisterRunSession(context.Background(), runKindSandbox, runID)
	if err != nil || !ok {
		t.Fatalf("RegisterRunSession = ok %v err %v", ok, err)
	}
	commitStarted := make(chan struct{})
	commitGate := make(chan struct{})
	assignDone := make(chan error, 1)
	go func() {
		_, err := p.Assign(baseCtx, fixture.sb.ID, func(gotRunID string) error {
			if gotRunID != runID {
				t.Errorf("commit runID = %q, want %q", gotRunID, runID)
			}
			changed, err := fixture.o.st.BindStartingRunner(context.Background(), fixture.sb.ID, gotRunID)
			if err != nil || !changed {
				return errors.New("BindStartingRunner did not commit")
			}
			close(commitStarted)
			<-commitGate
			return nil
		})
		assignDone <- err
	}()
	select {
	case <-commitStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("assignment commit did not start")
	}
	disconnectDone := make(chan struct{})
	go func() {
		session.Close(false)
		close(disconnectDone)
	}()
	select {
	case <-disconnectDone:
		t.Fatal("disconnect retirement completed while assignment commit was in progress")
	case <-time.After(50 * time.Millisecond):
	}
	close(commitGate)
	select {
	case err := <-assignDone:
		if err != nil {
			t.Fatalf("Assign: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("assignment did not finish")
	}
	select {
	case got := <-waiter.resp:
		if !got.ok || got.taskID != fixture.sb.ID || got.err != nil {
			t.Fatalf("waiter assignment = %+v", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waiter did not receive assignment")
	}
	select {
	case <-disconnectDone:
	case <-time.After(2 * time.Second):
		t.Fatal("disconnect did not finish after assignment")
	}
	select {
	case stopped := <-lc.stopped:
		t.Fatalf("disconnect retirement stopped assigned unit %s", stopped)
	case <-time.After(50 * time.Millisecond):
	}
	got, err := fixture.o.st.Get(context.Background(), fixture.sb.ID)
	if err != nil || got == nil || got.RunID != runID || got.State != types.StateStarting {
		t.Fatalf("durable assignment after disconnect race = %+v, %v", got, err)
	}
}

func waitRunDisconnectIdle(t *testing.T, o *Orchestrator, kind, runID string) {
	t.Helper()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	timeout := time.NewTimer(2 * time.Second)
	defer timeout.Stop()
	for {
		if !o.runDisconnectCheckActive(kind, runID) {
			return
		}
		select {
		case <-ticker.C:
		case <-timeout.C:
			t.Fatalf("run disconnect check for %s/%s did not retire", kind, runID)
		}
	}
}

func TestWaitAssignmentReplaysDurableSandboxRunBinding(t *testing.T) {
	fixture := newSandboxFinalizerFixture(t, "assignment-replay")
	fixture.sb.State = types.StateStarting
	fixture.sb.LaunchMode = types.LaunchImage
	if err := fixture.o.st.Put(context.Background(), fixture.sb); err != nil {
		t.Fatal(err)
	}

	sandboxID, ok, err := fixture.o.WaitAssignment(context.Background(), runKindSandbox, fixture.sb.RunID)
	if err != nil || !ok || sandboxID != fixture.sb.ID {
		t.Fatalf("durable sandbox assignment replay = %q, %t, %v", sandboxID, ok, err)
	}

	result := types.SandboxExecutionResult{SID: fixture.sb.ID, RunID: fixture.sb.RunID, Stage: types.SandboxResultStart, Error: "lost assignment response result"}
	if inserted, err := fixture.o.st.AcceptSandboxExecutionResult(context.Background(), fixture.sb.ID, fixture.sb.RunID, result); err != nil || !inserted {
		t.Fatalf("AcceptSandboxExecutionResult = %t, %v", inserted, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if replay, ok, err := fixture.o.WaitAssignment(ctx, runKindSandbox, fixture.sb.RunID); err == nil || ok || replay != "" {
		t.Fatalf("assignment replay after accepted result = %q, %t, %v; want no replay", replay, ok, err)
	}
}

func TestRunSessionAdmitsCurrentRunningSandboxOwner(t *testing.T) {
	fixture := newSandboxFinalizerFixture(t, "session-running-owner")
	session, ok, err := fixture.o.RegisterRunSession(context.Background(), runKindSandbox, fixture.sb.RunID)
	if err != nil || !ok || session == nil {
		t.Fatalf("RegisterRunSession for running sandbox = session %T ok %v err %v", session, ok, err)
	}
	session.Close(true)
}

func TestRunSessionDisconnectPreservesLiveAssignedSandbox(t *testing.T) {
	fixture := newSandboxFinalizerFixture(t, "session-live-running")
	session, ok, err := fixture.o.RegisterRunSession(context.Background(), runKindSandbox, fixture.sb.RunID)
	if err != nil || !ok {
		t.Fatalf("RegisterRunSession = ok %v err %v", ok, err)
	}
	session.Close(false)
	waitRunDisconnectIdle(t, fixture.o, runKindSandbox, fixture.sb.RunID)
	got, err := fixture.o.st.Get(context.Background(), fixture.sb.ID)
	if err != nil || got == nil {
		t.Fatalf("live assigned row after disconnect = %+v, %v", got, err)
	}
	if got.State != types.StateRunning || got.RunID != fixture.sb.RunID || got.ExecutionResult != nil {
		t.Fatalf("live assigned disconnect changed sandbox: %+v", got)
	}
	if _, stops := fixture.lc.stopSnapshot(); stops != 0 {
		t.Fatalf("live assigned disconnect stopped runner %d times", stops)
	}
}

func TestRunSessionDisconnectInactiveRunningAcceptsResultAndCleansDead(t *testing.T) {
	fixture := newSandboxFinalizerFixture(t, "session-dead-running")
	fixture.lc.mu.Lock()
	fixture.lc.state = "inactive"
	fixture.lc.mu.Unlock()
	session, ok, err := fixture.o.RegisterRunSession(context.Background(), runKindSandbox, fixture.sb.RunID)
	if err != nil || !ok {
		t.Fatalf("RegisterRunSession = ok %v err %v", ok, err)
	}
	session.Close(false)
	dead := waitForSandbox(t, fixture.o, context.Background(), fixture.sb.ID, func(sb *types.Sandbox) bool {
		return sb.State == types.StateDead && sb.ExecutionResult != nil
	}, "dead after inactive run session disconnect")
	if dead.ExecutionResult.RunID != fixture.sb.RunID || dead.ExecutionResult.Stage != types.SandboxResultRun || dead.ExecutionResult.Error == "" {
		t.Fatalf("disconnect result = %+v", dead.ExecutionResult)
	}
	for _, path := range []string{fixture.sb.RunDir, fixture.sb.BaseDir} {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("disconnect cleanup retained %s: %v", path, err)
		}
	}
}

func TestRunSessionDisconnectStartingPreservesHealthyActiveUnit(t *testing.T) {
	fixture := newSandboxFinalizerFixture(t, "session-starting-active")
	fixture.sb.State = types.StateStarting
	fixture.sb.LaunchMode = types.LaunchImage
	if err := fixture.o.st.Put(context.Background(), fixture.sb); err != nil {
		t.Fatal(err)
	}
	attempt, err := fixture.o.launches.Claim(context.Background(), fixture.sb.ID, launchCreate)
	if err != nil {
		t.Fatal(err)
	}
	attempt.SetRunID(fixture.sb.RunID)
	session, ok, err := fixture.o.RegisterRunSession(context.Background(), runKindSandbox, fixture.sb.RunID)
	if err != nil || !ok {
		t.Fatalf("RegisterRunSession = ok %v err %v", ok, err)
	}
	session.Close(false)
	waitRunDisconnectIdle(t, fixture.o, runKindSandbox, fixture.sb.RunID)
	select {
	case <-attempt.Context().Done():
		t.Fatal("active starting unit was canceled after session disconnect")
	case <-time.After(50 * time.Millisecond):
	}
	fixture.o.launches.Finish(attempt, nil)
}

func TestRunSessionDisconnectStartingWakesRollbackAfterUnitEnded(t *testing.T) {
	fixture := newSandboxFinalizerFixture(t, "session-starting-ended")
	fixture.sb.State = types.StateStarting
	fixture.sb.LaunchMode = types.LaunchImage
	if err := fixture.o.st.Put(context.Background(), fixture.sb); err != nil {
		t.Fatal(err)
	}
	setSandboxFinalizerUnitState(t, fixture, "inactive")
	attempt, err := fixture.o.launches.Claim(context.Background(), fixture.sb.ID, launchCreate)
	if err != nil {
		t.Fatal(err)
	}
	attempt.SetRunID(fixture.sb.RunID)
	session, ok, err := fixture.o.RegisterRunSession(context.Background(), runKindSandbox, fixture.sb.RunID)
	if err != nil || !ok {
		t.Fatalf("RegisterRunSession = ok %v err %v", ok, err)
	}
	session.Close(false)
	waitRunDisconnectIdle(t, fixture.o, runKindSandbox, fixture.sb.RunID)
	select {
	case <-attempt.Context().Done():
	case <-time.After(2 * time.Second):
		t.Fatal("ended starting unit did not cancel launch attempt")
	}
	fixture.o.launches.Finish(attempt, nil)
}

func TestRunSessionDisconnectDelegatesPausedAndDeletingCleanupOwners(t *testing.T) {
	fixture := newSandboxFinalizerFixture(t, "session-paused")
	fixture.sb.State = types.StatePaused
	fixture.sb.LaunchMode = ""
	fixture.sb.ResumeSource = types.ResumeSource{Kind: types.ResumeSourceSandbox, Ref: "manifest://cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"}
	if err := fixture.o.st.Put(context.Background(), fixture.sb); err != nil {
		t.Fatal(err)
	}
	session, ok, err := fixture.o.RegisterRunSession(context.Background(), runKindSandbox, fixture.sb.RunID)
	if err != nil || !ok {
		t.Fatalf("paused RegisterRunSession = ok %v err %v", ok, err)
	}
	session.Close(false)
	paused := waitForSandbox(t, fixture.o, context.Background(), fixture.sb.ID, func(sb *types.Sandbox) bool {
		return sb.State == types.StatePaused && sb.RunID == "" && sb.RunDir == "" && sb.VswitchPort == ""
	}, "paused ownership cleanup after disconnect")
	if paused.ResumeSource != fixture.sb.ResumeSource {
		t.Fatalf("paused cleanup changed checkpoint source: %+v", paused)
	}

	deleting := newSandboxFinalizerFixture(t, "session-deleting")
	beginDeletingForTest(t, deleting)
	session, ok, err = deleting.o.RegisterRunSession(context.Background(), runKindSandbox, deleting.sb.RunID)
	if err != nil || !ok {
		t.Fatalf("deleting RegisterRunSession = ok %v err %v", ok, err)
	}
	session.Close(false)
	waitForSandboxAbsent(t, deleting.o, context.Background(), deleting.sb.ID, "delete finalizer after disconnect")
}

func setSandboxFinalizerUnitState(t *testing.T, fixture sandboxFinalizerFixture, state string) {
	t.Helper()
	fixture.lc.mu.Lock()
	fixture.lc.state = state
	fixture.lc.mu.Unlock()
}

func TestRunSessionMaintenanceConvergesAfterHealthyDisconnectThenUnitExit(t *testing.T) {
	fixture := newSandboxFinalizerFixture(t, "session-maint-late-exit")
	session, ok, err := fixture.o.RegisterRunSession(context.Background(), runKindSandbox, fixture.sb.RunID)
	if err != nil || !ok {
		t.Fatalf("RegisterRunSession = ok %v err %v", ok, err)
	}
	session.Close(false)
	waitRunDisconnectIdle(t, fixture.o, runKindSandbox, fixture.sb.RunID)
	got, err := fixture.o.st.Get(context.Background(), fixture.sb.ID)
	if err != nil || got == nil || got.State != types.StateRunning || got.ExecutionResult != nil {
		t.Fatalf("immediate active disconnect changed sandbox = %+v, %v", got, err)
	}

	setSandboxFinalizerUnitState(t, fixture, "inactive")
	if err := fixture.o.queueMissingRunSessionChecks(context.Background()); err != nil {
		t.Fatal(err)
	}
	dead := waitForSandbox(t, fixture.o, context.Background(), fixture.sb.ID, func(sb *types.Sandbox) bool {
		return sb.State == types.StateDead && sb.ExecutionResult != nil
	}, "maintenance cleanup after later unit exit")
	if dead.ExecutionResult.RunID != fixture.sb.RunID || dead.ExecutionResult.Stage != types.SandboxResultRun {
		t.Fatalf("maintenance result = %+v", dead.ExecutionResult)
	}
}

func TestRunSessionMaintenanceSkipsReconnectedRunningOwner(t *testing.T) {
	fixture := newSandboxFinalizerFixture(t, "session-maint-reconnected")
	session, ok, err := fixture.o.RegisterRunSession(context.Background(), runKindSandbox, fixture.sb.RunID)
	if err != nil || !ok {
		t.Fatalf("RegisterRunSession = ok %v err %v", ok, err)
	}
	defer session.Close(true)
	setSandboxFinalizerUnitState(t, fixture, "inactive")
	if err := fixture.o.queueMissingRunSessionChecks(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-time.After(50 * time.Millisecond):
	}
	got, err := fixture.o.st.Get(context.Background(), fixture.sb.ID)
	if err != nil || got == nil {
		t.Fatalf("reconnected owner row = %+v, %v", got, err)
	}
	if got.State != types.StateRunning || got.ExecutionResult != nil || got.RunID != fixture.sb.RunID {
		t.Fatalf("maintenance touched reconnected owner: %+v", got)
	}
	if _, stops := fixture.lc.stopSnapshot(); stops != 0 {
		t.Fatalf("maintenance stopped reconnected owner %d times", stops)
	}
}

func TestRunSessionMaintenanceAcceptedResultOverridesActiveSession(t *testing.T) {
	fixture := newSandboxFinalizerFixture(t, "session-maint-result-active")
	session, ok, err := fixture.o.RegisterRunSession(context.Background(), runKindSandbox, fixture.sb.RunID)
	if err != nil || !ok {
		t.Fatalf("RegisterRunSession = ok %v err %v", ok, err)
	}
	defer session.Close(true)
	code := 0
	result := types.SandboxExecutionResult{SID: fixture.sb.ID, RunID: fixture.sb.RunID, Stage: types.SandboxResultRun, ExitCode: &code}
	if inserted, err := fixture.o.st.AcceptSandboxExecutionResult(context.Background(), fixture.sb.ID, fixture.sb.RunID, result); err != nil || !inserted {
		t.Fatalf("AcceptSandboxExecutionResult = %t, %v", inserted, err)
	}
	if err := fixture.o.queueMissingRunSessionChecks(context.Background()); err != nil {
		t.Fatal(err)
	}
	dead := waitForSandbox(t, fixture.o, context.Background(), fixture.sb.ID, func(sb *types.Sandbox) bool {
		return sb.State == types.StateDead && sb.ExecutionResult != nil
	}, "accepted result maintenance with active session")
	if dead.ExecutionResult.RunID != fixture.sb.RunID || dead.ExecutionResult.Stage != types.SandboxResultRun {
		t.Fatalf("accepted-result maintenance result = %+v", dead.ExecutionResult)
	}
}

func TestSandboxRunEndWorkersAreBoundedAndRetainPendingRetry(t *testing.T) {
	fixture := newSandboxFinalizerFixture(t, "session-bounded-first")
	fixture.o.runEndWorkerLimit = 1
	second := *fixture.sb
	second.ID = "session-bounded-second"
	second.RunID = "sr-00000000-0000-7000-8000-000000000288"
	second.RunDir = filepath.Join(filepath.Dir(fixture.sb.RunDir), second.ID)
	second.BaseDir = filepath.Join(filepath.Dir(fixture.sb.BaseDir), second.ID)
	second.VswitchPort = "port-288"
	second.FloatingIP = "192.0.2.88"
	second.ExecutionResult = nil
	materializeTestSandboxCredentials(t, &second)
	if err := fixture.o.st.Put(context.Background(), &second); err != nil {
		t.Fatal(err)
	}
	for _, dir := range []string{second.RunDir, second.BaseDir} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	code := 0
	for _, sb := range []*types.Sandbox{fixture.sb, &second} {
		result := types.SandboxExecutionResult{SID: sb.ID, RunID: sb.RunID, Stage: types.SandboxResultRun, ExitCode: &code}
		if inserted, err := fixture.o.st.AcceptSandboxExecutionResult(context.Background(), sb.ID, sb.RunID, result); err != nil || !inserted {
			t.Fatalf("AcceptSandboxExecutionResult(%s) = %t, %v", sb.ID, inserted, err)
		}
	}
	removeStarted := make(chan struct{})
	releaseRemove := make(chan struct{})
	var once sync.Once
	fixture.o.removeSandboxRunDir = func(path string) error {
		if path == fixture.sb.RunDir {
			once.Do(func() { close(removeStarted) })
			<-releaseRemove
		}
		return os.RemoveAll(path)
	}
	fixture.o.startSandboxRunEndCheck(fixture.sb.RunID)
	select {
	case <-removeStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("first cleanup did not acquire the single run-end slot")
	}
	fixture.o.startSandboxRunEndCheck(second.RunID)
	select {
	case <-time.After(50 * time.Millisecond):
	}
	gotSecond, err := fixture.o.st.Get(context.Background(), second.ID)
	if err != nil || gotSecond == nil || gotSecond.State != types.StateRunning || gotSecond.ExecutionResult == nil {
		t.Fatalf("second cleanup ran while single slot was blocked: %+v, %v", gotSecond, err)
	}
	close(releaseRemove)
	waitForSandbox(t, fixture.o, context.Background(), fixture.sb.ID, func(sb *types.Sandbox) bool {
		return sb.State == types.StateDead && sb.ExecutionResult != nil
	}, "first bounded cleanup")
	waitForSandbox(t, fixture.o, context.Background(), second.ID, func(sb *types.Sandbox) bool {
		return sb.State == types.StateDead && sb.ExecutionResult != nil
	}, "second pending bounded cleanup")
}

func TestRunSessionMaintenanceConvergesLegacyNoSessionRunningOwner(t *testing.T) {
	fixture := newSandboxFinalizerFixture(t, "session-maint-legacy")
	setSandboxFinalizerUnitState(t, fixture, "inactive")
	if err := fixture.o.queueMissingRunSessionChecks(context.Background()); err != nil {
		t.Fatal(err)
	}
	dead := waitForSandbox(t, fixture.o, context.Background(), fixture.sb.ID, func(sb *types.Sandbox) bool {
		return sb.State == types.StateDead && sb.ExecutionResult != nil
	}, "legacy no-session maintenance cleanup")
	if dead.ExecutionResult.RunID != fixture.sb.RunID || dead.ExecutionResult.Stage != types.SandboxResultRun {
		t.Fatalf("legacy maintenance result = %+v", dead.ExecutionResult)
	}
}

func TestReaperQueuesMissingRunSessionMaintenance(t *testing.T) {
	fixture := newSandboxFinalizerFixture(t, "session-maint-reaper")
	setSandboxFinalizerUnitState(t, fixture, "inactive")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go fixture.o.Reaper(ctx, 10*time.Millisecond)
	dead := waitForSandbox(t, fixture.o, context.Background(), fixture.sb.ID, func(sb *types.Sandbox) bool {
		return sb.State == types.StateDead && sb.ExecutionResult != nil
	}, "reaper missing-session maintenance cleanup")
	if dead.ExecutionResult.RunID != fixture.sb.RunID || dead.ExecutionResult.Stage != types.SandboxResultRun {
		t.Fatalf("reaper maintenance result = %+v", dead.ExecutionResult)
	}
}
