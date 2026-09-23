package orch

import (
	"context"
	"errors"
	"os"
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
	lc := newRunPoolTestLauncher()
	o.lc = lc
	runID := "sr-00000000-0000-7000-8000-000000000428"
	pool := &runPool{unitName: testRunUnit}
	o.runs.register(runID, pool)
	session, ok, err := o.RegisterRunSession(context.Background(), runKindSandbox, runID)
	if err != nil || !ok {
		t.Fatalf("RegisterRunSession = ok %v err %v", ok, err)
	}
	session.Close(false)
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

func TestRunSessionDisconnectStartingWakesLaunchRollbackOwner(t *testing.T) {
	fixture := newSandboxFinalizerFixture(t, "session-starting")
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
	case <-time.After(2 * time.Second):
		t.Fatal("starting disconnect did not cancel launch attempt")
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
