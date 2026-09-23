package orch

import (
	"context"
	"testing"
	"time"
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
