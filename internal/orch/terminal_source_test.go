package orch

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func holdTerminalSource(t *testing.T, o *Orchestrator, sb *types.Sandbox) func() {
	t.Helper()
	unlock := o.lifecycle.Lock(sb.ID)
	_, release, err := o.trySourceOperation(context.Background(), sb)
	unlock()
	if err != nil {
		t.Fatalf("hold source operation: %v", err)
	}
	var once sync.Once
	finish := func() { once.Do(release) }
	t.Cleanup(finish)
	return finish
}

func TestTerminalSourceBusyDoesNotOccupyRunEndWorker(t *testing.T) {
	f := newSandboxFinalizerFixture(t, "terminal-source-busy")
	f.o.runEndWorkerLimit = 1
	addRunEndResult(t, f, f.sb.ID, f.sb.RunID)
	second := addRunEndResult(t, f, "terminal-source-unrelated", "sr-00000000-0000-7000-8000-000000000904")
	release := holdTerminalSource(t, f.o, f.sb)
	if err := f.o.finalizeSandboxResultOnce(context.Background(), f.sb.ID, f.sb.RunID); !errors.Is(err, errSourceOperationBusy) {
		t.Fatalf("terminal cleanup with live source = %v, want source busy", err)
	}
	f.o.startSandboxRunEndCheck(f.sb.RunID)
	f.o.startSandboxRunEndCheck(second.RunID)
	waitForSandbox(t, f.o, context.Background(), second.ID, func(sb *types.Sandbox) bool { return sb.State == types.StateDead }, "source busy must yield the only worker")
	first, err := f.o.st.Get(context.Background(), f.sb.ID)
	if err != nil || first.State != types.StateRunning || first.ExecutionResult == nil || first.RunID != f.sb.RunID {
		t.Fatalf("busy terminal owner changed: %+v, %v", first, err)
	}
	for _, path := range []string{f.sb.RunDir, f.sb.BaseDir} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("busy terminal source removed %s: %v", path, err)
		}
	}
	release()
	drainRunEnds(t, f.o)
	dead, err := f.o.st.Get(context.Background(), f.sb.ID)
	if err != nil || dead == nil || dead.State != types.StateDead || dead.ExecutionResult == nil {
		t.Fatalf("released source did not reach retained dead history: %+v, %v", dead, err)
	}
}

func TestTerminalDeleteAdmissionWaitsForSource(t *testing.T) {
	f := newSandboxFinalizerFixture(t, "terminal-delete-admission")
	release := holdTerminalSource(t, f.o, f.sb)
	unlock := f.o.lifecycle.Lock(f.sb.ID)
	accepted, err := f.o.acceptSandboxDeleteLocked(context.Background(), f.sb)
	unlock()
	if accepted || !errors.Is(err, errSourceOperationBusy) {
		t.Fatalf("delete admission = %t, %v, want source busy", accepted, err)
	}
	current, err := f.o.st.Get(context.Background(), f.sb.ID)
	if err != nil || current == nil || current.State != types.StateRunning {
		t.Fatalf("busy delete changed durable owner: %+v, %v", current, err)
	}
	if f.o.lookup(f.sb.ID) == nil {
		t.Fatal("busy delete withdrew the running route before acceptance")
	}
	release()
}

func TestReconcileDefersBusySourceAndFinishesAfterRelease(t *testing.T) {
	for _, state := range []types.State{types.StateRunning, types.StateDeleting, types.StatePaused} {
		t.Run(string(state), func(t *testing.T) {
			f := newSandboxFinalizerFixture(t, "terminal-reconcile-"+string(state))
			switch state {
			case types.StateRunning:
				addRunEndResult(t, f, f.sb.ID, f.sb.RunID)
			case types.StateDeleting:
				beginDeletingForTest(t, f)
			case types.StatePaused:
				f.sb.State = state
				f.sb.ResumeSource = types.ResumeSource{Kind: types.ResumeSourceSandbox, Ref: "manifest://" + strings.Repeat("a", 64)}
				if err := f.o.st.Put(context.Background(), f.sb); err != nil {
					t.Fatal(err)
				}
			}
			// The publisher belongs to the previous conductor. A new instance has
			// no in-memory exportAttempt and must honor its inherited file lock.
			release := holdTerminalSource(t, f.o, f.sb)
			restarted := testOrchCfgAt(t, f.o.cfg, f.dbPath)
			restarted.lc, restarted.vs = f.lc, f.vs
			lifecycle, cancel := context.WithCancel(context.Background())
			restarted.SetLifecycleContext(lifecycle)
			t.Cleanup(func() {
				cancel()
				release()
				drainRunEnds(t, restarted)
				if err := restarted.DrainSandboxDeletes(context.Background()); err != nil {
					t.Error(err)
				}
			})
			if err := restarted.ReconcileSandboxes(context.Background()); err != nil {
				t.Fatalf("busy source blocked conductor startup: %v", err)
			}
			current, err := restarted.st.Get(context.Background(), f.sb.ID)
			if err != nil || current == nil || current.State != state || current.RunID != f.sb.RunID || current.VswitchPort != f.sb.VswitchPort {
				t.Fatalf("busy reconcile changed ownership: %+v, %v", current, err)
			}
			if _, stops := f.lc.stopSnapshot(); stops != 0 {
				t.Fatalf("busy reconcile stopped source runner: %d", stops)
			}
			for _, path := range []string{f.sb.RunDir, f.sb.BaseDir} {
				if _, err := os.Stat(path); err != nil {
					t.Fatalf("busy reconcile removed %s: %v", path, err)
				}
			}
			release()
			if state == types.StateDeleting {
				if err := restarted.DrainSandboxDeletes(context.Background()); err != nil {
					t.Fatal(err)
				}
			} else {
				drainRunEnds(t, restarted)
			}
			current, err = restarted.st.Get(context.Background(), f.sb.ID)
			if err != nil {
				t.Fatal(err)
			}
			switch state {
			case types.StateDeleting:
				if current != nil {
					t.Fatalf("delete retry retained row: %+v", current)
				}
			case types.StateRunning:
				if current == nil || current.State != types.StateDead || current.ExecutionResult == nil {
					t.Fatalf("result retry lost dead history: %+v", current)
				}
			case types.StatePaused:
				if current == nil || current.State != types.StatePaused || pausedCleanupPending(current) || current.ResumeSource != f.sb.ResumeSource {
					t.Fatalf("paused retry did not preserve source and clear old runtime: %+v", current)
				}
				if _, err := os.Stat(f.sb.BaseDir); err != nil {
					t.Fatalf("paused retry removed retained source: %v", err)
				}
			}
		})
	}
}

func TestReconcileMissingRunningSourceBusyRetainsResultlessDeadSemantics(t *testing.T) {
	for _, noRunID := range []bool{false, true} {
		name := "exact-run"
		if noRunID {
			name = "no-run-id"
		}
		t.Run(name, func(t *testing.T) {
			f := newSandboxFinalizerFixture(t, "terminal-missing-"+name)
			f.lc.state = "inactive"
			if noRunID {
				f.sb.RunID, f.lc.unit = "", ""
				if err := f.o.st.Put(context.Background(), f.sb); err != nil {
					t.Fatal(err)
				}
			}
			release := holdTerminalSource(t, f.o, f.sb)
			restarted := testOrchCfgAt(t, f.o.cfg, f.dbPath)
			restarted.lc, restarted.vs = f.lc, f.vs
			lifecycle, cancel := context.WithCancel(context.Background())
			restarted.SetLifecycleContext(lifecycle)
			t.Cleanup(func() { cancel(); release(); drainRunEnds(t, restarted) })
			if err := restarted.ReconcileSandboxes(context.Background()); err != nil {
				t.Fatalf("busy missing-run source blocked startup: %v", err)
			}
			pending, err := restarted.st.Get(context.Background(), f.sb.ID)
			if err != nil || pending == nil || pending.State != types.StateRunning || pending.ExecutionResult != nil {
				t.Fatalf("busy missing-run reconcile invented terminal history: %+v, %v", pending, err)
			}
			if _, stops := f.lc.stopSnapshot(); stops != 0 {
				t.Fatalf("busy missing-run reconcile stopped runner: %d", stops)
			}
			release()
			drainRunEnds(t, restarted)
			dead, err := restarted.st.Get(context.Background(), f.sb.ID)
			if err != nil || dead == nil || dead.State != types.StateDead || dead.ExecutionResult != nil || dead.RunID != "" || dead.RunDir != "" || dead.BaseDir != "" {
				t.Fatalf("missing-run retry changed dead/result semantics: %+v, %v", dead, err)
			}
			for _, path := range []string{f.sb.RunDir, f.sb.BaseDir} {
				if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("missing-run retry retained ownership %s: %v", path, err)
				}
			}
		})
	}
}

func TestMissingRunningRetryCannotCleanSameIDSuccessor(t *testing.T) {
	f := newSandboxFinalizerFixture(t, "terminal-missing-successor")
	observed := cloneSandbox(f.sb)
	successor := cloneSandbox(f.sb)
	successor.RunID = "sr-00000000-0000-7000-8000-000000000905"
	successor.CreatedUnix++
	if err := f.o.st.Delete(context.Background(), f.sb.ID); err != nil {
		t.Fatal(err)
	}
	if err := f.o.st.InsertSandbox(context.Background(), successor); err != nil {
		t.Fatal(err)
	}
	if err := f.o.finalizeMissingRunningSandboxOnce(context.Background(), observed); err != nil {
		t.Fatal(err)
	}
	current, err := f.o.st.Get(context.Background(), f.sb.ID)
	if err != nil || current == nil || current.State != types.StateRunning || current.RunID != successor.RunID || current.ExecutionResult != nil {
		t.Fatalf("stale lost-unit callback changed successor: %+v, %v", current, err)
	}
	if _, stops := f.lc.stopSnapshot(); stops != 0 {
		t.Fatalf("stale lost-unit callback stopped successor runner: %d", stops)
	}
	for _, path := range []string{successor.RunDir, successor.BaseDir} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("stale lost-unit callback removed successor ownership: %v", err)
		}
	}
	// Empty RunID is supported for legacy recovered rows, but does not make
	// every later incarnation with the same SID eligible for an old retry.
	observed.RunID = ""
	successor.RunID = ""
	if err := f.o.st.Put(context.Background(), successor); err != nil {
		t.Fatal(err)
	}
	if err := f.o.finalizeMissingRunningSandboxOnce(context.Background(), observed); err != nil {
		t.Fatal(err)
	}
	current, err = f.o.st.Get(context.Background(), f.sb.ID)
	if err != nil || current == nil || current.State != types.StateRunning || current.CreatedUnix != successor.CreatedUnix {
		t.Fatalf("empty-RunID retry changed successor: %+v, %v", current, err)
	}
}

func TestMissingRunningRetryConsumesNewObservationForEmptyRunID(t *testing.T) {
	f := newSandboxFinalizerFixture(t, "terminal-missing-handoff")
	f.sb.RunID = ""
	if err := f.o.st.Put(context.Background(), f.sb); err != nil {
		t.Fatal(err)
	}
	release := holdTerminalSource(t, f.o, f.sb)
	f.o.startMissingRunningSandboxCleanup(f.sb)
	deadline := time.Now().Add(3 * time.Second)
	for {
		f.o.runEndMu.Lock()
		work := f.o.runEndActive[resultCleanupKey(f.sb.ID, "")]
		retrying := work != nil && work.delay != 0
		f.o.runEndMu.Unlock()
		if retrying {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("missing-run cleanup did not enter source-busy retry")
		}
		time.Sleep(time.Millisecond)
	}
	successor := cloneSandbox(f.sb)
	successor.CreatedUnix++
	unlock := f.o.lifecycle.Lock(f.sb.ID)
	if err := f.o.st.Delete(context.Background(), f.sb.ID); err != nil {
		unlock()
		t.Fatal(err)
	}
	if err := f.o.st.InsertSandbox(context.Background(), successor); err != nil {
		unlock()
		t.Fatal(err)
	}
	f.o.startMissingRunningSandboxCleanup(successor)
	unlock()
	release()
	drainRunEnds(t, f.o)
	got, err := f.o.st.Get(context.Background(), successor.ID)
	if err != nil || got == nil || got.CreatedUnix != successor.CreatedUnix || got.State != types.StateDead || got.ExecutionResult != nil {
		t.Fatalf("coalesced retry lost the newly observed missing owner: %+v, %v", got, err)
	}
}

func TestAcceptedResultDuringCaptureWinsPauseCommit(t *testing.T) {
	installCheckpointSandboxCtl(t)
	f := newSandboxFinalizerFixture(t, "terminal-capture-result")
	f.o.cfg.Checkpoint.Mode = config.CheckpointLocal
	started, release := filepath.Join(t.TempDir(), "started"), filepath.Join(t.TempDir(), "release")
	t.Setenv("CHECKPOINT_STARTED_FILE", started)
	t.Setenv("CHECKPOINT_RELEASE_FILE", release)
	t.Cleanup(func() { _ = os.WriteFile(release, nil, 0o600) })
	paused := make(chan error, 1)
	go func() {
		_, err := f.o.Pause(context.Background(), f.sb.ID, f.apiKey, sandboxcfg.CaptureRequest{Kind: types.CaptureSandbox})
		paused <- err
	}()
	waitForCheckpointFile(t, started)
	code := 17
	result := types.SandboxExecutionResult{SID: f.sb.ID, RunID: f.sb.RunID, Stage: types.SandboxResultRun, ExitCode: &code}
	// Result receipt cannot wait for the capture's SID lock: the result must be
	// durable before acknowledging it to the runner, which may then disappear.
	if err := f.o.PostSandboxResult(context.Background(), f.sb.RunID, f.sb.ID, result); err != nil {
		t.Fatal(err)
	}
	current, err := f.o.st.Get(context.Background(), f.sb.ID)
	if err != nil || current.State != types.StatePausing || current.ExecutionResult == nil {
		t.Fatalf("capture hid accepted result: %+v, %v", current, err)
	}
	if err := os.WriteFile(release, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := waitCheckpointResult(t, paused); err == nil {
		t.Fatal("late capture replaced the accepted terminal result with paused state")
	}
	drainRunEnds(t, f.o)
	dead, err := f.o.st.Get(context.Background(), f.sb.ID)
	if err != nil || dead == nil || dead.State != types.StateDead || dead.ExecutionResult == nil || dead.ExecutionResult.RunID != f.sb.RunID || dead.ExecutionResult.ExitCode == nil || *dead.ExecutionResult.ExitCode != code {
		t.Fatalf("accepted result did not win terminal cleanup: %+v, %v", dead, err)
	}
	for _, path := range []string{f.sb.RunDir, f.sb.BaseDir} {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("terminal result retained capture ownership %s: %v", path, err)
		}
	}
}
