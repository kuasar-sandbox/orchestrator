package orch

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func assertTerminatedDataRetained(t *testing.T, sb *types.Sandbox) {
	t.Helper()
	if _, err := os.Stat(sb.RunDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("terminated RunDir remains: %v", err)
	}
	if _, err := os.Stat(sb.BaseDir); err != nil {
		t.Fatalf("terminated current data was removed: %v", err)
	}
}

func resetSource(sb *types.Sandbox) types.ResumeSource {
	return types.ResumeSource{Kind: types.ResumeSourceSandbox, Ref: "file://" + filepath.Join(sb.BaseDir, "checkpoint", "reset.sandbox")}
}

func resetLifecycle(t *testing.T, o *Orchestrator) context.CancelFunc {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	o.SetLifecycleContext(ctx)
	t.Cleanup(func() {
		cancel()
		deadline, stop := context.WithTimeout(context.Background(), 3*time.Second)
		defer stop()
		if err := o.acceptedOps.Drain(deadline); err != nil {
			t.Error(err)
		}
		if err := o.DrainSandboxDeletes(deadline); err != nil {
			t.Error(err)
		}
	})
	return cancel
}

func waitResetWork(t *testing.T, w *sandboxResetWork) error {
	t.Helper()
	select {
	case <-w.done:
		return w.err
	case <-time.After(3 * time.Second):
		t.Fatal("Reset worker did not finish")
		return nil
	}
}

func TestResetStateConflictsAndEOnlyIdempotence(t *testing.T) {
	for _, state := range []types.State{types.StateStarting, types.StateRunning, types.StatePausing, types.StateDeleting, types.StatePaused} {
		t.Run(string(state), func(t *testing.T) {
			f := newSandboxFinalizerFixture(t, "reset-state-"+string(state))
			resetLifecycle(t, f.o)
			f.sb.State = state
			if state == types.StateStarting {
				f.sb.LaunchMode = types.LaunchImage
			}
			if state == types.StatePaused {
				f.sb.ResumeSource = types.ResumeSource{Kind: types.ResumeSourceSnapshot, Ref: "file://old.snapshot", SandboxRef: "file://old.sandbox"}
			}
			if err := f.o.st.Put(context.Background(), f.sb); err != nil {
				t.Fatal(err)
			}
			if err := f.o.Reset(context.Background(), f.sb.ID, f.apiKey); !errors.Is(err, api.ErrSandboxChanged) {
				t.Fatalf("Reset %s = %v", state, err)
			}
			if err := f.o.Reset(context.Background(), f.sb.ID, "wrong-owner"); !errors.Is(err, api.ErrNotFound) {
				t.Fatalf("wrong owner=%v", err)
			}
			if state == types.StatePaused {
				f.sb.ResumeSource = resetSource(f.sb)
				if err := f.o.st.Put(context.Background(), f.sb); err != nil {
					t.Fatal(err)
				}
				before, _ := f.o.st.Get(context.Background(), f.sb.ID)
				if err := f.o.Reset(context.Background(), f.sb.ID, f.apiKey); err != nil {
					t.Fatal(err)
				}
				after, _ := f.o.st.Get(context.Background(), f.sb.ID)
				if !reflect.DeepEqual(before, after) {
					t.Fatal("E-only idempotence changed source/state")
				}
			}
		})
	}
}

func TestResetUnknownOutcomeContinuesSameOperationAndRecoversCommit(t *testing.T) {
	for _, committed := range []bool{false, true} {
		t.Run(map[bool]string{false: "preparation-interrupted", true: "paused-commit-response-lost"}[committed], func(t *testing.T) {
			f := newSandboxFinalizerFixture(t, "reset-unknown")
			resetLifecycle(t, f.o)
			if changed, err := f.o.st.CommitSandboxDead(context.Background(), f.sb); err != nil || !changed {
				t.Fatalf("dead=%v %v", changed, err)
			}
			var calls int
			var operation string
			f.o.prepareReset = func(ctx context.Context, sb *types.Sandbox) (types.ResumeSource, error) {
				calls++
				if operation != "" && operation != sb.ResetID {
					t.Error("unknown outcome created a new operation")
				}
				operation = sb.ResetID
				if calls == 1 {
					if committed {
						if changed, err := f.o.st.CommitSandboxReset(ctx, sb, resetSource(sb)); err != nil || !changed {
							t.Errorf("injected commit=%v %v", changed, err)
						}
					}
					return types.ResumeSource{}, context.Canceled
				}
				return resetSource(sb), nil
			}
			if err := f.o.Reset(context.Background(), f.sb.ID, f.apiKey); err != nil {
				t.Fatal(err)
			}
			wantCalls := 2
			if committed {
				wantCalls = 1
			}
			if calls != wantCalls {
				t.Fatalf("preparations=%d want %d", calls, wantCalls)
			}
			got, _ := f.o.st.Get(context.Background(), f.sb.ID)
			if got.State != types.StatePaused || got.ResetID != operation || got.ResetState != "succeeded" {
				t.Fatalf("recovered=%+v", got)
			}
		})
	}
}

func TestResetCLIInterruptionContinuesSameAutomaticOperation(t *testing.T) {
	f := newSandboxFinalizerFixture(t, "reset-cli-interrupted", true)
	resetLifecycle(t, f.o)
	if changed, err := f.o.st.CommitSandboxDead(context.Background(), f.sb); err != nil || !changed {
		t.Fatalf("dead=%v %v", changed, err)
	}
	var calls int
	var operation string
	f.o.prepareReset = func(ctx context.Context, sb *types.Sandbox) (types.ResumeSource, error) {
		calls++
		if operation != "" && operation != sb.ResetID {
			t.Error("CLI interruption changed the operation identity")
		}
		operation = sb.ResetID
		if calls == 1 {
			// A real process exit crosses the boundary as ExitError, losing the
			// context.Canceled type. sandboxer's CLI signal test owns exit 75.
			return types.ResumeSource{}, exec.CommandContext(ctx, "/bin/sh", "-c", "exit 75").Run()
		}
		if sb.AutoResetError != "" || sb.ResetState != "pending" {
			t.Error("interruption was persisted as definitive failure")
		}
		return resetSource(sb), nil
	}
	dead, _ := f.o.st.Get(context.Background(), f.sb.ID)
	if err := waitResetWork(t, f.o.startSandboxReset(dead, false)); err != nil {
		t.Fatal(err)
	}
	got, _ := f.o.st.Get(context.Background(), f.sb.ID)
	if calls != 2 || got.State != types.StatePaused || got.ResetID != operation || got.AutoResetError != "" {
		t.Fatalf("interrupted CLI recovery: calls=%d row=%+v", calls, got)
	}
}

func TestResetManualJoinsAutomaticAndRetainsParentData(t *testing.T) {
	f := newSandboxFinalizerFixture(t, "reset-join", true)
	resetLifecycle(t, f.o)
	started, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	f.o.prepareReset = func(ctx context.Context, sb *types.Sandbox) (types.ResumeSource, error) {
		calls.Add(1)
		close(started)
		select {
		case <-release:
			return resetSource(sb), nil
		case <-ctx.Done():
			return types.ResumeSource{}, ctx.Err()
		}
	}
	f.lc.state = "inactive"
	if err := f.o.finalizeMissingRunningSandboxOnce(context.Background(), f.sb); err != nil {
		t.Fatal(err)
	}
	<-started
	dead, err := f.o.st.Get(context.Background(), f.sb.ID)
	if err != nil || dead.State != types.StateDead || dead.DeadRunID != f.sb.RunID || dead.BaseDir != f.sb.BaseDir {
		t.Fatalf("terminal owner: %+v %v", dead, err)
	}
	if _, err := os.Stat(filepath.Join(f.sb.BaseDir, "owned")); err != nil {
		t.Fatal("current data was deleted", err)
	}
	if _, err := os.Stat(f.sb.RunDir); !os.IsNotExist(err) {
		t.Fatal("run directory survived terminal cleanup", err)
	}
	manual := make(chan error, 1)
	go func() { manual <- f.o.Reset(context.Background(), f.sb.ID, f.apiKey) }()
	// Observe admission through the same lifecycle fence; the SDK stays blocked.
	deadline := time.After(3 * time.Second)
	for {
		f.o.resetMu.Lock()
		w := f.o.resetActive[dead.InstanceID+"/"+dead.ResetID]
		joined := w != nil && w.manual.Load()
		f.o.resetMu.Unlock()
		if joined {
			break
		}
		select {
		case <-deadline:
			t.Fatal("manual request could not join active auto Reset")
		default:
		}
		time.Sleep(time.Millisecond)
	}
	close(release)
	if err := <-manual; err != nil {
		t.Fatal(err)
	}
	paused, _ := f.o.st.Get(context.Background(), f.sb.ID)
	if paused.State != types.StatePaused || paused.ResumeSource != resetSource(dead) || paused.DeadUnix != 0 || paused.ExecutionResult != nil || calls.Load() != 1 {
		t.Fatalf("Reset result: %+v calls=%d", paused, calls.Load())
	}
	if err := f.o.Reset(context.Background(), f.sb.ID, f.apiKey); err != nil || calls.Load() != 1 {
		t.Fatal("E-only idempotence", err)
	}
	if err := f.o.finalizeMissingRunningSandboxOnce(context.Background(), f.sb); err != nil {
		t.Fatal(err)
	}
	after, _ := f.o.st.Get(context.Background(), f.sb.ID)
	if !reflect.DeepEqual(paused, after) {
		t.Fatal("late old-run cleanup changed Reset result")
	}
}

func TestResetConfirmedFailureSurvivesRestartAndManualRetry(t *testing.T) {
	f := newSandboxFinalizerFixture(t, "reset-confirmed-failure", true)
	stop := resetLifecycle(t, f.o)
	var calls atomic.Int32
	f.o.prepareReset = func(context.Context, *types.Sandbox) (types.ResumeSource, error) {
		calls.Add(1)
		return types.ResumeSource{}, errors.New("current disk is not ready")
	}
	ctx := context.Background()
	code := 11
	result := types.SandboxExecutionResult{SID: f.sb.ID, RunID: f.sb.RunID, Stage: types.SandboxResultStart, ExitCode: &code}
	if _, err := f.o.st.AcceptSandboxExecutionResult(ctx, f.sb.ID, f.sb.RunID, result); err != nil {
		t.Fatal(err)
	}
	f.lc.state = "inactive"
	if err := f.o.finalizeSandboxResultOnce(ctx, f.sb.ID, f.sb.RunID); err != nil {
		t.Fatal(err)
	}
	failed := waitForSandbox(t, f.o, ctx, f.sb.ID, func(sb *types.Sandbox) bool { return sb.ResetState == "failed" }, "confirmed Reset failure")
	if calls.Load() != 1 || failed.AutoResetError == "" || failed.ExecutionResult == nil || *failed.ExecutionResult.ExitCode != 11 {
		t.Fatalf("failed automatic Reset: %+v calls=%d", failed, calls.Load())
	}
	stop()
	if err := f.o.acceptedOps.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	restarted := testOrchCfgAt(t, f.o.cfg, f.dbPath)
	restarted.lc, restarted.vs = f.lc, f.vs
	resetLifecycle(t, restarted)
	restarted.prepareReset = func(_ context.Context, sb *types.Sandbox) (types.ResumeSource, error) {
		calls.Add(1)
		return resetSource(sb), nil
	}
	if err := restarted.ReconcileSandboxes(ctx); err != nil {
		t.Fatal(err)
	}
	if err := restarted.reapTerminalHistory(ctx, time.Unix(failed.DeadUnix, 0)); err != nil {
		t.Fatal(err)
	}
	got, _ := restarted.st.Get(ctx, f.sb.ID)
	if !reflect.DeepEqual(failed, got) || calls.Load() != 1 {
		t.Fatal("restart retried confirmed failure or refreshed TTL")
	}
	if err := restarted.Reset(ctx, f.sb.ID, f.apiKey); err != nil {
		t.Fatal(err)
	}
	got, _ = restarted.st.Get(ctx, f.sb.ID)
	if got.State != types.StatePaused || got.AutoResetError != "" || got.ExecutionResult != nil || calls.Load() != 2 {
		t.Fatalf("manual retry: %+v calls=%d", got, calls.Load())
	}
}

func TestResetStartupAdoptsLiveAndDiscoversDurablePending(t *testing.T) {
	for _, state := range []string{"active", "inactive", "dead-committed", "query-error"} {
		t.Run(state, func(t *testing.T) {
			f := newSandboxFinalizerFixture(t, "reset-startup-"+state, true)
			resetLifecycle(t, f.o)
			var calls atomic.Int32
			f.o.prepareReset = func(_ context.Context, sb *types.Sandbox) (types.ResumeSource, error) {
				calls.Add(1)
				return resetSource(sb), nil
			}
			if state == "dead-committed" {
				f.lc.state = "inactive"
				// Simulate exiting after durable dead, before any callback/admission.
				if err := f.o.cleanupSandboxTerminalOwnership(context.Background(), f.sb); err != nil {
					t.Fatal(err)
				}
				if ok, err := f.o.st.CommitSandboxDead(context.Background(), f.sb); err != nil || !ok {
					t.Fatal(ok, err)
				}
			} else if state == "query-error" {
				f.lc.listErr = errors.New("systemd query failed")
			} else {
				f.lc.state = state
			}
			err := f.o.ReconcileSandboxes(context.Background())
			if state == "query-error" {
				if err == nil {
					t.Fatal("systemd error treated as absent")
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if state == "inactive" || state == "dead-committed" {
				got := waitForSandbox(t, f.o, context.Background(), f.sb.ID, func(sb *types.Sandbox) bool { return sb.State == types.StatePaused }, "reconciled Reset")
				if calls.Load() != 1 || got.RunID != "" {
					t.Fatal("startup lost Reset responsibility")
				}
			} else {
				got, _ := f.o.st.Get(context.Background(), f.sb.ID)
				if got.State != types.StateRunning || got.RunID != f.sb.RunID || got.DeadUnix != 0 || calls.Load() != 0 {
					t.Fatalf("running adoption changed: %+v", got)
				}
			}
		})
	}
}

func TestResetManualJoinBeforeWorkerLockPreventsAutomaticExpiry(t *testing.T) {
	f := newSandboxFinalizerFixture(t, "reset-join-expired", true)
	resetLifecycle(t, f.o)
	ctx := context.Background()
	if ok, err := f.o.st.CommitSandboxDead(ctx, f.sb); err != nil || !ok {
		t.Fatal(ok, err)
	}
	dead, _ := f.o.st.Get(ctx, f.sb.ID)
	f.o.resetNow = func() time.Time { return time.Unix(dead.DeadUnix, 0).Add(f.o.cfg.Sandbox.DeadTTLDur()) }
	var calls atomic.Int32
	f.o.prepareReset = func(_ context.Context, sb *types.Sandbox) (types.ResumeSource, error) {
		calls.Add(1)
		return resetSource(sb), nil
	}
	unlock := f.o.lifecycle.Lock(dead.ID)
	locked := true
	defer func() {
		if locked {
			unlock()
		}
	}()
	automatic := f.o.startSandboxReset(dead, false)
	// Wait for the automatic worker to contend on the exact lifecycle lock.
	// The manual join then happens after any pre-lock snapshot would be stale.
	deadline := time.Now().Add(3 * time.Second)
	for {
		f.o.lifecycle.mu.Lock()
		waiting := f.o.lifecycle.locks[dead.ID].refs == 2
		f.o.lifecycle.mu.Unlock()
		if waiting {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("automatic worker never reached lifecycle fence")
		}
		runtime.Gosched()
	}
	if ok, err := f.o.st.BeginSandboxReset(ctx, dead); err != nil || !ok {
		t.Fatal(ok, err)
	}
	manual := f.o.startSandboxReset(dead, true)
	if manual != automatic {
		t.Fatal("manual request created a second operation")
	}
	unlock()
	locked = false
	if err := waitResetWork(t, manual); err != nil {
		t.Fatal(err)
	}
	current, _ := f.o.st.Get(ctx, dead.ID)
	if current == nil || current.State != types.StatePaused || current.ResetID != dead.ResetID || calls.Load() != 1 {
		t.Fatalf("manual join was lost at expiry: %+v calls=%d", current, calls.Load())
	}
}

func TestResetExpiredAutomaticUsesRetryingDeleteFinalizer(t *testing.T) {
	f := newSandboxFinalizerFixture(t, "reset-expired", true)
	resetLifecycle(t, f.o)
	ctx := context.Background()
	if err := f.o.cleanupSandboxTerminalOwnership(ctx, f.sb); err != nil {
		t.Fatal(err)
	}
	if ok, err := f.o.st.CommitSandboxDead(ctx, f.sb); err != nil || !ok {
		t.Fatal(ok, err)
	}
	dead, _ := f.o.st.Get(ctx, f.sb.ID)
	f.o.resetNow = func() time.Time { return time.Unix(dead.DeadUnix, 0).Add(f.o.cfg.Sandbox.DeadTTLDur()) }
	var calls atomic.Int32
	f.o.prepareReset = func(context.Context, *types.Sandbox) (types.ResumeSource, error) {
		calls.Add(1)
		return types.ResumeSource{}, errors.New("must not prepare expired data")
	}
	blocked := make(chan struct{})
	allowRemoval := make(chan struct{})
	var removal atomic.Int32
	f.o.removeSandboxBaseDir = func(path string) error {
		if removal.Add(1) == 1 {
			close(blocked)
			return errors.New("injected disk cleanup failure")
		}
		<-allowRemoval
		return os.RemoveAll(path)
	}
	if err := waitResetWork(t, f.o.startSandboxReset(dead, false)); !errors.Is(err, api.ErrSandboxChanged) {
		t.Fatal(err)
	}
	<-blocked
	current, _ := f.o.st.Get(ctx, f.sb.ID)
	if current == nil || current.State != types.StateDeleting || calls.Load() != 0 {
		t.Fatalf("expired Reset lost deleting owner: %+v calls=%d", current, calls.Load())
	}
	if err := f.o.Reset(ctx, f.sb.ID, f.apiKey); !errors.Is(err, api.ErrSandboxChanged) {
		t.Fatal("Reset admitted deleting", err)
	}
	close(allowRemoval)
	if err := f.o.DrainSandboxDeletes(ctx); err != nil {
		t.Fatal(err)
	}
	current, _ = f.o.st.Get(ctx, f.sb.ID)
	if current != nil {
		t.Fatal("delete retry did not retire parent")
	}
	if _, err := os.Stat(f.sb.BaseDir); !os.IsNotExist(err) {
		t.Fatal("delete did not reclaim data", err)
	}
}

func TestResetAndTTLWaitForCheckpointExportOwnership(t *testing.T) {
	f := newSandboxFinalizerFixture(t, "reset-export-owner", true)
	resetLifecycle(t, f.o)
	ctx := context.Background()
	if ok, err := f.o.st.CommitSandboxDead(ctx, f.sb); err != nil || !ok {
		t.Fatal(ok, err)
	}
	dead, _ := f.o.st.Get(ctx, f.sb.ID)
	_, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	export, ok := f.o.exports.Begin(dead.ID, resetSource(dead), true, "checkpoint-A", cancel)
	if !ok {
		t.Fatal("export admission")
	}
	defer f.o.exports.Finish(export)
	var calls int
	f.o.prepareReset = func(context.Context, *types.Sandbox) (types.ResumeSource, error) {
		calls++
		return resetSource(dead), nil
	}
	if retry, err := f.o.completeSandboxResetOnce(ctx, dead, func() bool { return true }); !retry || !errors.Is(err, errSourceOperationBusy) {
		t.Fatalf("Reset crossed export fence: %v %v", retry, err)
	}
	unlock := f.o.lifecycle.Lock(dead.ID)
	changed, err := f.o.acceptSandboxDeleteLocked(ctx, dead)
	unlock()
	if changed || !errors.Is(err, errSourceOperationBusy) {
		t.Fatalf("Delete crossed export fence: %v %v", changed, err)
	}
	if err := f.o.reapDeadSandboxes(ctx, dead.DeadUnix); !errors.Is(err, errSourceOperationBusy) {
		t.Fatalf("TTL crossed export fence: %v", err)
	}
	current, _ := f.o.st.Get(ctx, dead.ID)
	if current.State != types.StateDead || current.DeadToken != dead.DeadToken || current.DeadUnix != dead.DeadUnix || calls != 0 {
		t.Fatal("source contention changed the terminal instance")
	}
	f.o.exports.Finish(export)
	// A manual request admitted before TTL's delete CAS may still win. The old
	// candidate must then be harmless even though its wall-clock TTL has expired.
	if retry, err := f.o.completeSandboxResetOnce(ctx, dead, func() bool { return true }); retry || err != nil {
		t.Fatal(retry, err)
	}
	if changed, err := f.o.st.DeleteDeadSandboxForRetention(ctx, dead, dead.DeadUnix); err != nil || changed {
		t.Fatal("old TTL removed reset parent", changed, err)
	}
	current, _ = f.o.st.Get(ctx, dead.ID)
	if current.State != types.StatePaused || calls != 1 {
		t.Fatal("Reset did not finish after export ownership released")
	}
}
