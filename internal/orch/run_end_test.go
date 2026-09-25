package orch

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/launcher"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

type runEndBlockingLauncher struct {
	unit    string
	started chan struct{}
	release chan struct{}
}

func (*runEndBlockingLauncher) Start(context.Context, string) error       { return nil }
func (*runEndBlockingLauncher) Stop(context.Context, string) error        { return nil }
func (*runEndBlockingLauncher) ResetFailed(context.Context, string) error { return nil }
func (*runEndBlockingLauncher) Reload(context.Context) error              { return nil }
func (*runEndBlockingLauncher) Close() error                              { return nil }

func (l *runEndBlockingLauncher) List(context.Context, string) ([]launcher.Unit, error) {
	select {
	case <-l.started:
	default:
		close(l.started)
	}
	<-l.release
	return []launcher.Unit{{Name: l.unit, ActiveState: "inactive"}}, nil
}

func TestRunEndStaleStartingDisconnectPreservesSuccessor(t *testing.T) {
	f := newSandboxFinalizerFixture(t, "supervisor-stale-starting-successor")
	f.sb.State = types.StateStarting
	f.sb.LaunchMode = types.LaunchImage
	if err := f.o.st.Put(context.Background(), f.sb); err != nil {
		t.Fatal(err)
	}

	oldRunID := f.sb.RunID
	unit := "sandbox-runner@" + oldRunID + ".service"
	lc := &runEndBlockingLauncher{
		unit: unit, started: make(chan struct{}), release: make(chan struct{}),
	}
	f.o.lc = lc
	f.o.runs.restore(oldRunID, unit)

	oldAttempt, err := f.o.launches.Claim(context.Background(), f.sb.ID, launchCreate)
	if err != nil {
		t.Fatal(err)
	}
	oldAttempt.SetRunID(oldRunID)

	errCh := make(chan error, 1)
	go func() {
		errCh <- f.o.checkDisconnectedRunOnce(context.Background(), runKindSandbox, oldRunID)
	}()

	select {
	case <-lc.started:
	case <-time.After(2 * time.Second):
		t.Fatal("old-run liveness check did not reach blocking boundary")
	}

	// The old attempt can finish while its disconnect checker is blocked in
	// external unit-liveness I/O. A same-SID successor may then legitimately
	// become the new process-local and durable owner.
	f.o.launches.Finish(oldAttempt, nil)
	successorRunID := "sr-00000000-0000-7000-8000-000000000999"
	successor := *f.sb
	successor.RunID = successorRunID
	successor.State = types.StateStarting
	successor.ExecutionResult = nil
	if err := f.o.st.Put(context.Background(), &successor); err != nil {
		t.Fatal(err)
	}
	nextAttempt, err := f.o.launches.Claim(context.Background(), successor.ID, launchCreate)
	if err != nil {
		t.Fatal(err)
	}
	nextAttempt.SetRunID(successorRunID)
	defer f.o.launches.Finish(nextAttempt, nil)

	close(lc.release)
	if err := <-errCh; err != nil {
		t.Fatalf("old-run disconnect check: %v", err)
	}

	select {
	case <-nextAttempt.Context().Done():
		t.Fatal("stale old-RunID disconnect canceled the same-SID successor launch")
	case <-time.After(50 * time.Millisecond):
	}
}

func addRunEndResult(t *testing.T, f sandboxFinalizerFixture, id, runID string) *types.Sandbox {
	t.Helper()
	sb := *f.sb
	sb.ID, sb.RunID = id, runID
	sb.RunDir = filepath.Join(filepath.Dir(f.sb.RunDir), id)
	sb.BaseDir = filepath.Join(filepath.Dir(f.sb.BaseDir), id)
	sb.ExecutionResult = nil
	materializeTestSandboxCredentials(t, &sb)
	if err := f.o.st.Put(context.Background(), &sb); err != nil {
		t.Fatal(err)
	}
	for _, dir := range []string{sb.RunDir, sb.BaseDir} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	code := 0
	result := types.SandboxExecutionResult{SID: sb.ID, RunID: sb.RunID, Stage: types.SandboxResultRun, ExitCode: &code}
	if inserted, err := f.o.st.AcceptSandboxExecutionResult(context.Background(), sb.ID, sb.RunID, result); err != nil || !inserted {
		t.Fatalf("accept result: inserted=%t err=%v", inserted, err)
	}
	return &sb
}

func waitRunEndSignal(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatal(what)
	}
}

func drainRunEnds(t *testing.T, o *Orchestrator) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := o.acceptedOps.Drain(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestRunEndBurstHasBoundedWorkersAndCoalescesPending(t *testing.T) {
	f := newSandboxFinalizerFixture(t, "runend-burst")
	f.o.runEndWorkerLimit = 1
	addRunEndResult(t, f, f.sb.ID, f.sb.RunID)
	started, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	f.o.removeSandboxRunDir = func(path string) error {
		close(started)
		<-release
		return os.RemoveAll(path)
	}
	baseline := runtime.NumGoroutine()
	f.o.startSandboxRunEndCheck(f.sb.RunID)
	waitRunEndSignal(t, started, "first worker did not enter cleanup")
	const count = 1024
	for i := 0; i < count; i++ {
		id := fmt.Sprintf("sr-00000000-0000-7000-8000-%012d", 2000+i)
		for repeat := 0; repeat < 3; repeat++ {
			f.o.startSandboxRunEndCheck(id)
		}
	}
	f.o.runEndMu.Lock()
	workers, pending, queued := f.o.runEndWorkers, len(f.o.runEndActive), len(f.o.runEndQueue)
	f.o.runEndMu.Unlock()
	if workers != 1 || pending != count+1 || queued != count {
		t.Errorf("workers/pending/queue=%d/%d/%d", workers, pending, queued)
	}
	if growth := runtime.NumGoroutine() - baseline; growth > 24 {
		t.Errorf("%d extra goroutines for %d pending runs", growth, count)
	}
	releaseOnce.Do(func() { close(release) })
	drainRunEnds(t, f.o)
	f.o.runEndMu.Lock()
	defer f.o.runEndMu.Unlock()
	if f.o.runEndWorkers != 0 || len(f.o.runEndActive) != 0 || len(f.o.runEndQueue) != 0 {
		t.Fatal("workers or pending ownership survived drain")
	}
}

func TestRunEndBlockedWorkerDoesNotBlockAnotherRun(t *testing.T) {
	f := newSandboxFinalizerFixture(t, "runend-blocked")
	f.o.runEndWorkerLimit = 2
	addRunEndResult(t, f, f.sb.ID, f.sb.RunID)
	second := addRunEndResult(t, f, "runend-progress", "sr-00000000-0000-7000-8000-000000000901")
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	f.o.removeSandboxRunDir = func(path string) error {
		if path == f.sb.RunDir {
			close(started)
			<-release
		}
		return os.RemoveAll(path)
	}
	f.o.startSandboxRunEndCheck(f.sb.RunID)
	waitRunEndSignal(t, started, "first worker did not block")
	f.o.startSandboxRunEndCheck(second.RunID)
	waitForSandbox(t, f.o, context.Background(), second.ID, func(sb *types.Sandbox) bool { return sb.State == types.StateDead }, "second run progresses while first is blocked")
	first, err := f.o.st.Get(context.Background(), f.sb.ID)
	if err != nil || first.State != types.StateRunning {
		t.Fatalf("first cleanup unexpectedly finished: %+v %v", first, err)
	}
	once.Do(func() { close(release) })
	drainRunEnds(t, f.o)
}

func TestRunEndRetryBackoffDoesNotOccupyOnlyWorker(t *testing.T) {
	f := newSandboxFinalizerFixture(t, "runend-retry")
	f.o.runEndWorkerLimit = 1
	addRunEndResult(t, f, f.sb.ID, f.sb.RunID)
	second := addRunEndResult(t, f, "runend-retry-progress", "sr-00000000-0000-7000-8000-000000000902")
	failed := make(chan struct{})
	var once sync.Once
	var allow atomic.Bool
	defer allow.Store(true)
	f.o.removeSandboxRunDir = func(path string) error {
		if path == f.sb.RunDir && !allow.Load() {
			once.Do(func() { close(failed) })
			return errors.New("retryable removal failure")
		}
		return os.RemoveAll(path)
	}
	f.o.startSandboxRunEndCheck(f.sb.RunID)
	waitRunEndSignal(t, failed, "first removal did not fail")
	f.o.startSandboxRunEndCheck(second.RunID)
	waitForSandbox(t, f.o, context.Background(), second.ID, func(sb *types.Sandbox) bool { return sb.State == types.StateDead }, "retry backoff must yield to second run")
	first, err := f.o.st.Get(context.Background(), f.sb.ID)
	if err != nil || first.State != types.StateRunning || first.ExecutionResult == nil {
		t.Fatalf("retry lost durable owner: %+v %v", first, err)
	}
	allow.Store(true)
	drainRunEnds(t, f.o)
}

func TestRunEndShutdownDrainsRealCleanupAndRetainsQueuedOwner(t *testing.T) {
	f := newSandboxFinalizerFixture(t, "runend-shutdown")
	f.o.runEndWorkerLimit = 1
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.o.lifecycleCtx = ctx
	addRunEndResult(t, f, f.sb.ID, f.sb.RunID)
	second := addRunEndResult(t, f, "runend-after-restart", "sr-00000000-0000-7000-8000-000000000903")
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	f.o.removeSandboxRunDir = func(path string) error {
		if path == f.sb.RunDir {
			close(started)
			<-release
		}
		return os.RemoveAll(path)
	}
	f.o.startSandboxRunEndCheck(f.sb.RunID)
	waitRunEndSignal(t, started, "cleanup did not start")
	f.o.startSandboxRunEndCheck(second.RunID)
	cancel()
	drained := make(chan error, 1)
	go func() { drained <- f.o.acceptedOps.Drain(context.Background()) }()
	select {
	case err := <-drained:
		t.Fatalf("drain abandoned synchronous cleanup: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	once.Do(func() { close(release) })
	select {
	case err := <-drained:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("drain did not finish")
	}
	pending, err := f.o.st.Get(context.Background(), second.ID)
	if err != nil || pending.State != types.StateRunning || pending.ExecutionResult == nil {
		t.Fatalf("shutdown lost durable queued result: %+v %v", pending, err)
	}
	f.o.runEndMu.Lock()
	defer f.o.runEndMu.Unlock()
	if f.o.runEndWorkers != 0 || len(f.o.runEndActive) != 0 {
		t.Fatal("canceled dispatcher retained workers")
	}
}

func TestRunEndStartingReconnectDuringUnitCheckPreservesAttempt(t *testing.T) {
	f := newSandboxFinalizerFixture(t, "runend-starting-reconnect")
	f.sb.State, f.sb.LaunchMode = types.StateStarting, types.LaunchImage
	if err := f.o.st.Put(context.Background(), f.sb); err != nil {
		t.Fatal(err)
	}
	attempt, err := f.o.launches.Claim(context.Background(), f.sb.ID, launchCreate)
	if err != nil {
		t.Fatal(err)
	}
	attempt.SetRunID(f.sb.RunID)
	defer f.o.launches.Finish(attempt, nil)
	lc := &runEndBlockingLauncher{unit: testRunUnit(f.sb.RunID), started: make(chan struct{}), release: make(chan struct{})}
	f.o.lc = lc
	f.o.runs.restore(f.sb.RunID, lc.unit)
	done := make(chan error, 1)
	go func() { done <- f.o.checkDisconnectedRunOnce(context.Background(), runKindSandbox, f.sb.RunID) }()
	waitRunEndSignal(t, lc.started, "unit check did not start")
	session, ok, err := f.o.RegisterRunSession(context.Background(), runKindSandbox, f.sb.RunID)
	if err != nil || !ok {
		close(lc.release)
		t.Fatalf("reconnect: %t %v", ok, err)
	}
	defer session.Close(true)
	close(lc.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if attempt.Context().Err() != nil {
		t.Fatal("old unit check canceled reconnected starting run")
	}
}

type runEndStopGate struct {
	*sandboxFinalizerLauncher
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (g *runEndStopGate) Stop(ctx context.Context, unit string) error {
	g.once.Do(func() { close(g.started) })
	select {
	case <-g.release:
	case <-ctx.Done():
		return ctx.Err()
	}
	return g.sandboxFinalizerLauncher.Stop(ctx, unit)
}

func TestRunEndResultACKDoesNotWaitForLifecycleOrStop(t *testing.T) {
	f := newSandboxFinalizerFixture(t, "runend-result-ack")
	gate := &runEndStopGate{sandboxFinalizerLauncher: f.lc, started: make(chan struct{}), release: make(chan struct{})}
	f.o.lc = gate
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(gate.release) })
	pidfile, ok := f.o.RunPidFile(runKindSandbox, f.sb.RunID)
	if !ok {
		t.Fatal("invalid fixture RunID")
	}
	if err := os.MkdirAll(filepath.Dir(pidfile), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(pidfile, []byte(strconv.Itoa(os.Getpid())), 0o600); err != nil {
		t.Fatal(err)
	}
	dir, err := os.MkdirTemp("", "runend-ack-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	socket := filepath.Join(dir, "ctl.sock")
	serverCtx, stopServer := context.WithCancel(context.Background())
	defer stopServer()
	ready, serverDone := make(chan struct{}), make(chan error, 1)
	go func() {
		serverDone <- configsock.New(socket, configsock.Deps{Provider: f.o}, f.o.log).ServeReady(serverCtx, ready)
	}()
	waitRunEndSignal(t, ready, "config socket did not start")
	code := 7
	result := types.SandboxExecutionResult{SID: f.sb.ID, RunID: f.sb.RunID, Stage: types.SandboxResultRun, ExitCode: &code}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	unlock := f.o.lifecycle.Lock(f.sb.ID)
	err = configsock.PostSandboxResultContext(ctx, socket, f.sb.RunID, f.sb.ID, result)
	unlock()
	if err != nil {
		t.Fatalf("result ACK waited for SID lock: %v", err)
	}
	waitRunEndSignal(t, gate.started, "accepted result did not attempt exact Stop")
	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()
	if err := configsock.PostSandboxResultContext(ctx2, socket, f.sb.RunID, f.sb.ID, result); err != nil {
		t.Fatalf("idempotent ACK waited for Stop: %v", err)
	}
	current, err := f.o.st.Get(ctx2, f.sb.ID)
	if err != nil || current.ExecutionResult == nil || current.ExecutionResult.RunID != f.sb.RunID {
		t.Fatalf("ACK lacked durable exact result: %+v %v", current, err)
	}
	releaseOnce.Do(func() { close(gate.release) })
	drainRunEnds(t, f.o)
	stopServer()
	select {
	case err := <-serverDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("config socket shutdown did not finish")
	}
}

func TestRunEndAcceptedResultDelegatesPausedAndDeletingOwners(t *testing.T) {
	for _, state := range []types.State{types.StatePaused, types.StateDeleting} {
		t.Run(string(state), func(t *testing.T) {
			f := newSandboxFinalizerFixture(t, "runend-delegate-"+string(state))
			addRunEndResult(t, f, f.sb.ID, f.sb.RunID)
			current, err := f.o.st.Get(context.Background(), f.sb.ID)
			if err != nil {
				t.Fatal(err)
			}
			current.State = state
			checkpoint := filepath.Join(current.BaseDir, "checkpoint", "keep")
			if state == types.StatePaused {
				current.ResumeSource = types.ResumeSource{Kind: types.ResumeSourceSandbox, Ref: "manifest://" + strings.Repeat("a", 64)}
				if err := os.MkdirAll(filepath.Dir(checkpoint), 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(checkpoint, []byte("retained checkpoint"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			if err := f.o.st.Put(context.Background(), current); err != nil {
				t.Fatal(err)
			}
			f.o.startSandboxRunEndCheck(current.RunID)
			if state == types.StatePaused {
				got := waitForSandbox(t, f.o, context.Background(), current.ID, func(sb *types.Sandbox) bool { return sb.State == types.StatePaused && !pausedCleanupPending(sb) }, "accepted result continues paused cleanup")
				if got.ResumeSource != current.ResumeSource {
					t.Fatal("result cleanup replaced checkpoint source")
				}
				if _, err := os.Stat(checkpoint); err != nil {
					t.Fatal(err)
				}
			} else {
				waitForSandboxAbsent(t, f.o, context.Background(), current.ID, "accepted result continues delete finalizer")
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				if err := f.o.DrainSandboxDeletes(ctx); err != nil {
					t.Fatal(err)
				}
			}
			drainRunEnds(t, f.o)
		})
	}
}
