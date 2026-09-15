package orch

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/launcher"
)

type runPoolTestLauncher struct {
	started chan string
	stopped chan string
	reset   chan string
	startFn func(context.Context, string) error
	stopFn  func(context.Context, string) error
}

func newRunPoolTestLauncher() *runPoolTestLauncher {
	l := &runPoolTestLauncher{
		started: make(chan string, 512),
		stopped: make(chan string, 512),
		reset:   make(chan string, 512),
	}
	l.startFn = func(ctx context.Context, unit string) error {
		select {
		case l.started <- unit:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	l.stopFn = func(ctx context.Context, unit string) error {
		select {
		case l.stopped <- unit:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return l
}

func (l *runPoolTestLauncher) Start(ctx context.Context, unit string) error {
	return l.startFn(ctx, unit)
}

func (l *runPoolTestLauncher) Stop(ctx context.Context, unit string) error {
	return l.stopFn(ctx, unit)
}

func (l *runPoolTestLauncher) ResetFailed(_ context.Context, unit string) error {
	l.reset <- unit
	return nil
}
func (l *runPoolTestLauncher) List(context.Context, string) ([]launcher.Unit, error) {
	return nil, nil
}
func (l *runPoolTestLauncher) Reload(context.Context) error { return nil }
func (l *runPoolTestLauncher) Close() error                 { return nil }

func testRunUnit(runID string) string { return "sandbox-runner@" + runID + ".service" }

func runIDFromTestUnit(unit string) string {
	return strings.TrimSuffix(strings.TrimPrefix(unit, "sandbox-runner@"), ".service")
}

func startRunPoolTest(t *testing.T, size int) (*runPool, *runPoolTestLauncher, context.Context, context.CancelFunc) {
	t.Helper()
	lc := newRunPoolTestLauncher()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	p := newRunPool(runKindSandbox, size, time.Second, t.TempDir(), lc, testRunUnit, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := p.Start(ctx); err != nil {
		t.Fatal(err)
	}
	return p, lc, ctx, cancel
}

func addIdleRunForTest(t *testing.T, p *runPool, lc *runPoolTestLauncher, ctx context.Context) (string, *runWaitReq) {
	t.Helper()
	var runID string
	select {
	case unit := <-lc.started:
		runID = runIDFromTestUnit(unit)
	case <-time.After(time.Second):
		t.Fatal("runner unit was not started")
	}
	req := &runWaitReq{runID: runID, ctx: ctx, resp: make(chan runWaitResp, 1)}
	select {
	case p.waitCh <- req:
	case <-time.After(time.Second):
		t.Fatal("WaitAssignment request was not accepted")
	}
	return runID, req
}

func TestRunPoolAssignCanceledBeforeQueue(t *testing.T) {
	p, _, _, _ := startRunPoolTest(t, 0)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	committed := false
	if runID, err := p.Assign(ctx, "never-queued", func(string) error {
		committed = true
		return nil
	}); !errors.Is(err, context.Canceled) || runID != "" {
		t.Fatalf("Assign canceled before queue = %q, %v", runID, err)
	}
	if committed {
		t.Fatal("pre-queue cancellation invoked commit")
	}
}

func TestRunPoolAssignCancelWhilePendingIsDecidedByLoop(t *testing.T) {
	p, lc, baseCtx, _ := startRunPoolTest(t, 0)
	ctx, cancel := context.WithCancel(baseCtx)
	done := make(chan error, 1)
	committed := false
	go func() {
		_, err := p.Assign(ctx, "pending-cancel", func(string) error {
			committed = true
			return nil
		})
		done <- err
	}()
	select {
	case <-lc.started: // demand start is queued only after the request is pending
	case <-time.After(time.Second):
		t.Fatal("pending assignment was not accepted")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("pending cancellation error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("pending cancellation was not replied to")
	}
	if committed {
		t.Fatal("pending cancellation invoked commit")
	}
}

func TestRunPoolCancelBeforeCommitNeverHandsTaskToRunner(t *testing.T) {
	p, lc, baseCtx, _ := startRunPoolTest(t, 0)
	assignCtx, cancelAssign := context.WithCancel(baseCtx)
	assignDone := make(chan error, 1)
	committed := false
	go func() {
		_, err := p.Assign(assignCtx, "canceled-task", func(string) error {
			committed = true
			return nil
		})
		assignDone <- err
	}()
	var runID string
	select {
	case unit := <-lc.started:
		runID = runIDFromTestUnit(unit)
	case <-time.After(time.Second):
		t.Fatal("on-demand runner was not started")
	}
	cancelAssign()
	if err := <-assignDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("Assign cancellation = %v", err)
	}

	waitCtx, cancelWait := context.WithTimeout(baseCtx, time.Second)
	defer cancelWait()
	taskID, ok, err := p.WaitAssignment(waitCtx, runID)
	if err == nil || ok || taskID != "" {
		t.Fatalf("runner received canceled task = %q, %v, %v", taskID, ok, err)
	}
	if committed {
		t.Fatal("commit ran after assignment had been canceled")
	}
}

func TestRunPoolCommitSuccessWinsConcurrentCancel(t *testing.T) {
	p, lc, baseCtx, _ := startRunPoolTest(t, 1)
	runID, waiter := addIdleRunForTest(t, p, lc, baseCtx)
	assignCtx, cancelAssign := context.WithCancel(baseCtx)
	commitStarted := make(chan struct{})
	commitGate := make(chan struct{})
	type assignResult struct {
		runID string
		err   error
	}
	done := make(chan assignResult, 1)
	go func() {
		got, err := p.Assign(assignCtx, "linearized-task", func(gotRunID string) error {
			if gotRunID != runID {
				t.Errorf("commit run id = %q, want %q", gotRunID, runID)
			}
			close(commitStarted)
			<-commitGate
			return nil
		})
		done <- assignResult{runID: got, err: err}
	}()
	select {
	case <-commitStarted:
	case <-time.After(time.Second):
		t.Fatal("commit did not start")
	}
	cancelAssign()
	close(commitGate)

	select {
	case result := <-done:
		if result.err != nil || result.runID != runID {
			t.Fatalf("Assign after committed cancellation race = %q, %v", result.runID, result.err)
		}
	case <-time.After(time.Second):
		t.Fatal("Assign did not return its committed result")
	}
	select {
	case result := <-waiter.resp:
		if result.err != nil || !result.ok || result.taskID != "linearized-task" {
			t.Fatalf("runner task after commit = %+v", result)
		}
	case <-time.After(time.Second):
		t.Fatal("runner did not receive committed task")
	}
}

func TestRunPoolCommitErrorDoesNotHandTaskAndRunnerCanBeReused(t *testing.T) {
	p, lc, ctx, _ := startRunPoolTest(t, 1)
	runID, waiter := addIdleRunForTest(t, p, lc, ctx)
	wantErr := errors.New("commit failed")
	if got, err := p.Assign(ctx, "rejected-task", func(string) error { return wantErr }); !errors.Is(err, wantErr) || got != "" {
		t.Fatalf("failed commit Assign = %q, %v", got, err)
	}
	select {
	case result := <-waiter.resp:
		t.Fatalf("runner received task for failed commit: %+v", result)
	default:
	}
	got, err := p.Assign(ctx, "accepted-task", func(gotRunID string) error {
		if gotRunID != runID {
			t.Fatalf("reused run id = %q, want %q", gotRunID, runID)
		}
		return nil
	})
	if err != nil || got != runID {
		t.Fatalf("Assign after commit error = %q, %v", got, err)
	}
	result := <-waiter.resp
	if result.err != nil || !result.ok || result.taskID != "accepted-task" {
		t.Fatalf("runner reuse task = %+v", result)
	}
}

func TestRunPoolShutdownRepliesPendingAndIdleRequests(t *testing.T) {
	t.Run("pending assignment", func(t *testing.T) {
		p, lc, ctx, stop := startRunPoolTest(t, 0)
		done := make(chan error, 1)
		go func() {
			_, err := p.Assign(ctx, "pending-at-stop", func(string) error { return nil })
			done <- err
		}()
		select {
		case <-lc.started:
		case <-time.After(time.Second):
			t.Fatal("assignment did not become pending")
		}
		stop()
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("pending shutdown error = %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("pending assignment was stuck at shutdown")
		}
	})

	t.Run("idle runner", func(t *testing.T) {
		p, lc, ctx, stop := startRunPoolTest(t, 1)
		_, waiter := addIdleRunForTest(t, p, lc, ctx)
		stop()
		select {
		case result := <-waiter.resp:
			if !errors.Is(result.err, context.Canceled) || result.ok {
				t.Fatalf("idle shutdown result = %+v", result)
			}
		case <-time.After(time.Second):
			t.Fatal("idle WaitAssignment was stuck at shutdown")
		}
	})
}

func TestRunPoolDemandAssignment(t *testing.T) {
	lc := newRunPoolTestLauncher()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	p := newRunPool(runKindSandbox, 0, time.Second, t.TempDir(), lc, testRunUnit, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := p.Start(ctx); err != nil {
		t.Fatal(err)
	}

	assignDone := make(chan string, 1)
	go func() {
		runID, err := p.Assign(ctx, "sbx-1", func(string) error { return nil })
		if err != nil {
			t.Errorf("Assign: %v", err)
			return
		}
		assignDone <- runID
	}()

	var runID string
	select {
	case unit := <-lc.started:
		runID = runIDFromTestUnit(unit)
	case <-time.After(time.Second):
		t.Fatal("Start was not called")
	}
	taskID, ok, err := p.WaitAssignment(ctx, runID)
	if err != nil || !ok || taskID != "sbx-1" {
		t.Fatalf("WaitAssignment = taskID %q ok %v err %v; want sbx-1 true nil", taskID, ok, err)
	}
	select {
	case got := <-assignDone:
		if got != runID {
			t.Fatalf("Assign runID = %q, want %q", got, runID)
		}
	case <-time.After(time.Second):
		t.Fatal("Assign did not complete")
	}
}

func TestRunPoolCanceledDemandDoesNotKeepIdleWhenSizeZero(t *testing.T) {
	lc := newRunPoolTestLauncher()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	p := newRunPool(runKindSandbox, 0, time.Second, t.TempDir(), lc, testRunUnit, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := p.Start(ctx); err != nil {
		t.Fatal(err)
	}

	assignCtx, cancelAssign := context.WithCancel(ctx)
	assignDone := make(chan error, 1)
	go func() {
		_, err := p.Assign(assignCtx, "sbx-canceled", func(string) error { return nil })
		assignDone <- err
	}()

	var unit, runID string
	select {
	case unit = <-lc.started:
		runID = runIDFromTestUnit(unit)
	case <-time.After(time.Second):
		t.Fatal("Start was not called")
	}
	cancelAssign()
	select {
	case err := <-assignDone:
		if err == nil {
			t.Fatal("Assign succeeded after cancellation")
		}
	case <-time.After(time.Second):
		t.Fatal("Assign did not observe cancellation")
	}

	waitCtx, cancelWait := context.WithTimeout(ctx, time.Second)
	defer cancelWait()
	_, ok, err := p.WaitAssignment(waitCtx, runID)
	if err == nil || ok {
		t.Fatalf("WaitAssignment after canceled demand = ok %v err %v; want false non-nil", ok, err)
	}
	select {
	case got := <-lc.stopped:
		if got != unit {
			t.Fatalf("stopped unit = %q, want %q", got, unit)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled demand idle run was not stopped")
	}
}

func TestRunPoolLargePrefillDoesNotDeadlockControlLoop(t *testing.T) {
	lc := newRunPoolTestLauncher()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	const size = 160
	p := newRunPool(runKindSandbox, size, 5*time.Second, t.TempDir(), lc, testRunUnit, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := p.Start(ctx); err != nil {
		t.Fatal(err)
	}

	seen := make(map[string]struct{}, size)
	deadline := time.After(3 * time.Second)
	for len(seen) < size {
		select {
		case unit := <-lc.started:
			seen[unit] = struct{}{}
		case <-deadline:
			t.Fatalf("started %d/%d units; control loop stalled", len(seen), size)
		}
	}
}

func TestRunPoolStartFailureIsCleanedAndRefilled(t *testing.T) {
	lc := newRunPoolTestLauncher()
	attempt := 0
	lc.startFn = func(ctx context.Context, unit string) error {
		select {
		case lc.started <- unit:
		case <-ctx.Done():
			return ctx.Err()
		}
		attempt++
		if attempt == 1 {
			return errors.New("start failed")
		}
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	p := newRunPool(runKindSandbox, 1, time.Second, t.TempDir(), lc, testRunUnit, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := p.Start(ctx); err != nil {
		t.Fatal(err)
	}

	first := <-lc.started
	select {
	case got := <-lc.stopped:
		if got != first {
			t.Fatalf("stopped unit = %q, want failed unit %q", got, first)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("failed unit was not stopped")
	}
	select {
	case got := <-lc.reset:
		if got != first {
			t.Fatalf("reset unit = %q, want failed unit %q", got, first)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("failed unit was not reset")
	}
	select {
	case second := <-lc.started:
		if second == first {
			t.Fatal("replacement reused the failed run id")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("failed pool slot was not replenished")
	}
}

func TestRunPoolDemandStartFailureReleasesAssignment(t *testing.T) {
	lc := newRunPoolTestLauncher()
	startErr := errors.New("injected demand start failure")
	lc.startFn = func(context.Context, string) error { return startErr }
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	p := newRunPool(runKindBuild, 0, time.Second, t.TempDir(), lc,
		func(runID string) string { return "sandbox-builder@" + runID + ".service" },
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := p.Start(ctx); err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		_, err := p.Assign(ctx, "build-with-failed-unit", func(string) error {
			t.Error("commit ran for a unit that failed to start")
			return nil
		})
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, startErr) {
			t.Fatalf("Assign error = %v, want start failure", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("failed demand-created unit left assignment pending")
	}
}

func TestRunPoolDoesNotBindUnrelatedStartFailureToPendingTask(t *testing.T) {
	lc := newRunPoolTestLauncher()
	firstGate := make(chan struct{})
	startErr := errors.New("injected baseline start failure")
	starts := 0
	lc.startFn = func(ctx context.Context, unit string) error {
		select {
		case lc.started <- unit:
		case <-ctx.Done():
			return ctx.Err()
		}
		starts++
		if starts == 1 {
			select {
			case <-firstGate:
				return startErr
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	p := newRunPool(runKindBuild, 1, time.Second, t.TempDir(), lc,
		func(runID string) string { return "sandbox-builder@" + runID + ".service" },
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := p.Start(ctx); err != nil {
		t.Fatal(err)
	}

	firstUnit := <-lc.started
	req := &runConsumeReq{
		taskID: "build-survives-baseline-failure", ctx: ctx,
		commit: func(string) error { return nil }, resp: make(chan runConsumeResp, 1),
	}
	enqueued := make(chan struct{})
	go func() {
		p.consumeCh <- req
		close(enqueued)
	}()
	<-enqueued
	// A second loop request is a barrier: it cannot be answered until the
	// pending task has caused ensure() to register another in-flight start.
	if _, _, err := p.WaitAssignment(ctx, "not-a-real-run"); err == nil {
		t.Fatal("barrier WaitAssignment unexpectedly succeeded")
	}
	close(firstGate)

	var secondUnit string
	select {
	case secondUnit = <-lc.started:
	case <-time.After(2 * time.Second):
		t.Fatal("replacement/demand unit was not started")
	}
	select {
	case stopped := <-lc.stopped:
		if stopped != firstUnit {
			t.Fatalf("stopped unit = %q, want failed baseline %q", stopped, firstUnit)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("failed baseline unit was not cleaned")
	}
	select {
	case result := <-req.resp:
		t.Fatalf("unrelated baseline failure completed pending task: %+v", result)
	default:
	}

	secondRunID := strings.TrimSuffix(strings.TrimPrefix(secondUnit, "sandbox-builder@"), ".service")
	taskID, ok, err := p.WaitAssignment(ctx, secondRunID)
	if err != nil || !ok || taskID != req.taskID {
		t.Fatalf("replacement assignment = task %q ok %t err %v", taskID, ok, err)
	}
	select {
	case result := <-req.resp:
		if result.err != nil || result.runID != secondRunID {
			t.Fatalf("pending task result = %+v", result)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("pending task did not use surviving capacity")
	}
}

func TestRunPoolBoundsAllFailedPrestartsForPendingTask(t *testing.T) {
	lc := newRunPoolTestLauncher()
	firstGate := make(chan struct{})
	startErr := errors.New("injected persistent start failure")
	starts := 0
	lc.startFn = func(ctx context.Context, unit string) error {
		select {
		case lc.started <- unit:
		case <-ctx.Done():
			return ctx.Err()
		}
		starts++
		if starts == 1 {
			select {
			case <-firstGate:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return startErr
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	p := newRunPool(runKindBuild, 1, time.Second, t.TempDir(), lc,
		func(runID string) string { return "sandbox-builder@" + runID + ".service" },
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := p.Start(ctx); err != nil {
		t.Fatal(err)
	}

	select {
	case <-lc.started: // baseline prestart is now blocked inside launcher.Start
	case <-time.After(time.Second):
		t.Fatal("baseline prestart did not begin")
	}
	done := make(chan error, 1)
	go func() {
		_, err := p.Assign(ctx, "build-with-no-startable-unit", func(string) error {
			t.Error("commit ran although every finite start attempt failed")
			return nil
		})
		done <- err
	}()
	// This loop request is a barrier proving the pending assignment was recorded
	// and its demand-created start attempt was added before the first failure.
	if _, _, err := p.WaitAssignment(ctx, "not-a-real-run"); err == nil {
		t.Fatal("barrier WaitAssignment unexpectedly succeeded")
	}
	close(firstGate)

	select {
	case err := <-done:
		if !errors.Is(err, startErr) {
			t.Fatalf("Assign error = %v, want persistent start failure", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("replacement failures extended the pending assignment forever")
	}
}

func TestRunPoolWaitTimeoutStopsAndRefills(t *testing.T) {
	lc := newRunPoolTestLauncher()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	p := newRunPool(runKindSandbox, 1, 25*time.Millisecond, t.TempDir(), lc, testRunUnit, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := p.Start(ctx); err != nil {
		t.Fatal(err)
	}

	first := <-lc.started
	select {
	case got := <-lc.stopped:
		if got != first {
			t.Fatalf("stopped unit = %q, want timed-out unit %q", got, first)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("unit that never called WaitAssignment was not stopped")
	}
	select {
	case second := <-lc.started:
		if second == first {
			t.Fatal("replacement reused the timed-out run id")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed-out pool slot was not replenished")
	}
}

func TestRunPoolStartCallUsesWaitTimeout(t *testing.T) {
	lc := newRunPoolTestLauncher()
	attempt := 0
	lc.startFn = func(ctx context.Context, unit string) error {
		lc.started <- unit
		attempt++
		if attempt == 1 {
			<-ctx.Done()
			return ctx.Err()
		}
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	p := newRunPool(runKindSandbox, 1, 25*time.Millisecond, t.TempDir(), lc, testRunUnit, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := p.Start(ctx); err != nil {
		t.Fatal(err)
	}

	first := <-lc.started
	select {
	case got := <-lc.stopped:
		if got != first {
			t.Fatalf("stopped unit = %q, want start-timeout unit %q", got, first)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("blocked Start call was not timed out and cleaned")
	}
	select {
	case second := <-lc.started:
		if second == first {
			t.Fatal("replacement reused the start-timeout run id")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("start-timeout pool slot was not replenished")
	}
}

func TestRunPoolStartReportsPidDirectoryFailure(t *testing.T) {
	root := t.TempDir()
	blocked := filepath.Join(root, "not-a-directory")
	if err := os.WriteFile(blocked, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	p := newRunPool(runKindSandbox, 1, time.Second, blocked, newRunPoolTestLauncher(), testRunUnit, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := p.Start(context.Background()); err == nil {
		t.Fatal("Start succeeded with an unusable run root")
	}
}
