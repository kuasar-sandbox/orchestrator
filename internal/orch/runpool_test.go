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
