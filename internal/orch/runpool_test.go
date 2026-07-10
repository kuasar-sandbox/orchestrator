package orch

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/launcher"
)

type runPoolTestLauncher struct {
	started chan string
	stopped chan string
	startFn func(context.Context, string) error
	stopFn  func(context.Context, string) error
}

func newRunPoolTestLauncher() *runPoolTestLauncher {
	l := &runPoolTestLauncher{started: make(chan string, 8), stopped: make(chan string, 8)}
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

func (l *runPoolTestLauncher) ResetFailed(context.Context, string) error { return nil }
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
	p.Start(ctx)

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
	p.Start(ctx)

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
