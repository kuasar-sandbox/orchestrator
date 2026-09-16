package orch

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/config"
)

func startPoolSet(t *testing.T, kind string, configs []config.RunPoolConfig, lc *runPoolTestLauncher, runs *runIndex) (*runPools, context.Context, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	pools := newRunPools(kind, configs, 3*time.Second, t.TempDir(), lc, runs, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := pools.Start(ctx); err != nil {
		cancel()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cancel()
		for _, p := range pools.pools {
			select {
			case <-p.done:
			case <-time.After(time.Second):
				t.Error("pool did not stop")
			}
		}
	})
	return pools, ctx, cancel
}

func poolPosition(runs *runIndex, pools *runPools, id string) int {
	runs.mu.Lock()
	defer runs.mu.Unlock()
	for i, p := range pools.pools {
		if runs.waiting[id] == p {
			return i
		}
	}
	return -1
}

func callbackLauncher(t *testing.T, runs *runIndex) *runPoolTestLauncher {
	t.Helper()
	lc := newRunPoolTestLauncher()
	lc.startFn = func(ctx context.Context, unit string) error {
		id := strings.TrimSuffix(strings.SplitN(unit, "@", 2)[1], ".service")
		if got := runs.unit(id); got != unit {
			return fmt.Errorf("Start observed unit %q, want %q", got, unit)
		}
		// Enter the callback before Start has returned: registration must precede it.
		_, _, err := runs.wait(ctx, id)
		return err
	}
	return lc
}

func TestRunPoolsIndependentRoundRobinAndFastCallback(t *testing.T) {
	runs := &runIndex{}
	lc := callbackLauncher(t, runs)
	entries := []config.RunPoolConfig{{Unit: "shared@.service", Size: 1}, {Unit: "shared@.service", Size: 3}, {Unit: "shared@.service", Size: 1}, {Unit: "other@.service", Size: 0}}
	runners, ctx, _ := startPoolSet(t, runKindSandbox, entries, lc, runs)
	builders, _, _ := startPoolSet(t, runKindBuild, []config.RunPoolConfig{{Unit: "build@.service"}, {Unit: "build@.service"}}, lc, runs)
	if len(runners.pools) != 4 || runners.pools[0] == runners.pools[2] {
		t.Fatal("duplicate pools collapsed")
	}
	for n := 0; n < 16; n++ {
		id, err := runners.Assign(ctx, fmt.Sprint(n), func(id string) error {
			if got := poolPosition(runs, runners, id); got != n%4 {
				return fmt.Errorf("runner position %d want %d", got, n%4)
			}
			if !validRunID(runKindSandbox, id) {
				return fmt.Errorf("changed RunID: %s", id)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if runs.unit(id) == "" {
			t.Fatal("assignment discarded execution unit")
		}
		if _, ok, err := runs.wait(ctx, id); err == nil || ok {
			t.Fatal("completed runner was offered again")
		}
		runs.forget(id)
		if n%3 == 0 {
			want := (n / 3) % 2
			id, err := builders.Assign(ctx, "build", func(id string) error {
				if got := poolPosition(runs, builders, id); got != want {
					return fmt.Errorf("builder position %d want %d", got, want)
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			runs.forget(id)
		}
	}
}

func TestRunPoolsConcurrentAssignments(t *testing.T) {
	runs := &runIndex{}
	lc := callbackLauncher(t, runs)
	pools, ctx, _ := startPoolSet(t, runKindSandbox, []config.RunPoolConfig{{Unit: "r@.service"}, {Unit: "r@.service"}, {Unit: "s@.service"}}, lc, runs)
	counts := make([]atomic.Int32, 3)
	var ids sync.Map
	var wg sync.WaitGroup
	for n := 0; n < 90; n++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			id, err := pools.Assign(ctx, fmt.Sprint(n), func(id string) error {
				pos := poolPosition(runs, pools, id)
				if pos < 0 {
					return errors.New("missing creating pool")
				}
				counts[pos].Add(1)
				if _, dup := ids.LoadOrStore(id, true); dup {
					return errors.New("RunID assigned twice")
				}
				return nil
			})
			if err != nil {
				t.Error(err)
				return
			}
			runs.forget(id)
		}(n)
	}
	wg.Wait()
	for i := range counts {
		if counts[i].Load() != 30 {
			t.Errorf("pool %d count %d", i, counts[i].Load())
		}
	}
	runs.mu.Lock()
	defer runs.mu.Unlock()
	if len(runs.waiting) != 0 || len(runs.units) != 0 {
		t.Fatalf("retained indexes %d/%d", len(runs.waiting), len(runs.units))
	}
}

func TestRunPoolsDoNotHoldSelectionLockOrFallback(t *testing.T) {
	runs := &runIndex{}
	lc := callbackLauncher(t, runs)
	fast := lc.startFn
	entered, release := make(chan struct{}), make(chan struct{})
	failure := errors.New("blocked pool failed")
	lc.startFn = func(ctx context.Context, unit string) error {
		if strings.HasPrefix(unit, "slow@") {
			close(entered)
			select {
			case <-release:
				return failure
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return fast(ctx, unit)
	}
	pools, ctx, _ := startPoolSet(t, runKindSandbox, []config.RunPoolConfig{{Unit: "slow@.service"}, {Unit: "fast@.service"}}, lc, runs)
	done := make(chan error, 1)
	go func() {
		_, err := pools.Assign(ctx, "slow", func(string) error { t.Error("failed request crossed pools"); return nil })
		done <- err
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	id, err := pools.Assign(ctx, "fast", func(id string) error {
		if poolPosition(runs, pools, id) != 1 {
			return errors.New("wrong fast pool")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	runs.forget(id)
	close(release)
	select {
	case err := <-done:
		if !errors.Is(err, failure) {
			t.Fatalf("failure %v", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func TestRunPoolsPrewarmDuplicatesAndShutdownReclaim(t *testing.T) {
	runs := &runIndex{}
	lc := newRunPoolTestLauncher()
	pools, ctx, cancel := startPoolSet(t, runKindSandbox, []config.RunPoolConfig{{Unit: "shared@.service", Size: 1}, {Unit: "shared@.service", Size: 2}, {Unit: "shared@.service", Size: 1}, {Unit: "zero@.service", Size: 0}}, lc, runs)
	counts := make([]int, 4)
	for n := 0; n < 4; n++ {
		select {
		case unit := <-lc.started:
			id := strings.TrimSuffix(strings.SplitN(unit, "@", 2)[1], ".service")
			pos := poolPosition(runs, pools, id)
			if pos < 0 {
				t.Fatal("unregistered start")
			}
			counts[pos]++
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	if fmt.Sprint(counts) != "[1 2 1 0]" {
		t.Fatalf("independent prewarm counts %v", counts)
	}
	// Exercise shutdown before the harness cleanup and assert routing reclamation.
	cancel()
	for _, p := range pools.pools {
		select {
		case <-p.done:
		case <-time.After(time.Second):
			t.Fatal("shutdown blocked")
		}
	}
	runs.mu.Lock()
	defer runs.mu.Unlock()
	if len(runs.waiting) != 0 || len(runs.units) != 0 {
		t.Fatal("unassigned index retained")
	}
}

func TestRunPoolsCancellationLateCallbackAndCommitFailure(t *testing.T) {
	for _, kind := range []string{runKindSandbox, runKindBuild} {
		t.Run(kind, func(t *testing.T) {
			runs := &runIndex{}
			lc := newRunPoolTestLauncher()
			pools, ctx, _ := startPoolSet(t, kind, []config.RunPoolConfig{{Unit: "same@.service"}, {Unit: "same@.service"}}, lc, runs)
			request, cancel := context.WithCancel(ctx)
			done := make(chan error, 1)
			go func() {
				_, err := pools.Assign(request, "cancelled", func(string) error { t.Error("cancelled task committed"); return nil })
				done <- err
			}()
			var id string
			select {
			case unit := <-lc.started:
				id = unitToRunIDFromTemplate("same@.service", unit)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			cancel()
			if err := <-done; !errors.Is(err, context.Canceled) {
				t.Fatalf("cancellation: %v", err)
			}
			if _, ok, err := runs.wait(ctx, id); ok || err == nil {
				t.Fatalf("late callback: %t %v", ok, err)
			}
			failure := errors.New("commit rejected")
			go func() {
				_, err := pools.Assign(ctx, "rejected", func(id string) error {
					if poolPosition(runs, pools, id) != 1 {
						t.Error("wrong pool")
					}
					return failure
				})
				done <- err
			}()
			var next string
			select {
			case unit := <-lc.started:
				next = unitToRunIDFromTemplate("same@.service", unit)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if _, ok, err := runs.wait(ctx, next); ok || err == nil {
				t.Fatalf("failed commit published assignment: %t %v", ok, err)
			}
			if err := <-done; !errors.Is(err, failure) {
				t.Fatalf("commit failure: %v", err)
			}
			for n := 0; n < 2; n++ {
				select {
				case <-lc.stopped:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			if runs.unit(id) != "" || runs.unit(next) != "" {
				t.Fatal("retired runs leaked indexes")
			}
		})
	}
}
