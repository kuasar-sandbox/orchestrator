package orch

import (
	"context"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/config"
)

func startReservationTestPool(t *testing.T, size, capacity int) (*runPool, *runIndex, context.Context) {
	t.Helper()
	runs := &runIndex{}
	var pool *runPool
	// Registered before startPoolSet so its cleanup stops the pool before we
	// remove ownership. Token cleanups registered by each test run first.
	t.Cleanup(func() {
		runs.mu.Lock()
		var ids []string
		for id := range runs.units {
			ids = append(ids, id)
		}
		reserved := 0
		if pool != nil {
			reserved = pool.reserved
		}
		runs.mu.Unlock()
		if reserved != 0 {
			t.Errorf("reservation cleanup left %d reserved slots", reserved)
		}
		for _, id := range ids {
			if err := runs.forget(id); err != nil {
				t.Errorf("forget run %s: %v", id, err)
			}
		}
	})
	pools, ctx, _ := startPoolSet(t, runKindSandbox, []config.RunPoolConfig{{
		Unit: "reservation@.service", Name: "reservation", Size: size, Capacity: &capacity,
	}}, callbackLauncher(t, runs), runs)
	pool = pools.pools[0]
	return pool, runs, ctx
}

func assertReservationState(t *testing.T, reservation *runReservation, wantActive bool, wantReserved int) {
	t.Helper()
	reservation.runs.mu.Lock()
	active, reserved := reservation.active, reservation.pool.reserved
	reservation.runs.mu.Unlock()
	if active != wantActive || reserved != wantReserved {
		t.Fatalf("reservation active=%t reserved=%d, want active=%t reserved=%d", active, reserved, wantActive, wantReserved)
	}
}

func TestRunPoolReservationTransfersAtHandoff(t *testing.T) {
	for _, tc := range []struct {
		name string
		size int
	}{
		{name: "on-demand", size: 0},
		{name: "idle-target", size: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pool, runs, ctx := startReservationTestPool(t, tc.size, 2)
			first := runs.reserve(pool)
			if first == nil {
				t.Fatal("first reservation was rejected")
			}
			t.Cleanup(first.release)
			firstID, err := pool.assignWithReservation(ctx, "first", nil, func(string) error { return nil }, first)
			if err != nil {
				t.Fatal(err)
			}

			// The request is bound, but the caller has not run its deferred
			// release. Holding this window open must not hide the second slot.
			assertReservationState(t, first, false, 0)
			if !runs.hasSupply(pool) {
				t.Fatal("bound request still hides the second slot before caller cleanup")
			}
			second := runs.reserve(pool)
			if second == nil {
				t.Fatal("second reservation was rejected before first caller cleanup")
			}
			t.Cleanup(second.release)

			// Delayed or repeated cleanup belongs to the first request only.
			first.release()
			first.release()
			assertReservationState(t, second, true, 1)
			if runs.hasSupply(pool) {
				t.Fatal("releasing the consumed token exposed the second request's slot")
			}
			if extra := runs.reserve(pool); extra != nil {
				extra.release()
				t.Fatal("reservation admitted beyond capacity while the second token was held")
			}

			secondID, err := pool.assignWithReservation(ctx, "second", nil, func(string) error { return nil }, second)
			if err != nil {
				t.Fatal(err)
			}
			if secondID == firstID {
				t.Fatal("two assignments received the same run")
			}
			assertReservationState(t, second, false, 0)
			second.release()
			second.release()
			assertReservationState(t, second, false, 0)
			if runs.hasSupply(pool) {
				t.Fatal("caller cleanup exposed capacity occupied by two bound runs")
			}
		})
	}
}

func TestRunPoolDirectAssignmentKeepsOtherReservation(t *testing.T) {
	pool, runs, ctx := startReservationTestPool(t, 0, 2)
	reservation := runs.reserve(pool)
	if reservation == nil {
		t.Fatal("reservation was rejected")
	}
	t.Cleanup(reservation.release)
	if _, err := pool.Assign(ctx, "direct", func(string) error { return nil }); err != nil {
		t.Fatal(err)
	}

	// Assign passes no token. Its handoff must not consume a reservation
	// belonging to a different, not-yet-enqueued request in the same pool.
	assertReservationState(t, reservation, true, 1)
	if runs.hasSupply(pool) {
		t.Fatal("direct assignment consumed another request's reservation")
	}
	if extra := runs.reserve(pool); extra != nil {
		extra.release()
		t.Fatal("another reservation was admitted while the remaining slot was reserved")
	}

	reservation.release()
	assertReservationState(t, reservation, false, 0)
	if !runs.hasSupply(pool) {
		t.Fatal("releasing the unused reservation did not restore the remaining slot")
	}
	next := runs.reserve(pool)
	if next == nil {
		t.Fatal("remaining slot could not be reserved after explicit release")
	}
	t.Cleanup(next.release)
	if _, err := pool.assignWithReservation(ctx, "reserved", nil, func(string) error { return nil }, next); err != nil {
		t.Fatal(err)
	}
	assertReservationState(t, next, false, 0)
	if runs.hasSupply(pool) {
		t.Fatal("two bound runs did not exhaust capacity")
	}
}
