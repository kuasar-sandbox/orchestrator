package orch

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestRunPoolsPriorityBeforeIdleAndCapacity(t *testing.T) {
	runs := &runIndex{}
	lc := callbackLauncher(t, runs)
	limit := 1
	pools, ctx, _ := startPoolSet(t, runKindSandbox, []config.RunPoolConfig{
		{Unit: "low@.service", Name: "low", Size: 1, ResourcePool: "numa0"},
		{Unit: "high@.service", Name: "high", Priority: 10, Capacity: &limit, ResourcePool: "numa0"},
	}, lc, runs)
	id, err := pools.assignInResourcePool(ctx, "numa0", "one", nil, func(id string) error {
		if poolPosition(runs, pools, id) != 1 {
			return errors.New("existing idle outranked high priority start")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer runs.forget(id)
	other, err := pools.assignInResourcePool(ctx, "numa0", "two", nil, func(id string) error {
		if poolPosition(runs, pools, id) != 0 {
			return errors.New("exhausted high-priority pool selected")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	runs.forget(other)
}

func TestRunPoolsConcurrentSlotsAndRefill(t *testing.T) {
	runs := &runIndex{}
	lc := callbackLauncher(t, runs)
	limit := 4
	pools, ctx, _ := startPoolSet(t, runKindSandbox, []config.RunPoolConfig{{Unit: "cap@.service", Name: "cap", Size: 2, Capacity: &limit}}, lc, runs)
	var wg sync.WaitGroup
	var admitted atomic.Int32
	var ids sync.Map
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id, err := pools.Assign(ctx, fmt.Sprint(i), func(string) error { return nil })
			if err == nil {
				admitted.Add(1)
				ids.Store(id, true)
			}
		}(i)
	}
	wg.Wait()
	if admitted.Load() != int32(limit) {
		t.Fatalf("admitted %d want %d", admitted.Load(), limit)
	}
	runs.mu.Lock()
	if runs.countLocked(pools.pools[0]) != limit || pools.pools[0].reserved != 0 {
		t.Error("capacity or assignment reservation leaked")
	}
	runs.mu.Unlock()
	ids.Range(func(key, value any) bool { runs.forget(key.(string)); return true })
	deadline := time.After(2 * time.Second)
	for {
		runs.mu.Lock()
		n := len(runs.waiting)
		total := runs.countLocked(pools.pools[0])
		runs.mu.Unlock()
		if n == 2 && total == 2 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("idle target not replenished: waiting=%d total=%d", n, total)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestRunPoolsStoppingFailureKeepsSlotUntilRetry(t *testing.T) {
	runs := &runIndex{}
	lc := newRunPoolTestLauncher()
	var allow atomic.Bool
	stopAttempt := make(chan struct{}, 8)
	lc.stopFn = func(context.Context, string) error {
		select {
		case stopAttempt <- struct{}{}:
		default:
		}
		if !allow.Load() {
			return errors.New("stop failed")
		}
		return nil
	}
	limit := 1
	pools, ctx, _ := startPoolSet(t, runKindSandbox, []config.RunPoolConfig{{Unit: "cap@.service", Capacity: &limit}}, lc, runs)
	req, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { _, err := pools.Assign(req, "cancel", func(string) error { return nil }); done <- err }()
	var id string
	select {
	case unit := <-lc.started:
		id = unitToRunIDFromTemplate("cap@.service", unit)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	cancel()
	if !errors.Is(<-done, context.Canceled) {
		t.Fatal("assignment not cancelled")
	}
	if _, _, err := runs.wait(ctx, id); err == nil {
		t.Fatal("cancelled assignment accepted")
	}
	select {
	case <-stopAttempt:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if pools.hasSupply("default") || runs.unit(id) == "" {
		t.Fatal("failed stop freed capacity")
	}
	allow.Store(true)
	deadline := time.After(2 * time.Second)
	for runs.unit(id) != "" {
		select {
		case <-deadline:
			t.Fatal("retirement did not retry")
		case <-time.After(10 * time.Millisecond):
		}
	}
	if !pools.hasSupply("default") {
		t.Fatal("confirmed stop did not free capacity")
	}
}

func TestPressureIdleFirstBusyAndErrors(t *testing.T) {
	rows := []*types.Sandbox{{ID: "old"}, {ID: "middle"}, {ID: "new"}}
	tests := []struct {
		name    string
		replies map[string]error
		want    []string
		sid     string
	}{
		{"idle-after-busy", map[string]error{"old": api.ErrSandboxBusy}, []string{"old:true", "middle:true"}, "middle"},
		{"oldest-busy-fallback", map[string]error{"old": api.ErrSandboxBusy, "middle": api.ErrSandboxBusy, "new": api.ErrSandboxBusy}, []string{"old:true", "middle:true", "new:true", "old:false"}, "old"},
		{"proxy-not-busy", map[string]error{"old": api.ErrProxyUnavailable, "middle": api.ErrSandboxBusy, "new": api.ErrSandboxChanged}, []string{"old:true", "middle:true", "new:true", "middle:false"}, "middle"},
		{"no-busy-evidence", map[string]error{"old": api.ErrProxyUnavailable, "middle": api.ErrSandboxChanged, "new": errSourceOperationBusy}, []string{"old:true", "middle:true", "new:true"}, ""},
		{"capture-stops-scan", map[string]error{"old": errors.Join(errPressureCaptureStarted, errors.New("capture failed"))}, []string{"old:true"}, "old"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var calls []string
			sid, _ := tryIdleFirst(rows, func(sb *types.Sandbox, idle bool) error {
				calls = append(calls, fmt.Sprintf("%s:%t", sb.ID, idle))
				if idle {
					return tt.replies[sb.ID]
				}
				return nil
			})
			if sid != tt.sid || !reflect.DeepEqual(calls, tt.want) {
				t.Fatalf("sid=%s calls=%v", sid, calls)
			}
		})
	}
}
