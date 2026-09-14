package orch

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	conductorextension "github.com/kuasar-sandbox/orchestrator/app/conductor/extension"
	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

type nativeTrafficFunc func(context.Context, string) (*api.TrafficStats, error)

func (f nativeTrafficFunc) SandboxTrafficStats(ctx context.Context, sid, _ string, _ types.Profile, _ types.State) (*api.TrafficStats, error) {
	return f(ctx, sid)
}

func batchSandboxes(t *testing.T, o *Orchestrator, count int) []string {
	t.Helper()
	ids := make([]string, count)
	for i := range ids {
		ids[i] = fmt.Sprintf("native-%03d", i)
		sb := &types.Sandbox{ID: ids[i], Profile: types.ProfileBare, State: types.StateRunning,
			APISecret: strings.Repeat("1", 64), ManifestKey: strings.Repeat("2", 64)}
		materializeTestSandboxCredentials(t, sb)
		if err := o.st.Put(context.Background(), sb); err != nil {
			t.Fatal(err)
		}
	}
	return ids
}

func TestNativeStatsBatchBoundsAndSourceErrors(t *testing.T) {
	o := testOrch(t)
	ids := batchSandboxes(t, o, conductorextension.MaxStatsSandboxes)
	var calls atomic.Int64
	o.SetSandboxTrafficProvider(nativeTrafficFunc(func(_ context.Context, sid string) (*api.TrafficStats, error) {
		calls.Add(1)
		return &api.TrafficStats{State: sid, Services: map[string]api.ServiceTrafficStats{}}, nil
	}))
	for _, request := range []conductorextension.StatsRequest{
		{}, {SandboxIDs: ids, Sections: nil}, {SandboxIDs: append(append([]string{}, ids...), "extra"), Sections: []string{"traffic"}},
		{SandboxIDs: []string{ids[0], ids[0]}, Sections: []string{"traffic"}},
		{SandboxIDs: []string{""}, Sections: []string{"traffic"}},
		{SandboxIDs: ids, Sections: []string{"resource", "resource"}},
		{SandboxIDs: ids, Sections: []string{"network"}},
		{SandboxIDs: ids, Sections: []string{"traffic"}, Usage: conductorextension.UsageQuery{View: "saved"}},
		{SandboxIDs: ids, Sections: []string{"usage"}, Usage: conductorextension.UsageQuery{View: "current", Limit: 1}},
	} {
		if rows, err := o.ReadStats(context.Background(), request); !errors.Is(err, api.ErrBadRequest) || rows != nil {
			t.Fatal("invalid batch accepted", request, rows, err)
		}
	}
	if calls.Load() != 0 {
		t.Fatal("invalid batch reached sources")
	}
	rows, err := o.ReadStats(context.Background(), conductorextension.StatsRequest{SandboxIDs: ids, Sections: []string{"traffic"}})
	if err != nil || len(rows) != len(ids) {
		t.Fatal(rows, err)
	}
	for i, row := range rows {
		if row.SandboxID != ids[i] || row.Traffic.State != ids[i] {
			t.Fatal("batch reordered identities", i, row)
		}
	}
	for _, source := range []nativeTrafficFunc{
		func(context.Context, string) (*api.TrafficStats, error) { return nil, api.ErrStatsUnavailable },
		func(context.Context, string) (*api.TrafficStats, error) { return nil, nil },
		func(context.Context, string) (*api.TrafficStats, error) {
			return &api.TrafficStats{Services: map[string]api.ServiceTrafficStats{strings.Repeat("x", conductorextension.MaxStatsResponseBytes): {}}}, nil
		},
	} {
		o.SetSandboxTrafficProvider(source)
		rows, err := o.ReadStats(context.Background(), conductorextension.StatsRequest{SandboxIDs: ids[:1], Sections: []string{"traffic"}})
		if !errors.Is(err, api.ErrStatsUnavailable) || rows != nil {
			t.Fatal("failed/incomplete/oversized source returned a complete batch", len(rows), err)
		}
	}
}

func TestNativeStatsBatchGlobalConcurrencyAndCancellation(t *testing.T) {
	o := testOrch(t)
	ids := batchSandboxes(t, o, conductorextension.MaxStatsSandboxes)
	var active, maximum atomic.Int64
	var blocking atomic.Bool
	blocking.Store(true)
	entered := make(chan struct{}, 2*len(ids))
	o.SetSandboxTrafficProvider(nativeTrafficFunc(func(ctx context.Context, _ string) (*api.TrafficStats, error) {
		current := active.Add(1)
		defer active.Add(-1)
		for old := maximum.Load(); current > old && !maximum.CompareAndSwap(old, current); old = maximum.Load() {
		}
		if blocking.Load() {
			entered <- struct{}{}
			<-ctx.Done()
			return nil, ctx.Err()
		}
		return &api.TrafficStats{State: "running"}, nil
	}))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 2)
	for range 2 {
		go func() {
			rows, err := o.ReadStats(ctx, conductorextension.StatsRequest{SandboxIDs: ids, Sections: []string{"traffic"}})
			if rows != nil {
				err = errors.New("cancellation returned partial rows")
			}
			done <- err
		}()
	}
	for range conductorextension.MaxStatsConcurrency {
		select {
		case <-entered:
		case <-time.After(3 * time.Second):
			t.Fatal("batch did not fill its bounded concurrency")
		}
	}
	// A saturated read budget also holds object prefetch. Looking up a missing
	// ID immediately here would bypass the eight active native source reads.
	limited, stopLimited := context.WithTimeout(context.Background(), 100*time.Millisecond)
	rows, err := o.ReadStats(limited, conductorextension.StatsRequest{SandboxIDs: []string{"not-present"}, Sections: []string{"traffic"}})
	stopLimited()
	if rows != nil || !errors.Is(err, api.ErrStatsUnavailable) {
		t.Fatal("traffic object lookup bypassed global read budget", rows, err)
	}
	cancel()
	for range 2 {
		select {
		case err := <-done:
			if !errors.Is(err, api.ErrStatsUnavailable) {
				t.Fatal("canceled batch error", err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("batch kept running after cancellation")
		}
	}
	if maximum.Load() > conductorextension.MaxStatsConcurrency || active.Load() != 0 {
		t.Fatal("global concurrency or cleanup", maximum.Load(), active.Load())
	}
	blocking.Store(false)
	rows, err = o.ReadStats(context.Background(), conductorextension.StatsRequest{SandboxIDs: ids, Sections: []string{"traffic"}})
	if err != nil || len(rows) != len(ids) {
		t.Fatal("cancellation leaked read slots", len(rows), err)
	}
}

func TestNativeStatsBatchMissingSandbox(t *testing.T) {
	o := testOrch(t)
	for _, section := range []string{"resource", "traffic", "usage"} {
		t.Run(section, func(t *testing.T) {
			rows, err := o.ReadStats(context.Background(), conductorextension.StatsRequest{SandboxIDs: []string{"missing-sandbox"}, Sections: []string{section}})
			if rows != nil || !errors.Is(err, api.ErrNotFound) {
				t.Fatalf("missing sandbox: rows=%+v err=%v", rows, err)
			}
		})
	}
}
