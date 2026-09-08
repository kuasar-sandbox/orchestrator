package proxyext

import (
	"context"
	"errors"
	"testing"

	proxyextension "github.com/kuasar-sandbox/orchestrator/app/proxy/extension"
	"github.com/kuasar-sandbox/orchestrator/internal/metrics"
	"github.com/kuasar-sandbox/orchestrator/internal/proxystats"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

func TestInvalidPolicyKeepsExtensionRouteFactsButNotTraffic(t *testing.T) {
	stats := proxystats.NewMasterStats(metrics.New(), []string{"w0"})
	if err := stats.BeginWorker("w0", 1); err != nil {
		t.Fatal(err)
	}
	for _, frame := range []proxystats.Frame{
		{Type: proxystats.TypeHello, Version: proxystats.Version, WorkerID: "w0", Epoch: 1},
		{Type: proxystats.TypeReady, Version: proxystats.Version, Epoch: 1, Sequence: 1},
	} {
		if err := stats.Receive("w0", 1, frame); err != nil {
			t.Fatal(err)
		}
	}
	host, sink, core := newTestHost(t, 8, stats)
	workerHost := NewWorkerHost(proxyextension.Process{Role: proxyextension.RoleWorker}, core.table, nil)
	for _, state := range []string{routesync.StateStarting, routesync.StateRunning, routesync.StatePaused} {
		sink.BeginSync()
		bad := testRoute("bad", "run-bad")
		bad.State = state
		bad.TrafficPolicyInvalid = true
		for _, route := range []routesync.RouteEntry{bad, testRoute("good", "run-good")} {
			if err := sink.ApplyUpsert(route); err != nil {
				t.Fatal(err)
			}
		}
		sink.Bookmark()
		got, found, err := host.Routes().Get(context.Background(), "bad")
		if err != nil || !found || string(got.State) != state || got.RunID != "run-bad" {
			t.Fatalf("lost route fact %+v found=%v err=%v", got, found, err)
		}
		if view, found := workerHost.GetRoute("bad"); !found || string(view.State) != state || view.RunID != "run-bad" {
			t.Fatalf("worker lost route facts: %+v found=%v", view, found)
		}
		if _, err := host.Traffic().Get(context.Background(), "bad"); !errors.Is(err, proxyextension.ErrTrafficUnavailable) {
			t.Fatalf("invalid traffic query err=%v", err)
		}
		if _, err := host.Traffic().Get(context.Background(), "good"); err != nil {
			t.Fatalf("good traffic query: %v", err)
		}
	}
	repaired := testRoute("bad", "run-bad")
	if err := sink.ApplyUpsert(repaired); err != nil {
		t.Fatal(err)
	}
	if view, found := workerHost.GetRoute("bad"); !found || string(view.State) != repaired.State || view.RunID != "run-bad" {
		t.Fatalf("worker lost route facts: %+v found=%v", view, found)
	}
	if _, err := host.Traffic().Get(context.Background(), "bad"); err != nil {
		t.Fatalf("repaired traffic query: %v", err)
	}
}
