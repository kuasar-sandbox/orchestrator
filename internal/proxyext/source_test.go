package proxyext

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	proxyextension "github.com/kuasar-sandbox/orchestrator/app/proxy/extension"
	"github.com/kuasar-sandbox/orchestrator/internal/metrics"
	internalproxy "github.com/kuasar-sandbox/orchestrator/internal/proxy"
	"github.com/kuasar-sandbox/orchestrator/internal/proxyshm"
	"github.com/kuasar-sandbox/orchestrator/internal/proxystats"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

type testCoreSink struct {
	table       *proxyshm.Table
	applyErr    error
	beforeApply func(routesync.RouteEntry)
	invalidated bool
}

func (s *testCoreSink) BeginSync() { s.table.BeginSync() }

func (s *testCoreSink) ApplyUpsert(route routesync.RouteEntry) error {
	if s.beforeApply != nil {
		s.beforeApply(route)
	}
	if s.applyErr != nil {
		return s.applyErr
	}
	return s.table.Upsert(route)
}

func (s *testCoreSink) ApplyDelete(sandboxID string) { s.table.Delete(sandboxID) }
func (s *testCoreSink) Bookmark()                    { s.table.Bookmark() }
func (s *testCoreSink) SetPolicy(policy routesync.Policy) {
	_ = s.table.SetPolicy(policy)
}
func (s *testCoreSink) InvalidateSync() { s.invalidated = true }

func newTestHost(t *testing.T, capacity int, stats *proxystats.MasterStats) (*Host, *ObservingSink, *testCoreSink) {
	t.Helper()
	table, err := proxyshm.Create(filepath.Join(t.TempDir(), "routes.shm"), 16)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = table.Close() })
	core := &testCoreSink{table: table}
	host, sink := newWithCapacity(core, table, stats, capacity)
	return host, sink, core
}

func testRoute(sandboxID, runID string) routesync.RouteEntry {
	return routesync.RouteEntry{
		SandboxID: sandboxID, AuthSandboxID: "auth-" + sandboxID,
		Profile: "bare", TemplateID: "tmpl-1",
		State: routesync.StateRunning, RunID: runID,
		FloatingIP: "10.0.0.2", SnapshotLocation: "remote",
		APISecret: "raw-api-secret", APISecretFingerprint: "api-fingerprint",
		ManifestKeyFingerprint: "manifest-fingerprint", ServiceSecret: "raw-service-secret",
		EnvdAccessToken: "raw-envd-token", TrafficAccessToken: "raw-traffic-token",
		ForwardAccessToken: "raw-forward-token", MmdsSecret: "raw-mmds-secret",
	}
}

func TestObservingSinkPublishesOnlyAfterCoreApply(t *testing.T) {
	host, sink, core := newTestHost(t, 8, nil)
	core.beforeApply = func(route routesync.RouteEntry) {
		if _, found, err := host.Routes().Get(context.Background(), route.SandboxID); err != nil || found {
			t.Fatalf("extension route visible before core apply: found=%v err=%v", found, err)
		}
	}
	sink.BeginSync()
	route := testRoute("s1", "run-1")
	if err := sink.ApplyUpsert(route); err != nil {
		t.Fatal(err)
	}
	view, found, err := host.Routes().Get(context.Background(), route.SandboxID)
	if err != nil || !found {
		t.Fatalf("Get after core apply: found=%v err=%v", found, err)
	}
	if view.Revision == 0 || view.APISecretFingerprint != route.APISecretFingerprint || view.ManifestKeyFingerprint != route.ManifestKeyFingerprint {
		t.Fatalf("projected route = %+v", view)
	}
	view.RunID = "mutated"
	again, found, err := host.Routes().Get(context.Background(), route.SandboxID)
	if err != nil || !found || again.RunID != "run-1" {
		t.Fatalf("Get shared mutable state: view=%+v found=%v err=%v", again, found, err)
	}

	core.applyErr = errors.New("route table full")
	if err := sink.ApplyUpsert(testRoute("s2", "run-2")); !errors.Is(err, core.applyErr) {
		t.Fatalf("ApplyUpsert error = %v", err)
	}
	if _, found, err := host.Routes().Get(context.Background(), "s2"); err != nil || found {
		t.Fatalf("failed core apply published: found=%v err=%v", found, err)
	}
}

func TestRouteViewProjectionHasNoRawCredentialFields(t *testing.T) {
	viewType := reflect.TypeOf(proxyextension.RouteView{})
	for _, forbidden := range []string{
		"APISecret", "ManifestKey", "ServiceSecret", "EnvdAccessToken",
		"TrafficAccessToken", "ForwardAccessToken", "MmdsSecret", "MMDSRouteSecretValues",
	} {
		if _, found := viewType.FieldByName(forbidden); found {
			t.Fatalf("RouteView exposes raw credential field %s", forbidden)
		}
	}
}

func TestRouteWatchSyncStateLostAndFullResync(t *testing.T) {
	host, sink, core := newTestHost(t, 8, nil)
	if got := host.Routes().SyncState(); got != proxyextension.RouteSyncInitializing {
		t.Fatalf("initial state = %q", got)
	}
	sink.BeginSync()
	if got := host.Routes().SyncState(); got != proxyextension.RouteSyncSyncing {
		t.Fatalf("syncing state = %q", got)
	}
	if err := sink.ApplyUpsert(testRoute("s1", "run-1")); err != nil {
		t.Fatal(err)
	}
	sink.Bookmark()
	if got := host.Routes().SyncState(); got != proxyextension.RouteSyncSynced {
		t.Fatalf("synced state = %q", got)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events := make(chan proxyextension.RouteEvent, 32)
	done := make(chan error, 1)
	go func() {
		done <- host.Routes().Watch(ctx, func(event proxyextension.RouteEvent) error {
			// A callback may safely re-enter the point source; it never runs under
			// the route source or core route-table lock.
			if event.SandboxID != "" {
				_, _, _ = host.Routes().Get(ctx, event.SandboxID)
			}
			events <- event
			return nil
		})
	}()
	first := collectCompleteGeneration(t, events)
	if len(first) != 3 || first[0].Kind != proxyextension.RouteSyncBegin || first[1].SandboxID != "s1" || first[2].Kind != proxyextension.RouteSyncEnd {
		t.Fatalf("initial generation = %+v", first)
	}
	sink.ApplyDelete("s1")
	deleted := receiveRouteEvent(t, events)
	if deleted.Kind != proxyextension.RouteDelete || deleted.View == nil || deleted.View.Revision == 0 {
		t.Fatalf("live delete = %+v", deleted)
	}
	if _, found, err := host.Routes().Get(context.Background(), "s1"); err != nil || found {
		t.Fatalf("deleted route Get: found=%v err=%v", found, err)
	}
	if err := sink.ApplyUpsert(testRoute("s1", "run-1b")); err != nil {
		t.Fatal(err)
	}
	upserted := receiveRouteEvent(t, events)
	if upserted.Kind != proxyextension.RouteUpsert || upserted.View == nil || upserted.View.RunID != "run-1b" {
		t.Fatalf("live upsert = %+v", upserted)
	}

	sink.InvalidateSync()
	if !core.invalidated || host.Routes().SyncState() != proxyextension.RouteSyncStale {
		t.Fatalf("invalidate: core=%v state=%q", core.invalidated, host.Routes().SyncState())
	}
	lost := receiveRouteEvent(t, events)
	if lost.Kind != proxyextension.RouteSyncLost || lost.Generation != first[0].Generation {
		t.Fatalf("sync lost = %+v", lost)
	}

	sink.BeginSync()
	if err := sink.ApplyUpsert(testRoute("s2", "run-2")); err != nil {
		t.Fatal(err)
	}
	sink.Bookmark()
	second := collectCompleteGeneration(t, events)
	if second[0].Generation == first[0].Generation || len(second) != 3 || second[1].SandboxID != "s2" {
		t.Fatalf("resync generation = %+v", second)
	}
	if _, found, err := host.Routes().Get(context.Background(), "s1"); err != nil || found {
		t.Fatalf("stale route survived full sync: found=%v err=%v", found, err)
	}

	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Watch cancellation = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Watch did not stop on context cancellation")
	}
}

func TestRouteWatchOverflowAbandonsIncompleteGenerationOnlyForSlowWatcher(t *testing.T) {
	host, sink, _ := newTestHost(t, 1, nil)
	sink.BeginSync()
	if err := sink.ApplyUpsert(testRoute("s1", "run-1")); err != nil {
		t.Fatal(err)
	}
	sink.Bookmark()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	slowStarted := make(chan struct{})
	releaseSlow := make(chan struct{})
	slowEvents := make(chan proxyextension.RouteEvent, 32)
	slowDone := make(chan error, 1)
	var slowOnce sync.Once
	go func() {
		slowDone <- host.Routes().Watch(ctx, func(event proxyextension.RouteEvent) error {
			slowEvents <- event
			if event.Kind == proxyextension.RouteUpsert && event.Generation == 1 {
				slowOnce.Do(func() {
					close(slowStarted)
					<-releaseSlow
				})
			}
			return nil
		})
	}()
	select {
	case <-slowStarted:
	case <-time.After(time.Second):
		t.Fatal("slow watcher did not enter snapshot callback")
	}

	fastEvents := make(chan proxyextension.RouteEvent, 32)
	fastDone := make(chan error, 1)
	go func() {
		fastDone <- host.Routes().Watch(ctx, func(event proxyextension.RouteEvent) error {
			fastEvents <- event
			return nil
		})
	}()
	fastInitial := collectCompleteGeneration(t, fastEvents)
	fastGeneration := fastInitial[0].Generation

	for _, runID := range []string{"run-2", "run-3"} {
		if err := sink.ApplyUpsert(testRoute("s1", runID)); err != nil {
			t.Fatal(err)
		}
		fast := receiveRouteEvent(t, fastEvents)
		if fast.Kind != proxyextension.RouteUpsert || fast.Generation != fastGeneration || fast.View.RunID != runID {
			t.Fatalf("fast live event = %+v", fast)
		}
	}
	close(releaseSlow)

	var completed []proxyextension.RouteEvent
	deadline := time.After(time.Second)
	for len(completed) == 0 {
		select {
		case event := <-slowEvents:
			if event.Generation == 1 && event.Kind == proxyextension.RouteSyncEnd {
				t.Fatal("overflowed snapshot generation was marked complete")
			}
			if event.Generation != 1 {
				completed = append(completed, event)
				if event.Kind == proxyextension.RouteSyncEnd {
					if len(completed) != 3 || completed[1].View == nil || completed[1].View.RunID != "run-3" {
						t.Fatalf("slow watcher resync = %+v", completed)
					}
					goto converged
				}
			}
		case <-deadline:
			t.Fatal("slow watcher did not resynchronize")
		}
	}

converged:
	cancel()
	for name, done := range map[string]<-chan error{"slow": slowDone, "fast": fastDone} {
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("%s Watch error = %v", name, err)
			}
		case <-time.After(time.Second):
			t.Fatalf("%s Watch did not stop", name)
		}
	}
}

func TestRouteWatchReturnsCallbackError(t *testing.T) {
	host, sink, _ := newTestHost(t, 4, nil)
	sink.BeginSync()
	sink.Bookmark()
	want := errors.New("stop watching")
	err := host.Routes().Watch(context.Background(), func(event proxyextension.RouteEvent) error {
		if event.Kind == proxyextension.RouteSyncEnd {
			return want
		}
		return nil
	})
	if !errors.Is(err, want) {
		t.Fatalf("Watch callback error = %v", err)
	}
}

func TestTrafficSourceUsesInProcessAggregateAndReturnsCopies(t *testing.T) {
	stats := proxystats.NewMasterStats(metrics.New(), []string{"w0"})
	if err := stats.BeginWorker("w0", 1); err != nil {
		t.Fatal(err)
	}
	if err := stats.Receive("w0", 1, proxystats.Frame{
		Type: proxystats.TypeHello, Version: proxystats.Version, WorkerID: "w0", Epoch: 1,
	}); err != nil {
		t.Fatal(err)
	}
	if err := stats.Receive("w0", 1, proxystats.Frame{
		Type: proxystats.TypeReady, Version: proxystats.Version, Epoch: 1, Sequence: 1,
	}); err != nil {
		t.Fatal(err)
	}
	if err := stats.Receive("w0", 1, proxystats.Frame{
		Type: proxystats.TypeUpdate, Version: proxystats.Version, Epoch: 1, Sequence: 2,
		Traffic: []proxystats.SandboxSnapshot{{SandboxID: "s1", Services: map[string]proxystats.ServiceSnapshot{
			string(internalproxy.ConnectServiceForward): {Parking: 2, Egress: 3},
		}}},
	}); err != nil {
		t.Fatal(err)
	}
	host, sink, _ := newTestHost(t, 8, stats)
	if _, err := host.Traffic().Get(context.Background(), "s1"); !errors.Is(err, proxyextension.ErrTrafficUnavailable) {
		t.Fatalf("traffic before sync = %v", err)
	}
	sink.BeginSync()
	if err := sink.ApplyUpsert(testRoute("s1", "run-1")); err != nil {
		t.Fatal(err)
	}
	sink.Bookmark()

	view, err := host.Traffic().Get(context.Background(), "s1")
	if err != nil {
		t.Fatal(err)
	}
	if view.SandboxID != "s1" || view.RunID != "run-1" || view.Profile != proxyextension.ProfileBare ||
		view.State != proxyextension.RouteStateRunning || view.Inflight.Parking != 2 || view.Inflight.Egress != 3 {
		t.Fatalf("traffic view = %+v", view)
	}
	forward := view.Services[string(internalproxy.ConnectServiceForward)]
	if forward.Parking != 2 || forward.Egress != 3 {
		t.Fatalf("forward traffic = %+v", forward)
	}
	delete(view.Services, string(internalproxy.ConnectServiceExec))
	again, err := host.Traffic().Get(context.Background(), "s1")
	if err != nil {
		t.Fatal(err)
	}
	if _, found := again.Services[string(internalproxy.ConnectServiceExec)]; !found {
		t.Fatal("TrafficSource returned a shared services map")
	}
	if _, err := host.Traffic().Get(context.Background(), "missing"); !errors.Is(err, proxyextension.ErrTrafficUnavailable) {
		t.Fatalf("missing traffic error = %v", err)
	}

	sink.InvalidateSync()
	stale, err := host.Traffic().Get(context.Background(), "s1")
	if err != nil || stale.RunID != "run-1" {
		t.Fatalf("retained stale route traffic = %+v err=%v", stale, err)
	}
}

func collectCompleteGeneration(t *testing.T, events <-chan proxyextension.RouteEvent) []proxyextension.RouteEvent {
	t.Helper()
	var generation uint64
	var result []proxyextension.RouteEvent
	for {
		event := receiveRouteEvent(t, events)
		if generation == 0 {
			generation = event.Generation
		}
		if event.Generation != generation {
			t.Fatalf("mixed generations: got %d, want %d", event.Generation, generation)
		}
		result = append(result, event)
		if event.Kind == proxyextension.RouteSyncEnd {
			return result
		}
	}
}

func receiveRouteEvent(t *testing.T, events <-chan proxyextension.RouteEvent) proxyextension.RouteEvent {
	t.Helper()
	select {
	case event := <-events:
		return event
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for route event")
		return proxyextension.RouteEvent{}
	}
}
