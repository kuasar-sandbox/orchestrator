//go:build linux

package proxyshm

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/config"
	"github.com/kuasar-sandbox/orchestrator/internal/proxy"
	"github.com/kuasar-sandbox/orchestrator/internal/proxyadmission"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

func TestInvalidTrafficFullSyncKeepsLifecycleCredentialsAndMMDS(t *testing.T) {
	for _, state := range []string{routesync.StateStarting, routesync.StateRunning, routesync.StatePaused} {
		t.Run(state, func(t *testing.T) {
			table, _, master := newTrafficMasterView(t, 4, 2, config.MaxInflight{Total: 2})
			master.BeginSync()
			bad := trafficRoute("bad", state, nil)
			bad.TrafficPolicyInvalid = true
			bad.FloatingIP = "100.100.0.2"
			bad.MmdsSecret = "616263"
			bad.TemplateID = "template"
			bad.RunID = "run"
			bad.MMDSRoutes = testMMDSRoutes
			bad.MMDSRouteSecretValues = routeValues(map[string][]byte{"key": []byte("secret")})
			for _, route := range []routesync.RouteEntry{bad, trafficRoute("good", routesync.StateRunning, nil)} {
				if err := master.ApplyUpsert(route); err != nil {
					t.Fatal(err)
				}
			}
			master.Bookmark()
			got, found := table.Lookup("bad")
			if !table.Synced() || !found || got.State != state || !got.TrafficPolicyInvalid || got.AdmissionSlot != 0 || got.AdmissionGeneration != 0 || !got.EffectiveMaxInflight.Unlimited() {
				t.Fatalf("bad route=%+v found=%v", got, found)
			}
			good, found := table.Lookup("good")
			if !found || good.EffectiveMaxInflight.Total != 2 || good.AdmissionSlot == 0 {
				t.Fatalf("good route=%+v found=%v", good, found)
			}
			var wakes atomic.Int32
			worker := NewWorkerView(table, nil, func(string) { wakes.Add(1) }, time.Second)
			binding, found, err := worker.LookupRoute(context.Background(), "bad", proxy.LegacyTarget(8080))
			if err != nil || !found || !binding.TrafficPolicyInvalid || binding.ExpectedAccessToken != "forward" {
				t.Fatalf("ordinary identity=%+v found=%v err=%v", binding, found, err)
			}
			identity, found, err := worker.LookupExec(context.Background(), "bad")
			if err != nil || !found || !identity.TrafficPolicyInvalid || identity.ServiceSecret != "service" {
				t.Fatalf("exec identity=%+v found=%v err=%v", identity, found, err)
			}
			if _, ok, err := worker.ActivateRoute(context.Background(), binding); ok || err != nil {
				t.Fatalf("invalid ActivateRoute ok=%v err=%v", ok, err)
			}
			if _, ok, err := worker.ActivateExec(context.Background(), "bad", identity); ok || err != nil {
				t.Fatalf("invalid ActivateExec ok=%v err=%v", ok, err)
			}
			if wakes.Load() != 0 {
				t.Fatal("invalid policy woke Sandbox")
			}
			if state != routesync.StatePaused {
				if sid, ok := worker.ByFloatingIP(bad.FloatingIP); !ok || sid != "bad" {
					t.Fatalf("MMDS source lost %s %v", sid, ok)
				}
				if _, token, ok := worker.SandboxInfo("bad"); !ok || token != "envd" {
					t.Fatal("MMDS root lost credentials")
				}
				if secret, ok := worker.MmdsSecret("bad"); !ok || string(secret) != "abc" {
					t.Fatal("MMDS token secret lost")
				}
				if route := master.ResolveMMDS("bad", "/static"); !route.Found || string(route.Body) != "x" {
					t.Fatal("static MMDS lost")
				}
				if route := master.ResolveMMDS("bad", "/secret"); !route.Found || !route.Present || string(route.Body) != "secret" {
					t.Fatal("secret MMDS lost")
				}
			}
		})
	}
}

func TestInvalidTrafficRetirementRollbackAndRepair(t *testing.T) {
	table, arena, master := newTrafficMasterView(t, 4, 1, config.MaxInflight{Total: 1})
	route := trafficRoute("s1", routesync.StateRunning, nil)
	if err := master.ApplyUpsert(route); err != nil {
		t.Fatal(err)
	}
	master.Bookmark()
	if err := arena.BeginWorker(0, 1); err != nil {
		t.Fatal(err)
	}
	fd, err := arena.DupFile()
	if err != nil {
		t.Fatal(err)
	}
	worker, err := proxyadmission.OpenWorker(fd, 4, 1, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer worker.Close()
	view := NewMMDSWorkerView(table, worker, nil, nil, time.Second, nil)
	old, found, err := view.LookupRoute(context.Background(), "s1", proxy.LegacyTarget(8080))
	if err != nil || !found {
		t.Fatal(err)
	}
	held, err := worker.TryAcquire(old.Admission, proxyadmission.ServiceForward)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Release()
	originalUpsert := master.upsertPrimary
	injected := errors.New("publish failed")
	master.upsertPrimary = func(routesync.RouteEntry) error { return injected }
	route.TrafficPolicyInvalid = true
	if err := master.ApplyUpsert(route); !errors.Is(err, injected) {
		t.Fatalf("rollback err=%v", err)
	}
	if !worker.Valid(old.Admission) {
		t.Fatal("rollback invalidated old binding")
	}
	if _, err := worker.TryAcquire(old.Admission, proxyadmission.ServiceForward); !errors.Is(err, proxyadmission.ErrLimitReached) {
		t.Fatalf("rollback lost outstanding count: %v", err)
	}
	if got, _ := table.Lookup("s1"); got.TrafficPolicyInvalid {
		t.Fatal("failed publish changed validity")
	}
	master.upsertPrimary = originalUpsert
	if err := master.ApplyUpsert(route); err != nil {
		t.Fatal(err)
	}
	if worker.Valid(old.Admission) {
		t.Fatal("invalidation left old generation enabled")
	}
	if _, ok, err := view.ActivateRoute(context.Background(), old); ok || err != nil {
		t.Fatalf("old authorized binding remained usable: %v %v", ok, err)
	}
	route.TrafficPolicyInvalid = false
	if err := master.ApplyUpsert(route); err != nil {
		t.Fatal(err)
	}
	repaired, _, err := view.LookupRoute(context.Background(), "s1", proxy.LegacyTarget(8080))
	if err != nil {
		t.Fatal(err)
	}
	next, err := worker.TryAcquire(repaired.Admission, proxyadmission.ServiceForward)
	if err != nil {
		t.Fatal(err)
	}
	defer next.Release()
	held.Release()
	if _, err := worker.TryAcquire(repaired.Admission, proxyadmission.ServiceForward); !errors.Is(err, proxyadmission.ErrLimitReached) {
		t.Fatalf("late old release changed repaired generation: %v", err)
	}
}

func TestInvalidTrafficFencesAlreadyWaitingActivation(t *testing.T) {
	for _, exec := range []bool{false, true} {
		table, _, master := newTrafficMasterView(t, 4, 1, config.MaxInflight{})
		route := trafficRoute("s1", routesync.StatePaused, nil)
		if err := master.ApplyUpsert(route); err != nil {
			t.Fatal(err)
		}
		master.Bookmark()
		woke := make(chan struct{}, 1)
		worker := NewWorkerView(table, nil, func(string) { woke <- struct{}{} }, time.Second)
		binding, _, _ := worker.LookupRoute(context.Background(), "s1", proxy.LegacyTarget(8080))
		identity, _, _ := worker.LookupExec(context.Background(), "s1")
		done := make(chan bool, 1)
		go func() {
			if exec {
				_, ok, _ := worker.ActivateExec(context.Background(), "s1", identity)
				done <- ok
			} else {
				_, ok, _ := worker.ActivateRoute(context.Background(), binding)
				done <- ok
			}
		}()
		select {
		case <-woke:
		case <-time.After(2 * time.Second):
			t.Fatal("activation did not reach Wake")
		}
		route.TrafficPolicyInvalid = true
		route.State = routesync.StateRunning
		if err := master.ApplyUpsert(route); err != nil {
			t.Fatal(err)
		}
		select {
		case ok := <-done:
			if ok {
				t.Fatal("invalidated waiting activation returned a backend")
			}
		case <-time.After(2 * time.Second):
			t.Fatal("activation did not finish")
		}
	}
}

func TestDefaultTerminalCapacityRemainsCapped(t *testing.T) {
	for _, capacity := range []int{-1, 0, defaultCapacity} {
		if got := terminalCapacity(capacity); got != maxTerminalRevisions {
			t.Fatalf("terminalCapacity(%d)=%d want=%d", capacity, got, maxTerminalRevisions)
		}
	}
}
