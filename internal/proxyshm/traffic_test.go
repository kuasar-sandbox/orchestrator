//go:build linux

package proxyshm

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/config"
	"github.com/kuasar-sandbox/orchestrator/internal/metrics"
	"github.com/kuasar-sandbox/orchestrator/internal/proxy"
	"github.com/kuasar-sandbox/orchestrator/internal/proxyadmission"
	"github.com/kuasar-sandbox/orchestrator/internal/proxystats"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
)

func trafficRoute(sid, state string, patch *sandboxcfg.MaxInflightPatch) routesync.RouteEntry {
	return routesync.RouteEntry{
		SandboxID: sid, StableID: "stable-" + sid, Profile: "e2b", State: state,
		APISecretFingerprint: "api", ManifestKeyFingerprint: "manifest",
		ServiceSecret: "service", EnvdAccessToken: "envd", TrafficAccessToken: "traffic",
		ForwardAccessToken: "forward", MaxInflightPatch: patch,
	}
}

func newTrafficMasterView(t *testing.T, capacity, workers int, defaults config.MaxInflight) (*Table, *proxyadmission.Master, *MasterView) {
	t.Helper()
	table, err := Create(filepath.Join(t.TempDir(), "routes.shm"), capacity)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = table.Close() })
	admission, err := proxyadmission.NewMaster(capacity, workers)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admission.Close() })
	return table, admission, NewMasterViewWithAdmission(table, admission, defaults, time.Second, nil)
}

func TestMasterResolvesTargetDefaultsAndExplicitZeroBeforeRoutePublication(t *testing.T) {
	defaults := config.MaxInflight{Total: 128, Forward: 96, E2BEnvd: 16, E2BCodeInterpreter: 8, Exec: 8}
	table, _, view := newTrafficMasterView(t, 8, 2, defaults)
	zero, two := uint32(0), uint32(2)
	route := trafficRoute("s1", routesync.StateStarting, &sandboxcfg.MaxInflightPatch{Total: &zero, Exec: &two})
	if err := view.ApplyUpsert(route); err != nil {
		t.Fatal(err)
	}
	got, found := table.Lookup("s1")
	if !found {
		t.Fatal("route was not published")
	}
	want := defaults
	want.Total = 0
	want.Exec = 2
	if got.EffectiveMaxInflight != want || got.AdmissionSlot == 0 || got.AdmissionGeneration == 0 {
		t.Fatalf("effective route = %+v", got)
	}
	if got.MaxInflightPatch != nil {
		t.Fatal("variable patch entered route SHM")
	}
}

func TestAbsentTrafficPatchResolvesAgainstEachTargetProxy(t *testing.T) {
	route := trafficRoute("migrated", routesync.StateStarting, nil)
	for name, defaults := range map[string]config.MaxInflight{
		"source":      {Total: 8, Forward: 4, Exec: 1},
		"destination": {Total: 32, Forward: 12, Exec: 3},
	} {
		t.Run(name, func(t *testing.T) {
			table, _, view := newTrafficMasterView(t, 4, 2, defaults)
			if err := view.ApplyUpsert(route); err != nil {
				t.Fatal(err)
			}
			got, found := table.Lookup(route.SandboxID)
			if !found || got.EffectiveMaxInflight != defaults {
				t.Fatalf("target effective policy = %+v found=%v, want %+v", got.EffectiveMaxInflight, found, defaults)
			}
		})
	}
}

func TestBareDefaultsConsumeOnlyApplicableServices(t *testing.T) {
	defaults := config.MaxInflight{Total: 8, Forward: 4, E2BEnvd: 3, E2BCodeInterpreter: 2, Exec: 1}
	table, _, view := newTrafficMasterView(t, 4, 1, defaults)
	route := trafficRoute("bare", routesync.StateStarting, nil)
	route.Profile = "bare"
	if err := view.ApplyUpsert(route); err != nil {
		t.Fatal(err)
	}
	got, _ := table.Lookup("bare")
	if got.EffectiveMaxInflight != (config.MaxInflight{Total: 8, Forward: 4, Exec: 1}) {
		t.Fatalf("bare effective policy = %+v", got.EffectiveMaxInflight)
	}
	zero := uint32(0)
	route.MaxInflightPatch = &sandboxcfg.MaxInflightPatch{E2BEnvd: &zero}
	if err := view.ApplyUpsert(route); err == nil {
		t.Fatal("bare explicit e2b service was accepted")
	}
}

func TestAllZeroPolicyHasNoAdmissionBinding(t *testing.T) {
	table, _, view := newTrafficMasterView(t, 4, 8, config.MaxInflight{})
	zero := uint32(0)
	if err := view.ApplyUpsert(trafficRoute("s1", routesync.StateStarting, &sandboxcfg.MaxInflightPatch{Total: &zero})); err != nil {
		t.Fatal(err)
	}
	got, _ := table.Lookup("s1")
	if !got.EffectiveMaxInflight.Unlimited() || got.AdmissionSlot != 0 || got.AdmissionGeneration != 0 {
		t.Fatalf("unlimited route has arena binding: %+v", got)
	}
}

func TestLifecycleStateUpdatesKeepAdmissionGenerationAndPointerIdentityDoesNotChangeRevision(t *testing.T) {
	table, _, view := newTrafficMasterView(t, 4, 2, config.MaxInflight{})
	one := uint32(1)
	route := trafficRoute("s1", routesync.StateStarting, &sandboxcfg.MaxInflightPatch{Total: &one})
	if err := view.ApplyUpsert(route); err != nil {
		t.Fatal(err)
	}
	starting, _, startingRev := table.LookupRevision("s1")

	// A separately parsed pointer with the same value must compare by fixed
	// policy, not pointer identity.
	oneAgain := uint32(1)
	route.MaxInflightPatch = &sandboxcfg.MaxInflightPatch{Total: &oneAgain}
	if err := view.ApplyUpsert(route); err != nil {
		t.Fatal(err)
	}
	_, _, replayRev := table.LookupRevision("s1")
	if replayRev != startingRev {
		t.Fatalf("equal patch pointer advanced revision: %d -> %d", startingRev, replayRev)
	}

	for _, state := range []string{routesync.StateRunning, routesync.StatePaused, routesync.StateStarting} {
		route.State = state
		route.RunID = "run-1"
		if err := view.ApplyUpsert(route); err != nil {
			t.Fatal(err)
		}
		got, _ := table.Lookup("s1")
		if got.AdmissionSlot != starting.AdmissionSlot || got.AdmissionGeneration != starting.AdmissionGeneration {
			t.Fatalf("state %s reset admission: start=%+v got=%+v", state, starting, got)
		}
	}
}

func TestAdmissionTransactionRollsBackWhenRouteSHMUpsertFails(t *testing.T) {
	table, admission, view := newTrafficMasterView(t, 4, 1, config.MaxInflight{})
	one := uint32(1)
	route := trafficRoute("s1", routesync.StateStarting, &sandboxcfg.MaxInflightPatch{Total: &one})
	if err := view.ApplyUpsert(route); err != nil {
		t.Fatal(err)
	}
	old, _ := table.Lookup("s1")
	if err := admission.BeginWorker(0, 1); err != nil {
		t.Fatal(err)
	}
	file, err := admission.DupFile()
	if err != nil {
		t.Fatal(err)
	}
	worker, err := proxyadmission.OpenWorker(file, 4, 1, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer worker.Close()

	injected := errors.New("injected route SHM failure")
	view.upsertPrimary = func(routesync.RouteEntry) error { return injected }
	two := uint32(2)
	route.MaxInflightPatch = &sandboxcfg.MaxInflightPatch{Total: &two}
	if err := view.ApplyUpsert(route); !errors.Is(err, injected) {
		t.Fatalf("ApplyUpsert error = %v", err)
	}
	got, _ := table.Lookup("s1")
	if got != old {
		t.Fatalf("route changed after rollback: old=%+v got=%+v", old, got)
	}
	binding := proxyadmission.Binding{Slot: old.AdmissionSlot, Generation: old.AdmissionGeneration, Limits: old.EffectiveMaxInflight}
	lease, err := worker.TryAcquire(binding, proxyadmission.ServiceForward)
	if err != nil {
		t.Fatalf("old admission was not restored: %v", err)
	}
	lease.Release()
}

func TestAdmissionTransactionRollsBackIdentityReplacementWithOpenLease(t *testing.T) {
	table, admission, view := newTrafficMasterView(t, 4, 1, config.MaxInflight{Total: 1})
	route := trafficRoute("s1", routesync.StateRunning, nil)
	if err := view.ApplyUpsert(route); err != nil {
		t.Fatal(err)
	}
	old, _ := table.Lookup("s1")
	if err := admission.BeginWorker(0, 1); err != nil {
		t.Fatal(err)
	}
	file, err := admission.DupFile()
	if err != nil {
		t.Fatal(err)
	}
	worker, err := proxyadmission.OpenWorker(file, 4, 1, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer worker.Close()
	binding := proxyadmission.Binding{
		Slot: old.AdmissionSlot, Generation: old.AdmissionGeneration, Limits: old.EffectiveMaxInflight,
	}
	held, err := worker.TryAcquire(binding, proxyadmission.ServiceForward)
	if err != nil {
		t.Fatal(err)
	}

	injected := errors.New("injected replacement publish failure")
	view.upsertPrimary = func(routesync.RouteEntry) error { return injected }
	route.ServiceSecret = "replacement-secret"
	if err := view.ApplyUpsert(route); !errors.Is(err, injected) {
		t.Fatalf("ApplyUpsert error = %v", err)
	}
	got, _ := table.Lookup("s1")
	if got != old || !worker.Valid(binding) {
		t.Fatalf("old route/admission was not restored: old=%+v got=%+v", old, got)
	}
	if _, err := worker.TryAcquire(binding, proxyadmission.ServiceForward); !errors.Is(err, proxyadmission.ErrLimitReached) {
		t.Fatalf("rollback lost the old absolute count: %v", err)
	}
	held.Release()
	if next, err := worker.TryAcquire(binding, proxyadmission.ServiceForward); err != nil {
		t.Fatalf("restored binding did not recover after release: %v", err)
	} else {
		next.Release()
	}
}

func TestAdmissionApplyFailureLeavesRouteUnpublished(t *testing.T) {
	table, admission, view := newTrafficMasterView(t, 4, 1, config.MaxInflight{Total: 1})
	if err := admission.Close(); err != nil {
		t.Fatal(err)
	}
	if err := view.ApplyUpsert(trafficRoute("s1", routesync.StateStarting, nil)); err == nil {
		t.Fatal("closed admission arena did not fail route apply")
	}
	if _, found := table.Lookup("s1"); found {
		t.Fatal("route became visible after admission apply failure")
	}
}

func TestMasterApplyWithNoServingWorkersAndIdentityReplacement(t *testing.T) {
	table, _, view := newTrafficMasterView(t, 4, 4, config.MaxInflight{Total: 3})
	route := trafficRoute("s1", routesync.StateStarting, nil)
	if err := view.ApplyUpsert(route); err != nil {
		t.Fatalf("apply with zero serving workers: %v", err)
	}
	old, _ := table.Lookup("s1")
	route.ServiceSecret = "replacement-secret"
	if err := view.ApplyUpsert(route); err != nil {
		t.Fatal(err)
	}
	replacement, _ := table.Lookup("s1")
	if replacement.AdmissionGeneration == old.AdmissionGeneration {
		t.Fatal("identity replacement retained generation")
	}
	old = replacement
	route.TemplateID = "replacement-template"
	if err := view.ApplyUpsert(route); err != nil {
		t.Fatal(err)
	}
	replacement, _ = table.Lookup("s1")
	if replacement.AdmissionGeneration == old.AdmissionGeneration {
		t.Fatal("template identity replacement retained generation")
	}
}

func TestMasterApplyIsIndependentOfWorkerStatsFaultAndRestart(t *testing.T) {
	table, admission, view := newTrafficMasterView(t, 4, 1, config.MaxInflight{Total: 3})
	stats := proxystats.NewMasterStats(metrics.New(), []string{"proxy-0"})
	if err := admission.BeginWorker(0, 1); err != nil {
		t.Fatal(err)
	}
	if err := stats.BeginWorker("proxy-0", 1); err != nil {
		t.Fatal(err)
	}
	stats.StreamFault("proxy-0", 1)
	if err := view.ApplyUpsert(trafficRoute("s1", routesync.StateStarting, nil)); err != nil {
		t.Fatalf("apply during stats fault: %v", err)
	}
	if _, found := table.Lookup("s1"); !found {
		t.Fatal("starting route was not visible after apply")
	}
	stats.WorkerExited("proxy-0", 1)
	if err := admission.ClearWorker(0, 1); err != nil {
		t.Fatal(err)
	}
	if err := admission.BeginWorker(0, 2); err != nil {
		t.Fatal(err)
	}
	if err := stats.BeginWorker("proxy-0", 2); err != nil {
		t.Fatal(err)
	}
	route := trafficRoute("s1", routesync.StateRunning, nil)
	route.RunID = "run-1"
	if err := view.ApplyUpsert(route); err != nil {
		t.Fatalf("apply after worker restart: %v", err)
	}
}

func TestActivationRejectsBindingCapturedBeforePolicyUpdate(t *testing.T) {
	table, admission, master := newTrafficMasterView(t, 4, 1, config.MaxInflight{})
	if err := admission.BeginWorker(0, 1); err != nil {
		t.Fatal(err)
	}
	file, err := admission.DupFile()
	if err != nil {
		t.Fatal(err)
	}
	arenaWorker, err := proxyadmission.OpenWorker(file, 4, 1, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer arenaWorker.Close()
	one := uint32(1)
	route := trafficRoute("s1", routesync.StateRunning, &sandboxcfg.MaxInflightPatch{Total: &one})
	route.FloatingIP = "100.100.0.2"
	master.BeginSync()
	if err := master.ApplyUpsert(route); err != nil {
		t.Fatal(err)
	}
	master.Bookmark()
	worker := NewWorkerView(table, nil, nil, time.Second)
	worker.admission = arenaWorker
	old, found, err := worker.LookupRoute(context.Background(), "s1", proxy.LegacyTarget(8080))
	if err != nil || !found {
		t.Fatalf("LookupRoute found=%v err=%v", found, err)
	}
	if _, found, err := worker.ActivateRoute(context.Background(), old); err != nil || !found {
		t.Fatalf("old policy was not valid before update: found=%v err=%v", found, err)
	}

	two := uint32(2)
	route.MaxInflightPatch = &sandboxcfg.MaxInflightPatch{Total: &two}
	if err := master.ApplyUpsert(route); err != nil {
		t.Fatal(err)
	}
	if _, found, err := worker.ActivateRoute(context.Background(), old); err != nil || found {
		t.Fatalf("ActivateRoute with old policy found=%v err=%v", found, err)
	}
}

func TestActivationRejectsOldGenerationBeforeReplacementRouteIsPublished(t *testing.T) {
	table, admission, master := newTrafficMasterView(t, 4, 1, config.MaxInflight{Total: 1})
	if err := admission.BeginWorker(0, 1); err != nil {
		t.Fatal(err)
	}
	file, err := admission.DupFile()
	if err != nil {
		t.Fatal(err)
	}
	arenaWorker, err := proxyadmission.OpenWorker(file, 4, 1, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer arenaWorker.Close()

	route := trafficRoute("s1", routesync.StateRunning, nil)
	route.FloatingIP = "100.100.0.2"
	master.BeginSync()
	if err := master.ApplyUpsert(route); err != nil {
		t.Fatal(err)
	}
	master.Bookmark()
	worker := NewWorkerView(table, nil, nil, time.Second)
	worker.admission = arenaWorker
	oldRoute, found, err := worker.LookupRoute(context.Background(), "s1", proxy.LegacyTarget(8080))
	if err != nil || !found {
		t.Fatalf("LookupRoute found=%v err=%v", found, err)
	}
	oldExec, found, err := worker.LookupExec(context.Background(), "s1")
	if err != nil || !found {
		t.Fatalf("LookupExec found=%v err=%v", found, err)
	}
	lease, err := arenaWorker.TryAcquire(oldRoute.Admission, proxyadmission.ServiceForward)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()

	entered := make(chan struct{})
	continuePublish := make(chan struct{})
	master.upsertPrimary = func(next routesync.RouteEntry) error {
		close(entered)
		<-continuePublish
		return table.Upsert(next)
	}
	replacement := route
	replacement.ServiceSecret = "replacement-service"
	applyResult := make(chan error, 1)
	go func() { applyResult <- master.ApplyUpsert(replacement) }()
	<-entered

	if _, found, err := worker.ActivateRoute(context.Background(), oldRoute); err != nil || found {
		t.Fatalf("old route crossed drained generation: found=%v err=%v", found, err)
	}
	if _, found, err := worker.ActivateExec(context.Background(), "s1", oldExec); err != nil || found {
		t.Fatalf("old exec crossed drained generation: found=%v err=%v", found, err)
	}
	close(continuePublish)
	if err := <-applyResult; err != nil {
		t.Fatal(err)
	}
}
