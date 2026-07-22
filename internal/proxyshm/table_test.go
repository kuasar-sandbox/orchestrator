package proxyshm

import (
	"context"
	"encoding/hex"
	"path/filepath"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/proxy"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

func TestTableSharedLookupAndDelete(t *testing.T) {
	path := filepath.Join(t.TempDir(), "routes.shm")
	master, err := Create(path, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer master.Close()
	worker, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer worker.Close()

	entry := routesync.RouteEntry{
		SandboxID: "s1", Profile: "e2b", State: routesync.StateRunning,
		EnvdUDS: "/run/s1/envd.sock", FloatingIP: "100.100.0.2", AccessToken: "tok",
	}
	master.BeginSync()
	if err := master.Upsert(entry); err != nil {
		t.Fatal(err)
	}
	master.Bookmark(true)

	got, ok := worker.Lookup("s1")
	if !ok || got.AccessToken != "tok" || got.FloatingIP != "100.100.0.2" {
		t.Fatalf("lookup = %+v ok=%v", got, ok)
	}
	if !master.Delete("s1") {
		t.Fatal("delete returned false")
	}
	if _, ok := worker.Lookup("s1"); ok {
		t.Fatal("worker still sees deleted route")
	}
}

func TestBookmarkSweepsMissingRoutes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "routes.shm")
	tbl, err := Create(path, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer tbl.Close()
	tbl.BeginSync()
	for _, sid := range []string{"s1", "s2"} {
		if err := tbl.Upsert(routesync.RouteEntry{SandboxID: sid, State: routesync.StateRunning}); err != nil {
			t.Fatal(err)
		}
	}
	tbl.Bookmark(true)
	tbl.BeginSync()
	if err := tbl.Upsert(routesync.RouteEntry{SandboxID: "s2", State: routesync.StateRunning}); err != nil {
		t.Fatal(err)
	}
	if _, ok := tbl.Lookup("s1"); !ok {
		t.Fatal("route swept before bookmark")
	}
	tbl.Bookmark(true)
	if _, ok := tbl.Lookup("s1"); ok {
		t.Fatal("route not swept at bookmark")
	}
}

func TestIncrementalBookmarkPreservesUnchangedRoutes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "routes.shm")
	tbl, err := Create(path, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer tbl.Close()
	tbl.BeginSync()
	for _, sid := range []string{"unchanged", "updated"} {
		if err := tbl.Upsert(routesync.RouteEntry{SandboxID: sid, State: routesync.StateRunning}); err != nil {
			t.Fatal(err)
		}
	}
	tbl.Bookmark(true)

	tbl.BeginSync()
	if err := tbl.Upsert(routesync.RouteEntry{SandboxID: "updated", State: routesync.StatePaused}); err != nil {
		t.Fatal(err)
	}
	tbl.Bookmark(false)
	if _, ok := tbl.Lookup("unchanged"); !ok {
		t.Fatal("incremental bookmark removed an unchanged route")
	}
	if got, ok := tbl.Lookup("updated"); !ok || got.State != routesync.StatePaused {
		t.Fatalf("updated route = %+v, ok=%v", got, ok)
	}
}

func TestFullSyncDoesNotRegressCurrentExecutionEvent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "routes.shm")
	tbl, err := Create(path, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer tbl.Close()
	current := routesync.RouteEntry{
		SandboxID: "s1", NodeID: "n1", NodeEpoch: 7, RegistryGeneration: "g1", BindingDigest: "binding",
		EventSeq: 2, Profile: "e2b", State: routesync.StateRunning, AccessToken: "current",
	}
	tbl.BeginSync()
	if err := tbl.Upsert(current); err != nil {
		t.Fatal(err)
	}
	tbl.Bookmark(true)

	tbl.BeginSync()
	stale := current
	stale.EventSeq = 1
	stale.State = routesync.StatePaused
	stale.AccessToken = "stale"
	if err := tbl.Upsert(stale); err != nil {
		t.Fatal(err)
	}
	tbl.Bookmark(true)
	got, ok := tbl.Lookup("s1")
	if !ok || got.EventSeq != 2 || got.State != routesync.StateRunning || got.AccessToken != "current" {
		t.Fatalf("route regressed during full sync: %+v ok=%v", got, ok)
	}
}

func TestManagedDeleteRequiresCurrentExecutionFence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "routes.shm")
	tbl, err := Create(path, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer tbl.Close()
	current := routesync.RouteEntry{
		SandboxID: "s1", NodeID: "n1", NodeEpoch: 7, RegistryGeneration: "g1", BindingDigest: "current",
		EventSeq: 5, Profile: "e2b", State: routesync.StateRunning,
	}
	if err := tbl.Upsert(current); err != nil {
		t.Fatal(err)
	}
	for name, delete := range map[string]routesync.RouteDelete{
		"unfenced":    {SandboxID: "s1"},
		"old binding": {SandboxID: "s1", NodeID: "n1", NodeEpoch: 7, RegistryGeneration: "g1", BindingDigest: "old", EventSeq: 6},
		"old event":   {SandboxID: "s1", NodeID: "n1", NodeEpoch: 7, RegistryGeneration: "g1", BindingDigest: "current", EventSeq: 4},
	} {
		t.Run(name, func(t *testing.T) {
			if tbl.DeleteRoute(delete) {
				t.Fatal("stale delete removed current execution")
			}
			if _, ok := tbl.Lookup("s1"); !ok {
				t.Fatal("current execution disappeared")
			}
		})
	}
	if !tbl.DeleteRoute(routesync.RouteDelete{
		SandboxID: "s1", NodeID: "n1", NodeEpoch: 7, RegistryGeneration: "g1", BindingDigest: "current", EventSeq: 6,
	}) {
		t.Fatal("current execution delete was not applied")
	}
	if _, ok := tbl.Lookup("s1"); ok {
		t.Fatal("deleted execution remains visible")
	}
}

func TestWorkerResolveWakesAndWaitsForSharedUpdate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "routes.shm")
	tbl, err := Create(path, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer tbl.Close()
	master := NewMasterView(tbl, time.Second, nil)
	updates := &Updates{ch: make(chan struct{})}
	worker := NewWorkerView(tbl, updates, master.Wake, 500*time.Millisecond)
	master.BeginSync()
	fence := routesync.RouteEntry{
		SandboxID: "s1", NodeID: "n1", NodeEpoch: 7, RegistryGeneration: "g1", BindingDigest: "d1",
		Profile: "e2b", State: routesync.StatePaused,
	}
	master.ApplyUpsert(fence)
	master.Bookmark(true)

	done := make(chan proxy.Route, 1)
	go func() {
		route, _ := worker.Route(context.Background(), proxy.RouteRequest{
			SandboxID: "s1", Port: 49983, ExpectedNodeID: "n1", ExpectedNodeEpoch: 7,
			ExpectedRegistryGeneration: "g1", ExpectedBindingDigest: "d1",
		})
		done <- route
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	wake, ok := master.NextWake(ctx)
	if !ok || wake != (routesync.RouteWake{SandboxID: "s1", NodeID: "n1", NodeEpoch: 7, RegistryGeneration: "g1", BindingDigest: "d1"}) {
		t.Fatalf("wake = %+v ok=%v", wake, ok)
	}
	master.ApplyUpsert(routesync.RouteEntry{
		SandboxID: "s1", NodeID: "n1", NodeEpoch: 7, RegistryGeneration: "g1", BindingDigest: "d1",
		Profile: "e2b", State: routesync.StateRunning,
		EnvdUDS: "/run/s1/envd.sock", AccessToken: "tok",
	})
	updates.bump()
	select {
	case route := <-done:
		if route.Kind != proxy.KindUDS || route.UDS == "" || route.AccessToken != "tok" {
			t.Fatalf("route = %+v", route)
		}
	case <-time.After(time.Second):
		t.Fatal("worker did not unpark")
	}
}

func TestWorkerRejectsStaleFenceWithoutWake(t *testing.T) {
	path := filepath.Join(t.TempDir(), "routes.shm")
	tbl, err := Create(path, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer tbl.Close()
	master := NewMasterView(tbl, time.Second, nil)
	worker := NewWorkerView(tbl, nil, master.Wake, time.Second)
	master.BeginSync()
	master.ApplyUpsert(routesync.RouteEntry{
		SandboxID: "s1", NodeID: "n1", NodeEpoch: 8, RegistryGeneration: "g2", BindingDigest: "new",
		Profile: "e2b", State: routesync.StatePaused,
	})
	master.Bookmark(true)

	route, err := worker.Route(context.Background(), proxy.RouteRequest{
		SandboxID: "s1", Port: 49983, ExpectedNodeID: "n1", ExpectedNodeEpoch: 7,
		ExpectedRegistryGeneration: "g1", ExpectedBindingDigest: "old",
	})
	if err != nil || route.Kind != proxy.KindWrongNodeEpoch {
		t.Fatalf("route = %+v err=%v", route, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if wake, ok := master.NextWake(ctx); ok {
		t.Fatalf("stale route emitted wake %+v", wake)
	}
}

func TestWorkerBindingChangeWhileParkedFailsClosed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "routes.shm")
	tbl, err := Create(path, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer tbl.Close()
	master := NewMasterView(tbl, time.Second, nil)
	updates := &Updates{ch: make(chan struct{})}
	worker := NewWorkerView(tbl, updates, master.Wake, time.Second)
	master.BeginSync()
	master.ApplyUpsert(routesync.RouteEntry{
		SandboxID: "s1", NodeID: "n1", NodeEpoch: 7, RegistryGeneration: "g1", BindingDigest: "old",
		Profile: "e2b", State: routesync.StatePaused,
	})
	master.Bookmark(true)

	done := make(chan proxy.Route, 1)
	go func() {
		route, _ := worker.Route(context.Background(), proxy.RouteRequest{
			SandboxID: "s1", Port: 49983, ExpectedNodeID: "n1", ExpectedNodeEpoch: 7,
			ExpectedRegistryGeneration: "g1", ExpectedBindingDigest: "old",
		})
		done <- route
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, ok := master.NextWake(ctx); !ok {
		t.Fatal("parked route did not emit wake")
	}
	master.ApplyUpsert(routesync.RouteEntry{
		SandboxID: "s1", NodeID: "n1", NodeEpoch: 7, RegistryGeneration: "g1", BindingDigest: "new",
		Profile: "e2b", State: routesync.StateRunning, EnvdUDS: "/run/s1/envd.sock",
	})
	updates.bump()
	select {
	case route := <-done:
		if route.Kind != proxy.KindWrongBinding {
			t.Fatalf("route after rebind = %+v", route)
		}
	case <-time.After(time.Second):
		t.Fatal("parked route did not fail after rebind")
	}
}

func TestWorkerRechecksRouteAtParkDeadline(t *testing.T) {
	path := filepath.Join(t.TempDir(), "routes.shm")
	tbl, err := Create(path, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer tbl.Close()
	master := NewMasterView(tbl, 30*time.Millisecond, nil)
	updates := &Updates{ch: make(chan struct{})}
	woke := make(chan struct{}, 1)
	worker := NewWorkerView(tbl, updates, func(routesync.RouteWake) { woke <- struct{}{} }, 30*time.Millisecond)
	master.BeginSync()
	entry := routesync.RouteEntry{
		SandboxID: "s1", NodeID: "n1", NodeEpoch: 7, RegistryGeneration: "g1", BindingDigest: "binding",
		EventSeq: 1, Profile: "e2b", State: routesync.StatePaused,
	}
	master.ApplyUpsert(entry)
	master.Bookmark(true)

	done := make(chan proxy.Route, 1)
	go func() {
		route, _ := worker.Route(context.Background(), proxy.RouteRequest{
			SandboxID: "s1", Port: 49983, ExpectedNodeID: "n1", ExpectedNodeEpoch: 7,
			ExpectedRegistryGeneration: "g1", ExpectedBindingDigest: "binding",
		})
		done <- route
	}()
	select {
	case <-woke:
	case <-time.After(time.Second):
		t.Fatal("worker did not park")
	}
	entry.EventSeq = 2
	entry.State = routesync.StateRunning
	entry.EnvdUDS = "/run/s1/envd.sock"
	master.ApplyUpsert(entry)
	// Deliberately omit updates.bump: the deadline path must still observe the
	// committed shared-table update in its final read.
	select {
	case route := <-done:
		if route.Kind != proxy.KindUDS || route.UDS == "" {
			t.Fatalf("route at park deadline = %+v", route)
		}
	case <-time.After(time.Second):
		t.Fatal("worker did not finish at park deadline")
	}
}

func TestMMDSSourceFromSharedTable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "routes.shm")
	tbl, err := Create(path, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer tbl.Close()
	secret := []byte("secret")
	tbl.BeginSync()
	if err := tbl.Upsert(routesync.RouteEntry{
		SandboxID: "s1", State: routesync.StateRunning, TemplateID: "tmpl",
		FloatingIP: "100.100.0.3", AccessToken: "tok", MmdsSecret: hex.EncodeToString(secret),
	}); err != nil {
		t.Fatal(err)
	}
	tbl.Bookmark(true)
	view := NewWorkerView(tbl, nil, nil, time.Second)
	if sid, ok := view.ByFloatingIP("100.100.0.3"); !ok || sid != "s1" {
		t.Fatalf("ByFloatingIP = %q ok=%v", sid, ok)
	}
	if tid, tok, ok := view.SandboxInfo("s1"); !ok || tid != "tmpl" || tok != "tok" {
		t.Fatalf("SandboxInfo = %q %q ok=%v", tid, tok, ok)
	}
	if got, ok := view.MmdsSecret("s1"); !ok || string(got) != string(secret) {
		t.Fatalf("MmdsSecret = %x ok=%v", got, ok)
	}
}
