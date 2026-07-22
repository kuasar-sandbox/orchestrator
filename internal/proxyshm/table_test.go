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
	master.Bookmark()

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
	tbl.Bookmark()
	tbl.BeginSync()
	if err := tbl.Upsert(routesync.RouteEntry{SandboxID: "s2", State: routesync.StateRunning}); err != nil {
		t.Fatal(err)
	}
	if _, ok := tbl.Lookup("s1"); !ok {
		t.Fatal("route swept before bookmark")
	}
	tbl.Bookmark()
	if _, ok := tbl.Lookup("s1"); ok {
		t.Fatal("route not swept at bookmark")
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
	master.Bookmark()

	done := make(chan proxy.Route, 1)
	go func() {
		route, _ := worker.Route(context.Background(), "s1", 49983)
		done <- route
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	sid, ok := master.NextWake(ctx)
	if !ok || sid != "s1" {
		t.Fatalf("wake = %q ok=%v", sid, ok)
	}
	master.ApplyUpsert(routesync.RouteEntry{
		SandboxID: "s1", Profile: "e2b", State: routesync.StateRunning,
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
	tbl.Bookmark()
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

func TestCurrentRunIDFromSharedTable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "routes.shm")
	tbl, err := Create(path, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer tbl.Close()
	tbl.BeginSync()
	if err := tbl.Upsert(routesync.RouteEntry{
		SandboxID: "s1", State: routesync.StateRunning, RunID: "sr-run-1",
	}); err != nil {
		t.Fatal(err)
	}
	tbl.Bookmark()
	view := NewWorkerView(tbl, nil, nil, time.Second)

	if got, ok := view.CurrentRunID("s1"); !ok || got != "sr-run-1" {
		t.Fatalf("CurrentRunID = %q ok=%v, want sr-run-1", got, ok)
	}
	if _, ok := view.CurrentRunID("unknown"); ok {
		t.Fatal("CurrentRunID found an unknown sandbox")
	}

	// A resume republishes the route with a new RunID (incarnation binding
	// relies on this becoming visible immediately).
	tbl.BeginSync()
	if err := tbl.Upsert(routesync.RouteEntry{
		SandboxID: "s1", State: routesync.StateRunning, RunID: "sr-run-2",
	}); err != nil {
		t.Fatal(err)
	}
	tbl.Bookmark()
	if got, ok := view.CurrentRunID("s1"); !ok || got != "sr-run-2" {
		t.Fatalf("CurrentRunID after resume = %q ok=%v, want sr-run-2", got, ok)
	}
}
