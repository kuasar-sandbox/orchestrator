package routetable

import (
	"context"
	"testing"
	"time"

	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/routesync"
)

func running(sid string) routesync.RouteEntry {
	return routesync.RouteEntry{SandboxID: sid, Profile: "e2b", State: routesync.StateRunning, EnvdUDS: "/run/" + sid + ".sock"}
}

func TestApplyAndLookup(t *testing.T) {
	tbl := New(time.Second)
	tbl.ApplySnapshot([]routesync.RouteEntry{running("s1")})
	if r, ok := tbl.Lookup("s1"); !ok || r.State != routesync.StateRunning {
		t.Fatalf("snapshot lookup s1 = %+v ok=%v", r, ok)
	}
	tbl.ApplyUpsert(routesync.RouteEntry{SandboxID: "s1", State: routesync.StatePaused})
	if r, _ := tbl.Lookup("s1"); r.State != routesync.StatePaused {
		t.Fatalf("after upsert paused, state = %q", r.State)
	}
	tbl.ApplyDelete("s1")
	if _, ok := tbl.Lookup("s1"); ok {
		t.Fatal("s1 still present after delete")
	}
}

func TestResolveImmediate(t *testing.T) {
	tbl := New(time.Second)
	tbl.ApplySnapshot([]routesync.RouteEntry{running("s1")})
	r, ok := tbl.Resolve(context.Background(), "s1")
	if !ok || r.EnvdUDS == "" {
		t.Fatalf("resolve running = %+v ok=%v", r, ok)
	}
}

func TestResolveParksThenResumes(t *testing.T) {
	tbl := New(2 * time.Second)
	tbl.ApplySnapshot(nil) // synced, but s1 absent

	got := make(chan bool, 1)
	go func() {
		_, ok := tbl.Resolve(context.Background(), "s1")
		got <- ok
	}()

	// Resolve should have prompted a Wake for the missing sandbox.
	sid, ok := tbl.NextWake(context.Background())
	if !ok || sid != "s1" {
		t.Fatalf("expected wake for s1, got %q ok=%v", sid, ok)
	}
	// Simulate the orchestrator resuming it: push the running route.
	tbl.ApplyUpsert(running("s1"))

	select {
	case ok := <-got:
		if !ok {
			t.Fatal("resolve returned not-ok after resume")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("resolve did not unpark after upsert")
	}
}

func TestResolveTimeout(t *testing.T) {
	tbl := New(150 * time.Millisecond)
	tbl.ApplySnapshot(nil) // synced, s1 never appears
	start := time.Now()
	_, ok := tbl.Resolve(context.Background(), "s1")
	if ok {
		t.Fatal("resolve should time out for an absent sandbox")
	}
	if d := time.Since(start); d < 100*time.Millisecond {
		t.Fatalf("resolve returned too fast (%v); did not park", d)
	}
	// A wake should still have been emitted (drain it).
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if sid, ok := tbl.NextWake(ctx); !ok || sid != "s1" {
		t.Fatalf("expected wake for s1 on timeout path, got %q ok=%v", sid, ok)
	}
}

func TestWakeDedup(t *testing.T) {
	tbl := New(time.Second)
	tbl.Wake("s1")
	tbl.Wake("s1") // deduped while still queued
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if sid, ok := tbl.NextWake(ctx); !ok || sid != "s1" {
		t.Fatalf("first NextWake = %q ok=%v", sid, ok)
	}
	// No second queued wake.
	ctx2, cancel2 := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel2()
	if sid, ok := tbl.NextWake(ctx2); ok {
		t.Fatalf("unexpected second wake %q", sid)
	}
}

func TestWaitSyncedTimeout(t *testing.T) {
	tbl := New(120 * time.Millisecond)
	// Never synced: Resolve should fail fast at the sync gate.
	_, ok := tbl.Resolve(context.Background(), "s1")
	if ok {
		t.Fatal("resolve should fail before first snapshot")
	}
}
