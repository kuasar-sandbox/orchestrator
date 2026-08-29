package proxyshm

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/proxy"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// A paused sandbox whose resume source is a Sandbox artifact (E) is a cold
// start owned by explicit Connect: activation must fail fast with
// proxy.ErrColdSandbox and never emit a Wake. Snapshot-kind paused routes keep
// the historical auto-resume-on-traffic behavior.
func TestWorkerActivationNeverWakesColdSandboxSource(t *testing.T) {
	tbl := newExecTable(t)
	for _, route := range []routesync.RouteEntry{
		{
			SandboxID: "cold", AuthSandboxID: "stable-cold", Profile: "e2b", State: routesync.StatePaused,
			EnvdUDS: "/run/cold/old.sock", EnvdAccessToken: "envd", ForwardAccessToken: "forward",
			ServiceSecret: workerExecServiceSecret,
			ResumeKind:    types.ResumeSandbox,
		},
		{
			SandboxID: "snapshot-paused", AuthSandboxID: "stable-snapshot-paused", Profile: "e2b", State: routesync.StatePaused,
			EnvdUDS: "/run/snapshot-paused/old.sock", EnvdAccessToken: "envd", ForwardAccessToken: "forward",
			ServiceSecret: workerExecServiceSecret,
			ResumeKind:    types.ResumeSnapshot,
		},
	} {
		if err := tbl.Upsert(route); err != nil {
			t.Fatal(err)
		}
	}
	tbl.Bookmark()

	var wakes atomic.Int32
	view := NewWorkerView(tbl, nil, func(string) { wakes.Add(1) }, time.Second)

	coldBinding, found, err := view.LookupRoute(context.Background(), "cold", proxy.LegacyTarget(49983))
	if err != nil || !found {
		t.Fatalf("LookupRoute(cold) = %+v, %v, %v", coldBinding, found, err)
	}
	if _, _, err := view.ActivateRoute(context.Background(), coldBinding); !errors.Is(err, proxy.ErrColdSandbox) {
		t.Fatalf("ActivateRoute(cold) err = %v, want ErrColdSandbox", err)
	}
	_, _, err = view.ActivateExec(context.Background(), "cold", execWorkerIdentity("cold"))
	if !errors.Is(err, proxy.ErrColdSandbox) {
		t.Fatalf("ActivateExec(cold) err = %v, want ErrColdSandbox", err)
	}

	kind, found, err := view.LookupResumeKind(context.Background(), "cold")
	if err != nil || !found || kind != types.ResumeSandbox {
		t.Fatalf("LookupResumeKind(paused cold) = %q, %v, %v, want ResumeSandbox", kind, found, err)
	}

	// When the route transitions to starting or running, LookupResumeKind must
	// return empty kind so exec requests proceed to normal live handling.
	if err := tbl.Upsert(routesync.RouteEntry{
		SandboxID: "cold", AuthSandboxID: "stable-cold", Profile: "e2b", State: routesync.StateRunning,
		EnvdUDS: "/run/cold/ctl.sock", EnvdAccessToken: "envd", ForwardAccessToken: "forward",
		ServiceSecret: workerExecServiceSecret,
		ResumeKind:    types.ResumeSandbox,
	}); err != nil {
		t.Fatal(err)
	}
	tbl.Bookmark()
	kind, found, err = view.LookupResumeKind(context.Background(), "cold")
	if err != nil || !found || kind != "" {
		t.Fatalf("LookupResumeKind(running) = %q, %v, %v, want empty kind", kind, found, err)
	}

	// Snapshot-kind paused routes keep the historical auto-resume behavior:
	// activation wakes and parks (no cold error).
	snapshotBinding, found, err := view.LookupRoute(context.Background(), "snapshot-paused", proxy.LegacyTarget(49983))
	if err != nil || !found {
		t.Fatalf("LookupRoute(snapshot-paused) = %+v, %v, %v", snapshotBinding, found, err)
	}
	if _, _, err := view.ActivateExec(context.Background(), "snapshot-paused", execWorkerIdentity("snapshot-paused")); errors.Is(err, proxy.ErrColdSandbox) {
		t.Fatal("snapshot-kind paused route rejected as cold")
	}
	if got := wakes.Load(); got != 1 {
		t.Fatalf("wakes = %d, want exactly the snapshot-kind wake", got)
	}
}
