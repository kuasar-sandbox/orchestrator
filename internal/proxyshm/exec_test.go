package proxyshm

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/proxy"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

const workerExecServiceSecret = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func TestWorkerLookupExecIsSideEffectFreeAndRequiresCompleteLiveIdentity(t *testing.T) {
	tbl := newExecTable(t)
	for _, route := range []routesync.RouteEntry{
		execWorkerRoute("starting", routesync.StateStarting),
		execWorkerRoute("paused", routesync.StatePaused),
		execWorkerRoute("running", routesync.StateRunning),
		execWorkerRoute("dead", routesync.StateDead),
		{SandboxID: "missing-auth", State: routesync.StatePaused, ServiceSecret: workerExecServiceSecret},
		{SandboxID: "missing-secret", State: routesync.StateRunning, AuthSandboxID: "stable-missing-secret"},
	} {
		if err := tbl.Upsert(route); err != nil {
			t.Fatal(err)
		}
	}
	tbl.Bookmark()

	var wakes atomic.Int32
	view := NewWorkerView(tbl, nil, func(string) { wakes.Add(1) }, time.Second)
	for _, sid := range []string{"starting", "paused", "running"} {
		got, found, err := view.LookupExec(context.Background(), sid)
		if err != nil || !found {
			t.Fatalf("LookupExec(%q) = %+v, %v, %v", sid, got, found, err)
		}
		want := execWorkerIdentity(sid)
		if got != want {
			t.Fatalf("LookupExec(%q) identity = %+v, want %+v", sid, got, want)
		}
	}
	for _, sid := range []string{"dead", "missing-auth", "missing-secret"} {
		got, found, err := view.LookupExec(context.Background(), sid)
		if err != nil || found || got != (proxy.ExecIdentity{}) {
			t.Fatalf("LookupExec(%q) = %+v, %v, %v, want absent", sid, got, found, err)
		}
	}
	if got := wakes.Load(); got != 0 {
		t.Fatalf("side-effect-free lookups emitted %d wakes", got)
	}
	paused, ok := tbl.Lookup("paused")
	if !ok || paused.State != routesync.StatePaused {
		t.Fatalf("paused route changed during lookup: %+v ok=%v", paused, ok)
	}
}

func TestWorkerLookupExecParksForInitialRouteWithoutWake(t *testing.T) {
	tbl := newExecTable(t)
	tbl.Bookmark()
	updates := &Updates{ch: make(chan struct{})}
	var wakes atomic.Int32
	view := NewWorkerView(tbl, updates, func(string) { wakes.Add(1) }, time.Second)

	type result struct {
		identity proxy.ExecIdentity
		found    bool
		err      error
	}
	done := make(chan result, 1)
	sid := "initial-route"
	go func() {
		identity, found, err := view.LookupExec(context.Background(), sid)
		done <- result{identity: identity, found: found, err: err}
	}()
	select {
	case got := <-done:
		t.Fatalf("missing initial route returned before propagation: %+v", got)
	case <-time.After(20 * time.Millisecond):
	}

	route := execWorkerRoute(sid, routesync.StateStarting)
	if err := tbl.Upsert(route); err != nil {
		t.Fatal(err)
	}
	updates.bump()
	select {
	case got := <-done:
		if got.err != nil || !got.found || got.identity != execWorkerIdentity(sid) {
			t.Fatalf("LookupExec after starting propagation = %+v, %v, %v", got.identity, got.found, got.err)
		}
	case <-time.After(time.Second):
		t.Fatal("LookupExec did not observe the initial starting route")
	}
	if got := wakes.Load(); got != 0 {
		t.Fatalf("identity propagation wait emitted %d wakes", got)
	}
}

func TestWorkerLookupExecInitialRouteWaitIsCancelable(t *testing.T) {
	tbl := newExecTable(t)
	tbl.Bookmark()
	updates := &Updates{ch: make(chan struct{})}
	var wakes atomic.Int32
	view := NewWorkerView(tbl, updates, func(string) { wakes.Add(1) }, time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	type result struct {
		identity proxy.ExecIdentity
		found    bool
		err      error
	}
	done := make(chan result, 1)
	go func() {
		identity, found, err := view.LookupExec(ctx, "cancel-initial-route")
		done <- result{identity: identity, found: found, err: err}
	}()
	select {
	case got := <-done:
		t.Fatalf("missing initial route returned before cancellation: %+v", got)
	case <-time.After(20 * time.Millisecond):
	}
	cancel()
	select {
	case got := <-done:
		if !errors.Is(got.err, context.Canceled) || got.found || got.identity != (proxy.ExecIdentity{}) {
			t.Fatalf("canceled LookupExec = %+v, %v, %v", got.identity, got.found, got.err)
		}
	case <-time.After(time.Second):
		t.Fatal("LookupExec did not observe caller cancellation")
	}
	if got := wakes.Load(); got != 0 {
		t.Fatalf("canceled identity wait emitted %d wakes", got)
	}
}

func TestWorkerLookupExecInitialDeleteEndsPromptly(t *testing.T) {
	tbl := newExecTable(t)
	tbl.Bookmark()
	updates := &Updates{ch: make(chan struct{})}
	var wakes atomic.Int32
	view := NewWorkerView(tbl, updates, func(string) { wakes.Add(1) }, time.Second)

	type result struct {
		identity proxy.ExecIdentity
		found    bool
		err      error
	}
	done := make(chan result, 1)
	sid := "initial-delete"
	go func() {
		identity, found, err := view.LookupExec(context.Background(), sid)
		done <- result{identity: identity, found: found, err: err}
	}()
	select {
	case got := <-done:
		t.Fatalf("missing initial route returned before terminal update: %+v", got)
	case <-time.After(20 * time.Millisecond):
	}
	tbl.Delete(sid)
	updates.bump()
	select {
	case got := <-done:
		if got.err != nil || got.found || got.identity != (proxy.ExecIdentity{}) {
			t.Fatalf("LookupExec after Delete = %+v, %v, %v", got.identity, got.found, got.err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("LookupExec consumed its park timeout after Delete")
	}
	if got := wakes.Load(); got != 0 {
		t.Fatalf("terminal identity wait emitted %d wakes", got)
	}
}

func TestWorkerActivateExecWaitsForStartingWithoutWake(t *testing.T) {
	tbl := newExecTable(t)
	route := execWorkerRoute("starting", routesync.StateStarting)
	if err := tbl.Upsert(route); err != nil {
		t.Fatal(err)
	}
	tbl.Bookmark()

	updates := &Updates{ch: make(chan struct{})}
	var wakes atomic.Int32
	view := NewWorkerView(tbl, updates, func(string) { wakes.Add(1) }, time.Second)
	expected := execWorkerIdentity(route.SandboxID)
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	identity, found, err := view.ActivateExec(canceled, route.SandboxID, expected)
	if !errors.Is(err, context.Canceled) || found || identity != (proxy.ExecIdentity{}) {
		t.Fatalf("canceled starting activation = %+v, %v, %v", identity, found, err)
	}
	if got := wakes.Load(); got != 0 {
		t.Fatalf("starting activation emitted %d wakes", got)
	}

	type result struct {
		identity proxy.ExecIdentity
		found    bool
		err      error
	}
	done := make(chan result, 1)
	go func() {
		identity, found, err := view.ActivateExec(context.Background(), route.SandboxID, expected)
		done <- result{identity: identity, found: found, err: err}
	}()
	select {
	case got := <-done:
		t.Fatalf("starting activation returned before running update: %+v", got)
	case <-time.After(20 * time.Millisecond):
	}
	route.State = routesync.StateRunning
	if err := tbl.Upsert(route); err != nil {
		t.Fatal(err)
	}
	updates.bump()
	select {
	case got := <-done:
		if got.err != nil || !got.found || got.identity != expected {
			t.Fatalf("starting activation after running = %+v, %v, %v", got.identity, got.found, got.err)
		}
	case <-time.After(time.Second):
		t.Fatal("starting activation did not observe running update")
	}
	if got := wakes.Load(); got != 0 {
		t.Fatalf("starting activation emitted %d wakes while waiting", got)
	}
}

func TestWorkerActivateExecStartingRollbackReturnsAbsent(t *testing.T) {
	for _, rollback := range []string{routesync.StatePaused, routesync.StateDead} {
		t.Run(rollback, func(t *testing.T) {
			tbl := newExecTable(t)
			route := execWorkerRoute("starting-rollback", routesync.StateStarting)
			if err := tbl.Upsert(route); err != nil {
				t.Fatal(err)
			}
			tbl.Bookmark()
			updates := &Updates{ch: make(chan struct{})}
			var wakes atomic.Int32
			view := NewWorkerView(tbl, updates, func(string) { wakes.Add(1) }, time.Second)
			type result struct {
				identity proxy.ExecIdentity
				found    bool
				err      error
			}
			done := make(chan result, 1)
			expected := execWorkerIdentity(route.SandboxID)
			go func() {
				identity, found, err := view.ActivateExec(context.Background(), route.SandboxID, expected)
				done <- result{identity: identity, found: found, err: err}
			}()
			select {
			case got := <-done:
				t.Fatalf("starting exec returned before rollback: %+v", got)
			case <-time.After(20 * time.Millisecond):
			}
			if rollback == routesync.StateDead {
				tbl.Delete(route.SandboxID)
			} else {
				route.State = rollback
				if err := tbl.Upsert(route); err != nil {
					t.Fatal(err)
				}
			}
			updates.bump()
			select {
			case got := <-done:
				if got.err != nil || got.found || got.identity != (proxy.ExecIdentity{}) {
					t.Fatalf("starting exec after %s rollback = %+v, %v, %v", rollback, got.identity, got.found, got.err)
				}
			case <-time.After(time.Second):
				t.Fatalf("starting exec did not stop waiting after %s rollback", rollback)
			}
			if got := wakes.Load(); got != 0 {
				t.Fatalf("starting exec rollback emitted %d wakes", got)
			}
		})
	}
}

func TestWorkerActivateExecWakesPausedAndReturnsOnlyMatchingRunningIdentity(t *testing.T) {
	tbl := newExecTable(t)
	route := execWorkerRoute("paused", routesync.StatePaused)
	if err := tbl.Upsert(route); err != nil {
		t.Fatal(err)
	}
	tbl.Bookmark()

	updates := &Updates{ch: make(chan struct{})}
	wakes := make(chan string, 1)
	view := NewWorkerView(tbl, updates, func(sid string) { wakes <- sid }, time.Second)
	type result struct {
		identity proxy.ExecIdentity
		found    bool
		err      error
	}
	done := make(chan result, 1)
	expected := execWorkerIdentity(route.SandboxID)
	go func() {
		identity, found, err := view.ActivateExec(context.Background(), route.SandboxID, expected)
		done <- result{identity: identity, found: found, err: err}
	}()

	select {
	case sid := <-wakes:
		if sid != route.SandboxID {
			t.Fatalf("wake sid = %q, want %q", sid, route.SandboxID)
		}
	case <-time.After(time.Second):
		t.Fatal("authorized paused activation did not wake sandbox")
	}
	route.State = routesync.StateStarting
	if err := tbl.Upsert(route); err != nil {
		t.Fatal(err)
	}
	updates.bump()
	select {
	case got := <-done:
		t.Fatalf("activation returned at starting: %+v", got)
	default:
	}
	route.State = routesync.StateRunning
	if err := tbl.Upsert(route); err != nil {
		t.Fatal(err)
	}
	updates.bump()

	select {
	case got := <-done:
		if got.err != nil || !got.found || got.identity != expected {
			t.Fatalf("ActivateExec() = %+v, %v, %v, want matching running identity", got.identity, got.found, got.err)
		}
	case <-time.After(time.Second):
		t.Fatal("activation did not observe running update")
	}
}

func TestWorkerActivateExecRejectsMismatchWithoutWakeAndDriftAfterWake(t *testing.T) {
	t.Run("already running", func(t *testing.T) {
		tbl := newExecTable(t)
		route := execWorkerRoute("running", routesync.StateRunning)
		if err := tbl.Upsert(route); err != nil {
			t.Fatal(err)
		}
		tbl.Bookmark()
		var wakes atomic.Int32
		view := NewWorkerView(tbl, nil, func(string) { wakes.Add(1) }, time.Second)
		expected := execWorkerIdentity(route.SandboxID)
		got, found, err := view.ActivateExec(context.Background(), route.SandboxID, expected)
		if err != nil || !found || got != expected {
			t.Fatalf("ActivateExec(running) = %+v, %v, %v", got, found, err)
		}
		if wakes.Load() != 0 {
			t.Fatal("running sandbox was woken")
		}
	})

	t.Run("mismatched expected identity", func(t *testing.T) {
		tbl := newExecTable(t)
		route := execWorkerRoute("mismatch", routesync.StatePaused)
		if err := tbl.Upsert(route); err != nil {
			t.Fatal(err)
		}
		tbl.Bookmark()
		var wakes atomic.Int32
		view := NewWorkerView(tbl, nil, func(string) { wakes.Add(1) }, 50*time.Millisecond)
		expected := execWorkerIdentity(route.SandboxID)
		expected.AuthSandboxID = "different-lineage"
		got, found, err := view.ActivateExec(context.Background(), route.SandboxID, expected)
		if err != nil || found || got != (proxy.ExecIdentity{}) {
			t.Fatalf("ActivateExec(mismatch) = %+v, %v, %v", got, found, err)
		}
		if wakes.Load() != 0 {
			t.Fatal("mismatched identity woke paused sandbox")
		}
	})

	t.Run("identity changes while resuming", func(t *testing.T) {
		tbl := newExecTable(t)
		route := execWorkerRoute("drift", routesync.StatePaused)
		if err := tbl.Upsert(route); err != nil {
			t.Fatal(err)
		}
		tbl.Bookmark()
		updates := &Updates{ch: make(chan struct{})}
		wakes := make(chan string, 1)
		view := NewWorkerView(tbl, updates, func(sid string) { wakes <- sid }, time.Second)
		type result struct {
			identity proxy.ExecIdentity
			found    bool
			err      error
		}
		done := make(chan result, 1)
		expected := execWorkerIdentity(route.SandboxID)
		go func() {
			identity, found, err := view.ActivateExec(context.Background(), route.SandboxID, expected)
			done <- result{identity: identity, found: found, err: err}
		}()
		select {
		case <-wakes:
		case <-time.After(time.Second):
			t.Fatal("activation did not wake paused sandbox")
		}
		route.State = routesync.StateRunning
		route.AuthSandboxID = "replacement-lineage"
		if err := tbl.Upsert(route); err != nil {
			t.Fatal(err)
		}
		updates.bump()
		select {
		case got := <-done:
			if got.err != nil || got.found || got.identity != (proxy.ExecIdentity{}) {
				t.Fatalf("ActivateExec(identity drift) = %+v, %v, %v", got.identity, got.found, got.err)
			}
		case <-time.After(time.Second):
			t.Fatal("activation did not reject identity drift")
		}
	})

	t.Run("authorized wake times out", func(t *testing.T) {
		tbl := newExecTable(t)
		route := execWorkerRoute("timeout", routesync.StatePaused)
		if err := tbl.Upsert(route); err != nil {
			t.Fatal(err)
		}
		tbl.Bookmark()
		var wakes atomic.Int32
		view := NewWorkerView(tbl, nil, func(string) { wakes.Add(1) }, time.Millisecond)
		expected := execWorkerIdentity(route.SandboxID)
		got, found, err := view.ActivateExec(context.Background(), route.SandboxID, expected)
		if !errors.Is(err, errExecActivationTimeout) || found || got != (proxy.ExecIdentity{}) {
			t.Fatalf("ActivateExec(timeout) = %+v, %v, %v", got, found, err)
		}
		if wakes.Load() != 1 {
			t.Fatalf("authorized timeout wakes = %d, want 1", wakes.Load())
		}
	})
}

func TestExternalExecInvalidKATDoesNotWakeOrDial(t *testing.T) {
	tbl := newExecTable(t)
	route := execWorkerRoute("external", routesync.StatePaused)
	if err := tbl.Upsert(route); err != nil {
		t.Fatal(err)
	}
	tbl.Bookmark()
	var wakes atomic.Int32
	var dials atomic.Int32
	view := NewWorkerView(tbl, nil, func(string) { wakes.Add(1) }, time.Second)
	px := proxy.NewWithDialer(view, func() string { return "off" }, nil, nil,
		func(context.Context, proxy.Route) (net.Conn, error) {
			dials.Add(1)
			return nil, fmt.Errorf("unexpected dial")
		}, t.TempDir())
	req := httptest.NewRequest(http.MethodConnect, "http://sandbox:443", nil)
	req.Host = "sandbox:443"
	req.Header.Set(proxy.HeaderSandboxID, route.SandboxID)
	req.Header.Set(proxy.HeaderSandboxService, string(proxy.ConnectServiceExec))
	req.Header.Set(proxy.HeaderAccessToken, "not-a-kat")
	resp := httptest.NewRecorder()
	px.ServeHTTP(resp, req)
	if resp.Code != http.StatusUnauthorized {
		t.Fatalf("invalid KAT response = %d, want 401", resp.Code)
	}
	if wakes.Load() != 0 || dials.Load() != 0 {
		t.Fatalf("invalid KAT caused side effects: wakes=%d dials=%d", wakes.Load(), dials.Load())
	}
}

func newExecTable(t *testing.T) *Table {
	t.Helper()
	path := filepath.Join(t.TempDir(), "routes.shm")
	tbl, err := Create(path, 16)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tbl.Close() })
	tbl.BeginSync()
	return tbl
}

func execWorkerRoute(sid, state string) routesync.RouteEntry {
	return routesync.RouteEntry{
		SandboxID:     sid,
		State:         state,
		AuthSandboxID: "stable-" + sid,
		ServiceSecret: workerExecServiceSecret,
	}
}

func execWorkerIdentity(sid string) proxy.ExecIdentity {
	return proxy.ExecIdentity{
		NodeSandboxID: sid,
		AuthSandboxID: "stable-" + sid,
		ServiceSecret: workerExecServiceSecret,
	}
}
