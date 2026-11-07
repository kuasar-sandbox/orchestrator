//go:build linux

package orch

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/proxy"
	"github.com/kuasar-sandbox/orchestrator/internal/proxyshm"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

type connectRouteResult struct {
	sb  *types.Sandbox
	err error
}

func TestConnectWithMMDSImportWaitsForProxyRouteApplication(t *testing.T) {
	f := newBlockedResumeFixture(t)
	installSnapshotRuntimeAdmissionStub(t, f.o, "compatible")
	runtimePath := filepath.Join(t.TempDir(), "runtime.erofs")
	if err := os.WriteFile(runtimePath, []byte("runtime"), 0o644); err != nil {
		t.Fatal(err)
	}
	f.o.cfg.Sandbox.Boot.Runtime = runtimePath
	apiSecret, apiKey := defaultTestCredentials(t, f.sb.ManifestKey)
	if _, err := f.o.st.AddKeyPair(f.ctx, store.KeyPair{APISecret: apiSecret, ManifestKey: f.sb.ManifestKey}, "", 0, ""); err != nil {
		t.Fatal(err)
	}
	token, err := f.o.mintSandboxToken(f.sb, f.sb.ResumeSource)
	if err != nil {
		t.Fatal(err)
	}
	targetID := "imported-route-target"
	targetFixture := f
	targetFixture.sb = &types.Sandbox{ID: targetID}
	ctx, cancel := context.WithCancel(f.ctx)
	barrier := newFakeRouteBarrier(nil, nil)
	f.o.SetProxyRouteBarrierCoordinator(&fakeRouteBarrierCoordinator{barrier: barrier})
	t.Cleanup(func() {
		cancel()
		barrier.release()
		close(f.startGate)
		finishConnectRouteLaunch(t, targetFixture)
	})
	done := make(chan connectRouteResult, 1)
	go func() {
		sb, err := f.o.ConnectWithMMDS(ctx, targetID, apiKey, token, api.ConnectOptions{}, nil, nil)
		done <- connectRouteResult{sb: sb, err: err}
	}()
	select {
	case got := <-done:
		t.Fatalf("MMDS import Connect returned before route ACK: err=%v", got.err)
	case <-barrier.waiting:
	case <-time.After(3 * time.Second):
		t.Fatal("MMDS import Connect neither waited for route ACK nor returned")
	}
	barrier.release()
	select {
	case got := <-done:
		if got.err != nil || got.sb == nil || got.sb.ID != targetID || got.sb.State != types.StateStarting {
			t.Fatalf("MMDS import Connect after route ACK: err=%v", got.err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("MMDS import Connect did not return after route ACK")
	}
}

func TestClusterConnectAcceptanceDoesNotUseStandaloneRouteFence(t *testing.T) {
	f := newClusterConnectFixture(t)
	failingVS := &failingClusterConnectVS{attempted: make(chan struct{}, 1)}
	f.o.vs = failingVS
	f.o.SetProxyRouteBarrierCoordinator(&fakeRouteBarrierCoordinator{err: configsock.ErrProxyRouteUnavailable})
	events, stopEvents := f.o.Subscribe()
	defer stopEvents()
	cmd := f.command("stable-route-scope", f.token)
	ack := f.o.HandleCommand(context.Background(), cmd)
	if ack.Status != routesync.AckAccepted || ack.Connect == nil || ack.Connect.NodeSandboxID != cmd.SID {
		t.Fatalf("cluster accepted import was rejected by the standalone route fence: status=%q HTTP=%d", ack.Status, ack.HTTPStatus)
	}
	waitClusterConnectAttempt(t, failingVS.attempted)
	waitClusterRouteStates(t, events, cmd.SID,
		routesync.StateStarting, routesync.StateStarting, routesync.StatePaused)
}

func TestConnectJoinerWaitsForProxyRouteApplication(t *testing.T) {
	f := newBlockedResumeFixture(t)
	ctx, cancel := context.WithCancel(f.ctx)
	barrier := newFakeRouteBarrier(nil, nil)
	t.Cleanup(func() {
		cancel()
		barrier.release()
		close(f.startGate)
		finishConnectRouteLaunch(t, f)
	})
	if _, err := f.o.Connect(ctx, f.sb.ID, f.apiKey, "", api.ConnectOptions{}); err != nil {
		t.Fatal(err)
	}
	f.o.SetProxyRouteBarrierCoordinator(&fakeRouteBarrierCoordinator{barrier: barrier})
	done := make(chan connectRouteResult, 1)
	go func() {
		sb, err := f.o.Connect(ctx, f.sb.ID, f.apiKey, "", api.ConnectOptions{})
		done <- connectRouteResult{sb: sb, err: err}
	}()
	select {
	case got := <-done:
		t.Fatalf("joining Connect returned before route ACK: err=%v", got.err)
	case <-barrier.waiting:
	case <-time.After(3 * time.Second):
		t.Fatal("joining Connect neither waited for route ACK nor returned")
	}
	barrier.release()
	select {
	case got := <-done:
		if got.err != nil || got.sb == nil || got.sb.State != types.StateStarting {
			t.Fatalf("joining Connect after route ACK: err=%v", got.err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("joining Connect did not return after route ACK")
	}
}

func TestConnectRouteBarrierFailurePreservesAcceptedResume(t *testing.T) {
	for _, phase := range []string{"begin", "wait", "commit", "request-cancel"} {
		t.Run(phase, func(t *testing.T) {
			f := newBlockedResumeFixture(t)
			ctx, cancel := context.WithCancel(f.ctx)
			barrier := newFakeRouteBarrier(nil, nil)
			coordinator := &fakeRouteBarrierCoordinator{barrier: barrier}
			cause := configsock.ErrProxyRouteDisconnected
			switch phase {
			case "begin":
				cause = configsock.ErrProxyRouteUnavailable
				coordinator.err = cause
			case "wait":
				barrier.waitErr = cause
			case "commit":
				cause = configsock.ErrProxyRouteLeaseChanged
				barrier.commitErr = cause
			case "request-cancel":
				cause = context.Canceled
			}
			f.o.SetProxyRouteBarrierCoordinator(coordinator)
			var release sync.Once
			releaseLaunch := func() { release.Do(func() { close(f.startGate) }) }
			t.Cleanup(func() {
				cancel()
				barrier.release()
				releaseLaunch()
				finishConnectRouteLaunch(t, f)
			})
			done := make(chan error, 1)
			go func() {
				_, err := f.o.Connect(ctx, f.sb.ID, f.apiKey, "", api.ConnectOptions{})
				done <- err
			}()
			if phase == "request-cancel" {
				select {
				case <-barrier.waiting:
					cancel()
				case err := <-done:
					t.Fatalf("Connect returned before cancellation at route barrier: %v", err)
				case <-time.After(3 * time.Second):
					t.Fatal("Connect did not reach route barrier")
				}
			} else {
				barrier.release()
			}
			select {
			case err := <-done:
				if !errors.Is(err, api.ErrProxyUnavailable) || !errors.Is(err, cause) {
					t.Fatalf("Connect error = %v, want proxy unavailable with cause %v", err, cause)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("Connect did not return route barrier failure")
			}
			stored, err := f.o.st.Get(f.ctx, f.sb.ID)
			if err != nil || stored == nil || stored.State != types.StateStarting {
				t.Fatalf("accepted resume was rolled back: err=%v", err)
			}
			attempt, found := f.o.launches.Lookup(f.sb.ID)
			if !found || attempt.Context().Err() != nil {
				t.Fatal("route barrier failure canceled the accepted launch")
			}
			releaseLaunch()
			waitCtx, stopWaiting := context.WithTimeout(f.ctx, 3*time.Second)
			defer stopWaiting()
			if err := attempt.wait(waitCtx); err != nil {
				t.Fatalf("accepted resume did not finish after route barrier failure: %v", err)
			}
			stored, err = f.o.st.Get(f.ctx, f.sb.ID)
			if err != nil || stored == nil || stored.State != types.StateRunning {
				t.Fatalf("accepted resume did not reach Running: err=%v", err)
			}
		})
	}
}

type observedConnectRouter struct {
	*proxyshm.WorkerView
	activated chan struct{}
	once      sync.Once
}

func (r *observedConnectRouter) ActivateRoute(ctx context.Context, binding proxy.RouteBinding) (proxy.Route, bool, error) {
	r.once.Do(func() { close(r.activated) })
	return r.WorkerView.ActivateRoute(ctx, binding)
}

func finishConnectRouteLaunch(t *testing.T, f blockedResumeFixture) {
	t.Helper()
	if attempt, found := f.o.launches.Lookup(f.sb.ID); found {
		ctx, cancel := context.WithTimeout(f.ctx, 3*time.Second)
		defer cancel()
		if err := attempt.wait(ctx); err != nil {
			t.Errorf("accepted test launch did not finish during cleanup: %v", err)
		}
	}
}

func TestConnectWaitsForProxyRouteApplication(t *testing.T) {
	f := newBlockedResumeFixture(t)
	ctx, cancel := context.WithCancel(f.ctx)
	barrier := newFakeRouteBarrier(nil, nil)
	f.o.SetProxyRouteBarrierCoordinator(&fakeRouteBarrierCoordinator{barrier: barrier})
	t.Cleanup(func() {
		cancel()
		barrier.release()
		close(f.startGate)
		finishConnectRouteLaunch(t, f)
	})

	table, err := proxyshm.Create(filepath.Join(t.TempDir(), "routes.shm"), 4)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = table.Close() })
	old := f.o.routeEntry(f.sb)
	old.State, old.RunID, old.FloatingIP = routesync.StateRunning, "old-run", "127.0.0.1"
	table.BeginSync()
	if err := table.Upsert(old); err != nil {
		t.Fatal(err)
	}
	table.Bookmark()
	router := &observedConnectRouter{
		WorkerView: proxyshm.NewWorkerView(table, nil, nil, time.Second),
		activated:  make(chan struct{}),
	}

	backend, guest := net.Pipe()
	t.Cleanup(func() { _ = backend.Close(); _ = guest.Close() })
	served := make(chan error, 1)
	go func() {
		defer guest.Close()
		request, err := http.ReadRequest(bufio.NewReader(guest))
		if err == nil {
			var body []byte
			body, err = io.ReadAll(request.Body)
			_ = request.Body.Close()
			if err == nil && (request.Method != http.MethodPost || string(body) != "one-command") {
				err = fmt.Errorf("unexpected request: method=%s body=%q", request.Method, body)
			}
		}
		if err == nil {
			_, err = io.WriteString(guest, "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
		}
		served <- err
	}()
	var oldDials, newDials atomic.Int32
	px := proxy.NewWithDialer(router, nil, nil, nil, func(_ context.Context, route proxy.Route) (net.Conn, error) {
		if route.RunID == old.RunID {
			oldDials.Add(1)
			return nil, &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}
		}
		newDials.Add(1)
		if route.RunID != "new-run" {
			return nil, fmt.Errorf("unexpected run ID %q", route.RunID)
		}
		return backend, nil
	}, "")
	request := func() *http.Request {
		req := httptest.NewRequest(http.MethodPost, "http://sandbox/task", strings.NewReader("one-command")).WithContext(ctx)
		req.Header.Set(proxy.HeaderSandboxID, f.sb.ID)
		req.Header.Set(proxy.HeaderSandboxPort, "8080")
		req.Header.Set(proxy.HeaderAccessToken, f.sb.ForwardAccessToken)
		return req
	}

	events, stopEvents := f.o.Subscribe()
	defer stopEvents()
	done := make(chan connectRouteResult, 1)
	go func() {
		sb, err := f.o.Connect(ctx, f.sb.ID, f.apiKey, "", api.ConnectOptions{})
		done <- connectRouteResult{sb: sb, err: err}
	}()

	select {
	case got := <-done:
		// Model a paused backend disappearing before its ordered route updates
		// reach the proxy. No update is applied until the response fence below.
		result := httptest.NewRecorder()
		px.ServeHTTP(result, request())
		var state types.State
		if got.sb != nil {
			state = got.sb.State
		}
		t.Fatalf("Connect returned before proxy route ACK: state=%v err=%v; first command=%d %q; old-run dials=%d",
			state, got.err, result.Code, result.Body.String(), oldDials.Load())
	case <-barrier.waiting:
	case <-time.After(3 * time.Second):
		t.Fatal("Connect neither waited for proxy route ACK nor returned")
	}

	sawStarting := false
	for {
		var event routesync.Event
		select {
		case event = <-events:
		case <-time.After(3 * time.Second):
			t.Fatal("missing ordered route barrier")
		}
		switch event.Kind {
		case routesync.TypeUpsert:
			if err := table.Upsert(event.Route); err != nil {
				t.Fatal(err)
			}
			if event.Route.SandboxID == f.sb.ID && event.Route.State == routesync.StateStarting {
				sawStarting = true
			}
		case routesync.TypeRouteBarrier:
			if event.BarrierID != barrier.ID() || !sawStarting {
				t.Fatalf("barrier did not follow Starting: %+v", event)
			}
			barrier.release()
			goto acknowledged
		}
	}

acknowledged:
	select {
	case got := <-done:
		if got.err != nil || got.sb == nil || got.sb.State != types.StateStarting {
			t.Fatalf("Connect after route ACK = %+v, %v; runtime is still blocked", got.sb, got.err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Connect did not return after route ACK")
	}
	current, found := table.Lookup(f.sb.ID)
	if !found || current.State != routesync.StateStarting || current.RunID == old.RunID {
		t.Fatalf("Connect exposed obsolete route: %+v, found=%v", current, found)
	}

	result := httptest.NewRecorder()
	requestDone := make(chan struct{})
	go func() {
		defer close(requestDone)
		px.ServeHTTP(result, request())
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-requestDone:
		case <-time.After(3 * time.Second):
			t.Error("first command did not stop during cleanup")
		}
	})
	select {
	case <-router.activated:
	case <-time.After(3 * time.Second):
		t.Fatal("first command did not enter route activation")
	}
	if oldDials.Load() != 0 || newDials.Load() != 0 {
		t.Fatalf("first command dialed before Running: old=%d new=%d", oldDials.Load(), newDials.Load())
	}
	current.State, current.RunID, current.FloatingIP = routesync.StateRunning, "new-run", "127.0.0.2"
	if err := table.Upsert(current); err != nil {
		t.Fatal(err)
	}
	select {
	case <-requestDone:
	case <-time.After(3 * time.Second):
		t.Fatal("first command did not complete after Running")
	}
	if result.Code != http.StatusOK || result.Body.String() != "ok" || oldDials.Load() != 0 || newDials.Load() != 1 {
		t.Fatalf("first command=%d %q; old-run dials=%d new-run dials=%d",
			result.Code, result.Body.String(), oldDials.Load(), newDials.Load())
	}
	if err := <-served; err != nil {
		t.Fatal(err)
	}
}
