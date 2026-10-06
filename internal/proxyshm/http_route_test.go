//go:build linux

package proxyshm

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/config"
	"github.com/kuasar-sandbox/orchestrator/internal/proxy"
	"github.com/kuasar-sandbox/orchestrator/internal/proxyadmission"
	"github.com/kuasar-sandbox/orchestrator/internal/proxystats"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
)

type httpRouteFixture struct {
	table   *Table
	updates *Updates
	view    *WorkerView
	arena   *proxyadmission.Worker
	owner   *proxyadmission.Master
	master  *MasterView
	stats   *httpRouteStats
	entry   routesync.RouteEntry
}

type httpRouteStats struct {
	*proxystats.WorkerStats
	begins atomic.Int32
}

func (s *httpRouteStats) BeginParking(sid string, service proxy.ConnectService) proxy.TrafficFlow {
	s.begins.Add(1)
	return s.WorkerStats.BeginParking(sid, service)
}

func (s *httpRouteStats) TryBeginParking(sid string, service proxy.ConnectService, binding proxyadmission.Binding) (proxy.TrafficFlow, error) {
	s.begins.Add(1)
	return s.WorkerStats.TryBeginParking(sid, service, binding)
}

func newHTTPRouteFixture(t *testing.T, wake func(string)) *httpRouteFixture {
	t.Helper()
	table, admission, master := newTrafficMasterView(t, 4, 1, config.MaxInflight{Total: 1})
	if err := admission.BeginWorker(0, 1); err != nil {
		t.Fatal(err)
	}
	file, err := admission.DupFile()
	if err != nil {
		t.Fatal(err)
	}
	arena, err := proxyadmission.OpenWorker(file, 4, 1, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = arena.Close() })
	entry := trafficRoute("s1", routesync.StateRunning, nil)
	entry.RunID, entry.FloatingIP = "run-1", "100.100.0.1"
	master.BeginSync()
	if err := master.ApplyUpsert(entry); err != nil {
		t.Fatal(err)
	}
	master.Bookmark()
	entry, _ = table.Lookup(entry.SandboxID)
	updates := &Updates{ch: make(chan struct{})}
	view := NewWorkerView(table, updates, wake, time.Second)
	view.admission = arena
	return &httpRouteFixture{
		table: table, updates: updates, view: view, arena: arena, owner: admission, master: master,
		stats: &httpRouteStats{WorkerStats: proxystats.NewWorkerStatsWithAdmission(arena)}, entry: entry,
	}
}

func (f *httpRouteFixture) update(t *testing.T, entry routesync.RouteEntry, notify bool) {
	t.Helper()
	if err := f.table.Upsert(entry); err != nil {
		t.Fatal(err)
	}
	if notify {
		f.updates.bump()
	}
}

func (f *httpRouteFixture) apply(t *testing.T, entry routesync.RouteEntry) {
	t.Helper()
	if err := f.master.ApplyUpsert(entry); err != nil {
		t.Fatal(err)
	}
	f.updates.bump()
}

func (f *httpRouteFixture) assertReleased(t *testing.T) {
	t.Helper()
	binding := proxyadmission.Binding{Slot: f.entry.AdmissionSlot, Generation: f.entry.AdmissionGeneration, Limits: f.entry.EffectiveMaxInflight}
	if binding.Unlimited() || !f.arena.Valid(binding) {
		return // An unlimited or retired generation has no acquire to check.
	}
	lease, err := f.arena.TryAcquire(binding, proxyadmission.ServiceForward)
	if err != nil {
		t.Fatalf("request leaked its single admission: %v", err)
	}
	lease.Release()
}

func startHTTPRouteRequest(t *testing.T, ctx context.Context, handler http.Handler) (*httptest.ResponseRecorder, <-chan struct{}) {
	t.Helper()
	ctx, cancel := context.WithCancel(ctx)
	req := httptest.NewRequest(http.MethodPost, "http://sandbox/task", strings.NewReader("business-body"))
	req = req.WithContext(ctx)
	req.Header.Set(proxy.HeaderSandboxID, "s1")
	req.Header.Set(proxy.HeaderSandboxPort, "8080")
	req.Header.Set(proxy.HeaderAccessToken, "forward")
	result := httptest.NewRecorder()
	done := make(chan struct{})
	go func() { defer close(done); handler.ServeHTTP(result, req) }()
	t.Cleanup(func() {
		cancel()
		awaitHTTPRoute(t, done)
	})
	return result, done
}

func awaitHTTPRoute[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(3 * time.Second):
		t.Fatal("HTTP route operation did not terminate")
		var zero T
		return zero
	}
}

func serveHTTPRoutePeer(conn net.Conn, reply bool, release <-chan struct{}, received chan<- error, closed chan<- error) {
	defer conn.Close()
	req, err := http.ReadRequest(bufio.NewReader(conn))
	if err == nil {
		var body []byte
		body, err = io.ReadAll(req.Body)
		_ = req.Body.Close()
		if err == nil && (req.Method != http.MethodPost || string(body) != "business-body") {
			err = fmt.Errorf("unexpected guest request: method=%s body=%q", req.Method, body)
		}
	}
	received <- err
	if err == nil && reply {
		if release != nil {
			<-release
		}
		_, err = io.WriteString(conn, "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
	} else if err == nil {
		_, err = conn.Read(make([]byte, 1))
	}
	closed <- err
}

func TestHTTPRouteStaleDialUsesNewestRouteWithoutNewAdmission(t *testing.T) {
	for _, change := range []string{"run-id", "address-without-notify", "address-late-dial", "paused-resume"} {
		t.Run(change, func(t *testing.T) {
			wakes := make(chan string, 1)
			f := newHTTPRouteFixture(t, func(sid string) { wakes <- sid })
			entered := make(chan struct{})
			cancelled := make(chan struct{})
			chosen := make(chan proxy.Route, 1)
			received, closed := make(chan error, 1), make(chan error, 1)
			backend, guest := net.Pipe()
			t.Cleanup(func() { _ = backend.Close(); _ = guest.Close() })
			go serveHTTPRoutePeer(guest, true, nil, received, closed)
			var staleBackend net.Conn
			staleClosed := make(chan error, 1)
			if change == "address-late-dial" {
				var staleGuest net.Conn
				staleBackend, staleGuest = net.Pipe()
				t.Cleanup(func() { _ = staleBackend.Close(); _ = staleGuest.Close() })
				go func() {
					_, err := staleGuest.Read(make([]byte, 1))
					staleClosed <- err
				}()
			}
			var dials atomic.Int32
			px := proxy.NewWithDialer(f.view, nil, nil, nil, func(ctx context.Context, route proxy.Route) (net.Conn, error) {
				if dials.Add(1) == 1 {
					close(entered)
					<-ctx.Done()
					close(cancelled)
					if staleBackend != nil {
						return staleBackend, nil // connect success raced cancellation
					}
					return nil, ctx.Err()
				}
				chosen <- route
				return backend, nil
			}, "").WithTrafficTracker(f.stats)
			result, done := startHTTPRouteRequest(t, context.Background(), px)
			awaitHTTPRoute(t, entered)
			next := f.entry
			switch change {
			case "run-id":
				next.RunID = "run-2" // same address, new incarnation
			case "address-without-notify", "address-late-dial":
				next.FloatingIP = "100.101.0.1"
			case "paused-resume":
				next.State = routesync.StatePaused
			}
			f.update(t, next, change != "address-without-notify")
			awaitHTTPRoute(t, cancelled)
			if change == "paused-resume" {
				if sid := awaitHTTPRoute(t, wakes); sid != f.entry.SandboxID {
					t.Fatalf("Wake = %q", sid)
				}
				next.State, next.RunID, next.FloatingIP = routesync.StateRunning, "run-2", "100.101.0.1"
				f.update(t, next, true)
			}
			got := awaitHTTPRoute(t, chosen)
			if got.Addr != next.FloatingIP+":8080" {
				t.Fatalf("dial used obsolete route: %+v, want %+v", got, next)
			}
			if err := awaitHTTPRoute(t, received); err != nil {
				t.Fatal(err)
			}
			awaitHTTPRoute(t, done)
			if result.Code != 200 || result.Body.String() != "ok" || dials.Load() != 2 || f.stats.begins.Load() != 1 {
				t.Fatalf("response=%d %q dials=%d", result.Code, result.Body.String(), dials.Load())
			}
			if err := awaitHTTPRoute(t, closed); err != nil {
				t.Fatal(err)
			}
			if staleBackend != nil {
				if err := awaitHTTPRoute(t, staleClosed); !errors.Is(err, io.EOF) {
					t.Fatalf("late obsolete connection received HTTP bytes or leaked: %v", err)
				}
			}
			f.assertReleased(t)
		})
	}
}

func TestHTTPRouteStaleDialRejectsRevokedBindingAndDeletion(t *testing.T) {
	for _, change := range []string{"credentials", "delete"} {
		t.Run(change, func(t *testing.T) {
			f := newHTTPRouteFixture(t, nil)
			entered := make(chan struct{})
			var dials atomic.Int32
			px := proxy.NewWithDialer(f.view, nil, nil, nil, func(ctx context.Context, _ proxy.Route) (net.Conn, error) {
				if dials.Add(1) == 1 {
					close(entered)
				}
				<-ctx.Done()
				return nil, ctx.Err()
			}, "").WithTrafficTracker(f.stats)
			result, done := startHTTPRouteRequest(t, context.Background(), px)
			awaitHTTPRoute(t, entered)
			if change == "delete" {
				f.table.Delete(f.entry.SandboxID)
				f.updates.bump()
			} else {
				next := f.entry
				next.ForwardAccessToken = "rotated"
				f.update(t, next, true)
			}
			awaitHTTPRoute(t, done)
			if result.Code != http.StatusNotFound || dials.Load() != 1 {
				t.Fatalf("revoked binding forwarded: status=%d dials=%d", result.Code, dials.Load())
			}
			f.assertReleased(t)
		})
	}
}

func TestHTTPRoutePreservesClientCancellationAndDeadline(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		t.Run(fmt.Sprintf("deadline=%v", deadline), func(t *testing.T) {
			f := newHTTPRouteFixture(t, nil)
			var ctx context.Context
			var cancel context.CancelFunc
			if deadline {
				ctx, cancel = context.WithTimeout(context.Background(), 150*time.Millisecond)
			} else {
				ctx, cancel = context.WithCancel(context.Background())
			}
			defer cancel()
			entered := make(chan context.Context, 1)
			var dials atomic.Int32
			px := proxy.NewWithDialer(f.view, nil, nil, nil, func(dialCtx context.Context, _ proxy.Route) (net.Conn, error) {
				dials.Add(1)
				entered <- dialCtx
				<-dialCtx.Done()
				return nil, dialCtx.Err()
			}, "").WithTrafficTracker(f.stats)
			_, done := startHTTPRouteRequest(t, ctx, px)
			dialCtx := awaitHTTPRoute(t, entered)
			wantDeadline, wantOK := ctx.Deadline()
			gotDeadline, gotOK := dialCtx.Deadline()
			if wantOK != gotOK || !wantDeadline.Equal(gotDeadline) {
				t.Fatalf("dial deadline=%v %v, want %v %v", gotDeadline, gotOK, wantDeadline, wantOK)
			}
			if !deadline {
				cancel()
			}
			awaitHTTPRoute(t, done)
			wantErr := context.Canceled
			if deadline {
				wantErr = context.DeadlineExceeded
			}
			if !errors.Is(dialCtx.Err(), wantErr) || dials.Load() != 1 {
				t.Fatalf("cancellation=%v dials=%d", dialCtx.Err(), dials.Load())
			}
			f.assertReleased(t)
		})
	}
}

func TestHTTPRouteEstablishedExchangeClosesWithoutReplay(t *testing.T) {
	f := newHTTPRouteFixture(t, nil)
	backend, guest := net.Pipe()
	t.Cleanup(func() { _ = backend.Close(); _ = guest.Close() })
	received, closed := make(chan error, 1), make(chan error, 1)
	go serveHTTPRoutePeer(guest, false, nil, received, closed)
	var dials atomic.Int32
	px := proxy.NewWithDialer(f.view, nil, nil, nil, func(context.Context, proxy.Route) (net.Conn, error) {
		dials.Add(1)
		return backend, nil
	}, "").WithTrafficTracker(f.stats)
	result, done := startHTTPRouteRequest(t, context.Background(), px)
	if err := awaitHTTPRoute(t, received); err != nil {
		t.Fatal(err)
	}
	next := f.entry
	next.RunID, next.FloatingIP = "run-2", "100.101.0.1"
	f.update(t, next, true)
	awaitHTTPRoute(t, done)
	if err := awaitHTTPRoute(t, closed); !errors.Is(err, io.EOF) {
		t.Fatalf("obsolete established backend was not closed: %v", err)
	}
	if result.Code != http.StatusBadGateway || result.Header().Get(proxy.HeaderProxyError) != proxy.ProxyErrorUpstreamError || dials.Load() != 1 {
		t.Fatalf("exchange was replayed or wrong error: status=%d dials=%d", result.Code, dials.Load())
	}
	f.assertReleased(t)
}

func TestWatchRouteStopJoins(t *testing.T) {
	f := newHTTPRouteFixture(t, nil)
	binding, found, err := f.view.LookupRoute(context.Background(), f.entry.SandboxID, proxy.LegacyTarget(8080))
	if err != nil || !found {
		t.Fatalf("lookup=%v %v", found, err)
	}
	route, found, err := f.view.ActivateRoute(context.Background(), binding)
	if err != nil || !found {
		t.Fatalf("activate=%v %v", found, err)
	}
	watcher, ok := any(f.view).(interface {
		WatchRoute(context.Context, proxy.RouteBinding, proxy.Route) (context.Context, context.CancelFunc)
	})
	if !ok {
		t.Fatal("ordinary HTTP route observation is unavailable")
	}
	ctx, stop := watcher.WatchRoute(context.Background(), binding, route)
	defer stop()
	stop() // Must join the watcher; a second stop is safe.
	if !errors.Is(ctx.Err(), context.Canceled) {
		t.Fatalf("stop did not cancel transport: %v", ctx.Err())
	}
}

// These changes use the real master/arena transaction. Direct table writes do
// not exercise retirement of a limited generation or rollback before publish.
func TestHTTPRouteEstablishedExchangeSurvivesPolicyChanges(t *testing.T) {
	for _, change := range []string{"limited-to-unlimited", "unlimited-to-limited", "other-service-credentials", "template", "invalid-policy", "rollback"} {
		t.Run(change, func(t *testing.T) {
			f := newHTTPRouteFixture(t, nil)
			if change == "unlimited-to-limited" {
				zero := uint32(0)
				next := f.entry
				next.MaxInflightPatch = &sandboxcfg.MaxInflightPatch{Total: &zero}
				f.apply(t, next)
				f.entry, _ = f.table.Lookup(f.entry.SandboxID)
			}
			backend, guest := net.Pipe()
			t.Cleanup(func() { _ = backend.Close(); _ = guest.Close() })
			received, closed := make(chan error, 1), make(chan error, 1)
			reply := make(chan struct{})
			release := func() {
				select {
				case <-reply:
				default:
					close(reply)
				}
			}
			t.Cleanup(release)
			go serveHTTPRoutePeer(guest, true, reply, received, closed)
			var dials atomic.Int32
			px := proxy.NewWithDialer(f.view, nil, nil, nil, func(context.Context, proxy.Route) (net.Conn, error) {
				dials.Add(1)
				return backend, nil
			}, "").WithTrafficTracker(f.stats)
			result, done := startHTTPRouteRequest(t, context.Background(), px)
			if err := awaitHTTPRoute(t, received); err != nil {
				t.Fatal(err)
			}
			next := f.entry
			var pending *proxyadmission.Update
			switch change {
			case "limited-to-unlimited", "unlimited-to-limited":
				limit := uint32(0)
				if change == "unlimited-to-limited" {
					limit = 1
				}
				next.MaxInflightPatch = &sandboxcfg.MaxInflightPatch{Total: &limit}
			case "other-service-credentials":
				next.EnvdAccessToken, next.ServiceSecret = "other-envd", "other-service-secret"
				next.APISecretFingerprint, next.ManifestKeyFingerprint = "other-api", "other-manifest"
			case "template":
				next.TemplateID = "other-template"
			case "invalid-policy":
				next.TrafficPolicyInvalid = true
			case "rollback":
				var err error
				pending, err = f.owner.PrepareUpsert(next.SandboxID, "replacement-identity", next.EffectiveMaxInflight)
				if err != nil {
					t.Fatal(err)
				}
				defer pending.Rollback()
			}
			if pending == nil {
				f.apply(t, next)
			}
			select {
			case <-done:
				t.Fatalf("accepted HTTP was evicted by %s: status=%d", change, result.Code)
			case <-time.After(100 * time.Millisecond):
			}
			if pending != nil {
				pending.Rollback()
			}
			release()
			awaitHTTPRoute(t, done)
			if err := awaitHTTPRoute(t, closed); err != nil {
				t.Fatalf("policy change closed healthy backend: %v", err)
			}
			if result.Code != http.StatusOK || result.Body.String() != "ok" || dials.Load() != 1 || f.stats.begins.Load() != 1 {
				t.Fatalf("response=%d %q dials=%d ingresses=%d", result.Code, result.Body.String(), dials.Load(), f.stats.begins.Load())
			}
			f.assertReleased(t)
		})
	}
}

func TestHTTPRoutePolicyUpdateKeepsDialButReactivationUsesOriginalBinding(t *testing.T) {
	f := newHTTPRouteFixture(t, nil)
	entered := make(chan context.Context, 1)
	var dials atomic.Int32
	px := proxy.NewWithDialer(f.view, nil, nil, nil, func(ctx context.Context, _ proxy.Route) (net.Conn, error) {
		dials.Add(1)
		entered <- ctx
		<-ctx.Done()
		return nil, ctx.Err()
	}, "").WithTrafficTracker(f.stats)
	result, done := startHTTPRouteRequest(t, context.Background(), px)
	dialCtx := awaitHTTPRoute(t, entered)
	next := f.entry
	two := uint32(2)
	next.MaxInflightPatch = &sandboxcfg.MaxInflightPatch{Total: &two}
	f.apply(t, next)
	select {
	case <-dialCtx.Done():
		t.Fatalf("limit-only change cancelled an admitted dial: %v", context.Cause(dialCtx))
	case <-time.After(100 * time.Millisecond):
	}
	next.RunID, next.FloatingIP = "run-2", "100.101.0.1"
	f.apply(t, next)
	awaitHTTPRoute(t, done)
	if result.Code != http.StatusNotFound || result.Header().Get(proxy.HeaderProxyError) != proxy.ProxyErrorRouteError || dials.Load() != 1 || f.stats.begins.Load() != 1 {
		t.Fatalf("reactivation bypassed original binding: status=%d dials=%d ingresses=%d", result.Code, dials.Load(), f.stats.begins.Load())
	}
	f.assertReleased(t)
}
