package proxy_test

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/envdsign"
	"github.com/kuasar-sandbox/orchestrator/internal/metrics"
	"github.com/kuasar-sandbox/orchestrator/internal/proxy"
	"github.com/kuasar-sandbox/orchestrator/internal/proxystats"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

type stubRouter struct {
	r     proxy.Route
	token string
}

func (s stubRouter) LookupRoute(_ context.Context, sid string, target proxy.ConnectTarget) (proxy.RouteBinding, bool, error) {
	if s.r.Kind == proxy.KindNotFound {
		return proxy.RouteBinding{}, false, nil
	}
	binding := proxy.BindRoute(sid, sid, types.ProfileE2B, s.token, s.token, target)
	binding.Kind = s.r.Kind
	return binding, true, nil
}

func (s stubRouter) ActivateRoute(_ context.Context, _ proxy.RouteBinding) (proxy.Route, bool, error) {
	return s.r, s.r.Kind != proxy.KindNotFound, nil
}

type countingListener struct {
	net.Listener
	accepts atomic.Int32
}

func (l *countingListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err == nil {
		l.accepts.Add(1)
	}
	return c, err
}

func TestRouteForTarget(t *testing.T) {
	tests := []struct {
		name      string
		profile   types.Profile
		target    proxy.ConnectTarget
		wantKind  proxy.Kind
		wantUDS   string
		wantAddr  string
		wantToken string
	}{
		{name: "legacy e2b envd", profile: "e2b", target: proxy.LegacyTarget(49983), wantKind: proxy.KindUDS, wantUDS: "/e.sock", wantToken: "envd"},
		{name: "legacy e2b interpreter", profile: "e2b", target: proxy.LegacyTarget(49999), wantKind: proxy.KindUDS, wantUDS: "/c.sock", wantToken: "envd"},
		{name: "legacy e2b forward", profile: "e2b", target: proxy.LegacyTarget(8080), wantKind: proxy.KindTCP, wantAddr: "10.0.0.5:8080", wantToken: "forward"},
		{name: "legacy bare 49983", profile: "bare", target: proxy.LegacyTarget(49983), wantKind: proxy.KindTCP, wantAddr: "10.0.0.5:49983", wantToken: "forward"},
		{name: "legacy bare 49999", profile: "bare", target: proxy.LegacyTarget(49999), wantKind: proxy.KindTCP, wantAddr: "10.0.0.5:49999", wantToken: "forward"},
		{name: "explicit e2b forward reserved port", profile: "e2b", target: proxy.ConnectTarget{Service: proxy.ConnectServiceForward, Port: 49983}, wantKind: proxy.KindTCP, wantAddr: "10.0.0.5:49983", wantToken: "forward"},
		{name: "explicit bare forward", profile: "bare", target: proxy.ConnectTarget{Service: proxy.ConnectServiceForward, Port: 49999}, wantKind: proxy.KindTCP, wantAddr: "10.0.0.5:49999", wantToken: "forward"},
		{name: "explicit envd ignores port", profile: "e2b", target: proxy.ConnectTarget{Service: proxy.ConnectServiceE2BEnvd, Port: 8080}, wantKind: proxy.KindUDS, wantUDS: "/e.sock", wantToken: "envd"},
		{name: "explicit interpreter ignores port", profile: "e2b", target: proxy.ConnectTarget{Service: proxy.ConnectServiceE2BInterpreter, Port: 49983}, wantKind: proxy.KindUDS, wantUDS: "/c.sock", wantToken: "envd"},
		{name: "bare envd unsupported", profile: "bare", target: proxy.ConnectTarget{Service: proxy.ConnectServiceE2BEnvd}, wantKind: proxy.KindDeny},
		{name: "bare interpreter unsupported", profile: "bare", target: proxy.ConnectTarget{Service: proxy.ConnectServiceE2BInterpreter}, wantKind: proxy.KindDeny},
		{name: "known exec pending issue 64", profile: "e2b", target: proxy.ConnectTarget{Service: proxy.ConnectServiceExec}, wantKind: proxy.KindDeny},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			binding := proxy.BindRoute("node-s1", "stable-s1", tc.profile, "envd", "forward", tc.target)
			route := proxy.RouteForTarget(tc.profile, "/e.sock", "/c.sock", "10.0.0.5", tc.target)
			if route.Kind != tc.wantKind || route.UDS != tc.wantUDS || route.Addr != tc.wantAddr || binding.ExpectedAccessToken != tc.wantToken {
				t.Fatalf("route = %+v, want kind=%v uds=%q addr=%q token=%q", route, tc.wantKind, tc.wantUDS, tc.wantAddr, tc.wantToken)
			}
		})
	}
}

func TestParseSandbox(t *testing.T) {
	req := httptest.NewRequest("GET", "http://8080-abc.example.com/x", nil)
	req.Host = "8080-abc.example.com"
	if sid, port, ok := proxy.ParseSandbox(req); !ok || sid != "abc" || port != 8080 {
		t.Fatalf("host parse: %q %d %v", sid, port, ok)
	}
	req = httptest.NewRequest("GET", "http://x/y", nil)
	req.Header.Set("E2b-Sandbox-Id", "zzz")
	req.Header.Set("E2b-Sandbox-Port", "49983")
	if sid, port, ok := proxy.ParseSandbox(req); !ok || sid != "zzz" || port != 49983 {
		t.Fatalf("header parse: %q %d %v", sid, port, ok)
	}
	req = httptest.NewRequest("GET", "http://x/y", nil)
	req.Header.Set("E2b-Sandbox-Id", "zzz")
	if _, _, ok := proxy.ParseSandbox(req); ok {
		t.Fatal("expected parse failure when E2b-Sandbox-Port is missing")
	}
	req = httptest.NewRequest("GET", "http://noport/y", nil)
	req.Host = "noport"
	if _, _, ok := proxy.ParseSandbox(req); ok {
		t.Fatal("expected parse failure for host without <port>-<sid>")
	}
}

func TestParseConnectCanonicalTargetAndSourceConflicts(t *testing.T) {
	request := func(authority string) *http.Request {
		req := httptest.NewRequest(http.MethodConnect, authority, nil)
		req.Host = authority
		return req
	}

	t.Run("portless exec ignores generic authority", func(t *testing.T) {
		req := request("sandbox:443")
		req.Header.Set(proxy.HeaderSandboxID, "s1")
		req.Header.Set(proxy.HeaderSandboxService, string(proxy.ConnectServiceExec))
		sid, target, ok := proxy.ParseConnect(req)
		if !ok || sid != "s1" || target != (proxy.ConnectTarget{Service: proxy.ConnectServiceExec}) {
			t.Fatalf("ParseConnect = %q %+v %v", sid, target, ok)
		}
	})

	t.Run("logical service retains explicit port and ignores authority", func(t *testing.T) {
		req := request("sandbox:443")
		req.Header.Set(proxy.HeaderSandboxID, "s1")
		req.Header.Set(proxy.HeaderSandboxService, string(proxy.ConnectServiceE2BEnvd))
		req.Header.Set(proxy.HeaderSandboxPort, "8080")
		sid, target, ok := proxy.ParseConnect(req)
		if !ok || sid != "s1" || target != (proxy.ConnectTarget{Service: proxy.ConnectServiceE2BEnvd, Port: 8080}) {
			t.Fatalf("ParseConnect = %q %+v %v", sid, target, ok)
		}
	})

	t.Run("forward uses authority port", func(t *testing.T) {
		req := request("sandbox:8080")
		req.Header.Set(proxy.HeaderSandboxID, "s1")
		req.Header.Set(proxy.HeaderSandboxService, string(proxy.ConnectServiceForward))
		sid, target, ok := proxy.ParseConnect(req)
		if !ok || sid != "s1" || target != (proxy.ConnectTarget{Service: proxy.ConnectServiceForward, Port: 8080}) {
			t.Fatalf("ParseConnect = %q %+v %v", sid, target, ok)
		}
	})

	t.Run("legacy host source", func(t *testing.T) {
		req := request("49983-s1.test.local:49983")
		sid, target, ok := proxy.ParseConnect(req)
		if !ok || sid != "s1" || target != proxy.LegacyTarget(49983) {
			t.Fatalf("ParseConnect = %q %+v %v", sid, target, ok)
		}
	})

	for _, tc := range []struct {
		name   string
		mutate func(*http.Request)
	}{
		{name: "empty service", mutate: func(r *http.Request) {
			r.Header.Set(proxy.HeaderSandboxID, "s1")
			r.Header[proxy.HeaderSandboxService] = []string{""}
		}},
		{name: "unknown service", mutate: func(r *http.Request) {
			r.Header.Set(proxy.HeaderSandboxID, "s1")
			r.Header.Set(proxy.HeaderSandboxService, "unknown")
		}},
		{name: "forward missing port", mutate: func(r *http.Request) {
			r.URL.Host, r.Host = "sandbox", "sandbox"
			r.Header.Set(proxy.HeaderSandboxID, "s1")
			r.Header.Set(proxy.HeaderSandboxService, string(proxy.ConnectServiceForward))
		}},
		{name: "conflicting forward ports", mutate: func(r *http.Request) {
			r.Header.Set(proxy.HeaderSandboxID, "s1")
			r.Header.Set(proxy.HeaderSandboxService, string(proxy.ConnectServiceForward))
			r.Header.Set(proxy.HeaderSandboxPort, "8080")
		}},
		{name: "conflicting sandbox ids", mutate: func(r *http.Request) {
			r.URL.Host, r.Host = "8080-s2.test.local:8080", "8080-s2.test.local:8080"
			r.Header.Set(proxy.HeaderSandboxID, "s1")
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := request("sandbox:9090")
			tc.mutate(req)
			if _, _, ok := proxy.ParseConnect(req); ok {
				t.Fatal("conflicting or invalid CONNECT target accepted")
			}
		})
	}
}

func TestWriteSandboxConnectPreservesServiceAndOptionalPort(t *testing.T) {
	tests := []struct {
		name          string
		target        proxy.ConnectTarget
		wantAuthority string
		wantService   string
		wantPort      string
	}{
		{name: "legacy", target: proxy.LegacyTarget(8080), wantAuthority: "sandbox:8080", wantPort: "8080"},
		{name: "forward", target: proxy.ConnectTarget{Service: proxy.ConnectServiceForward, Port: 49983}, wantAuthority: "sandbox:49983", wantService: "forward", wantPort: "49983"},
		{name: "logical with port", target: proxy.ConnectTarget{Service: proxy.ConnectServiceE2BEnvd, Port: 8080}, wantAuthority: "sandbox:443", wantService: "e2b:envd", wantPort: "8080"},
		{name: "portless exec", target: proxy.ConnectTarget{Service: proxy.ConnectServiceExec}, wantAuthority: "sandbox:443", wantService: "exec"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var wire bytes.Buffer
			if err := proxy.WriteSandboxConnect(&wire, "s1", tc.target, "tok"); err != nil {
				t.Fatal(err)
			}
			req, err := http.ReadRequest(bufio.NewReader(&wire))
			if err != nil {
				t.Fatal(err)
			}
			if req.Method != http.MethodConnect || req.URL.Host != tc.wantAuthority ||
				req.Header.Get(proxy.HeaderSandboxID) != "s1" ||
				req.Header.Get(proxy.HeaderSandboxService) != tc.wantService ||
				req.Header.Get(proxy.HeaderSandboxPort) != tc.wantPort ||
				req.Header.Get(proxy.HeaderAccessToken) != "tok" {
				t.Fatalf("CONNECT request = method=%q authority=%q headers=%v", req.Method, req.URL.Host, req.Header)
			}
		})
	}
}

func TestOrdinaryHTTPIgnoresAndPreservesServiceHeader(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get(proxy.HeaderSandboxService); got != "application-defined" {
			t.Errorf("backend service header = %q", got)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer backend.Close()

	var routed proxy.ConnectTarget
	router := &recordingRouter{
		route:  proxy.Route{Kind: proxy.KindTCP, Addr: strings.TrimPrefix(backend.URL, "http://")},
		token:  "tok",
		target: &routed,
	}
	px := proxy.New(router, func() string { return "enforce" }, log, nil)
	req := httptest.NewRequest(http.MethodGet, "http://sandbox/health", nil)
	req.Header.Set(proxy.HeaderSandboxID, "s1")
	req.Header.Set(proxy.HeaderSandboxPort, "49983")
	req.Header.Set(proxy.HeaderSandboxService, "application-defined")
	req.Header.Set(proxy.HeaderAccessToken, "tok")
	resp := httptest.NewRecorder()
	px.ServeHTTP(resp, req)
	if resp.Code != http.StatusNoContent {
		t.Fatalf("status = %d", resp.Code)
	}
	if routed != proxy.LegacyTarget(49983) {
		t.Fatalf("ordinary HTTP route target = %+v, want legacy port", routed)
	}
}

type recordingRouter struct {
	route  proxy.Route
	token  string
	target *proxy.ConnectTarget
}

func (r *recordingRouter) LookupRoute(_ context.Context, sid string, target proxy.ConnectTarget) (proxy.RouteBinding, bool, error) {
	if r.target != nil {
		*r.target = target
	}
	binding := proxy.BindRoute(sid, sid, types.ProfileE2B, r.token, r.token, target)
	binding.Kind = r.route.Kind
	return binding, true, nil
}

func (r *recordingRouter) ActivateRoute(_ context.Context, _ proxy.RouteBinding) (proxy.Route, bool, error) {
	return r.route, true, nil
}

type admissionRouter struct {
	binding       proxy.RouteBinding
	route         proxy.Route
	found         bool
	activateOK    bool
	lookupCalls   atomic.Int32
	activateCalls atomic.Int32
	activateSeen  chan struct{}
	activateWait  <-chan struct{}
}

type recordingTrafficTracker struct {
	begins   atomic.Int32
	attaches atomic.Int32
	closes   atomic.Int32
	service  proxy.ConnectService
}

func (t *recordingTrafficTracker) BeginParking(_ string, service proxy.ConnectService) proxy.TrafficFlow {
	t.begins.Add(1)
	t.service = service
	return recordingTrafficFlow{tracker: t}
}

type recordingTrafficFlow struct{ tracker *recordingTrafficTracker }

func (f recordingTrafficFlow) AttachBackend(conn net.Conn) net.Conn {
	f.tracker.attaches.Add(1)
	return conn
}

func (f recordingTrafficFlow) Close() { f.tracker.closes.Add(1) }

func (r *admissionRouter) LookupRoute(_ context.Context, _ string, _ proxy.ConnectTarget) (proxy.RouteBinding, bool, error) {
	r.lookupCalls.Add(1)
	return r.binding, r.found, nil
}

func (r *admissionRouter) ActivateRoute(_ context.Context, expected proxy.RouteBinding) (proxy.Route, bool, error) {
	r.activateCalls.Add(1)
	if r.activateSeen != nil {
		select {
		case r.activateSeen <- struct{}{}:
		default:
		}
	}
	if r.activateWait != nil {
		<-r.activateWait
	}
	if expected != r.binding {
		return proxy.Route{}, false, nil
	}
	return r.route, r.activateOK, nil
}

func waitTraffic(t *testing.T, master *proxystats.MasterStats, predicate func(*api.TrafficStats) bool) *api.TrafficStats {
	return waitTrafficFor(t, master, "node-s1", types.ProfileE2B, types.StateRunning, predicate)
}

func waitTrafficFor(t *testing.T, master *proxystats.MasterStats, sandboxID string, profile types.Profile, state types.State, predicate func(*api.TrafficStats) bool) *api.TrafficStats {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		stats, err := master.SandboxTrafficStats(context.Background(), sandboxID, "run-1", profile, state)
		if err == nil && predicate(stats) {
			return stats
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("traffic stats did not reach expected state")
	return nil
}

func newTrafficHarness(t *testing.T) (*proxystats.WorkerStats, *proxystats.MasterStats) {
	t.Helper()
	worker := proxystats.NewWorkerStats()
	master := proxystats.NewMasterStats(metrics.New(), []string{"internal"})
	if err := master.BeginWorker("internal", 1); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if _, err := worker.StartSender(ctx, "internal", 1, func(frame proxystats.Frame) error {
		return master.Receive("internal", 1, frame)
	}); err != nil {
		t.Fatal(err)
	}
	return worker, master
}

func waitInflight(t *testing.T, master *proxystats.MasterStats, parking, egress uint64) *api.TrafficStats {
	t.Helper()
	return waitTraffic(t, master, func(stats *api.TrafficStats) bool {
		return stats.Inflight.Parking == parking && stats.Inflight.Connected == egress
	})
}

func waitInflightFor(t *testing.T, master *proxystats.MasterStats, sandboxID string, parking, egress uint64) *api.TrafficStats {
	t.Helper()
	return waitTrafficFor(t, master, sandboxID, types.ProfileE2B, types.StateRunning, func(stats *api.TrafficStats) bool {
		return stats.Inflight.Parking == parking && stats.Inflight.Connected == egress
	})
}

func TestAuthorizedRequestParksDuringActivationAndEndsOnDialFailure(t *testing.T) {
	target := proxy.LegacyTarget(8080)
	release := make(chan struct{})
	router := &admissionRouter{
		binding:      proxy.BindRoute("node-s1", "stable-s1", types.ProfileE2B, "envd", "forward", target),
		route:        proxy.Route{Kind: proxy.KindTCP, Addr: "127.0.0.1:1"},
		found:        true,
		activateOK:   true,
		activateSeen: make(chan struct{}, 1),
		activateWait: release,
	}
	worker, master := newTrafficHarness(t)
	px := proxy.NewWithDialer(router, func() string { return "enforce" }, nil, nil,
		func(context.Context, proxy.Route) (net.Conn, error) { return nil, fmt.Errorf("dial failed") }, "").
		WithTrafficTracker(worker)
	req := httptest.NewRequest(http.MethodGet, "http://sandbox/", nil)
	req.Header.Set(proxy.HeaderSandboxID, "node-s1")
	req.Header.Set(proxy.HeaderSandboxPort, "8080")
	req.Header.Set(proxy.HeaderAccessToken, "forward")
	resp := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		px.ServeHTTP(resp, req)
		close(done)
	}()
	select {
	case <-router.activateSeen:
	case <-time.After(time.Second):
		t.Fatal("request did not enter activation")
	}
	stats := waitTraffic(t, master, func(stats *api.TrafficStats) bool { return stats.Inflight.Parking == 1 })
	if service := stats.Services[string(proxy.ConnectServiceForward)]; service.Parking != 1 || service.Connected != 0 {
		t.Fatalf("parking service stats = %+v", service)
	}
	close(release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("request did not finish after activation release")
	}
	if resp.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", resp.Code)
	}
	waitTraffic(t, master, func(stats *api.TrafficStats) bool {
		return stats.Inflight.Parking == 0 && stats.Inflight.Connected == 0 && stats.IdleSince != nil
	})
}

func TestOrdinaryHTTPSuccessTracksFinalBackendUntilClose(t *testing.T) {
	target := proxy.LegacyTarget(8080)
	router := &admissionRouter{
		binding:    proxy.BindRoute("node-s1", "stable-s1", types.ProfileE2B, "envd", "forward", target),
		route:      proxy.Route{Kind: proxy.KindTCP, Addr: "unused"},
		found:      true,
		activateOK: true,
	}
	worker, master := newTrafficHarness(t)
	backend, proxySide := net.Pipe()
	received := make(chan struct{})
	release := make(chan struct{})
	backendDone := make(chan error, 1)
	go func() {
		defer backend.Close()
		request, err := http.ReadRequest(bufio.NewReader(backend))
		if err != nil {
			backendDone <- err
			return
		}
		request.Body.Close()
		close(received)
		<-release
		_, err = io.WriteString(backend, "HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
		backendDone <- err
	}()
	var dialed atomic.Bool
	px := proxy.NewWithDialer(router, func() string { return "enforce" }, nil, nil,
		func(context.Context, proxy.Route) (net.Conn, error) {
			if !dialed.CompareAndSwap(false, true) {
				return nil, fmt.Errorf("unexpected second dial")
			}
			return proxySide, nil
		}, "").WithTrafficTracker(worker)
	req := httptest.NewRequest(http.MethodGet, "http://sandbox/health", nil)
	req.Header.Set(proxy.HeaderSandboxID, "node-s1")
	req.Header.Set(proxy.HeaderSandboxPort, "8080")
	req.Header.Set(proxy.HeaderAccessToken, "forward")
	resp := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		px.ServeHTTP(resp, req)
		close(done)
	}()
	select {
	case <-received:
	case <-time.After(time.Second):
		t.Fatal("backend did not receive HTTP request")
	}
	waitInflight(t, master, 0, 1)
	close(release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("HTTP request did not finish")
	}
	if err := <-backendDone; err != nil {
		t.Fatal(err)
	}
	if resp.Code != http.StatusNoContent {
		t.Fatalf("status=%d, want 204", resp.Code)
	}
	stats := waitInflight(t, master, 0, 0)
	if stats.IdleSince == nil {
		t.Fatal("closed HTTP backend did not establish idleSince")
	}
}

func TestOrdinaryAdmissionRejectsBeforeActivationAndDial(t *testing.T) {
	target := proxy.LegacyTarget(8080)
	for _, method := range []string{http.MethodGet, http.MethodConnect} {
		t.Run(method, func(t *testing.T) {
			router := &admissionRouter{
				binding:    proxy.BindRoute("node-s1", "stable-s1", types.ProfileE2B, "envd", "forward", target),
				route:      proxy.Route{Kind: proxy.KindTCP, Addr: "127.0.0.1:1"},
				found:      true,
				activateOK: true,
			}
			var dials atomic.Int32
			traffic := &recordingTrafficTracker{}
			px := proxy.NewWithDialer(router, func() string { return "enforce" }, nil, nil,
				func(context.Context, proxy.Route) (net.Conn, error) {
					dials.Add(1)
					return nil, fmt.Errorf("unexpected dial")
				}, "").WithTrafficTracker(traffic)
			var req *http.Request
			if method == http.MethodConnect {
				req = httptest.NewRequest(method, "http://sandbox", nil)
				req.Header.Set(proxy.HeaderSandboxPort, "8080")
			} else {
				req = httptest.NewRequest(method, "http://sandbox/", nil)
				req.Header.Set(proxy.HeaderSandboxPort, "8080")
			}
			req.Header.Set(proxy.HeaderSandboxID, "node-s1")
			req.Header.Set(proxy.HeaderAccessToken, "invalid")
			resp := httptest.NewRecorder()
			px.ServeHTTP(resp, req)
			if resp.Code != http.StatusUnauthorized {
				t.Fatalf("status = %d, want 401", resp.Code)
			}
			if router.lookupCalls.Load() != 1 || router.activateCalls.Load() != 0 || dials.Load() != 0 || traffic.begins.Load() != 0 {
				t.Fatalf("calls lookup=%d activate=%d dial=%d parking=%d, want 1/0/0/0",
					router.lookupCalls.Load(), router.activateCalls.Load(), dials.Load(), traffic.begins.Load())
			}
		})
	}
}

func TestOrdinaryAdmissionFailsClosedWhenActivationRejectsBinding(t *testing.T) {
	target := proxy.LegacyTarget(8080)
	router := &admissionRouter{
		binding: proxy.BindRoute("node-s1", "stable-s1", types.ProfileE2B, "envd", "forward", target),
		route:   proxy.Route{Kind: proxy.KindTCP, Addr: "127.0.0.1:1"},
		found:   true,
	}
	var dials atomic.Int32
	traffic := &recordingTrafficTracker{}
	px := proxy.NewWithDialer(router, func() string { return "enforce" }, nil, nil,
		func(context.Context, proxy.Route) (net.Conn, error) {
			dials.Add(1)
			return nil, fmt.Errorf("unexpected dial")
		}, "").WithTrafficTracker(traffic)
	req := httptest.NewRequest(http.MethodGet, "http://sandbox/", nil)
	req.Header.Set(proxy.HeaderSandboxID, "node-s1")
	req.Header.Set(proxy.HeaderSandboxPort, "8080")
	req.Header.Set(proxy.HeaderAccessToken, "forward")
	resp := httptest.NewRecorder()
	px.ServeHTTP(resp, req)
	if resp.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.Code)
	}
	if kind := resp.Header().Get(proxy.HeaderProxyError); kind != proxy.ProxyErrorRouteError {
		t.Fatalf("post-admission proxy error = %q, want %q", kind, proxy.ProxyErrorRouteError)
	}
	if router.activateCalls.Load() != 1 || dials.Load() != 0 || traffic.begins.Load() != 1 || traffic.closes.Load() != 1 {
		t.Fatalf("calls activate=%d dial=%d parking=%d close=%d, want 1/0/1/1",
			router.activateCalls.Load(), dials.Load(), traffic.begins.Load(), traffic.closes.Load())
	}
}

func TestTrafficServiceClassification(t *testing.T) {
	tests := []struct {
		name    string
		profile types.Profile
		target  proxy.ConnectTarget
		method  string
		want    proxy.ConnectService
	}{
		{name: "e2b legacy envd", profile: types.ProfileE2B, target: proxy.LegacyTarget(49983), method: http.MethodGet, want: proxy.ConnectServiceE2BEnvd},
		{name: "e2b legacy interpreter", profile: types.ProfileE2B, target: proxy.LegacyTarget(49999), method: http.MethodGet, want: proxy.ConnectServiceE2BInterpreter},
		{name: "e2b legacy forward", profile: types.ProfileE2B, target: proxy.LegacyTarget(8080), method: http.MethodGet, want: proxy.ConnectServiceForward},
		{name: "bare reserved-looking port is forward", profile: types.ProfileBare, target: proxy.LegacyTarget(49983), method: http.MethodGet, want: proxy.ConnectServiceForward},
		{name: "explicit forward", profile: types.ProfileE2B, target: proxy.ConnectTarget{Service: proxy.ConnectServiceForward, Port: 49983}, method: http.MethodConnect, want: proxy.ConnectServiceForward},
		{name: "explicit envd", profile: types.ProfileE2B, target: proxy.ConnectTarget{Service: proxy.ConnectServiceE2BEnvd}, method: http.MethodConnect, want: proxy.ConnectServiceE2BEnvd},
		{name: "explicit interpreter", profile: types.ProfileE2B, target: proxy.ConnectTarget{Service: proxy.ConnectServiceE2BInterpreter}, method: http.MethodConnect, want: proxy.ConnectServiceE2BInterpreter},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			binding := proxy.BindRoute("node-s1", "stable-s1", test.profile, "envd", "forward", test.target)
			router := &admissionRouter{
				binding: binding, route: proxy.Route{Kind: binding.Kind}, found: true, activateOK: true,
			}
			tracker := &recordingTrafficTracker{}
			px := proxy.NewWithDialer(router, func() string { return "enforce" }, nil, nil,
				func(context.Context, proxy.Route) (net.Conn, error) { return nil, fmt.Errorf("dial failed") }, "").
				WithTrafficTracker(tracker)
			authorityPort := 443
			if test.target.Service == proxy.ConnectServiceLegacy || test.target.Service == proxy.ConnectServiceForward {
				authorityPort = test.target.Port
			}
			authority := fmt.Sprintf("sandbox:%d", authorityPort)
			req := httptest.NewRequest(test.method, "http://"+authority+"/", nil)
			req.Host = authority
			req.Header.Set(proxy.HeaderSandboxID, "node-s1")
			if test.method == http.MethodGet || test.target.Port > 0 {
				req.Header.Set(proxy.HeaderSandboxPort, fmt.Sprint(test.target.Port))
			}
			if test.target.Service != proxy.ConnectServiceLegacy {
				req.Header.Set(proxy.HeaderSandboxService, string(test.target.Service))
			}
			req.Header.Set(proxy.HeaderAccessToken, binding.ExpectedAccessToken)
			resp := httptest.NewRecorder()
			px.ServeHTTP(resp, req)
			if resp.Code != http.StatusBadGateway {
				t.Fatalf("status=%d, want 502", resp.Code)
			}
			if tracker.begins.Load() != 1 || tracker.closes.Load() != 1 || tracker.attaches.Load() != 0 || tracker.service != test.want {
				t.Fatalf("tracker begins=%d closes=%d attaches=%d service=%q, want %q",
					tracker.begins.Load(), tracker.closes.Load(), tracker.attaches.Load(), tracker.service, test.want)
			}
		})
	}
}

func TestProxyForwardAndAuth(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	upSock := filepath.Join(t.TempDir(), "up.sock")
	upLn, err := net.Listen("unix", upSock)
	if err != nil {
		t.Fatal(err)
	}
	defer upLn.Close()
	up := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "hello from envd")
	})}
	go up.Serve(upLn)
	defer up.Close()

	mode := "enforce"
	px := proxy.New(stubRouter{r: proxy.Route{Kind: proxy.KindUDS, UDS: upSock}, token: "tok"},
		func() string { return mode }, log, nil)
	ts := httptest.NewServer(px)
	defer ts.Close()

	do := func(token, path, query, host string) (int, string) {
		req, _ := http.NewRequest("GET", ts.URL+path+query, nil)
		if host == "" {
			req.Header.Set("E2b-Sandbox-Id", "s1")
			req.Header.Set("E2b-Sandbox-Port", "49983")
		} else {
			req.Host = host
		}
		if token != "" {
			req.Header.Set("X-Access-Token", token)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b)
	}

	if code, body := do("tok", "/echo", "", ""); code != 200 || body != "hello from envd" {
		t.Fatalf("valid token: code=%d body=%q", code, body)
	}
	if code, _ := do("wrong", "/echo", "", ""); code != 401 {
		t.Fatalf("wrong token: code=%d (want 401)", code)
	}
	if code, _ := do("", "/echo", "", ""); code != 401 {
		t.Fatalf("missing token: code=%d (want 401)", code)
	}
	if code, _ := do("", "/echo", "?signature=abc&username=user", ""); code != 401 {
		t.Fatalf("non-file signature bypass: code=%d (want 401)", code)
	}
	sig := envdsign.Signature("/tmp/a.txt", "", envdsign.OperationRead, "tok", nil)
	query := "?path=" + url.QueryEscape("/tmp/a.txt") + "&signature=" + url.QueryEscape(sig)
	if code, body := do("", "/files", query, ""); code != 200 || body != "hello from envd" {
		t.Fatalf("pre-signed file url: code=%d body=%q", code, body)
	}
	if code, _ := do("", "/files", "?path=/tmp/a.txt&signature=bad", ""); code != 401 {
		t.Fatalf("bad file signature: code=%d (want 401)", code)
	}
	if code, _ := do("", "/files", query, "8080-s1.test.local"); code != 401 {
		t.Fatalf("file signature on user port: code=%d (want 401)", code)
	}
	mode = "off"
	if code, _ := do("", "/echo", "", ""); code != 200 {
		t.Fatalf("auth off: code=%d (want 200)", code)
	}
	mode = "log"
	if code, body := do("wrong", "/echo", "", ""); code != 200 || body != "hello from envd" {
		t.Fatalf("auth log forwards: code=%d body=%q", code, body)
	}
}

func TestProxyMissingExpectedTokenHonorsAuthMode(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	upSock := filepath.Join(t.TempDir(), "up.sock")
	upLn, err := net.Listen("unix", upSock)
	if err != nil {
		t.Fatal(err)
	}
	defer upLn.Close()
	up := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "ok")
	})}
	go up.Serve(upLn)
	defer up.Close()

	mode := "enforce"
	px := proxy.New(stubRouter{r: proxy.Route{Kind: proxy.KindUDS, UDS: upSock}},
		func() string { return mode }, log, nil)
	ts := httptest.NewServer(px)
	defer ts.Close()

	request := func() int {
		req, _ := http.NewRequest(http.MethodGet, ts.URL+"/echo", nil)
		req.Header.Set(proxy.HeaderSandboxID, "s1")
		req.Header.Set(proxy.HeaderSandboxPort, "49983")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}

	if code := request(); code != http.StatusUnauthorized {
		t.Fatalf("missing expected token in enforce mode: code=%d, want 401", code)
	}
	mode = "log"
	if code := request(); code != http.StatusOK {
		t.Fatalf("missing expected token in log mode: code=%d, want 200", code)
	}
	mode = "off"
	if code := request(); code != http.StatusOK {
		t.Fatalf("missing expected token with auth off: code=%d, want 200", code)
	}
}

func TestProxyDoesNotReuseBackendConnections(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	upSock := filepath.Join(t.TempDir(), "up.sock")
	rawLn, err := net.Listen("unix", upSock)
	if err != nil {
		t.Fatal(err)
	}
	upLn := &countingListener{Listener: rawLn}
	defer upLn.Close()
	up := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "ok")
	})}
	go up.Serve(upLn)
	defer up.Close()

	px := proxy.New(stubRouter{r: proxy.Route{Kind: proxy.KindUDS, UDS: upSock}, token: "tok"},
		func() string { return "enforce" }, log, nil)
	ts := httptest.NewServer(px)
	defer ts.Close()

	client := &http.Client{}
	for i := 0; i < 2; i++ {
		req, _ := http.NewRequest("GET", ts.URL+"/echo", nil)
		req.Header.Set("E2b-Sandbox-Id", "s1")
		req.Header.Set("E2b-Sandbox-Port", "49983")
		req.Header.Set("X-Access-Token", "tok")
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("request %d status=%d, want 200", i, resp.StatusCode)
		}
	}
	if got := upLn.accepts.Load(); got != 2 {
		t.Fatalf("backend accepts=%d, want 2 (one fresh upstream connection per request)", got)
	}
}

func TestProxyUsesCustomDialerForHTTP(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	backLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer backLn.Close()
	go func() {
		for {
			c, err := backLn.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				br := bufio.NewReader(c)
				if _, err := http.ReadRequest(br); err != nil {
					return
				}
				_, _ = io.WriteString(c, "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
			}(c)
		}
	}()

	var dials atomic.Int32
	px := proxy.NewWithDialer(stubRouter{r: proxy.Route{Kind: proxy.KindTCP, Addr: backLn.Addr().String()}, token: "tok"},
		func() string { return "enforce" }, log, nil,
		func(ctx context.Context, r proxy.Route) (net.Conn, error) {
			dials.Add(1)
			if r.Kind != proxy.KindTCP {
				return nil, fmt.Errorf("dial route kind = %v, want KindTCP", r.Kind)
			}
			return (&net.Dialer{}).DialContext(ctx, "tcp", r.Addr)
		}, "")
	ts := httptest.NewServer(px)
	defer ts.Close()

	req, _ := http.NewRequest("GET", ts.URL+"/echo", nil)
	req.Header.Set("E2b-Sandbox-Id", "s1")
	req.Header.Set("E2b-Sandbox-Port", "8080")
	req.Header.Set("X-Access-Token", "tok")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK || string(b) != "ok" {
		t.Fatalf("response code=%d body=%q", resp.StatusCode, string(b))
	}
	if got := dials.Load(); got != 1 {
		t.Fatalf("custom dials=%d, want 1", got)
	}
}

// TestConnectTunnel drives an HTTP/1.1 CONNECT: the target host is ignored (only the
// port is honored), the access token is enforced, and bytes splice both ways to the
// resolved backend.
func TestConnectTunnel(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	// Backend the tunnel splices to: a byte echo server.
	backLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer backLn.Close()
	go func() {
		for {
			c, err := backLn.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) { defer c.Close(); _, _ = io.Copy(c, c) }(c)
		}
	}()

	var dials atomic.Int32
	px := proxy.NewWithDialer(stubRouter{r: proxy.Route{Kind: proxy.KindTCP, Addr: backLn.Addr().String()}, token: "tok"},
		func() string { return "enforce" }, log, nil,
		func(ctx context.Context, r proxy.Route) (net.Conn, error) {
			dials.Add(1)
			if r.Kind != proxy.KindTCP {
				return nil, fmt.Errorf("dial route kind = %v, want KindTCP", r.Kind)
			}
			return (&net.Dialer{}).DialContext(ctx, "tcp", r.Addr)
		}, "")
	ts := httptest.NewServer(px)
	defer ts.Close()
	_, bport, _ := net.SplitHostPort(backLn.Addr().String())

	connect := func(token string) (int, net.Conn, *bufio.Reader) {
		c, err := net.Dial("tcp", ts.Listener.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		var hdr strings.Builder
		// Target host "ignored" proves the host is dropped; only :<bport> matters.
		fmt.Fprintf(&hdr, "CONNECT ignored:%s HTTP/1.1\r\nHost: ignored:%s\r\nE2b-Sandbox-Id: s1\r\nE2b-Sandbox-Port: %s\r\n", bport, bport, bport)
		if token != "" {
			fmt.Fprintf(&hdr, "X-Access-Token: %s\r\n", token)
		}
		hdr.WriteString("\r\n")
		if _, err := io.WriteString(c, hdr.String()); err != nil {
			t.Fatal(err)
		}
		br := bufio.NewReader(c)
		resp, err := http.ReadResponse(br, &http.Request{Method: http.MethodConnect})
		if err != nil {
			t.Fatal(err)
		}
		return resp.StatusCode, c, br
	}

	// Valid token: 200, and the tunnel echoes.
	code, c, br := connect("tok")
	if code != http.StatusOK {
		t.Fatalf("CONNECT code = %d (want 200)", code)
	}
	if _, err := io.WriteString(c, "ping\n"); err != nil {
		t.Fatal(err)
	}
	line, _ := br.ReadString('\n')
	if strings.TrimSpace(line) != "ping" {
		t.Fatalf("tunnel echo = %q (want ping)", line)
	}
	c.Close()
	if got := dials.Load(); got != 1 {
		t.Fatalf("CONNECT custom dials=%d, want 1", got)
	}

	// Missing token in enforce mode: 401, no tunnel.
	code, c2, _ := connect("")
	if code != http.StatusUnauthorized {
		t.Fatalf("CONNECT without token = %d (want 401)", code)
	}
	c2.Close()
	if got := dials.Load(); got != 1 {
		t.Fatalf("CONNECT unauthorized dials=%d, want still 1", got)
	}
}

func TestProxyNotFoundAndDeny(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	nf := proxy.New(stubRouter{r: proxy.Route{Kind: proxy.KindNotFound}}, nil, log, nil)
	deny := proxy.New(stubRouter{r: proxy.Route{Kind: proxy.KindDeny}}, nil, log, nil)
	for _, tc := range []struct {
		px   *proxy.Proxy
		want int
	}{{nf, 404}, {deny, 501}} {
		ts := httptest.NewServer(tc.px)
		req, _ := http.NewRequest("GET", ts.URL+"/x", nil)
		req.Header.Set("E2b-Sandbox-Id", "s1")
		req.Header.Set("E2b-Sandbox-Port", "49983")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		ts.Close()
		if resp.StatusCode != tc.want {
			t.Fatalf("got %d want %d", resp.StatusCode, tc.want)
		}
	}
}
