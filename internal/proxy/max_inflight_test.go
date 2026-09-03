package proxy_test

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

	publicconfig "github.com/kuasar-sandbox/orchestrator/config"
	"github.com/kuasar-sandbox/orchestrator/internal/metrics"
	"github.com/kuasar-sandbox/orchestrator/internal/proxy"
	"github.com/kuasar-sandbox/orchestrator/internal/proxyadmission"
	"github.com/kuasar-sandbox/orchestrator/internal/proxystats"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

type limitedTrafficHarness struct {
	master  *proxyadmission.Master
	worker  *proxyadmission.Worker
	traffic *proxystats.WorkerStats
	stats   *proxystats.MasterStats
}

func newLimitedTrafficHarness(t testing.TB) *limitedTrafficHarness {
	t.Helper()
	master, err := proxyadmission.NewMaster(8, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := master.BeginWorker(0, 1); err != nil {
		_ = master.Close()
		t.Fatal(err)
	}
	file, err := master.DupFile()
	if err != nil {
		_ = master.Close()
		t.Fatal(err)
	}
	worker, err := proxyadmission.OpenWorker(file, 8, 1, 0, 1)
	if err != nil {
		_ = master.Close()
		t.Fatal(err)
	}
	stats := proxystats.NewMasterStats(metrics.New(), []string{"limited"})
	if err := stats.BeginWorker("limited", 1); err != nil {
		_ = worker.Close()
		_ = master.Close()
		t.Fatal(err)
	}
	harness := &limitedTrafficHarness{
		master: master, worker: worker,
		traffic: proxystats.NewWorkerStatsWithAdmission(worker),
		stats:   stats,
	}
	ctx, cancel := context.WithCancel(context.Background())
	if _, err := harness.traffic.StartSender(ctx, "limited", 1, func(frame proxystats.Frame) error {
		return stats.Receive("limited", 1, frame)
	}); err != nil {
		cancel()
		_ = worker.Close()
		_ = master.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cancel()
		_ = worker.Close()
		_ = master.Close()
	})
	return harness
}

func (h *limitedTrafficHarness) assertAvailable(t *testing.T, binding proxyadmission.Binding, service proxyadmission.Service) {
	t.Helper()
	lease, err := h.worker.TryAcquire(binding, service)
	if err != nil {
		t.Fatalf("admission capacity was not released: %v", err)
	}
	lease.Release()
}

func (h *limitedTrafficHarness) bind(t testing.TB, sid string, limits publicconfig.MaxInflight) proxyadmission.Binding {
	t.Helper()
	update, err := h.master.PrepareUpsert(sid, sid+"-identity", limits)
	if err != nil {
		t.Fatal(err)
	}
	binding := update.Binding()
	update.Commit()
	return binding
}

func ordinaryRequest(method, sid string, port int, token string) *http.Request {
	req := httptest.NewRequest(method, "http://sandbox/", nil)
	req.Host = "sandbox"
	req.Header.Set(proxy.HeaderSandboxID, sid)
	req.Header.Set(proxy.HeaderSandboxPort, fmt.Sprint(port))
	req.Header.Set(proxy.HeaderAccessToken, token)
	return req
}

func metricValue(t *testing.T, registry *metrics.M, name string) int {
	t.Helper()
	response := httptest.NewRecorder()
	registry.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	for _, line := range strings.Split(response.Body.String(), "\n") {
		var gotName string
		var value int
		if _, err := fmt.Sscanf(line, "%s %d", &gotName, &value); err == nil && gotName == name {
			return value
		}
	}
	return 0
}

func TestMaxInflightRejectsAfterCredentialAdmissionWithoutLifecycleSideEffects(t *testing.T) {
	harness := newLimitedTrafficHarness(t)
	target := proxy.LegacyTarget(8080)
	binding := proxy.BindRoute("node-s1", "stable-s1", types.ProfileE2B, "envd", "forward", target)
	binding.Admission = harness.bind(t, binding.SandboxID, publicconfig.MaxInflight{Total: 1})
	router := &admissionRouter{
		binding: binding, route: proxy.Route{Kind: proxy.KindTCP, Addr: "unused"}, found: true, activateOK: true,
	}
	registry := metrics.New()
	var dials atomic.Int32
	tracker := &countingAdmissionTracker{traffic: harness.traffic}
	px := proxy.NewWithDialer(router, func() string { return "enforce" }, nil, registry,
		func(context.Context, proxy.Route) (net.Conn, error) {
			dials.Add(1)
			return nil, errors.New("dial failed")
		}, "").WithTrafficTracker(tracker)

	unauthorized := httptest.NewRecorder()
	px.ServeHTTP(unauthorized, ordinaryRequest(http.MethodGet, binding.SandboxID, 8080, "wrong"))
	if unauthorized.Code != http.StatusUnauthorized || router.activateCalls.Load() != 0 || dials.Load() != 0 || tracker.tries.Load() != 0 {
		t.Fatalf("unauthorized response=%d admission=%d activate=%d dial=%d", unauthorized.Code, tracker.tries.Load(), router.activateCalls.Load(), dials.Load())
	}
	// A subsequent authorized request must still acquire the only slot. Its dial
	// failure also proves that the lease is released on the failure path.
	authorized := httptest.NewRecorder()
	px.ServeHTTP(authorized, ordinaryRequest(http.MethodGet, binding.SandboxID, 8080, "forward"))
	if authorized.Code != http.StatusBadGateway || router.activateCalls.Load() != 1 || dials.Load() != 1 {
		t.Fatalf("authorized response=%d activate=%d dial=%d", authorized.Code, router.activateCalls.Load(), dials.Load())
	}
	again := httptest.NewRecorder()
	px.ServeHTTP(again, ordinaryRequest(http.MethodGet, binding.SandboxID, 8080, "forward"))
	if again.Code != http.StatusBadGateway || router.activateCalls.Load() != 2 || dials.Load() != 2 {
		t.Fatalf("post-failure response=%d activate=%d dial=%d", again.Code, router.activateCalls.Load(), dials.Load())
	}
}

func TestMaxInflightActivationFailureReleases(t *testing.T) {
	harness := newLimitedTrafficHarness(t)
	target := proxy.LegacyTarget(8080)
	binding := proxy.BindRoute("node-s1", "stable-s1", types.ProfileE2B, "envd", "forward", target)
	binding.Admission = harness.bind(t, binding.SandboxID, publicconfig.MaxInflight{Forward: 1})
	router := &admissionRouter{
		binding: binding, route: proxy.Route{Kind: proxy.KindTCP, Addr: "unused"}, found: true,
	}
	var dials atomic.Int32
	px := proxy.NewWithDialer(router, func() string { return "enforce" }, nil, nil,
		func(context.Context, proxy.Route) (net.Conn, error) {
			dials.Add(1)
			return nil, errors.New("unexpected dial")
		}, "").WithTrafficTracker(harness.traffic)

	response := httptest.NewRecorder()
	px.ServeHTTP(response, ordinaryRequest(http.MethodGet, binding.SandboxID, 8080, "forward"))
	if response.Code != http.StatusNotFound || router.activateCalls.Load() != 1 || dials.Load() != 0 {
		t.Fatalf("response=%d activate=%d dial=%d", response.Code, router.activateCalls.Load(), dials.Load())
	}
	harness.assertAvailable(t, binding.Admission, proxyadmission.ServiceForward)
}

func TestMaxInflightReachedReturnsTyped429BeforeActivationAndDial(t *testing.T) {
	for _, method := range []string{http.MethodGet, http.MethodConnect} {
		t.Run(method, func(t *testing.T) {
			harness := newLimitedTrafficHarness(t)
			target := proxy.LegacyTarget(8080)
			binding := proxy.BindRoute("node-s1", "stable-s1", types.ProfileE2B, "envd", "forward", target)
			binding.Admission = harness.bind(t, binding.SandboxID, publicconfig.MaxInflight{Total: 1})
			held, err := harness.traffic.TryBeginParking(binding.SandboxID, proxy.ConnectServiceExec, binding.Admission)
			if err != nil {
				t.Fatal(err)
			}
			defer held.Close()
			router := &admissionRouter{
				binding: binding, route: proxy.Route{Kind: proxy.KindTCP, Addr: "unused"}, found: true, activateOK: true,
			}
			registry := metrics.New()
			var dials atomic.Int32
			px := proxy.NewWithDialer(router, func() string { return "enforce" }, nil, registry,
				func(context.Context, proxy.Route) (net.Conn, error) {
					dials.Add(1)
					return nil, errors.New("unexpected dial")
				}, "").WithTrafficTracker(harness.traffic)

			response := httptest.NewRecorder()
			px.ServeHTTP(response, ordinaryRequest(method, binding.SandboxID, 8080, "forward"))
			if response.Code != http.StatusTooManyRequests || response.Body.String() != "max inflight reached\n" ||
				response.Header().Get(proxy.HeaderProxyError) != proxy.ProxyErrorMaxInflightReached ||
				response.Header().Get("Retry-After") != "" {
				t.Fatalf("response=%d error=%q retry=%q body=%q", response.Code,
					response.Header().Get(proxy.HeaderProxyError), response.Header().Get("Retry-After"), response.Body.String())
			}
			if router.activateCalls.Load() != 0 || dials.Load() != 0 {
				t.Fatalf("limit rejection activate=%d dial=%d", router.activateCalls.Load(), dials.Load())
			}
			if got := metricValue(t, registry, `data_requests_total{result="max_inflight_reached"}`); got != 1 {
				t.Fatalf("max_inflight_reached counter=%d, want 1", got)
			}
		})
	}
}

func TestMaxInflightServiceAndSandboxIsolation(t *testing.T) {
	harness := newLimitedTrafficHarness(t)
	limits := publicconfig.MaxInflight{Total: 4, E2BEnvd: 1, Forward: 2}
	binding := harness.bind(t, "node-s1", limits)
	held, err := harness.traffic.TryBeginParking("node-s1", proxy.ConnectServiceE2BEnvd, binding)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()

	request := func(sid string, target proxy.ConnectTarget, admission proxyadmission.Binding) (*httptest.ResponseRecorder, *admissionRouter) {
		routeBinding := proxy.BindRoute(sid, "stable-"+sid, types.ProfileE2B, "envd", "forward", target)
		routeBinding.Admission = admission
		router := &admissionRouter{binding: routeBinding, route: proxy.Route{Kind: routeBinding.Kind}, found: true, activateOK: true}
		px := proxy.NewWithDialer(router, func() string { return "enforce" }, nil, nil,
			func(context.Context, proxy.Route) (net.Conn, error) { return nil, errors.New("dial failed") }, "").
			WithTrafficTracker(harness.traffic)
		response := httptest.NewRecorder()
		port := target.Port
		if port == 0 {
			port = 49983
		}
		px.ServeHTTP(response, ordinaryRequest(http.MethodGet, sid, port, routeBinding.ExpectedAccessToken))
		return response, router
	}

	envdResponse, envdRouter := request("node-s1", proxy.LegacyTarget(49983), binding)
	if envdResponse.Code != http.StatusTooManyRequests || envdRouter.activateCalls.Load() != 0 {
		t.Fatalf("envd response=%d activate=%d", envdResponse.Code, envdRouter.activateCalls.Load())
	}
	forwardResponse, forwardRouter := request("node-s1", proxy.LegacyTarget(8080), binding)
	if forwardResponse.Code != http.StatusBadGateway || forwardRouter.activateCalls.Load() != 1 {
		t.Fatalf("forward response=%d activate=%d", forwardResponse.Code, forwardRouter.activateCalls.Load())
	}
	otherBinding := harness.bind(t, "node-s2", publicconfig.MaxInflight{Total: 1})
	otherResponse, otherRouter := request("node-s2", proxy.LegacyTarget(8080), otherBinding)
	if otherResponse.Code != http.StatusBadGateway || otherRouter.activateCalls.Load() != 1 {
		t.Fatalf("other Sandbox response=%d activate=%d", otherResponse.Code, otherRouter.activateCalls.Load())
	}
}

type countingAdmissionTracker struct {
	traffic *proxystats.WorkerStats
	tries   atomic.Int32
}

func (t *countingAdmissionTracker) BeginParking(sid string, service proxy.ConnectService) proxy.TrafficFlow {
	return t.traffic.BeginParking(sid, service)
}

func (t *countingAdmissionTracker) TryBeginParking(sid string, service proxy.ConnectService, binding proxyadmission.Binding) (proxy.TrafficFlow, error) {
	t.tries.Add(1)
	return t.traffic.TryBeginParking(sid, service, binding)
}

func TestForwardAuthorizedUsesAdmissionExactlyOnceAndReleasesOnFailure(t *testing.T) {
	harness := newLimitedTrafficHarness(t)
	target := proxy.ConnectTarget{Service: proxy.ConnectServiceForward, Port: 8080}
	binding := proxy.BindRoute("node-s1", "stable-s1", types.ProfileE2B, "envd", "forward", target)
	binding.Admission = harness.bind(t, binding.SandboxID, publicconfig.MaxInflight{Forward: 1})
	router := &admissionRouter{
		binding: binding, route: proxy.Route{Kind: proxy.KindTCP, Addr: "unused"}, found: true, activateOK: true,
	}
	tracker := &countingAdmissionTracker{traffic: harness.traffic}
	px := proxy.NewWithDialer(router, nil, nil, nil,
		func(context.Context, proxy.Route) (net.Conn, error) { return nil, errors.New("dial failed") }, "").
		WithTrafficTracker(tracker)
	response := httptest.NewRecorder()
	px.ForwardAuthorized(response, httptest.NewRequest(http.MethodGet, "http://private/", nil), proxy.AuthorizedForwardRequest{
		SandboxID: binding.SandboxID, Target: target,
	})
	if response.Code != http.StatusBadGateway || tracker.tries.Load() != 1 || router.activateCalls.Load() != 1 {
		t.Fatalf("response=%d admission=%d activate=%d", response.Code, tracker.tries.Load(), router.activateCalls.Load())
	}
	harness.assertAvailable(t, binding.Admission, proxyadmission.ServiceForward)
}

func TestMaxInflightHTTPForwardErrorReleases(t *testing.T) {
	harness := newLimitedTrafficHarness(t)
	target := proxy.LegacyTarget(8080)
	binding := proxy.BindRoute("node-s1", "stable-s1", types.ProfileE2B, "envd", "forward", target)
	binding.Admission = harness.bind(t, binding.SandboxID, publicconfig.MaxInflight{Forward: 1})
	router := &admissionRouter{
		binding: binding, route: proxy.Route{Kind: proxy.KindTCP, Addr: "unused"}, found: true, activateOK: true,
	}
	px := proxy.NewWithDialer(router, nil, nil, nil,
		func(context.Context, proxy.Route) (net.Conn, error) {
			backend, proxySide := net.Pipe()
			_ = backend.Close()
			return proxySide, nil
		}, "").WithTrafficTracker(harness.traffic)
	for attempt := 0; attempt < 2; attempt++ {
		response := httptest.NewRecorder()
		px.ServeHTTP(response, ordinaryRequest(http.MethodGet, binding.SandboxID, 8080, "forward"))
		if response.Code != http.StatusBadGateway {
			body, _ := io.ReadAll(response.Result().Body)
			t.Fatalf("attempt %d response=%d body=%q", attempt, response.Code, body)
		}
	}
}

func TestMaxInflightHTTPContextCancellationReleases(t *testing.T) {
	harness := newLimitedTrafficHarness(t)
	target := proxy.LegacyTarget(8080)
	binding := proxy.BindRoute("node-s1", "stable-s1", types.ProfileE2B, "envd", "forward", target)
	binding.Admission = harness.bind(t, binding.SandboxID, publicconfig.MaxInflight{Forward: 1})
	router := &admissionRouter{
		binding: binding, route: proxy.Route{Kind: proxy.KindTCP, Addr: "unused"}, found: true, activateOK: true,
	}
	requestReceived := make(chan struct{})
	px := proxy.NewWithDialer(router, nil, nil, nil,
		func(context.Context, proxy.Route) (net.Conn, error) {
			proxySide, guestSide := net.Pipe()
			go func() {
				defer guestSide.Close()
				request, err := http.ReadRequest(bufio.NewReader(guestSide))
				if err == nil {
					_ = request.Body.Close()
					close(requestReceived)
				}
				_, _ = io.Copy(io.Discard, guestSide)
			}()
			return proxySide, nil
		}, "").WithTrafficTracker(harness.traffic)

	ctx, cancel := context.WithCancel(context.Background())
	request := ordinaryRequest(http.MethodGet, binding.SandboxID, 8080, "forward").WithContext(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		px.ServeHTTP(httptest.NewRecorder(), request)
	}()
	select {
	case <-requestReceived:
	case <-time.After(time.Second):
		t.Fatal("guest did not receive the admitted request")
	}
	if _, err := harness.worker.TryAcquire(binding.Admission, proxyadmission.ServiceForward); err == nil {
		t.Fatal("blocked HTTP request did not hold its admission lease")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("context cancellation did not unblock the HTTP forward")
	}
	harness.assertAvailable(t, binding.Admission, proxyadmission.ServiceForward)
}

func TestIngressWrapperNextAcquiresOnceAndOrdinaryResponseReleases(t *testing.T) {
	harness := newLimitedTrafficHarness(t)
	target := proxy.LegacyTarget(8080)
	binding := proxy.BindRoute("node-s1", "stable-s1", types.ProfileE2B, "envd", "forward", target)
	binding.Admission = harness.bind(t, binding.SandboxID, publicconfig.MaxInflight{Forward: 1})
	router := &admissionRouter{
		binding: binding, route: proxy.Route{Kind: proxy.KindTCP, Addr: "unused"}, found: true, activateOK: true,
	}
	tracker := &countingAdmissionTracker{traffic: harness.traffic}
	px := proxy.NewWithDialer(router, nil, nil, nil,
		func(context.Context, proxy.Route) (net.Conn, error) {
			proxySide, guestSide := net.Pipe()
			go func() {
				defer guestSide.Close()
				request, err := http.ReadRequest(bufio.NewReader(guestSide))
				if err == nil {
					_ = request.Body.Close()
					_, _ = io.WriteString(guestSide, "HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n")
				}
			}()
			return proxySide, nil
		}, "").WithTrafficTracker(tracker)
	wrapped := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		px.ServeHTTP(w, r)
	})
	response := httptest.NewRecorder()
	wrapped.ServeHTTP(response, ordinaryRequest(http.MethodGet, binding.SandboxID, 8080, "forward"))
	if response.Code != http.StatusNoContent || tracker.tries.Load() != 1 {
		t.Fatalf("response=%d admission=%d, want 204/1", response.Code, tracker.tries.Load())
	}
	harness.assertAvailable(t, binding.Admission, proxyadmission.ServiceForward)
}

func BenchmarkOrdinaryHTTPMaxInflight(b *testing.B) {
	for _, limited := range []bool{false, true} {
		name := "unlimited"
		if limited {
			name = "limited"
		}
		b.Run(name, func(b *testing.B) {
			harness := newBenchmarkTrafficHarness(b)
			target := proxy.LegacyTarget(8080)
			binding := proxy.BindRoute("node-s1", "stable-s1", types.ProfileE2B, "envd", "forward", target)
			if limited {
				binding.Admission = harness.bind(b, binding.SandboxID, publicconfig.MaxInflight{Forward: 1 << 30})
			}
			router := &admissionRouter{
				binding: binding, route: proxy.Route{Kind: proxy.KindTCP, Addr: "benchmark"}, found: true, activateOK: true,
			}
			px := proxy.NewWithDialer(router, func() string { return "enforce" }, nil, nil,
				func(context.Context, proxy.Route) (net.Conn, error) {
					proxySide, guestSide := net.Pipe()
					go func() {
						defer guestSide.Close()
						request, err := http.ReadRequest(bufio.NewReader(guestSide))
						if err == nil {
							_ = request.Body.Close()
							_, err = io.WriteString(guestSide, "HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n")
						}
					}()
					return proxySide, nil
				}, "").WithTrafficTracker(harness.traffic)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				response := httptest.NewRecorder()
				px.ServeHTTP(response, ordinaryRequest(http.MethodGet, binding.SandboxID, 8080, "forward"))
				if response.Code != http.StatusNoContent {
					b.Fatalf("response status = %d", response.Code)
				}
			}
		})
	}
}

type benchmarkHijackWriter struct {
	header http.Header
	conn   net.Conn
}

func (w *benchmarkHijackWriter) Header() http.Header       { return w.header }
func (*benchmarkHijackWriter) Write(p []byte) (int, error) { return len(p), nil }
func (*benchmarkHijackWriter) WriteHeader(int)             {}
func (w *benchmarkHijackWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return w.conn, bufio.NewReadWriter(bufio.NewReader(w.conn), bufio.NewWriter(w.conn)), nil
}

func BenchmarkConnectEstablishmentMaxInflight(b *testing.B) {
	for _, limited := range []bool{false, true} {
		name := "unlimited"
		if limited {
			name = "limited"
		}
		b.Run(name, func(b *testing.B) {
			harness := newBenchmarkTrafficHarness(b)
			target := proxy.LegacyTarget(8080)
			binding := proxy.BindRoute("node-s1", "stable-s1", types.ProfileE2B, "envd", "forward", target)
			if limited {
				binding.Admission = harness.bind(b, binding.SandboxID, publicconfig.MaxInflight{Forward: 1 << 30})
			}
			router := &admissionRouter{
				binding: binding, route: proxy.Route{Kind: proxy.KindTCP, Addr: "benchmark"}, found: true, activateOK: true,
			}
			px := proxy.NewWithDialer(router, func() string { return "enforce" }, nil, nil,
				func(context.Context, proxy.Route) (net.Conn, error) {
					proxySide, guestSide := net.Pipe()
					_ = guestSide.Close()
					return proxySide, nil
				}, "").WithTrafficTracker(harness.traffic)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				proxySide, clientSide := net.Pipe()
				writer := &benchmarkHijackWriter{header: make(http.Header), conn: proxySide}
				clientDone := make(chan error, 1)
				go func() {
					response, err := http.ReadResponse(bufio.NewReader(clientSide), &http.Request{Method: http.MethodConnect})
					if err == nil {
						_ = response.Body.Close()
					}
					_ = clientSide.Close()
					clientDone <- err
				}()
				px.ServeHTTP(writer, ordinaryRequest(http.MethodConnect, binding.SandboxID, 8080, "forward"))
				if err := <-clientDone; err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func newBenchmarkTrafficHarness(b *testing.B) *limitedTrafficHarness {
	b.Helper()
	master, err := proxyadmission.NewMaster(8, 1)
	if err != nil {
		b.Fatal(err)
	}
	if err := master.BeginWorker(0, 1); err != nil {
		_ = master.Close()
		b.Fatal(err)
	}
	file, err := master.DupFile()
	if err != nil {
		_ = master.Close()
		b.Fatal(err)
	}
	worker, err := proxyadmission.OpenWorker(file, 8, 1, 0, 1)
	if err != nil {
		_ = master.Close()
		b.Fatal(err)
	}
	b.Cleanup(func() {
		_ = worker.Close()
		_ = master.Close()
	})
	return &limitedTrafficHarness{
		master:  master,
		worker:  worker,
		traffic: proxystats.NewWorkerStatsWithAdmission(worker),
	}
}
