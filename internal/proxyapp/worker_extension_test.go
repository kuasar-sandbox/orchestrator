package proxyapp

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	proxyextension "github.com/kuasar-sandbox/orchestrator/app/proxy/extension"
	internalproxy "github.com/kuasar-sandbox/orchestrator/internal/proxy"
	"github.com/kuasar-sandbox/orchestrator/internal/proxyshm"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

type testWorkerHost struct {
	process proxyextension.Process
	route   proxyextension.RouteView
	found   bool
}

type markerHandler struct{}

func (*markerHandler) ServeHTTP(w http.ResponseWriter, _ *http.Request) { http.NotFound(w, nil) }

func (h *testWorkerHost) Process() proxyextension.Process { return h.process }
func (h *testWorkerHost) GetRoute(string) (proxyextension.RouteView, bool) {
	return h.route, h.found
}
func (*testWorkerHost) ForwardAuthorized(http.ResponseWriter, *http.Request, proxyextension.ForwardRequest) {
}

type startOnlyWorkerExtension struct {
	calls atomic.Int32
	start func(context.Context, proxyextension.WorkerHost) error
}

func (e *startOnlyWorkerExtension) Start(ctx context.Context, host proxyextension.WorkerHost) error {
	e.calls.Add(1)
	if e.start != nil {
		return e.start(ctx, host)
	}
	return nil
}

type wrappingWorkerExtension struct {
	startOnlyWorkerExtension
	wrapCalls atomic.Int32
	wrap      func(http.Handler) http.Handler
}

func (e *wrappingWorkerExtension) WrapIngress(next http.Handler) http.Handler {
	e.wrapCalls.Add(1)
	if e.wrap != nil {
		return e.wrap(next)
	}
	return next
}

func TestStartWorkerIngressFreezesOneExtensionAndWrapsRawRequests(t *testing.T) {
	process := proxyextension.Process{Role: proxyextension.RoleWorker, WorkerID: "proxy-4", WorkerEpoch: 9}
	host := &testWorkerHost{
		process: process, found: true,
		route: proxyextension.RouteView{SandboxID: "s1", Revision: 12},
	}
	var nextCalls atomic.Int32
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		nextCalls.Add(1)
		http.Error(w, "core parser", http.StatusBadRequest)
	})
	extension := &wrappingWorkerExtension{}
	extension.start = func(ctx context.Context, got proxyextension.WorkerHost) error {
		if ctx == nil || got.Process() != process {
			t.Fatalf("Start host process=%+v", got.Process())
		}
		view, found := got.GetRoute("s1")
		if !found || view.Revision != 12 {
			t.Fatalf("Start GetRoute=%+v found=%v", view, found)
		}
		return nil
	}
	extension.wrap = func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/private/raw" && r.Header.Get("X-Sandbox-Id") != "" {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
	handler, err := startWorkerIngress(context.Background(), extension, host, next)
	if err != nil {
		t.Fatal(err)
	}
	if extension.calls.Load() != 1 || extension.wrapCalls.Load() != 1 {
		t.Fatalf("Start=%d WrapIngress=%d", extension.calls.Load(), extension.wrapCalls.Load())
	}
	privateRequest := httptest.NewRequest(http.MethodGet, "http://not-canonical/private/raw", nil)
	privateRequest.Header.Set("X-Sandbox-Id", "s1")
	privateResponse := httptest.NewRecorder()
	handler.ServeHTTP(privateResponse, privateRequest)
	if privateResponse.Code != http.StatusNoContent || nextCalls.Load() != 0 {
		t.Fatalf("private response=%d next=%d", privateResponse.Code, nextCalls.Load())
	}
	defaultResponse := httptest.NewRecorder()
	handler.ServeHTTP(defaultResponse, httptest.NewRequest(http.MethodGet, "http://not-canonical/unmatched", nil))
	if defaultResponse.Code != http.StatusBadRequest || nextCalls.Load() != 1 {
		t.Fatalf("default response=%d next=%d", defaultResponse.Code, nextCalls.Load())
	}
}

func TestStartWorkerIngressFailureAndNilWrapperPrecedeServe(t *testing.T) {
	want := errors.New("worker policy unavailable")
	failed := &wrappingWorkerExtension{}
	failed.start = func(context.Context, proxyextension.WorkerHost) error { return want }
	if handler, err := startWorkerIngress(context.Background(), failed, &testWorkerHost{}, http.NotFoundHandler()); handler != nil || !errors.Is(err, want) {
		t.Fatalf("Start failure handler=%v err=%v", handler, err)
	}
	if failed.calls.Load() != 1 || failed.wrapCalls.Load() != 0 {
		t.Fatalf("failed Start=%d WrapIngress=%d", failed.calls.Load(), failed.wrapCalls.Load())
	}

	nilWrapper := &wrappingWorkerExtension{wrap: func(http.Handler) http.Handler { return nil }}
	if handler, err := startWorkerIngress(context.Background(), nilWrapper, &testWorkerHost{}, http.NotFoundHandler()); handler != nil || err == nil || !strings.Contains(err.Error(), "nil ingress handler") {
		t.Fatalf("nil wrapper handler=%v err=%v", handler, err)
	}
	if nilWrapper.calls.Load() != 1 || nilWrapper.wrapCalls.Load() != 1 {
		t.Fatalf("nil wrapper Start=%d WrapIngress=%d", nilWrapper.calls.Load(), nilWrapper.wrapCalls.Load())
	}
}

func TestStartWorkerIngressNilAndStartOnlyKeepCoreHandler(t *testing.T) {
	next := &markerHandler{}
	if handler, err := startWorkerIngress(context.Background(), nil, nil, next); err != nil || handler != next {
		t.Fatalf("nil extension handler=%v err=%v", handler, err)
	}
	extension := &startOnlyWorkerExtension{}
	if handler, err := startWorkerIngress(context.Background(), extension, &testWorkerHost{}, next); err != nil || handler != next {
		t.Fatalf("start-only handler=%v err=%v", handler, err)
	}
	if extension.calls.Load() != 1 {
		t.Fatalf("Start=%d", extension.calls.Load())
	}
}

type acceptingListener struct {
	net.Listener
	accepts atomic.Int32
}

func (l *acceptingListener) Accept() (net.Conn, error) {
	connection, err := l.Listener.Accept()
	if err == nil {
		l.accepts.Add(1)
	}
	return connection, err
}

type preparedWorkerHarness struct {
	worker      *PreparedWorker
	table       *proxyshm.Table
	data        *acceptingListener
	notifyWrite *os.File
	wakeRead    *os.File
	statsMaster net.Conn
	mmdsMaster  net.Conn
}

func newPreparedWorkerHarness(t *testing.T) *preparedWorkerHarness {
	t.Helper()
	effective, err := FreezeConfig(testProxyConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	table, err := proxyshm.Create(filepath.Join(t.TempDir(), "routes.shm"), 8)
	if err != nil {
		t.Fatal(err)
	}
	notifyRead, notifyWrite, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	wakeRead, wakeWrite, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	statsMaster, statsWorker := net.Pipe()
	mmdsMaster, mmdsWorker := net.Pipe()
	dataBase, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	data := &acceptingListener{Listener: dataBase}
	harness := &preparedWorkerHarness{
		table: table, data: data, notifyWrite: notifyWrite,
		wakeRead: wakeRead, statsMaster: statsMaster, mmdsMaster: mmdsMaster,
	}
	harness.worker = &PreparedWorker{
		effective: effective,
		process: proxyextension.Process{
			Role: proxyextension.RoleWorker, WorkerID: "proxy-test", WorkerEpoch: 3,
		},
		table: table, wakeFile: wakeWrite, notifyFile: notifyRead,
		statsConn: statsWorker, mmdsRPCConn: mmdsWorker,
		data: data,
	}
	go func() { _, _ = io.Copy(io.Discard, statsMaster) }()
	t.Cleanup(func() {
		_ = harness.worker.Close()
		_ = notifyWrite.Close()
		_ = wakeRead.Close()
		_ = statsMaster.Close()
		_ = mmdsMaster.Close()
	})
	return harness
}

func TestPreparedWorkerWaitsForSyncThenUsesWrapperForDataIngress(t *testing.T) {
	harness := newPreparedWorkerHarness(t)
	started := make(chan proxyextension.WorkerHost, 1)
	startContext := make(chan context.Context, 1)
	extension := &wrappingWorkerExtension{}
	extension.start = func(ctx context.Context, host proxyextension.WorkerHost) error {
		startContext <- ctx
		started <- host
		return nil
	}
	extension.wrap = func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/private/raw" {
				w.Header().Set("X-Worker-Epoch", "3")
				w.WriteHeader(http.StatusNoContent)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() {
		runDone <- harness.worker.Run(ctx, &Runtime{
			Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), WorkerExtension: extension,
		})
	}()
	select {
	case <-started:
		t.Fatal("WorkerExtension.Start ran before initial route sync")
	case <-time.After(30 * time.Millisecond):
	}
	harness.table.BeginSync()
	if err := harness.table.Upsert(routesync.RouteEntry{
		SandboxID: "s1", StableID: "s1", Profile: "e2b", State: routesync.StateRunning,
		RunID: "run-1", FloatingIP: "127.0.0.1", ForwardAccessToken: "core-token",
	}); err != nil {
		t.Fatal(err)
	}
	harness.table.Bookmark()
	var host proxyextension.WorkerHost
	select {
	case host = <-started:
	case err := <-runDone:
		t.Fatalf("worker exited before Start: %v", err)
	case <-time.After(time.Second):
		t.Fatal("WorkerExtension.Start did not run after initial sync")
	}
	if host.Process() != (proxyextension.Process{Role: proxyextension.RoleWorker, WorkerID: "proxy-test", WorkerEpoch: 3}) {
		t.Fatalf("host process=%+v", host.Process())
	}

	request := func(address string) {
		t.Helper()
		req, err := http.NewRequest(http.MethodGet, "http://"+address+"/private/raw", nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("X-Sandbox-Id", "private-s1")
		response, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusNoContent || response.Header.Get("X-Worker-Epoch") != "3" {
			t.Fatalf("response=%d headers=%v", response.StatusCode, response.Header)
		}
	}
	request(harness.data.Addr().String())
	standardRequest, err := http.NewRequest(http.MethodGet, "http://"+harness.data.Addr().String()+"/standard", nil)
	if err != nil {
		t.Fatal(err)
	}
	standardRequest.Header.Set(internalproxy.HeaderSandboxID, "s1")
	standardRequest.Header.Set(internalproxy.HeaderSandboxPort, "8080")
	standardResponse, err := http.DefaultClient.Do(standardRequest)
	if err != nil {
		t.Fatal(err)
	}
	standardResponse.Body.Close()
	if standardResponse.StatusCode != http.StatusUnauthorized || standardResponse.Header.Get(internalproxy.HeaderProxyError) != internalproxy.ProxyErrorUnauthorized {
		t.Fatalf("standard next response=%d headers=%v", standardResponse.StatusCode, standardResponse.Header)
	}
	if extension.calls.Load() != 1 || extension.wrapCalls.Load() != 1 || harness.data.accepts.Load() == 0 {
		t.Fatalf("Start=%d Wrap=%d data accepts=%d", extension.calls.Load(), extension.wrapCalls.Load(), harness.data.accepts.Load())
	}
	extensionCtx := <-startContext
	cancel()
	select {
	case <-extensionCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("worker extension context was not canceled")
	}
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("worker Run=%v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("worker Run did not stop")
	}
}

func TestPreparedWorkerStartFailureDoesNotServeListeners(t *testing.T) {
	harness := newPreparedWorkerHarness(t)
	harness.table.BeginSync()
	harness.table.Bookmark()
	want := errors.New("private worker startup failed")
	extension := &startOnlyWorkerExtension{
		start: func(context.Context, proxyextension.WorkerHost) error { return want },
	}
	err := harness.worker.Run(context.Background(), &Runtime{
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), WorkerExtension: extension,
	})
	if !errors.Is(err, want) {
		t.Fatalf("Run error=%v", err)
	}
	if extension.calls.Load() != 1 || harness.data.accepts.Load() != 0 {
		t.Fatalf("Start=%d data accepts=%d", extension.calls.Load(), harness.data.accepts.Load())
	}
}
