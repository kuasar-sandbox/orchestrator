package proxyapp

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	proxyextension "github.com/kuasar-sandbox/orchestrator/app/proxy/extension"
)

type startOnlyMasterExtension struct {
	start func(context.Context, proxyextension.MasterHost) error
}

func (e *startOnlyMasterExtension) Start(ctx context.Context, host proxyextension.MasterHost) error {
	return e.start(ctx, host)
}

type wrappingMasterExtension struct {
	start func(context.Context, proxyextension.MasterHost) error
	wrap  func(http.Handler) http.Handler
}

func (e *wrappingMasterExtension) Start(ctx context.Context, host proxyextension.MasterHost) error {
	return e.start(ctx, host)
}

func (e *wrappingMasterExtension) WrapManagement(next http.Handler) http.Handler {
	return e.wrap(next)
}

func TestRunMasterExtensionStartFailurePrecedesListenersAndTasks(t *testing.T) {
	effective, paths := testMasterEffective(t)
	want := errors.New("extension unavailable")
	var calls atomic.Int32
	extension := &startOnlyMasterExtension{start: func(ctx context.Context, host proxyextension.MasterHost) error {
		calls.Add(1)
		if ctx == nil || host == nil || host.Routes() == nil || host.Traffic() == nil {
			t.Fatal("Start received an incomplete Host")
		}
		if got := host.Routes().SyncState(); got != proxyextension.RouteSyncInitializing {
			t.Fatalf("Start route state = %q", got)
		}
		return want
	}}
	err := RunMaster(context.Background(), effective, &Runtime{
		Logger: testMasterLogger(), MasterExtension: extension,
	})
	if !errors.Is(err, want) || !strings.Contains(err.Error(), "extension start") {
		t.Fatalf("RunMaster error = %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("Start calls = %d", calls.Load())
	}
	for _, path := range paths {
		if _, statErr := os.Stat(path); !errors.Is(statErr, os.ErrNotExist) {
			t.Fatalf("startup artifact %s survived Start failure: %v", path, statErr)
		}
	}
}

func TestRunMasterWrapsManagementHandlerOnce(t *testing.T) {
	effective, paths := testMasterEffective(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var starts atomic.Int32
	var wraps atomic.Int32
	shutdownObserved := make(chan struct{})
	extension := &wrappingMasterExtension{
		start: func(ctx context.Context, host proxyextension.MasterHost) error {
			starts.Add(1)
			if _, found, err := host.Routes().Get(ctx, "missing"); err != nil || found {
				t.Fatalf("initial route Get: found=%v err=%v", found, err)
			}
			go func() {
				<-ctx.Done()
				close(shutdownObserved)
			}()
			return nil
		},
		wrap: func(next http.Handler) http.Handler {
			wraps.Add(1)
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.URL.Path == "/private/health":
					w.WriteHeader(http.StatusNoContent)
				case r.Header.Get("X-Extension-Override") == "yes":
					w.WriteHeader(http.StatusAccepted)
				default:
					next.ServeHTTP(w, r)
				}
			})
		},
	}
	done := make(chan error, 1)
	go func() {
		done <- RunMaster(ctx, effective, &Runtime{
			Logger: testMasterLogger(), MasterExtension: extension,
		})
	}()

	statsSocket := paths[1]
	response := requestUnixEventually(t, statsSocket, http.MethodGet, "/private/health", nil)
	if response.StatusCode != http.StatusNoContent {
		response.Body.Close()
		t.Fatalf("private management status = %d", response.StatusCode)
	}
	response.Body.Close()

	request, err := http.NewRequest(http.MethodPost, "http://proxy/v1/traffic:batchGet", nil)
	if err != nil {
		t.Fatal(err)
	}
	response = doUnixRequest(t, statsSocket, request)
	if response.StatusCode != http.StatusServiceUnavailable {
		response.Body.Close()
		t.Fatalf("next built-in status = %d, want 503", response.StatusCode)
	}
	response.Body.Close()

	request, err = http.NewRequest(http.MethodPost, "http://proxy/v1/traffic:batchGet", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("X-Extension-Override", "yes")
	response = doUnixRequest(t, statsSocket, request)
	if response.StatusCode != http.StatusAccepted {
		response.Body.Close()
		t.Fatalf("override status = %d", response.StatusCode)
	}
	response.Body.Close()

	if starts.Load() != 1 || wraps.Load() != 1 {
		t.Fatalf("Start=%d WrapManagement=%d", starts.Load(), wraps.Load())
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("RunMaster shutdown = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("RunMaster did not stop")
	}
	select {
	case <-shutdownObserved:
	case <-time.After(time.Second):
		t.Fatal("extension did not observe master context cancellation")
	}
	for _, path := range paths[1:] {
		if _, statErr := os.Stat(path); !errors.Is(statErr, os.ErrNotExist) {
			t.Fatalf("startup artifact %s survived shutdown: %v", path, statErr)
		}
	}
}

func TestRunMasterRejectsNilManagementWrapperResult(t *testing.T) {
	effective, paths := testMasterEffective(t)
	var wraps atomic.Int32
	extension := &wrappingMasterExtension{
		start: func(context.Context, proxyextension.MasterHost) error { return nil },
		wrap: func(http.Handler) http.Handler {
			wraps.Add(1)
			return nil
		},
	}
	err := RunMaster(context.Background(), effective, &Runtime{
		Logger: testMasterLogger(), MasterExtension: extension,
	})
	if err == nil || !strings.Contains(err.Error(), "nil management handler") {
		t.Fatalf("RunMaster error = %v", err)
	}
	if wraps.Load() != 1 {
		t.Fatalf("WrapManagement calls = %d", wraps.Load())
	}
	for _, path := range paths[1:] {
		if _, statErr := os.Stat(path); !errors.Is(statErr, os.ErrNotExist) {
			t.Fatalf("startup artifact %s survived nil wrapper: %v", path, statErr)
		}
	}
}

func testMasterEffective(t *testing.T) (*EffectiveConfig, []string) {
	t.Helper()
	temp := t.TempDir()
	cfg := testProxyConfig(t)
	cfg.ConfigSocket = filepath.Join(temp, "config.sock")
	cfg.StatsSocket = filepath.Join(temp, "stats.sock")
	cfg.ShmPath = filepath.Join(temp, "routes.shm")
	cfg.Paths.RunRoot = filepath.Join(temp, "run")
	cfg.ProxyNetNS = ""
	cfg.DataListen = "127.0.0.1:0"
	cfg.MetricsListen = ""
	effective, err := FreezeConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return effective, []string{cfg.ConfigSocket, cfg.StatsSocket, cfg.ShmPath}
}

func testMasterLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func requestUnixEventually(t *testing.T, socket, method, path string, body io.Reader) *http.Response {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		request, err := http.NewRequest(method, "http://proxy"+path, body)
		if err != nil {
			t.Fatal(err)
		}
		response, err := unixHTTPClient(socket).Do(request)
		if err == nil {
			return response
		}
		if time.Now().After(deadline) {
			t.Fatalf("request %s over %s: %v", path, socket, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func doUnixRequest(t *testing.T, socket string, request *http.Request) *http.Response {
	t.Helper()
	response, err := unixHTTPClient(socket).Do(request)
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func unixHTTPClient(socket string) *http.Client {
	return &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socket)
	}}}
}
