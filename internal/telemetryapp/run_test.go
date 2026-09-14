package telemetryapp

import (
	"context"
	"encoding/json"
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

	"github.com/kuasar-sandbox/orchestrator/app/telemetry/extension"
	"github.com/kuasar-sandbox/orchestrator/config"
	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/telemetry"
)

func testConfig(t *testing.T) *config.Telemetry {
	t.Helper()
	directory, err := os.MkdirTemp("", "telemetry-app-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		// All files in this private test directory were created by this test.
		if err := os.RemoveAll(directory); err != nil {
			t.Error(err)
		}
	})
	cfg, err := config.DecodeTelemetry(strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConfigSocket = filepath.Join(directory, "plugin.sock")
	cfg.APISocket = filepath.Join(directory, "query.sock")
	cfg.Telemetry.Storage.Type = "local"
	cfg.Telemetry.Storage.Path = filepath.Join(directory, "db")
	cfg.Telemetry.Storage.MaxSize = "64MiB"
	cfg.Collector = map[string]any{
		"receivers": map[string]any{"envd": map[string]any{"collection_interval": "1s"}},
		"exporters": map[string]any{"sandboxstorage": map[string]any{}},
		"service":   map[string]any{"telemetry": map[string]any{"metrics": map[string]any{"level": "none"}}, "pipelines": map[string]any{"metrics": map[string]any{"receivers": []any{"envd"}, "exporters": []any{"sandboxstorage"}}}},
	}
	return cfg
}
func logger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

type lifecycleExtension struct {
	start, stop       atomic.Int32
	startErr, stopErr error
	reader            extension.Reader
	done              chan struct{}
}

func (e *lifecycleExtension) Start(ctx context.Context, host extension.Host) error {
	e.start.Add(1)
	e.reader = host.Reader()
	e.done = make(chan struct{})
	go func() { <-ctx.Done(); close(e.done) }()
	return e.startErr
}
func (e *lifecycleExtension) Shutdown(ctx context.Context) error {
	e.stop.Add(1)
	select {
	case <-e.done:
		return e.stopErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

type source struct {
	route  routesync.RouteEntry
	events chan routesync.Event
	wakes  atomic.Int32
	syncs  chan struct{}
}

func (s *source) Range(ctx context.Context, emit func(routesync.RouteEntry) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.syncs != nil {
		select {
		case s.syncs <- struct{}{}:
		default:
		}
	}
	return emit(s.route)
}
func (s *source) Subscribe() (<-chan routesync.Event, func()) { return s.events, func() {} }
func (s *source) OnWake(context.Context, string)              { s.wakes.Add(1) }
func (*source) Policy() routesync.Policy                      { return routesync.Policy{} }

func servePlugins(t *testing.T, cfg *config.Telemetry, src *source) *configsock.Registry {
	t.Helper()
	registry := configsock.NewRegistry()
	server := configsock.New(cfg.ConfigSocket, configsock.Deps{Plugins: registry, RouteSource: src, API: http.NotFoundHandler()}, logger())
	ctx, cancel := context.WithCancel(context.Background())
	done, ready := make(chan error, 1), make(chan struct{})
	go func() { done <- server.ServeReady(ctx, ready) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(3 * time.Second):
			t.Error("config socket leaked")
		}
	})
	select {
	case <-ready:
	case <-time.After(3 * time.Second):
		t.Fatal("config socket startup")
	}
	return registry
}

func TestBuiltinCollectorPluginQueryAndShutdown(t *testing.T) {
	cfg := testConfig(t)
	extension := &lifecycleExtension{}
	resolved, err := ResolveRuntime(context.Background(), cfg, Bindings{Logger: logger(), Extension: extension})
	if err != nil {
		t.Fatal(err)
	}
	envdSocket := filepath.Join(filepath.Dir(cfg.APISocket), "envd.sock")
	ln, err := net.Listen("unix", envdSocket)
	if err != nil {
		t.Fatal(err)
	}
	var badToken atomic.Bool
	envd := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Access-Token") != "guest-token" || r.URL.Path != "/metrics" {
			badToken.Store(true)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"ts": time.Now().Unix(), "cpu_count": 2, "cpu_used_pct": 20.5, "mem_total": 1024, "mem_used": 500, "mem_cache": 128, "disk_total": 4096, "disk_used": 1000})
	})}
	go func() { _ = envd.Serve(ln) }()
	defer envd.Close()
	src := &source{route: routesync.RouteEntry{SandboxID: "sid", StableID: "stable-sid", Profile: "e2b", State: routesync.StateRunning, EnvdUDS: envdSocket, EnvdAccessToken: "guest-token", FloatingIP: "127.0.0.1"}, events: make(chan routesync.Event, 4)}
	registry := servePlugins(t, cfg, src)
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, cfg, resolved) }()
	defer func() {
		stop()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(5 * time.Second):
			t.Error("telemetry shutdown hung")
		}
	}()
	query := func(id string) *httptest.ResponseRecorder {
		response := httptest.NewRecorder()
		registry.TelemetryAPI().ServeHTTP(response, httptest.NewRequest("GET", "/sandboxes/"+id+"/metrics", nil))
		return response
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		response := query("sid")
		if response.Code == 200 && strings.Contains(response.Body.String(), `"memCache":128`) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("Collector -> TSDB -> registered query did not become readable: %d %s", response.Code, response.Body.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	if badToken.Load() {
		t.Fatal("envd token/path mismatch")
	}
	paused := src.route
	paused.State = routesync.StatePaused
	src.events <- routesync.Event{Kind: routesync.TypeUpsert, Route: paused}
	if response := query("sid"); response.Code != 200 || !strings.Contains(response.Body.String(), `"memCache":128`) {
		t.Fatal("paused history missing", response.Code, response.Body.String())
	}
	if response := query("stable-sid"); response.Code != 200 || response.Body.String() != "[]\n" {
		t.Fatal("StableID fallback", response.Code, response.Body.String())
	}
	stop()
	// Wait without consuming done, so the deferred cleanup validates the result.
	deadline = time.Now().Add(5 * time.Second)
	for extension.stop.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("extension not shut down")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if response := query("sid"); response.Code != 503 {
		t.Fatal("query lease survived shutdown", response.Code)
	}
	if src.wakes.Load() != 0 {
		t.Fatal("telemetry issued a lifecycle wake")
	}
	if extension.start.Load() != 1 || extension.stop.Load() != 1 || extension.reader == nil {
		t.Fatal("extension lifecycle")
	}
}

func TestStartupFailuresCleanUpStorageExtensionAndSockets(t *testing.T) {
	for _, phase := range []string{"extension", "query", "otlp"} {
		t.Run(phase, func(t *testing.T) {
			cfg := testConfig(t)
			ext := &lifecycleExtension{}
			if phase == "extension" {
				ext.startErr = errors.New("extension unavailable")
			}
			if phase == "query" {
				if err := os.WriteFile(cfg.APISocket, []byte("preserve me"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if phase == "otlp" {
				ln, err := net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				defer ln.Close()
				cfg.Collector["receivers"].(map[string]any)["sandboxotlp"] = map[string]any{"http_listen": "127.0.0.1:0", "grpc_listen": ln.Addr().String()}
				cfg.Collector["service"].(map[string]any)["pipelines"].(map[string]any)["metrics"].(map[string]any)["receivers"] = []any{"envd", "sandboxotlp"}
			}
			resolved, err := ResolveRuntime(context.Background(), cfg, Bindings{Logger: logger(), Extension: ext})
			if err != nil {
				t.Fatal(err)
			}
			if err := Run(context.Background(), cfg, resolved); err == nil {
				t.Fatal("startup should fail")
			}
			if ext.start.Load() != 1 || ext.stop.Load() != 1 {
				t.Fatal("extension cleanup", ext.start.Load(), ext.stop.Load())
			}
			backend, err := telemetry.OpenLocal(cfg.Telemetry.Storage, logger())
			if err != nil {
				t.Fatal("DB left locked", err)
			}
			if err := backend.Shutdown(context.Background()); err != nil {
				t.Fatal(err)
			}
			if phase == "query" {
				raw, err := os.ReadFile(cfg.APISocket)
				if err != nil || string(raw) != "preserve me" {
					t.Fatal("query startup overwrote regular file")
				}
			}
		})
	}
}

func TestQuerySocketExclusiveOwnershipAndRestart(t *testing.T) {
	cfg := testConfig(t)
	ln, release, err := listenQuery(cfg.APISocket)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := listenQuery(cfg.APISocket); err == nil {
		t.Fatal("replaced active socket owner")
	}
	info, err := os.Stat(cfg.APISocket)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatal("query permissions", err)
	}
	_ = ln.Close()
	release()
	ln, release, err = listenQuery(cfg.APISocket)
	if err != nil {
		t.Fatal("socket restart", err)
	}
	_ = ln.Close()
	release()
}

func TestRuntimeProvidersAreAuthoritative(t *testing.T) {
	cfg := testConfig(t)
	cfg.Telemetry.Storage.Type = "prometheus"
	cfg.Telemetry.Storage.Prometheus = config.TelemetryRemote{Endpoint: "https://example.com", Headers: map[string]string{"Authorization": "fallback"}}
	if _, err := ResolveRuntime(context.Background(), cfg, Bindings{StorageHeaders: func(context.Context) (map[string]string, error) { return nil, errors.New("credential failure") }}); err == nil {
		t.Fatal("credentials fell back")
	}
	cfg.Telemetry.Storage.Type = "custom"
	if _, err := ResolveRuntime(context.Background(), cfg, Bindings{}); err == nil {
		t.Fatal("custom storage fell back")
	}
	cfg.Telemetry.Storage.Type = "none"
	cfg.Collector = nil
	if err := Run(context.Background(), cfg, &Runtime{Bindings: Bindings{Logger: logger()}}); err == nil {
		t.Fatal("no output accepted")
	}
}

type failedStorage struct {
	extension.Storage
	stops atomic.Int32
}

func (s *failedStorage) Shutdown(context.Context) error { s.stops.Add(1); return nil }

func TestCustomStorageConstructionFailureCleanup(t *testing.T) {
	for _, typedNil := range []bool{false, true} {
		cfg := testConfig(t)
		cfg.Telemetry.Storage.Type = "custom"
		backend := &failedStorage{}
		var nilBackend *failedStorage
		resolved, err := ResolveRuntime(context.Background(), cfg, Bindings{Logger: logger(), Storage: func(context.Context, config.TelemetryStorage) (extension.Storage, error) {
			if typedNil {
				return nilBackend, nil
			}
			return backend, errors.New("constructor failed after allocation")
		}})
		if err != nil {
			t.Fatal(err)
		}
		if err := Run(context.Background(), cfg, resolved); err == nil {
			t.Fatal("provider failure accepted")
		}
		if !typedNil && backend.stops.Load() != 1 {
			t.Fatal("partially constructed storage not closed")
		}
	}
}

func TestForwardOnlyRegistersWithoutReadableEndpoint(t *testing.T) {
	cfg := testConfig(t)
	cfg.Telemetry.Storage.Type = "none"
	sink := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) }))
	defer sink.Close()
	cfg.Collector["exporters"] = map[string]any{"otlp_http/extra": map[string]any{"endpoint": sink.URL}}
	cfg.Collector["service"].(map[string]any)["pipelines"].(map[string]any)["metrics"].(map[string]any)["exporters"] = []any{"otlp_http/extra"}
	src := &source{route: routesync.RouteEntry{SandboxID: "sid", StableID: "stable-sid", State: routesync.StatePaused}, events: make(chan routesync.Event), syncs: make(chan struct{}, 1)}
	registry := servePlugins(t, cfg, src)
	ext := &lifecycleExtension{}
	resolved, err := ResolveRuntime(context.Background(), cfg, Bindings{Logger: logger(), Extension: ext})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, cfg, resolved) }()
	select {
	case <-src.syncs:
	case err := <-done:
		t.Fatal("forward-only startup", err)
	case <-time.After(3 * time.Second):
		t.Fatal("no route registration")
	}
	if _, err := os.Stat(cfg.APISocket); !os.IsNotExist(err) {
		t.Fatal("forward-only created query listener", err)
	}
	response := httptest.NewRecorder()
	registry.TelemetryAPI().ServeHTTP(response, httptest.NewRequest("GET", "/sandboxes/sid/metrics", nil))
	if response.Code != 503 {
		t.Fatal("extra exporter became a primary reader")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("forward-only shutdown")
	}
	if ext.reader != nil || ext.start.Load() != 1 || ext.stop.Load() != 1 {
		t.Fatal("forward-only extension lifecycle")
	}
}

type unhealthyStorage struct {
	failedStorage
	errors chan error
}

func (s *unhealthyStorage) Errors() <-chan error { return s.errors }

func TestStorageHealthFailureRevokesQueryLease(t *testing.T) {
	cfg := testConfig(t)
	cfg.Telemetry.Storage.Type = "custom"
	backend := &unhealthyStorage{errors: make(chan error, 1)}
	src := &source{route: routesync.RouteEntry{SandboxID: "sid", StableID: "stable-sid", State: routesync.StatePaused}, events: make(chan routesync.Event), syncs: make(chan struct{}, 1)}
	registry := servePlugins(t, cfg, src)
	resolved, err := ResolveRuntime(context.Background(), cfg, Bindings{Logger: logger(), Storage: func(context.Context, config.TelemetryStorage) (extension.Storage, error) { return backend, nil }})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, cfg, resolved) }()
	select {
	case <-src.syncs:
	case err := <-done:
		t.Fatal(err)
	case <-time.After(3 * time.Second):
		t.Fatal("no primary registration")
	}
	backend.errors <- errors.New("background storage failed")
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "background storage failed") {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("storage error did not stop component")
	}
	response := httptest.NewRecorder()
	registry.TelemetryAPI().ServeHTTP(response, httptest.NewRequest("GET", "/sandboxes/sid/metrics", nil))
	if response.Code != 503 || backend.stops.Load() != 1 {
		t.Fatal("failed primary retained lease/resources")
	}
}
