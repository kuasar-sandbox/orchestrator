package configsock

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

func telemetryPlugin(path string) *Plugin {
	return &Plugin{ID: routesync.TelemetryPluginID, Caps: routesync.Register{
		Subscribe: &routesync.Subscribe{Kind: routesync.KindRoute},
		Telemetry: &routesync.Telemetry{API: &routesync.Socket{Path: path}},
	}}
}

func TestTelemetryRegistrationRejectsIncorrectCapabilities(t *testing.T) {
	for _, change := range []func(*Plugin){
		func(p *Plugin) { p.ID = "observer" },
		func(p *Plugin) { p.Caps.Subscribe.Kind = routesync.KindRouteWake },
		func(p *Plugin) { p.Caps.Proxy = &routesync.Proxy{} },
		func(p *Plugin) { p.Caps.Mmds = true },
		func(p *Plugin) { p.Caps.Telemetry.API.Path = "http://localhost:4318" },
	} {
		p := telemetryPlugin("/run/query.sock")
		change(p)
		var wire bytes.Buffer
		if err := routesync.WriteMsg(&wire, &routesync.Msg{Type: routesync.TypeRegister, Register: &p.Caps}); err != nil {
			t.Fatal(err)
		}
		request := httptest.NewRequest(http.MethodPut, "/internal/plugin/"+p.ID+"/register", &wire)
		request.SetPathValue("id", p.ID)
		request = request.WithContext(context.WithValue(request.Context(), peerPIDKey{}, os.Getpid()))
		response := httptest.NewRecorder()
		server := &Server{deps: Deps{Plugins: NewRegistry()}}
		server.handlePluginRegister(response, request)
		if response.Code != 400 && response.Code != 403 {
			t.Fatal("invalid capabilities accepted", response.Code)
		}
	}
}

func TestTelemetryNoUpgradeChannel(t *testing.T) {
	path := telemetryTestSocket(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		conn, buffered, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.Close()
		_, _ = fmt.Fprint(buffered, "HTTP/1.1 101 Switching Protocols\r\nConnection: upgrade\r\nUpgrade: arbitrary\r\n\r\n")
		_ = buffered.Flush()
	}))
	registry := NewRegistry()
	registry.Add(telemetryPlugin(path))
	response := httptest.NewRecorder()
	registry.TelemetryAPI().ServeHTTP(response, httptest.NewRequest("GET", "/sandboxes/sid/metrics", nil))
	if response.Code != 503 {
		t.Fatal("upgrade accepted", response.Code)
	}
}

func telemetryTestSocket(t *testing.T, handler http.Handler) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "api.sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: handler}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { _ = server.Close() })
	return path
}

func TestTelemetryEndpointLease(t *testing.T) {
	registry := NewRegistry()
	first := telemetryPlugin("/run/old.sock")
	registry.Add(first)
	path, lease, ok := registry.telemetryTarget()
	if !ok || path != "/run/old.sock" {
		t.Fatalf("target = %q, %v", path, ok)
	}
	second := telemetryPlugin("/run/new.sock")
	registry.Add(second)
	if lease.Err() == nil {
		t.Fatal("replacement did not revoke old query lease")
	}
	registry.Remove(first)
	path, successor, ok := registry.telemetryTarget()
	if !ok || path != "/run/new.sock" || successor.Err() != nil {
		t.Fatal("old Remove revoked successor")
	}
	registry.Remove(second)
	if successor.Err() == nil {
		t.Fatal("disconnect did not revoke query lease")
	}
	if _, _, ok := registry.telemetryTarget(); ok {
		t.Fatal("disconnected endpoint still usable")
	}
	response := httptest.NewRecorder()
	registry.TelemetryAPI().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/sandboxes/sid/metrics", nil))
	if response.Code != 503 {
		t.Fatalf("disconnected status = %d", response.Code)
	}
}

func TestTelemetryOnlyLocalTrustedEndpoint(t *testing.T) {
	for _, path := range []string{"", "relative.sock", "http://127.0.0.1:1234", "unix:///run/api.sock", "/", "/run/../api.sock", "/run/a\x00b", "/" + strings.Repeat("a", 107)} {
		registry := NewRegistry()
		registry.Add(telemetryPlugin(path))
		if _, _, ok := registry.telemetryTarget(); ok {
			t.Errorf("accepted endpoint %q", path)
		}
	}
	registry := NewRegistry()
	p := telemetryPlugin("/run/api.sock")
	p.ID = "observer"
	registry.Add(p)
	if _, _, ok := registry.telemetryTarget(); ok {
		t.Fatal("used non-telemetry plugin")
	}
}

func TestTelemetryOpaqueHTTPForwarding(t *testing.T) {
	const query = "start=one&end=two&unknown=%2f&broken=a;b"
	path := telemetryTestSocket(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if r.URL.Path != "/sandboxes/exact-sid/metrics" || r.URL.RawQuery != query || string(body) != "opaque-request" {
			t.Errorf("request changed: %s body=%q", r.URL, body)
		}
		if r.Header.Get("X-Hop") != "" || r.Header.Get("Upgrade") != "" {
			t.Error("request hop header forwarded")
		}
		w.Header().Set("Connection", "X-Response-Hop")
		w.Header().Set("X-Response-Hop", "private-hop")
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Retry-After", "7")
		w.WriteHeader(http.StatusTeapot)
		_, _ = w.Write([]byte("not-json\x00\xff"))
	}))
	registry := NewRegistry()
	registry.Add(telemetryPlugin(path))
	req := httptest.NewRequest(http.MethodGet, "/sandboxes/exact-sid/metrics?"+query, strings.NewReader("opaque-request"))
	req.Header.Set("Connection", "X-Hop, Upgrade")
	req.Header.Set("X-Hop", "request-hop")
	req.Header.Set("Upgrade", "unused")
	response := httptest.NewRecorder()
	registry.TelemetryAPI().ServeHTTP(response, req)
	if response.Code != http.StatusTeapot || response.Body.String() != "not-json\x00\xff" || response.Header().Get("Retry-After") != "7" || response.Header().Get("Content-Type") != "application/octet-stream" {
		t.Fatalf("response changed: %d %v %q", response.Code, response.Header(), response.Body.String())
	}
	if response.Header().Get("X-Response-Hop") != "" || response.Header().Get("Connection") != "" {
		t.Fatal("response hop header forwarded")
	}
}

func TestTelemetryInFlightCancellation(t *testing.T) {
	for _, reason := range []string{"client", "disconnect", "replacement", "stream-context"} {
		t.Run(reason, func(t *testing.T) {
			entered, stopped := make(chan struct{}), make(chan struct{})
			path := telemetryTestSocket(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				close(entered)
				<-r.Context().Done()
				close(stopped)
			}))
			registry := NewRegistry()
			p := telemetryPlugin(path)
			streamCtx, disconnect := context.WithCancel(context.Background())
			defer disconnect()
			p.lease = streamCtx
			registry.Add(p)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			req := httptest.NewRequest(http.MethodGet, "/sandboxes/sid/metrics", nil).WithContext(ctx)
			response, done := httptest.NewRecorder(), make(chan struct{})
			go func() { registry.TelemetryAPI().ServeHTTP(response, req); close(done) }()
			select {
			case <-entered:
			case <-time.After(3 * time.Second):
				t.Fatal("query did not arrive")
			}
			switch reason {
			case "client":
				cancel()
			case "disconnect":
				registry.Remove(p)
			case "replacement":
				registry.Add(telemetryPlugin("/run/successor.sock"))
				registry.Remove(p)
			case "stream-context":
				disconnect()
			}
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				t.Fatal("query did not cancel")
			}
			select {
			case <-stopped:
			case <-time.After(3 * time.Second):
				t.Fatal("UDS connection retained after cancellation")
			}
			if response.Code != 503 {
				t.Fatalf("status = %d", response.Code)
			}
		})
	}
}

func TestTelemetryReplacementAbortsBodyAndUsesOnlySuccessor(t *testing.T) {
	var oldCalls, newCalls atomic.Int32
	stopped := make(chan struct{})
	oldPath := telemetryTestSocket(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		oldCalls.Add(1)
		_, _ = w.Write([]byte("prefix"))
		w.(http.Flusher).Flush()
		<-r.Context().Done()
		close(stopped)
	}))
	newPath := telemetryTestSocket(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		newCalls.Add(1)
		_, _ = w.Write([]byte("successor"))
	}))
	registry := NewRegistry()
	old := telemetryPlugin(oldPath)
	registry.Add(old)
	// A real HTTP server handles ReverseProxy's ErrAbortHandler when a lease
	// ends after headers were sent; it must not finish a truncated body as 200.
	server := httptest.NewServer(registry.TelemetryAPI())
	defer server.Close()
	client := &http.Client{Timeout: 3 * time.Second}
	response, err := client.Get(server.URL + "/sandboxes/sid/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if _, err := io.ReadFull(response.Body, make([]byte, len("prefix"))); err != nil {
		t.Fatal(err)
	}
	successor := telemetryPlugin(newPath)
	registry.Add(successor)
	registry.Remove(old)
	if _, err := io.ReadAll(response.Body); err == nil {
		t.Fatal("revoked body was completed successfully")
	}
	select {
	case <-stopped:
	case <-time.After(3 * time.Second):
		t.Fatal("old UDS body stream retained after replacement")
	}
	for range 3 {
		response := httptest.NewRecorder()
		registry.TelemetryAPI().ServeHTTP(response, httptest.NewRequest("GET", "/sandboxes/sid/metrics", nil))
		if response.Code != 200 || response.Body.String() != "successor" {
			t.Fatal("replacement did not use successor", response.Code, response.Body.String())
		}
	}
	if oldCalls.Load() != 1 || newCalls.Load() != 3 {
		t.Fatal("stale endpoint was reused", oldCalls.Load(), newCalls.Load())
	}
}
