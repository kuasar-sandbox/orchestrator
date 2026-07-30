package mmdssvc

import (
	"context"
	"net"
	"net/http"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/config"
)

// startFakeService serves handler over a real Unix domain socket in a temp
// dir and returns a Registry with one entry, "svc", pointing at it.
func startFakeService(t *testing.T, handler http.HandlerFunc, timeout time.Duration) Registry {
	t.Helper()
	sockPath := filepath.Join(t.TempDir(), "svc.sock")
	ln, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: handler}
	go srv.Serve(ln)
	t.Cleanup(func() { srv.Close() })
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	return Registry{"svc": {SocketPath: sockPath, Timeout: timeout, MaxResponseBytes: 64 * 1024}}
}

func TestCallHappyPathInjectsExactlyThreeFixedHeaders(t *testing.T) {
	var gotHeaders http.Header
	var gotPath string
	reg := startFakeService(t, func(w http.ResponseWriter, r *http.Request) {
		gotHeaders = r.Header.Clone()
		gotPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}, 0)

	result := Call(context.Background(), reg, "svc", "/latest/meta-data/credentials", "sbx-1", "auth-subj-1", "svc1")

	if result.StatusCode != 200 || result.Body != `{"ok":true}` || result.ContentType != "application/json" {
		t.Fatalf("result = %+v", result)
	}
	if gotPath != "/latest/meta-data/credentials" {
		t.Fatalf("path = %q", gotPath)
	}
	if gotHeaders.Get(HeaderSandboxID) != "sbx-1" {
		t.Fatalf("HeaderSandboxID = %q", gotHeaders.Get(HeaderSandboxID))
	}
	if gotHeaders.Get(HeaderSandboxSubject) != "auth-subj-1" {
		t.Fatalf("HeaderSandboxSubject = %q", gotHeaders.Get(HeaderSandboxSubject))
	}
	if gotHeaders.Get(HeaderService) != "svc1" {
		t.Fatalf("HeaderService = %q", gotHeaders.Get(HeaderService))
	}
	// Only our 3 fixed headers plus stdlib-added transport headers may be
	// present -- nothing guest-controlled ever reaches the service, since
	// Call's signature has no *http.Request parameter to leak one from.
	allowed := map[string]bool{
		strings.ToLower(HeaderSandboxID):      true,
		strings.ToLower(HeaderSandboxSubject): true,
		strings.ToLower(HeaderService):        true,
		"host":                                true,
		"user-agent":                          true,
		"accept-encoding":                     true,
		"content-length":                      true,
		"connection":                          true, // DisableKeepAlives adds "Connection: close"
	}
	for k := range gotHeaders {
		if !allowed[strings.ToLower(k)] {
			t.Fatalf("unexpected header reached the service: %s", k)
		}
	}
}

// TestCallDoesNotLeakConnectionsOrGoroutines guards against a regression
// where each Call built a fresh, never-reused http.Transport with keep-alive
// left on: the response's connection would be pooled as idle by a Transport
// nobody ever drains, leaking its persistConn read-loop goroutine (and the
// underlying socket) for as long as the registered service kept it open.
func TestCallDoesNotLeakConnectionsOrGoroutines(t *testing.T) {
	reg := startFakeService(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte("ok"))
	}, 0)

	before := runtime.NumGoroutine()
	const n = 50
	for i := 0; i < n; i++ {
		Call(context.Background(), reg, "svc", "/x", "sbx-1", "subj-1", "svc1")
	}
	runtime.GC()
	time.Sleep(200 * time.Millisecond)
	after := runtime.NumGoroutine()

	if after > before+5 {
		t.Fatalf("goroutine count grew by %d after %d Call()s (before=%d after=%d) -- suspected connection/goroutine leak", after-before, n, before, after)
	}
}

func TestCallPassesThrough4xxAnd5xx(t *testing.T) {
	for _, code := range []int{404, 429, 500} {
		reg := startFakeService(t, func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(code)
			_, _ = w.Write([]byte("body"))
		}, 0)
		result := Call(context.Background(), reg, "svc", "/x", "sbx-1", "subj-1", "svc1")
		if result.StatusCode != code || result.Body != "body" {
			t.Fatalf("code=%d: result = %+v", code, result)
		}
	}
}

func TestCallOversizedResponseReturns502(t *testing.T) {
	reg := startFakeService(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write(make([]byte, 200))
	}, 0)
	reg["svc"] = Entry{SocketPath: reg["svc"].SocketPath, Timeout: reg["svc"].Timeout, MaxResponseBytes: 100}

	result := Call(context.Background(), reg, "svc", "/x", "sbx-1", "subj-1", "svc1")
	if result.StatusCode != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", result.StatusCode)
	}
}

func TestCallSlowServiceReturns504(t *testing.T) {
	reg := startFakeService(t, func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		w.WriteHeader(200)
	}, 30*time.Millisecond)

	start := time.Now()
	result := Call(context.Background(), reg, "svc", "/x", "sbx-1", "subj-1", "svc1")
	elapsed := time.Since(start)
	if result.StatusCode != http.StatusGatewayTimeout {
		t.Fatalf("status = %d, want 504", result.StatusCode)
	}
	if elapsed > time.Second {
		t.Fatalf("took too long: %v", elapsed)
	}
}

func TestCallNothingListeningReturns503(t *testing.T) {
	reg := Registry{"svc": {SocketPath: filepath.Join(t.TempDir(), "nope.sock"), Timeout: time.Second, MaxResponseBytes: 1024}}
	result := Call(context.Background(), reg, "svc", "/x", "sbx-1", "subj-1", "svc1")
	if result.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", result.StatusCode)
	}
}

func TestCallAbruptCloseMidResponseReturns502(t *testing.T) {
	sockPath := filepath.Join(t.TempDir(), "svc.sock")
	ln, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		// Write a truncated/malformed HTTP response then close -- the client
		// must see this as a protocol failure (502), not a clean response.
		_, _ = conn.Write([]byte("HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\nshort"))
		conn.Close()
	}()
	reg := Registry{"svc": {SocketPath: sockPath, Timeout: 2 * time.Second, MaxResponseBytes: 64 * 1024}}

	result := Call(context.Background(), reg, "svc", "/x", "sbx-1", "subj-1", "svc1")
	if result.StatusCode != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", result.StatusCode)
	}
}

func TestCallDoesNotFollowRedirects(t *testing.T) {
	var otherHit atomic.Bool
	otherSock := filepath.Join(t.TempDir(), "other.sock")
	otherLn, err := net.Listen("unix", otherSock)
	if err != nil {
		t.Fatal(err)
	}
	otherSrv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		otherHit.Store(true)
		w.WriteHeader(200)
	})}
	go otherSrv.Serve(otherLn)
	t.Cleanup(func() { otherSrv.Close() })

	reg := startFakeService(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", "http://ignored/elsewhere")
		w.WriteHeader(302)
	}, 0)

	result := Call(context.Background(), reg, "svc", "/x", "sbx-1", "subj-1", "svc1")
	if result.StatusCode != 302 {
		t.Fatalf("status = %d, want 302 passed through unfollowed", result.StatusCode)
	}
	if otherHit.Load() {
		t.Fatal("Call followed the redirect to another service")
	}
}

func TestCallSanitizesContentTypeAndRetryAfter(t *testing.T) {
	reg := startFakeService(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", strings.Repeat("x", 300))
		w.Header().Set("Retry-After", "not-a-valid-value")
		w.WriteHeader(503)
	}, 0)
	result := Call(context.Background(), reg, "svc", "/x", "sbx-1", "subj-1", "svc1")
	if result.ContentType != "" {
		t.Fatalf("expected oversized Content-Type to be dropped, got %q", result.ContentType)
	}
	if result.RetryAfter != "" {
		t.Fatalf("expected garbage Retry-After to be dropped, got %q", result.RetryAfter)
	}

	reg2 := startFakeService(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.Header().Set("Retry-After", "5")
		w.WriteHeader(503)
	}, 0)
	result2 := Call(context.Background(), reg2, "svc", "/x", "sbx-1", "subj-1", "svc1")
	if result2.ContentType != "text/plain" {
		t.Fatalf("expected valid Content-Type to pass through, got %q", result2.ContentType)
	}
	if result2.RetryAfter != "5" {
		t.Fatalf("expected valid Retry-After to pass through, got %q", result2.RetryAfter)
	}
}

func TestCallUnregisteredTargetReturns503WithoutDialing(t *testing.T) {
	var hits atomic.Int32
	reg := startFakeService(t, func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(200)
	}, 0)

	result := Call(context.Background(), reg, "unregistered-target", "/x", "sbx-1", "subj-1", "svc1")
	if result.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", result.StatusCode)
	}
	if hits.Load() != 0 {
		t.Fatal("expected zero dial attempts for an unregistered target")
	}
}

func TestCallEmptyTargetReturns503(t *testing.T) {
	reg := Registry{}
	result := Call(context.Background(), reg, "", "/x", "sbx-1", "subj-1", "svc1")
	if result.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", result.StatusCode)
	}
}

func TestSanitizeHelpers(t *testing.T) {
	if got := sanitizeContentType(""); got != "" {
		t.Fatalf("empty Content-Type: got %q", got)
	}
	if got := sanitizeContentType("text/plain; charset=utf-8"); got != "text/plain; charset=utf-8" {
		t.Fatalf("valid Content-Type: got %q", got)
	}
	if got := sanitizeContentType("not a media type;;;"); got != "" {
		t.Fatalf("invalid Content-Type: got %q", got)
	}
	if got := sanitizeRetryAfter("120"); got != "120" {
		t.Fatalf("delay-seconds Retry-After: got %q", got)
	}
	if got := sanitizeRetryAfter("Wed, 21 Oct 2026 07:28:00 GMT"); got != "Wed, 21 Oct 2026 07:28:00 GMT" {
		t.Fatalf("HTTP-date Retry-After: got %q", got)
	}
	if got := sanitizeRetryAfter("-1"); got != "" {
		t.Fatalf("negative delay-seconds Retry-After must be rejected (RFC 9110 delay-seconds is 1*DIGIT, no sign): got %q", got)
	}
	if got := sanitizeRetryAfter("+5"); got != "" {
		t.Fatalf("signed delay-seconds Retry-After must be rejected: got %q", got)
	}
}

func TestBuildRegistryConvertsEntries(t *testing.T) {
	reg, err := BuildRegistry(map[string]config.MMDSServiceRegistryEntry{
		"credential-broker": {Endpoint: "unix:///run/kuasar/mmds/credential-broker.sock", Timeout: "5s", MaxResponseBytes: 4096},
	})
	if err != nil {
		t.Fatal(err)
	}
	e, ok := reg["credential-broker"]
	if !ok {
		t.Fatal("expected credential-broker to be present")
	}
	if e.SocketPath != "/run/kuasar/mmds/credential-broker.sock" {
		t.Fatalf("SocketPath = %q", e.SocketPath)
	}
	if e.Timeout != 5*time.Second {
		t.Fatalf("Timeout = %v, want 5s", e.Timeout)
	}
	if e.MaxResponseBytes != 4096 {
		t.Fatalf("MaxResponseBytes = %d, want 4096", e.MaxResponseBytes)
	}
}

func TestBuildRegistryDefaultsZeroMaxResponseBytes(t *testing.T) {
	reg, err := BuildRegistry(map[string]config.MMDSServiceRegistryEntry{
		"svc": {Endpoint: "unix:///run/svc.sock"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if reg["svc"].MaxResponseBytes != 64*1024 {
		t.Fatalf("MaxResponseBytes = %d, want default 64KiB", reg["svc"].MaxResponseBytes)
	}
}

func TestBuildRegistryRejectsNonUnixScheme(t *testing.T) {
	_, err := BuildRegistry(map[string]config.MMDSServiceRegistryEntry{
		"svc": {Endpoint: "tcp://127.0.0.1:8080"},
	})
	if err == nil || !strings.Contains(err.Error(), "unix://") {
		t.Fatalf("err = %v, want a unix:// complaint", err)
	}
}

func TestBuildRegistryRejectsNonAbsolutePath(t *testing.T) {
	_, err := BuildRegistry(map[string]config.MMDSServiceRegistryEntry{
		"svc": {Endpoint: "unix://relative/path.sock"},
	})
	if err == nil || !strings.Contains(err.Error(), "absolute") {
		t.Fatalf("err = %v, want an absolute-path complaint", err)
	}
}
