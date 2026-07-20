package proxy_test

import (
	"bufio"
	"bytes"
	"context"
	"errors"
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

	"github.com/kuasar-sandbox/orchestrator/internal/envdsign"
	"github.com/kuasar-sandbox/orchestrator/internal/proxy"
)

type stubRouter struct{ r proxy.Route }

func (s stubRouter) Route(ctx context.Context, request proxy.RouteRequest) (proxy.Route, error) {
	return s.r, nil
}

type routeFunc func(context.Context, proxy.RouteRequest) (proxy.Route, error)

func (f routeFunc) Route(ctx context.Context, request proxy.RouteRequest) (proxy.Route, error) {
	return f(ctx, request)
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
	if r := proxy.RouteForTarget("e2b", "/e.sock", "/c.sock", "10.0.0.5", "tok", 49983); r.Kind != proxy.KindUDS || r.UDS != "/e.sock" || r.AccessToken != "tok" {
		t.Fatalf("envd port: %+v", r)
	}
	if r := proxy.RouteForTarget("e2b", "/e.sock", "/c.sock", "10.0.0.5", "tok", 49999); r.Kind != proxy.KindUDS || r.UDS != "/c.sock" {
		t.Fatalf("ci port: %+v", r)
	}
	if r := proxy.RouteForTarget("e2b", "/e.sock", "/c.sock", "10.0.0.5", "tok", 8080); r.Kind != proxy.KindTCP || r.Addr != "10.0.0.5:8080" {
		t.Fatalf("user port: %+v", r)
	}
	if r := proxy.RouteForTarget("bare", "", "", "10.0.0.5", "", 49983); r.Kind != proxy.KindDeny {
		t.Fatalf("bare control: %+v", r)
	}
	if r := proxy.RouteForTarget("bare", "", "", "10.0.0.5", "", 8080); r.Kind != proxy.KindTCP {
		t.Fatalf("bare user: %+v", r)
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

func TestRouteFenceFailure(t *testing.T) {
	exact := proxy.RouteRequest{
		SandboxID: "s1", Port: 49983, ExpectedNodeID: "n1", ExpectedNodeEpoch: 7,
		ExpectedRegistryGeneration: "g1", ExpectedBindingDigest: "d1",
	}
	tests := []struct {
		name    string
		request proxy.RouteRequest
		managed bool
		nodeID  string
		epoch   uint64
		gen     string
		digest  string
		kind    proxy.Kind
		failed  bool
	}{
		{name: "unmanaged", request: proxy.RouteRequest{SandboxID: "s1"}},
		{name: "fenced request to unmanaged object", request: exact, kind: proxy.KindWrongBinding, failed: true},
		{name: "managed requires fence", request: proxy.RouteRequest{SandboxID: "s1"}, managed: true, nodeID: "n1", epoch: 7, gen: "g1", digest: "d1", kind: proxy.KindWrongBinding, failed: true},
		{name: "wrong node", request: exact, managed: true, nodeID: "n2", epoch: 7, gen: "g1", digest: "d1", kind: proxy.KindWrongNodeEpoch, failed: true},
		{name: "wrong epoch", request: exact, managed: true, nodeID: "n1", epoch: 8, gen: "g1", digest: "d1", kind: proxy.KindWrongNodeEpoch, failed: true},
		{name: "wrong generation", request: exact, managed: true, nodeID: "n1", epoch: 7, gen: "g2", digest: "d1", kind: proxy.KindWrongBinding, failed: true},
		{name: "wrong binding", request: exact, managed: true, nodeID: "n1", epoch: 7, gen: "g1", digest: "d2", kind: proxy.KindWrongBinding, failed: true},
		{name: "exact", request: exact, managed: true, nodeID: "n1", epoch: 7, gen: "g1", digest: "d1"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			kind, failed := proxy.RouteFenceFailure(tc.request, tc.managed, tc.nodeID, tc.epoch, tc.gen, tc.digest)
			if kind != tc.kind || failed != tc.failed {
				t.Fatalf("failure = (%v,%v), want (%v,%v)", kind, failed, tc.kind, tc.failed)
			}
		})
	}
}

func TestProxyFenceFailuresNeverDial(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	tests := []struct {
		kind  proxy.Kind
		error string
	}{
		{proxy.KindWrongNodeEpoch, proxy.ProxyErrorWrongNodeEpoch},
		{proxy.KindWrongBinding, proxy.ProxyErrorWrongBinding},
		{proxy.KindRouteInactive, proxy.ProxyErrorRouteInactive},
	}
	for _, tc := range tests {
		t.Run(tc.error, func(t *testing.T) {
			var dials atomic.Int32
			px := proxy.NewWithDialer(stubRouter{proxy.Route{Kind: tc.kind}}, nil, log, nil,
				func(context.Context, proxy.Route) (net.Conn, error) {
					dials.Add(1)
					return nil, errors.New("unexpected dial")
				})
			req := httptest.NewRequest(http.MethodGet, "http://x/", nil)
			req.Header.Set(proxy.HeaderSandboxID, "s1")
			req.Header.Set(proxy.HeaderSandboxPort, "49983")
			w := httptest.NewRecorder()
			px.ServeHTTP(w, req)
			if w.Code != http.StatusConflict || w.Header().Get(proxy.HeaderProxyError) != tc.error {
				t.Fatalf("response = %d %q", w.Code, w.Header().Get(proxy.HeaderProxyError))
			}
			if dials.Load() != 0 {
				t.Fatal("fence failure dialed backend")
			}
		})
	}
}

func TestProxyRejectsIncompleteFenceBeforeRouting(t *testing.T) {
	var routes atomic.Int32
	px := proxy.New(routeFunc(func(context.Context, proxy.RouteRequest) (proxy.Route, error) {
		routes.Add(1)
		return proxy.Route{Kind: proxy.KindTCP}, nil
	}), nil, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	req := httptest.NewRequest(http.MethodGet, "http://x/", nil)
	req.Header.Set(proxy.HeaderSandboxID, "s1")
	req.Header.Set(proxy.HeaderSandboxPort, "49983")
	req.Header.Set(proxy.HeaderNodeID, "n1")
	w := httptest.NewRecorder()
	px.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest || w.Header().Get(proxy.HeaderProxyError) != proxy.ProxyErrorBadRequest {
		t.Fatalf("response = %d %q", w.Code, w.Header().Get(proxy.HeaderProxyError))
	}
	if routes.Load() != 0 {
		t.Fatal("incomplete fence reached Router")
	}
}

func TestSandboxConnectCarriesCompleteFence(t *testing.T) {
	request := proxy.SandboxConnectRequest{
		RouteRequest: proxy.RouteRequest{
			SandboxID: "s1", Port: 49983, ExpectedNodeID: "n1", ExpectedNodeEpoch: 7,
			ExpectedRegistryGeneration: "g1", ExpectedBindingDigest: "d1",
		},
		AccessToken: "token",
	}
	var wire bytes.Buffer
	if err := proxy.WriteSandboxConnect(&wire, request); err != nil {
		t.Fatal(err)
	}
	req, err := http.ReadRequest(bufio.NewReader(&wire))
	if err != nil {
		t.Fatal(err)
	}
	if req.Method != http.MethodConnect || req.Header.Get(proxy.HeaderNodeID) != "n1" ||
		req.Header.Get(proxy.HeaderNodeEpoch) != "7" || req.Header.Get(proxy.HeaderRegistryGeneration) != "g1" ||
		req.Header.Get(proxy.HeaderBindingDigest) != "d1" || req.Header.Get(proxy.HeaderAccessToken) != "token" {
		t.Fatalf("CONNECT request = %+v headers=%v", req, req.Header)
	}
	request.ExpectedBindingDigest = ""
	if err := proxy.WriteSandboxConnect(io.Discard, request); err == nil {
		t.Fatal("incomplete CONNECT fence accepted")
	}
}

func TestForwardHTTPStripsExecutionFenceHeaders(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	seen := make(chan http.Header, 1)
	go func() {
		defer server.Close()
		req, err := http.ReadRequest(bufio.NewReader(server))
		if err != nil {
			seen <- nil
			return
		}
		seen <- req.Header.Clone()
		_, _ = io.WriteString(server, "HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n")
	}()
	req := httptest.NewRequest(http.MethodGet, "http://sandbox/", nil)
	for name, value := range map[string]string{
		proxy.HeaderNodeID: "n1", proxy.HeaderNodeEpoch: "7",
		proxy.HeaderRegistryGeneration: "g1", proxy.HeaderBindingDigest: "d1",
	} {
		req.Header.Set(name, value)
	}
	resp, err := proxy.ForwardHTTPOnce(req, client, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	headers := <-seen
	for _, name := range []string{proxy.HeaderNodeID, proxy.HeaderNodeEpoch, proxy.HeaderRegistryGeneration, proxy.HeaderBindingDigest} {
		if headers.Get(name) != "" {
			t.Fatalf("internal header %s reached guest", name)
		}
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
	px := proxy.New(stubRouter{proxy.Route{Kind: proxy.KindUDS, UDS: upSock, AccessToken: "tok"}},
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

	px := proxy.New(stubRouter{proxy.Route{Kind: proxy.KindUDS, UDS: upSock, AccessToken: "tok"}},
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
	px := proxy.NewWithDialer(stubRouter{proxy.Route{Kind: proxy.KindTCP, Addr: backLn.Addr().String(), AccessToken: "tok"}},
		func() string { return "enforce" }, log, nil,
		func(ctx context.Context, r proxy.Route) (net.Conn, error) {
			dials.Add(1)
			if r.Kind != proxy.KindTCP {
				return nil, fmt.Errorf("dial route kind = %v, want KindTCP", r.Kind)
			}
			return (&net.Dialer{}).DialContext(ctx, "tcp", r.Addr)
		})
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
	px := proxy.NewWithDialer(stubRouter{proxy.Route{Kind: proxy.KindTCP, Addr: backLn.Addr().String(), AccessToken: "tok"}},
		func() string { return "enforce" }, log, nil,
		func(ctx context.Context, r proxy.Route) (net.Conn, error) {
			dials.Add(1)
			if r.Kind != proxy.KindTCP {
				return nil, fmt.Errorf("dial route kind = %v, want KindTCP", r.Kind)
			}
			return (&net.Dialer{}).DialContext(ctx, "tcp", r.Addr)
		})
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
	nf := proxy.New(stubRouter{proxy.Route{Kind: proxy.KindNotFound}}, nil, log, nil)
	deny := proxy.New(stubRouter{proxy.Route{Kind: proxy.KindDeny}}, nil, log, nil)
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
