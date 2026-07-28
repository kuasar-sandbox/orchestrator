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

	"github.com/kuasar-sandbox/orchestrator/internal/envdsign"
	"github.com/kuasar-sandbox/orchestrator/internal/proxy"
)

type stubRouter struct{ r proxy.Route }

func (s stubRouter) Route(ctx context.Context, sid string, target proxy.ConnectTarget) (proxy.Route, error) {
	return s.r, nil
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
		profile   string
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
			route := proxy.RouteForTarget(tc.profile, "/e.sock", "/c.sock", "10.0.0.5", "envd", "forward", tc.target)
			if route.Kind != tc.wantKind || route.UDS != tc.wantUDS || route.Addr != tc.wantAddr || route.AccessToken != tc.wantToken {
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
		route:  proxy.Route{Kind: proxy.KindTCP, Addr: strings.TrimPrefix(backend.URL, "http://"), AccessToken: "tok"},
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
	target *proxy.ConnectTarget
}

func (r *recordingRouter) Route(_ context.Context, _ string, target proxy.ConnectTarget) (proxy.Route, error) {
	if r.target != nil {
		*r.target = target
	}
	return r.route, nil
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
	px := proxy.New(stubRouter{proxy.Route{Kind: proxy.KindUDS, UDS: upSock}},
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
