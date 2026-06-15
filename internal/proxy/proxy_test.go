package proxy_test

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/proxy"
)

type stubRouter struct{ r proxy.Route }

func (s stubRouter) Route(ctx context.Context, sid string, port int) (proxy.Route, error) {
	return s.r, nil
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
	if sid, port, ok := proxy.ParseSandbox(req); !ok || sid != "zzz" || port != 49983 {
		t.Fatalf("header parse: %q %d %v", sid, port, ok)
	}
	req = httptest.NewRequest("GET", "http://noport/y", nil)
	req.Host = "noport"
	if _, _, ok := proxy.ParseSandbox(req); ok {
		t.Fatal("expected parse failure for host without <port>-<sid>")
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

	do := func(token, query string) (int, string) {
		req, _ := http.NewRequest("GET", ts.URL+"/echo"+query, nil)
		req.Header.Set("E2b-Sandbox-Id", "s1")
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

	if code, body := do("tok", ""); code != 200 || body != "hello from envd" {
		t.Fatalf("valid token: code=%d body=%q", code, body)
	}
	if code, _ := do("wrong", ""); code != 401 {
		t.Fatalf("wrong token: code=%d (want 401)", code)
	}
	if code, _ := do("", ""); code != 401 {
		t.Fatalf("missing token: code=%d (want 401)", code)
	}
	if code, _ := do("", "?signature=abc&username=user"); code != 200 {
		t.Fatalf("pre-signed file url: code=%d (want 200)", code)
	}
	mode = "off"
	if code, _ := do("", ""); code != 200 {
		t.Fatalf("auth off: code=%d (want 200)", code)
	}
	mode = "log"
	if code, body := do("wrong", ""); code != 200 || body != "hello from envd" {
		t.Fatalf("auth log forwards: code=%d body=%q", code, body)
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

	px := proxy.New(stubRouter{proxy.Route{Kind: proxy.KindTCP, Addr: backLn.Addr().String(), AccessToken: "tok"}},
		func() string { return "enforce" }, log, nil)
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
		fmt.Fprintf(&hdr, "CONNECT ignored:%s HTTP/1.1\r\nHost: ignored:%s\r\nE2b-Sandbox-Id: s1\r\n", bport, bport)
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

	// Missing token in enforce mode: 401, no tunnel.
	code, c2, _ := connect("")
	if code != http.StatusUnauthorized {
		t.Fatalf("CONNECT without token = %d (want 401)", code)
	}
	c2.Close()
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
