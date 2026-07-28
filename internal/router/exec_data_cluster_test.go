package router

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/keys"
	proxypkg "github.com/kuasar-sandbox/orchestrator/internal/proxy"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

type observedExecConnect struct {
	authority string
	sid       string
	service   string
	port      string
	token     string
}

func TestClusterExecOrdinaryHTTPReturns405BeforeRouteLookup(t *testing.T) {
	var controlHits atomic.Int32
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		controlHits.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer control.Close()
	rt := New(strings.TrimPrefix(control.URL, "http://"), "test.local", 0, nil, discardRouterLogger())
	req := httptest.NewRequest(http.MethodGet, "http://sandbox:443/", nil)
	req.Host = "sandbox:443"
	req.Header.Set(proxypkg.HeaderSandboxID, "stable")
	req.Header.Set(proxypkg.HeaderSandboxService, string(proxypkg.ConnectServiceExec))
	req.Header.Set(HeaderGroup, "/g")
	req.Header.Set(HeaderRouteKey, "rk")
	resp := httptest.NewRecorder()
	rt.Handler().ServeHTTP(resp, req)
	if resp.Code != http.StatusMethodNotAllowed || resp.Header().Get("Allow") != http.MethodConnect {
		t.Fatalf("response = %d Allow=%q, want 405 Allow=CONNECT", resp.Code, resp.Header().Get("Allow"))
	}
	if controlHits.Load() != 0 {
		t.Fatalf("ordinary exec reached route-link %d times", controlHits.Load())
	}
}

func TestClusterExecRejectsInvalidKATBeforeReserve(t *testing.T) {
	route := routerTestRouteResolve(t, "stable", "/g", "rk", "", types.ProfileBare)
	route.State = "paused"
	wrongSID, err := keys.MintExecAccessToken(route.ServiceSecret, "other-stable", 0)
	if err != nil {
		t.Fatal(err)
	}
	expired, err := keys.MintExecAccessToken(route.ServiceSecret, route.AuthSandboxID, time.Now().Add(-time.Second).Unix())
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name  string
		token string
	}{
		{name: "missing"},
		{name: "malformed", token: "not-a-kat"},
		{name: "expired", token: expired},
		{name: "wrong sid", token: wrongSID},
		{name: "wrong audience", token: route.ForwardAccessToken},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var routeHits, reserveHits atomic.Int32
			control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/route-link/route":
					routeHits.Add(1)
					_ = json.NewEncoder(w).Encode(route)
				case "/route-link/reserve":
					reserveHits.Add(1)
					w.WriteHeader(http.StatusInternalServerError)
				default:
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			defer control.Close()
			rt := New(strings.TrimPrefix(control.URL, "http://"), "test.local", 0, nil, discardRouterLogger())
			rt.SetDataPlaneAuth("off")
			req := clusterExecRequest(test.token, 0)
			resp := httptest.NewRecorder()
			rt.Handler().ServeHTTP(resp, req)
			if resp.Code != http.StatusUnauthorized {
				t.Fatalf("status = %d, want 401", resp.Code)
			}
			if routeHits.Load() != 1 || reserveHits.Load() != 0 {
				t.Fatalf("route hits=%d reserve hits=%d, want read-only lookup only", routeHits.Load(), reserveHits.Load())
			}
		})
	}
}

func TestClusterExecSanitizesRouteLookupFailure(t *testing.T) {
	const internalDetail = "registry sqlite failed at /private/registry.db"
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/route-link/route" {
			t.Fatalf("route-link path = %q", r.URL.Path)
		}
		http.Error(w, internalDetail, http.StatusInternalServerError)
	}))
	defer control.Close()
	rt := New(strings.TrimPrefix(control.URL, "http://"), "test.local", 0, nil, discardRouterLogger())

	resp := httptest.NewRecorder()
	rt.Handler().ServeHTTP(resp, clusterExecRequest("unparsed-before-route", 0))
	if resp.Code != http.StatusServiceUnavailable || resp.Body.String() != "routing unavailable\n" {
		t.Fatalf("status = %d, body = %q", resp.Code, resp.Body.String())
	}
	if strings.Contains(resp.Body.String(), internalDetail) || strings.Contains(resp.Body.String(), "/private/") {
		t.Fatalf("public route error leaked internal detail: %q", resp.Body.String())
	}
}

func TestClusterExecSanitizesReserveFailure(t *testing.T) {
	paused := routerTestRouteResolve(t, "stable", "/g", "rk", "", types.ProfileBare)
	paused.State = "paused"
	token, err := keys.MintExecAccessToken(paused.ServiceSecret, paused.AuthSandboxID, 0)
	if err != nil {
		t.Fatal(err)
	}
	const internalDetail = "node stable-g7 rejected ctl path /private/run/ctl.sock"
	for _, test := range []struct {
		name       string
		upstream   int
		wantStatus int
		wantBody   string
	}{
		{name: "stale token", upstream: http.StatusUnauthorized, wantStatus: http.StatusUnauthorized, wantBody: "invalid access token\n"},
		{name: "route disappeared", upstream: http.StatusNotFound, wantStatus: http.StatusNotFound, wantBody: "sandbox not found\n"},
		{name: "activation failed", upstream: http.StatusInternalServerError, wantStatus: http.StatusServiceUnavailable, wantBody: "sandbox activation failed\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var reserveHits atomic.Int32
			control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/route-link/route":
					_ = json.NewEncoder(w).Encode(paused)
				case "/route-link/reserve":
					reserveHits.Add(1)
					http.Error(w, internalDetail, test.upstream)
				default:
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			defer control.Close()
			rt := New(strings.TrimPrefix(control.URL, "http://"), "test.local", 0, nil, discardRouterLogger())

			resp := httptest.NewRecorder()
			rt.Handler().ServeHTTP(resp, clusterExecRequest(token, 0))
			if resp.Code != test.wantStatus || resp.Body.String() != test.wantBody || reserveHits.Load() == 0 {
				t.Fatalf("status = %d, body = %q, reserve hits = %d; want %d, %q and at least one Reserve attempt",
					resp.Code, resp.Body.String(), reserveHits.Load(), test.wantStatus, test.wantBody)
			}
			if strings.Contains(resp.Body.String(), internalDetail) ||
				strings.Contains(resp.Body.String(), "stable-g7") || strings.Contains(resp.Body.String(), "/private/") {
				t.Fatalf("public reserve error leaked internal detail: %q", resp.Body.String())
			}
		})
	}
}

func TestClusterExecReadyCacheRewritesOnlySIDAndPreservesTunnel(t *testing.T) {
	node, observed, backendInput := newExecTunnelNode(t, "node-prefetched/", "node-tail")
	defer node.Close()
	var controlHits atomic.Int32
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		controlHits.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer control.Close()
	route := routerTestRouteResolve(t, "stable", "/g", "rk", strings.TrimPrefix(node.URL, "http://"), types.ProfileBare)
	rt := New(strings.TrimPrefix(control.URL, "http://"), "test.local", 0, nil, discardRouterLogger())
	rt.rememberRoute(&route)
	front := httptest.NewServer(rt.Handler())
	defer front.Close()
	token, err := keys.MintExecAccessToken(route.ServiceSecret, route.AuthSandboxID, 0)
	if err != nil {
		t.Fatal(err)
	}
	clientInput := []byte("buffered-client-frame/mux-tail")
	status, output := rawClusterExecConnect(t, front.Listener.Addr().String(), token, 8123, clientInput)
	if status != http.StatusOK || string(output) != "node-prefetched/node-tail" {
		t.Fatalf("CONNECT status=%d output=%q", status, output)
	}
	if controlHits.Load() != 0 {
		t.Fatalf("ready cached route called Registry %d times", controlHits.Load())
	}
	got := <-observed
	want := observedExecConnect{
		authority: "sandbox:443",
		sid:       route.NodeSandboxID,
		service:   string(proxypkg.ConnectServiceExec),
		port:      "8123",
		token:     token,
	}
	if got != want {
		t.Fatalf("node CONNECT = %#v, want %#v", got, want)
	}
	if gotInput := <-backendInput; !bytes.Equal(gotInput, clientInput) {
		t.Fatalf("node tunnel input = %q, want %q", gotInput, clientInput)
	}
}

func TestClusterExecPausedReserveRechecksKATAndUsesCurrentNode(t *testing.T) {
	node, observed, backendInput := newExecTunnelNode(t, "", "resumed-tail")
	defer node.Close()
	paused := routerTestRouteResolve(t, "stable", "/g", "rk", "", types.ProfileBare)
	paused.State = "paused"
	current := routerTestRouteResolve(t, "stable", "/g", "rk", strings.TrimPrefix(node.URL, "http://"), types.ProfileBare)
	current.NodeSandboxID = "stable-g7"
	current.RouteRevision = 7
	var routeHits, reserveHits atomic.Int32
	var reserveService, reserveToken string
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/route-link/route":
			routeHits.Add(1)
			_ = json.NewEncoder(w).Encode(paused)
		case "/route-link/reserve":
			reserveHits.Add(1)
			if query := r.URL.Query(); query.Get("operation") != "data" || query.Get("sid") != "stable" || query.Get("port") != "" {
				t.Fatalf("reserve query = %q", r.URL.RawQuery)
			}
			reserveService = r.Header.Get(proxypkg.HeaderSandboxService)
			reserveToken = r.Header.Get(HeaderAccessTok)
			body, err := io.ReadAll(r.Body)
			if err != nil || len(body) != 0 {
				t.Fatalf("reserve body = %q err=%v, want empty", body, err)
			}
			_ = json.NewEncoder(w).Encode(reserveResult{Route: current})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer control.Close()
	rt := New(strings.TrimPrefix(control.URL, "http://"), "test.local", 0, nil, discardRouterLogger())
	front := httptest.NewServer(rt.Handler())
	defer front.Close()
	token, err := keys.MintExecAccessToken(paused.ServiceSecret, paused.AuthSandboxID, 0)
	if err != nil {
		t.Fatal(err)
	}
	status, output := rawClusterExecConnect(t, front.Listener.Addr().String(), token, 0, nil)
	if status != http.StatusOK || string(output) != "resumed-tail" {
		t.Fatalf("CONNECT status=%d output=%q", status, output)
	}
	if routeHits.Load() != 1 || reserveHits.Load() != 1 {
		t.Fatalf("route hits=%d reserve hits=%d", routeHits.Load(), reserveHits.Load())
	}
	if reserveService != string(proxypkg.ConnectServiceExec) || reserveToken != token {
		t.Fatalf("reserve service=%q token changed=%v", reserveService, reserveToken != token)
	}
	got := <-observed
	if got.sid != current.NodeSandboxID || got.service != string(proxypkg.ConnectServiceExec) || got.port != "" || got.token != token {
		t.Fatalf("current node CONNECT = %#v", got)
	}
	if gotInput := <-backendInput; len(gotInput) != 0 {
		t.Fatalf("unexpected tunnel input %q", gotInput)
	}
}

func clusterExecRequest(token string, port int) *http.Request {
	req := httptest.NewRequest(http.MethodConnect, "http://sandbox:443", nil)
	req.Host = "sandbox:443"
	req.Header.Set(proxypkg.HeaderSandboxID, "stable")
	req.Header.Set(proxypkg.HeaderSandboxService, string(proxypkg.ConnectServiceExec))
	if port > 0 {
		req.Header.Set(proxypkg.HeaderSandboxPort, fmt.Sprint(port))
	}
	req.Header.Set(HeaderGroup, "/g")
	req.Header.Set(HeaderRouteKey, "rk")
	if token != "" {
		req.Header.Set(HeaderAccessTok, token)
	}
	return req
}

func rawClusterExecConnect(t *testing.T, addr, token string, port int, input []byte) (int, []byte) {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	tcpConn := conn.(*net.TCPConn)
	defer tcpConn.Close()
	_ = tcpConn.SetDeadline(time.Now().Add(5 * time.Second))
	var request bytes.Buffer
	fmt.Fprintf(&request, "CONNECT sandbox:443 HTTP/1.1\r\nHost: sandbox:443\r\n%s: stable\r\n%s: exec\r\n%s: /g\r\n%s: rk\r\n%s: %s\r\n",
		proxypkg.HeaderSandboxID, proxypkg.HeaderSandboxService, HeaderGroup, HeaderRouteKey, HeaderAccessTok, token)
	if port > 0 {
		fmt.Fprintf(&request, "%s: %d\r\n", proxypkg.HeaderSandboxPort, port)
	}
	request.WriteString("\r\n")
	request.Write(input)
	if _, err := tcpConn.Write(request.Bytes()); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(tcpConn)
	resp, err := http.ReadResponse(reader, &http.Request{Method: http.MethodConnect})
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return resp.StatusCode, nil
	}
	if err := tcpConn.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	output, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, output
}

func newExecTunnelNode(t *testing.T, prefetched, tail string) (*httptest.Server, <-chan observedExecConnect, <-chan []byte) {
	t.Helper()
	observed := make(chan observedExecConnect, 1)
	input := make(chan []byte, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		observed <- observedExecConnect{
			authority: r.Host,
			sid:       r.Header.Get(proxypkg.HeaderSandboxID),
			service:   r.Header.Get(proxypkg.HeaderSandboxService),
			port:      r.Header.Get(proxypkg.HeaderSandboxPort),
			token:     r.Header.Get(HeaderAccessTok),
		}
		hijacker, ok := w.(http.Hijacker)
		if !ok {
			t.Error("node response writer cannot hijack")
			return
		}
		conn, rw, err := hijacker.Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.Close()
		if _, err := rw.WriteString("HTTP/1.1 200 Connection Established\r\n\r\n" + prefetched); err != nil {
			t.Error(err)
			return
		}
		if err := rw.Flush(); err != nil {
			t.Error(err)
			return
		}
		got, err := io.ReadAll(rw.Reader)
		if err != nil {
			t.Error(err)
			return
		}
		input <- got
		if _, err := io.WriteString(conn, tail); err != nil {
			t.Error(err)
			return
		}
		if closer, ok := conn.(interface{ CloseWrite() error }); ok {
			_ = closer.CloseWrite()
		}
	}))
	return server, observed, input
}

func discardRouterLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
