package router

import (
	"bufio"
	"bytes"
	"encoding/binary"
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
	sandboxctl "github.com/kuasar-sandbox/sandboxer/pkg/ctl"
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
	expired, err := keys.MintExecAccessToken(route.ServiceSecret, route.StableID, time.Now().Add(-time.Second).Unix())
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
	token, err := keys.MintExecAccessToken(paused.ServiceSecret, paused.StableID, 0)
	if err != nil {
		t.Fatal(err)
	}
	const internalDetail = "node stable-g7 rejected ctl path /private/run/ctl.sock"
	for _, test := range []struct {
		name     string
		upstream int
	}{
		{name: "stale token", upstream: http.StatusUnauthorized},
		{name: "route disappeared", upstream: http.StatusNotFound},
		{name: "activation failed", upstream: http.StatusInternalServerError},
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
			if resp.Code != http.StatusOK || reserveHits.Load() == 0 {
				t.Fatalf("status = %d, body = %q, reserve hits = %d; want post-accept 200 and Reserve attempt",
					resp.Code, resp.Body.String(), reserveHits.Load())
			}
			assertRouterExecRejected(t, resp.Body.Bytes())
			if strings.Contains(resp.Body.String(), internalDetail) ||
				strings.Contains(resp.Body.String(), "stable-g7") || strings.Contains(resp.Body.String(), "/private/") {
				t.Fatalf("public reserve error leaked internal detail: %q", resp.Body.String())
			}
		})
	}
}

func TestClusterExecConditionFailureDoesNotReserveOrDialNode(t *testing.T) {
	for _, test := range []struct {
		name     string
		endpoint bool
	}{
		{name: "missing target"},
		{name: "direct target", endpoint: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			var nodeHits atomic.Int32
			node := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
				nodeHits.Add(1)
			}))
			defer node.Close()
			endpoint := ""
			if test.endpoint {
				endpoint = strings.TrimPrefix(node.URL, "http://")
			}
			route := routerTestRouteResolve(t, "stable", "/g", "rk", endpoint, types.ProfileBare)
			route.State = "paused"
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
			token, err := keys.MintExecAccessTokenWithConditions(
				route.ServiceSecret, route.StableID, 0,
				[]string{`request.argv == ['/bin/allowed']`},
			)
			if err != nil {
				t.Fatal(err)
			}
			rt := New(strings.TrimPrefix(control.URL, "http://"), "test.local", 0, nil, discardRouterLogger())
			response := httptest.NewRecorder()
			rt.Handler().ServeHTTP(response, clusterExecRequest(token, 0))
			if response.Code != http.StatusOK {
				t.Fatalf("status = %d, want post-accept 200", response.Code)
			}
			assertRouterExecRejected(t, response.Body.Bytes())
			if routeHits.Load() != 1 || reserveHits.Load() != 0 || nodeHits.Load() != 0 {
				t.Fatalf("condition failure route=%d reserve=%d node=%d", routeHits.Load(), reserveHits.Load(), nodeHits.Load())
			}
		})
	}
}

func TestClusterExecReserveRevalidatesStableLineageBeforeNodeDial(t *testing.T) {
	var nodeHits atomic.Int32
	node := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		nodeHits.Add(1)
	}))
	defer node.Close()
	initial := routerTestRouteResolve(t, "stable", "/g", "rk", "", types.ProfileBare)
	initial.State = "paused"
	changed := routerTestRouteResolve(t, "stable", "/g", "rk", strings.TrimPrefix(node.URL, "http://"), types.ProfileBare)
	changed.StableID = "different-lineage"
	var reserveHits atomic.Int32
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/route-link/route":
			_ = json.NewEncoder(w).Encode(initial)
		case "/route-link/reserve":
			reserveHits.Add(1)
			_ = json.NewEncoder(w).Encode(reserveResult{Route: changed})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer control.Close()
	token, err := keys.MintExecAccessToken(initial.ServiceSecret, initial.StableID, 0)
	if err != nil {
		t.Fatal(err)
	}
	rt := New(strings.TrimPrefix(control.URL, "http://"), "test.local", 0, nil, discardRouterLogger())
	response := httptest.NewRecorder()
	rt.Handler().ServeHTTP(response, clusterExecRequest(token, 0))
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want post-accept 200", response.Code)
	}
	assertRouterExecRejected(t, response.Body.Bytes())
	if reserveHits.Load() != 1 || nodeHits.Load() != 0 {
		t.Fatalf("lineage change reserve=%d node=%d", reserveHits.Load(), nodeHits.Load())
	}
}

func TestClusterExecRelaysNodeCtlErrorWithoutSynthesizingAnother(t *testing.T) {
	frame := validRouterExecFrame()
	var want bytes.Buffer
	if err := sandboxctl.WriteMessage(&want, sandboxctl.Response{Type: sandboxctl.TypeError, Msg: "node rejected exec"}); err != nil {
		t.Fatal(err)
	}
	node := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
		if _, err := rw.WriteString("HTTP/1.1 200 Connection Established\r\n\r\n"); err != nil {
			t.Error(err)
			return
		}
		if err := rw.Flush(); err != nil {
			t.Error(err)
			return
		}
		got := make([]byte, len(frame))
		if _, err := io.ReadFull(rw.Reader, got); err != nil || !bytes.Equal(got, frame) {
			t.Errorf("node first frame = %q, %v", got, err)
			return
		}
		if _, err := conn.Write(want.Bytes()); err != nil {
			t.Error(err)
			return
		}
		if closer, ok := conn.(interface{ CloseWrite() error }); ok {
			_ = closer.CloseWrite()
		}
	}))
	defer node.Close()
	route := routerTestRouteResolve(t, "stable", "/g", "rk", strings.TrimPrefix(node.URL, "http://"), types.ProfileBare)
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(route)
	}))
	defer control.Close()
	token, err := keys.MintExecAccessToken(route.ServiceSecret, route.StableID, 0)
	if err != nil {
		t.Fatal(err)
	}
	rt := New(strings.TrimPrefix(control.URL, "http://"), "test.local", 0, nil, discardRouterLogger())
	front := httptest.NewServer(rt.Handler())
	defer front.Close()
	status, output := rawClusterExecConnect(t, front.Listener.Addr().String(), token, 0, frame)
	if status != http.StatusOK || !bytes.Equal(output, want.Bytes()) {
		t.Fatalf("status=%d node ctl output=%q, want %q", status, output, want.Bytes())
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
	token, err := keys.MintExecAccessToken(route.ServiceSecret, route.StableID, 0)
	if err != nil {
		t.Fatal(err)
	}
	clientInput := append(validRouterExecFrame(), []byte("buffered-client-mux-tail")...)
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

func TestClusterExecKnownNonReadyTargetSkipsReserve(t *testing.T) {
	for _, state := range []string{"paused", "starting"} {
		t.Run(state, func(t *testing.T) {
			node, observed, backendInput := newExecTunnelNode(t, "", "direct-tail")
			defer node.Close()
			route := routerTestRouteResolve(t, "stable", "/g", "rk", strings.TrimPrefix(node.URL, "http://"), types.ProfileBare)
			route.State = state
			var reserveHits atomic.Int32
			control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/route-link/route":
					_ = json.NewEncoder(w).Encode(route)
				case "/route-link/reserve":
					reserveHits.Add(1)
					http.Error(w, "must not reserve a known target", http.StatusInternalServerError)
				default:
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			defer control.Close()
			rt := New(strings.TrimPrefix(control.URL, "http://"), "test.local", 0, nil, discardRouterLogger())
			front := httptest.NewServer(rt.Handler())
			defer front.Close()
			token, err := keys.MintExecAccessToken(route.ServiceSecret, route.StableID, 0)
			if err != nil {
				t.Fatal(err)
			}
			status, output := rawClusterExecConnect(t, front.Listener.Addr().String(), token, 0, nil)
			if status != http.StatusOK || string(output) != "direct-tail" {
				t.Fatalf("CONNECT status=%d output=%q", status, output)
			}
			if reserveHits.Load() != 0 {
				t.Fatalf("reserve hits=%d, want 0", reserveHits.Load())
			}
			got := <-observed
			if got.sid != route.NodeSandboxID || got.service != string(proxypkg.ConnectServiceExec) || got.token != token {
				t.Fatalf("node CONNECT = %#v", got)
			}
			if gotInput := <-backendInput; !bytes.Equal(gotInput, validRouterExecFrame()) {
				t.Fatalf("tunnel input = %q", gotInput)
			}
		})
	}
}

func TestClusterExecTypedStaleTargetRefreshesThroughReserve(t *testing.T) {
	var staleHits, reserveHits atomic.Int32
	stale := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		staleHits.Add(1)
		w.Header().Set(proxypkg.HeaderProxyError, proxypkg.ProxyErrorNotFound)
		http.Error(w, "stale node-local sandbox", http.StatusNotFound)
	}))
	defer stale.Close()
	fresh, observed, backendInput := newExecTunnelNode(t, "", "fresh-tail")
	defer fresh.Close()
	staleRoute := routerTestRouteResolve(t, "stable", "/g", "rk", strings.TrimPrefix(stale.URL, "http://"), types.ProfileBare)
	staleRoute.State = "starting"
	freshRoute := routerTestRouteResolve(t, "stable", "/g", "rk", strings.TrimPrefix(fresh.URL, "http://"), types.ProfileBare)
	freshRoute.NodeSandboxID = "stable-g1"
	freshRoute.RouteRevision = 2
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/route-link/route":
			_ = json.NewEncoder(w).Encode(staleRoute)
		case "/route-link/reserve":
			reserveHits.Add(1)
			if r.Header.Get(proxypkg.HeaderSandboxService) != string(proxypkg.ConnectServiceExec) {
				t.Fatalf("reserve service=%q", r.Header.Get(proxypkg.HeaderSandboxService))
			}
			_ = json.NewEncoder(w).Encode(reserveResult{Route: freshRoute})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer control.Close()
	rt := New(strings.TrimPrefix(control.URL, "http://"), "test.local", 0, nil, discardRouterLogger())
	front := httptest.NewServer(rt.Handler())
	defer front.Close()
	token, err := keys.MintExecAccessToken(staleRoute.ServiceSecret, staleRoute.StableID, 0)
	if err != nil {
		t.Fatal(err)
	}
	status, output := rawClusterExecConnect(t, front.Listener.Addr().String(), token, 0, nil)
	if status != http.StatusOK || string(output) != "fresh-tail" {
		t.Fatalf("CONNECT status=%d output=%q", status, output)
	}
	if staleHits.Load() != 1 || reserveHits.Load() != 1 {
		t.Fatalf("stale hits=%d reserve hits=%d", staleHits.Load(), reserveHits.Load())
	}
	got := <-observed
	if got.sid != freshRoute.NodeSandboxID || got.service != string(proxypkg.ConnectServiceExec) || got.token != token {
		t.Fatalf("fresh node CONNECT = %#v", got)
	}
	if gotInput := <-backendInput; !bytes.Equal(gotInput, validRouterExecFrame()) {
		t.Fatalf("tunnel input = %q", gotInput)
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
	token, err := keys.MintExecAccessToken(paused.ServiceSecret, paused.StableID, 0)
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
	if gotInput := <-backendInput; !bytes.Equal(gotInput, validRouterExecFrame()) {
		t.Fatalf("tunnel input = %q", gotInput)
	}
}

func clusterExecRequest(token string, port int) *http.Request {
	req := httptest.NewRequest(http.MethodConnect, "http://sandbox:443", bytes.NewReader(validRouterExecFrame()))
	req.ProtoMajor = 2
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
	if input == nil {
		input = validRouterExecFrame()
	}
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

func validRouterExecFrame() []byte {
	payload := []byte(`{"type":"exec_request","exec":{"argv":["/bin/true"]}}`)
	frame := make([]byte, 4+len(payload))
	binary.LittleEndian.PutUint32(frame[:4], uint32(len(payload)))
	copy(frame[4:], payload)
	return frame
}

func assertRouterExecRejected(t *testing.T, raw []byte) {
	t.Helper()
	var response sandboxctl.Response
	if err := sandboxctl.ReadMessage(bytes.NewReader(raw), &response); err != nil {
		t.Fatalf("read ctl rejection %q: %v", raw, err)
	}
	if response.Type != sandboxctl.TypeError || response.Msg != "exec request rejected" {
		t.Fatalf("ctl rejection = %+v", response)
	}
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
