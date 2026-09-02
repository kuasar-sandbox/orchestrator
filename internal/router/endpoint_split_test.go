package router

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
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

func TestSandboxControlUsesOnlyAPIEndpoint(t *testing.T) {
	var apiHits, dataHits atomic.Int32
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		apiHits.Add(1)
		if r.Method == http.MethodGet {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]string{"sandboxID": "sb-1-g0"})
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer api.Close()
	data := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		dataHits.Add(1)
		http.Error(w, "control reached data endpoint", http.StatusTeapot)
	}))
	defer data.Close()
	route := routerTestRouteResolve(t, "sb-1", "/g", "rk", strings.TrimPrefix(data.URL, "http://"), types.ProfileE2B)
	route.APIEndpoint = strings.TrimPrefix(api.URL, "http://")
	control := endpointRouteServer(t, route, nil)
	defer control.Close()
	rt := New(strings.TrimPrefix(control.URL, "http://"), "test.local", 0, nil, discardRouterLogger())
	front := httptest.NewServer(rt.Handler())
	defer front.Close()

	requests := []struct {
		method string
		path   string
	}{
		{method: http.MethodGet, path: "/sandboxes/sb-1"},
		{method: http.MethodPost, path: "/sandboxes/sb-1/pause"},
		{method: http.MethodPost, path: "/sandboxes/sb-1/timeout"},
	}
	for _, test := range requests {
		req, _ := http.NewRequest(test.method, front.URL+test.path, nil)
		req.Host = "api.test.local"
		req.Header.Set(HeaderGroup, "/g")
		req.Header.Set(HeaderRouteKey, "rk")
		req.Header.Set(HeaderAPIKey, "e2b_test")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			t.Fatalf("%s %s status=%d", test.method, test.path, resp.StatusCode)
		}
	}
	if apiHits.Load() != int32(len(requests)) || dataHits.Load() != 0 {
		t.Fatalf("control endpoint hits api=%d data=%d", apiHits.Load(), dataHits.Load())
	}
}

func TestSandboxControlMissingAPIEndpointDoesNotUseDataEndpoint(t *testing.T) {
	var dataHits atomic.Int32
	data := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		dataHits.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer data.Close()
	route := routerTestRouteResolve(t, "sb-1", "/g", "rk", strings.TrimPrefix(data.URL, "http://"), types.ProfileE2B)
	route.APIEndpoint = ""
	control := endpointRouteServer(t, route, nil)
	defer control.Close()
	rt := New(strings.TrimPrefix(control.URL, "http://"), "test.local", 0, nil, discardRouterLogger())
	front := httptest.NewServer(rt.Handler())
	defer front.Close()

	req, _ := http.NewRequest(http.MethodGet, front.URL+"/sandboxes/sb-1", nil)
	req.Host = "api.test.local"
	req.Header.Set(HeaderGroup, "/g")
	req.Header.Set(HeaderRouteKey, "rk")
	req.Header.Set(HeaderAPIKey, "e2b_test")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound || dataHits.Load() != 0 {
		t.Fatalf("missing API endpoint status=%d data hits=%d", resp.StatusCode, dataHits.Load())
	}
}

func TestBuildFollowupsUseOnlyAPIEndpoint(t *testing.T) {
	var apiHits, dataHits atomic.Int32
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		apiHits.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer api.Close()
	data := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		dataHits.Add(1)
		http.Error(w, "build reached data endpoint", http.StatusTeapot)
	}))
	defer data.Close()
	build := map[string]any{
		"build_id": "b1", "template_id": "t1", "node_id": "n1", "profile": "e2b",
		"api_endpoint":  strings.TrimPrefix(api.URL, "http://"),
		"data_endpoint": strings.TrimPrefix(data.URL, "http://"),
	}
	control := endpointRouteServer(t, routeResolve{}, build)
	defer control.Close()
	rt := New(strings.TrimPrefix(control.URL, "http://"), "test.local", 0, nil, discardRouterLogger())
	front := httptest.NewServer(rt.Handler())
	defer front.Close()

	for _, test := range []struct {
		method string
		path   string
	}{
		{http.MethodPost, "/v2/templates/t1/builds/b1"},
		{http.MethodGet, "/v2/templates/t1/builds/b1/status"},
		{http.MethodGet, "/v2/templates/t1/builds/b1/files"},
	} {
		req, _ := http.NewRequest(test.method, front.URL+test.path, nil)
		req.Host = "api.test.local"
		req.Header.Set(HeaderGroup, "/g")
		req.Header.Set(HeaderAPIKey, "e2b_test")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusNoContent {
			t.Fatalf("build %s status=%d", test.path, resp.StatusCode)
		}
	}
	if apiHits.Load() != 3 || dataHits.Load() != 0 {
		t.Fatalf("build endpoint hits api=%d data=%d", apiHits.Load(), dataHits.Load())
	}
}

func TestBuildMissingAPIEndpointDoesNotUseDataEndpoint(t *testing.T) {
	var dataHits atomic.Int32
	data := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		dataHits.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer data.Close()
	build := map[string]any{
		"build_id": "b1", "template_id": "t1", "node_id": "n1", "profile": "e2b",
		"api_endpoint": "", "data_endpoint": strings.TrimPrefix(data.URL, "http://"),
	}
	control := endpointRouteServer(t, routeResolve{}, build)
	defer control.Close()
	rt := New(strings.TrimPrefix(control.URL, "http://"), "test.local", 0, nil, discardRouterLogger())
	front := httptest.NewServer(rt.Handler())
	defer front.Close()
	req, _ := http.NewRequest(http.MethodGet, front.URL+"/v2/templates/t1/builds/b1/status", nil)
	req.Host = "api.test.local"
	req.Header.Set(HeaderGroup, "/g")
	req.Header.Set(HeaderAPIKey, "e2b_test")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound || dataHits.Load() != 0 {
		t.Fatalf("missing build API endpoint status=%d data hits=%d", resp.StatusCode, dataHits.Load())
	}
}

func TestOrdinaryHTTPUsesOnlyDataEndpoint(t *testing.T) {
	var apiHits, dataHits atomic.Int32
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		apiHits.Add(1)
		http.Error(w, "data reached API endpoint", http.StatusTeapot)
	}))
	defer api.Close()
	data := newDataTunnelServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		dataHits.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	route := routerTestRouteResolve(t, "sb-1", "/g", "rk", strings.TrimPrefix(data.URL, "http://"), types.ProfileBare)
	route.APIEndpoint = strings.TrimPrefix(api.URL, "http://")
	control := endpointRouteServer(t, route, nil)
	defer control.Close()
	rt := New(strings.TrimPrefix(control.URL, "http://"), "test.local", 0, nil, discardRouterLogger())
	rt.SetDataPlaneAuth("off")
	front := httptest.NewServer(rt.Handler())
	defer front.Close()
	req, _ := http.NewRequest(http.MethodGet, front.URL+"/health", nil)
	req.Host = "8080-sb-1.test.local"
	req.Header.Set(HeaderGroup, "/g")
	req.Header.Set(HeaderRouteKey, "rk")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent || apiHits.Load() != 0 || dataHits.Load() != 1 {
		t.Fatalf("ordinary HTTP status=%d api=%d data=%d", resp.StatusCode, apiHits.Load(), dataHits.Load())
	}
}

func TestOrdinaryCONNECTUsesOnlyDataEndpoint(t *testing.T) {
	var apiHits, dataHits atomic.Int32
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		apiHits.Add(1)
		http.Error(w, "CONNECT reached API endpoint", http.StatusTeapot)
	}))
	defer api.Close()
	data := newEchoConnectServer(t, &dataHits)
	defer data.Close()
	route := routerTestRouteResolve(t, "sb-1", "/g", "rk", strings.TrimPrefix(data.URL, "http://"), types.ProfileBare)
	route.APIEndpoint = strings.TrimPrefix(api.URL, "http://")
	control := endpointRouteServer(t, route, nil)
	defer control.Close()
	rt := New(strings.TrimPrefix(control.URL, "http://"), "test.local", 0, nil, discardRouterLogger())
	rt.SetDataPlaneAuth("off")
	front := httptest.NewServer(rt.Handler())
	defer front.Close()

	conn, err := net.Dial("tcp", front.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	_, _ = fmt.Fprintf(conn, "CONNECT 8080-sb-1.test.local:8080 HTTP/1.1\r\nHost: 8080-sb-1.test.local:8080\r\n%s: sb-1\r\n%s: forward\r\n%s: 8080\r\n%s: /g\r\n%s: rk\r\n\r\n",
		proxypkg.HeaderSandboxID, proxypkg.HeaderSandboxService, proxypkg.HeaderSandboxPort, HeaderGroup, HeaderRouteKey)
	reader := bufio.NewReader(conn)
	resp, err := http.ReadResponse(reader, &http.Request{Method: http.MethodConnect})
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		t.Fatalf("ordinary CONNECT status=%d body=%q", resp.StatusCode, body)
	}
	const payload = "through-data-endpoint"
	if _, err := io.WriteString(conn, payload); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(payload))
	if _, err := io.ReadFull(reader, got); err != nil {
		t.Fatal(err)
	}
	if string(got) != payload || apiHits.Load() != 0 || dataHits.Load() != 1 {
		t.Fatalf("ordinary CONNECT payload=%q api=%d data=%d", got, apiHits.Load(), dataHits.Load())
	}
}

func TestNativeExecUsesOnlyDataEndpoint(t *testing.T) {
	var apiHits atomic.Int32
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		apiHits.Add(1)
		http.Error(w, "exec reached API endpoint", http.StatusTeapot)
	}))
	defer api.Close()
	data, observed, backendInput := newExecTunnelNode(t, "", "exec-data")
	defer data.Close()
	route := routerTestRouteResolve(t, "stable", "/g", "rk", strings.TrimPrefix(data.URL, "http://"), types.ProfileBare)
	route.APIEndpoint = strings.TrimPrefix(api.URL, "http://")
	control := endpointRouteServer(t, route, nil)
	defer control.Close()
	token, err := keys.MintExecAccessToken(route.ServiceSecret, route.StableID, 0)
	if err != nil {
		t.Fatal(err)
	}
	rt := New(strings.TrimPrefix(control.URL, "http://"), "test.local", 0, nil, discardRouterLogger())
	front := httptest.NewServer(rt.Handler())
	defer front.Close()
	status, output := rawClusterExecConnect(t, front.Listener.Addr().String(), token, 0, validRouterExecFrame())
	if status != http.StatusOK || string(output) != "exec-data" || apiHits.Load() != 0 {
		t.Fatalf("native exec status=%d output=%q api=%d", status, output, apiHits.Load())
	}
	if got := <-observed; got.sid != route.NodeSandboxID || got.service != string(proxypkg.ConnectServiceExec) {
		t.Fatalf("native exec data request=%+v", got)
	}
	if got := <-backendInput; !bytes.Equal(got, validRouterExecFrame()) {
		t.Fatalf("native exec frame=%q", got)
	}
}

func TestMissingDataEndpointDoesNotUseAPIEndpoint(t *testing.T) {
	var apiHits atomic.Int32
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		apiHits.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer api.Close()
	route := routerTestRouteResolve(t, "sb-1", "/g", "rk", "", types.ProfileBare)
	route.APIEndpoint = strings.TrimPrefix(api.URL, "http://")
	control := endpointRouteServer(t, route, nil)
	defer control.Close()
	rt := New(strings.TrimPrefix(control.URL, "http://"), "test.local", 0, nil, discardRouterLogger())
	rt.SetDataPlaneAuth("off")
	front := httptest.NewServer(rt.Handler())
	defer front.Close()
	req, _ := http.NewRequest(http.MethodGet, front.URL+"/health", nil)
	req.Host = "8080-sb-1.test.local"
	req.Header.Set(HeaderGroup, "/g")
	req.Header.Set(HeaderRouteKey, "rk")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable || apiHits.Load() != 0 {
		t.Fatalf("missing data endpoint status=%d API hits=%d", resp.StatusCode, apiHits.Load())
	}
}

func endpointRouteServer(t *testing.T, route routeResolve, build map[string]any) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/route-link/verify-key":
			w.WriteHeader(http.StatusOK)
		case "/route-link/route":
			_ = json.NewEncoder(w).Encode(route)
		case "/route-link/reserve":
			_ = json.NewEncoder(w).Encode(reserveResult{Route: route})
		case "/route-link/build":
			if build == nil {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			_ = json.NewEncoder(w).Encode(build)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
}

func newEchoConnectServer(t *testing.T, hits *atomic.Int32) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodConnect {
			http.Error(w, "CONNECT required", http.StatusMethodNotAllowed)
			return
		}
		hits.Add(1)
		hijacker, ok := w.(http.Hijacker)
		if !ok {
			http.Error(w, "hijack unavailable", http.StatusInternalServerError)
			return
		}
		conn, rw, err := hijacker.Hijack()
		if err != nil {
			return
		}
		defer conn.Close()
		_, _ = rw.WriteString("HTTP/1.1 200 Connection Established\r\n\r\n")
		_ = rw.Flush()
		_, _ = io.Copy(conn, rw)
	}))
}
