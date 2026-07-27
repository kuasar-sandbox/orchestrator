package router

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/envdsign"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func newDataTunnelServer(t *testing.T, h http.HandlerFunc) *httptest.Server {
	t.Helper()
	return newObservedDataTunnelServer(t, nil, h)
}

func newObservedDataTunnelServer(t *testing.T, onConnect func(*http.Request), h http.HandlerFunc) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodConnect {
			h(w, r)
			return
		}
		if onConnect != nil {
			onConnect(r.Clone(r.Context()))
		}
		hj, ok := w.(http.Hijacker)
		if !ok {
			http.Error(w, "connect unsupported", http.StatusInternalServerError)
			return
		}
		conn, br, err := hj.Hijack()
		if err != nil {
			return
		}
		defer conn.Close()
		if _, err := io.WriteString(conn, "HTTP/1.1 200 Connection established\r\n\r\n"); err != nil {
			return
		}
		_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
		inner, err := http.ReadRequest(br.Reader)
		if err != nil {
			return
		}
		_ = conn.SetReadDeadline(time.Time{})
		rec := httptest.NewRecorder()
		h(rec, inner)
		resp := rec.Result()
		defer resp.Body.Close()
		_ = resp.Write(conn)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// TestAuthModeOff: with router.auth=off, caller auth is skipped (front with an
// front auth layer) — a bad key is NOT rejected.
func TestAuthModeOff(t *testing.T) {
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/route-link/reserve" {
			_ = json.NewEncoder(w).Encode(routerTestReserveResult(t, "sb-1", "10.0.0.1:1", types.ProfileE2B))
			return
		}
		w.WriteHeader(http.StatusForbidden) // verify-key would reject
	}))
	defer control.Close()
	rt := New(strings.TrimPrefix(control.URL, "http://"), "test.local", 0, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	rt.SetAuthMode("off")
	srv := httptest.NewServer(rt.Handler())
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/sandboxes", nil)
	req.Host = "api.test.local"
	req.Header.Set(HeaderGroup, "/g")
	req.Header.Set(HeaderAPIKey, "e2b_bad")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusForbidden {
		t.Fatal("auth=off should not reject a bad key")
	}
}

func TestCreateGeneratesRouteKeyWhenHeaderMissing(t *testing.T) {
	var routeKeys []string
	var results []reserveResult
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/route-link/reserve" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		rk := r.URL.Query().Get("route_key")
		routeKeys = append(routeKeys, rk)
		profile := types.ProfileE2B
		if len(routeKeys) == 2 {
			profile = types.ProfileBare
		}
		result := routerTestReserveResult(t, fmt.Sprintf("sb-%d", len(routeKeys)), "10.0.0.1:1", profile)
		results = append(results, result)
		_ = json.NewEncoder(w).Encode(result)
	}))
	defer control.Close()
	rt := New(strings.TrimPrefix(control.URL, "http://"), "test.local", 0, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	rt.SetAuthMode("off")
	srv := httptest.NewServer(rt.Handler())
	defer srv.Close()

	for i := 0; i < 2; i++ {
		req, _ := http.NewRequest(http.MethodPost, srv.URL+"/sandboxes", nil)
		req.Host = "api.test.local"
		req.Header.Set(HeaderGroup, "/g")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		var out map[string]string
		_ = json.NewDecoder(resp.Body).Decode(&out)
		resp.Body.Close()
		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("create %d status=%d, want 201", i, resp.StatusCode)
		}
		if out["routeKey"] == "" {
			t.Fatalf("create %d omitted generated routeKey in response", i)
		}
		if out["sandboxID"] != results[i].SID || out["clientID"] != results[i].NodeID ||
			out["forwardAccessToken"] != results[i].ForwardAccessToken || out["domain"] != "test.local" {
			t.Fatalf("create %d public response did not preserve public identifiers and forward token", i)
		}
		allowed := map[string]bool{
			"sandboxID": true, "routeKey": true, "clientID": true,
			"forwardAccessToken": true, "domain": true,
		}
		if results[i].Profile == string(types.ProfileE2B) {
			allowed["envdAccessToken"] = true
			allowed["trafficAccessToken"] = true
			if out["envdAccessToken"] != results[i].EnvdAccessToken ||
				out["trafficAccessToken"] != results[i].TrafficAccessToken {
				t.Fatalf("create %d omitted e2b tokens", i)
			}
		}
		if len(out) != len(allowed) {
			t.Fatalf("create %d response field count=%d, want %d", i, len(out), len(allowed))
		}
		for key := range out {
			if !allowed[key] {
				t.Fatalf("create %d exposed unexpected field %q", i, key)
			}
		}
	}
	if len(routeKeys) != 2 || routeKeys[0] == "" || routeKeys[1] == "" || routeKeys[0] == routeKeys[1] {
		t.Fatalf("generated route keys=%v, want two non-empty unique values", routeKeys)
	}
}

func TestServeDataSandboxHostUsesRouteResolve(t *testing.T) {
	var reserveHits int
	var routeHits int
	var gotHost string
	node := newDataTunnelServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHost = r.Host
		w.WriteHeader(http.StatusNoContent)
	}))
	nodeHost := strings.TrimPrefix(node.URL, "http://")
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/route-link/reserve":
			reserveHits++
			_ = json.NewEncoder(w).Encode(routerTestReserveResult(t, "wrong", nodeHost, types.ProfileE2B))
		case "/route-link/route":
			routeHits++
			_ = json.NewEncoder(w).Encode(routerTestRouteResolve(t, "sb-1", "/g", "rk", nodeHost, types.ProfileE2B))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer control.Close()
	rt := New(strings.TrimPrefix(control.URL, "http://"), "test.local", 0, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	rt.SetAuthMode("off")
	rt.SetDataPlaneAuth("off")
	srv := httptest.NewServer(rt.Handler())
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/health", nil)
	req.Host = "49983-sb-1.test.local"
	req.Header.Set(HeaderGroup, "/g")
	req.Header.Set(HeaderRouteKey, "rk")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("sid-host data status=%d, want 204", resp.StatusCode)
	}
	if routeHits != 1 || reserveHits != 0 {
		t.Fatalf("routeHits=%d reserveHits=%d, want route only", routeHits, reserveHits)
	}
	if gotHost != "49983-sb-1.test.local" {
		t.Fatalf("node saw Host=%q, want original sid host", gotHost)
	}
}

func TestServeDataSignedFileURLAuth(t *testing.T) {
	var nodeHits int32
	var outerTokens []string
	var innerTokens []string
	node := newObservedDataTunnelServer(t, func(r *http.Request) {
		outerTokens = append(outerTokens, r.Header.Get(HeaderAccessTok))
	}, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&nodeHits, 1)
		innerTokens = append(innerTokens, r.Header.Get(HeaderAccessTok))
		w.WriteHeader(http.StatusNoContent)
	}))
	nodeHost := strings.TrimPrefix(node.URL, "http://")
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/route-link/route" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_ = json.NewEncoder(w).Encode(routerTestRouteResolve(t, "sb-1", "/g", "rk", nodeHost, types.ProfileE2B))
	}))
	defer control.Close()

	rt := New(strings.TrimPrefix(control.URL, "http://"), "test.local", 0, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	rt.SetAuthMode("off")
	rt.SetDataPlaneAuth("enforce")
	srv := httptest.NewServer(rt.Handler())
	defer srv.Close()

	signedQuery := func(signature string) string {
		return "?path=" + url.QueryEscape("/tmp/a.txt") + "&signature=" + url.QueryEscape(signature)
	}
	sig := envdsign.Signature("/tmp/a.txt", "", envdsign.OperationRead, routerTestEnvdAccessToken, nil)
	do := func(host, path, query, token string) int {
		req, _ := http.NewRequest(http.MethodGet, srv.URL+path+query, nil)
		req.Host = host
		req.Header.Set(HeaderGroup, "/g")
		req.Header.Set(HeaderRouteKey, "rk")
		if token != "" {
			req.Header.Set(HeaderAccessTok, token)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}

	if code := do("49983-sb-1.test.local", "/files", signedQuery(sig), ""); code != http.StatusNoContent {
		t.Fatalf("valid signed file status=%d, want 204", code)
	}
	if got := outerTokens[len(outerTokens)-1]; got != routerTestEnvdAccessToken {
		t.Fatal("signed file outer CONNECT did not use EnvdAccessToken")
	}
	if got := innerTokens[len(innerTokens)-1]; got != "" {
		t.Fatalf("signed file inner HTTP X-Access-Token=%q, want empty", got)
	}

	if code := do("49983-sb-1.test.local", "/files", signedQuery("bad"), ""); code != http.StatusUnauthorized {
		t.Fatalf("bad signature status=%d, want 401", code)
	}
	if code := do("49983-sb-1.test.local", "/files", signedQuery(sig), "wrong"); code != http.StatusUnauthorized {
		t.Fatalf("wrong header with valid signature status=%d, want 401", code)
	}
	if code := do("8080-sb-1.test.local", "/files", signedQuery(sig), ""); code != http.StatusUnauthorized {
		t.Fatalf("signature on user port status=%d, want 401", code)
	}
	if got := atomic.LoadInt32(&nodeHits); got != 1 {
		t.Fatalf("node hits after rejected signed requests=%d, want 1", got)
	}

	if code := do("49983-sb-1.test.local", "/health", "", routerTestEnvdAccessToken); code != http.StatusNoContent {
		t.Fatalf("valid token status=%d, want 204", code)
	}
	if got := outerTokens[len(outerTokens)-1]; got != routerTestEnvdAccessToken {
		t.Fatal("ordinary request outer CONNECT changed the client token")
	}
	if got := innerTokens[len(innerTokens)-1]; got != routerTestEnvdAccessToken {
		t.Fatal("ordinary request inner HTTP changed the client token")
	}
}

func TestServeDataForwardTokenByProfile(t *testing.T) {
	for _, profile := range []types.Profile{types.ProfileE2B, types.ProfileBare} {
		t.Run(string(profile), func(t *testing.T) {
			var connectHits int32
			var innerHits int32
			var outerToken string
			var innerToken string
			node := newObservedDataTunnelServer(t, func(r *http.Request) {
				atomic.AddInt32(&connectHits, 1)
				outerToken = r.Header.Get(HeaderAccessTok)
			}, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				atomic.AddInt32(&innerHits, 1)
				innerToken = r.Header.Get(HeaderAccessTok)
				w.WriteHeader(http.StatusNoContent)
			}))
			nodeHost := strings.TrimPrefix(node.URL, "http://")
			route := routerTestRouteResolve(t, "sb-1", "/g", "rk", nodeHost, profile)
			control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/route-link/route" {
					w.WriteHeader(http.StatusNotFound)
					return
				}
				_ = json.NewEncoder(w).Encode(route)
			}))
			defer control.Close()

			rt := New(strings.TrimPrefix(control.URL, "http://"), "test.local", 0, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
			rt.SetAuthMode("off")
			rt.SetDataPlaneAuth("enforce")
			srv := httptest.NewServer(rt.Handler())
			defer srv.Close()

			do := func(token string) int {
				req, _ := http.NewRequest(http.MethodGet, srv.URL+"/health", nil)
				req.Host = "8080-sb-1.test.local"
				req.Header.Set(HeaderGroup, "/g")
				req.Header.Set(HeaderRouteKey, "rk")
				req.Header.Set(HeaderAccessTok, token)
				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					t.Fatal(err)
				}
				resp.Body.Close()
				return resp.StatusCode
			}

			if code := do(route.ForwardAccessToken); code != http.StatusNoContent {
				t.Fatalf("valid forward token status=%d, want 204", code)
			}
			if outerToken != route.ForwardAccessToken || innerToken != route.ForwardAccessToken {
				t.Fatalf("forward token changed across hops: outer=%q inner=%q", outerToken, innerToken)
			}
			if code := do("wrong"); code != http.StatusUnauthorized {
				t.Fatalf("wrong forward token status=%d, want 401", code)
			}
			if got := atomic.LoadInt32(&connectHits); got != 1 {
				t.Fatalf("CONNECT hits=%d after rejected token, want 1", got)
			}
			if got := atomic.LoadInt32(&innerHits); got != 1 {
				t.Fatalf("inner hits=%d after rejected token, want 1", got)
			}
		})
	}
}

func TestServeDataRejectsInvalidSignedFileBeforeReserve(t *testing.T) {
	var reserveHits int32
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/route-link/route":
			route := routerTestRouteResolve(t, "sb-1", "/g", "rk", "", types.ProfileE2B)
			route.State = "paused"
			_ = json.NewEncoder(w).Encode(route)
		case "/route-link/reserve":
			atomic.AddInt32(&reserveHits, 1)
			w.WriteHeader(http.StatusServiceUnavailable)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer control.Close()

	rt := New(strings.TrimPrefix(control.URL, "http://"), "test.local", 0, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	rt.SetAuthMode("off")
	rt.SetDataPlaneAuth("enforce")
	srv := httptest.NewServer(rt.Handler())
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/files?path=%2Ftmp%2Fa.txt&signature=bad", nil)
	req.Host = "49983-sb-1.test.local"
	req.Header.Set(HeaderGroup, "/g")
	req.Header.Set(HeaderRouteKey, "rk")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status=%d, want 401", resp.StatusCode)
	}
	if got := atomic.LoadInt32(&reserveHits); got != 0 {
		t.Fatalf("reserve hits=%d, want 0", got)
	}
}

func TestServeDataNonSandboxHostDoesNotReserve(t *testing.T) {
	var reserveHits int
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/route-link/reserve" {
			reserveHits++
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer control.Close()
	rt := New(strings.TrimPrefix(control.URL, "http://"), "test.local", 0, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	srv := httptest.NewServer(rt.Handler())
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/health", nil)
	req.Host = "data.test.local"
	req.Header.Set(HeaderGroup, "/g")
	req.Header.Set(HeaderRouteKey, "u1:s1")
	req.Header.Set(HeaderAPIKey, "e2b_ok")
	req.Header.Set("E2b-Sandbox-Port", "49983")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("non-sandbox data host status=%d, want 400", resp.StatusCode)
	}
	if reserveHits != 0 {
		t.Fatalf("non-sandbox data host triggered %d Reserve calls, want 0", reserveHits)
	}
}

func TestServeDataUnknownSandboxReturnsNotFoundWithoutReserve(t *testing.T) {
	var routeHits int32
	var reserveHits int32
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/route-link/route":
			atomic.AddInt32(&routeHits, 1)
			http.Error(w, "sandbox not found", http.StatusNotFound)
		case "/route-link/reserve":
			atomic.AddInt32(&reserveHits, 1)
			w.WriteHeader(http.StatusServiceUnavailable)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer control.Close()

	rt := New(strings.TrimPrefix(control.URL, "http://"), "test.local", 0, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	rt.SetAuthMode("off")
	srv := httptest.NewServer(rt.Handler())
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/health", nil)
	req.Host = "8080-missing.test.local"
	req.Header.Set(HeaderGroup, "/g")
	req.Header.Set(HeaderRouteKey, "rk")
	req.Header.Set(HeaderAccessTok, "unused")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status=%d, want 404", resp.StatusCode)
	}
	if got := atomic.LoadInt32(&routeHits); got != 1 {
		t.Fatalf("route hits=%d, want 1", got)
	}
	if got := atomic.LoadInt32(&reserveHits); got != 0 {
		t.Fatalf("reserve hits=%d, want 0", got)
	}
}

func TestSandboxVerbRejectsRouteFromDifferentGroup(t *testing.T) {
	var nodeHits int32
	node := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&nodeHits, 1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer node.Close()
	nodeHost := strings.TrimPrefix(node.URL, "http://")
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/route-link/route" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_ = json.NewEncoder(w).Encode(routerTestRouteResolve(t, "sb-1", "/other", "rk", nodeHost, types.ProfileE2B))
	}))
	defer control.Close()
	rt := New(strings.TrimPrefix(control.URL, "http://"), "test.local", 0, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	rt.SetAuthMode("off")
	srv := httptest.NewServer(rt.Handler())
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodDelete, srv.URL+"/sandboxes/sb-1", nil)
	req.Host = "api.test.local"
	req.Header.Set(HeaderGroup, "/g")
	req.Header.Set(HeaderRouteKey, "rk")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status=%d, want 404", resp.StatusCode)
	}
	if got := atomic.LoadInt32(&nodeHits); got != 0 {
		t.Fatalf("node hits=%d, want 0", got)
	}
}
