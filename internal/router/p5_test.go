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
	"github.com/kuasar-sandbox/orchestrator/internal/migrationtoken"
	proxypkg "github.com/kuasar-sandbox/orchestrator/internal/proxy"
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

func TestListAlwaysEnforcesAPIKey(t *testing.T) {
	var verifyHits, listHits int
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/route-link/verify-key":
			verifyHits++
			w.WriteHeader(http.StatusForbidden)
		case "/route-link/list":
			listHits++
			_, _ = io.WriteString(w, "[]")
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer control.Close()
	rt := New(strings.TrimPrefix(control.URL, "http://"), "test.local", 0, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	srv := httptest.NewServer(rt.Handler())
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/sandboxes", nil)
	req.Host = "api.test.local"
	req.Header.Set(HeaderGroup, "/g")
	req.Header.Set(HeaderAPIKey, "e2b_bad")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden || verifyHits != 1 || listHits != 0 {
		t.Fatalf("list status=%d verify=%d list=%d, want 403/1/0", resp.StatusCode, verifyHits, listHits)
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
		if r.URL.Query().Get("operation") != "create" || r.Header.Get(HeaderAPIKey) != "api-key" {
			t.Fatalf("create reserve query=%q api-key=%q", r.URL.RawQuery, r.Header.Get(HeaderAPIKey))
		}
		rk := r.URL.Query().Get("route_key")
		routeKeys = append(routeKeys, rk)
		profile := types.ProfileE2B
		if len(routeKeys) == 2 {
			profile = types.ProfileBare
		}
		result := routerTestReserveResult(t, fmt.Sprintf("sb-%d", len(routeKeys)), "/g", rk, "10.0.0.1:1", profile)
		results = append(results, result)
		_ = json.NewEncoder(w).Encode(result)
	}))
	defer control.Close()
	rt := New(strings.TrimPrefix(control.URL, "http://"), "test.local", 0, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	srv := httptest.NewServer(rt.Handler())
	defer srv.Close()

	for i := 0; i < 2; i++ {
		req, _ := http.NewRequest(http.MethodPost, srv.URL+"/sandboxes", nil)
		req.Host = "api.test.local"
		req.Header.Set(HeaderGroup, "/g")
		req.Header.Set(HeaderAPIKey, "api-key")
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
		if out["sandboxID"] != results[i].Route.SandboxID || out["clientID"] != results[i].Route.NodeID ||
			out["forwardAccessToken"] != results[i].Route.ForwardAccessToken || out["domain"] != "test.local" {
			t.Fatalf("create %d public response did not preserve public identifiers and forward token", i)
		}
		allowed := map[string]bool{
			"sandboxID": true, "routeKey": true, "clientID": true,
			"forwardAccessToken": true, "domain": true,
		}
		if results[i].Route.Profile == string(types.ProfileE2B) {
			allowed["envdAccessToken"] = true
			allowed["trafficAccessToken"] = true
			if out["envdAccessToken"] != results[i].Route.EnvdAccessToken ||
				out["trafficAccessToken"] != results[i].Route.TrafficAccessToken {
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

func TestConnectUsesOperationAwareReserveAndShapesStableResponse(t *testing.T) {
	for _, tc := range []struct {
		name    string
		path    string
		profile types.Profile
	}{
		{name: "e2b", path: "/sandboxes/sb-1/connect", profile: types.ProfileE2B},
		{name: "bare v2", path: "/v2/sandboxes/sb-1/connect", profile: types.ProfileBare},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var reserveHits, routeHits, nodeHits int
			node := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				nodeHits++
				w.WriteHeader(http.StatusInternalServerError)
			}))
			defer node.Close()
			nodeHost := strings.TrimPrefix(node.URL, "http://")
			control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/route-link/reserve":
					reserveHits++
					query := r.URL.Query()
					if query.Get("operation") != "connect" || query.Get("group") != "/g" ||
						query.Get("route_key") != "rk" || query.Get("sid") != "sb-1" || query.Get("timeout") != "37" {
						t.Fatalf("connect reserve query=%q", r.URL.RawQuery)
					}
					if r.Header.Get(HeaderAPIKey) != "api-key" || r.Header.Get(HeaderMigration) != "kmt1.token" {
						t.Fatalf("connect reserve credentials api=%q migration=%q", r.Header.Get(HeaderAPIKey), r.Header.Get(HeaderMigration))
					}
					if body, err := io.ReadAll(r.Body); err != nil || len(body) != 0 {
						t.Fatalf("connect reserve body=%q err=%v, want empty", body, err)
					}
					_ = json.NewEncoder(w).Encode(routerTestConnectReserveResult(t, "sb-1", "/g", "rk", nodeHost, tc.profile))
				case "/route-link/route":
					routeHits++
					w.WriteHeader(http.StatusInternalServerError)
				case "/route-link/verify-key":
					t.Fatal("connect performed a separate verify-key call")
				default:
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			defer control.Close()

			rt := New(strings.TrimPrefix(control.URL, "http://"), "test.local", 0, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
			srv := httptest.NewServer(rt.Handler())
			defer srv.Close()

			req, _ := http.NewRequest(http.MethodPost, srv.URL+tc.path, strings.NewReader(`{"timeout":37}`))
			req.Host = "api.test.local"
			req.Header.Set(HeaderGroup, "/g")
			req.Header.Set(HeaderRouteKey, "rk")
			req.Header.Set(HeaderAPIKey, "api-key")
			req.Header.Set(HeaderMigration, "kmt1.token")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			var out map[string]string
			if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()
			if resp.StatusCode != http.StatusOK || reserveHits != 1 || routeHits != 0 || nodeHits != 0 {
				t.Fatalf("connect status=%d reserve=%d route=%d node=%d", resp.StatusCode, reserveHits, routeHits, nodeHits)
			}
			if out["sandboxID"] != "sb-1" || out["templateID"] != string(tc.profile)+"-img-template" ||
				out["clientID"] != "orchestrator" || out["domain"] != "test.local" || out["alias"] != "" ||
				out["forwardAccessToken"] == "" {
				t.Fatalf("connect public response=%v", out)
			}
			wantFields := 7
			if tc.profile == types.ProfileE2B {
				wantFields = 9
				if out["envdVersion"] != "0.6.1" || out["envdAccessToken"] != routerTestEnvdAccessToken ||
					out["trafficAccessToken"] != routerTestTrafficAccessToken {
					t.Fatalf("e2b connect response=%v", out)
				}
			} else {
				if out["envdVersion"] != "0.1.0" {
					t.Fatalf("bare connect envdVersion=%q", out["envdVersion"])
				}
				if _, found := out["envdAccessToken"]; found {
					t.Fatalf("bare connect exposed envd token: %v", out)
				}
				if _, found := out["trafficAccessToken"]; found {
					t.Fatalf("bare connect exposed traffic token: %v", out)
				}
			}
			if len(out) != wantFields {
				t.Fatalf("connect response fields=%v, want %d fields", out, wantFields)
			}
			for _, forbidden := range []string{"nodeSandboxID", "routeKey", "apiSecret", "serviceSecret", "apiSecretFingerprint", "manifestKeyFingerprint"} {
				if _, found := out[forbidden]; found {
					t.Fatalf("connect exposed %s: %v", forbidden, out)
				}
			}
		})
	}
}

func TestConnectRejectsInconsistentReserveResult(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*reserveResult)
	}{
		{name: "missing connect", mutate: func(result *reserveResult) { result.Connect = nil }},
		{name: "node sandbox", mutate: func(result *reserveResult) { result.Connect.NodeSandboxID = "sb-1-g9" }},
		{name: "template", mutate: func(result *reserveResult) { result.Connect.TemplateID = "different" }},
		{name: "profile", mutate: func(result *reserveResult) { result.Connect.Profile = string(types.ProfileBare) }},
		{name: "envd token", mutate: func(result *reserveResult) { result.Connect.EnvdAccessToken = "different" }},
		{name: "traffic token", mutate: func(result *reserveResult) { result.Connect.TrafficAccessToken = "different" }},
		{name: "forward token", mutate: func(result *reserveResult) { result.Connect.ForwardAccessToken = "different" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/route-link/reserve" {
					w.WriteHeader(http.StatusNotFound)
					return
				}
				result := routerTestConnectReserveResult(t, "sb-1", "/g", "rk", "node.invalid:1", types.ProfileE2B)
				tc.mutate(&result)
				_ = json.NewEncoder(w).Encode(result)
			}))
			defer control.Close()
			rt := New(strings.TrimPrefix(control.URL, "http://"), "test.local", 0, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
			srv := httptest.NewServer(rt.Handler())
			defer srv.Close()

			req, _ := http.NewRequest(http.MethodPost, srv.URL+"/sandboxes/sb-1/connect", strings.NewReader(`{}`))
			req.Host = "api.test.local"
			req.Header.Set(HeaderGroup, "/g")
			req.Header.Set(HeaderRouteKey, "rk")
			req.Header.Set(HeaderAPIKey, "api-key")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()
			if resp.StatusCode != http.StatusBadGateway {
				t.Fatalf("inconsistent connect status=%d, want 502", resp.StatusCode)
			}
			if rt.cachedRoute("/g", "rk", "sb-1") != nil {
				t.Fatal("inconsistent connect result entered route cache")
			}
		})
	}
}

func TestConnectRejectsOversizedMigrationTokenBeforeReserve(t *testing.T) {
	var reserveHits int
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/route-link/reserve" {
			reserveHits++
		}
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer control.Close()
	rt := New(strings.TrimPrefix(control.URL, "http://"), "test.local", 0, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	srv := httptest.NewServer(rt.Handler())
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/sandboxes/sb-1/connect", nil)
	req.Host = "api.test.local"
	req.Header.Set(HeaderGroup, "/g")
	req.Header.Set(HeaderRouteKey, "rk")
	req.Header.Set(HeaderAPIKey, "api-key")
	req.Header.Set(HeaderMigration, strings.Repeat("x", migrationtoken.MaxWireSize+1))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusRequestHeaderFieldsTooLarge || reserveHits != 0 {
		t.Fatalf("oversized migration status=%d reserveHits=%d", resp.StatusCode, reserveHits)
	}
}

func TestConnectBoundsAndValidatesRequestBody(t *testing.T) {
	for _, tc := range []struct {
		name          string
		body          func() io.Reader
		contentLength int64
		wantStatus    int
		wantReserve   int
	}{
		{
			name:          "exact limit",
			body:          func() io.Reader { return sizedJSONBody(maxClusterConnectBodyBytes) },
			contentLength: -1,
			wantStatus:    http.StatusOK,
			wantReserve:   1,
		},
		{
			name:          "chunked over limit",
			body:          func() io.Reader { return sizedJSONBody(maxClusterConnectBodyBytes + 1) },
			contentLength: -1,
			wantStatus:    http.StatusRequestEntityTooLarge,
		},
		{
			name:          "declared over limit",
			body:          func() io.Reader { return strings.NewReader(`{}`) },
			contentLength: maxClusterConnectBodyBytes + 1,
			wantStatus:    http.StatusRequestEntityTooLarge,
		},
		{
			name:          "malformed",
			body:          func() io.Reader { return strings.NewReader(`{"timeout":`) },
			contentLength: -1,
			wantStatus:    http.StatusBadRequest,
		},
		{
			name:          "trailing value",
			body:          func() io.Reader { return strings.NewReader(`{} {}`) },
			contentLength: -1,
			wantStatus:    http.StatusBadRequest,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var reserveHits int
			control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/route-link/reserve" {
					w.WriteHeader(http.StatusNotFound)
					return
				}
				reserveHits++
				_ = json.NewEncoder(w).Encode(routerTestConnectReserveResult(t, "sb-1", "/g", "rk", "node.invalid:1", types.ProfileBare))
			}))
			defer control.Close()
			rt := New(strings.TrimPrefix(control.URL, "http://"), "test.local", 0, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))

			req := httptest.NewRequest(http.MethodPost, "/sandboxes/sb-1/connect", nil)
			req.Body = io.NopCloser(tc.body())
			req.ContentLength = tc.contentLength
			req.Header.Set(HeaderGroup, "/g")
			req.Header.Set(HeaderRouteKey, "rk")
			req.Header.Set(HeaderAPIKey, "api-key")
			rec := httptest.NewRecorder()

			rt.handleConnect(rec, req)

			if rec.Code != tc.wantStatus || reserveHits != tc.wantReserve {
				t.Fatalf("connect status=%d reserveHits=%d, want %d/%d; body=%q", rec.Code, reserveHits, tc.wantStatus, tc.wantReserve, rec.Body.String())
			}
		})
	}
}

func TestConnectPropagatesRouteLinkMigrationRejection(t *testing.T) {
	var reserveHits int
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/route-link/reserve" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		reserveHits++
		http.Error(w, "migration credential not allowed", http.StatusForbidden)
	}))
	defer control.Close()
	rt := New(strings.TrimPrefix(control.URL, "http://"), "test.local", 0, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	srv := httptest.NewServer(rt.Handler())
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/sandboxes/sb-1/connect", nil)
	req.Host = "api.test.local"
	req.Header.Set(HeaderGroup, "/g")
	req.Header.Set(HeaderRouteKey, "rk")
	req.Header.Set(HeaderAPIKey, "api-key")
	req.Header.Set(HeaderMigration, "kmt1.invalid")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusForbidden || strings.TrimSpace(string(body)) != "migration credential not allowed" || reserveHits != 1 {
		t.Fatalf("status=%d body=%q reserveHits=%d", resp.StatusCode, body, reserveHits)
	}
}

func TestServeDataSandboxHostUsesRouteResolve(t *testing.T) {
	var reserveHits int
	var routeHits int
	var outerSandboxID, innerSandboxID, innerHost, innerMarker string
	node := newObservedDataTunnelServer(t, func(r *http.Request) {
		outerSandboxID = r.Header.Get(proxypkg.HeaderSandboxID)
	}, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		innerSandboxID = r.Header.Get(proxypkg.HeaderSandboxID)
		innerHost = r.Host
		innerMarker = r.Header.Get("X-Test-Marker")
		w.WriteHeader(http.StatusNoContent)
	}))
	nodeHost := strings.TrimPrefix(node.URL, "http://")
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/route-link/reserve":
			reserveHits++
			_ = json.NewEncoder(w).Encode(routerTestReserveResult(t, "wrong", "/g", "rk", nodeHost, types.ProfileE2B))
		case "/route-link/route":
			routeHits++
			_ = json.NewEncoder(w).Encode(routerTestRouteResolve(t, "sb-1", "/g", "rk", nodeHost, types.ProfileE2B))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer control.Close()
	rt := New(strings.TrimPrefix(control.URL, "http://"), "test.local", 0, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	rt.SetDataPlaneAuth("off")
	srv := httptest.NewServer(rt.Handler())
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/health", nil)
	req.Host = "49983-sb-1.test.local"
	req.Header.Set(HeaderGroup, "/g")
	req.Header.Set(HeaderRouteKey, "rk")
	req.Header.Set(proxypkg.HeaderSandboxID, "sb-1")
	req.Header.Set("X-Test-Marker", "unchanged")
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
	if outerSandboxID != "sb-1-g0" {
		t.Fatalf("outer CONNECT sandbox ID=%q, want node identity", outerSandboxID)
	}
	if innerHost != "49983-sb-1-g0.test.local" || innerSandboxID != "sb-1-g0" {
		t.Fatalf("inner identity Host=%q header=%q, want node identity", innerHost, innerSandboxID)
	}
	if innerMarker != "unchanged" {
		t.Fatalf("ordinary end-to-end header=%q, want unchanged", innerMarker)
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

func TestServeDataSignedFileExplicitBadTokenNeverFallsBackWhenAuthOff(t *testing.T) {
	var reserveHits, nodeHits int32
	node := newDataTunnelServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&nodeHits, 1)
		w.WriteHeader(http.StatusNoContent)
	}))
	nodeHost := strings.TrimPrefix(node.URL, "http://")
	paused := routerTestRouteResolve(t, "sb-1", "/g", "rk", "", types.ProfileE2B)
	paused.State = "paused"
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/route-link/route":
			_ = json.NewEncoder(w).Encode(paused)
		case "/route-link/reserve":
			atomic.AddInt32(&reserveHits, 1)
			_ = json.NewEncoder(w).Encode(routerTestReserveResult(t, "sb-1", "/g", "rk", nodeHost, types.ProfileE2B))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer control.Close()

	rt := New(strings.TrimPrefix(control.URL, "http://"), "test.local", 0, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	rt.SetDataPlaneAuth("off")
	srv := httptest.NewServer(rt.Handler())
	defer srv.Close()

	sig := envdsign.Signature("/tmp/a.txt", "", envdsign.OperationRead, paused.EnvdAccessToken, nil)
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/files?path=%2Ftmp%2Fa.txt&signature="+url.QueryEscape(sig), nil)
	req.Host = "49983-sb-1.test.local"
	req.Header.Set(HeaderGroup, "/g")
	req.Header.Set(HeaderRouteKey, "rk")
	req.Header.Set(HeaderAccessTok, "wrong")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("signed file with explicit bad token status=%d, want 401", resp.StatusCode)
	}
	if hits := atomic.LoadInt32(&reserveHits); hits != 0 {
		t.Fatalf("signed file bad token reached Reserve %d times", hits)
	}
	if hits := atomic.LoadInt32(&nodeHits); hits != 0 {
		t.Fatalf("signed file bad token reached node %d times", hits)
	}
}

func TestServeDataPausedUsesOperationAwareReserve(t *testing.T) {
	var reserveHits int32
	var outerSandboxID, outerToken, innerSandboxID, innerToken string
	node := newObservedDataTunnelServer(t, func(r *http.Request) {
		outerSandboxID = r.Header.Get(proxypkg.HeaderSandboxID)
		outerToken = r.Header.Get(HeaderAccessTok)
	}, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		innerSandboxID = r.Header.Get(proxypkg.HeaderSandboxID)
		innerToken = r.Header.Get(HeaderAccessTok)
		w.WriteHeader(http.StatusNoContent)
	}))
	nodeHost := strings.TrimPrefix(node.URL, "http://")
	paused := routerTestRouteResolve(t, "sb-1", "/g", "rk", "", types.ProfileBare)
	paused.State = "paused"
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/route-link/route":
			_ = json.NewEncoder(w).Encode(paused)
		case "/route-link/reserve":
			atomic.AddInt32(&reserveHits, 1)
			query := r.URL.Query()
			if query.Get("operation") != "data" || query.Get("group") != "/g" ||
				query.Get("route_key") != "rk" || query.Get("sid") != "sb-1" || query.Get("port") != "8080" {
				t.Fatalf("data reserve query=%q", r.URL.RawQuery)
			}
			if r.Header.Get(HeaderAccessTok) != paused.ForwardAccessToken {
				t.Fatalf("data reserve access token=%q", r.Header.Get(HeaderAccessTok))
			}
			if body, err := io.ReadAll(r.Body); err != nil || len(body) != 0 {
				t.Fatalf("data reserve body=%q err=%v, want empty", body, err)
			}
			result := routerTestReserveResult(t, "sb-1", "/g", "rk", nodeHost, types.ProfileBare)
			result.Route.NodeSandboxID = "sb-1-g1"
			result.Route.RouteRevision = 2
			_ = json.NewEncoder(w).Encode(result)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer control.Close()

	rt := New(strings.TrimPrefix(control.URL, "http://"), "test.local", 0, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	rt.SetDataPlaneAuth("enforce")
	srv := httptest.NewServer(rt.Handler())
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/health", nil)
	req.Host = "8080-sb-1.test.local"
	req.Header.Set(HeaderGroup, "/g")
	req.Header.Set(HeaderRouteKey, "rk")
	req.Header.Set(HeaderAccessTok, paused.ForwardAccessToken)
	req.Header.Set(proxypkg.HeaderSandboxID, "sb-1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent || atomic.LoadInt32(&reserveHits) != 1 {
		t.Fatalf("paused data status=%d reserveHits=%d", resp.StatusCode, reserveHits)
	}
	if outerSandboxID != "sb-1-g1" || innerSandboxID != "sb-1-g1" {
		t.Fatalf("paused data identity outer=%q inner=%q", outerSandboxID, innerSandboxID)
	}
	if outerToken != paused.ForwardAccessToken || innerToken != paused.ForwardAccessToken {
		t.Fatalf("paused data token outer=%q inner=%q", outerToken, innerToken)
	}
}

func TestServeDataPausedSignedFileUsesEnvdTokenOnlyOutside(t *testing.T) {
	var reserveToken, outerToken, innerToken string
	node := newObservedDataTunnelServer(t, func(r *http.Request) {
		outerToken = r.Header.Get(HeaderAccessTok)
	}, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		innerToken = r.Header.Get(HeaderAccessTok)
		w.WriteHeader(http.StatusNoContent)
	}))
	nodeHost := strings.TrimPrefix(node.URL, "http://")
	paused := routerTestRouteResolve(t, "sb-1", "/g", "rk", "", types.ProfileE2B)
	paused.State = "paused"
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/route-link/route":
			_ = json.NewEncoder(w).Encode(paused)
		case "/route-link/reserve":
			if r.URL.Query().Get("operation") != "data" || r.URL.Query().Get("port") != "49983" {
				t.Fatalf("signed data reserve query=%q", r.URL.RawQuery)
			}
			reserveToken = r.Header.Get(HeaderAccessTok)
			result := routerTestReserveResult(t, "sb-1", "/g", "rk", nodeHost, types.ProfileE2B)
			result.Route.NodeSandboxID = "sb-1-g1"
			result.Route.RouteRevision = 2
			_ = json.NewEncoder(w).Encode(result)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer control.Close()

	rt := New(strings.TrimPrefix(control.URL, "http://"), "test.local", 0, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	rt.SetDataPlaneAuth("enforce")
	srv := httptest.NewServer(rt.Handler())
	defer srv.Close()

	sig := envdsign.Signature("/tmp/a.txt", "", envdsign.OperationRead, paused.EnvdAccessToken, nil)
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/files?path=%2Ftmp%2Fa.txt&signature="+url.QueryEscape(sig), nil)
	req.Host = "49983-sb-1.test.local"
	req.Header.Set(HeaderGroup, "/g")
	req.Header.Set(HeaderRouteKey, "rk")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("paused signed file status=%d", resp.StatusCode)
	}
	if reserveToken != paused.EnvdAccessToken || outerToken != paused.EnvdAccessToken {
		t.Fatalf("signed file protected token reserve=%q outer=%q", reserveToken, outerToken)
	}
	if innerToken != "" {
		t.Fatalf("signed file inner token=%q, want empty", innerToken)
	}
}

func TestServeDataForwardsKnownTargetReturnedByFallbackReserve(t *testing.T) {
	var nodeHits int32
	node := newDataTunnelServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&nodeHits, 1)
		w.WriteHeader(http.StatusNoContent)
	}))
	nodeHost := strings.TrimPrefix(node.URL, "http://")
	paused := routerTestRouteResolve(t, "sb-1", "/g", "rk", "", types.ProfileBare)
	paused.State = "paused"
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/route-link/route":
			_ = json.NewEncoder(w).Encode(paused)
		case "/route-link/reserve":
			result := routerTestReserveResult(t, "sb-1", "/g", "rk", nodeHost, types.ProfileBare)
			result.Route.State = "paused"
			_ = json.NewEncoder(w).Encode(result)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer control.Close()

	rt := New(strings.TrimPrefix(control.URL, "http://"), "test.local", 0, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	rt.SetDataPlaneAuth("enforce")
	srv := httptest.NewServer(rt.Handler())
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/health", nil)
	req.Host = "8080-sb-1.test.local"
	req.Header.Set(HeaderGroup, "/g")
	req.Header.Set(HeaderRouteKey, "rk")
	req.Header.Set(HeaderAccessTok, paused.ForwardAccessToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("non-ready reserve status=%d, want 204", resp.StatusCode)
	}
	if hits := atomic.LoadInt32(&nodeHits); hits != 1 {
		t.Fatalf("known target returned by reserve reached node %d times, want 1", hits)
	}
}

func TestServeDataKnownNonReadyTargetSkipsReserve(t *testing.T) {
	for _, state := range []string{"paused", "starting"} {
		t.Run(state, func(t *testing.T) {
			var reserveHits, nodeHits atomic.Int32
			node := newDataTunnelServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				nodeHits.Add(1)
				w.WriteHeader(http.StatusNoContent)
			}))
			nodeHost := strings.TrimPrefix(node.URL, "http://")
			route := routerTestRouteResolve(t, "sb-1", "/g", "rk", nodeHost, types.ProfileBare)
			route.State = state
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

			rt := New(strings.TrimPrefix(control.URL, "http://"), "test.local", 0, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
			rt.SetDataPlaneAuth("enforce")
			srv := httptest.NewServer(rt.Handler())
			defer srv.Close()

			req, _ := http.NewRequest(http.MethodGet, srv.URL+"/health", nil)
			req.Host = "8080-sb-1.test.local"
			req.Header.Set(HeaderGroup, "/g")
			req.Header.Set(HeaderRouteKey, "rk")
			req.Header.Set(HeaderAccessTok, route.ForwardAccessToken)
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()
			if resp.StatusCode != http.StatusNoContent {
				t.Fatalf("status=%d, want 204", resp.StatusCode)
			}
			if reserveHits.Load() != 0 || nodeHits.Load() != 1 {
				t.Fatalf("reserve hits=%d node hits=%d, want 0/1", reserveHits.Load(), nodeHits.Load())
			}
		})
	}
}

func TestServeDataTypedStaleTargetRefreshesThroughReserve(t *testing.T) {
	var staleHits, reserveHits atomic.Int32
	stale := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		staleHits.Add(1)
		w.Header().Set(proxypkg.HeaderProxyError, proxypkg.ProxyErrorNotFound)
		http.Error(w, "stale node-local sandbox", http.StatusNotFound)
	}))
	defer stale.Close()
	var innerSID string
	fresh := newDataTunnelServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		innerSID = r.Header.Get(proxypkg.HeaderSandboxID)
		w.WriteHeader(http.StatusNoContent)
	}))
	staleRoute := routerTestRouteResolve(t, "sb-1", "/g", "rk", strings.TrimPrefix(stale.URL, "http://"), types.ProfileBare)
	staleRoute.State = "paused"
	freshRoute := routerTestRouteResolve(t, "sb-1", "/g", "rk", strings.TrimPrefix(fresh.URL, "http://"), types.ProfileBare)
	freshRoute.NodeSandboxID = "sb-1-g1"
	freshRoute.RouteRevision = 2
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/route-link/route":
			_ = json.NewEncoder(w).Encode(staleRoute)
		case "/route-link/reserve":
			reserveHits.Add(1)
			if r.URL.Query().Get("operation") != "data" || r.URL.Query().Get("port") != "8080" {
				t.Fatalf("reserve query=%q", r.URL.RawQuery)
			}
			_ = json.NewEncoder(w).Encode(reserveResult{Route: freshRoute})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer control.Close()

	rt := New(strings.TrimPrefix(control.URL, "http://"), "test.local", 0, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	rt.SetDataPlaneAuth("enforce")
	srv := httptest.NewServer(rt.Handler())
	defer srv.Close()
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/health", nil)
	req.Host = "8080-sb-1.test.local"
	req.Header.Set(HeaderGroup, "/g")
	req.Header.Set(HeaderRouteKey, "rk")
	req.Header.Set(HeaderAccessTok, staleRoute.ForwardAccessToken)
	req.Header.Set(proxypkg.HeaderSandboxID, "sb-1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("status=%d, want 204", resp.StatusCode)
	}
	if staleHits.Load() != 1 || reserveHits.Load() != 1 || innerSID != freshRoute.NodeSandboxID {
		t.Fatalf("stale hits=%d reserve hits=%d inner sid=%q", staleHits.Load(), reserveHits.Load(), innerSID)
	}
}

func TestServeDataRejectsReserveIdentityChange(t *testing.T) {
	var reserveHits int32
	var nodeHits int32
	node := newDataTunnelServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&nodeHits, 1)
		w.WriteHeader(http.StatusNoContent)
	}))
	nodeHost := strings.TrimPrefix(node.URL, "http://")
	paused := routerTestRouteResolve(t, "sb-1", "/g", "rk", "", types.ProfileBare)
	paused.State = "paused"
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/route-link/route":
			_ = json.NewEncoder(w).Encode(paused)
		case "/route-link/reserve":
			atomic.AddInt32(&reserveHits, 1)
			_ = json.NewEncoder(w).Encode(routerTestReserveResult(t, "sb-recreated", "/g", "rk", nodeHost, types.ProfileBare))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer control.Close()

	rt := New(strings.TrimPrefix(control.URL, "http://"), "test.local", 0, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	rt.SetDataPlaneAuth("enforce")
	srv := httptest.NewServer(rt.Handler())
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/health", nil)
	req.Host = "8080-sb-1.test.local"
	req.Header.Set(HeaderGroup, "/g")
	req.Header.Set(HeaderRouteKey, "rk")
	req.Header.Set(HeaderAccessTok, paused.ForwardAccessToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status=%d, want 503", resp.StatusCode)
	}
	if got := atomic.LoadInt32(&reserveHits); got != 1 {
		t.Fatalf("reserve hits=%d, want 1", got)
	}
	if got := atomic.LoadInt32(&nodeHits); got != 0 {
		t.Fatalf("identity-changing reserve reached node %d times", got)
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
		if r.URL.Path == "/route-link/verify-key" {
			w.WriteHeader(http.StatusOK)
			return
		}
		if r.URL.Path != "/route-link/route" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_ = json.NewEncoder(w).Encode(routerTestRouteResolve(t, "sb-1", "/other", "rk", nodeHost, types.ProfileE2B))
	}))
	defer control.Close()
	rt := New(strings.TrimPrefix(control.URL, "http://"), "test.local", 0, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	srv := httptest.NewServer(rt.Handler())
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodDelete, srv.URL+"/sandboxes/sb-1", nil)
	req.Host = "api.test.local"
	req.Header.Set(HeaderGroup, "/g")
	req.Header.Set(HeaderRouteKey, "rk")
	req.Header.Set(HeaderAPIKey, "e2b_test")
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
