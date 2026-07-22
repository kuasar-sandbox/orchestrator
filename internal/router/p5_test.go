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
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
)

func newDataTunnelServer(t *testing.T, h http.HandlerFunc) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodConnect {
			h(w, r)
			return
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
			_ = json.NewEncoder(w).Encode(reserveResult{NodeID: "n1", SID: "sb-1", AccessToken: "t", DataEndpoint: "10.0.0.1:1"})
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
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/route-link/reserve" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		rk := r.URL.Query().Get("route_key")
		routeKeys = append(routeKeys, rk)
		_ = json.NewEncoder(w).Encode(reserveResult{
			NodeID: "n1", SID: fmt.Sprintf("sb-%d", len(routeKeys)), AccessToken: "envd-tok",
			TrafficAccessToken: "traffic-tok", DataEndpoint: "10.0.0.1:1",
		})
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
		var out struct {
			RouteKey           string `json:"routeKey"`
			EnvdAccessToken    string `json:"envdAccessToken"`
			TrafficAccessToken string `json:"trafficAccessToken"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&out)
		resp.Body.Close()
		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("create %d status=%d, want 201", i, resp.StatusCode)
		}
		if out.RouteKey == "" {
			t.Fatalf("create %d omitted generated routeKey in response", i)
		}
		if out.EnvdAccessToken != "envd-tok" || out.TrafficAccessToken != "traffic-tok" {
			t.Fatalf("create %d tokens envd=%q traffic=%q", i, out.EnvdAccessToken, out.TrafficAccessToken)
		}
	}
	if len(routeKeys) != 2 || routeKeys[0] == "" || routeKeys[1] == "" || routeKeys[0] == routeKeys[1] {
		t.Fatalf("generated route keys=%v, want two non-empty unique values", routeKeys)
	}
}

func TestCreateCarriesMetadataAndRestoreHeaderToRouteLink(t *testing.T) {
	var gotConfig map[string]string
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/route-link/reserve" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		var body struct {
			Config map[string]string `json:"config"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		gotConfig = body.Config
		_ = json.NewEncoder(w).Encode(reserveResult{
			NodeID: "n1", SID: "sb-1", AccessToken: "tok", DataEndpoint: "10.0.0.1:1",
		})
	}))
	defer control.Close()
	rt := New(strings.TrimPrefix(control.URL, "http://"), "test.local", 0, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	rt.SetAuthMode("off")
	srv := httptest.NewServer(rt.Handler())
	defer srv.Close()

	body, err := json.Marshal(map[string]any{"metadata": map[string]string{
		sandboxcfg.NsRestore: `{"prefetch":"off"}`,
		"application":        "kept",
	}})
	if err != nil {
		t.Fatal(err)
	}
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/sandboxes", strings.NewReader(string(body)))
	req.Host = "api.test.local"
	req.Header.Set(HeaderGroup, "/g")
	req.Header.Set("X-Kuasar-Sandbox-Restore", `{"prefetch":"memory"}`)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status=%d body=%s", resp.StatusCode, b)
	}
	if gotConfig[sandboxcfg.NsRestore] != `{"prefetch":"memory"}` {
		t.Fatalf("restore config=%q, want header value", gotConfig[sandboxcfg.NsRestore])
	}
	if gotConfig["application"] != "kept" {
		t.Fatalf("create metadata lost: %v", gotConfig)
	}
}

func TestCreateRejectsInvalidRestoreBeforeReserve(t *testing.T) {
	var reserveHits int32
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&reserveHits, 1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer control.Close()
	rt := New(strings.TrimPrefix(control.URL, "http://"), "test.local", 0, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	rt.SetAuthMode("off")
	srv := httptest.NewServer(rt.Handler())
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/sandboxes", strings.NewReader(`{"metadata":{}}`))
	req.Host = "api.test.local"
	req.Header.Set(HeaderGroup, "/g")
	req.Header.Set("X-Kuasar-Sandbox-Restore", `{"prefetch":"disk"}`)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status=%d body=%s, want 400", resp.StatusCode, b)
	}
	if got := atomic.LoadInt32(&reserveHits); got != 0 {
		t.Fatalf("invalid restore reached route-link %d times", got)
	}
}

func TestNormalizedConfigSizeBoundary(t *testing.T) {
	configAtSize := func(size int) map[string]string {
		const key = "application"
		empty, err := json.Marshal(map[string]string{key: ""})
		if err != nil {
			t.Fatal(err)
		}
		return map[string]string{key: strings.Repeat("x", size-len(empty))}
	}
	for _, tc := range []struct {
		name    string
		size    int
		wantErr bool
	}{
		{name: "at limit", size: maxNormalizedConfigBytes},
		{name: "over limit", size: maxNormalizedConfigBytes + 1, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			config := configAtSize(tc.size)
			wire, err := json.Marshal(config)
			if err != nil {
				t.Fatal(err)
			}
			if len(wire) != tc.size {
				t.Fatalf("wire size=%d, want %d", len(wire), tc.size)
			}
			if err := checkNormalizedConfigSize(config); (err != nil) != tc.wantErr {
				t.Fatalf("checkNormalizedConfigSize() error=%v, wantErr=%v", err, tc.wantErr)
			}
		})
	}
}

func TestCreateRejectsOversizedConfigBeforeReserve(t *testing.T) {
	var reserveHits int32
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&reserveHits, 1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer control.Close()
	rt := New(strings.TrimPrefix(control.URL, "http://"), "test.local", 0, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	rt.SetAuthMode("off")
	srv := httptest.NewServer(rt.Handler())
	defer srv.Close()

	requests := []struct {
		name   string
		body   string
		header string
		value  string
	}{
		{
			name: "raw body over 1 MiB",
			body: `{"metadata":{"application":"` + strings.Repeat("x", maxControlBodyBytes) + `"}}`,
		},
		{
			name:   "header normalized config over 512 KiB",
			body:   `{}`,
			header: "X-Kuasar-Sandbox-Metadata",
			value:  `{"blob":"` + strings.Repeat("x", maxNormalizedConfigBytes) + `"}`,
		},
	}
	for _, tc := range requests {
		t.Run(tc.name, func(t *testing.T) {
			req, _ := http.NewRequest(http.MethodPost, srv.URL+"/sandboxes", strings.NewReader(tc.body))
			req.Host = "api.test.local"
			req.Header.Set(HeaderGroup, "/g")
			if tc.header != "" {
				req.Header.Set(tc.header, tc.value)
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusBadRequest {
				b, _ := io.ReadAll(resp.Body)
				t.Fatalf("status=%d body=%s, want 400", resp.StatusCode, b)
			}
		})
	}
	if got := atomic.LoadInt32(&reserveHits); got != 0 {
		t.Fatalf("oversized config reached reserve %d times", got)
	}
}

func TestCreatePropagatesRouteLinkConfigRejection(t *testing.T) {
	var reserveHits int32
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&reserveHits, 1)
		http.Error(w, "effective config exceeds cluster budget", http.StatusBadRequest)
	}))
	defer control.Close()
	rt := New(strings.TrimPrefix(control.URL, "http://"), "test.local", 0, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	rt.SetAuthMode("off")
	srv := httptest.NewServer(rt.Handler())
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/sandboxes", strings.NewReader(`{"metadata":{}}`))
	req.Host = "api.test.local"
	req.Header.Set(HeaderGroup, "/g")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status=%d body=%s, want 400", resp.StatusCode, b)
	}
	if got := atomic.LoadInt32(&reserveHits); got != 1 {
		t.Fatalf("reserve hits=%d, want 1", got)
	}
}

func TestServeDataSidHostDoesNotUseByKeyReserve(t *testing.T) {
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
			_ = json.NewEncoder(w).Encode(reserveResult{NodeID: "n1", SID: "wrong", AccessToken: "tok", DataEndpoint: nodeHost})
		case "/route-link/route":
			routeHits++
			_ = json.NewEncoder(w).Encode(routeResolve{SID: "sb-1", Group: "/g", RouteKey: "rk", NodeID: "n1", DataEndpoint: nodeHost, AccessToken: "tok", State: "ready"})
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
	var gotTokens []string
	node := newDataTunnelServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&nodeHits, 1)
		gotTokens = append(gotTokens, r.Header.Get(HeaderAccessTok))
		w.WriteHeader(http.StatusNoContent)
	}))
	nodeHost := strings.TrimPrefix(node.URL, "http://")
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/route-link/route" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_ = json.NewEncoder(w).Encode(routeResolve{SID: "sb-1", Group: "/g", RouteKey: "rk", NodeID: "n1", DataEndpoint: nodeHost, AccessToken: "tok", State: "ready"})
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
	sig := envdsign.Signature("/tmp/a.txt", "", envdsign.OperationRead, "tok", nil)
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
	if got := gotTokens[len(gotTokens)-1]; got != "" {
		t.Fatalf("signed file forwarded X-Access-Token=%q, want empty", got)
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

	if code := do("49983-sb-1.test.local", "/health", "", "tok"); code != http.StatusNoContent {
		t.Fatalf("valid token status=%d, want 204", code)
	}
	if got := gotTokens[len(gotTokens)-1]; got != "tok" {
		t.Fatalf("token-auth forward X-Access-Token=%q, want tok", got)
	}
}

// TestServeDataByKey: a data-plane request carrying group + route-key headers (no
// prior create) Reserves and forwards to the node (cluster-router.md).
func TestServeDataByKey(t *testing.T) {
	var gotHost string
	var reserveHits int
	node := newDataTunnelServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHost = r.Host
		w.WriteHeader(http.StatusNoContent)
	}))
	nodeHost := strings.TrimPrefix(node.URL, "http://")
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/route-link/reserve":
			reserveHits++
			_ = json.NewEncoder(w).Encode(reserveResult{NodeID: "n1", SID: "sb-9", AccessToken: "tok", DataEndpoint: nodeHost})
		case "/route-link/verify-key":
			w.WriteHeader(http.StatusOK)
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
	req.Host = "data.test.local" // not api.<domain> → data plane; no <port>-<sid>
	req.Header.Set(HeaderGroup, "/g")
	req.Header.Set(HeaderRouteKey, "u1:s1")
	req.Header.Set(HeaderAPIKey, "e2b_ok")
	req.Header.Set("E2b-Sandbox-Port", "49983")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("by-key forward status=%d, want 204", resp.StatusCode)
	}
	if gotHost != "49983-sb-9.test.local" {
		t.Fatalf("node saw Host=%q, want 49983-sb-9.test.local (synthesized <port>-<sid>)", gotHost)
	}

	req2, _ := http.NewRequest(http.MethodGet, srv.URL+"/health", nil)
	req2.Host = "data.test.local"
	req2.Header.Set(HeaderGroup, "/g")
	req2.Header.Set(HeaderRouteKey, "u1:s1")
	req2.Header.Set(HeaderAPIKey, "e2b_ok")
	req2.Header.Set("E2b-Sandbox-Port", "49983")
	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusNoContent {
		t.Fatalf("second by-key forward status=%d, want 204", resp2.StatusCode)
	}
	if reserveHits != 1 {
		t.Fatalf("reserve hits=%d, want 1 (second request should use route cache)", reserveHits)
	}
}

func TestServeDataByKeyRequiresPortWithoutTargetPort(t *testing.T) {
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/route-link/reserve":
			_ = json.NewEncoder(w).Encode(reserveResult{NodeID: "n1", SID: "sb-9", AccessToken: "tok", DataEndpoint: "127.0.0.1:1"})
		case "/route-link/verify-key":
			w.WriteHeader(http.StatusOK)
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
	req.Host = "data.test.local"
	req.Header.Set(HeaderGroup, "/g")
	req.Header.Set(HeaderRouteKey, "u1:s1")
	req.Header.Set(HeaderAPIKey, "e2b_ok")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status=%d, want 400 when neither request nor route supplies target port", resp.StatusCode)
	}
}

func TestServeDataByKeyUsesTargetPort(t *testing.T) {
	var gotHost string
	node := newDataTunnelServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHost = r.Host
		w.WriteHeader(http.StatusNoContent)
	}))
	nodeHost := strings.TrimPrefix(node.URL, "http://")
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/route-link/reserve":
			_ = json.NewEncoder(w).Encode(reserveResult{NodeID: "n1", SID: "sb-9", AccessToken: "tok", TargetPort: 7000, DataEndpoint: nodeHost})
		case "/route-link/verify-key":
			w.WriteHeader(http.StatusOK)
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
	req.Host = "data.test.local"
	req.Header.Set(HeaderGroup, "/g")
	req.Header.Set(HeaderRouteKey, "u1:s1")
	req.Header.Set(HeaderAPIKey, "e2b_ok")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("status=%d, want 204", resp.StatusCode)
	}
	if gotHost != "7000-sb-9.test.local" {
		t.Fatalf("node saw Host=%q, want target_port sandbox host", gotHost)
	}
}

func TestServeDataRejectsTargetPortMismatch(t *testing.T) {
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/route-link/reserve":
			_ = json.NewEncoder(w).Encode(reserveResult{NodeID: "n1", SID: "sb-9", AccessToken: "tok", TargetPort: 7000, DataEndpoint: "127.0.0.1:1"})
		case "/route-link/verify-key":
			w.WriteHeader(http.StatusOK)
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
		t.Fatalf("status=%d, want 400 for target_port mismatch", resp.StatusCode)
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
		_ = json.NewEncoder(w).Encode(routeResolve{
			SID: "sb-1", Group: "/other", RouteKey: "rk", NodeID: "n1", DataEndpoint: nodeHost, AccessToken: "tok", State: "ready",
		})
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

func TestServeDataByKeyUsesActiveRouteWhenRouteCacheEvicted(t *testing.T) {
	var reserveHits int32
	var nodeHits int32
	started := make(chan struct{})
	release := make(chan struct{})
	node := newDataTunnelServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&nodeHits, 1) == 1 {
			close(started)
			<-release
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	nodeHost := strings.TrimPrefix(node.URL, "http://")
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/route-link/reserve" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		atomic.AddInt32(&reserveHits, 1)
		_ = json.NewEncoder(w).Encode(reserveResult{NodeID: "n1", SID: "sb-9", AccessToken: "tok", DataEndpoint: nodeHost})
	}))
	defer control.Close()
	rt := New(strings.TrimPrefix(control.URL, "http://"), "test.local", 0, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	rt.SetAuthMode("off")
	rt.SetDataPlaneAuth("off")
	srv := httptest.NewServer(rt.Handler())
	defer srv.Close()

	client := &http.Client{Timeout: 5 * time.Second}
	firstDone := make(chan error, 1)
	go func() {
		req, _ := http.NewRequest(http.MethodGet, srv.URL+"/health", nil)
		req.Host = "data.test.local"
		req.Header.Set(HeaderGroup, "/g")
		req.Header.Set(HeaderRouteKey, "u1:s1")
		req.Header.Set("E2b-Sandbox-Port", "49983")
		resp, err := client.Do(req)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode != http.StatusNoContent {
				err = fmt.Errorf("first status=%d", resp.StatusCode)
			}
		}
		firstDone <- err
	}()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("first request did not reach node")
	}

	// Simulate ordinary route-cache churn while the first data-plane request is
	// still active. The active-route map must still carry this route.
	rt.cacheMu.Lock()
	delete(rt.cache, "sb-9")
	delete(rt.byKey, routeCacheKey("/g", "u1:s1"))
	rt.cacheMu.Unlock()

	req2, _ := http.NewRequest(http.MethodGet, srv.URL+"/health", nil)
	req2.Host = "data.test.local"
	req2.Header.Set(HeaderGroup, "/g")
	req2.Header.Set(HeaderRouteKey, "u1:s1")
	req2.Header.Set("E2b-Sandbox-Port", "49983")
	resp2, err := client.Do(req2)
	if err != nil {
		t.Fatal(err)
	}
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusNoContent {
		t.Fatalf("second status=%d, want 204", resp2.StatusCode)
	}
	if got := atomic.LoadInt32(&reserveHits); got != 1 {
		t.Fatalf("reserve hits=%d, want 1 (second request should use active route)", got)
	}
	close(release)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
}
