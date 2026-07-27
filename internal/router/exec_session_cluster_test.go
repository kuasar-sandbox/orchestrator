package router

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/execsession"
	"github.com/kuasar-sandbox/orchestrator/internal/migrationtoken"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestClusterExecSessionUsesReserveAndReturnsOnlyToken(t *testing.T) {
	var reserveHits atomic.Int32
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/route-link/reserve" {
			t.Fatalf("route-link path = %q", r.URL.Path)
		}
		reserveHits.Add(1)
		query := r.URL.Query()
		if query.Get("operation") != "exec-session" || query.Get("group") != "/g" ||
			query.Get("route_key") != "rk" || query.Get("sid") != "stable" || query.Get("ttl_seconds") != "37" ||
			query.Get("port") != "" || query.Get("timeout") != "" {
			t.Fatalf("exec-session reserve query = %q", r.URL.RawQuery)
		}
		if got := r.Header.Get(HeaderAPIKey); got != "api-key" {
			t.Fatalf("reserve X-API-KEY = %q", got)
		}
		if got := r.Header.Get(HeaderMigration); got != "kmt1.opaque" {
			t.Fatalf("reserve migration token = %q", got)
		}
		body, err := io.ReadAll(r.Body)
		if err != nil || len(body) != 0 {
			t.Fatalf("reserve body = %q, %v; want empty", body, err)
		}
		if got := r.Header.Get("Content-Type"); got != "" {
			t.Fatalf("reserve Content-Type = %q, want empty", got)
		}
		result := routerTestReserveResult(t, "stable", "/g", "rk", "node.invalid:9443", types.ProfileBare)
		result.Route.State = "reserved"
		result.Route.NodeSandboxID = "stable-g3"
		result.Route.RouteRevision = 9
		result.ExecSession = &execSessionResult{ExecAccessToken: "kat1.exec"}
		_ = json.NewEncoder(w).Encode(result)
	}))
	defer control.Close()

	rt := New(strings.TrimPrefix(control.URL, "http://"), "test.local", 0, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	srv := httptest.NewServer(rt.Handler())
	defer srv.Close()
	req, err := http.NewRequest(http.MethodPost, srv.URL+"/sandboxes/stable/exec-sessions", strings.NewReader(`{"ttlSeconds":37}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Host = "api.test.local"
	req.Header.Set(HeaderGroup, "/g")
	req.Header.Set(HeaderRouteKey, "rk")
	req.Header.Set(HeaderAPIKey, "api-key")
	req.Header.Set(HeaderMigration, "kmt1.opaque")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated || reserveHits.Load() != 1 {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, reserve hits = %d, body = %q", resp.StatusCode, reserveHits.Load(), body)
	}
	if got := resp.Header.Get("Cache-Control"); got != "no-store" {
		t.Fatalf("Cache-Control = %q, want no-store", got)
	}
	var payload map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatal(err)
	}
	if len(payload) != 1 || payload["execAccessToken"] != "kat1.exec" {
		t.Fatalf("public response = %#v", payload)
	}
	cached := rt.cachedRoute("/g", "rk", "stable")
	if cached == nil || cached.NodeSandboxID != "stable-g3" || cached.State != "reserved" || cached.RouteRevision != 9 {
		t.Fatalf("cached current route = %+v", cached)
	}
}

func TestClusterExecSessionUsesSharedStrictBodyContract(t *testing.T) {
	var reserveHits atomic.Int32
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reserveHits.Add(1)
		if got := r.URL.Query().Get("ttl_seconds"); got != "" && got != "37" {
			t.Fatalf("ttl_seconds = %q, want empty or 37", got)
		}
		result := routerTestReserveResult(t, "stable", "/g", "rk", "node.invalid:9443", types.ProfileBare)
		result.ExecSession = &execSessionResult{ExecAccessToken: "kat1.exec"}
		_ = json.NewEncoder(w).Encode(result)
	}))
	defer control.Close()
	rt := New(strings.TrimPrefix(control.URL, "http://"), "test.local", 0, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	for _, body := range []string{"", `{}`, `{"ttlSeconds":0}`} {
		response := clusterExecSessionRequest(t, rt, body, -1, "")
		if response.Code != http.StatusCreated {
			t.Fatalf("default TTL body %q status = %d, body = %q", body, response.Code, response.Body.String())
		}
	}
	if got := reserveHits.Load(); got != 3 {
		t.Fatalf("default TTL reserve hits = %d, want 3", got)
	}

	valid := `{"ttlSeconds":37}`
	valid += strings.Repeat(" ", int(execsession.MaxRequestBodyBytes)-len(valid))
	recorder := clusterExecSessionRequest(t, rt, valid, -1, "")
	if recorder.Code != http.StatusCreated || reserveHits.Load() != 4 {
		t.Fatalf("exact-limit status = %d, reserve hits = %d, body = %q", recorder.Code, reserveHits.Load(), recorder.Body.String())
	}

	tests := []struct {
		name          string
		body          string
		contentLength int64
		wantStatus    int
	}{
		{name: "unknown field", body: `{"unknown":true}`, contentLength: -1, wantStatus: http.StatusBadRequest},
		{name: "null", body: `null`, contentLength: -1, wantStatus: http.StatusBadRequest},
		{name: "negative ttl", body: `{"ttlSeconds":-1}`, contentLength: -1, wantStatus: http.StatusBadRequest},
		{name: "second value", body: `{} {}`, contentLength: -1, wantStatus: http.StatusBadRequest},
		{name: "content length", body: `{}`, contentLength: execsession.MaxRequestBodyBytes + 1, wantStatus: http.StatusRequestEntityTooLarge},
		{name: "streamed", body: `{}` + strings.Repeat(" ", int(execsession.MaxRequestBodyBytes)-1), contentLength: -1, wantStatus: http.StatusRequestEntityTooLarge},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			before := reserveHits.Load()
			response := clusterExecSessionRequest(t, rt, test.body, test.contentLength, "")
			if response.Code != test.wantStatus {
				t.Fatalf("status = %d, want %d; body = %q", response.Code, test.wantStatus, response.Body.String())
			}
			if got := reserveHits.Load(); got != before {
				t.Fatalf("invalid request reached Reserve: before=%d after=%d", before, got)
			}
		})
	}
}

func TestClusterExecSessionRejectsOversizedMigrationBeforeReserve(t *testing.T) {
	var reserveHits atomic.Int32
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reserveHits.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer control.Close()
	rt := New(strings.TrimPrefix(control.URL, "http://"), "test.local", 0, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	response := clusterExecSessionRequest(t, rt, `{}`, -1, strings.Repeat("x", migrationtoken.MaxWireSize+1))
	if response.Code != http.StatusRequestHeaderFieldsTooLarge || reserveHits.Load() != 0 {
		t.Fatalf("status = %d, reserve hits = %d", response.Code, reserveHits.Load())
	}
}

func TestClusterExecSessionPropagatesReserveTTLOverflow(t *testing.T) {
	var reserveHits atomic.Int32
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reserveHits.Add(1)
		if got := r.URL.Query().Get("ttl_seconds"); got != "9223372036854775807" {
			t.Fatalf("ttl_seconds = %q", got)
		}
		http.Error(w, "registry: bad reserve request: ttl_seconds overflows Unix time", http.StatusBadRequest)
	}))
	defer control.Close()
	rt := New(strings.TrimPrefix(control.URL, "http://"), "test.local", 0, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	response := clusterExecSessionRequest(t, rt, `{"ttlSeconds":9223372036854775807}`, -1, "")
	if response.Code != http.StatusBadRequest || reserveHits.Load() != 1 {
		t.Fatalf("status = %d, reserve hits = %d, body = %q", response.Code, reserveHits.Load(), response.Body.String())
	}
	if cached := rt.cachedRoute("/g", "rk", "stable"); cached != nil {
		t.Fatalf("overflowing request entered cache: %+v", cached)
	}
}

func TestClusterExecSessionRequiresPostAndRawAPIKey(t *testing.T) {
	var reserveHits atomic.Int32
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reserveHits.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer control.Close()
	rt := New(strings.TrimPrefix(control.URL, "http://"), "test.local", 0, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))

	bearerOnly := httptest.NewRequest(http.MethodPost, "/sandboxes/stable/exec-sessions", strings.NewReader(`{}`))
	bearerOnly.Host = "api.test.local"
	bearerOnly.Header.Set(HeaderGroup, "/g")
	bearerOnly.Header.Set(HeaderRouteKey, "rk")
	bearerOnly.Header.Set("Authorization", "Bearer api-key")
	bearerResponse := httptest.NewRecorder()
	rt.Handler().ServeHTTP(bearerResponse, bearerOnly)
	if bearerResponse.Code != http.StatusUnauthorized {
		t.Fatalf("Bearer-only status = %d, want 401; body = %q", bearerResponse.Code, bearerResponse.Body.String())
	}

	getRequest := httptest.NewRequest(http.MethodGet, "/sandboxes/stable/exec-sessions", nil)
	getRequest.Host = "api.test.local"
	getRequest.Header.Set(HeaderAPIKey, "api-key")
	getResponse := httptest.NewRecorder()
	rt.Handler().ServeHTTP(getResponse, getRequest)
	if getResponse.Code != http.StatusMethodNotAllowed {
		t.Fatalf("GET status = %d, want 405; body = %q", getResponse.Code, getResponse.Body.String())
	}
	if reserveHits.Load() != 0 {
		t.Fatalf("rejected requests reached Registry %d times", reserveHits.Load())
	}
}

func TestClusterExecSessionRejectsInconsistentReserveResult(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*reserveResult)
	}{
		{name: "missing result", mutate: func(result *reserveResult) { result.ExecSession = nil }},
		{name: "empty token", mutate: func(result *reserveResult) { result.ExecSession.ExecAccessToken = "" }},
		{name: "wrong stable identity", mutate: func(result *reserveResult) { result.Route.SandboxID = "replacement" }},
		{name: "wrong route key", mutate: func(result *reserveResult) { result.Route.RouteKey = "other" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				result := routerTestReserveResult(t, "stable", "/g", "rk", "node.invalid:9443", types.ProfileBare)
				result.ExecSession = &execSessionResult{ExecAccessToken: "kat1.exec"}
				test.mutate(&result)
				_ = json.NewEncoder(w).Encode(result)
			}))
			defer control.Close()
			rt := New(strings.TrimPrefix(control.URL, "http://"), "test.local", 0, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
			response := clusterExecSessionRequest(t, rt, `{}`, -1, "")
			if response.Code != http.StatusBadGateway {
				t.Fatalf("status = %d, want 502; body = %q", response.Code, response.Body.String())
			}
			if cached := rt.cachedRoute("/g", "rk", "stable"); cached != nil {
				t.Fatalf("invalid result entered cache: %+v", cached)
			}
		})
	}
}

func clusterExecSessionRequest(t *testing.T, rt *Router, body string, contentLength int64, migrationToken string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, "/sandboxes/stable/exec-sessions", strings.NewReader(body))
	request.Host = "api.test.local"
	request.ContentLength = contentLength
	request.Header.Set(HeaderGroup, "/g")
	request.Header.Set(HeaderRouteKey, "rk")
	request.Header.Set(HeaderAPIKey, "api-key")
	if migrationToken != "" {
		request.Header.Set(HeaderMigration, migrationToken)
	}
	response := httptest.NewRecorder()
	rt.Handler().ServeHTTP(response, request)
	return response
}
