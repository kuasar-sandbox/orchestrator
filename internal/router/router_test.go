package router

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/clusterclient"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestExtractBuildID(t *testing.T) {
	cases := map[string]string{
		"/v2/templates/tid/builds/bid":       "bid",
		"/templates/tid/builds/bid/status":   "bid",
		"/v2/templates/tid/builds/bid/files": "bid",
		"/sandboxes":                         "",
		"/v3/templates":                      "",
	}
	for path, want := range cases {
		if got := extractBuildID(path); got != want {
			t.Errorf("extractBuildID(%q)=%q want %q", path, got, want)
		}
	}
}

func TestCreateRestoreMetadataHeaderWinsAndNarrows(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/sandboxes", strings.NewReader(
		`{"metadata":{"kuasar-sandbox.restore":"{\"prefetch\":\"memory\"}","ignored":"value"}}`,
	))
	req.Header.Set(HeaderRestore, ` { "prefetch": "off" } `)

	got, err := createSandboxMetadata(httptest.NewRecorder(), req)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got["kuasar-sandbox.restore"] != `{"prefetch":"off"}` {
		t.Fatalf("restore metadata=%v", got)
	}
}

func TestCreateRestoreMetadataRejectsInvalidPolicy(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/sandboxes", strings.NewReader(
		`{"metadata":{"kuasar-sandbox.restore":"{\"prefetch\":\"disk\"}"}}`,
	))
	if _, err := createSandboxMetadata(httptest.NewRecorder(), req); err == nil {
		t.Fatal("invalid restore prefetch should be rejected")
	}
}

func TestCreateRestoreMetadataEmptyHeaderOverridesBodyAndRejects(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/sandboxes", strings.NewReader(
		`{"metadata":{"kuasar-sandbox.restore":"{\"prefetch\":\"memory\"}"}}`,
	))
	req.Header.Set(HeaderRestore, "")
	if _, err := createSandboxMetadata(httptest.NewRecorder(), req); err == nil {
		t.Fatal("an explicitly empty restore header should override body metadata and be rejected")
	}
}

func TestCreateSandboxMetadataCredentialsHeaderWinsAsWholeObject(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/sandboxes", strings.NewReader(
		`{"metadata":{"kuasar-sandbox.restore":"{\"prefetch\":\"memory\"}","kuasar-sandbox.credentials":"{\"envd_access_token\":\"body\"}"}}`,
	))
	req.Header.Set(HeaderCredentials, ` { "traffic_access_token": "header" } `)

	got, err := createSandboxMetadata(httptest.NewRecorder(), req)
	if err != nil {
		t.Fatal(err)
	}
	if got[sandboxcfg.NsRestore] != `{"prefetch":"memory"}` ||
		got[sandboxcfg.NsCredentials] != `{"traffic_access_token":"header"}` {
		t.Fatalf("create metadata = %+v", got)
	}
	if strings.Contains(got[sandboxcfg.NsCredentials], "body") {
		t.Fatalf("credentials objects were field-merged: %+v", got)
	}
}

func TestCreateSandboxMetadataRejectsInvalidCredentials(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/sandboxes", strings.NewReader(`{}`))
	req.Header.Set(HeaderCredentials, `{"forward_access_token":"forbidden"}`)
	if _, err := createSandboxMetadata(httptest.NewRecorder(), req); err == nil {
		t.Fatal("forbidden credentials field was accepted")
	}
}

func TestCreateRestoreMetadataAcceptsBodyPastOneMiB(t *testing.T) {
	body := `{"envVars":{"BIG":"` + strings.Repeat("x", (1<<20)+64) +
		`"},"metadata":{"kuasar-sandbox.restore":"{\"prefetch\":\"memory\"}"}}`
	if len(body) <= 1<<20 || strings.Index(body, `"metadata"`) <= 1<<20 || !json.Valid([]byte(body)) {
		t.Fatal("test body must be valid JSON with restore metadata after 1 MiB")
	}
	req := httptest.NewRequest(http.MethodPost, "/sandboxes", strings.NewReader(body))
	got, err := createSandboxMetadata(httptest.NewRecorder(), req)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got["kuasar-sandbox.restore"] != `{"prefetch":"memory"}` {
		t.Fatalf("restore metadata=%v", got)
	}
}

type failOnRead struct{ read bool }

func (r *failOnRead) Read([]byte) (int, error) {
	r.read = true
	return 0, errors.New("body must not be read")
}

type fillReader byte

func (r fillReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = byte(r)
	}
	return len(p), nil
}

func TestCreateSandboxMetadataHeadersBypassBody(t *testing.T) {
	body := &failOnRead{}
	req := httptest.NewRequest(http.MethodPost, "/sandboxes", nil)
	req.Body = io.NopCloser(body)
	req.ContentLength = maxClusterCreateBodyBytes + 1
	req.Header.Set(HeaderRestore, `{"prefetch":"memory"}`)
	req.Header.Set(HeaderCredentials, `{}`)

	got, err := createSandboxMetadata(httptest.NewRecorder(), req)
	if err != nil {
		t.Fatal(err)
	}
	if body.read {
		t.Fatal("restore header path read the create body")
	}
	if len(got) != 2 || got["kuasar-sandbox.restore"] != `{"prefetch":"memory"}` || got[sandboxcfg.NsCredentials] != `{}` {
		t.Fatalf("restore metadata=%v", got)
	}
}

func TestHandleCreateRejectsBodyPast16MiBWithoutHeader(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/sandboxes", strings.NewReader(`{}`))
	req.ContentLength = maxClusterCreateBodyBytes + 1
	req.Header.Set(HeaderGroup, "/g")
	rec := httptest.NewRecorder()

	(&Router{}).handleCreate(rec, req)

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status=%d body=%q, want 413", rec.Code, rec.Body.String())
	}
}

func sizedJSONBody(size int64) io.Reader {
	return io.MultiReader(
		strings.NewReader(`{}`),
		io.LimitReader(fillReader(' '), size-2),
	)
}

func TestCreateRestoreMetadataAcceptsExact16MiBChunkedBody(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/sandboxes", nil)
	req.Body = io.NopCloser(sizedJSONBody(maxClusterCreateBodyBytes))
	req.ContentLength = -1

	if _, err := createSandboxMetadata(httptest.NewRecorder(), req); err != nil {
		t.Fatalf("exact-limit body rejected: %v", err)
	}
}

func TestHandleCreateRejectsChunkedBodyPast16MiBWithoutHeader(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/sandboxes", nil)
	req.Body = io.NopCloser(sizedJSONBody(maxClusterCreateBodyBytes + 1))
	req.ContentLength = -1
	req.Header.Set(HeaderGroup, "/g")
	rec := httptest.NewRecorder()

	(&Router{}).handleCreate(rec, req)

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status=%d body=%q, want 413", rec.Code, rec.Body.String())
	}
}

func TestRouteLinkHTTPFailsOverOnServerError(t *testing.T) {
	var calls []string
	first := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		calls = append(calls, "first")
		return textResponse(http.StatusServiceUnavailable, "down"), nil
	})}
	second := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		calls = append(calls, "second")
		return textResponse(http.StatusOK, `{"ok":true}`), nil
	})}
	rt := &Router{routeRegistry: routeRegistryFunc(func(context.Context, string) ([]clusterclient.Endpoint, error) {
		return []clusterclient.Endpoint{
			{MemberID: "r1", BaseURL: "http://r1", Client: first},
			{MemberID: "r2", BaseURL: "http://r2", Client: second},
		}, nil
	})}
	resp, err := rt.routeLinkHTTP(context.Background(), "/g", http.MethodGet, "/route-link/test", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || strings.Join(calls, ",") != "first,second" {
		t.Fatalf("status=%d calls=%v", resp.StatusCode, calls)
	}
}

func TestRouteLinkHTTPRefreshesMembershipAndRetries(t *testing.T) {
	var calls []string
	oldClient := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		calls = append(calls, "old")
		return textResponse(http.StatusServiceUnavailable, "old owner"), nil
	})}
	newClient := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		calls = append(calls, "new")
		return textResponse(http.StatusOK, `{"ok":true}`), nil
	})}
	reg := &refreshingRouteRegistry{old: oldClient, new: newClient}
	rt := &Router{routeRegistry: reg}

	resp, err := rt.routeLinkHTTP(context.Background(), "/g", http.MethodGet, "/route-link/test", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK || reg.refreshes != 1 || strings.Join(calls, ",") != "old,new" {
		t.Fatalf("status=%d refreshes=%d calls=%v", resp.StatusCode, reg.refreshes, calls)
	}
}

func TestRouteLinkReserveCarriesRestoreConfig(t *testing.T) {
	var got struct {
		Config map[string]string `json:"config"`
	}
	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.Path != "/route-link/reserve" || req.URL.Query().Get("operation") != "create" ||
			req.URL.Query().Get("group") != "/g" || req.URL.Query().Get("route_key") != "rk" {
			t.Fatalf("reserve request path=%q query=%q", req.URL.Path, req.URL.RawQuery)
		}
		if req.Header.Get(HeaderAPIKey) != "api-key" {
			t.Fatalf("reserve X-API-KEY=%q", req.Header.Get(HeaderAPIKey))
		}
		if err := json.NewDecoder(req.Body).Decode(&got); err != nil {
			t.Fatal(err)
		}
		body, err := json.Marshal(routerTestReserveResult(t, "s1", "/g", "rk", "node:1", types.ProfileE2B))
		if err != nil {
			t.Fatal(err)
		}
		return textResponse(http.StatusOK, string(body)), nil
	})}
	rt := &Router{routeRegistry: routeRegistryFunc(func(context.Context, string) ([]clusterclient.Endpoint, error) {
		return []clusterclient.Endpoint{{MemberID: "r1", BaseURL: "http://r1", Client: client}}, nil
	})}

	config := map[string]string{"kuasar-sandbox.restore": `{"prefetch":"memory"}`}
	if _, err := rt.routeLinkReserve(context.Background(), "create", "/g", "rk", "", 0, 0, config, map[string]string{HeaderAPIKey: "api-key"}); err != nil {
		t.Fatal(err)
	}
	if got.Config["kuasar-sandbox.restore"] != `{"prefetch":"memory"}` {
		t.Fatalf("route-link reserve config=%v", got.Config)
	}
}

func TestRouteLinkReserveConnectAndDataUseQueryAndHeadersOnly(t *testing.T) {
	for _, tc := range []struct {
		name      string
		operation string
		port      int
		timeout   int
		headers   map[string]string
	}{
		{
			name: "connect", operation: "connect", timeout: 37,
			headers: map[string]string{HeaderAPIKey: "api-key", HeaderMigration: "kmt1.token"},
		},
		{
			name: "data", operation: "data", port: 8080,
			headers: map[string]string{HeaderAccessTok: "access-token"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				query := req.URL.Query()
				if req.URL.Path != "/route-link/reserve" || query.Get("operation") != tc.operation ||
					query.Get("group") != "/g" || query.Get("route_key") != "rk" || query.Get("sid") != "s1" {
					t.Fatalf("reserve path=%q query=%q", req.URL.Path, req.URL.RawQuery)
				}
				wantPort := ""
				if tc.port > 0 {
					wantPort = strconv.Itoa(tc.port)
				}
				if query.Get("port") != wantPort {
					t.Fatalf("reserve port=%q, want %q", query.Get("port"), wantPort)
				}
				wantTimeout := ""
				if tc.timeout > 0 {
					wantTimeout = strconv.Itoa(tc.timeout)
				}
				if query.Get("timeout") != wantTimeout {
					t.Fatalf("reserve timeout=%q, want %q", query.Get("timeout"), wantTimeout)
				}
				if body, err := io.ReadAll(req.Body); err != nil || len(body) != 0 {
					t.Fatalf("reserve body=%q err=%v, want empty", body, err)
				}
				if contentType := req.Header.Get("Content-Type"); contentType != "" {
					t.Fatalf("reserve Content-Type=%q, want empty", contentType)
				}
				for name, want := range tc.headers {
					if got := req.Header.Get(name); got != want {
						t.Fatalf("reserve %s=%q, want %q", name, got, want)
					}
				}
				result := routerTestReserveResult(t, "s1", "/g", "rk", "node:1", types.ProfileBare)
				if tc.operation == "connect" {
					result = routerTestConnectReserveResult(t, "s1", "/g", "rk", "node:1", types.ProfileBare)
				}
				body, err := json.Marshal(result)
				if err != nil {
					t.Fatal(err)
				}
				return textResponse(http.StatusOK, string(body)), nil
			})}
			rt := &Router{routeRegistry: routeRegistryFunc(func(context.Context, string) ([]clusterclient.Endpoint, error) {
				return []clusterclient.Endpoint{{MemberID: "r1", BaseURL: "http://r1", Client: client}}, nil
			})}
			if _, err := rt.routeLinkReserve(
				context.Background(), tc.operation, "/g", "rk", "s1", tc.port, tc.timeout, nil, tc.headers,
			); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestReserveBuildRequestCarriesStableIDsAcrossRouteLinkRetry(t *testing.T) {
	var bodies []map[string]any
	first := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		var body map[string]any
		if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		bodies = append(bodies, body)
		return textResponse(http.StatusServiceUnavailable, "try next owner"), nil
	})}
	second := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		var body map[string]any
		if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		bodies = append(bodies, body)
		return textResponse(http.StatusOK, `{"build_id":"`+body["build_id"].(string)+`","template_id":"`+body["template_id"].(string)+`","node_id":"n1","profile":"`+body["profile"].(string)+`"}`), nil
	})}
	rt := &Router{routeRegistry: routeRegistryFunc(func(context.Context, string) ([]clusterclient.Endpoint, error) {
		return []clusterclient.Endpoint{
			{MemberID: "r1", BaseURL: "http://r1", Client: first},
			{MemberID: "r2", BaseURL: "http://r2", Client: second},
		}, nil
	})}

	res, err := rt.routeLinkReserveBuild(context.Background(), "/g", types.ProfileBare, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(bodies) != 2 {
		t.Fatalf("bodies=%d, want 2", len(bodies))
	}
	if bodies[0]["build_id"] == "" || bodies[0]["template_id"] == "" {
		t.Fatalf("first reserve-build body missing stable ids: %v", bodies[0])
	}
	if bodies[0]["build_id"] != bodies[1]["build_id"] || bodies[0]["template_id"] != bodies[1]["template_id"] {
		t.Fatalf("reserve-build retry changed ids: %v then %v", bodies[0], bodies[1])
	}
	if bodies[0]["profile"] != string(types.ProfileBare) || bodies[1]["profile"] != string(types.ProfileBare) {
		t.Fatalf("reserve-build retry lost profile: %v then %v", bodies[0], bodies[1])
	}
	if res.BuildID != bodies[0]["build_id"] || res.TemplateID != bodies[0]["template_id"] {
		t.Fatalf("reserve result=%+v bodies=%v", res, bodies)
	}
	if res.Profile != types.ProfileBare {
		t.Fatalf("reserve profile=%q, want %q", res.Profile, types.ProfileBare)
	}
}

func TestHandleListPropagatesRouteLinkStatus(t *testing.T) {
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/route-link/verify-key":
			w.WriteHeader(http.StatusOK)
		case "/route-link/list":
			http.Error(w, "not ready", http.StatusServiceUnavailable)
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
	req.Header.Set(HeaderAPIKey, "e2b_test")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("list status=%d, want 503", resp.StatusCode)
	}
}

func TestControlForwardTransportIsEndpointScoped(t *testing.T) {
	var node1Hits, node2Hits int
	node1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		node1Hits++
		if r.URL.Path != "/sandboxes/sb-1-g0" {
			t.Fatalf("node1 saw path %q", r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"sandboxID": "sb-1-g0", "opaque": "sb-1-g0"})
	}))
	defer node1.Close()
	node2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		node2Hits++
		if r.URL.Path != "/sandboxes/sb-2-g0" {
			t.Fatalf("node2 saw path %q", r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"sandboxID": "sb-2-g0", "opaque": "sb-2-g0"})
	}))
	defer node2.Close()
	node1Host := strings.TrimPrefix(node1.URL, "http://")
	node2Host := strings.TrimPrefix(node2.URL, "http://")
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/route-link/verify-key":
			w.WriteHeader(http.StatusOK)
		case "/route-link/route":
			switch r.URL.Query().Get("sid") {
			case "sb-1":
				_ = json.NewEncoder(w).Encode(routerTestRouteResolve(t, "sb-1", "/g", "rk1", node1Host, types.ProfileE2B))
			case "sb-2":
				_ = json.NewEncoder(w).Encode(routerTestRouteResolve(t, "sb-2", "/g", "rk2", node2Host, types.ProfileE2B))
			default:
				w.WriteHeader(http.StatusNotFound)
			}
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer control.Close()
	rt := New(strings.TrimPrefix(control.URL, "http://"), "test.local", 0, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	srv := httptest.NewServer(rt.Handler())
	defer srv.Close()

	client := &http.Client{}
	for _, tc := range []struct {
		sid      string
		routeKey string
	}{{"sb-1", "rk1"}, {"sb-2", "rk2"}} {
		req, _ := http.NewRequest(http.MethodGet, srv.URL+"/sandboxes/"+tc.sid, nil)
		req.Host = "api.test.local"
		req.Header.Set(HeaderGroup, "/g")
		req.Header.Set(HeaderRouteKey, tc.routeKey)
		req.Header.Set(HeaderAPIKey, "e2b_test")
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		var body map[string]string
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%s status=%d, want 200", tc.sid, resp.StatusCode)
		}
		if body["sandboxID"] != tc.sid || body["opaque"] != tc.sid+"-g0" {
			t.Fatalf("%s adapted response=%v", tc.sid, body)
		}
	}
	if node1Hits != 1 || node2Hits != 1 {
		t.Fatalf("node hits node1=%d node2=%d, want one hit each", node1Hits, node2Hits)
	}
}

func TestSandboxControlForwardRewritesOnlyPathIdentity(t *testing.T) {
	var nodeHits int
	node := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		nodeHits++
		if r.URL.Path != "/v2/sandboxes/sb-1-g0/pause" || r.URL.RawQuery != "reason=sb-1" {
			t.Fatalf("node request path=%q query=%q", r.URL.Path, r.URL.RawQuery)
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		if string(body) != `{"opaque":"sb-1"}` {
			t.Fatalf("node body=%q, want unchanged", body)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer node.Close()
	nodeHost := strings.TrimPrefix(node.URL, "http://")
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/route-link/verify-key" {
			w.WriteHeader(http.StatusOK)
			return
		}
		if r.URL.Path != "/route-link/route" || r.URL.Query().Get("sid") != "sb-1" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_ = json.NewEncoder(w).Encode(routerTestRouteResolve(t, "sb-1", "/g", "rk", nodeHost, types.ProfileE2B))
	}))
	defer control.Close()
	rt := New(strings.TrimPrefix(control.URL, "http://"), "test.local", 0, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	srv := httptest.NewServer(rt.Handler())
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodPost, srv.URL+`/v2/sandboxes/sb-1/pause?reason=sb-1`, strings.NewReader(`{"opaque":"sb-1"}`))
	req.Host = "api.test.local"
	req.Header.Set(HeaderGroup, "/g")
	req.Header.Set(HeaderRouteKey, "rk")
	req.Header.Set(HeaderAPIKey, "e2b_test")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent || nodeHits != 1 {
		t.Fatalf("control forward status=%d nodeHits=%d", resp.StatusCode, nodeHits)
	}
}

type routeRegistryFunc func(context.Context, string) ([]clusterclient.Endpoint, error)

func (f routeRegistryFunc) RouteCandidates(ctx context.Context, group string) ([]clusterclient.Endpoint, error) {
	return f(ctx, group)
}

func (f routeRegistryFunc) Refresh(context.Context) error { return nil }

type refreshingRouteRegistry struct {
	old       *http.Client
	new       *http.Client
	refreshes int
}

func (r *refreshingRouteRegistry) RouteCandidates(context.Context, string) ([]clusterclient.Endpoint, error) {
	client := r.old
	id := "old"
	if r.refreshes > 0 {
		client = r.new
		id = "new"
	}
	return []clusterclient.Endpoint{{MemberID: id, BaseURL: "http://" + id, Client: client}}, nil
}

func (r *refreshingRouteRegistry) Refresh(context.Context) error {
	r.refreshes++
	return nil
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func textResponse(code int, body string) *http.Response {
	return &http.Response{
		StatusCode: code,
		Status:     http.StatusText(code),
		Body:       io.NopCloser(strings.NewReader(body)),
		Header:     http.Header{"Content-Type": []string{"text/plain"}},
	}
}

// TestBuildRoutingThroughRouter checks the Phase 5 path: a build register
// reserves a node and the router records (group, build_id) -> node, so a
// follow-up trigger routes to the same node.
func TestBuildRoutingThroughRouter(t *testing.T) {
	var triggeredBuild string
	var reserveProfiles []string
	node := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/templates"):
			_ = json.NewEncoder(w).Encode(map[string]string{"templateID": "t1", "buildID": "b1"})
		case strings.Contains(r.URL.Path, "/builds/"):
			triggeredBuild = extractBuildID(r.URL.Path)
			w.WriteHeader(http.StatusAccepted)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer node.Close()
	nodeHost := strings.TrimPrefix(node.URL, "http://")

	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/route-link/reserve-build":
			// The registry assigns the build/template ids + places it (§7.5); the
			// router synthesizes the e2b register response and routes follow-ups.
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			profile, ok := body["profile"].(string)
			if !ok {
				http.Error(w, "profile missing", http.StatusBadRequest)
				return
			}
			reserveProfiles = append(reserveProfiles, profile)
			_ = json.NewEncoder(w).Encode(buildReserveResult{
				BuildID: "b1", TemplateID: "t1", NodeID: "n1", DataEndpoint: nodeHost, Profile: types.ProfileE2B,
			})
		case "/route-link/verify-key":
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer control.Close()

	rt := New(strings.TrimPrefix(control.URL, "http://"), "test.local", 0, nil, slog.Default())
	srv := httptest.NewServer(rt.Handler())
	defer srv.Close()

	badReq, _ := http.NewRequest(http.MethodPost, srv.URL+"/v3/templates", strings.NewReader(`{"profile":"unknown"}`))
	badReq.Host = "api.test.local"
	badReq.Header.Set(HeaderGroup, "/g")
	badReq.Header.Set(HeaderAPIKey, "e2b_test")
	badResp, err := http.DefaultClient.Do(badReq)
	if err != nil {
		t.Fatal(err)
	}
	badResp.Body.Close()
	if badResp.StatusCode != http.StatusBadRequest || len(reserveProfiles) != 0 {
		t.Fatalf("invalid profile status=%d reserveProfiles=%v", badResp.StatusCode, reserveProfiles)
	}

	// register a build via the router (control plane: Host api.<domain> + group).
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v3/templates", nil)
	req.Host = "api.test.local"
	req.Header.Set(HeaderGroup, "/g")
	req.Header.Set(HeaderAPIKey, "e2b_test")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	var reg struct {
		BuildID string `json:"buildID"`
		Profile string `json:"profile"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&reg)
	resp.Body.Close()
	if reg.BuildID != "b1" || reg.Profile != string(types.ProfileE2B) || len(reserveProfiles) != 1 || reserveProfiles[0] != string(types.ProfileE2B) {
		t.Fatalf("register result=%+v reserveProfiles=%v", reg, reserveProfiles)
	}

	// a trigger for b1 (no group header) must route to the recorded node.
	req2, _ := http.NewRequest(http.MethodPost, srv.URL+"/v2/templates/t1/builds/b1", nil)
	req2.Host = "api.test.local"
	req2.Header.Set(HeaderGroup, "/g")
	req2.Header.Set(HeaderAPIKey, "e2b_test")
	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatal(err)
	}
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusAccepted {
		t.Fatalf("trigger status=%d, want 202", resp2.StatusCode)
	}
	if triggeredBuild != "b1" {
		t.Fatalf("node saw build %q, want b1 (router build-id routing)", triggeredBuild)
	}
}

func TestBuildCacheIsGroupScoped(t *testing.T) {
	var hits [2]int
	nodes := make([]*httptest.Server, 2)
	for i := range nodes {
		idx := i
		nodes[i] = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			hits[idx]++
			w.WriteHeader(http.StatusAccepted)
		}))
		defer nodes[i].Close()
	}
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/route-link/verify-key" {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer control.Close()

	rt := New(strings.TrimPrefix(control.URL, "http://"), "test.local", 0, nil, slog.Default())
	rt.builds[buildCacheKey("/g1", "b1")] = buildEntry{node: strings.TrimPrefix(nodes[0].URL, "http://"), at: time.Now()}
	rt.builds[buildCacheKey("/g2", "b1")] = buildEntry{node: strings.TrimPrefix(nodes[1].URL, "http://"), at: time.Now()}
	srv := httptest.NewServer(rt.Handler())
	defer srv.Close()

	for _, group := range []string{"/g1", "/g2"} {
		req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v2/templates/t1/builds/b1", nil)
		req.Host = "api.test.local"
		req.Header.Set(HeaderGroup, group)
		req.Header.Set(HeaderAPIKey, "e2b_test")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusAccepted {
			t.Fatalf("group %s status=%d, want 202", group, resp.StatusCode)
		}
	}
	if hits != [2]int{1, 1} {
		t.Fatalf("node hits=%v, want [1 1]", hits)
	}
}
