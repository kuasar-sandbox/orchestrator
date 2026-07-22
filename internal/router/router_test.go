package router

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/buildcfg"
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

	metadata := map[string]string{sandboxcfg.NsRestore: `{"prefetch":"memory"}`}
	res, err := rt.routeLinkReserveBuild(context.Background(), "/g", types.ProfileBare, nil, metadata)
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
	for i, body := range bodies {
		got, ok := body["metadata"].(map[string]any)
		if !ok || got[sandboxcfg.NsRestore] != metadata[sandboxcfg.NsRestore] {
			t.Fatalf("reserve-build body %d metadata=%v, want %v", i, got, metadata)
		}
	}
	if res.BuildID != bodies[0]["build_id"] || res.TemplateID != bodies[0]["template_id"] {
		t.Fatalf("reserve result=%+v bodies=%v", res, bodies)
	}
	if res.Profile != types.ProfileBare {
		t.Fatalf("reserve profile=%q, want %q", res.Profile, types.ProfileBare)
	}
}

func TestReserveSandboxRequestCarriesStableConfigAcrossRouteLinkRetry(t *testing.T) {
	var bodies []string
	transport := func(status int, response string) *http.Client {
		return &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			body, err := io.ReadAll(req.Body)
			if err != nil {
				t.Fatal(err)
			}
			bodies = append(bodies, string(body))
			if got := req.Header.Get("Content-Type"); got != "application/json" {
				t.Fatalf("Content-Type=%q, want application/json", got)
			}
			return textResponse(status, response), nil
		})}
	}
	first := transport(http.StatusServiceUnavailable, "try next owner")
	second := transport(http.StatusOK, `{"node_id":"n1","sid":"sb-1","access_token":"tok","data_endpoint":"10.0.0.1:1"}`)
	rt := &Router{routeRegistry: routeRegistryFunc(func(context.Context, string) ([]clusterclient.Endpoint, error) {
		return []clusterclient.Endpoint{
			{MemberID: "r1", BaseURL: "http://r1", Client: first},
			{MemberID: "r2", BaseURL: "http://r2", Client: second},
		}, nil
	})}
	config := map[string]string{
		sandboxcfg.NsRestore: `{"prefetch":"memory"}`,
		"application":        "stable",
	}

	res, err := rt.routeLinkReserve(context.Background(), "/g", "rk", config)
	if err != nil {
		t.Fatal(err)
	}
	if res.SID != "sb-1" || len(bodies) != 2 {
		t.Fatalf("result=%+v bodies=%d", res, len(bodies))
	}
	if bodies[0] != bodies[1] {
		t.Fatalf("reserve retry changed request body: %q then %q", bodies[0], bodies[1])
	}
	var wire struct {
		Config map[string]string `json:"config"`
	}
	if err := json.Unmarshal([]byte(bodies[0]), &wire); err != nil {
		t.Fatal(err)
	}
	if wire.Config[sandboxcfg.NsRestore] != config[sandboxcfg.NsRestore] || wire.Config["application"] != "stable" {
		t.Fatalf("reserve config=%v, want %v", wire.Config, config)
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
		if r.URL.Path != "/sandboxes/sb-1" {
			t.Fatalf("node1 saw path %q", r.URL.Path)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer node1.Close()
	node2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		node2Hits++
		if r.URL.Path != "/sandboxes/sb-2" {
			t.Fatalf("node2 saw path %q", r.URL.Path)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer node2.Close()
	node1Host := strings.TrimPrefix(node1.URL, "http://")
	node2Host := strings.TrimPrefix(node2.URL, "http://")
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/route-link/route":
			switch r.URL.Query().Get("sid") {
			case "sb-1":
				_ = json.NewEncoder(w).Encode(routeResolve{SID: "sb-1", Group: "/g", RouteKey: "rk1", DataEndpoint: node1Host, State: "ready"})
			case "sb-2":
				_ = json.NewEncoder(w).Encode(routeResolve{SID: "sb-2", Group: "/g", RouteKey: "rk2", DataEndpoint: node2Host, State: "ready"})
			default:
				w.WriteHeader(http.StatusNotFound)
			}
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer control.Close()
	rt := New(strings.TrimPrefix(control.URL, "http://"), "test.local", 0, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	rt.SetAuthMode("off")
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
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusNoContent {
			t.Fatalf("%s status=%d, want 204", tc.sid, resp.StatusCode)
		}
	}
	if node1Hits != 1 || node2Hits != 1 {
		t.Fatalf("node hits node1=%d node2=%d, want one hit each", node1Hits, node2Hits)
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

func TestBuildRegisterCarriesConfigHeadersToRouteLink(t *testing.T) {
	var reserveHits int
	var gotMetadata map[string]string
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/route-link/reserve-build" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		reserveHits++
		var body struct {
			Metadata map[string]string `json:"metadata"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		gotMetadata = body.Metadata
		_ = json.NewEncoder(w).Encode(buildReserveResult{
			BuildID: "b1", TemplateID: "t1", NodeID: "n1", Profile: types.ProfileE2B,
		})
	}))
	defer control.Close()

	rt := New(strings.TrimPrefix(control.URL, "http://"), "test.local", 0, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	rt.SetAuthMode("off")
	srv := httptest.NewServer(rt.Handler())
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v3/templates", strings.NewReader(`{"cpu_count":2,"memory_mb":512}`))
	req.Host = "api.test.local"
	req.Header.Set(HeaderGroup, "/g")
	req.Header.Set("X-Kuasar-Sandbox-Restore", `{"prefetch":"memory"}`)
	req.Header.Set("X-Kuasar-Sandbox-Resource", `{"capacity":{"cpu":9}}`)
	req.Header.Set(headerSandboxBuilder, `{"referer":{"enabled":false}}`)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status=%d body=%s, want 202", resp.StatusCode, b)
	}
	if reserveHits != 1 {
		t.Fatalf("reserve hits=%d, want 1", reserveHits)
	}
	if got := gotMetadata[sandboxcfg.NsRestore]; got != `{"prefetch":"memory"}` {
		t.Fatalf("restore metadata=%q", got)
	}
	if got := gotMetadata[buildcfg.NsBuilder]; got != `{"referer":{"enabled":false}}` {
		t.Fatalf("builder metadata=%q", got)
	}
	if got := gotMetadata[sandboxcfg.NsResource]; got != `{"capacity":{"cpu":2,"memory":"512MiB"}}` {
		t.Fatalf("resource metadata=%q", got)
	}
}

func TestBuildRegisterRejectsInvalidConfigBeforeReserve(t *testing.T) {
	for _, tc := range []struct {
		name   string
		header string
		value  string
	}{
		{name: "restore", header: "X-Kuasar-Sandbox-Restore", value: `{"prefetch":"disk"}`},
		{name: "builder", header: headerSandboxBuilder, value: `{"referer":{"unknown":true}}`},
		{name: "oversized", header: "X-Kuasar-Sandbox-Metadata", value: `{"blob":"` + strings.Repeat("x", maxNormalizedConfigBytes) + `"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var reserveHits int
			control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				reserveHits++
				w.WriteHeader(http.StatusInternalServerError)
			}))
			defer control.Close()
			rt := New(strings.TrimPrefix(control.URL, "http://"), "test.local", 0, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
			rt.SetAuthMode("off")
			srv := httptest.NewServer(rt.Handler())
			defer srv.Close()

			req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v3/templates", nil)
			req.Host = "api.test.local"
			req.Header.Set(HeaderGroup, "/g")
			req.Header.Set(tc.header, tc.value)
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusBadRequest {
				b, _ := io.ReadAll(resp.Body)
				t.Fatalf("status=%d body=%s, want 400", resp.StatusCode, b)
			}
			if reserveHits != 0 {
				t.Fatalf("invalid config reached reserve %d times", reserveHits)
			}
		})
	}
}

func TestBuildRegisterPropagatesRouteLinkConfigRejection(t *testing.T) {
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "effective build config exceeds cluster budget", http.StatusBadRequest)
	}))
	defer control.Close()
	rt := New(strings.TrimPrefix(control.URL, "http://"), "test.local", 0, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	rt.SetAuthMode("off")
	srv := httptest.NewServer(rt.Handler())
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v3/templates", strings.NewReader(`{"name":"t"}`))
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
