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

	"github.com/kuasar-sandbox/orchestrator/internal/clusterclient"
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
		return textResponse(http.StatusOK, `{"build_id":"`+body["build_id"].(string)+`","template_id":"`+body["template_id"].(string)+`","node_id":"n1"}`), nil
	})}
	rt := &Router{routeRegistry: routeRegistryFunc(func(context.Context, string) ([]clusterclient.Endpoint, error) {
		return []clusterclient.Endpoint{
			{MemberID: "r1", BaseURL: "http://r1", Client: first},
			{MemberID: "r2", BaseURL: "http://r2", Client: second},
		}, nil
	})}

	res, err := rt.routeLinkReserveBuild(context.Background(), "/g", nil)
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
	if res.BuildID != bodies[0]["build_id"] || res.TemplateID != bodies[0]["template_id"] {
		t.Fatalf("reserve result=%+v bodies=%v", res, bodies)
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
		req, _ := http.NewRequest(http.MethodDelete, srv.URL+"/sandboxes/"+tc.sid, nil)
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
// reserves a node and the router records build_id -> node, so a follow-up trigger
// (which carries no group) still routes to the same node.
func TestBuildRoutingThroughRouter(t *testing.T) {
	var triggeredBuild string
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
			_ = json.NewEncoder(w).Encode(buildReserveResult{BuildID: "b1", TemplateID: "t1", NodeID: "n1", DataEndpoint: nodeHost})
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
	}
	_ = json.NewDecoder(resp.Body).Decode(&reg)
	resp.Body.Close()
	if reg.BuildID != "b1" {
		t.Fatalf("register passed through buildID=%q, want b1", reg.BuildID)
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
