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

	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/clusterclient"
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

type routeRegistryFunc func(context.Context, string) ([]clusterclient.Endpoint, error)

func (f routeRegistryFunc) RouteCandidates(ctx context.Context, group string) ([]clusterclient.Endpoint, error) {
	return f(ctx, group)
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
