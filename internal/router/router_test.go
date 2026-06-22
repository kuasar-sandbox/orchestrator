package router

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
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

	op := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/op/reserve-build":
			// The registry assigns the build/template ids + places it (§7.5); the
			// router synthesizes the e2b register response and routes follow-ups.
			_ = json.NewEncoder(w).Encode(buildReserveResult{BuildID: "b1", TemplateID: "t1", NodeID: "n1", DataEndpoint: nodeHost})
		case "/op/verify-key":
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer op.Close()

	rt := New(strings.TrimPrefix(op.URL, "http://"), "test.local", 0, nil, slog.Default())
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
