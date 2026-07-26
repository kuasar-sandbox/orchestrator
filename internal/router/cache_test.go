package router

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestAuthRejectsBadKey checks the Phase 7g router auth: a create whose api key
// the registry rejects is 403'd at the router, before any reserve.
func TestAuthRejectsBadKey(t *testing.T) {
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/route-link/verify-key" {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer control.Close()
	rt := New(strings.TrimPrefix(control.URL, "http://"), "test.local", 0, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
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
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("create with a rejected key status=%d, want 403", resp.StatusCode)
	}
}

// TestSandboxDeleteUsesRouteOwnerCommand checks the cluster control plane delete
// path: DELETE is a lifecycle command owned by route_link/node_link, not a
// reverse proxy to the node's local e2b API. This keeps cluster APISecret
// independent from the node-local manifest_key.
func TestSandboxDeleteUsesRouteOwnerCommand(t *testing.T) {
	var deletePath, deleteQuery string
	var nodeHits int
	node := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		nodeHits++
		w.WriteHeader(http.StatusTeapot)
	}))
	defer node.Close()

	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/route-link/delete":
			deletePath, deleteQuery = r.URL.Path, r.URL.RawQuery
			w.WriteHeader(http.StatusNoContent)
		case "/route-link/verify-key":
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
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
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("delete status=%d, want 204", resp.StatusCode)
	}
	if nodeHits != 0 {
		t.Fatalf("DELETE was forwarded to node %d time(s), want route_link command path only", nodeHits)
	}
	if deletePath != "/route-link/delete" || !strings.Contains(deleteQuery, "group=%2Fg") || !strings.Contains(deleteQuery, "route_key=rk") || !strings.Contains(deleteQuery, "sid=sb-1") {
		t.Fatalf("route_link delete path=%q query=%q", deletePath, deleteQuery)
	}
}
