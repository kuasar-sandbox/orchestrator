package router

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestStableSandboxCacheDoesNotPinOrEvictNewNodeIdentity(t *testing.T) {
	rt := &Router{
		cache:  map[string]*routeResolve{},
		active: map[string]*activeRoute{},
	}
	g0 := &routeResolve{Group: "/g", RouteKey: "rk", SandboxID: "sb-1", NodeSandboxID: "sb-1-g0", RouteRevision: 10}
	g1 := &routeResolve{Group: "/g", RouteKey: "rk", SandboxID: "sb-1", NodeSandboxID: "sb-1-g1", RouteRevision: 11}
	rt.rememberRoute(g0)
	done := rt.beginActiveRoute(g0)
	defer done()
	rt.rememberRoute(g1)
	rt.rememberRoute(g0) // a late g0 result must not overwrite current g1
	sameRevisionDifferentTarget := *g1
	sameRevisionDifferentTarget.NodeSandboxID = "sb-1-g2"
	rt.rememberRoute(&sameRevisionDifferentTarget)

	if got := rt.cachedRoute("/g", "rk", "sb-1"); got == nil || got.NodeSandboxID != "sb-1-g1" {
		t.Fatalf("cached route after cutover=%+v, want g1", got)
	}
	rt.evictRouteIfCurrent("/g", "rk", "sb-1", "sb-1-g0")
	if got := rt.cachedRoute("/g", "rk", "sb-1"); got == nil || got.NodeSandboxID != "sb-1-g1" {
		t.Fatalf("old generation evicted current route: %+v", got)
	}
	rt.evictRouteIfCurrent("/g", "rk", "sb-1", "sb-1-g1")
	if got := rt.cachedRoute("/g", "rk", "sb-1"); got != nil {
		t.Fatalf("current generation remained cached: %+v", got)
	}
}

// TestAuthRejectsBadKey checks that create authentication is part of Reserve:
// the router carries the client's API key to the registry and returns its 403
// without a separate verify-key round trip.
func TestAuthRejectsBadKey(t *testing.T) {
	var reserveHits, verifyHits int
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/route-link/reserve":
			reserveHits++
			if r.URL.Query().Get("operation") != "create" || r.Header.Get(HeaderAPIKey) != "e2b_bad" {
				t.Fatalf("create reserve query=%q api-key=%q", r.URL.RawQuery, r.Header.Get(HeaderAPIKey))
			}
			w.WriteHeader(http.StatusForbidden)
		case "/route-link/verify-key":
			verifyHits++
			w.WriteHeader(http.StatusInternalServerError)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
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
	if reserveHits != 1 || verifyHits != 0 {
		t.Fatalf("create auth calls reserve=%d verify-key=%d, want 1/0", reserveHits, verifyHits)
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
