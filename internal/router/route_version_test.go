package router

import (
	"encoding/binary"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/cluster/metadata"
)

// Fixture revisions exercise all ten bytes, including carry in the low bytes.
// Production versions come from the stored record, not a client-side counter.
func routeVersionForTest(high uint64, low uint16) metadata.Version {
	var version metadata.Version
	binary.BigEndian.PutUint64(version[:8], high)
	binary.BigEndian.PutUint16(version[8:], low)
	return version
}

func versionedRouteForTest(version metadata.Version, node, state string) routeResolve {
	return routeResolve{
		Group: "/g", RouteKey: "rk", SandboxID: "stable", NodeSandboxID: node,
		RouteRevision: version, State: state,
	}
}

func TestRouteVersionWirePreservesFullRevision(t *testing.T) {
	want := versionedRouteForTest(routeVersionForTest(0x0102030405060708, 0xfffe), "stable-g1", "ready")
	encoded, err := json.Marshal(reserveResult{Route: want})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(encoded), `"route_revision":"0102030405060708fffe"`) {
		t.Fatalf("wire version must contain all 10 bytes: %s", encoded)
	}
	var got reserveResult
	if err := json.Unmarshal(encoded, &got); err != nil {
		t.Fatal(err)
	}
	if got.Route.RouteRevision != want.RouteRevision || got.Route.NodeSandboxID != want.NodeSandboxID {
		t.Fatalf("route wire roundtrip = %+v, want %+v", got.Route, want)
	}
}

func TestRouteVersionWireRejectsMissingOrInvalidRevision(t *testing.T) {
	for _, field := range []string{
		"",
		`,"route_revision":9`,
		`,"route_revision":null`,
		`,"route_revision":"00000000000000000000"`,
		`,"route_revision":"ffffffffffffffffffff"`,
		`,"route_revision":"0102030405060708"`,
		`,"route_revision":"0102030405060708zzzz"`,
	} {
		t.Run(field, func(t *testing.T) {
			encoded := `{"group":"/g","route_key":"rk","sandbox_id":"stable","node_sandbox_id":"stable-g1"` + field + `}`
			before := versionedRouteForTest(routeVersionForTest(5, 1), "stable-g0", "ready")
			got := before
			if err := json.Unmarshal([]byte(encoded), &got); err == nil {
				t.Fatalf("accepted an unfenced route: %s", encoded)
			}
			if got != before {
				t.Fatalf("failed decode partially replaced the route: %+v", got)
			}
		})
	}
}

func TestRouteLinkRejectsUnversionedSuccessfulResponse(t *testing.T) {
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		route := `{"group":"/g","route_key":"rk","sandbox_id":"stable","node_sandbox_id":"stable-g1"}`
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/route-link/v2/reserve" {
			_, _ = w.Write([]byte(`{"route":` + route + `}`))
			return
		}
		_, _ = w.Write([]byte(route))
	}))
	defer control.Close()
	rt := New(strings.TrimPrefix(control.URL, "http://"), "test.local", 0, nil, nil)
	if _, err := rt.routeLinkRoute(t.Context(), "/g", "rk", "stable"); err == nil {
		t.Fatal("resolved an unversioned route")
	}
	if _, err := rt.routeLinkReserve(t.Context(), "data", "/g", "rk", "stable", 0, 0, nil, nil, nil, nil, nil, nil); err == nil {
		t.Fatal("accepted an unversioned reserve response")
	}
	if got := rt.cachedRoute("/g", "rk", "stable"); got != nil {
		t.Fatalf("invalid response entered cache: %+v", got)
	}
}

func TestRouteCacheOrdersLowRevisionBytes(t *testing.T) {
	rt := &Router{cache: map[string]*routeResolve{}}
	old := versionedRouteForTest(routeVersionForTest(100, 255), "stable-g0", "ready")
	fresh := versionedRouteForTest(routeVersionForTest(100, 256), "stable-g1", "ready")
	rt.rememberRoute(&old)
	rt.rememberRoute(&fresh)
	rt.rememberRoute(&old)
	got := rt.cachedRoute("/g", "rk", "stable")
	if got == nil || got.RouteRevision != fresh.RouteRevision || got.NodeSandboxID != fresh.NodeSandboxID {
		t.Fatalf("late response replaced newer revision: %+v", got)
	}
}

func TestRouteCacheRejectsUncommittedVersions(t *testing.T) {
	var incomplete metadata.Version
	for i := range incomplete {
		incomplete[i] = 0xff
	}
	for _, version := range []metadata.Version{{}, incomplete} {
		rt := &Router{cache: map[string]*routeResolve{}}
		invalid := versionedRouteForTest(version, "stable-g2", "ready")
		rt.rememberRoute(&invalid)
		if got := rt.cachedRoute("/g", "rk", "stable"); got != nil {
			t.Fatalf("uncommitted version entered empty cache: %+v", got)
		}
		fresh := versionedRouteForTest(routeVersionForTest(100, 1), "stable-g1", "ready")
		rt.rememberRoute(&fresh)
		rt.rememberRoute(&invalid)
		if got := rt.cachedRoute("/g", "rk", "stable"); got == nil || got.RouteRevision != fresh.RouteRevision {
			t.Fatalf("uncommitted version replaced valid route: %+v", got)
		}
	}
}

func TestRouteCachePreservesNewStateAndAllowsSameVersionRefresh(t *testing.T) {
	rt := &Router{cache: map[string]*routeResolve{}}
	old := versionedRouteForTest(routeVersionForTest(100, 1), "stable-g0", "pending")
	fresh := versionedRouteForTest(routeVersionForTest(101, 0), "stable-g0", "ready")
	rt.rememberRoute(&fresh)
	rt.rememberRoute(&old)
	if got := rt.cachedRoute("/g", "rk", "stable"); got == nil || got.State != fresh.State {
		t.Fatalf("late state replaced current generation: %+v", got)
	}
	refresh := fresh
	refresh.APIEndpoint = "new-endpoint:9443"
	rt.rememberRoute(&refresh)
	if got := rt.cachedRoute("/g", "rk", "stable"); got == nil || got.APIEndpoint != refresh.APIEndpoint {
		t.Fatalf("same version/target refresh was lost: %+v", got)
	}
}

func TestRouteCacheConcurrentResponsesKeepFullVersionOrder(t *testing.T) {
	rt := &Router{cache: map[string]*routeResolve{}}
	var wg sync.WaitGroup
	for i := 0; i < 128; i++ {
		wg.Add(1)
		go func(batch uint16) {
			defer wg.Done()
			route := versionedRouteForTest(routeVersionForTest(1000, batch), "stable-g0", "ready")
			rt.rememberRoute(&route)
		}(uint16(i))
	}
	wg.Wait()
	if got := rt.cachedRoute("/g", "rk", "stable"); got == nil || got.RouteRevision != routeVersionForTest(1000, 127) {
		t.Fatalf("concurrent response order regressed cache: %+v", got)
	}
}
