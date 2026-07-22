package registry

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
)

func TestServeRouteLinkOmitsRuntimeSnapshotEndpoints(t *testing.T) {
	mux := http.NewServeMux()
	New(NewStores(), nil, 0, nil).ServeRouteLink(mux)

	for _, tc := range []struct {
		method string
		path   string
	}{
		{method: http.MethodGet, path: "/route-link/export"},
		{method: http.MethodPost, path: "/route-link/import"},
	} {
		req := httptest.NewRequest(tc.method, tc.path, nil)
		if _, pattern := mux.Handler(req); pattern != "" {
			t.Fatalf("%s remains mounted as %q", tc.path, pattern)
		}
	}

	req := httptest.NewRequest(http.MethodGet, RouteLinkListPath, nil)
	if _, pattern := mux.Handler(req); pattern != RouteLinkListPath {
		t.Fatalf("normal route-link API pattern=%q, want %q", pattern, RouteLinkListPath)
	}
}

func TestServeReserveCarriesCreateConfig(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	reg.SetPlacer(placementWithToken("n1"))
	if err := reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1"}); err != nil {
		t.Fatal(err)
	}
	var gotConfig map[string]string
	reg.addNode(&fakeConn{nodeID: "n1", onCmd: func(cmd *routesync.Command) {
		if cmd.Kind != routesync.CmdCreate {
			return
		}
		gotConfig = cloneStringMap(cmd.Config)
		go reg.ackCommand(&routesync.CmdAck{CmdID: cmd.CmdID, Status: routesync.AckAccepted})
		go reg.applyRoute(context.Background(), "n1", &routesync.RouteEntry{
			SandboxID: cmd.SID, State: routesync.StateRunning, AccessToken: cmd.AccessToken,
		})
	}})

	body, err := json.Marshal(ReserveSandboxRequest{Config: map[string]string{
		sandboxcfg.NsRestore: `{"prefetch":"memory"}`,
		"application":        "kept",
	}})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, RouteLinkReservePath+"?group=%2Fg&route_key=rk", strings.NewReader(string(body)))
	w := httptest.NewRecorder()
	reg.serveReserve(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	if gotConfig[sandboxcfg.NsRestore] != `{"prefetch":"memory"}` || gotConfig["application"] != "kept" {
		t.Fatalf("node create config=%v", gotConfig)
	}
}

func TestServeReserveRejectsInvalidRestore(t *testing.T) {
	reg := New(NewStores(), nil, 0, nil)
	body := `{"config":{"kuasar-sandbox.restore":"{\"prefetch\":\"disk\"}"}}`
	req := httptest.NewRequest(http.MethodPost, RouteLinkReservePath+"?group=%2Fg&route_key=rk", strings.NewReader(body))
	w := httptest.NewRecorder()
	reg.serveReserve(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s, want 400", w.Code, w.Body.String())
	}
}
