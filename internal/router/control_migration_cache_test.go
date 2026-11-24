package router

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestControlMigrationInvalidatesPreviousDataTarget(t *testing.T) {
	for _, operation := range []string{"create", "connect", "exec-session"} {
		for _, target := range []string{"migrated", "unchanged", "newer-cache"} {
			t.Run(operation+"/"+target, func(t *testing.T) {
				result := routerTestConnectReserveResult(t, "stable", "/g", "rk", "new-node:1234", types.ProfileE2B)
				result.Route.NodeID = "node-new"
				result.Route.NodeSandboxID = "stable-g1"
				result.Route.RouteRevision = routeVersionForTest(20, 0)
				result.Connect.NodeSandboxID = result.Route.NodeSandboxID
				result.ExecSession = &execSessionResult{ExecAccessToken: "kat1.result"}
				control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path != "/route-link/v2/reserve" || r.URL.Query().Get("operation") != operation {
						t.Errorf("unexpected control request: %s", r.URL)
						w.WriteHeader(http.StatusNotFound)
						return
					}
					_ = json.NewEncoder(w).Encode(result)
				}))
				defer control.Close()
				rt := New(strings.TrimPrefix(control.URL, "http://"), "test.local", 0, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
				cached := routerTestRouteResolve(t, "stable", "/g", "rk", "old-node:1234", types.ProfileE2B)
				cached.NodeID, cached.NodeSandboxID = "node-old", "stable-g0"
				cached.RouteRevision = routeVersionForTest(10, 0)
				if target == "unchanged" {
					cached.NodeID, cached.NodeSandboxID = result.Route.NodeID, result.Route.NodeSandboxID
				} else if target == "newer-cache" {
					cached.NodeID, cached.NodeSandboxID = "node-newest", "stable-g2"
					cached.RouteRevision = routeVersionForTest(30, 0)
				}
				rt.rememberRoute(&cached)
				path, status := "/sandboxes", http.StatusCreated
				if operation == "connect" {
					path, status = "/sandboxes/stable/connect", http.StatusOK
				} else if operation == "exec-session" {
					path = "/sandboxes/stable/exec-sessions"
				}
				req := httptest.NewRequest(http.MethodPost, "http://api.test.local"+path, strings.NewReader(`{}`))
				req.Header.Set(HeaderGroup, "/g")
				req.Header.Set(HeaderRouteKey, "rk")
				req.Header.Set(HeaderAPIKey, "api-key")
				response := httptest.NewRecorder()
				rt.Handler().ServeHTTP(response, req)
				if response.Code != status {
					t.Fatalf("status=%d, body=%s", response.Code, response.Body)
				}
				got := rt.cachedRoute("/g", "rk", "stable")
				if target == "migrated" {
					if got != nil {
						t.Fatal("successful migration retained the previous data target")
					}
				} else if got == nil || got.NodeSandboxID != cached.NodeSandboxID || got.ServiceSecret != cached.ServiceSecret {
					t.Fatal("control response replaced an unchanged or newer data target")
				}
			})
		}
	}
}
