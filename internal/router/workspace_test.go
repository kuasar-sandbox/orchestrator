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
	"github.com/kuasar-sandbox/orchestrator/internal/registry"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/orchestrator/internal/workspace"
)

func TestWorkspaceRouterKeepsSourcesOutOfPlacementConfig(t *testing.T) {
	const raw = `[{"name":"w","mounts":[{"path":"/data"}],"disk":{"migrationToken":"kwt1.private"}}]`
	for _, path := range []string{"/sandboxes", "/v3/templates"} {
		client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			if r.URL.Path == registry.RouteLinkVerifyKeyPath {
				return textResponse(200, ""), nil
			}
			var entries []workspace.Entry
			var meta map[string]string
			var body []byte
			if path == "/sandboxes" {
				var wire registry.SandboxReserveReq
				if err := json.NewDecoder(r.Body).Decode(&wire); err != nil {
					t.Fatal(err)
				}
				entries, meta = wire.Workspaces, wire.Config
				body, _ = json.Marshal(routerTestReserveResult(t, "s", "/g", "r", "node:1", types.ProfileBare))
			} else {
				var wire registry.BuildReserveReq
				if err := json.NewDecoder(r.Body).Decode(&wire); err != nil {
					t.Fatal(err)
				}
				entries, meta = wire.Workspaces, wire.Metadata
				body = []byte(`{"build_id":"b","template_id":"t","profile":"bare"}`)
			}
			if len(entries) != 1 || !strings.Contains(string(entries[0].Disk), "kwt1.private") || meta[workspace.Namespace] != `[{"name":"w","mounts":[{"path":"/data"}]}]` {
				t.Fatalf("private envelope lost or leaked %s", path)
			}
			return textResponse(200, string(body)), nil
		})}
		rt := New("unused:80", "test.local", 0, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
		rt.routeRegistry = routeRegistryFunc(func(context.Context, string) ([]clusterclient.Endpoint, error) {
			return []clusterclient.Endpoint{{MemberID: "r1", BaseURL: "http://r1", Client: client}}, nil
		})
		r := httptest.NewRequest("POST", path, strings.NewReader(`{"metadata":{"kuasar-sandbox.workspace":"shadowed"},"cpuCount":1,"memoryMB":256}`))
		r.Host = "api.test.local"
		r.Header.Set(workspace.Header, raw)
		r.Header.Set(HeaderGroup, "/g")
		r.Header.Set(HeaderRouteKey, "r")
		r.Header.Set(HeaderAPIKey, "key")
		w := httptest.NewRecorder()
		rt.Handler().ServeHTTP(w, r)
		if (w.Code != 201 && w.Code != 202) || strings.Contains(w.Body.String(), "kwt1.private") {
			t.Fatalf("%s: %d %s", path, w.Code, w.Body)
		}
	}
}
