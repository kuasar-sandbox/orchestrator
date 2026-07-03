package main

import (
	"github.com/kuasar-sandbox/orchestrator/internal/registry"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
)

func TestRegistryExportDefaultKindUsesRouteLink(t *testing.T) {
	reg := registry.New(registry.NewStores(), nil, 0, slog.New(slog.NewTextHandler(io.Discard, nil)))
	mux := http.NewServeMux()
	reg.ServeRouteLink(mux)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	cfgPath := writeRegistryAdminTestConfig(t, srv.URL)
	outPath := t.TempDir() + "/route_link.jsonl"
	if err := registryExportCmd([]string{"--config", cfgPath, "--group", "/g", "-o", outPath}); err != nil {
		t.Fatalf("registry export default kind failed: %v", err)
	}
}

func writeRegistryAdminTestConfig(t *testing.T, endpoint string) string {
	t.Helper()
	path := t.TempDir() + "/registry.yaml"
	cfg := `member:
  id: registry
  listen: "` + endpoint + `"
membership:
  active: 1
  versions:
    - version: 1
      members:
        - { id: registry, advertise: "` + endpoint + `" }
  owners:
    route_link: 1
    node_link: 1
    placer_link: 1
    node_list: 1
`
	if err := os.WriteFile(path, []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
