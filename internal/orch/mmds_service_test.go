package orch

import (
	"context"
	"net"
	"net/http"
	"path/filepath"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestInternalMMDSServiceRouteUsesConductorRegistry(t *testing.T) {
	socket := filepath.Join(t.TempDir(), "service.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("E2b-Sandbox-Id") != "sandbox-1" || r.Header.Get("E2b-Sandbox-Service") != "local" {
			t.Errorf("service identity headers mismatch")
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"internal":true}`))
	})}
	go server.Serve(listener)
	defer server.Close()

	cfg := mmdsFeatureConfig()
	cfg.MMDS.Services = map[string]config.MMDSServiceRegistryEntry{
		"local": {Endpoint: "unix://" + socket},
	}
	o := testOrchCfg(t, cfg)
	sb := &types.Sandbox{
		ID: "sandbox-1", Profile: types.ProfileE2B, State: types.StateRunning, RunID: "run-1",
		Metadata: map[string]string{sandboxcfg.NsMMDS: `{"routes":[{"path":"/service","type":"service","service":"local"}]}`},
	}
	o.cache(sb)
	route, found, err := o.MMDSRoute(context.Background(), sb.ID, "/service")
	if err != nil || !found || route.StatusCode != http.StatusAccepted || route.ContentType != "application/json" || string(route.Body) != `{"internal":true}` {
		t.Fatalf("internal service route metadata mismatch: found=%t status=%d content-type=%q err=%v", found, route.StatusCode, route.ContentType, err)
	}
}

func TestInternalMMDSMissingServiceIs503(t *testing.T) {
	o := testOrchCfg(t, mmdsFeatureConfig())
	sb := &types.Sandbox{
		ID: "sandbox-1", Profile: types.ProfileE2B, State: types.StateRunning, RunID: "run-1",
		Metadata: map[string]string{sandboxcfg.NsMMDS: `{"routes":[{"path":"/service","type":"service","service":"missing"}]}`},
	}
	o.cache(sb)
	route, found, err := o.MMDSRoute(context.Background(), sb.ID, "/service")
	if err != nil || !found || route.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("missing service route: found=%t status=%d err=%v", found, route.StatusCode, err)
	}
}
