package main

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/proxystats"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestExternalTrafficProviderUsesRegisteredMasterCache(t *testing.T) {
	registry := configsock.NewRegistry()
	provider := &externalTrafficProvider{plugins: registry}
	if _, err := provider.SandboxTrafficStats(context.Background(), "s1", "run-1", types.ProfileBare, types.StateRunning); !errors.Is(err, api.ErrStatsUnavailable) {
		t.Fatalf("missing registration error=%v", err)
	}

	socket := filepath.Join(t.TempDir(), "proxy-stats.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != proxystats.BatchPath {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		var request proxystats.BatchRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil || len(request.Sandboxes) != 1 {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		query := request.Sandboxes[0]
		if query.SandboxID != "s1" || query.RunID != "run-1" || query.Profile != types.ProfileBare || query.State != types.StateRunning {
			http.Error(w, "identity mismatch", http.StatusServiceUnavailable)
			return
		}
		_ = json.NewEncoder(w).Encode(proxystats.BatchResponse{Sandboxes: []proxystats.BatchResult{{
			SandboxID: "s1",
			Stats: &api.TrafficStats{
				State:    string(types.StatePaused),
				Inflight: api.TrafficInflight{Parking: 1},
				Services: map[string]api.ServiceTrafficStats{
					"forward": {Parking: 1},
					"exec":    {},
				},
			},
		}}})
	})}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	plugin := &configsock.Plugin{ID: routesync.ProxyPluginID, Caps: routesync.Register{Proxy: &routesync.Proxy{
		StatsSocket: &routesync.Socket{Path: socket},
	}}}
	registry.Add(plugin)
	stats, err := provider.SandboxTrafficStats(context.Background(), "s1", "run-1", types.ProfileBare, types.StateRunning)
	if err != nil || stats.State != string(types.StatePaused) || stats.Services["forward"].Parking != 1 {
		t.Fatalf("external traffic stats=%+v err=%v", stats, err)
	}
	registry.Remove(plugin)
	if _, err := provider.SandboxTrafficStats(context.Background(), "s1", "run-1", types.ProfileBare, types.StateRunning); !errors.Is(err, api.ErrStatsUnavailable) {
		t.Fatalf("removed registration error=%v", err)
	}
	if err := server.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil && !errors.Is(err, http.ErrServerClosed) {
		t.Fatal(err)
	}
}

func TestProxyConfigSkeletonRendersStatsSocket(t *testing.T) {
	path := filepath.Join(t.TempDir(), "proxy.yaml")
	if err := os.WriteFile(path, []byte(proxyConfigSkeleton), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.LoadProxy(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.StatsSocket != "/run/sandbox/proxy-stats.sock" {
		t.Fatalf("rendered stats_socket=%q", cfg.StatsSocket)
	}
}
