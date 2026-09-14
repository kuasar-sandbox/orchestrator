package telemetry

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/app/telemetry/extension"
	customotel "github.com/kuasar-sandbox/orchestrator/app/telemetry/otel"
	"github.com/kuasar-sandbox/orchestrator/config"
)

func TestCollectorDeploymentExamplesStart(t *testing.T) {
	for _, file := range []string{"telemetry.example.yaml", "telemetry-fanout.example.yaml"} {
		t.Run(file, func(t *testing.T) {
			sink := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/x-protobuf")
			}))
			defer sink.Close()
			t.Setenv("OTLP_ENVD_ENDPOINT", sink.URL)
			t.Setenv("OTLP_NATIVE_ENDPOINT", sink.URL)
			cfg, err := config.LoadTelemetry(filepath.Join("..", "..", "deploy", file))
			if err != nil {
				t.Fatal(err)
			}
			// Test-owned paths/ports replace deployment host resources. The graph,
			// native options and provider references are read from the real file.
			cfg.Telemetry.Storage.Path = filepath.Join(t.TempDir(), "db")
			otlp := cfg.Collector["receivers"].(map[string]any)["sandboxotlp"].(map[string]any)
			otlp["http_listen"], otlp["grpc_listen"] = "127.0.0.1:0", "127.0.0.1:0"
			if err := config.ValidateTelemetryFinal(cfg); err != nil {
				t.Fatal(err)
			}
			var backend extension.Storage
			if cfg.Telemetry.Storage.Type == "local" {
				backend, err = OpenLocal(cfg.Telemetry.Storage, testLogger())
				if err != nil {
					t.Fatal(err)
				}
				defer backend.Shutdown(context.Background())
			}
			collector, err := NewCollector(t.Context(), *cfg, NewView(1), backend, customotel.Components{}, testLogger(), make(chan error, 1))
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if err := collector.Shutdown(ctx); err != nil {
					t.Error(err)
				}
			}()
			if err := collector.Start(t.Context()); err != nil {
				t.Fatal(err)
			}
		})
	}
}
