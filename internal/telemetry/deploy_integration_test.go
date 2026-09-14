package telemetry

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/app/telemetry/extension"
	customotel "github.com/kuasar-sandbox/orchestrator/app/telemetry/otel"
	"github.com/kuasar-sandbox/orchestrator/config"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
)

func TestRemoteDeploymentExamplesIntegration(t *testing.T) {
	for _, kind := range []string{"prometheus", "clickhouse"} {
		t.Run(kind, func(t *testing.T) {
			variable := "TELEMETRY_PROMETHEUS_TEST_URL"
			if kind == "clickhouse" {
				variable = "TELEMETRY_CLICKHOUSE_TEST_URL"
			}
			endpoint := requireBackendEndpoint(t, variable)
			cfg, err := config.LoadTelemetry(filepath.Join("..", "..", "deploy", "telemetry-"+kind+".example.yaml"))
			requireNoQueryError(t, err)
			id := fmt.Sprintf("deploy-%d-%d", os.Getpid(), time.Now().UnixNano())
			exporters := cfg.Collector["exporters"].(map[string]any)
			var backend extension.QueryBackend
			metric := "task.temperature"
			if kind == "prometheus" {
				metric = "task_temperature"
				cfg.Query.Prometheus.Endpoint = endpoint
				exporters["prometheusremotewrite"].(map[string]any)["endpoint"] = endpoint + "/api/v1/write"
				backend, err = NewPrometheus(cfg.Query)
			} else {
				cfg.Query.ClickHouse.Endpoint = endpoint
				tables := clickhouseQueryConfig(endpoint, fmt.Sprintf("deploy_%d_%d", os.Getpid(), time.Now().UnixNano())).ClickHouse.Tables
				cfg.Query.ClickHouse.Tables = tables
				t.Cleanup(func() { cleanupClickHouseTables(t, cfg.Query) })
				exporter := exporters["clickhouse"].(map[string]any)
				exporter["endpoint"] = endpoint
				exporter["metrics_tables"] = clickhouseExporterConfig(cfg.Query)["metrics_tables"]
				backend, err = NewClickHouse(cfg.Query)
			}
			requireNoQueryError(t, err)
			t.Cleanup(func() { requireNoQueryError(t, backend.Shutdown(context.Background())) })
			// Add the optional local sink to the actual deployment pipeline. Read
			// selection remains remote, independent of this extra writer.
			cfg.Local.Enabled, cfg.Local.Path = true, filepath.Join(t.TempDir(), "db")
			exporters["sandboxlocal"] = map[string]any{}
			pipeline := cfg.Collector["service"].(map[string]any)["pipelines"].(map[string]any)["metrics"].(map[string]any)
			pipeline["exporters"] = append(pipeline["exporters"].([]any), "sandboxlocal")
			local, err := OpenLocal(cfg.Local, testLogger())
			requireNoQueryError(t, err)
			t.Cleanup(func() { requireNoQueryError(t, local.Shutdown(context.Background())) })
			listener, err := net.Listen("tcp4", "127.0.0.1:0")
			requireNoQueryError(t, err)
			address := listener.Addr().String()
			requireNoQueryError(t, listener.Close())
			otlp := cfg.Collector["receivers"].(map[string]any)["sandboxotlp"].(map[string]any)
			otlp["http_listen"], otlp["grpc_listen"] = address, "127.0.0.1:0"
			requireNoQueryError(t, config.ValidateTelemetryFinal(cfg))
			view := NewView(1)
			defer view.InvalidateSync()
			route := testRoute(id)
			route.EnvdUDS = ""
			upsert(t, view, route)
			view.Bookmark()
			collector, err := NewCollector(t.Context(), *cfg, view, local, customotel.Components{}, testLogger(), make(chan error, 1))
			requireNoQueryError(t, err)
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				requireNoQueryError(t, collector.Shutdown(ctx))
			})
			requireNoQueryError(t, collector.Start(t.Context()))
			stamp := time.Now().Add(-time.Minute).Truncate(time.Millisecond)
			raw, err := pmetricotlp.NewExportRequestFromMetrics(arbitraryMetricsForTest("forged-sandbox", stamp)).MarshalProto()
			requireNoQueryError(t, err)
			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			defer cancel()
			request, err := http.NewRequestWithContext(ctx, "POST", "http://"+address+"/v1/metrics", bytes.NewReader(raw))
			requireNoQueryError(t, err)
			request.Header.Set("Content-Type", "application/x-protobuf")
			transport := &http.Transport{}
			defer transport.CloseIdleConnections()
			response, err := (&http.Client{Transport: transport}).Do(request)
			requireNoQueryError(t, err)
			body, err := io.ReadAll(response.Body)
			response.Body.Close()
			requireNoQueryError(t, err)
			if response.StatusCode != http.StatusOK {
				t.Fatalf("sandbox OTLP: %d %s", response.StatusCode, body)
			}
			view.ApplyDelete(id) // Already accepted data must survive the batch.
			query := extension.Query{Selection: extension.Selection{SandboxID: id, Metrics: []string{metric}, Attributes: map[string]string{sourceAttribute: "otlp"}}, Start: stamp, End: stamp, Aggregation: extension.Raw}
			var series []extension.Series
			for {
				series, err = backend.Query(ctx, query)
				if err != nil || len(series) != 0 {
					break
				}
				select {
				case <-ctx.Done():
					t.Fatal("deployment did not export queued sample", ctx.Err())
				case <-time.After(20 * time.Millisecond):
				}
			}
			requireNoQueryError(t, err)
			if len(series) != 1 || len(series[0].Points) != 1 || series[0].Points[0].Value != -7.25 || series[0].Attributes[StableIDAttribute] != "stable-"+id {
				t.Fatal("accepted identity/name/value lost after delete", series)
			}
			query.Metrics = []string{"task.temperature"}
			series, err = local.Query(ctx, query)
			if err != nil || len(series) != 1 || len(series[0].Points) != 1 || series[0].Points[0].Value != -7.25 {
				t.Fatal("local and remote fan-out diverged", series, err)
			}
			if kind == "prometheus" {
				readOnly, err := config.LoadTelemetry(filepath.Join("..", "..", "deploy", "telemetry-query-only.example.yaml"))
				requireNoQueryError(t, err)
				readOnly.Query.Prometheus.Endpoint = endpoint
				requireNoQueryError(t, config.ValidateTelemetryFinal(readOnly))
				if readOnly.Local.Enabled || readOnly.Collector != nil {
					t.Fatal("query-only example opens a writer")
				}
				reader, err := NewPrometheus(readOnly.Query)
				requireNoQueryError(t, err)
				defer reader.Shutdown(context.Background())
				query.Metrics = []string{metric}
				series, err = reader.Query(ctx, query)
				if err != nil || len(series) != 1 {
					t.Fatal("separately configured query-only Reader", series, err)
				}
			}
			t.Log("real deployment parsed and started: sandbox OTLP -> native batch/queue -> local + standard remote exporter; accepted identity survives deletion; explicit remote read")
		})
	}
}
