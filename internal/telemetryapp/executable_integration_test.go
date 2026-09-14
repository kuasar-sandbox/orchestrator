package telemetryapp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/app/telemetry/extension"
	customotel "github.com/kuasar-sandbox/orchestrator/app/telemetry/otel"
	"github.com/kuasar-sandbox/orchestrator/config"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/telemetry"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver"
	"gopkg.in/yaml.v3"
)

type executableFixtureReceiver struct{}

func (*executableFixtureReceiver) Start(context.Context, component.Host) error { return nil }
func (*executableFixtureReceiver) Shutdown(context.Context) error              { return nil }

func TestCustomExecutableIntegration(t *testing.T) {
	variables := []string{"TELEMETRY_NODE_TEST_BIN", "TELEMETRY_CUSTOM_TEST_BIN", "TELEMETRY_PROMETHEUS_TEST_URL"}
	for _, name := range variables {
		if os.Getenv(name) == "" {
			if os.Getenv("REQUIRE_TELEMETRY_BACKENDS") == "1" {
				t.Fatalf("required executable/backend missing: %s", name)
			}
			t.Skip("set " + name + " for the actual static executable integration")
		}
	}
	cfg, err := config.LoadTelemetry(filepath.Join("..", "..", "examples", "custom-telemetry", "query.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	paths := testConfig(t)
	cfg.ConfigSocket, cfg.APISocket, cfg.Local.Path = paths.ConfigSocket, paths.APISocket, paths.Local.Path
	cfg.Paths.TelemetryExecutable = os.Getenv("TELEMETRY_CUSTOM_TEST_BIN")
	cfg.Query.Prometheus.Endpoint = os.Getenv("TELEMETRY_PROMETHEUS_TEST_URL")
	cfg.ProxyNetNS = "unused-query-only-namespace"
	if cfg.Collector != nil || cfg.Local.Enabled {
		t.Fatal("example is not query-only")
	}
	if err := config.ValidateTelemetryFinal(cfg); err != nil {
		t.Fatal(err)
	}
	id := fmt.Sprintf("executable-%d-%d", os.Getpid(), time.Now().UnixNano())
	stamp := time.Now().Add(-time.Minute).Truncate(time.Millisecond)
	// Populate the actual engine through a standard Collector exporter. The
	// custom query process below has no receiver, writer or local database.
	var input consumer.Metrics
	factory := receiver.NewFactory(component.MustNewType("fixture"), func() component.Config { return &struct{}{} }, receiver.WithMetrics(func(_ context.Context, _ receiver.Settings, _ component.Config, next consumer.Metrics) (receiver.Metrics, error) {
		input = next
		return &executableFixtureReceiver{}, nil
	}, component.StabilityLevelStable))
	writerCfg := cfg.Clone()
	writerCfg.Collector = map[string]any{
		"receivers": map[string]any{"fixture": map[string]any{}},
		"exporters": map[string]any{"prometheusremotewrite": map[string]any{"endpoint": cfg.Query.Prometheus.Endpoint + "/api/v1/write", "add_metric_suffixes": false, "resource_to_telemetry_conversion": map[string]any{"enabled": true}, "remote_write_queue": map[string]any{"enabled": false}, "target_info": map[string]any{"enabled": false}}},
		"service":   map[string]any{"telemetry": map[string]any{"metrics": map[string]any{"level": "none"}}, "pipelines": map[string]any{"metrics": map[string]any{"receivers": []any{"fixture"}, "exporters": []any{"prometheusremotewrite"}}}},
	}
	writer, err := telemetry.NewCollector(t.Context(), *writerCfg, telemetry.NewView(1), nil, customotel.Components{Receivers: []receiver.Factory{factory}}, logger(), make(chan error, 1))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := writer.Shutdown(context.Background()); err != nil {
			t.Error(err)
		}
	})
	if err := writer.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	metrics := pmetric.NewMetrics()
	resource := metrics.ResourceMetrics().AppendEmpty()
	resource.Resource().Attributes().PutStr("sandbox.id", id)
	resource.Resource().Attributes().PutStr("sandbox.telemetry.source", "application")
	metric := resource.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	metric.SetName("application.queue")
	point := metric.SetEmptyGauge().DataPoints().AppendEmpty()
	point.SetTimestamp(pcommon.NewTimestampFromTime(stamp))
	point.SetIntValue(23)
	if err := input.ConsumeMetrics(t.Context(), metrics); err != nil {
		t.Fatal(err)
	}
	src := &source{route: routesync.RouteEntry{SandboxID: id, StableID: "stable-" + id, State: routesync.StatePaused}, events: make(chan routesync.Event), syncs: make(chan struct{}, 1)}
	registry := servePlugins(t, cfg, src)
	raw, err := yaml.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(filepath.Dir(cfg.ConfigSocket), "query.yaml")
	if err := os.WriteFile(file, raw, 0600); err != nil {
		t.Fatal(err)
	}
	log, err := os.Create(filepath.Join(filepath.Dir(file), "executable.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	cmd := exec.Command(os.Getenv("TELEMETRY_NODE_TEST_BIN"), "telemetry", "serve", "--config", file)
	cmd.Stdout, cmd.Stderr = log, log
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	exited := false
	t.Cleanup(func() {
		if !exited {
			_ = cmd.Process.Signal(syscall.SIGTERM)
			select {
			case err := <-done:
				if err != nil {
					t.Error(err)
				}
			case <-time.After(25 * time.Second):
				_ = cmd.Process.Kill()
				<-done
				t.Error("custom executable leaked")
			}
		}
		if t.Failed() {
			raw, _ := os.ReadFile(log.Name())
			t.Log(string(raw))
		}
	})
	select {
	case <-src.syncs:
	case err := <-done:
		exited = true
		t.Fatal("sealed executable bootstrap", err)
	case <-time.After(5 * time.Second):
		t.Fatal("custom query executable did not register")
	}
	response := httptest.NewRecorder()
	registry.TelemetryAPI().ServeHTTP(response, httptest.NewRequest("GET", "/sandboxes/"+id+"/metrics?metric=application_queue&sid=other", nil))
	var series []extension.Series
	if response.Code != 200 || response.Header().Get("X-Metrics-Contract") != "kuasar-example-series-v1" || json.Unmarshal(response.Body.Bytes(), &series) != nil || len(series) != 1 || len(series[0].Points) != 1 || series[0].Points[0].Value != 23 || series[0].Attributes["sandbox.id"] != id {
		t.Fatal("opaque non-E2B custom query", response.Code, response.Header(), response.Body.String())
	}
	if _, err := os.Stat(cfg.Local.Path); !os.IsNotExist(err) || src.wakes.Load() != 0 {
		t.Fatal("query-only opened local DB or woke sandbox", err)
	}
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		exited = true
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(25 * time.Second):
		t.Fatal("custom executable did not drain")
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		response = httptest.NewRecorder()
		registry.TelemetryAPI().ServeHTTP(response, httptest.NewRequest("GET", "/sandboxes/"+id+"/metrics", nil))
		if response.Code == 503 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("executable disconnect retained query lease")
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Log("actual node-ctl -> sealed custom-telemetry executable -> current plugin lease/query UDS -> real Prometheus: generic custom HTTP, precise SID, paused query, no graph/local DB and clean exit")
}
