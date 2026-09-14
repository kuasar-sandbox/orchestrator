package telemetry

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	customotel "github.com/kuasar-sandbox/orchestrator/app/telemetry/otel"
	"github.com/kuasar-sandbox/orchestrator/config"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/exporter"
	collectorextension "go.opentelemetry.io/collector/extension"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver"
)

type graphProvider struct{ stops atomic.Int32 }

func (*graphProvider) Scheme() string { return "fixture" }
func (*graphProvider) Retrieve(_ context.Context, uri string, _ confmap.WatcherFunc) (*confmap.Retrieved, error) {
	if uri != "fixture:receivers" {
		return nil, fmt.Errorf("unexpected provider URI %s", uri)
	}
	return confmap.NewRetrieved(map[string]any{"envd/fast": map[string]any{"collection_interval": "1s"}, "envd/slow": map[string]any{"collection_interval": "2s"}})
}
func (p *graphProvider) Shutdown(context.Context) error { p.stops.Add(1); return nil }

type graphConverter struct{ calls atomic.Int32 }

func (c *graphConverter) Convert(_ context.Context, conf *confmap.Conf) error {
	c.calls.Add(1)
	return conf.Merge(confmap.NewFromStringMap(map[string]any{"service": map[string]any{"telemetry": map[string]any{"metrics": map[string]any{"level": "none"}}}}))
}

type graphExtension struct{ starts, stops atomic.Int32 }

func (e *graphExtension) Start(context.Context, component.Host) error { e.starts.Add(1); return nil }
func (e *graphExtension) Shutdown(context.Context) error              { e.stops.Add(1); return nil }

func TestCollectorNativeProvidersExtensionsConnectorsAndEnvdInstances(t *testing.T) {
	view := NewView(1)
	defer view.InvalidateSync()
	route := testRoute("same-sandbox")
	route.EnvdUDS = unixHTTP(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _ = json.NewEncoder(w).Encode(envdBody()) }))
	upsert(t, view, route)
	view.Bookmark()
	provider, converter, extension := &graphProvider{}, &graphConverter{}, &graphExtension{}
	seen := make(chan string, 32)
	custom := customotel.Components{
		Providers:  []confmap.ProviderFactory{confmap.NewProviderFactory(func(confmap.ProviderSettings) confmap.Provider { return provider })},
		Converters: []confmap.ConverterFactory{confmap.NewConverterFactory(func(confmap.ConverterSettings) confmap.Converter { return converter })},
		Extensions: []collectorextension.Factory{collectorextension.NewFactory(component.MustNewType("observe_lifecycle"), func() component.Config { return &emptyConfig{} }, func(context.Context, collectorextension.Settings, component.Config) (collectorextension.Extension, error) {
			return extension, nil
		}, component.StabilityLevelStable)},
		Connectors: []connector.Factory{connector.NewFactory(component.MustNewType("bridge"), func() component.Config { return &emptyConfig{} }, connector.WithMetricsToMetrics(func(_ context.Context, _ connector.Settings, _ component.Config, next consumer.Metrics) (connector.Metrics, error) {
			return &testExporter{Metrics: next}, nil
		}, component.StabilityLevelStable))},
		Exporters: []exporter.Factory{exporter.NewFactory(component.MustNewType("capture"), func() component.Config { return &emptyConfig{} }, exporter.WithMetrics(func(_ context.Context, settings exporter.Settings, _ component.Config) (exporter.Metrics, error) {
			return &testExporter{Metrics: metricsConsumer(t, func(_ context.Context, m pmetric.Metrics) error {
				if m.DataPointCount() != 7 {
					t.Error("envd instance dropped fields")
				}
				id, _ := m.ResourceMetrics().At(0).Resource().Attributes().Get(SandboxIDAttribute)
				if id.Str() != route.SandboxID {
					t.Error("wrong instance identity")
				}
				seen <- settings.ID.String()
				return nil
			})}, nil
		}, component.StabilityLevelStable))},
	}
	cfg, err := config.DecodeTelemetry(strings.NewReader(`collector:
  receivers: ${fixture:receivers}
  connectors: {bridge: {}}
  exporters: {capture/fast: {}, capture/slow: {}}
  extensions: {observe_lifecycle: {}}
  service:
    extensions: [observe_lifecycle]
    pipelines:
      metrics/fast: {receivers: [envd/fast], exporters: [bridge]}
      metrics/bridge: {receivers: [bridge], exporters: [capture/fast]}
      metrics/slow: {receivers: [envd/slow], exporters: [capture/slow]}
`))
	if err != nil {
		t.Fatal(err)
	}
	collector, err := NewCollector(t.Context(), *cfg, view, nil, custom, testLogger(), make(chan error, 1))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := collector.Shutdown(ctx); err != nil {
			t.Error(err)
		}
		if extension.starts.Load() != 1 || extension.stops.Load() != 1 || provider.stops.Load() != 1 || converter.calls.Load() == 0 {
			t.Error("extension/provider/converter lifecycle not honored")
		}
	})
	if err := collector.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	counts := map[string]int{}
	deadline := time.After(5 * time.Second)
	for counts["capture/fast"] < 2 || counts["capture/slow"] < 2 {
		select {
		case id := <-seen:
			counts[id]++
		case <-deadline:
			t.Fatal("same receiver type instances did not collect independently", counts)
		}
	}
}

func TestCollectorNativeConfigErrorsAndFactoryDuplicates(t *testing.T) {
	for _, raw := range []string{
		`collector: {receivers: {envd: {bogus: true}}, exporters: {debug: {}}, service: {pipelines: {metrics: {receivers: [envd], exporters: [debug]}}}}`,
		`collector: {receivers: {envd: {concurrency: 0}}, exporters: {debug: {}}, service: {pipelines: {metrics: {receivers: [envd], exporters: [debug]}}}}`,
		`collector: {receivers: {sandboxotlp: {grpc_listen: broken}}, exporters: {debug: {}}, service: {pipelines: {metrics: {receivers: [sandboxotlp], exporters: [debug]}}}}`,
		`collector: {receivers: {sandboxstats: {resource_interval: 0s, traffic_interval: 0s, usage_interval: 0s}}, exporters: {debug: {}}, service: {pipelines: {metrics: {receivers: [sandboxstats], exporters: [debug]}}}}`,
		`collector: {receivers: {envd: {}}, exporters: {debug: {}}, service: {pipelines: {metrics: {receivers: [envd/missing], exporters: [debug]}}}}`,
		`collector: {receivers: {envd: {}}, exporters: {debug: {}}, service: {pipelines: {logs: {receivers: [envd], exporters: [debug]}}}}`,
	} {
		cfg, err := config.DecodeTelemetry(strings.NewReader(raw))
		if err != nil {
			t.Fatal(err)
		}
		graph, err := NewCollector(t.Context(), *cfg, NewView(1), nil, customotel.Components{}, testLogger(), make(chan error, 1))
		if graph != nil {
			_ = graph.Shutdown(context.Background())
		}
		if err == nil {
			t.Fatal("invalid native Collector graph accepted", raw)
		}
	}
	view := NewView(1)
	if _, err := collectorFactories(config.Telemetry{}, view, nil, customotel.Components{Receivers: []receiver.Factory{envdFactory(view, testLogger())}}, testLogger(), make(chan error, 1)); err == nil {
		t.Fatal("duplicate receiver factory type accepted")
	}
	if _, err := factoryMap([]receiver.Factory{nil}); err == nil {
		t.Fatal("nil factory accepted")
	}
}
