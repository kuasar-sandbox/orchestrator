package telemetry

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/app/telemetry/extension"
	customotel "github.com/kuasar-sandbox/orchestrator/app/telemetry/otel"
	"github.com/kuasar-sandbox/orchestrator/config"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor"
)

type observedStorage struct {
	extension.Reader
	writes chan []extension.Sample
}

func (s *observedStorage) Write(_ context.Context, samples []extension.Sample) error {
	s.writes <- samples
	return nil
}
func (*observedStorage) Shutdown(context.Context) error { return nil }

type testProcessor struct {
	consumer.Metrics
	starts, stops *atomic.Int32
}

func (p *testProcessor) Start(context.Context, component.Host) error { p.starts.Add(1); return nil }
func (p *testProcessor) Shutdown(context.Context) error              { p.stops.Add(1); return nil }

type testExporter struct{ consumer.Metrics }

func (*testExporter) Start(context.Context, component.Host) error { return nil }
func (*testExporter) Shutdown(context.Context) error              { return nil }

func TestCollectorAcceptsBeforeDetachedContextAndRouteDeletion(t *testing.T) {
	for _, dropContext := range []bool{false, true} {
		t.Run(map[bool]string{false: "preserve", true: "context-loss"}[dropContext], func(t *testing.T) {
			cfg, err := config.DecodeTelemetry(bytes.NewReader([]byte("{}")))
			if err != nil {
				t.Fatal(err)
			}
			view := NewView(1)
			defer view.InvalidateSync()
			route := testRoute("sid")
			route.EnvdUDS = unixHTTP(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _ = json.NewEncoder(w).Encode(envdBody()) }))
			upsert(t, view, route)
			view.Bookmark()
			backend := &observedStorage{writes: make(chan []extension.Sample, 8)}
			processed, exported := make(chan error, 8), make(chan pmetric.Metrics, 8)
			var starts, stops atomic.Int32
			factory := processor.NewFactory(component.MustNewType("privateprocessor"), func() component.Config { return &emptyConfig{} },
				processor.WithMetrics(func(_ context.Context, _ processor.Settings, _ component.Config, next consumer.Metrics) (processor.Metrics, error) {
					c, err := consumer.NewMetrics(func(ctx context.Context, m pmetric.Metrics) error {
						attrs := m.ResourceMetrics().At(0).Resource().Attributes()
						attrs.PutStr("application.run_id", "application-owned")
						view.ApplyDelete("sid")
						if dropContext {
							ctx = context.Background()
						}
						err := next.ConsumeMetrics(ctx, m)
						processed <- err
						return err
					}, consumer.WithCapabilities(consumer.Capabilities{MutatesData: true}))
					return &testProcessor{Metrics: c, starts: &starts, stops: &stops}, err
				}, component.StabilityLevelStable))
			fanout := exporter.NewFactory(component.MustNewType("privateexporter"), func() component.Config { return &emptyConfig{} },
				exporter.WithMetrics(func(context.Context, exporter.Settings, component.Config) (exporter.Metrics, error) {
					return &testExporter{metricsConsumer(t, func(_ context.Context, m pmetric.Metrics) error {
						copy := pmetric.NewMetrics()
						m.CopyTo(copy)
						exported <- copy
						return nil
					})}, nil
				}, component.StabilityLevelStable))
			cfg.Collector = map[string]any{
				"receivers":  map[string]any{"envd": map[string]any{"collection_interval": "1s"}},
				"processors": map[string]any{"privateprocessor": map[string]any{}},
				"exporters":  map[string]any{"sandboxlocal": map[string]any{}, "privateexporter": map[string]any{}},
				"service": map[string]any{"telemetry": map[string]any{"metrics": map[string]any{"level": "none"}}, "pipelines": map[string]any{"metrics": map[string]any{
					"receivers": []any{"envd"}, "processors": []any{"privateprocessor"}, "exporters": []any{"sandboxlocal", "privateexporter"},
				}}},
			}
			collector, err := NewCollector(context.Background(), *cfg, view, backend, customotel.Components{
				Processors: []processor.Factory{factory}, Exporters: []exporter.Factory{fanout},
			}, testLogger(), make(chan error, 1))
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				if err := collector.Shutdown(ctx); err != nil {
					t.Error(err)
				}
				if starts.Load() != 1 || stops.Load() != 1 {
					t.Error("custom component lifecycle", starts.Load(), stops.Load())
				}
			}()
			if err := collector.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-processed:
				if err != nil {
					t.Fatal("accepted sample delivery", err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("custom pipeline did not run")
			}
			samples := <-backend.writes
			if len(samples) != 7 {
				t.Fatal("Collector storage mapping", len(samples))
			}
			for _, sample := range samples {
				if sample.Labels[SandboxIDAttribute] != "sid" || sample.Labels[StableIDAttribute] != "stable-sid" {
					t.Fatal("forged stored identity", sample.Labels)
				}
				for key := range sample.Labels {
					if forbiddenAttribute(key) {
						t.Fatal("run identity stored")
					}
				}
			}
			attrs := (<-exported).ResourceMetrics().At(0).Resource().Attributes()
			if id, _ := attrs.Get(SandboxIDAttribute); id.Str() != "sid" {
				t.Fatal("forged exported identity")
			}
		})
	}
}
