package telemetry

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/app/telemetry/extension"
	customotel "github.com/kuasar-sandbox/orchestrator/app/telemetry/otel"
	"github.com/kuasar-sandbox/orchestrator/config"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver"
)

func startBackendCollector(t testing.TB, exporters map[string]any, names []any, local sampleWriter) consumer.Metrics {
	t.Helper()
	var input consumer.Metrics
	factory := receiver.NewFactory(component.MustNewType("fixture"), func() component.Config { return &emptyConfig{} }, receiver.WithMetrics(func(_ context.Context, _ receiver.Settings, _ component.Config, next consumer.Metrics) (receiver.Metrics, error) {
		input = next
		return &graphReceiver{}, nil
	}, component.StabilityLevelStable))
	cfg, err := config.DecodeTelemetry(strings.NewReader("{}"))
	requireNoQueryError(t, err)
	cfg.Collector = map[string]any{"receivers": map[string]any{"fixture": map[string]any{}}, "exporters": exporters, "service": map[string]any{"telemetry": map[string]any{"metrics": map[string]any{"level": "none"}}, "pipelines": map[string]any{"metrics": map[string]any{"receivers": []any{"fixture"}, "exporters": names}}}}
	collector, err := NewCollector(context.Background(), *cfg, NewView(1), local, customotel.Components{Receivers: []receiver.Factory{factory}}, testLogger(), make(chan error, 1))
	requireNoQueryError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := collector.Shutdown(ctx); err != nil {
			t.Error(err)
		}
	})
	requireNoQueryError(t, collector.Start(context.Background()))
	if input == nil {
		t.Fatal("Collector did not create fixture receiver")
	}
	return input
}
func envdMetricsForTest(t testing.TB, id string, stamp time.Time) pmetric.Metrics {
	t.Helper()
	metrics, err := decodeEnvd(bytes.NewReader(envdJSON()))
	requireNoQueryError(t, err)
	requireNoQueryError(t, enrich(metrics, &target{route: testRoute(id)}, "envd"))
	values := metrics.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics()
	for i := 0; i < values.Len(); i++ {
		values.At(i).Gauge().DataPoints().At(0).SetTimestamp(pcommon.NewTimestampFromTime(stamp))
	}
	return metrics
}
func setEnvdValue(metrics pmetric.Metrics, field e2bField, value float64) {
	metrics.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(int(field)).Gauge().DataPoints().At(0).SetDoubleValue(value)
}
func arbitraryMetricsForTest(id string, stamp time.Time) pmetric.Metrics {
	metrics := pmetric.NewMetrics()
	resource := metrics.ResourceMetrics().AppendEmpty()
	resource.Resource().Attributes().PutStr(SandboxIDAttribute, id)
	resource.Resource().Attributes().PutStr(StableIDAttribute, "stable-"+id)
	resource.Resource().Attributes().PutStr(sourceAttribute, "application")
	resource.Resource().Attributes().PutStr("application.run_id", "keep-this-application-label")
	metric := resource.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	metric.SetName("task.temperature")
	metric.SetUnit("Cel")
	point := metric.SetEmptyGauge().DataPoints().AppendEmpty()
	point.SetTimestamp(pcommon.NewTimestampFromTime(stamp))
	point.SetDoubleValue(-7.25)
	point.Attributes().PutStr("rack", "west")
	return metrics
}

// Exercise the linked exporter's actual number and distribution schemas. This
// fixture is accepted application telemetry, not an additional native stat.
func metricKindsForTest(id string, stamp time.Time) pmetric.Metrics {
	metrics := arbitraryMetricsForTest(id, stamp)
	scope := metrics.ResourceMetrics().At(0).ScopeMetrics().At(0)
	scope.Scope().SetName("schema-fixture")
	scope.Scope().SetVersion("1.0")
	scope.Scope().Attributes().PutStr("region", "west")
	values := scope.Metrics()
	missing := values.At(0).Gauge().DataPoints().AppendEmpty()
	missing.SetTimestamp(pcommon.NewTimestampFromTime(stamp.Add(time.Second)))
	missing.SetDoubleValue(999)
	missing.SetFlags(pmetric.DefaultDataPointFlags.WithNoRecordedValue(true))
	missing.Attributes().PutStr("rack", "west")
	start, end := pcommon.NewTimestampFromTime(stamp.Add(-time.Minute)), pcommon.NewTimestampFromTime(stamp)
	metric := values.AppendEmpty()
	metric.SetName("task.cpu")
	metric.SetUnit("s")
	sum := metric.SetEmptySum()
	sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	sum.SetIsMonotonic(true)
	number := sum.DataPoints().AppendEmpty()
	number.SetStartTimestamp(start)
	number.SetTimestamp(end)
	number.SetDoubleValue(4.5)
	metric = values.AppendEmpty()
	metric.SetName("task.summary")
	metric.SetUnit("ms")
	summary := metric.SetEmptySummary().DataPoints().AppendEmpty()
	summary.SetStartTimestamp(start)
	summary.SetTimestamp(end)
	summary.SetCount(3)
	summary.SetSum(7)
	quantile := summary.QuantileValues().AppendEmpty()
	quantile.SetQuantile(0.5)
	quantile.SetValue(2)
	metric = values.AppendEmpty()
	metric.SetName("task.histogram")
	metric.SetUnit("ms")
	histogram := metric.SetEmptyHistogram()
	histogram.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	hist := histogram.DataPoints().AppendEmpty()
	hist.SetStartTimestamp(start)
	hist.SetTimestamp(end)
	hist.SetCount(3)
	hist.SetSum(7)
	hist.ExplicitBounds().FromRaw([]float64{1, 4})
	hist.BucketCounts().FromRaw([]uint64{1, 1, 1})
	metric = values.AppendEmpty()
	metric.SetName("task.exponential")
	exponential := metric.SetEmptyExponentialHistogram()
	exponential.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	exp := exponential.DataPoints().AppendEmpty()
	exp.SetStartTimestamp(start)
	exp.SetTimestamp(end)
	exp.SetCount(4)
	exp.SetSum(0)
	exp.SetScale(1)
	exp.SetZeroCount(1)
	exp.SetZeroThreshold(0.01)
	exp.Positive().SetOffset(-1)
	exp.Positive().BucketCounts().FromRaw([]uint64{1, 1})
	exp.Negative().SetOffset(0)
	exp.Negative().BucketCounts().FromRaw([]uint64{1})
	return metrics
}
func requireBackendEndpoint(t testing.TB, name string) string {
	t.Helper()
	endpoint := os.Getenv(name)
	if endpoint == "" {
		if os.Getenv("REQUIRE_TELEMETRY_BACKENDS") == "1" {
			t.Fatalf("required real backend missing: %s", name)
		}
		t.Skip("set " + name + " to run the real backend")
	}
	return endpoint
}
func prometheusQueryConfig(endpoint string) config.TelemetryQuery {
	cfg := queryConfigForTest()
	cfg.Backend = "prometheus"
	cfg.Handler = "e2b"
	cfg.Prometheus.Endpoint = endpoint
	for field, name := range cfg.E2B.Metrics {
		cfg.E2B.Metrics[field] = strings.ReplaceAll(name, ".", "_")
	}
	return cfg
}
func e2bSelectionForConfig(id string, cfg config.TelemetryQuery) extension.Selection {
	result := extension.Selection{SandboxID: id, Attributes: map[string]string{sourceAttribute: cfg.E2B.Source}}
	for _, field := range e2bFields {
		result.Metrics = append(result.Metrics, cfg.E2B.Metrics[field])
	}
	return result
}
func clickhouseQueryConfig(endpoint, prefix string) config.TelemetryQuery {
	cfg := queryConfigForTest()
	cfg.Backend = "clickhouse"
	cfg.Handler = "e2b"
	cfg.ClickHouse.Endpoint = endpoint
	cfg.ClickHouse.Tables = config.TelemetryClickHouseTables{Gauge: prefix + "_gauge", Sum: prefix + "_sum", Summary: prefix + "_summary", Histogram: prefix + "_histogram", ExponentialHistogram: prefix + "_exp"}
	return cfg
}
func clickhouseExporterConfig(cfg config.TelemetryQuery) map[string]any {
	tables := cfg.ClickHouse.Tables
	return map[string]any{"endpoint": cfg.ClickHouse.Endpoint, "database": cfg.ClickHouse.Database, "ttl": "1h", "async_insert": false, "sending_queue": map[string]any{"enabled": false}, "metrics_tables": map[string]any{"gauge": map[string]any{"name": tables.Gauge}, "sum": map[string]any{"name": tables.Sum}, "summary": map[string]any{"name": tables.Summary}, "histogram": map[string]any{"name": tables.Histogram}, "exponential_histogram": map[string]any{"name": tables.ExponentialHistogram}}}
}
func cleanupClickHouseTables(t testing.TB, cfg config.TelemetryQuery) {
	t.Helper()
	tables := cfg.ClickHouse.Tables
	for _, table := range []string{tables.Gauge, tables.Sum, tables.Summary, tables.Histogram, tables.ExponentialHistogram} {
		endpoint, _ := url.Parse(cfg.ClickHouse.Endpoint)
		endpoint.RawQuery = url.Values{"query": {"DROP TABLE IF EXISTS `" + cfg.ClickHouse.Database + "`.`" + table + "`"}}.Encode()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		request, err := http.NewRequestWithContext(ctx, "POST", endpoint.String(), nil)
		if err == nil {
			var response *http.Response
			response, err = (&http.Client{Timeout: 5 * time.Second}).Do(request)
			if err == nil {
				response.Body.Close()
				if response.StatusCode != 200 {
					err = fmt.Errorf("drop test table: %d", response.StatusCode)
				}
			}
		}
		cancel()
		if err != nil {
			t.Error(err)
		}
	}
}
