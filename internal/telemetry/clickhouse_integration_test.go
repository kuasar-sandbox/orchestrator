package telemetry

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/app/telemetry/extension"
)

func TestClickHouseIntegration(t *testing.T) {
	endpoint := requireBackendEndpoint(t, "TELEMETRY_CLICKHOUSE_TEST_URL")
	cfg := clickhouseQueryConfig(endpoint, fmt.Sprintf("telemetry_%d_%d", os.Getpid(), time.Now().UnixNano()))
	// Cleanup is test ownership, never a query Reader operation.
	t.Cleanup(func() { cleanupClickHouseTables(t, cfg) })
	input := startBackendCollector(t, map[string]any{"clickhouse": clickhouseExporterConfig(cfg)}, []any{"clickhouse"}, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	backend, err := NewClickHouse(cfg)
	requireNoQueryError(t, err)
	defer backend.Shutdown(context.Background())
	stamp := time.Now().Add(-time.Minute).Truncate(5 * time.Second)
	first, second := envdMetricsForTest(t, "sid", stamp.Add(123*time.Millisecond)), envdMetricsForTest(t, "sid", stamp.Add(2123*time.Millisecond))
	setEnvdValue(first, e2bCPUUsedPct, 70)
	setEnvdValue(second, e2bCPUUsedPct, 20)
	setEnvdValue(first, e2bMemCache, 100)
	setEnvdValue(second, e2bMemCache, 300)
	requireNoQueryError(t, input.ConsumeMetrics(ctx, second))
	requireNoQueryError(t, input.ConsumeMetrics(ctx, first))
	requireNoQueryError(t, input.ConsumeMetrics(ctx, first))
	requireNoQueryError(t, input.ConsumeMetrics(ctx, envdMetricsForTest(t, "other", stamp)))
	requireNoQueryError(t, backend.Shutdown(ctx))
	backend, err = NewClickHouse(cfg)
	requireNoQueryError(t, err)
	defer backend.Shutdown(context.Background())
	selection := envdSelection("sid")
	start, end, found, err := backend.Bounds(ctx, selection)
	if err != nil || !found || !start.Equal(stamp.Add(123*time.Millisecond)) || !end.Equal(stamp.Add(2123*time.Millisecond)) {
		t.Fatal("reopen/bounds/millisecond precision", start, end, found, err)
	}
	if _, _, found, err := backend.Bounds(ctx, envdSelection("stable-sid")); err != nil || found {
		t.Fatal("StableID fallback", err)
	}
	query := extension.Query{Selection: selection, Start: start, End: end, Step: 5 * time.Second, Aggregation: extension.Max}
	series, err := backend.Query(ctx, query)
	requireNoQueryError(t, err)
	result, err := aggregateSeriesForTest(series, query)
	if err != nil || len(result) != 1 || result[0].CPUUsedPct != 70 || result[0].MemCache != 300 {
		t.Fatal("independent E2B MAX", result, err)
	}
	query.Start, query.End = end, end
	series, err = backend.Query(ctx, query)
	requireNoQueryError(t, err)
	result, err = aggregateSeriesForTest(series, query)
	if err != nil || len(result) != 1 || result[0].CPUUsedPct != 20 {
		t.Fatal("raw point boundary used lookback", result, err)
	}
	query.Start, query.End = stamp.Add(3*time.Second), stamp.Add(15*time.Second)
	if series, err := backend.Query(ctx, query); err != nil || len(series) != 0 {
		t.Fatal("gap extended last Gauge", series, err)
	}
	requireNoQueryError(t, input.ConsumeMetrics(ctx, arbitraryMetricsForTest("sid", stamp)))
	generic := extension.Query{Selection: extension.Selection{SandboxID: "sid", Metrics: []string{"task.temperature"}, Attributes: map[string]string{sourceAttribute: "application", "point.rack": "west"}}, Start: stamp, End: stamp, Aggregation: extension.Raw}
	series, err = backend.Query(ctx, generic)
	if err != nil || len(series) != 1 || len(series[0].Points) != 1 || series[0].Points[0].Value != -7.25 || series[0].Attributes["resource.application.run_id"] != "keep-this-application-label" {
		t.Fatal("arbitrary metric/source/negative Gauge/native attributes", series, err)
	}
	generic.Attributes["otel.kind"] = "absent-kind"
	if series, err := backend.Query(ctx, generic); err != nil || len(series) != 0 {
		t.Fatal("absent attribute selection must return empty history", series, err)
	}
	if _, _, found, err := backend.Bounds(ctx, generic.Selection); err != nil || found {
		t.Fatal("absent attribute selection must have no bounds", found, err)
	}
	t.Log("standard ClickHouse exporter -> real native schema Reader: exact SID, raw/MAX edges, gaps, duplicate/OOO inserts, arbitrary source/negative Gauge and attributes; Reader creates no schema")
}

func TestClickHouseMetricKindsIntegration(t *testing.T) {
	endpoint := requireBackendEndpoint(t, "TELEMETRY_CLICKHOUSE_TEST_URL")
	cfg := clickhouseQueryConfig(endpoint, fmt.Sprintf("kinds_%d_%d", os.Getpid(), time.Now().UnixNano()))
	t.Cleanup(func() { cleanupClickHouseTables(t, cfg) })
	input := startBackendCollector(t, map[string]any{"clickhouse": clickhouseExporterConfig(cfg)}, []any{"clickhouse"}, nil)
	backend, err := NewClickHouse(cfg)
	requireNoQueryError(t, err)
	defer backend.Shutdown(context.Background())
	stamp := time.Now().Add(-time.Minute).Truncate(time.Millisecond)
	requireNoQueryError(t, input.ConsumeMetrics(t.Context(), metricKindsForTest("sid", stamp)))
	query := extension.Query{Selection: extension.Selection{SandboxID: "sid"}, Start: stamp, End: stamp.Add(time.Second), Aggregation: extension.Raw}
	series, err := backend.Query(t.Context(), query)
	requireNoQueryError(t, err)
	want := map[string]float64{
		"task.temperature||": -7.25, "task.cpu||": 4.5,
		"task.summary|count|": 3, "task.summary|sum|": 7, "task.summary|quantile|0.5": 2,
		"task.histogram|count|": 3, "task.histogram|bucket|1": 1, "task.histogram|bucket|4": 2, "task.histogram|bucket|+Inf": 3,
		"task.exponential|count|": 4, "task.exponential|zero_count|": 1,
		"task.exponential|positive_bucket|-1": 1, "task.exponential|positive_bucket|0": 1, "task.exponential|negative_bucket|0": 1,
	}
	if len(series) != len(want) {
		t.Fatalf("five native schemas: got %d series, want %d: %+v", len(series), len(want), series)
	}
	for _, result := range series {
		key := result.Metric + "|" + result.Attributes["otel.part"] + "|" + result.Attributes["otel.bound"]
		value, ok := want[key]
		if !ok || len(result.Points) != 1 || result.Points[0].Value != value || !result.Points[0].Timestamp.Equal(stamp) {
			t.Fatalf("schema projection/NoRecordedValue: %s %+v", key, result)
		}
		if result.Attributes["otel.scope.name"] != "schema-fixture" || result.Attributes["otel.scope.version"] != "1.0" || result.Attributes["scope.region"] != "west" {
			t.Fatalf("scope labels: %+v", result)
		}
		if result.Metric == "task.cpu" && (result.Attributes["otel.monotonic"] != "true" || result.Attributes["otel.temporality"] != "Cumulative" || result.Attributes["otel.unit"] != "s") {
			t.Fatalf("sum semantics: %+v", result)
		}
		if result.Metric == "task.exponential" && result.Attributes["otel.scale"] != "1" {
			t.Fatalf("stored exponential scale: %+v", result)
		}
		delete(want, key)
	}
	query.Selection.Metrics = []string{"task.temperature"}
	query.Start = stamp.Add(time.Second)
	series, err = backend.Query(t.Context(), query)
	if err != nil || len(series) != 0 {
		t.Fatal("NoRecordedValue became an observation", series, err)
	}
	t.Log("all five standard schemas, finite scalar parts, scope/resource labels, temporality, NoRecordedValue and actual timestamps verified; absent HasSum/ZeroThreshold facts are not fabricated")
}
