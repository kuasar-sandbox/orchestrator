package telemetry

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/app/telemetry/extension"
)

type prometheusTestTransport func(*http.Request) (*http.Response, error)

func (f prometheusTestTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// Writes go through the linked standard Collector exporter into a real engine.
// The test endpoint must be disposable with remote-write enabled and >=5m OOO.
func TestPrometheusIntegration(t *testing.T) {
	endpoint := requireBackendEndpoint(t, "TELEMETRY_PROMETHEUS_TEST_URL")
	cfg := prometheusQueryConfig(endpoint)
	backend, err := NewPrometheus(cfg)
	requireNoQueryError(t, err)
	defer backend.Shutdown(context.Background())
	input := startBackendCollector(t, map[string]any{"prometheusremotewrite": map[string]any{"endpoint": endpoint + "/api/v1/write", "add_metric_suffixes": false, "resource_to_telemetry_conversion": map[string]any{"enabled": true}, "remote_write_queue": map[string]any{"enabled": false}, "target_info": map[string]any{"enabled": false}}}, []any{"prometheusremotewrite"}, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	id := fmt.Sprintf("telemetry-test-%d-%d", os.Getpid(), time.Now().UnixNano())
	stamp := time.Now().Add(-time.Minute).Truncate(5 * time.Second)
	first, second := envdMetricsForTest(t, id, stamp.Add(123*time.Millisecond)), envdMetricsForTest(t, id, stamp.Add(2123*time.Millisecond))
	setEnvdValue(first, e2bCPUUsedPct, 70)
	setEnvdValue(second, e2bCPUUsedPct, 20)
	setEnvdValue(first, e2bMemCache, 100)
	setEnvdValue(second, e2bMemCache, 300)
	requireNoQueryError(t, input.ConsumeMetrics(ctx, second))
	requireNoQueryError(t, input.ConsumeMetrics(ctx, first))
	requireNoQueryError(t, input.ConsumeMetrics(ctx, first))
	requireNoQueryError(t, input.ConsumeMetrics(ctx, envdMetricsForTest(t, id+"-other", stamp)))
	baseTransport := backend.client.Transport
	var streams int
	backend.client.Transport = prometheusTestTransport(func(r *http.Request) (*http.Response, error) {
		response, err := baseTransport.RoundTrip(r)
		if err == nil && r.URL.Path == "/api/v1/read" {
			if response.Header.Get("Content-Type") != "application/x-streamed-protobuf; proto=prometheus.ChunkedReadResponse" {
				t.Error("real engine did not negotiate streaming", response.Header)
			}
			streams++
		}
		return response, err
	})
	defer func() { backend.client.Transport = baseTransport }()
	selection := e2bSelectionForConfig(id, cfg)
	start, end, found, err := backend.Bounds(ctx, selection)
	if err != nil || !found || !start.Equal(stamp.Add(123*time.Millisecond)) || !end.Equal(stamp.Add(2123*time.Millisecond)) {
		t.Fatal("streamed bounds/millisecond precision", start, end, found, err)
	}
	query := extension.Query{Selection: selection, Start: start, End: end, Step: 5 * time.Second, Aggregation: extension.Max}
	series, err := backend.Query(ctx, query)
	requireNoQueryError(t, err)
	result, err := aggregateSeriesForTest(series, query)
	if err != nil || len(result) != 1 || result[0].CPUUsedPct != 70 || result[0].MemCache != 300 {
		t.Fatal("independent E2B MAX", result, err)
	}
	if _, _, found, err := backend.Bounds(ctx, e2bSelectionForConfig("stable-"+id, cfg)); err != nil || found {
		t.Fatal("StableID fallback", found, err)
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
		t.Fatal("gap extended the last Gauge", series, err)
	}
	requireNoQueryError(t, input.ConsumeMetrics(ctx, arbitraryMetricsForTest(id, stamp)))
	generic := extension.Query{Selection: extension.Selection{SandboxID: id, Metrics: []string{"task_temperature"}, Attributes: map[string]string{sourceAttribute: "application", "rack": "west"}}, Start: stamp, End: stamp, Aggregation: extension.Raw}
	series, err = backend.Query(ctx, generic)
	if err != nil || len(series) != 1 || len(series[0].Points) != 1 || series[0].Points[0].Value != -7.25 || series[0].Attributes["application_run_id"] != "keep-this-application-label" {
		t.Fatal("arbitrary metric/source/negative Gauge/attributes", series, err)
	}
	requireMissingAttributeUnmatched(t, backend, generic)
	if streams < 6 {
		t.Fatal("expected real remote read requests", streams)
	}
	t.Log("standard PRW exporter -> real Prometheus streamed Reader: transformed names/resource labels, exact SID, raw/MAX edges, gaps, duplicates/OOO, arbitrary source and negative Gauge")
}

func TestPrometheusMetricKindsIntegration(t *testing.T) {
	endpoint := requireBackendEndpoint(t, "TELEMETRY_PROMETHEUS_TEST_URL")
	cfg := prometheusQueryConfig(endpoint)
	backend, err := NewPrometheus(cfg)
	requireNoQueryError(t, err)
	defer backend.Shutdown(context.Background())
	input := startBackendCollector(t, map[string]any{"prometheusremotewrite": map[string]any{"endpoint": endpoint + "/api/v1/write", "add_metric_suffixes": true, "resource_to_telemetry_conversion": map[string]any{"enabled": true}, "remote_write_queue": map[string]any{"enabled": false}, "target_info": map[string]any{"enabled": false}}}, []any{"prometheusremotewrite"}, nil)
	id := fmt.Sprintf("kinds-%d-%d", os.Getpid(), time.Now().UnixNano())
	stamp := time.Now().Add(-time.Minute).Truncate(time.Millisecond)
	requireNoQueryError(t, input.ConsumeMetrics(t.Context(), metricKindsForTest(id, stamp)))
	query := extension.Query{Selection: extension.Selection{SandboxID: id}, Start: stamp, End: stamp.Add(time.Second), Aggregation: extension.Raw}
	series, err := backend.Query(t.Context(), query)
	requireNoQueryError(t, err)
	want := map[string]float64{
		"task_temperature_celsius": -7.25, "task_cpu_seconds_total": 4.5,
		"task_summary_milliseconds_count": 3, "task_summary_milliseconds_sum": 7,
		"task_summary_milliseconds": 2, "task_histogram_milliseconds_count": 3,
		"task_histogram_milliseconds_sum": 7,
	}
	buckets, native := 0, 0
	for _, result := range series {
		if len(result.Points) != 1 || !result.Points[0].Timestamp.Equal(stamp) {
			t.Fatalf("actual time/NoRecordedValue: %+v", result)
		}
		if number, ok := want[result.Metric]; ok {
			if result.Points[0].Value != number {
				t.Fatalf("unit/type conversion: %+v", result)
			}
			delete(want, result.Metric)
			continue
		}
		switch result.Metric {
		case "task_histogram_milliseconds_bucket":
			buckets++
			want := map[string]float64{"1": 1, "4": 2, "+Inf": 3}[result.Attributes["le"]]
			if want == 0 || result.Points[0].Value != want {
				t.Fatalf("classic bucket: %+v", result)
			}
		case "task_exponential":
			native++
			switch result.Attributes["otel.part"] {
			case "count":
				if result.Points[0].Value != 4 {
					t.Fatal(result)
				}
			case "sum":
				if result.Points[0].Value != 0 {
					t.Fatal(result)
				}
			case "native_bucket":
				if result.Points[0].Value != 1 || result.Attributes["otel.bound"] == "" {
					t.Fatal(result)
				}
			default:
				t.Fatalf("native histogram part: %+v", result)
			}
		default:
			t.Fatalf("unexpected physical metric: %+v", result)
		}
	}
	if len(want) != 0 || buckets != 3 || native != 6 {
		t.Fatal("standard suffixes/schema conversion", want, buckets, native, series)
	}
	query.Selection.Metrics = []string{"task_exponential"}
	query.Selection.Attributes = map[string]string{"otel.part": "count"}
	series, err = backend.Query(t.Context(), query)
	if err != nil || len(series) != 1 || series[0].Points[0].Value != 4 {
		t.Fatal("native histogram scalar attribute selection", series, err)
	}
	query.Selection.Attributes = nil
	query.Start = stamp.Add(time.Second)
	if series, err := backend.Query(t.Context(), query); err != nil || len(series) != 0 {
		t.Fatal("native histogram extended into gap", series, err)
	}
	t.Log("standard suffix-enabled exporter: Gauge, Sum, Summary, classic and native histograms; physical units/names, stale flags, raw times and scalar parts verified")
}
