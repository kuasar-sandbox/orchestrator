package telemetry

import (
	"errors"
	"math"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

func TestCanonicalScalarMappingAndUnsignedTimestamp(t *testing.T) {
	data := pmetric.NewMetrics()
	resource := data.ResourceMetrics().AppendEmpty()
	metrics := resource.ScopeMetrics().AppendEmpty().Metrics()
	// A valid OTLP unsigned timestamp beyond signed UnixNano's 2262 boundary.
	stamp := time.Date(2299, 1, 2, 3, 4, 5, 123456789, time.UTC)
	when := pcommon.Timestamp(uint64(stamp.Unix())*uint64(time.Second) + uint64(stamp.Nanosecond()))
	gauge := metrics.AppendEmpty()
	gauge.SetName("custom.gauge")
	g := gauge.SetEmptyGauge().DataPoints().AppendEmpty()
	g.SetTimestamp(when)
	g.SetIntValue(42)
	sum := metrics.AppendEmpty()
	sum.SetName("custom.sum")
	sum.SetEmptySum().SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
	sum.Sum().SetIsMonotonic(true)
	s := sum.Sum().DataPoints().AppendEmpty()
	s.SetTimestamp(when)
	s.SetDoubleValue(2.5)
	hist := metrics.AppendEmpty()
	hist.SetName("custom.histogram")
	h := hist.SetEmptyHistogram().DataPoints().AppendEmpty()
	h.SetTimestamp(when)
	h.SetCount(3)
	h.SetSum(4)
	h.ExplicitBounds().FromRaw([]float64{1, 2})
	h.BucketCounts().FromRaw([]uint64{1, 1, 1})
	exp := metrics.AppendEmpty()
	exp.SetName("custom.exponential")
	e := exp.SetEmptyExponentialHistogram().DataPoints().AppendEmpty()
	e.SetTimestamp(when)
	e.SetCount(3)
	e.SetSum(0)
	e.SetZeroCount(1)
	e.SetScale(0)
	e.Positive().BucketCounts().FromRaw([]uint64{1})
	e.Negative().BucketCounts().FromRaw([]uint64{1})
	summary := metrics.AppendEmpty()
	summary.SetName("custom.summary")
	su := summary.SetEmptySummary().DataPoints().AppendEmpty()
	su.SetTimestamp(when)
	su.SetCount(3)
	su.SetSum(6)
	q := su.QuantileValues().AppendEmpty()
	q.SetQuantile(.5)
	q.SetValue(2)
	if err := enrich(data, &target{route: testRoute("sid")}, "otlp"); err != nil {
		t.Fatal(err)
	}
	points, err := canonicalSamples(data)
	if err != nil || len(points) != 16 {
		t.Fatal("mapping", len(points), err)
	}
	seen := map[string]float64{}
	for _, point := range points {
		if !point.Timestamp.Equal(stamp) || point.Labels[SandboxIDAttribute] != "sid" {
			t.Fatal("timestamp or identity lost", point)
		}
		seen[point.Metric+":"+point.Labels["otel.part"]+":"+point.Labels["otel.bound"]] = point.Value
		if point.Metric == "custom.sum" && (point.Labels["otel.temporality"] != "Delta" || point.Labels["otel.monotonic"] != "true") {
			t.Fatal("sum temporality lost")
		}
	}
	for key, want := range map[string]float64{"custom.gauge::": 42, "custom.sum::": 2.5,
		"custom.histogram:bucket:1": 1, "custom.histogram:bucket:2": 2, "custom.histogram:bucket:+Inf": 3,
		"custom.exponential:bucket:-1": 1, "custom.exponential:bucket:0": 2, "custom.exponential:bucket:2": 3,
		"custom.summary:quantile:0.5": 2} {
		if got, ok := seen[key]; !ok || got != want {
			t.Errorf("%s = %v, want %v", key, got, want)
		}
	}
	h.BucketCounts().SetAt(2, 2)
	if _, err := canonicalSamples(data); !errors.Is(err, ErrInvalidMetrics) {
		t.Fatal("inconsistent histogram count accepted", err)
	}
	h.BucketCounts().SetAt(2, 1)
	g.SetDoubleValue(math.NaN())
	if _, err := canonicalSamples(data); !errors.Is(err, ErrInvalidMetrics) {
		t.Fatal("NaN accepted", err)
	}
}
