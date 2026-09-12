package telemetry

import (
	"context"
	"errors"
	"maps"
	"math"
	"sort"
	"strconv"
	"time"

	"github.com/kuasar-sandbox/orchestrator/app/telemetry/extension"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// primaryFactory is the only bridge from Collector pdata to primary storage.
// Receivers do not receive a Storage handle. The backend's lifetime is owned by
// the app so it outlives both the Collector and outstanding history readers.
func primaryFactory(backend extension.Storage) exporter.Factory {
	return exporter.NewFactory(component.MustNewType("sandboxstorage"), func() component.Config { return &emptyConfig{} },
		exporter.WithMetrics(func(context.Context, exporter.Settings, component.Config) (exporter.Metrics, error) {
			return &primaryExporter{backend: backend}, nil
		}, component.StabilityLevelStable))
}

type primaryExporter struct{ backend extension.Storage }

func (*primaryExporter) Start(context.Context, component.Host) error { return nil }
func (*primaryExporter) Shutdown(context.Context) error              { return nil }
func (*primaryExporter) Capabilities() consumer.Capabilities         { return consumer.Capabilities{} }
func (e *primaryExporter) ConsumeMetrics(ctx context.Context, metrics pmetric.Metrics) error {
	samples, err := canonicalSamples(metrics)
	if err != nil {
		return consumererror.NewPermanent(err)
	}
	return e.backend.Write(ctx, samples)
}

const maxWriteSamples = 65536

// Prometheus 3 supports UTF-8 names: keep OTel metric names verbatim. Attribute
// namespaces avoid collisions without a lossy punctuation-to-underscore map.
// Histogram components use kind/part labels instead of colliding with user
// metrics named e.g. foo_count. Temporality is explicit, never silently changed.
func canonicalSamples(metrics pmetric.Metrics) ([]extension.Sample, error) {
	var out []extension.Sample
	resources := metrics.ResourceMetrics()
	for i := 0; i < resources.Len(); i++ {
		rm := resources.At(i)
		base := make(map[string]string)
		for _, key := range []string{SandboxIDAttribute, StableIDAttribute, sourceAttribute} {
			value, ok := rm.Resource().Attributes().Get(key)
			if !ok || value.Type() != pcommon.ValueTypeStr || value.Str() == "" {
				return nil, ErrIdentity
			}
			base[key] = value.Str()
		}
		rm.Resource().Attributes().Range(func(key string, value pcommon.Value) bool {
			if key != SandboxIDAttribute && key != StableIDAttribute && key != sourceAttribute {
				base["resource."+key] = value.AsString()
			}
			return true
		})
		for j := 0; j < rm.ScopeMetrics().Len(); j++ {
			scope := rm.ScopeMetrics().At(j)
			scopeLabels := maps.Clone(base)
			if len(scope.Scope().Name()) > 256 || len(scope.Scope().Version()) > 256 {
				return nil, ErrInvalidMetrics
			}
			scopeLabels["otel.scope.name"] = scope.Scope().Name()
			scopeLabels["otel.scope.version"] = scope.Scope().Version()
			scope.Scope().Attributes().Range(func(key string, value pcommon.Value) bool { scopeLabels["scope."+key] = value.AsString(); return true })
			for k := 0; k < scope.Metrics().Len(); k++ {
				metric := scope.Metrics().At(k)
				metricLabels := maps.Clone(scopeLabels)
				metricLabels["otel.kind"] = metric.Type().String()
				if len(metric.Unit()) > 64 {
					return nil, ErrInvalidMetrics
				}
				metricLabels["otel.unit"] = metric.Unit()
				appendValue := func(stamp pcommon.Timestamp, attrs pcommon.Map, part, bound string, value float64) error {
					if len(out) >= maxWriteSamples || stamp == 0 || math.IsNaN(value) || math.IsInf(value, 0) {
						return ErrInvalidMetrics
					}
					ls := maps.Clone(metricLabels)
					attrs.Range(func(key string, value pcommon.Value) bool { ls["point."+key] = value.AsString(); return true })
					if part != "" {
						ls["otel.part"] = part
					}
					if bound != "" {
						ls["otel.bound"] = bound
					}
					// pdata's AsTime casts nanoseconds to int64, which wraps after
					// 2262. Preserve the unsigned OTLP timestamp through seconds.
					timestamp := time.Unix(int64(uint64(stamp)/uint64(time.Second)), int64(uint64(stamp)%uint64(time.Second))).UTC()
					out = append(out, extension.Sample{Metric: metric.Name(), Labels: ls, Timestamp: timestamp, Value: value})
					return nil
				}
				numbers := func(points pmetric.NumberDataPointSlice) error {
					for n := 0; n < points.Len(); n++ {
						p := points.At(n)
						if p.Flags().NoRecordedValue() {
							continue
						}
						var value float64
						switch p.ValueType() {
						case pmetric.NumberDataPointValueTypeInt:
							value = float64(p.IntValue())
						case pmetric.NumberDataPointValueTypeDouble:
							value = p.DoubleValue()
						default:
							return ErrInvalidMetrics
						}
						if err := appendValue(p.Timestamp(), p.Attributes(), "", "", value); err != nil {
							return err
						}
					}
					return nil
				}
				var err error
				switch metric.Type() {
				case pmetric.MetricTypeGauge:
					err = numbers(metric.Gauge().DataPoints())
				case pmetric.MetricTypeSum:
					metricLabels["otel.temporality"] = metric.Sum().AggregationTemporality().String()
					metricLabels["otel.monotonic"] = strconv.FormatBool(metric.Sum().IsMonotonic())
					err = numbers(metric.Sum().DataPoints())
				case pmetric.MetricTypeHistogram:
					metricLabels["otel.temporality"] = metric.Histogram().AggregationTemporality().String()
					for n := 0; n < metric.Histogram().DataPoints().Len() && err == nil; n++ {
						p := metric.Histogram().DataPoints().At(n)
						if p.Flags().NoRecordedValue() {
							continue
						}
						buckets := make([]histogramBucket, p.ExplicitBounds().Len())
						if len(buckets) > 160 || p.BucketCounts().Len() != len(buckets)+1 {
							return nil, ErrInvalidMetrics
						}
						remaining := p.Count()
						for b := 0; b < p.BucketCounts().Len(); b++ {
							if p.BucketCounts().At(b) > remaining {
								return nil, ErrInvalidMetrics
							}
							remaining -= p.BucketCounts().At(b)
						}
						if remaining != 0 {
							return nil, ErrInvalidMetrics
						}
						for b := range buckets {
							buckets[b] = histogramBucket{p.ExplicitBounds().At(b), p.BucketCounts().At(b)}
						}
						err = histogramSamples(p.Timestamp(), p.Attributes(), buckets, p.Count(), p.HasSum(), p.Sum(), appendValue)
					}
				case pmetric.MetricTypeExponentialHistogram:
					metricLabels["otel.temporality"] = metric.ExponentialHistogram().AggregationTemporality().String()
					for n := 0; n < metric.ExponentialHistogram().DataPoints().Len() && err == nil; n++ {
						p := metric.ExponentialHistogram().DataPoints().At(n)
						if p.Flags().NoRecordedValue() {
							continue
						}
						buckets, e := exponentialBuckets(p)
						if e != nil {
							return nil, e
						}
						err = histogramSamples(p.Timestamp(), p.Attributes(), buckets, p.Count(), p.HasSum(), p.Sum(), appendValue)
					}
				case pmetric.MetricTypeSummary:
					for n := 0; n < metric.Summary().DataPoints().Len() && err == nil; n++ {
						p := metric.Summary().DataPoints().At(n)
						if p.Flags().NoRecordedValue() {
							continue
						}
						if p.QuantileValues().Len() > 160 {
							return nil, ErrInvalidMetrics
						}
						err = appendValue(p.Timestamp(), p.Attributes(), "count", "", float64(p.Count()))
						if err == nil {
							err = appendValue(p.Timestamp(), p.Attributes(), "sum", "", p.Sum())
						}
						for b := 0; b < p.QuantileValues().Len() && err == nil; b++ {
							q := p.QuantileValues().At(b)
							if math.IsNaN(q.Quantile()) || q.Quantile() < 0 || q.Quantile() > 1 {
								return nil, ErrInvalidMetrics
							}
							err = appendValue(p.Timestamp(), p.Attributes(), "quantile", strconv.FormatFloat(q.Quantile(), 'g', -1, 64), q.Value())
						}
					}
				default:
					return nil, ErrInvalidMetrics
				}
				if err != nil {
					return nil, err
				}
			}
		}
	}
	return out, nil
}

type histogramBucket struct {
	upper float64
	count uint64
}
type appendScalar func(pcommon.Timestamp, pcommon.Map, string, string, float64) error

func histogramSamples(stamp pcommon.Timestamp, attrs pcommon.Map, buckets []histogramBucket, count uint64, hasSum bool, sum float64, appendValue appendScalar) error {
	if err := appendValue(stamp, attrs, "count", "", float64(count)); err != nil {
		return err
	}
	if hasSum {
		if err := appendValue(stamp, attrs, "sum", "", sum); err != nil {
			return err
		}
	}
	var cumulative uint64
	last := math.Inf(-1)
	for _, b := range buckets {
		if math.IsNaN(b.upper) || math.IsInf(b.upper, 0) || b.upper <= last || b.count > count-cumulative {
			return ErrInvalidMetrics
		}
		last = b.upper
		cumulative += b.count
		if err := appendValue(stamp, attrs, "bucket", strconv.FormatFloat(b.upper, 'g', -1, 64), float64(cumulative)); err != nil {
			return err
		}
	}
	return appendValue(stamp, attrs, "bucket", "+Inf", float64(count))
}

func exponentialBuckets(p pmetric.ExponentialHistogramDataPoint) ([]histogramBucket, error) {
	if p.Positive().BucketCounts().Len()+p.Negative().BucketCounts().Len() > 160 || p.Scale() < -10 || p.Scale() > 20 || p.ZeroThreshold() < 0 || math.IsNaN(p.ZeroThreshold()) {
		return nil, ErrInvalidMetrics
	}
	var out []histogramBucket
	factor := math.Ldexp(1, -int(p.Scale()))
	for _, side := range []struct {
		buckets  pmetric.ExponentialHistogramDataPointBuckets
		negative bool
	}{{p.Negative(), true}, {p.Positive(), false}} {
		for b := 0; b < side.buckets.BucketCounts().Len(); b++ {
			index := int64(side.buckets.Offset()) + int64(b)
			upper := math.Exp2(float64(index+1) * factor)
			if side.negative {
				upper = -math.Exp2(float64(index) * factor)
			}
			if math.IsInf(upper, 0) || upper == 0 {
				return nil, errors.New("exponential histogram bounds exceed float precision")
			}
			out = append(out, histogramBucket{upper, side.buckets.BucketCounts().At(b)})
		}
	}
	if p.ZeroCount() != 0 {
		out = append(out, histogramBucket{p.ZeroThreshold(), p.ZeroCount()})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].upper < out[j].upper })
	remaining := p.Count()
	for _, bucket := range out {
		if bucket.count > remaining {
			return nil, ErrInvalidMetrics
		}
		remaining -= bucket.count
	}
	if remaining != 0 {
		return nil, ErrInvalidMetrics
	}
	return out, nil
}
