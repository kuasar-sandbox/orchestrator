package telemetry

import (
	"context"
	"errors"
	"unicode/utf8"

	"github.com/prometheus/otlptranslator"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

var ErrInvalidMetrics = errors.New("telemetry: invalid or oversized metrics")

type identityContextKey struct{}

type ingressIdentity struct {
	entry  *target
	source string
}

const sourceAttribute = "sandbox.telemetry.source"

func withIdentity(ctx context.Context, entry *target, source string) context.Context {
	return context.WithValue(ctx, identityContextKey{}, ingressIdentity{entry: entry, source: source})
}

type emptyConfig struct{}

// acceptMetrics linearizes bounded identity validation and pdata enrichment
// with route invalidation. It never calls a downstream component under the view
// lock. Once accepted, identity belongs to each pdata resource, independent of
// the request context and any later route pause, replacement or deletion.
func (v *View) acceptMetrics(ctx context.Context, entry *target, source string, metrics pmetric.Metrics) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return v.withCurrent(entry, func() error { return enrich(metrics, entry, source) })
}

// These exact reserved spellings are obsolete platform run identity. Application
// attributes such as application.run_id are unrelated and must be retained.
func forbiddenAttribute(key string) bool {
	switch key {
	case "sandbox.run_id", "run_id", "runId", "RunID":
		return true
	default:
		return false
	}
}

func reservedIdentityAttribute(key string) bool {
	switch key {
	case SandboxIDAttribute, StableIDAttribute, sourceAttribute:
		return true
	}
	// Standard exporters can normalize punctuation and merge colliding label
	// values. Reserve those spellings at acceptance too, before any async work.
	// Collapsing repeated underscores covers both linked normalization modes;
	// the trusted canonical labels contain only single separators.
	namer := otlptranslator.LabelNamer{}
	normalized, err := namer.Build(key)
	if err != nil {
		return false
	}
	switch normalized {
	case "sandbox_id", "sandbox_stable_id", "sandbox_telemetry_source":
		return true
	default:
		return false
	}
}

func boundedAttributes(attrs pcommon.Map, removeIdentity bool) error {
	attrs.RemoveIf(func(key string, _ pcommon.Value) bool {
		return forbiddenAttribute(key) || (removeIdentity && reservedIdentityAttribute(key))
	})
	if attrs.Len() > 32 {
		return ErrInvalidMetrics
	}
	var invalid bool
	attrs.Range(func(key string, value pcommon.Value) bool {
		if len(key) > 128 || !utf8.ValidString(key) || len(value.AsString()) > 256 {
			invalid = true
			return false
		}
		return true
	})
	if invalid {
		return ErrInvalidMetrics
	}
	return nil
}

func enrich(metrics pmetric.Metrics, entry *target, source string) error {
	if entry == nil || entry.route.SandboxID == "" || source == "" || len(source) > 128 || !utf8.ValidString(source) {
		return ErrIdentity
	}
	if metrics.DataPointCount() > 16384 || metrics.ResourceMetrics().Len() > 128 {
		return ErrInvalidMetrics
	}
	resources := metrics.ResourceMetrics()
	for i := 0; i < resources.Len(); i++ {
		resource := resources.At(i)
		attrs := resource.Resource().Attributes()
		if err := boundedAttributes(attrs, true); err != nil {
			return err
		}
		attrs.PutStr(SandboxIDAttribute, entry.route.SandboxID)
		attrs.PutStr(StableIDAttribute, entry.route.StableID)
		// Guest OTLP metrics cannot masquerade as envd guest-resource samples in
		// the E2B reader, even if they copy every metric and scope name.
		attrs.PutStr(sourceAttribute, source)
		scopes := resource.ScopeMetrics()
		for j := 0; j < scopes.Len(); j++ {
			scope := scopes.At(j)
			if err := boundedAttributes(scope.Scope().Attributes(), true); err != nil {
				return err
			}
			if scope.Metrics().Len() > 4096 {
				return ErrInvalidMetrics
			}
			for k := 0; k < scope.Metrics().Len(); k++ {
				metric := scope.Metrics().At(k)
				if metric.Name() == "" || len(metric.Name()) > 128 || !utf8.ValidString(metric.Name()) {
					return ErrInvalidMetrics
				}
				if err := eachPointAttributes(metric, func(attrs pcommon.Map) error { return boundedAttributes(attrs, true) }); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func eachPointAttributes(metric pmetric.Metric, visit func(pcommon.Map) error) error {
	switch metric.Type() {
	case pmetric.MetricTypeGauge:
		points := metric.Gauge().DataPoints()
		for i := 0; i < points.Len(); i++ {
			if err := visit(points.At(i).Attributes()); err != nil {
				return err
			}
		}
	case pmetric.MetricTypeSum:
		points := metric.Sum().DataPoints()
		for i := 0; i < points.Len(); i++ {
			if err := visit(points.At(i).Attributes()); err != nil {
				return err
			}
		}
	case pmetric.MetricTypeHistogram:
		points := metric.Histogram().DataPoints()
		for i := 0; i < points.Len(); i++ {
			if err := visit(points.At(i).Attributes()); err != nil {
				return err
			}
		}
	case pmetric.MetricTypeExponentialHistogram:
		points := metric.ExponentialHistogram().DataPoints()
		for i := 0; i < points.Len(); i++ {
			if err := visit(points.At(i).Attributes()); err != nil {
				return err
			}
		}
	case pmetric.MetricTypeSummary:
		points := metric.Summary().DataPoints()
		for i := 0; i < points.Len(); i++ {
			if err := visit(points.At(i).Attributes()); err != nil {
				return err
			}
		}
	default:
		return ErrInvalidMetrics
	}
	return nil
}
