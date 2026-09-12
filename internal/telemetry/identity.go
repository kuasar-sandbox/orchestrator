package telemetry

import (
	"context"
	"errors"
	"strings"
	"unicode"
	"unicode/utf8"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor"
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

type identityProcessor struct {
	view  *View
	next  consumer.Metrics
	final bool
}

func identityFactory(view *View, final bool) processor.Factory {
	name := "sandboxidentity"
	if final {
		name = "sandboxidentityguard"
	}
	return processor.NewFactory(component.MustNewType(name), func() component.Config { return &emptyConfig{} },
		processor.WithMetrics(func(_ context.Context, _ processor.Settings, _ component.Config, next consumer.Metrics) (processor.Metrics, error) {
			return &identityProcessor{view: view, next: next, final: final}, nil
		}, component.StabilityLevelStable))
}

func (*identityProcessor) Start(context.Context, component.Host) error { return nil }
func (*identityProcessor) Shutdown(context.Context) error              { return nil }
func (*identityProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

func (p *identityProcessor) ConsumeMetrics(ctx context.Context, metrics pmetric.Metrics) error {
	identity, _ := ctx.Value(identityContextKey{}).(ingressIdentity)
	entry := identity.entry
	if err := ctx.Err(); err != nil {
		return err
	}
	err := p.view.withCurrent(entry, func() error {
		if err := enrich(metrics, entry, identity.source); err != nil {
			return err
		}
		if p.final {
			return p.next.ConsumeMetrics(ctx, metrics)
		}
		return nil
	})
	if err != nil || p.final {
		return err
	}
	return p.next.ConsumeMetrics(ctx, metrics)
}

// Run identity is excluded even if a guest supplies similarly spelled labels.
// The original RouteEntry run field is deliberately never read by telemetry.
func forbiddenAttribute(key string) bool {
	normalized := strings.Map(func(r rune) rune {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			return unicode.ToLower(r)
		}
		return -1
	}, key)
	return strings.HasSuffix(normalized, "runid")
}

func boundedAttributes(attrs pcommon.Map, removeIdentity bool) error {
	attrs.RemoveIf(func(key string, _ pcommon.Value) bool {
		return forbiddenAttribute(key) || (removeIdentity && (key == SandboxIDAttribute || key == StableIDAttribute || key == sourceAttribute))
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
	if source != "envd" && source != "otlp" {
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
