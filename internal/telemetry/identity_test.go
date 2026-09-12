package telemetry

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

func TestIdentityOverwritesGuestAndExcludesRunAttributes(t *testing.T) {
	view := NewView(1, 1)
	entry := upsert(t, view, testRoute("sandbox"))
	view.Bookmark()
	metrics, err := decodeEnvd(bytes.NewReader(envdJSON()))
	if err != nil {
		t.Fatal(err)
	}
	resource := metrics.ResourceMetrics().At(0)
	for _, attrs := range []pcommon.Map{resource.Resource().Attributes(), resource.ScopeMetrics().At(0).Scope().Attributes(), resource.ScopeMetrics().At(0).Metrics().At(0).Gauge().DataPoints().At(0).Attributes()} {
		attrs.PutStr(SandboxIDAttribute, "fake")
		attrs.PutStr(StableIDAttribute, "fake-stable")
		attrs.PutStr(sourceAttribute, "envd")
		attrs.PutStr("sandbox.run_id", "forged")
		attrs.PutStr("RunID", "forged")
		attrs.PutStr("app.label", "kept")
	}
	var consumed bool
	guard := &identityProcessor{view: view, final: true, next: metricsConsumer(t, func(_ context.Context, got pmetric.Metrics) error {
		consumed = true
		attrs := got.ResourceMetrics().At(0).Resource().Attributes()
		for key, want := range map[string]string{SandboxIDAttribute: "sandbox", StableIDAttribute: "stable-sandbox", sourceAttribute: "otlp", "app.label": "kept"} {
			value, ok := attrs.Get(key)
			if !ok || value.Str() != want {
				t.Errorf("%s = %v", key, value)
			}
		}
		for _, attrs := range []pcommon.Map{attrs, resource.ScopeMetrics().At(0).Scope().Attributes(), resource.ScopeMetrics().At(0).Metrics().At(0).Gauge().DataPoints().At(0).Attributes()} {
			attrs.Range(func(key string, _ pcommon.Value) bool {
				if forbiddenAttribute(key) {
					t.Error("run attribute escaped", key)
				}
				return true
			})
		}
		if _, ok := resource.ScopeMetrics().At(0).Metrics().At(0).Gauge().DataPoints().At(0).Attributes().Get(SandboxIDAttribute); ok {
			t.Error("point identity can override resource")
		}
		return nil
	})}
	if err := guard.ConsumeMetrics(withIdentity(context.Background(), entry, "otlp"), metrics); err != nil {
		t.Fatal(err)
	}
	if !consumed {
		t.Fatal("no delivery")
	}
	if err := guard.ConsumeMetrics(context.Background(), metrics); !errors.Is(err, ErrIdentity) {
		t.Fatal("untrusted context accepted", err)
	}
	view.InvalidateSync()
	if err := guard.ConsumeMetrics(withIdentity(context.Background(), entry, "otlp"), metrics); !errors.Is(err, ErrIdentity) {
		t.Fatal("stale identity accepted", err)
	}
}

func TestIdentityBoundsAttributes(t *testing.T) {
	view := NewView(1, 1)
	entry := upsert(t, view, testRoute("sandbox"))
	metrics, err := decodeEnvd(bytes.NewReader(envdJSON()))
	if err != nil {
		t.Fatal(err)
	}
	for n := 0; n < 33; n++ {
		metrics.ResourceMetrics().At(0).Resource().Attributes().PutInt(string(rune('a'+n)), int64(n))
	}
	if err := enrich(metrics, entry, "otlp"); !errors.Is(err, ErrInvalidMetrics) {
		t.Fatal("cardinality limit", err)
	}
}
