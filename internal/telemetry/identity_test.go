package telemetry

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/resourcetotelemetry"
	prw "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/translator/prometheusremotewrite"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

func TestIdentityOverwritesGuestAndExcludesRunAttributes(t *testing.T) {
	view := NewView(1)
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
		attrs.PutStr("application.run_id", "user-owned")
	}
	var consumed bool
	check := func(got pmetric.Metrics) error {
		consumed = true
		attrs := got.ResourceMetrics().At(0).Resource().Attributes()
		for key, want := range map[string]string{SandboxIDAttribute: "sandbox", StableIDAttribute: "stable-sandbox", sourceAttribute: "otlp", "app.label": "kept", "application.run_id": "user-owned"} {
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
	}
	if err := view.acceptMetrics(context.Background(), entry, "otlp", metrics); err != nil {
		t.Fatal(err)
	}
	if err := check(metrics); err != nil {
		t.Fatal(err)
	}
	if !consumed {
		t.Fatal("no delivery")
	}
	if err := view.acceptMetrics(context.Background(), nil, "otlp", metrics); !errors.Is(err, ErrIdentity) {
		t.Fatal("untrusted context accepted", err)
	}
	view.InvalidateSync()
	if err := view.acceptMetrics(context.Background(), entry, "otlp", metrics); !errors.Is(err, ErrIdentity) {
		t.Fatal("stale identity accepted", err)
	}
}

func TestIdentityBoundsAttributes(t *testing.T) {
	view := NewView(1)
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

func TestAcceptedIdentitySurvivesPrometheusLabelNormalization(t *testing.T) {
	view := NewView(1)
	entry := upsert(t, view, testRoute("sandbox"))
	view.Bookmark()
	metrics := pmetric.NewMetrics()
	resource := metrics.ResourceMetrics().AppendEmpty()
	scope := resource.ScopeMetrics().AppendEmpty()
	metric := scope.Metrics().AppendEmpty()
	metric.SetName("identity.probe")
	point := metric.SetEmptyGauge().DataPoints().AppendEmpty()
	point.SetDoubleValue(1)
	point.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	for _, attrs := range []pcommon.Map{resource.Resource().Attributes(), scope.Scope().Attributes(), point.Attributes()} {
		attrs.PutStr("sandbox_id", "forged")
		attrs.PutStr("sandbox-id", "forged")
		attrs.PutStr("sandbox.stable.id", "forged-stable")
		attrs.PutStr("sandbox_telemetry_source", "envd")
		attrs.PutStr("application.run_id", "user-owned")
		attrs.PutStr("sandbox..id", "forged")
		attrs.PutStr("sandbox__telemetry_source", "envd")
	}
	if err := view.acceptMetrics(t.Context(), entry, "otlp", metrics); err != nil {
		t.Fatal(err)
	}
	for _, attrs := range []pcommon.Map{resource.Resource().Attributes(), scope.Scope().Attributes(), point.Attributes()} {
		for _, key := range []string{"sandbox_id", "sandbox-id", "sandbox.stable.id", "sandbox_telemetry_source", "sandbox..id", "sandbox__telemetry_source"} {
			if _, found := attrs.Get(key); found {
				t.Errorf("conflicting resource/point/scope identity survived: %s", key)
			}
		}
	}
	// Use the actual standard exporter's resource conversion and remote-write
	// translator: punctuation aliases must not merge into trusted label values.
	converted := 0
	sink := &testExporter{metricsConsumer(t, func(_ context.Context, accepted pmetric.Metrics) error {
		series, err := prw.FromMetrics(accepted, prw.Settings{DisableTargetInfo: true})
		if err != nil {
			return err
		}
		for _, row := range series {
			labels := map[string]string{}
			for _, label := range row.Labels {
				labels[label.Name] = label.Value
			}
			for key, want := range map[string]string{"sandbox_id": "sandbox", "sandbox_stable_id": "stable-sandbox", "sandbox_telemetry_source": "otlp", "application_run_id": "user-owned"} {
				if labels[key] != want {
					t.Errorf("standard exporter changed %s: got %q, want %q", key, labels[key], want)
				}
			}
			converted++
		}
		return nil
	})}
	exporter := resourcetotelemetry.WrapMetricsExporter(resourcetotelemetry.Settings{Enabled: true}, sink)
	if err := exporter.ConsumeMetrics(t.Context(), metrics); err != nil || converted != 1 {
		t.Fatal("standard conversion did not execute", converted, err)
	}
}
