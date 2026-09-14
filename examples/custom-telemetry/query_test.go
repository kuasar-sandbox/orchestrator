package main

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/app/telemetry/extension"
	"github.com/kuasar-sandbox/orchestrator/config"
)

type queryReader struct{ selection extension.Selection }

func (r *queryReader) Bounds(_ context.Context, s extension.Selection) (time.Time, time.Time, bool, error) {
	r.selection = s
	return time.Unix(1, 0), time.Unix(1, 0), true, nil
}
func (r *queryReader) Query(_ context.Context, q extension.Query) ([]extension.Series, error) {
	r.selection = q.Selection
	return []extension.Series{{Metric: q.Metrics[0], Attributes: map[string]string{"sandbox.id": q.SandboxID}, Points: []extension.Point{{Timestamp: time.Unix(1, 0), Value: 17}}}}, nil
}
func TestExampleGenericHandlerAndQueryOnlyConfig(t *testing.T) {
	cfg, err := config.LoadTelemetry("query.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Local.Enabled || cfg.Collector != nil || cfg.Query.Backend != "prometheus" || cfg.Query.Handler != "custom" {
		t.Fatal("query-only config", cfg)
	}
	if err := config.ValidateTelemetryFinal(cfg); err != nil {
		t.Fatal(err)
	}
	reader := &queryReader{}
	handler := metricSeriesHandler(extension.QueryScope{SandboxID: "exact-sid", Reader: reader})
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest("GET", "/sandboxes/exact-sid/metrics?metric=application_queue_length&sid=forged", nil))
	if response.Code != 200 || response.Header().Get("X-Metrics-Contract") != "kuasar-example-series-v1" || reader.selection.SandboxID != "exact-sid" || reader.selection.Metrics[0] != "application_queue_length" {
		t.Fatal(response.Code, response.Body.String(), reader.selection)
	}
}
