package telemetry

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/app/telemetry/extension"
)

func TestLocalGenericSeriesRawMAXAndSourceSelection(t *testing.T) {
	backend := openLocal(t, localConfig(t))
	ctx := context.Background()
	stamp := time.Now().Add(-time.Minute).Truncate(5 * time.Second)
	var samples []extension.Sample
	for _, id := range []string{"sid", "other"} {
		for _, zone := range []string{"a", "b"} {
			attrs := map[string]string{SandboxIDAttribute: id, StableIDAttribute: "shared-stable", sourceAttribute: "custom", "point.zone": zone}
			for i, value := range []float64{-8, -3, -6} {
				samples = append(samples, extension.Sample{Metric: "application.temperature", Labels: attrs, Timestamp: stamp.Add(time.Duration(i+1) * 100 * time.Millisecond), Value: value})
			}
		}
	}
	requireNoQueryError(t, backend.Write(ctx, samples))
	requireNoQueryError(t, backend.Write(ctx, samples))
	selection := extension.Selection{SandboxID: "sid", Metrics: []string{"application.temperature"}, Attributes: map[string]string{sourceAttribute: "custom"}}
	query := extension.Query{Selection: selection, Start: stamp.Add(150 * time.Millisecond), End: stamp.Add(300 * time.Millisecond), Aggregation: extension.Raw}
	raw, err := backend.Query(ctx, query)
	if err != nil || len(raw) != 2 || seriesPointCount(raw) != 4 || raw[0].Points[0].Value != -3 {
		t.Fatal("arbitrary series/raw/duplicates/partial start", raw, err)
	}
	query.Aggregation, query.Step = extension.Max, 5*time.Second
	maxima, err := backend.Query(ctx, query)
	if err != nil || len(maxima) != 2 {
		t.Fatal(maxima, err)
	}
	for _, series := range maxima {
		if len(series.Points) != 1 || series.Points[0].Value != -3 || !series.Points[0].Timestamp.Equal(stamp) || series.Attributes[SandboxIDAttribute] != "sid" {
			t.Fatal("negative MAX/full attributes/scope", series)
		}
	}
	// Generic discovery within the exact SID includes arbitrary sources/names.
	all, err := backend.Query(ctx, extension.Query{Selection: extension.Selection{SandboxID: "sid"}, Start: stamp, End: stamp.Add(time.Second)})
	if err != nil || len(all) != 2 || seriesPointCount(all) != 6 {
		t.Fatal("generic all metrics selection", all, err)
	}
	if _, _, found, err := backend.Bounds(ctx, envdSelection("sid")); err != nil || found {
		t.Fatal("default E2B selection leaked another source", found, err)
	}
	if _, _, found, err := backend.Bounds(ctx, extension.Selection{SandboxID: "shared-stable"}); err != nil || found {
		t.Fatal("StableID became query alias", found, err)
	}
	query.Start, query.End = stamp.Add(time.Second), stamp.Add(time.Minute)
	if result, err := backend.Query(ctx, query); err != nil || len(result) != 0 {
		t.Fatal("last Gauge extended across a gap", result, err)
	}
	for _, selection := range []extension.Selection{{}, {SandboxID: "sid", Attributes: map[string]string{SandboxIDAttribute: "other"}}, {SandboxID: "sid", Metrics: []string{""}}} {
		if _, _, _, err := backend.Bounds(ctx, selection); err == nil {
			t.Fatal("invalid generic selection accepted", selection)
		}
	}
}

func TestCustomHTTPHandlerUsesImmutableSandboxScope(t *testing.T) {
	backend := openLocal(t, localConfig(t))
	stamp := time.Now().Add(-time.Second).Truncate(time.Millisecond)
	for _, id := range []string{"sid", "other"} {
		requireNoQueryError(t, backend.Write(context.Background(), []extension.Sample{{Metric: "application.latency", Labels: map[string]string{SandboxIDAttribute: id}, Timestamp: stamp, Value: 42}}))
	}
	factory := extension.MetricsHandler(func(scope extension.QueryScope) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// A separate context cannot substitute a different authorization scope.
			selection := extension.Selection{SandboxID: r.URL.Query().Get("sid"), Metrics: []string{"application.latency"}}
			if override := r.URL.Query().Get("attribute"); override != "" {
				selection.Attributes = map[string]string{SandboxIDAttribute: override}
			}
			result, err := scope.Reader.Query(context.Background(), extension.Query{Selection: selection, Start: stamp, End: stamp})
			if err != nil {
				http.Error(w, "scope rejected", 400)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Metrics-Contract", "custom-series-v1")
			_ = json.NewEncoder(w).Encode(result)
		})
	})
	handler := QueryHandler(backend, factory)
	for _, query := range []string{"", "?sid=sid", "?sid=other", "?attribute=other"} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest("GET", "/sandboxes/sid/metrics"+query, nil))
		if query == "?sid=other" || query == "?attribute=other" {
			if response.Code != 400 {
				t.Fatal("scope replaced", response.Code, response.Body.String())
			}
			continue
		}
		if response.Code != 200 || response.Header().Get("X-Metrics-Contract") != "custom-series-v1" || response.Header().Get("Cache-Control") != "no-store" {
			t.Fatal(response.Code, response.Header(), response.Body.String())
		}
		var series []extension.Series
		requireNoQueryError(t, json.Unmarshal(response.Body.Bytes(), &series))
		want := []extension.Series{{Metric: "application.latency", Attributes: map[string]string{SandboxIDAttribute: "sid"}, Points: []extension.Point{{Timestamp: stamp.UTC(), Value: 42}}}}
		if !reflect.DeepEqual(series, want) {
			t.Fatal("custom metric response", series)
		}
	}
}
