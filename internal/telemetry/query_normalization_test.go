package telemetry

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/app/telemetry/extension"
)

func TestSelectionDistinguishesEmptyAndMissingAttributes(t *testing.T) {
	selection := extension.Selection{SandboxID: "sid", Metrics: []string{"task.temperature"}, Attributes: map[string]string{"point.optional": ""}}
	for _, tc := range []struct {
		name       string
		attributes map[string]string
		want       bool
	}{
		{"present empty", map[string]string{SandboxIDAttribute: "sid", "point.optional": ""}, true},
		{"missing", map[string]string{SandboxIDAttribute: "sid"}, false},
		{"nonempty", map[string]string{SandboxIDAttribute: "sid", "point.optional": "value"}, false},
		{"other sandbox with missing attribute", map[string]string{SandboxIDAttribute: "other"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := selectedSeries(selection, "task.temperature", tc.attributes); got != tc.want {
				t.Fatalf("selected = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestCustomReaderReceivesNormalizedAggregation(t *testing.T) {
	stamp := time.Now().Truncate(time.Millisecond)
	for _, tc := range []struct {
		name        string
		aggregation extension.Aggregation
		step        time.Duration
		want        extension.Aggregation
	}{
		{"default", "", 0, extension.Raw},
		{"raw", extension.Raw, 0, extension.Raw},
		{"max", extension.Max, time.Second, extension.Max},
		{"default with step", "", time.Second, ""},
		{"max without step", extension.Max, 0, ""},
		{"unknown", "other", 0, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			backend := &fakeReader{}
			handler := QueryHandler(backend, func(scope extension.QueryScope) http.Handler {
				return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					_, err := scope.Reader.Query(r.Context(), extension.Query{Start: stamp, End: stamp, Aggregation: tc.aggregation, Step: tc.step})
					if err != nil {
						http.Error(w, err.Error(), http.StatusBadRequest)
					}
				})
			})
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest("GET", "/sandboxes/sid/metrics", nil))
			if tc.want == "" {
				if response.Code != 400 || len(backend.queries) != 0 {
					t.Fatalf("invalid query reached custom backend: %d %+v", response.Code, backend.queries)
				}
				return
			}
			if response.Code != 200 || len(backend.queries) != 1 || backend.queries[0].Aggregation != tc.want || backend.queries[0].SandboxID != "sid" || backend.queries[0].Step != tc.step {
				t.Fatalf("normalized query/scope: %d %+v", response.Code, backend.queries)
			}
		})
	}
}

func TestMissingAttributeDoesNotHideForeignPrometheusSeries(t *testing.T) {
	selection := extension.Selection{SandboxID: "sid", Metrics: []string{"task.temperature"}, Attributes: map[string]string{"point.optional": "", sourceAttribute: "application"}}
	for _, attributes := range []map[string]string{
		{SandboxIDAttribute: "other", sourceAttribute: "application"},
		{SandboxIDAttribute: "sid", sourceAttribute: "envd"},
	} {
		if _, err := matchPrometheusSeries(selection, "task.temperature", attributes); err == nil {
			t.Fatal("missing empty attribute hid an out-of-scope backend result", attributes)
		}
	}
	if matched, err := matchPrometheusSeries(selection, "task.temperature", map[string]string{SandboxIDAttribute: "sid", sourceAttribute: "application"}); err != nil || matched {
		t.Fatal("valid broader physical match should be filtered", matched, err)
	}
	if matched, err := matchPrometheusSeries(selection, "task.temperature", map[string]string{SandboxIDAttribute: "sid", sourceAttribute: "application", "point.optional": ""}); err != nil || !matched {
		t.Fatal("present empty label should match", matched, err)
	}
}

func TestScopedReaderRejectsMissingSelectedAttribute(t *testing.T) {
	stamp := time.Now().Truncate(time.Millisecond)
	backend := &fakeReader{points: []e2bPoint{{Timestamp: stamp, Field: e2bCPUCount, Value: 2}}}
	reader := scopedReader{reader: backend, id: "sid"}
	query := extension.Query{Selection: extension.Selection{Attributes: map[string]string{"point.optional": ""}}, Start: stamp, End: stamp}
	if _, err := reader.Query(t.Context(), query); err == nil || len(backend.queries) != 1 {
		t.Fatal("custom backend result missing selected empty attribute accepted", err)
	}
}

// Real storage engines may physically treat label="" as also matching missing
// labels. The generic Reader must retain exact equality for Raw, Max and Bounds.
func requireMissingAttributeUnmatched(t *testing.T, reader extension.Reader, query extension.Query) {
	t.Helper()
	query.Attributes = map[string]string{"absent_attribute": ""}
	for _, aggregation := range []extension.Aggregation{extension.Raw, extension.Max} {
		query.Aggregation, query.Step = aggregation, 0
		if aggregation == extension.Max {
			query.Step = time.Second
		}
		if result, err := reader.Query(t.Context(), query); err != nil || len(result) != 0 {
			t.Errorf("%s empty selector matched absent attribute: %+v %v", aggregation, result, err)
		}
	}
	if _, _, found, err := reader.Bounds(t.Context(), query.Selection); err != nil || found {
		t.Errorf("empty selector matched absent attribute in Bounds: %v %v", found, err)
	}
}

func TestLocalMissingAttributeIsNotEmpty(t *testing.T) {
	backend := openLocal(t, localConfig(t))
	stamp := time.Now().Add(-time.Second).Truncate(time.Millisecond)
	requireNoQueryError(t, backend.Write(context.Background(), []extension.Sample{{Metric: "task.temperature", Labels: map[string]string{SandboxIDAttribute: "sid"}, Timestamp: stamp, Value: 42}}))
	query := extension.Query{Selection: extension.Selection{SandboxID: "sid", Metrics: []string{"task.temperature"}}, Start: stamp, End: stamp}
	if result, err := backend.Query(t.Context(), query); err != nil || len(result) != 1 {
		t.Fatal("unfiltered sample missing", result, err)
	}
	requireMissingAttributeUnmatched(t, backend, query)
}
