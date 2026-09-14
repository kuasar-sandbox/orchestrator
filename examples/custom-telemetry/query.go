package main

import (
	"encoding/json"
	"net/http"

	"github.com/kuasar-sandbox/orchestrator/app/telemetry/extension"
)

// This handler publishes generic raw series as its own HTTP contract. E2B SDKs
// should use query.handler=e2b instead. Conductor forwards either opaquely.
func metricSeriesHandler(scope extension.QueryScope) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		names := r.URL.Query()["metric"]
		if len(names) == 0 || len(names) > 64 {
			http.Error(w, "provide 1..64 metric parameters", http.StatusBadRequest)
			return
		}
		for _, name := range names {
			if name == "" || len(name) > 128 {
				http.Error(w, "invalid metric name", http.StatusBadRequest)
				return
			}
		}
		selection := extension.Selection{SandboxID: scope.SandboxID, Metrics: names}
		start, end, found, err := scope.Reader.Bounds(r.Context(), selection)
		if err != nil {
			http.Error(w, "query backend unavailable", http.StatusServiceUnavailable)
			return
		}
		series := []extension.Series{}
		if found {
			series, err = scope.Reader.Query(r.Context(), extension.Query{Selection: selection, Start: start, End: end, Aggregation: extension.Raw})
			if err != nil {
				http.Error(w, "query backend unavailable", http.StatusServiceUnavailable)
				return
			}
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Metrics-Contract", "kuasar-example-series-v1")
		_ = json.NewEncoder(w).Encode(series)
	})
}
