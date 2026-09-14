package telemetry

import (
	"net/http"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/app/telemetry/extension"
	"github.com/kuasar-sandbox/orchestrator/config"
)

func envdSelection(id string) extension.Selection {
	selection := extension.Selection{SandboxID: id, Attributes: map[string]string{sourceAttribute: "envd"}}
	for _, metric := range resourceMetrics {
		selection.Metrics = append(selection.Metrics, metric.name)
	}
	return selection
}

func queryConfigForTest() config.TelemetryQuery {
	cfg, err := config.DecodeTelemetry(strings.NewReader("{}"))
	if err != nil {
		panic(err)
	}
	return cfg.Query
}
func queryHandlerForTest(reader extension.Reader) http.Handler {
	return QueryHandler(reader, E2BHandler(queryConfigForTest().E2B))
}
func aggregateSeriesForTest(series []extension.Series, query extension.Query) ([]SandboxMetric, error) {
	points, err := e2bPoints(series, query.Metrics)
	if err != nil {
		return nil, err
	}
	return aggregate(points, query)
}
func seriesPointCount(series []extension.Series) int {
	count := 0
	for _, series := range series {
		count += len(series.Points)
	}
	return count
}
func requireNoQueryError(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
