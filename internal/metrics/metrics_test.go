package metrics

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHandlerDistinguishesCountersAndGauges(t *testing.T) {
	m := New()
	m.Inc(`requests_total{result="ok"}`)
	m.Set("builder_execution_used_builds", 3)
	m.Set("builder_execution_used_builds", 2)
	response := httptest.NewRecorder()
	m.Handler().ServeHTTP(response, httptest.NewRequest("GET", "/metrics", nil))
	got := response.Body.String()
	for _, want := range []string{
		"# TYPE requests_total counter\n",
		`requests_total{result="ok"} 1` + "\n",
		"# TYPE builder_execution_used_builds gauge\n",
		"builder_execution_used_builds 2\n",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("metrics output missing %q:\n%s", want, got)
		}
	}
}
