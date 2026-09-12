package telemetry

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/app/telemetry/extension"
	"github.com/kuasar-sandbox/orchestrator/config"
)

type prometheusTestTransport func(*http.Request) (*http.Response, error)

func (f prometheusTestTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// Optional live-engine test. The explicitly supplied disposable Prometheus
// must enable remote write and a >= 5m out-of-order window. Only a unique sandbox
// series is written; it expires with the backend's own retention policy.
func TestPrometheusIntegration(t *testing.T) {
	endpoint := os.Getenv("TELEMETRY_PROMETHEUS_TEST_URL")
	if endpoint == "" {
		t.Skip("set TELEMETRY_PROMETHEUS_TEST_URL for live remote-read/write verification")
	}
	backend, err := NewPrometheus(config.TelemetryStorage{Retention: "8760h", Prometheus: config.TelemetryRemote{Endpoint: endpoint}})
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Shutdown(context.Background())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	id := fmt.Sprintf("telemetry-test-%d-%d", os.Getpid(), time.Now().UnixNano())
	stamp := time.Now().Add(-time.Minute).Truncate(5 * time.Second)
	first, second := resourceSamples(t, id, stamp.Add(123*time.Millisecond)), resourceSamples(t, id, stamp.Add(2123*time.Millisecond))
	first[1].Value, second[1].Value = 70, 20
	first[4].Value, second[4].Value = 100, 300
	for _, samples := range [][]extension.Sample{second, first, first} {
		if err := backend.Write(ctx, samples); err != nil {
			t.Fatal("remote write / duplicate / out-of-order", err)
		}
	}
	// Require the real streaming protocol, not just the SAMPLES fallback.
	baseTransport := backend.client.Transport
	var streams int
	backend.client.Transport = prometheusTestTransport(func(r *http.Request) (*http.Response, error) {
		response, err := baseTransport.RoundTrip(r)
		if err == nil && r.URL.Path == "/api/v1/read" {
			if response.Header.Get("Content-Type") != "application/x-streamed-protobuf; proto=prometheus.ChunkedReadResponse" {
				t.Error("engine did not negotiate streaming", response.Header)
			}
			streams++
		}
		return response, err
	})
	defer func() { backend.client.Transport = baseTransport }()
	start, end, found, err := backend.Bounds(ctx, id)
	if err != nil || !found || !start.Equal(first[0].Timestamp) || !end.Equal(second[0].Timestamp) {
		t.Fatal("streamed bounds / millisecond precision", start, end, found, err)
	}
	query := extension.Query{SandboxID: id, Start: start, End: end, Step: 5 * time.Second}
	points, err := backend.Query(ctx, query)
	if err != nil {
		t.Fatal(err)
	}
	result, err := aggregate(points, query)
	if err != nil || len(result) != 1 || result[0].CPUUsedPct != 70 || result[0].MemCache != 300 {
		t.Fatal("independent field MAX", result, err)
	}
	if _, _, found, err := backend.Bounds(ctx, "stable-"+id); err != nil || found {
		t.Fatal("StableID fallback / empty long-retention history", found, err)
	}
	query.Start, query.End = end, end
	if points, err := backend.Query(ctx, query); err != nil || len(points) != 7 {
		t.Fatal("inclusive edge chunk filtering", points, err)
	}
	if streams != 4 {
		t.Fatal("expected one streaming request per read", streams)
	}
}
