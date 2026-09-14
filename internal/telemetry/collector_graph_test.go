package telemetry

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	customotel "github.com/kuasar-sandbox/orchestrator/app/telemetry/otel"
	"github.com/kuasar-sandbox/orchestrator/config"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/receiver"
)

func TestCollectorInfrastructureLogsAndTraces(t *testing.T) {
	address, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	endpoint := address.Addr().String()
	if err := address.Close(); err != nil {
		t.Fatal(err)
	}
	received := make(chan string, 4)
	sink := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
			w.WriteHeader(400)
			return
		}
		var attrs pcommon.Map
		switch r.URL.Path {
		case "/v1/logs":
			request := plogotlp.NewExportRequest()
			if err := request.UnmarshalProto(body); err != nil {
				t.Error(err)
				w.WriteHeader(400)
				return
			}
			if request.Logs().LogRecordCount() != 1 {
				t.Error("lost infrastructure log")
				w.WriteHeader(400)
				return
			}
			attrs = request.Logs().ResourceLogs().At(0).Resource().Attributes()
		case "/v1/traces":
			request := ptraceotlp.NewExportRequest()
			if err := request.UnmarshalProto(body); err != nil {
				t.Error(err)
				w.WriteHeader(400)
				return
			}
			if request.Traces().SpanCount() != 1 {
				t.Error("lost infrastructure trace")
				w.WriteHeader(400)
				return
			}
			attrs = request.Traces().ResourceSpans().At(0).Resource().Attributes()
		default:
			t.Error("unexpected signal", r.URL.Path)
			w.WriteHeader(400)
			return
		}
		if _, ok := attrs.Get(SandboxIDAttribute); ok {
			t.Error("invented sandbox identity on infrastructure")
		}
		if value, _ := attrs.Get("application.run_id"); value.Str() != "infra-job" {
			t.Error("application attribute removed")
		}
		received <- r.URL.Path
		w.Header().Set("Content-Type", "application/x-protobuf")
	}))
	defer sink.Close()
	// Only sandboxotlp opens ProxyNetNS. Infrastructure receivers and exports
	// must continue working even when this unused sandbox namespace is absent.
	cfg, err := config.DecodeTelemetry(strings.NewReader(fmt.Sprintf(`proxy_netns: no-such-unused-sandbox-namespace
collector:
  receivers:
    otlp/infra:
      protocols:
        http: {endpoint: %s}
  processors: {batch: {timeout: 20ms}}
  exporters:
    otlp_http/sink: {endpoint: %s, compression: none}
  service:
    telemetry: {metrics: {level: none}}
    pipelines:
      logs: {receivers: [otlp/infra], processors: [batch], exporters: [otlp_http/sink]}
      traces: {receivers: [otlp/infra], processors: [batch], exporters: [otlp_http/sink]}
`, endpoint, sink.URL)))
	if err != nil {
		t.Fatal(err)
	}
	collector, err := NewCollector(t.Context(), *cfg, NewView(1), nil, customotel.Components{}, testLogger(), make(chan error, 1))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := collector.Shutdown(ctx); err != nil {
			t.Error(err)
		}
	})
	if err := collector.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	logs := plogotlp.NewExportRequest()
	resourceLogs := logs.Logs().ResourceLogs().AppendEmpty()
	resourceLogs.Resource().Attributes().PutStr("application.run_id", "infra-job")
	resourceLogs.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("infrastructure message")
	logBody, err := logs.MarshalProto()
	if err != nil {
		t.Fatal(err)
	}
	traces := ptraceotlp.NewExportRequest()
	resourceSpans := traces.Traces().ResourceSpans().AppendEmpty()
	resourceSpans.Resource().Attributes().PutStr("application.run_id", "infra-job")
	span := resourceSpans.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	span.SetName("infrastructure operation")
	span.SetTraceID(pcommon.TraceID{1})
	span.SetSpanID(pcommon.SpanID{1})
	traceBody, err := traces.MarshalProto()
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Timeout: 3 * time.Second}
	for path, body := range map[string][]byte{"/v1/logs": logBody, "/v1/traces": traceBody} {
		response, err := client.Post("http://"+endpoint+path, "application/x-protobuf", bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(response.Body)
		response.Body.Close()
		if err != nil || response.StatusCode != 200 {
			t.Fatalf("%s: %d %s %v", path, response.StatusCode, data, err)
		}
	}
	seen := map[string]bool{}
	for len(seen) < 2 {
		select {
		case path := <-received:
			seen[path] = true
		case <-time.After(3 * time.Second):
			t.Fatal("infra signals not exported", seen)
		}
	}
}

type graphReceiver struct{}

func (*graphReceiver) Start(context.Context, component.Host) error { return nil }
func (*graphReceiver) Shutdown(context.Context) error              { return nil }

// This exercises the linked native components and their actual OTLP HTTP wire,
// not replacement implementations of batch, routing, queue or retry.
func TestCollectorNativeMixedBatchRoutingQueueRetry(t *testing.T) {
	view := NewView(2)
	defer view.InvalidateSync()
	a := upsert(t, view, testRoute("a"))
	bRoute := testRoute("b")
	bRoute.FloatingIP = "127.0.0.2"
	b := upsert(t, view, bRoute)
	view.Bookmark()
	type sink struct {
		received chan pmetric.Metrics
		attempts atomic.Int32
		server   *httptest.Server
	}
	newSink := func() *sink {
		s := &sink{received: make(chan pmetric.Metrics, 4)}
		s.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if s.attempts.Add(1) == 1 {
				w.WriteHeader(503)
				return
			}
			data, err := io.ReadAll(r.Body)
			request := pmetricotlp.NewExportRequest()
			if err == nil {
				err = request.UnmarshalProto(data)
			}
			if err != nil {
				t.Errorf("OTLP decode: %v", err)
				w.WriteHeader(400)
				return
			}
			s.received <- request.Metrics()
			w.Header().Set("Content-Type", "application/x-protobuf")
		}))
		t.Cleanup(s.server.Close)
		return s
	}
	first, second := newSink(), newSink()
	var input consumer.Metrics
	source := receiver.NewFactory(component.MustNewType("fixture"), func() component.Config { return &emptyConfig{} },
		receiver.WithMetrics(func(_ context.Context, _ receiver.Settings, _ component.Config, next consumer.Metrics) (receiver.Metrics, error) {
			input = next
			return &graphReceiver{}, nil
		}, component.StabilityLevelStable))
	batchSizes := make(chan int, 4)
	observer := processor.NewFactory(component.MustNewType("observe"), func() component.Config { return &emptyConfig{} },
		processor.WithMetrics(func(_ context.Context, _ processor.Settings, _ component.Config, next consumer.Metrics) (processor.Metrics, error) {
			c, err := consumer.NewMetrics(func(ctx context.Context, m pmetric.Metrics) error {
				batchSizes <- m.ResourceMetrics().Len()
				return next.ConsumeMetrics(ctx, m)
			})
			return &testProcessor{Metrics: c, starts: &atomic.Int32{}, stops: &atomic.Int32{}}, err
		}, component.StabilityLevelStable))
	cfg, err := config.DecodeTelemetry(strings.NewReader(fmt.Sprintf(`collector:
  receivers: {fixture: {}}
  processors:
    filter/drop:
      metrics: {metric: ['name == "drop.me"']}
    transform/rename:
      metric_statements:
        - context: metric
          statements: ['set(name, "renamed.metric") where name == "keep.me"']
    batch/mixed: {timeout: 200ms, send_batch_size: 100}
    observe: {}
  connectors:
    routing/by_sandbox:
      default_pipelines: [metrics/second]
      table:
        - context: resource
          condition: 'attributes["sandbox.id"] == "a"'
          pipelines: [metrics/first]
  exporters:
    otlp_http/first:
      endpoint: %s
      compression: none
      sending_queue: {enabled: true, num_consumers: 1, queue_size: 8}
      retry_on_failure: {enabled: true, initial_interval: 10ms, max_interval: 20ms, max_elapsed_time: 2s}
    otlp_http/second:
      endpoint: %s
      compression: none
      sending_queue: {enabled: true, num_consumers: 1, queue_size: 8}
      retry_on_failure: {enabled: true, initial_interval: 10ms, max_interval: 20ms, max_elapsed_time: 2s}
  service:
    telemetry: {metrics: {level: none}}
    pipelines:
      metrics/input:
        receivers: [fixture]
        processors: [filter/drop, transform/rename, batch/mixed, observe]
        exporters: [routing/by_sandbox]
      metrics/first: {receivers: [routing/by_sandbox], exporters: [otlp_http/first]}
      metrics/second: {receivers: [routing/by_sandbox], exporters: [otlp_http/second]}
`, first.server.URL, second.server.URL)))
	if err != nil {
		t.Fatal(err)
	}
	collector, err := NewCollector(t.Context(), *cfg, view, nil, customotel.Components{Receivers: []receiver.Factory{source}, Processors: []processor.Factory{observer}}, testLogger(), make(chan error, 1))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := collector.Shutdown(ctx); err != nil {
			t.Error(err)
		}
	})
	if err := collector.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	stamp := time.Now().Add(-time.Second)
	for _, entry := range []*target{a, b} {
		m := pmetric.NewMetrics()
		resource := m.ResourceMetrics().AppendEmpty()
		resource.Resource().Attributes().PutStr("application.run_id", "keep")
		for _, name := range []string{"keep.me", "drop.me"} {
			metric := resource.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
			metric.SetName(name)
			point := metric.SetEmptyGauge().DataPoints().AppendEmpty()
			point.SetDoubleValue(12)
			point.SetTimestamp(pcommon.NewTimestampFromTime(stamp))
		}
		if err := view.acceptMetrics(t.Context(), entry, "accepted_custom", m); err != nil {
			t.Fatal(err)
		}
		if err := input.ConsumeMetrics(context.Background(), m); err != nil {
			t.Fatal(err)
		}
	}
	// Both state changes happen before batch flush and exporter retries.
	view.ApplyDelete("a")
	bRoute.State = routesync.StatePaused
	upsert(t, view, bRoute)
	select {
	case n := <-batchSizes:
		if n != 2 {
			t.Fatalf("expected one mixed-SID batch, got %d resources", n)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("batch did not flush")
	}
	for id, sink := range map[string]*sink{"a": first, "b": second} {
		select {
		case m := <-sink.received:
			if m.ResourceMetrics().Len() != 1 || m.DataPointCount() != 1 {
				t.Fatalf("routing/filter: %v", m)
			}
			r := m.ResourceMetrics().At(0)
			for key, expected := range map[string]string{SandboxIDAttribute: id, StableIDAttribute: "stable-" + id, sourceAttribute: "accepted_custom", "application.run_id": "keep"} {
				v, ok := r.Resource().Attributes().Get(key)
				if !ok || v.Str() != expected {
					t.Fatalf("%s: %s=%v", id, key, v)
				}
			}
			metric := r.ScopeMetrics().At(0).Metrics().At(0)
			if metric.Name() != "renamed.metric" || !metric.Gauge().DataPoints().At(0).Timestamp().AsTime().Equal(stamp) {
				t.Fatal("transform/timestamp mismatch")
			}
			if sink.attempts.Load() < 2 {
				t.Fatal("retry was not exercised")
			}
		case <-time.After(5 * time.Second):
			t.Fatal("accepted data was lost after pause/delete")
		}
	}
}
