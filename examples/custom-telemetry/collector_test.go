package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/app/telemetry"
	"github.com/kuasar-sandbox/orchestrator/config"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	coretelemetry "github.com/kuasar-sandbox/orchestrator/internal/telemetry"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
)

func TestExampleCollectorConfigurationAndProcessor(t *testing.T) {
	received := make(chan pmetricotlp.ExportRequest, 1)
	sink := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body io.Reader = r.Body
		if r.Header.Get("Content-Encoding") == "gzip" {
			decoded, err := gzip.NewReader(r.Body)
			if err != nil {
				http.Error(w, "invalid gzip", http.StatusBadRequest)
				return
			}
			defer decoded.Close()
			body = decoded
		}
		raw, err := io.ReadAll(io.LimitReader(body, 4<<20))
		request := pmetricotlp.NewExportRequest()
		if err != nil || request.UnmarshalProto(raw) != nil {
			http.Error(w, "invalid OTLP", http.StatusBadRequest)
			return
		}
		received <- request
		w.Header().Set("Content-Type", "application/x-protobuf")
	}))
	defer sink.Close()
	t.Setenv("OTLP_ENDPOINT", sink.URL)
	cfg, err := config.LoadTelemetry("telemetry.yaml")
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	otlp := cfg.Collector["receivers"].(map[string]any)["sandboxotlp"].(map[string]any)
	otlp["http_listen"], otlp["grpc_listen"] = address, "127.0.0.1:0"
	view := coretelemetry.NewView(1)
	defer view.InvalidateSync()
	if err := view.ApplyUpsert(routesync.RouteEntry{SandboxID: "exact-sid", StableID: "stable", FloatingIP: "127.0.0.1", State: routesync.StateRunning}); err != nil {
		t.Fatal(err)
	}
	view.Bookmark()
	var runtime telemetry.Runtime
	bindCollector(&runtime)
	collector, err := coretelemetry.NewCollector(t.Context(), *cfg, view, nil, runtime.Collector, slog.New(slog.NewTextHandler(io.Discard, nil)), make(chan error, 1))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := collector.Shutdown(ctx); err != nil {
			t.Error(err)
		}
	}()
	if err := collector.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	request := pmetricotlp.NewExportRequest()
	resource := request.Metrics().ResourceMetrics().AppendEmpty()
	resource.Resource().Attributes().PutStr("sandbox.id", "guest-forged")
	metric := resource.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	metric.SetName("application.queue.length")
	point := metric.SetEmptyGauge().DataPoints().AppendEmpty()
	point.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	point.SetIntValue(17)
	raw, err := request.MarshalProto()
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Timeout: 3 * time.Second}
	response, err := client.Post("http://"+address+"/v1/metrics", "application/x-protobuf", bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatal("sandbox OTLP rejected", response.Status)
	}
	select {
	case exported := <-received:
		attrs := exported.Metrics().ResourceMetrics().At(0).Resource().Attributes()
		deployment, _ := attrs.Get("deployment.environment.name")
		id, _ := attrs.Get("sandbox.id")
		if deployment.Str() != "example" || id.Str() != "exact-sid" || exported.Metrics().DataPointCount() != 1 {
			t.Fatal("example processor or accepted identity missing", attrs.AsRaw())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("example batch/exporter did not deliver")
	}
}
