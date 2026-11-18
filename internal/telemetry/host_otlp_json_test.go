package telemetry

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/app/telemetry/extension"
	customotel "github.com/kuasar-sandbox/orchestrator/app/telemetry/otel"
	"github.com/kuasar-sandbox/orchestrator/config"
)

func TestHostJSONOTLPOverUDSAcceptedByRealCollector(t *testing.T) {
	socket := filepath.Join(t.TempDir(), "receiver.sock")
	cfg, err := config.DecodeTelemetry(strings.NewReader(`collector:
  receivers:
    otlp/host:
      protocols:
        http: {transport: unix, endpoint: ` + socket + `}
  exporters: {sandboxlocal: {}}
  service:
    telemetry: {metrics: {level: none}}
    pipelines:
      metrics: {receivers: [otlp/host], exporters: [sandboxlocal]}
`))
	if err != nil {
		t.Fatal(err)
	}
	cfg.Local.Enabled = true
	cfg.Local.Path = filepath.Join(t.TempDir(), "local")
	backend, err := OpenLocal(cfg.Local, testLogger())
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Shutdown(context.Background())
	collector, err := NewCollector(t.Context(), *cfg, NewView(1), backend, customotel.Components{}, testLogger(), make(chan error, 1))
	if err != nil {
		t.Fatal(err)
	}
	if err := collector.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	defer collector.Shutdown(context.Background())
	stamp := time.Now().Add(-time.Second).Truncate(time.Millisecond)
	jsonPayload := fmt.Sprintf(`{"resourceMetrics":[{"resource":{"attributes":[
  {"key":"service.name","value":{"stringValue":"connector-ctl"}},
  {"key":"sandbox.telemetry.source","value":{"stringValue":"connector"}},
  {"key":"sandbox.id","value":{"stringValue":"s1"}},
  {"key":"sandbox.stable_id","value":{"stringValue":"stable1"}}
 ]},"scopeMetrics":[{"scope":{"name":"connector-ctl"},"metrics":[
  {"name":"sandbox.traffic.platform.rx.bytes","unit":"By","gauge":{"dataPoints":[
     {"timeUnixNano":"%d","asInt":"123","attributes":[{"key":"port","value":{"stringValue":"4"}}]}
  ]}}
 ]}]}]}`, stamp.UnixNano())
	client := &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socket)
	}}}
	req, err := http.NewRequest("POST", "http://host/v1/metrics", bytes.NewBufferString(jsonPayload))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("OTLP JSON: %d %s", resp.StatusCode, body)
	}
	series, err := backend.Query(context.Background(), extension.Query{Selection: extension.Selection{SandboxID: "s1", Metrics: []string{"sandbox.traffic.platform.rx.bytes"}, Attributes: map[string]string{sourceAttribute: "connector"}}, Start: stamp, End: stamp, Aggregation: extension.Raw})
	if err != nil {
		t.Fatal(err)
	}
	if len(series) != 1 || len(series[0].Points) != 1 || series[0].Points[0].Value != 123 {
		t.Fatalf("JSON lost in real receiver/TSDB: %+v", series)
	}
}
