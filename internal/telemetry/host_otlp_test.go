package telemetry

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/app/telemetry/extension"
	customotel "github.com/kuasar-sandbox/orchestrator/app/telemetry/otel"
	"github.com/kuasar-sandbox/orchestrator/config"
	"github.com/kuasar-sandbox/orchestrator/internal/hostmetrics"
)

func TestHostOTLPIndependentOfGuestRouteAndSourceIsolation(t *testing.T) {
	sock := filepath.Join(t.TempDir(), "host.sock")
	cfg, err := config.DecodeTelemetry(strings.NewReader(`collector:
  receivers:
    otlp/host:
      protocols:
        http: {transport: unix, endpoint: ` + sock + `}
  exporters:
    sandboxlocal: {}
  service:
    telemetry: {metrics: {level: none}}
    pipelines:
      metrics: {receivers: [otlp/host], exporters: [sandboxlocal]}
`))
	if err != nil {
		t.Fatal(err)
	}
	cfg.Local.Enabled = true
	cfg.Local.Path = filepath.Join(t.TempDir(), "db")
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
	t.Setenv("KS_HOST_OTLP_SOCKET", sock)
	stamp := time.Now().UTC().Add(-time.Second).Truncate(time.Millisecond)
	for _, source := range []string{"runner", "conductor"} {
		p := hostmetrics.New("node-ctl", source, map[string]string{"sandbox.id": "s1", "sandbox.stable_id": "stable1", "sandbox.run_id": "run1"})
		err := p.Emit(context.Background(), []hostmetrics.Sample{{Name: "sandbox.cpu.seconds", Unit: "s", Timestamp: stamp, Value: 1}})
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, source := range []string{"runner", "conductor"} {
		rows, err := backend.Query(context.Background(), extension.Query{Selection: extension.Selection{SandboxID: "s1", Metrics: []string{"sandbox.cpu.seconds"}, Attributes: map[string]string{sourceAttribute: source}}, Start: stamp, End: stamp, Aggregation: extension.Raw})
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) != 1 || len(rows[0].Points) != 1 || rows[0].Attributes["resource.sandbox.run_id"] != "run1" {
			t.Fatalf("source %s: %+v", source, rows)
		}
	}
}
