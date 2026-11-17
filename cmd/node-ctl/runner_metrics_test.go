package main

import (
	"context"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	nativeusage "github.com/kuasar-sandbox/sandboxer/pkg/usage"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
)

func TestRunnerFinalSavedUsageEmitsOnceWithOriginalTimestamp(t *testing.T) {
	for _, test := range []struct {
		name     string
		rejected int64
	}{{"accepted", 0}, {"partial", 1}} {
		t.Run(test.name, func(t *testing.T) { testFinalSavedUsage(t, test.rejected) })
	}
}

func testFinalSavedUsage(t *testing.T, rejected int64) {
	t.Helper()
	dir := t.TempDir()
	writer, err := nativeusage.Open(dir, "logical", "epoch", time.Now().Add(-3*time.Second), time.Second, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Counter("guest.cpu", "process", 15, 100, true); err != nil {
		t.Fatal(err)
	}
	savedAt := time.Now().Add(-time.Second)
	c, cancel := context.WithTimeout(context.Background(), time.Second)
	writer.Close(c, savedAt)
	cancel()
	sock := filepath.Join(t.TempDir(), "otlp.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	got := make(chan pmetric.Metrics, 8)
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req pmetricotlp.ExportRequest = pmetricotlp.NewExportRequest()
		data, e := io.ReadAll(io.LimitReader(r.Body, 16385))
		if e != nil || len(data) > 16384 {
			t.Error("invalid request", e)
			w.WriteHeader(400)
			return
		}
		if e = req.UnmarshalProto(data); e != nil {
			t.Error(e)
			w.WriteHeader(400)
			return
		}
		got <- req.Metrics()
		if rejected > 0 {
			reply := pmetricotlp.NewExportResponse()
			reply.PartialSuccess().SetRejectedDataPoints(rejected)
			body, err := reply.MarshalProto()
			if err != nil {
				t.Error(err)
				w.WriteHeader(500)
				return
			}
			w.Header().Set("Content-Type", "application/x-protobuf")
			_, _ = w.Write(body)
		}
	})}
	go srv.Serve(ln)
	defer srv.Shutdown(context.Background())
	t.Setenv("KS_HOST_OTLP_SOCKET", sock)
	m := newRunnerMetrics(runnerMetricsIdentity{sandboxID: "logical", stableID: "stable", runID: "run-abc", runDir: filepath.Join(dir, "running"), baseDir: dir}, nil)
	m.usageSample(context.Background(), true)
	m.usageSample(context.Background(), true)
	// Two status reads are separate observations; only the saved cumulative
	// endpoint is idempotent and must retain the original saved timestamp.
	endpointCount, statusCount := 0, 0
	for i := 0; i < 2; i++ {
		select {
		case metrics := <-got:
			if metrics.DataPointCount() == 0 {
				t.Fatal("missing metrics")
			}
			a := metrics.ResourceMetrics().At(0).Resource().Attributes()
			if _, exists := a.Get("sandbox.run_id"); exists {
				t.Fatal("logical Usage incorrectly split by run id")
			}
			if v, _ := a.Get("sandbox.id"); v.Str() != "logical" {
				t.Fatal("wrong sandbox id")
			}
			if v, _ := a.Get("sandbox.stable_id"); v.Str() != "stable" {
				t.Fatal("wrong stable id")
			}
			if v, _ := a.Get("sandbox.telemetry.source"); v.Str() != "runner" {
				t.Fatal("wrong source")
			}
			metricsList := metrics.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics()
			hasEndpoint, hasStatus := false, false
			for n := 0; n < metricsList.Len(); n++ {
				metric := metricsList.At(n)
				if metric.Name() == "sandbox.usage.saved.available" {
					hasStatus = true
				}
				if metric.Name() != "sandbox.usage.cpu.seconds" {
					continue
				}
				hasEndpoint = true
				dp := metric.Gauge().DataPoints().At(0)
				if dp.DoubleValue() != 0.15 {
					t.Fatalf("guest.cpu = %v seconds, want 0.15", dp.DoubleValue())
				}
				delta := int64(dp.Timestamp()) - savedAt.UnixNano()
				if delta < -int64(time.Millisecond) || delta > int64(time.Millisecond) {
					t.Fatalf("checkpoint timestamp differs by %v", time.Duration(delta))
				}
			}
			if hasEndpoint {
				endpointCount++
			}
			if hasStatus {
				statusCount++
			}
		case <-time.After(time.Second):
			t.Fatal("expected status and final usage")
		}
	}
	if endpointCount != 1 || statusCount != 2 {
		t.Fatalf("endpoint=%d status=%d", endpointCount, statusCount)
	}
	select {
	case <-got:
		t.Fatal("unexpected extra metrics envelope")
	case <-time.After(50 * time.Millisecond):
	}
	if _, err := os.Stat(filepath.Join(dir, "logical.usage")); err != nil {
		t.Fatal("unexpected usage file deletion", err)
	}
}

func TestRunnerUsageSkipsPreviousRunSavedCheckpoint(t *testing.T) {
	old := &nativeusage.Record{Sequence: 3, Snapshot: nativeusage.Snapshot{SandboxID: "sandbox", RunEpoch: "previous"}}
	newRun := &nativeusage.Snapshot{SandboxID: "sandbox", RunEpoch: "current"}
	view := nativeusage.View{Saved: old, Live: newRun, Enabled: true}
	if shouldPublishUsageEndpoint(view, false, 0) {
		t.Fatal("active runner replayed previous run's checkpoint")
	}
	if !shouldPublishUsageEndpoint(view, true, 0) {
		t.Fatal("offline final record suppressed")
	}
	old.Snapshot.RunEpoch = "current"
	if !shouldPublishUsageEndpoint(view, false, 2) {
		t.Fatal("current run saved checkpoint suppressed")
	}
	if shouldPublishUsageEndpoint(view, false, 3) {
		t.Fatal("duplicate saved checkpoint emitted")
	}
}

func TestRunnerDoesNotReemitPreviousFinalWhenChildWritesNothing(t *testing.T) {
	dir := t.TempDir()
	writer, err := nativeusage.Open(dir, "logical", "previous", time.Now(), time.Second, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	writer.Close(context.Background(), time.Now())
	m := newRunnerMetrics(runnerMetricsIdentity{sandboxID: "logical", runID: "next", baseDir: dir, runDir: dir}, nil)
	m.captureUsageBaseline(context.Background())
	view, err := m.readUsage(context.Background(), true)
	if err != nil {
		t.Fatal(err)
	}
	if m.lastSaved == 0 || shouldPublishUsageEndpoint(view, true, m.lastSaved) {
		t.Fatal("new runner would replay the old closed file after a failed start")
	}
	writer, err = nativeusage.Open(dir, "logical", "next", time.Now(), time.Second, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	writer.Close(context.Background(), time.Now())
	view, err = m.readUsage(context.Background(), true)
	if err != nil || !shouldPublishUsageEndpoint(view, true, m.lastSaved) {
		t.Fatalf("first fresh final endpoint missing: %v", err)
	}
}
