package telemetry

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/app/telemetry/extension"
	"github.com/kuasar-sandbox/orchestrator/config"
	"github.com/prometheus/prometheus/storage"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

func localConfig(t testing.TB) config.TelemetryLocal {
	return config.TelemetryLocal{Enabled: true, Path: t.TempDir(), Retention: "168h", MaxSize: "64MiB", MaxSeries: 10000}
}
func openLocal(t testing.TB, cfg config.TelemetryLocal) *Local {
	t.Helper()
	backend, err := OpenLocal(cfg, testLogger())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := backend.Shutdown(context.Background()); err != nil {
			t.Error(err)
		}
	})
	return backend
}
func resourceSamples(t testing.TB, id string, stamp time.Time) []extension.Sample {
	t.Helper()
	metrics, err := decodeEnvd(bytes.NewReader(envdJSON()))
	if err != nil {
		t.Fatal(err)
	}
	entry := &target{route: testRoute(id)}
	if err := enrich(metrics, entry, "envd"); err != nil {
		t.Fatal(err)
	}
	scope := metrics.ResourceMetrics().At(0).ScopeMetrics().At(0)
	for i := 0; i < scope.Metrics().Len(); i++ {
		scope.Metrics().At(i).Gauge().DataPoints().At(0).SetTimestamp(pcommon.NewTimestampFromTime(stamp))
	}
	samples, err := canonicalSamples(metrics)
	if err != nil {
		t.Fatal(err)
	}
	return samples
}

func TestLocalCollectorExporterRestartPrecisionAndSandboxLookup(t *testing.T) {
	ctx := context.Background()
	cfg := localConfig(t)
	backend := openLocal(t, cfg)
	metrics, err := decodeEnvd(bytes.NewReader(envdJSON()))
	if err != nil {
		t.Fatal(err)
	}
	entry := &target{route: testRoute("sandbox")}
	if err := enrich(metrics, entry, "envd"); err != nil {
		t.Fatal(err)
	}
	stamp := time.Now().Add(-time.Second).Truncate(time.Second).Add(123456789 * time.Nanosecond)
	scope := metrics.ResourceMetrics().At(0).ScopeMetrics().At(0)
	for n := 0; n < scope.Metrics().Len(); n++ {
		scope.Metrics().At(n).Gauge().DataPoints().At(0).SetTimestamp(pcommon.NewTimestampFromTime(stamp))
	}
	factory := localFactory(backend)
	writer, err := factory.CreateMetrics(ctx, exporter.Settings{ID: component.NewID(factory.Type())}, factory.CreateDefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Start(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if err := writer.ConsumeMetrics(ctx, metrics); err != nil {
		t.Fatal(err)
	}
	if err := writer.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if err := backend.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	backend = openLocal(t, cfg)
	first, last, found, err := backend.Bounds(ctx, envdSelection("sandbox"))
	if err != nil || !found || first.UnixMilli() != stamp.UnixMilli() || last.UnixNano()%int64(time.Millisecond) != 0 {
		t.Fatalf("bounds/precision %s %s %v %v", first, last, found, err)
	}
	if _, _, found, err := backend.Bounds(ctx, envdSelection("stable-sandbox")); err != nil || found {
		t.Fatal("StableID fallback", found, err)
	}
	points, err := backend.Query(ctx, extension.Query{Selection: envdSelection("sandbox"), Aggregation: extension.Max, Start: stamp.Add(-time.Second), End: stamp.Add(time.Second), Step: 5 * time.Second})
	if err != nil || len(points) != 7 {
		t.Fatalf("restart read = %v %v", points, err)
	}
	if files, err := os.ReadDir(filepath.Join(cfg.Path, "wal")); err != nil || len(files) == 0 {
		t.Fatal("WAL missing", err)
	}
	if _, err := OpenLocal(cfg, testLogger()); err == nil {
		t.Fatal("second process opened locked TSDB")
	}
}

func TestLocalDuplicateAndOutOfOrderSamples(t *testing.T) {
	backend := openLocal(t, localConfig(t))
	ctx := context.Background()
	stamp := time.Now().Add(-time.Second).Truncate(time.Millisecond)
	samples := resourceSamples(t, "sandbox", stamp)
	if err := backend.Write(ctx, samples); err != nil {
		t.Fatal(err)
	}
	if err := backend.Write(ctx, samples); err != nil {
		t.Fatal("exact duplicates not idempotent", err)
	}
	conflict := append([]extension.Sample(nil), samples...)
	conflict[0].Value++
	if err := backend.Write(ctx, conflict); !errors.Is(err, storage.ErrDuplicateSampleForTimestamp) {
		t.Fatal("conflicting duplicate", err)
	}
	earlier := resourceSamples(t, "sandbox", stamp.Add(-time.Second))
	if err := backend.Write(ctx, earlier); err != nil {
		t.Fatal("bounded out-of-order sample rejected", err)
	}
	tooOld := resourceSamples(t, "sandbox", stamp.Add(-10*time.Minute))
	if err := backend.Write(ctx, tooOld); err == nil {
		t.Fatal("unbounded out-of-order accepted")
	}
	// Failed transactions cannot leak changed data into the reader.
	points, err := backend.Query(ctx, extension.Query{Selection: envdSelection("sandbox"), Aggregation: extension.Max, Start: stamp.Add(-2 * time.Second), End: stamp, Step: time.Second})
	if err != nil || seriesPointCount(points) != 14 {
		t.Fatalf("samples after rollback = %v %v", points, err)
	}
	for _, series := range points {
		for _, point := range series.Points {
			if series.Metric == resourceMetrics[e2bCPUCount].name && point.Value != 4 {
				t.Fatal("failed append changed history", point)
			}
		}
	}
}

func TestLocalRetentionCardinalityAndSourceIsolation(t *testing.T) {
	cfg := localConfig(t)
	cfg.Retention = "1m"
	cfg.MaxSeries = 7
	ctx := context.Background()
	now := time.Now()
	var clock atomic.Int64
	clock.Store(now.UnixNano())
	backend, err := openLocalWithClock(cfg, testLogger(), func() time.Time { return time.Unix(0, clock.Load()) })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := backend.Shutdown(ctx); err != nil {
			t.Error(err)
		}
	})
	if err := backend.Write(ctx, resourceSamples(t, "expired", now.Add(-2*time.Minute))); err != nil {
		t.Fatal(err)
	}
	if _, _, found, err := backend.Bounds(ctx, envdSelection("expired")); err != nil || found {
		t.Fatal("expired write retained", err)
	}
	if err := backend.Write(ctx, resourceSamples(t, "sid", now.Add(-time.Second))); err != nil {
		t.Fatal(err)
	}
	if err := backend.Write(ctx, resourceSamples(t, "overflow", now)); !errors.Is(err, ErrSeriesLimit) {
		t.Fatal("series cap", err)
	}
	if err := backend.Write(ctx, resourceSamples(t, "sid", now.Add(2*time.Minute))); !errors.Is(err, ErrInvalidMetrics) {
		t.Fatal("future head poisoning", err)
	}
	// The retention clock is atomic because TSDB block maintenance runs in the background.
	clock.Store(now.Add(2 * time.Minute).UnixNano())
	if _, _, found, err := backend.Bounds(ctx, envdSelection("sid")); err != nil || found {
		t.Fatal("expired head readable", err)
	}
	if err := backend.expireHead(); err != nil {
		t.Fatal(err)
	}
	if backend.db.Head().NumSeries() != 0 {
		t.Fatal("idle head series not reclaimed")
	}
	// New identities can use the capacity reclaimed by physical retention.
	if err := backend.Write(ctx, resourceSamples(t, "new", backend.now())); err != nil {
		t.Fatal(err)
	}
}

func TestLocalGuestResourceNameCannotPolluteE2BHistory(t *testing.T) {
	backend := openLocal(t, localConfig(t))
	samples := resourceSamples(t, "sid", time.Now())
	for i := range samples {
		samples[i].Labels[sourceAttribute] = "otlp"
	}
	if err := backend.Write(context.Background(), samples); err != nil {
		t.Fatal(err)
	}
	if _, _, found, err := backend.Bounds(context.Background(), envdSelection("sid")); found || err != nil {
		t.Fatal("guest OTLP metric masqueraded as envd", found, err)
	}
}

func TestLocalOpenAndBackgroundErrorsPropagate(t *testing.T) {
	cfg := localConfig(t)
	cfg.Path = filepath.Join(cfg.Path, "not-directory")
	if err := os.WriteFile(cfg.Path, []byte("do not overwrite"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenLocal(cfg, testLogger()); err == nil {
		t.Fatal("invalid DB path opened")
	}
	backend := openLocal(t, localConfig(t))
	want := errors.New("disk failure")
	backend.fail(want)
	select {
	case err := <-backend.Errors():
		if !errors.Is(err, want) {
			t.Fatal(err)
		}
	default:
		t.Fatal("missing background error")
	}
	if err := backend.Write(context.Background(), nil); !errors.Is(err, want) {
		t.Fatal("unhealthy write", err)
	}
	if _, _, _, err := backend.Bounds(context.Background(), envdSelection("sid")); !errors.Is(err, want) {
		t.Fatal("unhealthy reader", err)
	}
}

func TestLocalCorruptWALDoesNotAdvertisePartialHistory(t *testing.T) {
	cfg := localConfig(t)
	backend := openLocal(t, cfg)
	if err := backend.Write(context.Background(), resourceSamples(t, "sid", time.Now().Add(-time.Second))); err != nil {
		t.Fatal(err)
	}
	if err := backend.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(filepath.Join(cfg.Path, "wal"))
	if err != nil || len(entries) == 0 {
		t.Fatal("missing WAL", err)
	}
	var segment string
	for _, entry := range entries {
		if !entry.IsDir() && len(entry.Name()) == 8 {
			segment = filepath.Join(cfg.Path, "wal", entry.Name())
			break
		}
	}
	if segment == "" {
		t.Fatal("missing WAL segment")
	}
	file, err := os.OpenFile(segment, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, writeErr := file.WriteAt(bytes.Repeat([]byte{255}, 16), 0)
	closeErr := file.Close()
	if writeErr != nil || closeErr != nil {
		t.Fatal(writeErr, closeErr)
	}
	reopened, err := OpenLocal(cfg, testLogger())
	if reopened != nil {
		_ = reopened.Shutdown(context.Background())
	}
	if err == nil {
		t.Fatal("WAL repair/corruption silently accepted as readable primary")
	}
}

func TestLocalSizeRetentionDropsCompactedHistory(t *testing.T) {
	// Keep this library-level fixture tiny. Final deployment validation separately
	// enforces >=64MiB; a 1KiB target here exercises actual size-based block deletion.
	cfg := localConfig(t)
	cfg.MaxSize = "1KiB"
	backend := openLocal(t, cfg)
	now := time.Now().Add(-8 * time.Hour).Truncate(time.Hour)
	var samples []extension.Sample
	for n := range 8 {
		samples = append(samples, resourceSamples(t, "sid", now.Add(time.Duration(n)*time.Hour))...)
	}
	if err := backend.Write(context.Background(), samples); err != nil {
		t.Fatal(err)
	}
	if err := backend.db.Compact(context.Background()); err != nil {
		t.Fatal(err)
	}
	start, _, found, err := backend.Bounds(context.Background(), envdSelection("sid"))
	if err != nil || !found || !start.After(now) {
		t.Fatal("size retention did not remove old compacted samples", start, found, err)
	}
}

func BenchmarkLocalWriteBatch(b *testing.B) {
	for _, batchSize := range []int{7, 700, 7000} {
		b.Run(fmt.Sprint(batchSize), func(b *testing.B) {
			backend := openLocal(b, localConfig(b))
			now := time.Now().Add(-time.Hour)
			var samples []extension.Sample
			for i := 0; i < batchSize/7; i++ {
				samples = append(samples, resourceSamples(b, fmt.Sprint(i), now)...)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				stamp := now.Add(time.Duration(n) * time.Millisecond)
				for i := range samples {
					samples[i].Timestamp = stamp
				}
				if err := backend.Write(context.Background(), samples); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(batchSize), "samples/batch")
		})
	}
}

func BenchmarkLocalHistoryQuery(b *testing.B) {
	backend := openLocal(b, localConfig(b))
	now := time.Now().Add(-time.Hour).Truncate(time.Second)
	var samples []extension.Sample
	for i := 0; i < 720; i++ {
		samples = append(samples, resourceSamples(b, "sandbox", now.Add(time.Duration(i)*5*time.Second))...)
	}
	if err := backend.Write(context.Background(), samples); err != nil {
		b.Fatal(err)
	}
	query := extension.Query{Selection: envdSelection("sandbox"), Aggregation: extension.Max, Start: now, End: now.Add(time.Hour), Step: 30 * time.Second}
	if _, err := backend.Query(context.Background(), query); err != nil {
		b.Fatal(err)
	}
	filesBefore, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		b.Fatal(err)
	}
	goroutinesBefore := runtime.NumGoroutine()
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		if _, err := backend.Query(context.Background(), query); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	filesAfter, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		b.Fatal(err)
	}
	b.ReportMetric(float64(len(filesAfter)-len(filesBefore)), "FD-delta")
	b.ReportMetric(float64(runtime.NumGoroutine()-goroutinesBefore), "goroutine-delta")
}
