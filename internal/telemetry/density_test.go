package telemetry

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pmetric"
)

// Run with -run '^$' -bench BenchmarkEnvdDensity -benchtime=2x. Each iteration
// is one real five-second scrape period, not a fake-clock throughput estimate.
// One fixture UDS stands in for guests; distinct pool keys still force isolated
// per-sandbox connections. Its server goroutines/FDs are included in the peaks.
// Measurements are diagnostics, deliberately not timing-dependent CI assertions.
func BenchmarkEnvdDensity(b *testing.B) {
	for _, count := range []int{1000, 10000, 50000} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			body := envdJSON()
			path := unixHTTP(b, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(body) }))
			view := NewView(count)
			defer view.InvalidateSync()
			for n := range count {
				route := testRoute(fmt.Sprint(n))
				route.EnvdUDS, route.FloatingIP = path, ""
				upsert(b, view, route)
			}
			view.Bookmark()
			var received atomic.Int64
			guard := metricsConsumer(b, func(context.Context, pmetric.Metrics) error { received.Add(1); return nil })
			r := &envdReceiver{view: view, cfg: envdReceiverConfig{CollectionInterval: 5 * time.Second, Timeout: time.Second, Concurrency: 64}, next: guard, log: testLogger()}
			fds := func() int {
				entries, err := os.ReadDir("/proc/self/fd")
				if err != nil {
					b.Fatal(err)
				}
				return len(entries)
			}
			baseG, baseFD := runtime.NumGoroutine(), fds()
			peakG, peakFD := baseG, baseFD
			b.ReportAllocs()
			b.ResetTimer()
			if err := r.Start(context.Background(), nil); err != nil {
				b.Fatal(err)
			}
			ticker := time.NewTicker(100 * time.Millisecond)
			deadline := time.NewTimer(time.Duration(b.N) * 5 * time.Second)
		loop:
			for {
				select {
				case <-ticker.C:
					peakG, peakFD = max(peakG, runtime.NumGoroutine()), max(peakFD, fds())
				case <-deadline.C:
					break loop
				}
			}
			ticker.Stop()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			if err := r.Shutdown(ctx); err != nil {
				b.Fatal(err)
			}
			cancel()
			b.StopTimer()
			b.ReportMetric(float64(count), "targets")
			b.ReportMetric(float64(received.Load())/float64(b.N), "scrapes/period")
			b.ReportMetric(float64(peakG-baseG), "peak-added-goroutines")
			b.ReportMetric(float64(peakFD-baseFD), "peak-added-FDs")
		})
	}
}
