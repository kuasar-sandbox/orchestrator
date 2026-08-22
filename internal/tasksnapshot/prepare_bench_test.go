package tasksnapshot

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/sandboxer/pkg/snapshot"
)

// BenchmarkPrepareConcurrency is a repeatable artifact-free proxy for the task
// portion of launch latency. The implementation has one root reader call and
// no parent traversal or sandbox-ctl process launch; those structural counts
// are emitted beside latency percentiles so regressions are visible in output.
func BenchmarkPrepareConcurrency(b *testing.B) {
	root := benchmarkSnapshot(b)
	for _, concurrency := range []int{1, 15, 30} {
		b.Run(fmt.Sprintf("c=%d", concurrency), func(b *testing.B) {
			latencies := make([]time.Duration, 0, b.N*concurrency)
			started := time.Now()
			b.ResetTimer()
			for range b.N {
				batch := make(chan time.Duration, concurrency)
				var wg sync.WaitGroup
				wg.Add(concurrency)
				for range concurrency {
					go func() {
						defer wg.Done()
						at := time.Now()
						result, err := Prepare(context.Background(), configsock.SnapshotPrepareSpec{
							RootRef: root,
							MaxRefs: 4,
						})
						if err != nil || result.Summary.RequiredRefCount != 3 {
							b.Errorf("Prepare() = %+v, %v", result, err)
							return
						}
						batch <- time.Since(at)
					}()
				}
				wg.Wait()
				close(batch)
				for latency := range batch {
					latencies = append(latencies, latency)
				}
			}
			b.StopTimer()
			elapsed := time.Since(started)
			sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
			if len(latencies) > 0 {
				b.ReportMetric(float64(percentile(latencies, 50).Microseconds()), "p50-us")
				b.ReportMetric(float64(percentile(latencies, 95).Microseconds()), "p95-us")
				b.ReportMetric(float64(percentile(latencies, 99).Microseconds()), "p99-us")
				b.ReportMetric(float64(len(latencies))/elapsed.Seconds(), "tasks/s")
			}
			b.ReportMetric(0, "info-subprocesses/task")
			b.ReportMetric(0, "parent-cfg-reads/task")
			b.ReportMetric(1, "root-cfg-reads/task")
		})
	}
}

func percentile(sorted []time.Duration, percent int) time.Duration {
	index := (len(sorted)*percent + 99) / 100
	if index < 1 {
		index = 1
	}
	return sorted[index-1]
}

func benchmarkSnapshot(b *testing.B) string {
	b.Helper()
	zipBody, err := snapshot.BuildZIP(map[string][]byte{
		"config.json": {},
		"snapshot.cfg": []byte(`resources:
  capacity: {cpu: 2, memory: 2GiB}
from_refs:
  - manifest://aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
boot:
  root:
    base_ref: manifest://bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
`),
		"state.json": {},
	})
	if err != nil {
		b.Fatal(err)
	}
	dir := b.TempDir()
	_, path, err := snapshot.NewFileSink(dir, "benchmark-root", nil, false, nil).AbsorbBundle(
		context.Background(), bytes.NewReader(bytes.Repeat([]byte{0x5a}, 4096)), nil, bytes.NewReader(zipBody),
	)
	if err != nil {
		b.Fatal(err)
	}
	return filepath.Clean(path)
}
