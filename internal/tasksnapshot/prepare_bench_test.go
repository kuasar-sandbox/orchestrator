package tasksnapshot

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/reflocation"
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
						if err != nil || result.Summary.RequiredRefCount != 4 {
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

// BenchmarkPrepareBundleRefLocations measures the task-local metadata-prefix
// discovery and mapping cost. Referenced files are intentionally absent: the
// orchestration preflight must remain flat and metadata-only.
func BenchmarkPrepareBundleRefLocations(b *testing.B) {
	for _, count := range []int{0, 1, 8, 32} {
		b.Run(fmt.Sprintf("refs=%d", count), func(b *testing.B) {
			parent := "file://" + filepath.Join(b.TempDir(), "locations")
			rootLocation, err := reflocation.Resolve(parent, "root-20260824")
			if err != nil {
				b.Fatal(err)
			}
			if err := os.MkdirAll(rootLocation.Path, 0o700); err != nil {
				b.Fatal(err)
			}
			refs := make([]string, 0, count)
			for index := range count {
				refs = append(refs, fmt.Sprintf("file://%064x.bundle@location:L%02d-20260824", index+1, index))
			}
			rootPath, rootKey, manifestConfig := writeTaskManifestBundle(b, rootLocation.Path, refs,
				"resources:\n  capacity: {cpu: 2, memory: 2GiB}\nboot: {}\n")
			rootRef := manifest.Ref{
				Scheme: manifest.RefSchemeFile, Path: filepath.Base(rootPath), DigestScheme: "manifest",
				Digest: rootKey, Location: "root-20260824",
			}.String()
			spec := configsock.SnapshotPrepareSpec{
				RootRef: rootRef, ManifestConfig: manifestConfig, RefLocationParent: parent, MaxRefs: 4,
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				result, err := Prepare(context.Background(), spec)
				if err != nil {
					b.Fatal(err)
				}
				if len(result.RefLocationURIs) != count+1 {
					b.Fatalf("location map size = %d, want %d", len(result.RefLocationURIs), count+1)
				}
			}
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
	_, path := writeTaskSnapshot(b, `resources:
  capacity: {cpu: 2, memory: 2GiB}
from_refs:
  - manifest://aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
boot:
  root:
    base: self
    base_from_refs:
      - manifest://bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
`)
	return filepath.Clean(path)
}
