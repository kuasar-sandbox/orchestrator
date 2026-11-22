// Package metadatabench exercises the public repository interface. The same
// source can run against the pre-extraction FDB implementation for paired data.
package metadatabench

import (
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/cluster/metadata"
)

func route(id string) metadata.SandboxRecord {
	return metadata.SandboxRecord{Group: "/bench", RouteKey: id, SandboxID: id, NodeSandboxID: id + "-g0", NextSandboxGeneration: 1, State: metadata.StateReserved, NodeID: "node", Profile: "bare", APISecretFingerprint: strings.Repeat("a", 64), MaxCheckpoints: 3}
}
func require(b *testing.B, err error) {
	b.Helper()
	if err != nil {
		b.Fatal(err)
	}
}

// Run uses serial operations so latency and throughput can be paired on one
// local database. It is a regression workload, not a production capacity claim.
func Run(b *testing.B, open func(*testing.B) metadata.Repositories) {
	for _, workload := range []string{"ReadResolution", "IntentAndOwnership", "List128", "ReadObserveWriteWake"} {
		b.Run(workload, func(b *testing.B) {
			repos := open(b)
			ctx := b.Context()
			_, err := repos.Nodes.Register(ctx, metadata.Version{}, metadata.NodeProfileRecord{NodeID: "node", LinkOwner: "registry", Capacity: 100, APIEndpoint: "api:7443", DataEndpoint: "data:8080"})
			require(b, err)
			value := route("seed")
			key := metadata.RouteKey{Group: value.Group, RouteKey: value.RouteKey}
			_, err = repos.Routes.CommitIntentAndOwnership(ctx, metadata.Version{}, value, metadata.SandboxRef(value))
			require(b, err)
			if workload == "List128" {
				for i := 0; i < 127; i++ {
					r := route(fmt.Sprintf("list-%04d", i))
					_, err := repos.Routes.CompareAndPutRoute(ctx, metadata.RouteKey{Group: r.Group, RouteKey: r.RouteKey}, metadata.Version{}, r)
					require(b, err)
				}
			}
			latencies := make([]time.Duration, b.N)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				started := time.Now()
				switch workload {
				case "ReadResolution":
					out, err := repos.Routes.ReadResolution(ctx, key)
					require(b, err)
					if !out.Route.Exists || !out.Node.Exists {
						b.Fatal("missing resolution")
					}
				case "IntentAndOwnership":
					r := route(fmt.Sprintf("intent-%08d", i))
					_, err := repos.Routes.CommitIntentAndOwnership(ctx, metadata.Version{}, r, metadata.SandboxRef(r))
					require(b, err)
				case "List128":
					out, err := repos.Routes.ListRoutes(ctx, "/bench", "", 128)
					require(b, err)
					if len(out.Items) != 128 || out.Next != "" {
						b.Fatal("unexpected list page")
					}
				case "ReadObserveWriteWake":
					out, watch, err := repos.Routes.ReadAndObserveRoute(ctx, key)
					require(b, err)
					value.MaxCheckpoints = i + 4
					_, err = repos.Routes.CompareAndPutRoute(ctx, key, out.Version, value)
					if err != nil {
						watch.Cancel()
						require(b, err)
					}
					select {
					case <-watch.Done():
					case <-time.After(5 * time.Second):
						watch.Cancel()
						b.Fatal("watch timeout")
					}
					require(b, watch.Err())
					watch.Cancel()
				}
				latencies[i] = time.Since(started)
			}
			b.StopTimer()
			slices.Sort(latencies)
			for _, p := range []int{50, 95, 99} {
				b.ReportMetric(float64(latencies[min(len(latencies)-1, len(latencies)*p/100)].Nanoseconds())/1000, fmt.Sprintf("p%d-us", p))
			}
			b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/s")
			b.ReportMetric(0, "errors")
		})
	}
}
