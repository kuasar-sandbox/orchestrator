package metastore

import (
	"fmt"
	"os"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/cluster/metadata"
	"github.com/kuasar-sandbox/orchestrator/internal/nodegroup"
)

// Uses the same real-engine factories as BenchmarkStorage. Candidate reads
// include publication, registration and lifecycle checks, not just an index scan.
func BenchmarkGroupDirectory(b *testing.B) {
	for name, open := range benchmarkBackends {
		if selected := os.Getenv("STORAGE_BENCH_BACKEND"); selected != "" && selected != name {
			continue
		}
		b.Run(name, func(b *testing.B) {
			must := func(b *testing.B, err error) {
				b.Helper()
				if err != nil {
					b.Fatal(err)
				}
			}
			newStore := func(b *testing.B) *Store {
				s := open(b)
				b.Cleanup(func() {
					if err := s.Close(); err != nil {
						b.Error(err)
					}
				})
				return s
			}
			publish := func(b *testing.B, s *Store, reg metadata.Registration, id string, seq int64, groups []nodegroup.Support) {
				must(b, s.BeginGroupSupport(b.Context(), reg, id, seq, len(groups)))
				for offset := 0; offset < len(groups); offset += metadata.MaxGroupSupportBatchRows {
					must(b, s.WriteGroupSupportBatch(b.Context(), reg, id, offset, groups[offset:min(offset+metadata.MaxGroupSupportBatchRows, len(groups))]))
				}
				for {
					done, err := s.FinishGroupSupport(b.Context(), reg, id)
					must(b, err)
					if done {
						break
					}
				}
			}
			register := func(b *testing.B, s *Store, id string) metadata.Registration {
				node, err := s.Register(b.Context(), metadata.Version{}, metadata.NodeProfileRecord{NodeID: id, LinkOwner: "registry", Capacity: 100, APIEndpoint: "api:7443", DataEndpoint: "data:8080"})
				must(b, err)
				return node.Registration
			}
			for _, nodes := range []int{4, 128, 256} {
				b.Run(fmt.Sprintf("Candidates/%d", nodes), func(b *testing.B) {
					s := newStore(b)
					for i := 0; i < nodes; i++ {
						reg := register(b, s, fmt.Sprintf("node-%04d", i))
						publish(b, s, reg, "initial", 1, []nodegroup.Support{supportFixture("g", 1)})
					}
					b.ReportAllocs()
					for b.Loop() {
						count := 0
						for cursor := metadata.Cursor(""); ; {
							page, err := s.ListGroupSupport(b.Context(), "g", cursor, metadata.MaxGroupSupportBatchRows)
							must(b, err)
							count += len(page.Items)
							if page.Next == "" {
								break
							}
							cursor = page.Next
						}
						if count != nodes {
							b.Fatalf("candidate count=%d, want %d", count, nodes)
						}
					}
					b.ReportMetric(float64(nodes), "nodes/op")
				})
			}
			b.Run("Delta", func(b *testing.B) {
				s := newStore(b)
				reg := register(b, s, "node")
				g := supportFixture("g", 1)
				publish(b, s, reg, "initial", 1, []nodegroup.Support{g})
				b.ReportAllocs()
				for b.Loop() {
					g.Version++
					g.Entries[0].Routing.TargetPort = int(g.Version%2) + 80
					must(b, s.ApplyGroupSupport(b.Context(), reg, g.Version, &g, ""))
				}
			})
			b.Run("FullSnapshot/32", func(b *testing.B) {
				s := newStore(b)
				reg := register(b, s, "node")
				groups := make([]nodegroup.Support, 32)
				for i := range groups {
					groups[i] = supportFixture(fmt.Sprintf("g%02d", i), 1)
				}
				var seq int64 = 1
				b.ReportAllocs()
				for b.Loop() {
					publish(b, s, reg, fmt.Sprintf("snapshot-%d", seq), seq, groups)
					seq++
				}
				b.ReportMetric(32, "groups/op")
			})
		})
	}
}
