package nodelink

import (
	"strconv"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/nodegroup"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

// BenchmarkGroupStreamSnapshot measures the production snapshot batching and
// message validation path. The snapshot and output are in memory; it excludes
// database snapshot reads, network transport and Registry publication.
func BenchmarkGroupStreamSnapshot(b *testing.B) {
	for _, count := range []int{1_000, 10_000, 100_000} {
		b.Run(strconv.Itoa(count), func(b *testing.B) {
			groups := make([]nodegroup.Support, count)
			for i := range groups {
				groups[i] = nodegroup.Support{
					Group: "/g/" + strconv.Itoa(i), Version: 1,
					Entries: []nodegroup.Entry{{AuthRef: testFingerprint(testAPISecret), AuthMaterial: nodegroup.AuthMaterial{APISecret: testAPISecret}}},
				}
			}
			source := &groupTestSource{
				events:   make(chan routesync.GroupEvent),
				snapshot: func() (int64, []nodegroup.Support, error) { return 1, groups, nil },
			}
			close(source.events) // End the event phase immediately after GroupEnd.
			out := make(chan *routesync.Msg, (count+127)/128+2)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := streamGroups(b.Context(), source, out); err == nil {
					b.Fatal("closed event source did not end the stream")
				}
				last := ""
				for len(out) > 0 {
					last = (<-out).Type
				}
				if last != routesync.TypeGroupEnd {
					b.Fatalf("snapshot did not complete: last message %q", last)
				}
			}
		})
	}
}
