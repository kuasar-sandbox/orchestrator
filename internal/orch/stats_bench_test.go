package orch

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	conductorextension "github.com/kuasar-sandbox/orchestrator/app/conductor/extension"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/secretbox"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/sandboxer/pkg/usage"
)

// BenchmarkNativeUsageBatch includes real SQLite object reads, shared file
// locks, the native codec/recovery and bounded conductor batch assembly. It
// excludes native sampling and does not claim guest/remote-export performance.
func BenchmarkNativeUsageBatch(b *testing.B) {
	for _, count := range []int{1, 16, 64} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			root := b.TempDir()
			box, err := secretbox.NewFromColonHex(strings.Repeat("0", 64))
			if err != nil {
				b.Fatal(err)
			}
			st, err := store.Open(filepath.Join(root, "node.db"), box)
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = st.Close() })
			o := &Orchestrator{st: st, nativeStatsSlots: make(chan struct{}, conductorextension.MaxStatsConcurrency)}
			request := conductorextension.StatsRequest{Sections: []string{"usage"}, Usage: conductorextension.UsageQuery{View: "saved"}}
			for i := range count {
				id := fmt.Sprintf("saved-%03d", i)
				base := filepath.Join(root, id)
				if err := os.Mkdir(base, 0700); err != nil {
					b.Fatal(err)
				}
				sb := &types.Sandbox{ID: id, BaseDir: base, Profile: types.ProfileBare, State: types.StatePaused,
					APISecret: strings.Repeat("1", 64), ManifestKey: strings.Repeat("2", 64),
					TemplateID:   types.TemplateID{Profile: types.ProfileBare, Kind: types.KindImg, Ref: "manifest://" + strings.Repeat("a", 64)}.String(),
					ResumeSource: types.ResumeSource{Kind: types.ResumeSourceSnapshot, Ref: "manifest://" + strings.Repeat("b", 64)}}
				if err := materializeSandboxCredentials(sb, sandboxcfg.Credentials{}); err != nil {
					b.Fatal(err)
				}
				if err := st.Put(context.Background(), sb); err != nil {
					b.Fatal(err)
				}
				record, err := usage.EncodeRecord(usage.Record{Sequence: 1, SavedUTC: 1,
					Snapshot: usage.Snapshot{SandboxID: id, RunEpoch: "native-bench", Closed: true,
						SampleInterval: int64(time.Second), FlushInterval: int64(2 * time.Second)}})
				if err != nil {
					b.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(base, id+".usage"), record, 0600); err != nil {
					b.Fatal(err)
				}
				request.SandboxIDs = append(request.SandboxIDs, id)
			}
			read := func() {
				rows, err := o.ReadStats(context.Background(), request)
				if err != nil || len(rows) != count {
					b.Fatal("native batch", len(rows), err)
				}
			}
			read() // Warm the same database pool before observing retained resources.
			fdCount := func() int {
				fds, err := os.ReadDir("/proc/self/fd")
				if err != nil {
					b.Fatal(err)
				}
				return len(fds)
			}
			fds, goroutines := fdCount(), runtime.NumGoroutine()
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				read()
			}
			b.StopTimer()
			b.ReportMetric(float64(fdCount()-fds), "fd-delta")
			b.ReportMetric(float64(runtime.NumGoroutine()-goroutines), "goroutine-delta")
			b.ReportMetric(float64(count), "sandboxes/op")
		})
	}
}
