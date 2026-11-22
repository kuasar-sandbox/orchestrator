package metastore

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/cluster/metadata"
	"github.com/kuasar-sandbox/orchestrator/internal/storage/sqlengine"
	"github.com/kuasar-sandbox/orchestrator/internal/testutil/metadatabench"
)

var benchmarkBackends = map[string]func(*testing.B) *Store{
	"sqlite": func(b *testing.B) *Store {
		e, err := sqlengine.OpenSQLite(b.Context(), filepath.Join(b.TempDir(), "metadata.sqlite"), "benchmark")
		if err != nil {
			b.Fatal(err)
		}
		return New(e, e)
	},
	"postgres": func(b *testing.B) *Store {
		dsn := os.Getenv("STORAGE_TEST_POSTGRES_DSN")
		if dsn == "" {
			b.Skip("requires PostgreSQL")
		}
		e, err := sqlengine.OpenPostgres(b.Context(), dsn, fmt.Sprintf("benchmark-%d", time.Now().UnixNano()))
		if err != nil {
			b.Fatal(err)
		}
		return New(e, e)
	},
}

func BenchmarkStorage(b *testing.B) {
	names := make([]string, 0, len(benchmarkBackends))
	for name := range benchmarkBackends {
		names = append(names, name)
	}
	slices.Sort(names)
	for _, name := range names {
		if selected := os.Getenv("STORAGE_BENCH_BACKEND"); selected != "" && selected != name {
			continue
		}
		b.Run(name, func(b *testing.B) {
			metadatabench.Run(b, func(b *testing.B) metadata.Repositories {
				s := benchmarkBackends[name](b)
				b.Cleanup(func() {
					if err := s.Close(); err != nil {
						b.Error(err)
					}
				})
				return s.Repositories()
			})
		})
	}
}
