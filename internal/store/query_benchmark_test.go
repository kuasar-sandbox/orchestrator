package store

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/secretbox"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// BenchmarkSandboxQueryScaling exercises the real modernc driver, encrypted
// rows, Store.List decoding and cursor traversal. Setup is outside the timer.
// "previous" reconstructs the superseded index layout only in this temporary
// database; neither variant disables WAL or changes the production query.
// Run complete inventories with -benchtime=1x to keep their total work explicit.
func BenchmarkSandboxQueryScaling(b *testing.B) {
	for _, count := range []int{1000, 10000, 100000} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			box, err := secretbox.NewFromColonHex(strings.Repeat("0", 64))
			if err != nil {
				b.Fatal(err)
			}
			st, err := Open(filepath.Join(b.TempDir(), "queries.db"), box)
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { st.Close() })
			seedSandboxQueryRows(b, st, count, 2, []types.State{types.StateRunning, types.StatePaused, types.StatePausing})
			hash, err := APISecretHash(strings.Repeat("1", 64))
			if err != nil {
				b.Fatal(err)
			}
			for _, layout := range []string{"previous", "composite"} {
				setSandboxQueryIndexLayout(b, st, layout)
				b.Run(layout, func(b *testing.B) {
					for _, query := range []struct {
						name, state, owner string
					}{
						{"OwnerPage", "", hash[:24]},
						{"StatePage", string(types.StateRunning), ""},
						{"PublicStatesPage", "", ""},
					} {
						b.Run(query.name, func(b *testing.B) {
							cursor := fmt.Sprintf("query-%08d", count/2)
							b.ReportAllocs()
							for b.Loop() {
								rows, _, err := st.List(b.Context(), query.state, query.owner, 100, cursor)
								if err != nil || len(rows) != 100 {
									b.Fatalf("page rows=%d err=%v", len(rows), err)
								}
							}
						})
					}
					b.Run("Inventory", func(b *testing.B) {
						b.ReportAllocs()
						for b.Loop() {
							cursor, previous := "", ""
							total := 0
							for {
								rows, next, err := st.List(b.Context(), "", "", 100, cursor)
								if err != nil {
									b.Fatal(err)
								}
								for _, row := range rows {
									if row.ID <= previous {
										b.Fatalf("inventory repeated or reordered %q after %q", row.ID, previous)
									}
									previous = row.ID
								}
								total += len(rows)
								if next == "" {
									break
								}
								cursor = next
							}
							if total != count {
								b.Fatalf("inventory returned %d rows, want %d", total, count)
							}
						}
						b.ReportMetric(float64(count), "rows/op")
						b.ReportMetric(float64((count+99)/100), "pages/op")
					})
				})
			}
		})
	}
}

func setSandboxQueryIndexLayout(b *testing.B, st *Store, layout string) {
	b.Helper()
	statement := `DROP INDEX IF EXISTS idx_sandboxes_state;
DROP INDEX IF EXISTS idx_sandboxes_ascandidate;
CREATE INDEX IF NOT EXISTS idx_sandboxes_state_id ON sandboxes(state,id);
CREATE INDEX IF NOT EXISTS idx_sandboxes_ascandidate_id ON sandboxes(substr(api_secret_hash,1,24),id);`
	if layout == "previous" {
		statement = `DROP INDEX IF EXISTS idx_sandboxes_state_id;
DROP INDEX IF EXISTS idx_sandboxes_ascandidate_id;
CREATE INDEX IF NOT EXISTS idx_sandboxes_state ON sandboxes(state);
CREATE INDEX IF NOT EXISTS idx_sandboxes_ascandidate ON sandboxes(substr(api_secret_hash,1,24));`
	}
	if _, err := st.db.ExecContext(b.Context(), statement); err != nil {
		b.Fatal(err)
	}
}

// The common reaper pass has no expired roots: permanent roots and unexpired
// leases must not make that pass scan the whole allowlist every five seconds.
func BenchmarkKeyExpiryScaling(b *testing.B) {
	for _, count := range []int{1000, 10000, 100000} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			box, err := secretbox.NewFromColonHex(strings.Repeat("0", 64))
			if err != nil {
				b.Fatal(err)
			}
			st, err := Open(filepath.Join(b.TempDir(), "keys.db"), box)
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { st.Close() })
			tx, err := st.db.BeginTx(b.Context(), nil)
			if err != nil {
				b.Fatal(err)
			}
			defer tx.Rollback()
			insert, err := tx.PrepareContext(b.Context(), `INSERT INTO manifest_keys
  (api_secret_hash,api_secret_enc,manifest_key_hash,manifest_key_enc,created_unix,expires_unix)
  VALUES (?,?,?,?,?,?)`)
			if err != nil {
				b.Fatal(err)
			}
			defer insert.Close()
			manifestHash, manifestEnc, err := st.encSecret("manifest key", strings.Repeat("a", 64))
			if err != nil {
				b.Fatal(err)
			}
			now := time.Now().Unix()
			for i := 0; i < count; i++ {
				apiHash, apiEnc, err := st.encSecret("API secret", fmt.Sprintf("%064x", i+1))
				if err != nil {
					b.Fatal(err)
				}
				expires := int64(0)
				if i%2 != 0 {
					expires = now + 86400
				}
				if _, err := insert.ExecContext(b.Context(), apiHash, apiEnc, manifestHash, manifestEnc, now, expires); err != nil {
					b.Fatal(err)
				}
			}
			if err := tx.Commit(); err != nil {
				b.Fatal(err)
			}
			for _, layout := range []string{"previous", "expiry_index"} {
				statement := "DROP INDEX IF EXISTS idx_manifest_keys_expiry"
				if layout == "expiry_index" {
					statement = "CREATE INDEX idx_manifest_keys_expiry ON manifest_keys(expires_unix) WHERE expires_unix>0"
				}
				if _, err := st.db.ExecContext(b.Context(), statement); err != nil {
					b.Fatal(err)
				}
				b.Run(layout, func(b *testing.B) {
					b.ReportAllocs()
					for b.Loop() {
						removed, err := st.PruneExpiredKeyPairs(b.Context())
						if err != nil || removed != 0 {
							b.Fatalf("prune unexpired roots: removed=%d err=%v", removed, err)
						}
					}
				})
			}
		})
	}
}
