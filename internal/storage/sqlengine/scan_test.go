package sqlengine

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/storage"
)

func scanTestEngine(t *testing.T, backend string) *Engine {
	t.Helper()
	var e *Engine
	var err error
	if backend == "postgres" {
		dsn := os.Getenv("STORAGE_TEST_POSTGRES_DSN")
		if dsn == "" {
			t.Skip("requires isolated PostgreSQL")
		}
		e, err = OpenPostgres(t.Context(), dsn, fmt.Sprintf("scan-bounds-%d", time.Now().UnixNano()))
	} else {
		e, err = OpenSQLite(t.Context(), filepath.Join(t.TempDir(), "scan.sqlite"), "scan-bounds")
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := e.Close(); err != nil {
			t.Error(err)
		}
	})
	return e
}

func TestScanBinaryBounds(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			e := scanTestEngine(t, backend)
			keys := [][]byte{{0}, {0, 0}, {'a'}, {'a', 0}, {'a', 255}, {'b'}, {255}, {255, 255}}
			if err := e.Update(t.Context(), func(tx storage.Tx) error {
				for _, key := range keys {
					if err := tx.Put(key, append([]byte("value:"), key...)); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			// Cover every Begin/End/After combination, including nil versus an
			// empty byte slice, exact keys, a gap, NUL and 0xff. Invalid bounds
			// must retain their contract; valid ranges can legitimately be empty.
			bounds := append([][]byte{nil, {}, {'a', 1}}, keys...)
			valid, invalid, empty := 0, 0, 0
			err := e.View(t.Context(), func(tx storage.ReadTx) error {
				for bi, begin := range bounds {
					for ei, end := range bounds {
						for ai, after := range bounds {
							bad := end != nil && bytes.Compare(begin, end) >= 0 || after != nil && (bytes.Compare(after, begin) < 0 || end != nil && bytes.Compare(after, end) >= 0)
							page, err := tx.Scan(storage.Range{Begin: begin, End: end, After: after}, storage.Limit{Rows: 3, Bytes: 4096})
							if bad {
								invalid++
								if !errors.Is(err, storage.ErrInvalid) {
									return fmt.Errorf("bounds %d/%d/%d: invalid range returned %v", bi, ei, ai, err)
								}
								continue
							}
							if err != nil {
								return fmt.Errorf("bounds %d/%d/%d: %w", bi, ei, ai, err)
							}
							valid++
							var want [][]byte
							for _, key := range keys {
								if bytes.Compare(key, begin) >= 0 && (end == nil || bytes.Compare(key, end) < 0) && (after == nil || bytes.Compare(key, after) > 0) {
									want = append(want, key)
								}
							}
							if len(want) == 0 {
								empty++
							}
							if len(page.Rows) != min(3, len(want)) || page.More != (len(want) > 3) {
								return fmt.Errorf("bounds %d/%d/%d: rows=%d more=%v, want %d matches", bi, ei, ai, len(page.Rows), page.More, len(want))
							}
							for i, row := range page.Rows {
								if !bytes.Equal(row.Key, want[i]) || !bytes.Equal(row.Value, append([]byte("value:"), want[i]...)) {
									return fmt.Errorf("bounds %d/%d/%d: row %d = %x, want %x", bi, ei, ai, i, row.Key, want[i])
								}
							}
						}
					}
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("checked %d valid ranges (%d empty) and %d invalid ranges", valid, empty, invalid)
		})
	}
}

func TestSQLiteScanSeeksToCursor(t *testing.T) {
	e := scanTestEngine(t, "sqlite")
	for _, tc := range []struct {
		name, seek string
		after      []byte
	}{
		{"first_page", "SeekGE", nil},
		{"after_begin", "SeekGT", []byte("a")},
		{"later_page", "SeekGT", []byte{'a', 0xff}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bounds := storage.Range{Begin: []byte("a"), End: []byte("b"), After: tc.after}
			query, args := (&transaction{owner: e.owner}).scanQuery(bounds, storage.Limit{Rows: 128, Bytes: 4096})
			// Inspect the real modernc VM for the exact query used by Scan. An
			// EXPLAIN QUERY PLAN range alone cannot distinguish the old Begin
			// seek + per-row After filter from a direct seek to the cursor.
			rows, err := e.owner.db.QueryContext(t.Context(), "EXPLAIN "+query, args...)
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			variables := make(map[int]int)
			found := false
			for rows.Next() {
				var addr, p1, p2, p3, p5 int
				var op string
				var p4, comment any
				if err := rows.Scan(&addr, &op, &p1, &p2, &p3, &p4, &p5, &comment); err != nil {
					t.Fatal(err)
				}
				if op == "Variable" {
					variables[p2] = p1
				}
				if op != "SeekGE" && op != "SeekGT" {
					continue
				}
				found = true
				// The compound index seeks with (namespace,key), so the key's
				// input register immediately follows the namespace register.
				arg := variables[p3+1]
				lower := bounds.Begin
				if tc.after != nil {
					lower = tc.after
				}
				if op != tc.seek || arg < 1 || arg > len(args) {
					t.Fatalf("VM seeks with %s argument %d, want %s at %x", op, arg, tc.seek, lower)
				}
				key, ok := args[arg-1].([]byte)
				if !ok || !bytes.Equal(key, lower) {
					t.Fatalf("VM lower bound = %v, want %x", args[arg-1], lower)
				}
				t.Logf("VM %s uses parameter %d = %x", op, arg, key)
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			if !found {
				t.Fatal("query does not use an index range seek")
			}
		})
	}
}
