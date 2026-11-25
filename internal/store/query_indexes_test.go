package store

import (
	"context"
	"fmt"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/secretbox"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func seedSandboxQueryRows(t testing.TB, st *Store, count, owners int, states []types.State) {
	t.Helper()
	ctx := context.Background()
	tx, err := st.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	insert, err := tx.PrepareContext(ctx, sandboxInsertSQL)
	if err != nil {
		t.Fatal(err)
	}
	defer insert.Close()
	// Reverse insertion order makes pagination depend on the query's id order,
	// not rowid/insertion order. Setup uses the production encryption/validation.
	for i := count - 1; i >= 0; i-- {
		sb := testSandboxWithServiceCredentials(fmt.Sprintf("query-%08d", i), types.ProfileBare)
		sb.TemplateID = types.TemplateID{Profile: types.ProfileBare, Kind: types.KindImg, Ref: "manifest://" + strings.Repeat("a", 64)}.String()
		sb.APISecret = strings.Repeat(fmt.Sprintf("%x", 1+(i/len(states))%owners), 64)
		sb.State = states[i%len(states)]
		switch sb.State {
		case types.StatePaused:
			sb.ResumeSource = types.ResumeSource{Kind: types.ResumeSourceSandbox, Ref: "sandbox-query-source"}
		case types.StateStarting:
			sb.LaunchMode = types.LaunchImage
		}
		args, err := st.prepareSandboxInsert(sb)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := insert.ExecContext(ctx, args...); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func queryPlan(t testing.TB, st *Store, query string, args ...any) string {
	t.Helper()
	rows, err := st.db.QueryContext(context.Background(), "EXPLAIN QUERY PLAN "+query, args...)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var details []string
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		details = append(details, detail)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return strings.Join(details, "\n")
}

func TestSandboxQueryIndexesUseCursorRanges(t *testing.T) {
	st := testStore(t)
	for _, tc := range []struct {
		name, where, index string
		args               []any
		ordered            bool
	}{
		{"state", "state=? AND id>?", "idx_sandboxes_state_id", []any{"running", "query-00000050"}, true},
		{"public_states", "state IN (?,?,?) AND id>?", "idx_sandboxes_state_id", []any{"running", "pausing", "paused", "query-00000050"}, false},
		{"owner", "state IN (?,?,?) AND substr(api_secret_hash,1,24)=? AND id>?", "idx_sandboxes_ascandidate_id", []any{"running", "pausing", "paused", strings.Repeat("1", 24), "query-00000050"}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plan := queryPlan(t, st, "SELECT "+cols+" FROM sandboxes WHERE "+tc.where+" ORDER BY id ASC LIMIT 101", tc.args...)
			t.Log(plan)
			if !strings.Contains(plan, tc.index) || !strings.Contains(plan, "id>?") {
				t.Fatalf("cursor is not an index range: %s", plan)
			}
			// Multiple state ranges may still need merging into global id order.
			if tc.ordered && strings.Contains(plan, "TEMP B-TREE") {
				t.Fatalf("single ordered range still sorts: %s", plan)
			}
		})
	}
	plan := queryPlan(t, st, "DELETE FROM manifest_keys WHERE expires_unix>0 AND expires_unix<?", time.Now().Unix())
	t.Log(plan)
	if !strings.Contains(plan, "idx_manifest_keys_expiry") || !strings.Contains(plan, "expires_unix>?") || !strings.Contains(plan, "expires_unix<?") {
		t.Fatalf("key expiry is not an indexed range: %s", plan)
	}
}

func TestSandboxListPagingAcrossStatesAndOwners(t *testing.T) {
	st := testStore(t)
	states := []types.State{types.StateRunning, types.StatePaused, types.StatePausing, types.StateStarting, types.StateDead, types.StateDeleting}
	const count = 72
	seedSandboxQueryRows(t, st, count, 2, states)
	for _, owner := range []int{-1, 0, 1, 2} {
		candidate := ""
		if owner >= 0 {
			hash, err := APISecretHash(strings.Repeat(fmt.Sprintf("%x", owner+1), 64))
			if err != nil {
				t.Fatal(err)
			}
			candidate = hash[:24]
		}
		for _, state := range append([]types.State{""}, states...) {
			t.Run(fmt.Sprintf("owner=%d/state=%s", owner, state), func(t *testing.T) {
				var want []string
				for i := 0; i < count; i++ {
					if owner >= 0 && (i/len(states))%2 != owner {
						continue
					}
					actual := states[i%len(states)]
					if state != "" && actual != state || state == "" && !slices.Contains([]types.State{types.StateRunning, types.StatePausing, types.StatePaused}, actual) {
						continue
					}
					want = append(want, fmt.Sprintf("query-%08d", i))
				}
				var got []string
				cursor := ""
				for page := 0; ; page++ {
					if page > count {
						t.Fatal("pagination did not terminate")
					}
					rows, next, err := st.List(t.Context(), string(state), candidate, 5, cursor)
					if err != nil {
						t.Fatal(err)
					}
					if len(rows) > 5 {
						t.Fatalf("page size = %d", len(rows))
					}
					for _, row := range rows {
						got = append(got, row.ID)
					}
					if next == "" {
						break
					}
					if next <= cursor || next != rows[len(rows)-1].ID {
						t.Fatalf("cursor did not advance through the last returned row: %q -> %q", cursor, next)
					}
					cursor = next
				}
				if !slices.Equal(got, want) {
					t.Fatalf("paged IDs = %v, want %v", got, want)
				}
			})
		}
	}
}

func TestOpenReplacesSandboxQueryIndexesWithoutChangingRecords(t *testing.T) {
	path := filepath.Join(t.TempDir(), "existing.db")
	box, err := secretbox.NewFromColonHex(strings.Repeat("0", 64))
	if err != nil {
		t.Fatal(err)
	}
	st, err := Open(path, box)
	if err != nil {
		t.Fatal(err)
	}
	seedSandboxQueryRows(t, st, 6, 1, []types.State{types.StateRunning, types.StatePaused, types.StateDead})
	before := make([]*types.Sandbox, 6)
	for i := range before {
		before[i], err = st.Get(t.Context(), fmt.Sprintf("query-%08d", i))
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, statement := range []string{
		`CREATE INDEX idx_sandboxes_state ON sandboxes(state)`,
		`CREATE INDEX idx_sandboxes_ascandidate ON sandboxes(substr(api_secret_hash,1,24))`,
		`DROP INDEX idx_sandboxes_state_id`,
		`DROP INDEX idx_sandboxes_ascandidate_id`,
		`DROP INDEX idx_manifest_keys_expiry`,
	} {
		if _, err := st.db.ExecContext(t.Context(), statement); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	// Both the additive upgrade and a second Open must preserve records/indexes.
	for attempt := 0; attempt < 2; attempt++ {
		st, err = Open(path, box)
		if err != nil {
			t.Fatal(err)
		}
		for _, old := range before {
			got, err := st.Get(t.Context(), old.ID)
			if err != nil || !reflect.DeepEqual(got, old) {
				t.Fatalf("reopen %d changed sandbox %s: %v", attempt, old.ID, err)
			}
		}
		for _, name := range []string{"idx_sandboxes_state_id", "idx_sandboxes_ascandidate_id", "idx_manifest_keys_expiry", "idx_sandboxes_dead_retention", "idx_builds_terminal_retention"} {
			var exists int
			if err := st.db.QueryRowContext(t.Context(), `SELECT count(*) FROM sqlite_schema WHERE type='index' AND name=?`, name).Scan(&exists); err != nil || exists != 1 {
				t.Fatalf("reopen %d index %s: count=%d err=%v", attempt, name, exists, err)
			}
		}
		var redundant int
		if err := st.db.QueryRowContext(t.Context(), `SELECT count(*) FROM sqlite_schema WHERE type='index' AND name IN ('idx_sandboxes_state','idx_sandboxes_ascandidate')`).Scan(&redundant); err != nil || redundant != 0 {
			t.Fatalf("superseded indexes remain: count=%d err=%v", redundant, err)
		}
		if err := st.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
