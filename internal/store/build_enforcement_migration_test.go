package store

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/secretbox"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// Snapshot every persisted value, including ciphertext and rowid. The obsolete
// marker is the only omitted column; decoding alone would miss a re-encryption
// or a changed owner/digest that happened to produce the same public view.
func enforcementMigrationRows(t *testing.T, db *sql.DB, query string) [][]any {
	t.Helper()
	rows, err := db.Query(query)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	out := [][]any{}
	for rows.Next() {
		row := make([]any, len(cols))
		dest := make([]any, len(cols))
		for i := range row {
			dest[i] = &row[i]
		}
		if err := rows.Scan(dest...); err != nil {
			t.Fatal(err)
		}
		for i, value := range row {
			if b, ok := value.([]byte); ok {
				row[i] = append([]byte(nil), b...)
			}
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestOpenDropsBuildEnforcementPreservingBusinessData(t *testing.T) {
	for _, withActions := range []bool{false, true} {
		t.Run(fmt.Sprintf("action-columns=%t", withActions), func(t *testing.T) {
			testDropBuildEnforcementPreservingBusinessData(t, withActions)
		})
	}
}

func testDropBuildEnforcementPreservingBusinessData(t *testing.T, withActions bool) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "old.db")
	box, err := secretbox.NewFromColonHex(strings.Repeat("a", 64))
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=foreign_keys(1)")
	if err != nil {
		t.Fatal(err)
	}
	// Reproduce the supported old column at its original position, including
	// NOT NULL/default, rather than relying on a new-schema-only Open fixture.
	oldSchema := strings.Replace(schema, "  phase              TEXT", "  enforcement_status TEXT NOT NULL DEFAULT '',\n  phase              TEXT", 1)
	if oldSchema == schema {
		t.Fatal("old schema column was not inserted")
	}
	if _, err := db.Exec(oldSchema); err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{
		"CREATE INDEX idx_sandboxes_dead_retention ON sandboxes(state,dead_unix)",
		"CREATE INDEX idx_builds_terminal_retention ON builds(status,finished_unix)",
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	old := &Store{db: db, box: box}
	states := []types.BuildState{types.BuildRegistered, types.BuildWaiting, types.BuildBuilding, types.BuildReady, types.BuildError}
	markers := []string{"pending", "cpu,memory", ""}
	views := map[string]*types.Build{}
	for i, state := range states {
		for j, marker := range markers {
			id := fmt.Sprintf("old-%d-%d", i, j)
			b := retentionTerminalBuild(id, state, 100)
			b.Resources = types.BuildResources{CPU: 2000, Memory: 2 << 30, Storage: 4 << 30}
			b.WaitingUnix, b.WaitingSequence = 50, int64(i*3+j+1)
			b.Env = map[string]string{"SECRET": "instance-secret"}
			if withActions && j > 0 {
				b.CancelRequestedUnix = 70
				if j == 2 {
					b.DeleteRequestedUnix = 80
				}
			}
			if state != types.BuildReady && state != types.BuildError {
				b.FinishedUnix = 0
			}
			if state == types.BuildBuilding {
				b.ExecutionClaimed, b.ExecutionClaimedUnix, b.RunID = true, 60, "run-"+id
				b.Phase, b.PhaseSandboxID = "b", "phase-"+id
				b.RuntimeVswitchPort, b.RuntimeFloatingIP, b.RuntimePortMAC = "port-"+id, "192.0.2.1", "02:00:00:00:00:01"
				b.RuntimeEnvdAccessToken, b.RuntimePrepareJSON = "runtime-secret", `{"version":4,"digest":"preserved"}`
				b.ExecutionResult = &types.BuildResult{Target: types.BuildTarget{Kind: types.BuildTargetImage}, ImageRef: "manifest://" + strings.Repeat("b", 64)}
			}
			if err := old.InsertBuildWithMMDSRouteSecretValues(ctx, b, "routes", MMDSRouteSecretValues{"key": []byte("build-secret")}); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec("UPDATE builds SET enforcement_status=? WHERE build_id=?", marker, id); err != nil {
				t.Fatal(err)
			}
			views[id], err = old.GetBuild(ctx, id)
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	sandbox := sandboxInsertFixture("old-sandbox", 0)
	if err := old.InsertSandboxWithMMDSRouteSecretValues(ctx, sandbox, "routes", MMDSRouteSecretValues{"key": []byte("sandbox-secret")}); err != nil {
		t.Fatal(err)
	}
	if _, err := old.AddKeyPair(ctx, testKeyPair("3", "4"), "retained", 0, `{"registry":"secret"}`); err != nil {
		t.Fatal(err)
	}
	queries := map[string]string{
		"builds":  "SELECT rowid," + buildCols + " FROM builds ORDER BY build_id",
		"indexes": "SELECT type,name,tbl_name,sql FROM sqlite_schema WHERE type IN ('index','trigger','view') ORDER BY type,name",
	}
	for _, table := range []string{"sandboxes", "manifest_keys", "build_mmds_route_secret_values", "sandbox_mmds_route_secret_values"} {
		queries[table] = "SELECT rowid,* FROM " + table + " ORDER BY rowid"
	}
	for _, table := range []string{"builds", "sandboxes", "build_mmds_route_secret_values", "sandbox_mmds_route_secret_values"} {
		queries[table+"-fk"] = "PRAGMA foreign_key_list(" + table + ")"
	}
	before := map[string][][]any{}
	for name, query := range queries {
		before[name] = enforcementMigrationRows(t, db, query)
	}
	if !withActions {
		// The supported pre-#373 schema has neither action column. Their
		// existing additive initialization must still compose with this drop;
		// every business value and the absent columns' zero defaults survive.
		for _, column := range []string{"cancel_requested_unix", "delete_requested_unix"} {
			if _, err := db.Exec("ALTER TABLE builds DROP COLUMN " + column); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}
	for attempt := 0; attempt < 3; attempt++ {
		current, err := Open(path, box)
		if err != nil {
			t.Fatal(err)
		}
		for name, query := range queries {
			if got := enforcementMigrationRows(t, current.db, query); !reflect.DeepEqual(got, before[name]) {
				t.Errorf("Open %d changed %s", attempt, name)
			}
		}
		for id, want := range views {
			got, err := current.GetBuild(ctx, id)
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Errorf("Open %d changed decoded Build %s: %v", attempt, id, err)
			}
			values, _, found, err := current.GetMMDSRouteSecretValues(ctx, MMDSRouteSecretOwnerBuild, id, "routes")
			if err != nil || !found || string(values["key"]) != "build-secret" {
				t.Errorf("Build MMDS lost for %s: %v", id, err)
			}
		}
		gotSandbox, err := current.Get(ctx, sandbox.ID)
		if err != nil || !reflect.DeepEqual(gotSandbox, sandbox) {
			t.Errorf("Sandbox changed after migration: %v", err)
		}
		var count int
		if err := current.db.QueryRow("SELECT count(*) FROM pragma_table_info('builds') WHERE name='enforcement_status'").Scan(&count); err != nil || count != 0 {
			t.Fatalf("obsolete column count=%d: %v", count, err)
		}
		if got := enforcementMigrationRows(t, current.db, "PRAGMA foreign_key_check"); len(got) != 0 {
			t.Fatal("foreign key integrity failure")
		}
		var integrity string
		if err := current.db.QueryRow("PRAGMA integrity_check").Scan(&integrity); err != nil || integrity != "ok" {
			t.Fatalf("integrity=%s, %v", integrity, err)
		}
		if attempt == 2 {
			candidates, err := current.TerminalBuildsForRetention(ctx, 150, 100)
			if err != nil || len(candidates) != 6 {
				t.Fatalf("migrated retention candidates=%d, %v", len(candidates), err)
			}
			for _, candidate := range candidates {
				if deleted, err := current.DeleteTerminalBuildForRetention(ctx, candidate, 150); err != nil || !deleted {
					t.Fatalf("old marker blocked retention: %t, %v", deleted, err)
				}
				var attached int
				if err := current.db.QueryRow("SELECT count(*) FROM build_mmds_route_secret_values WHERE build_id=?", candidate.BuildID).Scan(&attached); err != nil || attached != 0 {
					t.Fatalf("MMDS FK did not cascade: %d, %v", attached, err)
				}
			}
		}
		if err := current.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestOpenFailsWhenObsoleteColumnHasDependentIndex(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dependent-index.db")
	box, err := secretbox.NewFromColonHex(strings.Repeat("a", 64))
	if err != nil {
		t.Fatal(err)
	}
	st, err := Open(path, box)
	if err != nil {
		t.Fatal(err)
	}
	b := retentionTerminalBuild("retained", types.BuildReady, 100)
	if err := st.PutBuild(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{
		"ALTER TABLE builds ADD COLUMN enforcement_status TEXT NOT NULL DEFAULT 'pending'",
		"CREATE INDEX operator_enforcement ON builds(enforcement_status)",
	} {
		if _, err := st.db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	before := enforcementMigrationRows(t, st.db, "SELECT * FROM builds")
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	opened, err := Open(path, box)
	if opened != nil {
		opened.Close()
		t.Fatal("Open ignored migration failure")
	}
	if err == nil || !strings.Contains(err.Error(), "drop obsolete build enforcement column") {
		t.Fatalf("migration error=%v", err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if got := enforcementMigrationRows(t, db, "SELECT * FROM builds"); !reflect.DeepEqual(got, before) {
		t.Fatal("failed migration changed business row")
	}
}

func TestTerminalRetentionKeepsEveryRealOwnershipFence(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	for column, value := range map[string]any{
		"run_id": "run", "execution_claimed": 1, "execution_claimed_unix": 1,
		"phase": "b", "phase_sandbox_id": "phase", "runtime_vswitch_port": "port",
		"runtime_floating_ip": "192.0.2.1", "runtime_port_mac": "02:00:00:00:00:01",
		"runtime_envd_access_token_enc": "owned", "runtime_prepare_json": "{}", "execution_result_json": "{}",
	} {
		t.Run(column, func(t *testing.T) {
			b := retentionTerminalBuild("fenced-"+column, types.BuildReady, 100)
			if err := st.PutBuild(ctx, b); err != nil {
				t.Fatal(err)
			}
			stale, err := st.GetBuild(ctx, b.BuildID)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := st.db.Exec("UPDATE builds SET "+column+"=? WHERE build_id=?", value, b.BuildID); err != nil {
				t.Fatal(err)
			}
			candidates, err := st.TerminalBuildsForRetention(ctx, 150, 100)
			if err != nil || len(candidates) != 0 {
				t.Fatalf("owned row selected: %d, %v", len(candidates), err)
			}
			if deleted, err := st.DeleteTerminalBuildForRetention(ctx, stale, 150); err != nil || deleted {
				t.Fatalf("stale candidate lost %s fence: %t, %v", column, deleted, err)
			}
		})
	}
}
