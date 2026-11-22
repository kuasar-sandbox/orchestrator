package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/secretbox"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func legacyCheckpointStore(t *testing.T) (*Store, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "checkpoint-v1.db")
	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=foreign_keys(1)&_txlock=immediate")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	old := strings.Replace(schema, "CHECK(typeof(max_checkpoints)='integer' AND max_checkpoints>0)", "CHECK(max_checkpoints=1)", 1)
	old = strings.Replace(old, "\n sandbox_id TEXT NOT NULL REFERENCES sandboxes(id) ON DELETE CASCADE,", "\n sandbox_id TEXT PRIMARY KEY REFERENCES sandboxes(id) ON DELETE CASCADE,", 1)
	old = strings.Replace(old, " sandbox_ref TEXT NOT NULL,\n PRIMARY KEY(sandbox_id,checkpoint_id)", " sandbox_ref TEXT NOT NULL", 1)
	if _, err := db.Exec(old); err != nil {
		t.Fatal(err)
	}
	box, err := secretbox.NewFromColonHex(strings.Repeat("a", 64))
	if err != nil {
		t.Fatal(err)
	}
	st := &Store{db: db, box: box}
	ctx := context.Background()
	for n := range 2 {
		sb := sandboxInsertFixture(fmt.Sprintf("parent-%d", n), n)
		if err := st.InsertSandboxWithMMDSRouteSecretValues(ctx, sb, "routes", MMDSRouteSecretValues{"key": []byte("encrypted-value")}); err != nil {
			t.Fatal(err)
		}
		for _, stmt := range []string{
			`INSERT INTO workspace_defaults(sandbox_id,name,source_enc) VALUES(?,'workspace','preserved-source')`,
			`INSERT INTO sandbox_checkpoints(sandbox_id,checkpoint_id,source_kind,source_ref,sandbox_ref) VALUES(?,'original','sandbox','file://original.sandbox','')`,
			`INSERT INTO resource_recovery_placement(sandbox_id,placement) VALUES(?,'preserved-placement')`,
		} {
			if _, err := db.Exec(stmt, sb.ID); err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, stmt := range []string{
		`CREATE TABLE migration_delete_audit(kind TEXT NOT NULL, id TEXT NOT NULL)`,
		`CREATE TRIGGER migration_parent_delete AFTER DELETE ON sandboxes BEGIN INSERT INTO migration_delete_audit VALUES('parent',OLD.id); END`,
		`CREATE TRIGGER migration_checkpoint_delete AFTER DELETE ON sandbox_checkpoints BEGIN INSERT INTO migration_delete_audit VALUES('checkpoint',OLD.checkpoint_id); END`,
		`CREATE INDEX migration_checkpoint_ref ON sandbox_checkpoints(source_ref)`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatal(err)
		}
	}
	return st, path
}

func checkpointMigrationQueries(t *testing.T, db *sql.DB) map[string]string {
	t.Helper()
	rows, err := db.Query(`SELECT name FROM pragma_table_info('sandboxes') ORDER BY name`)
	if err != nil {
		t.Fatal(err)
	}
	var columns []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		columns = append(columns, `"`+name+`"`)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	queries := map[string]string{
		"parent-values":       "SELECT rowid," + strings.Join(columns, ",") + " FROM sandboxes ORDER BY id",
		"checkpoint-values":   "SELECT * FROM sandbox_checkpoints ORDER BY sandbox_id,checkpoint_id",
		"schema-dependencies": "SELECT type,name,tbl_name,sql FROM sqlite_schema WHERE type IN ('trigger','index') AND sql IS NOT NULL ORDER BY type,name",
	}
	for _, table := range []string{"workspace_defaults", "sandbox_mmds_route_secret_values", "resource_recovery_placement", "migration_delete_audit"} {
		queries[table] = "SELECT rowid,* FROM " + table + " ORDER BY rowid"
	}
	for _, table := range []string{"sandbox_checkpoints", "workspace_defaults", "sandbox_mmds_route_secret_values"} {
		queries[table+"-fk"] = "PRAGMA foreign_key_list(" + table + ")"
	}
	return queries
}

func TestCheckpointMigrationPreservesParentsChildrenAndTriggers(t *testing.T) {
	st, _ := legacyCheckpointStore(t)
	queries := checkpointMigrationQueries(t, st.db)
	before := map[string][][]any{}
	for name, q := range queries {
		before[name] = enforcementMigrationRows(t, st.db, q)
	}
	for range 3 {
		if err := migrateCheckpointSchema(context.Background(), st.db); err != nil {
			t.Fatal(err)
		}
		for name, q := range queries {
			if after := enforcementMigrationRows(t, st.db, q); !reflect.DeepEqual(before[name], after) {
				t.Fatalf("migration changed %s", name)
			}
		}
	}
	for _, bad := range []any{0, -1, 1.5, "bad"} {
		if _, err := st.db.Exec(`UPDATE sandboxes SET max_checkpoints=? WHERE id='parent-0'`, bad); err == nil {
			t.Fatalf("accepted invalid quota %v", bad)
		}
	}
	if _, err := st.db.Exec(`UPDATE sandboxes SET max_checkpoints=3 WHERE id='parent-0'`); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.Exec(`INSERT INTO sandbox_checkpoints VALUES('parent-0','second','sandbox','file://second.sandbox','')`); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.Exec(`INSERT INTO sandbox_checkpoints VALUES('absent','orphan','sandbox','file://orphan.sandbox','')`); err == nil {
		t.Fatal("foreign key stopped enforcing")
	}
	if rows := enforcementMigrationRows(t, st.db, `PRAGMA foreign_key_check`); len(rows) != 0 {
		t.Fatal("foreign key integrity changed")
	}
	var integrity string
	if err := st.db.QueryRow(`PRAGMA integrity_check`).Scan(&integrity); err != nil || integrity != "ok" {
		t.Fatalf("integrity: %s %v", integrity, err)
	}
	if err := st.Delete(context.Background(), "parent-0"); err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"sandbox_checkpoints", "workspace_defaults", "sandbox_mmds_route_secret_values", "resource_recovery_placement"} {
		var count int
		if err := st.db.QueryRow("SELECT count(*) FROM " + table + " WHERE sandbox_id='parent-0'").Scan(&count); err != nil || count != 0 {
			t.Fatalf("parent cascade lost for %s: %d %v", table, count, err)
		}
	}
	var parents, checkpoints int
	if err := st.db.QueryRow(`SELECT count(*) FROM migration_delete_audit WHERE kind='parent'`).Scan(&parents); err != nil {
		t.Fatal(err)
	}
	if err := st.db.QueryRow(`SELECT count(*) FROM migration_delete_audit WHERE kind='checkpoint'`).Scan(&checkpoints); err != nil {
		t.Fatal(err)
	}
	if parents != 1 || checkpoints != 2 {
		t.Fatalf("delete triggers changed: parent=%d checkpoint=%d", parents, checkpoints)
	}
	remaining, err := st.Checkpoints(context.Background(), "parent-1")
	if err != nil || len(remaining) != 1 {
		t.Fatalf("other parent changed: %+v %v", remaining, err)
	}
}

func TestCheckpointMigrationRollsBackEveryDDLStep(t *testing.T) {
	for _, fault := range []string{"quota-index", "checkpoint-table-collision"} {
		t.Run(fault, func(t *testing.T) {
			st, _ := legacyCheckpointStore(t)
			stmt := `CREATE INDEX dependent_quota ON sandboxes(max_checkpoints)`
			if fault == "checkpoint-table-collision" {
				stmt = `CREATE TABLE sandbox_checkpoints_v2(preserve_me TEXT)`
			}
			if _, err := st.db.Exec(stmt); err != nil {
				t.Fatal(err)
			}
			queries := checkpointMigrationQueries(t, st.db)
			queries["all-schema"] = `SELECT type,name,tbl_name,sql FROM sqlite_schema ORDER BY type,name`
			before := map[string][][]any{}
			for name, q := range queries {
				before[name] = enforcementMigrationRows(t, st.db, q)
			}
			if err := migrateCheckpointSchema(context.Background(), st.db); err == nil {
				t.Fatal("fault did not stop migration")
			}
			for name, q := range queries {
				if after := enforcementMigrationRows(t, st.db, q); !reflect.DeepEqual(before[name], after) {
					t.Fatalf("failed migration changed %s", name)
				}
			}
			if _, err := st.db.Exec(`UPDATE sandboxes SET max_checkpoints=2 WHERE id='parent-0'`); err == nil {
				t.Fatal("failed migration lost original CHECK")
			}
		})
	}
}

func TestNamedCheckpointQuotaAndExactConsumption(t *testing.T) {
	ctx, st := context.Background(), testStore(t)
	sb := sandboxInsertFixture("multiple", 0)
	sb.MaxCheckpoints = 3
	if err := st.InsertSandbox(ctx, sb); err != nil {
		t.Fatal(err)
	}
	pause := func(id string) (bool, error) {
		current, err := st.Get(ctx, sb.ID)
		if err != nil {
			return false, err
		}
		current.State = types.StateRunning
		if err := st.Put(ctx, current); err != nil {
			return false, err
		}
		if ok, err := st.SetPausing(ctx, sb.ID, sb.RunID, true); err != nil || !ok {
			return false, fmt.Errorf("gate %t: %w", ok, err)
		}
		return st.CommitPaused(ctx, sb.ID, sb.RunID, types.ResumeSource{Kind: types.ResumeSourceSandbox, Ref: "file://" + id + ".sandbox"}, id)
	}
	for _, id := range []string{"a", "b", "c"} {
		if err := st.CheckCheckpointQuota(ctx, sb.ID); err != nil {
			t.Fatal(err)
		}
		if ok, err := pause(id); err != nil || !ok {
			t.Fatalf("pause %s: %t %v", id, ok, err)
		}
	}
	if err := st.CheckCheckpointQuota(ctx, sb.ID); !errors.Is(err, ErrCheckpointLimit) {
		t.Fatalf("preflight quota=%v", err)
	}
	if ok, err := pause("overflow"); ok || !errors.Is(err, ErrCheckpointLimit) {
		t.Fatalf("transaction quota %t %v", ok, err)
	}
	current, err := st.Get(ctx, sb.ID)
	if err != nil || current.State != types.StatePausing || current.ResumeSource.Ref != "file://c.sandbox" {
		t.Fatalf("quota failure tore source/state: %+v %v", current, err)
	}
	if _, err := st.Checkpoint(ctx, sb.ID); err == nil {
		t.Fatal("ambiguous legacy lookup silently chose a checkpoint")
	}
	b, err := st.Checkpoint(ctx, sb.ID, "b")
	if err != nil || b == nil || b.Source.Ref != "file://b.sandbox" {
		t.Fatalf("exact lookup: %+v %v", b, err)
	}
	// An ordinary pause is legal while every named slot is occupied.
	if ok, err := pause(""); err != nil || !ok {
		t.Fatalf("ordinary pause: %t %v", ok, err)
	}
	if changed, err := st.DeleteCheckpoint(ctx, sb.ID, "b"); err != nil || !changed {
		t.Fatalf("consume b: %t %v", changed, err)
	}
	if err := st.CheckCheckpointQuota(ctx, sb.ID); err != nil {
		t.Fatal(err)
	}
	all, err := st.Checkpoints(ctx, sb.ID)
	if err != nil || len(all) != 2 || all[0].ID != "a" || all[1].ID != "c" {
		t.Fatalf("selected consume changed other roots: %+v %v", all, err)
	}
}
