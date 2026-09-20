package store

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/secretbox"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestSnapshotPairAtomicPauseReopenAndColdMemoryCAS(t *testing.T) {
	ctx := context.Background()
	st := testStore(t)
	sb := sandboxInsertFixture("pair-owner", 0)
	sb.State = types.StateRunning
	sb.LaunchMode = ""
	sb.RunID = "producer"
	sb.RunDir = ""
	sb.EnvdUDS = ""
	sb.CiUDS = ""
	sb.VswitchPort = ""
	sb.FloatingIP = ""
	sb.InnerIP = ""
	sb.PortMAC = ""
	if err := st.Put(ctx, sb); err != nil {
		t.Fatal(err)
	}
	pair := types.ResumeSource{Kind: types.ResumeSourceSnapshot, Ref: "manifest://" + strings.Repeat("1", 64), SandboxRef: "manifest://" + strings.Repeat("2", 64)}
	if _, err := st.db.ExecContext(ctx, `CREATE TRIGGER fail_pair BEFORE UPDATE OF resume_sandbox_ref ON sandboxes BEGIN SELECT RAISE(ABORT,'pair rollback'); END`); err != nil {
		t.Fatal(err)
	}
	if changed, err := st.CommitRunningPaused(ctx, sb.ID, sb.RunID, pair); err == nil || changed {
		t.Fatalf("failed transaction=%t %v", changed, err)
	}
	got, err := st.Get(ctx, sb.ID)
	if err != nil || got.State != types.StateRunning || got.ResumeSource != sb.ResumeSource {
		t.Fatalf("torn rollback=%+v %v", got, err)
	}
	if _, err := st.db.ExecContext(ctx, `DROP TRIGGER fail_pair`); err != nil {
		t.Fatal(err)
	}
	if changed, err := st.CommitRunningPaused(ctx, sb.ID, "stale", pair); err != nil || changed {
		t.Fatalf("stale capture=%t %v", changed, err)
	}
	if changed, err := st.CommitRunningPaused(ctx, sb.ID, sb.RunID, pair); err != nil || !changed {
		t.Fatalf("commit=%t %v", changed, err)
	}
	var seq int
	var name, path string
	if err := st.db.QueryRowContext(ctx, `PRAGMA database_list`).Scan(&seq, &name, &path); err != nil {
		t.Fatal(err)
	}
	box := st.box
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	st, err = Open(path, box)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	got, err = st.Get(ctx, sb.ID)
	if err != nil || got.ResumeSource != pair || got.State != types.StatePaused {
		t.Fatalf("reopened pair=%+v %v", got, err)
	}
	if changed, err := st.ClearPausedRunner(ctx, sb.ID, sb.RunID); err != nil || !changed {
		t.Fatalf("clear=%t %v", changed, err)
	}
	for _, mode := range []types.LaunchMode{types.LaunchCold, types.LaunchMemory} {
		if changed, err := st.BeginResume(ctx, sb.ID, 0, mode, "/run/pair", "", ""); err != nil || !changed {
			t.Fatalf("resume %s=%t %v", mode, changed, err)
		}
		if changed, err := st.BindStartingRunner(ctx, sb.ID, "resume"); err != nil || !changed {
			t.Fatalf("bind=%t %v", changed, err)
		}
		before, err := st.Get(ctx, sb.ID)
		if err != nil {
			t.Fatal(err)
		}
		replacement := *before
		replacement.ResumeSource.SandboxRef = "manifest://" + strings.Repeat("3", 64)
		if err := st.Put(ctx, &replacement); err != nil {
			t.Fatal(err)
		}
		if changed, err := st.RollbackStartingPaused(ctx, before); err != nil || changed {
			t.Fatalf("stale E rollback=%t %v", changed, err)
		}
		if changed, err := st.BeginSandboxDelete(ctx, before); err != nil || changed {
			t.Fatalf("stale E delete=%t %v", changed, err)
		}
		if changed, err := st.CommitPreparedRunning(ctx, before, pair); err != nil || changed {
			t.Fatalf("stale E prepare=%t %v", changed, err)
		}
		if changed, err := st.RollbackStartingPaused(ctx, &replacement); err != nil || !changed {
			t.Fatalf("rollback %s=%t %v", mode, changed, err)
		}
		got, err = st.Get(ctx, sb.ID)
		if err != nil || got.ResumeSource != replacement.ResumeSource {
			t.Fatalf("rollback lost pair=%+v %v", got, err)
		}
		got.ResumeSource = pair
		if err := st.Put(ctx, got); err != nil {
			t.Fatal(err)
		}
	}
	got.State = types.StateRunning
	got.RunID = "cold-capture"
	if err := st.Put(ctx, got); err != nil {
		t.Fatal(err)
	}
	e := types.ResumeSource{Kind: types.ResumeSourceSandbox, Ref: "manifest://" + strings.Repeat("4", 64)}
	if changed, err := st.CommitRunningPaused(ctx, sb.ID, got.RunID, e); err != nil || !changed {
		t.Fatalf("E capture=%t %v", changed, err)
	}
	got, err = st.Get(ctx, sb.ID)
	if err != nil || got.ResumeSource != e {
		t.Fatalf("E capture retained S=%+v %v", got, err)
	}
}

func TestInitialSnapshotPairCommitsOnlyMatchingStartingRun(t *testing.T) {
	ctx := context.Background()
	st := testStore(t)
	sb := sandboxInsertFixture("initial-pair", 0)
	sb.State = types.StateStarting
	sb.LaunchMode = types.LaunchMemory
	sb.ResumeSource = types.ResumeSource{}
	sb.RunID = "first"
	root := "manifest://" + strings.Repeat("5", 64)
	sb.TemplateID = types.TemplateID{Profile: sb.Profile, Kind: types.KindSnp, Ref: root}.String()
	if err := st.Put(ctx, sb); err != nil {
		t.Fatal(err)
	}
	if changed, err := st.CommitStartingRunning(ctx, sb.ID, sb.RunID); err != nil || changed {
		t.Fatalf("initial S-only bypassed preparation: %t %v", changed, err)
	}
	pair := types.ResumeSource{Kind: types.ResumeSourceSnapshot, Ref: root, SandboxRef: "manifest://" + strings.Repeat("6", 64)}
	stale := *sb
	stale.RunID = "old"
	if changed, err := st.CommitPreparedRunning(ctx, &stale, pair); err != nil || changed {
		t.Fatalf("stale run=%t %v", changed, err)
	}
	wrong := pair
	wrong.Ref = "manifest://" + strings.Repeat("7", 64)
	if changed, err := st.CommitPreparedRunning(ctx, sb, wrong); err == nil || changed {
		t.Fatalf("wrong root=%t %v", changed, err)
	}
	if changed, err := st.CommitPreparedRunning(ctx, sb, types.ResumeSource{Kind: pair.Kind, Ref: pair.Ref}); err == nil || changed {
		t.Fatalf("missing E=%t %v", changed, err)
	}
	if _, err := st.db.ExecContext(ctx, `CREATE TRIGGER fail_prepared BEFORE UPDATE ON sandboxes BEGIN SELECT RAISE(ABORT,'prepared rollback'); END`); err != nil {
		t.Fatal(err)
	}
	if changed, err := st.CommitPreparedRunning(ctx, sb, pair); err == nil || changed {
		t.Fatalf("failed prepared transaction=%t %v", changed, err)
	}
	got, err := st.Get(ctx, sb.ID)
	if err != nil || got.State != types.StateStarting || !got.ResumeSource.Empty() {
		t.Fatalf("torn prepared pair=%+v %v", got, err)
	}
	if _, err := st.db.ExecContext(ctx, `DROP TRIGGER fail_prepared`); err != nil {
		t.Fatal(err)
	}
	if changed, err := st.CommitPreparedRunning(ctx, sb, pair); err != nil || !changed {
		t.Fatalf("commit pair=%t %v", changed, err)
	}
	got, err = st.Get(ctx, sb.ID)
	if err != nil || got.State != types.StateRunning || got.ResumeSource != pair {
		t.Fatalf("completed pair=%+v %v", got, err)
	}
	for _, state := range []types.State{types.StatePaused, types.StateStarting, types.StateRunning, types.StateDeleting} {
		incomplete := *got
		incomplete.State = state
		incomplete.ResumeSource.SandboxRef = ""
		if state == types.StateStarting {
			incomplete.LaunchMode = types.LaunchMemory
		}
		if err := st.Put(ctx, &incomplete); err == nil {
			t.Fatalf("accepted incomplete durable %s pair", state)
		}
	}
}

func TestOldPairSchemaRejectedWithoutBackfillOrDataDeletion(t *testing.T) {
	path := filepath.Join(t.TempDir(), "old.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	old := strings.Replace(schema, "  resume_sandbox_ref   TEXT NOT NULL DEFAULT '',\n", "", 1)
	if old == schema {
		t.Fatal("test did not remove new schema column")
	}
	if _, err := db.Exec(old); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE operator_data (value TEXT); INSERT INTO operator_data VALUES ('retain')`); err != nil {
		t.Fatal(err)
	}
	box, err := secretbox.NewFromColonHex(strings.Repeat("0", 64))
	if err != nil {
		t.Fatal(err)
	}
	if st, err := Open(path, box); err == nil {
		st.Close()
		t.Fatal("old DB silently upgraded")
	} else if !strings.Contains(err.Error(), "resume_sandbox_ref") {
		t.Fatal(err)
	}
	var count int
	var value string
	if err := db.QueryRow(`SELECT count(*) FROM pragma_table_info('sandboxes') WHERE name='resume_sandbox_ref'`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("backfilled old DB=%d %v", count, err)
	}
	if err := db.QueryRow(`SELECT value FROM operator_data`).Scan(&value); err != nil || value != "retain" {
		t.Fatalf("operator data changed=%q %v", value, err)
	}
	db.Close()
}
