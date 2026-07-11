package orch

import (
	"context"
	"database/sql"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/apikey"
	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// TestExportImportRoundTrip exercises the migration core that connect's auto-import
// builds on: export (move) mints a token and relinquishes the source; import on a
// node with the tenant key + matching runtime re-inserts the paused row; re-import
// is rejected.
func TestExportImportRoundTrip(t *testing.T) {
	dir := t.TempDir()
	rt := filepath.Join(dir, "rt-e2b.erofs")
	if err := os.WriteFile(rt, []byte("fake-runtime-bytes"), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{}
	cfg.Sandbox.Boot.Runtime = rt
	cfg.Paths.RunRoot, cfg.Paths.BaseRoot = dir+"/run", dir+"/lib"
	o := testOrchCfg(t, cfg)
	ctx := context.Background()

	mk := strings.Repeat("6", 64)
	raw, _ := hex.DecodeString(mk)
	apiKey, _ := apikey.Mint(raw)
	if _, err := o.st.AddManifestKey(ctx, mk, "", 0, ""); err != nil { // import precondition: key allowlisted
		t.Fatal(err)
	}

	sid := "sbx-mig-1"
	sb := &types.Sandbox{
		ID: sid, TemplateID: "e2b-snp-" + strings.Repeat("a", 64), State: types.StatePaused,
		ManifestKey: mk, SnapshotRef: "manifest://" + strings.Repeat("b", 64),
		RunDir: dir + "/run/" + sid, BaseDir: dir + "/lib/" + sid,
		Env: map[string]string{"FOO": "bar"}, Metadata: map[string]string{"k": "v"},
		CreatedUnix: 1,
	}
	if err := o.st.Put(ctx, sb); err != nil {
		t.Fatal(err)
	}
	o.cache(sb)

	// export (move): mints a token, deletes the source row, and removes any
	// cached source so an immediate connect+import cannot resume stale local state.
	tok, err := o.ExportSandbox(ctx, apiKey, sid, false, false)
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	if s, _ := o.st.Get(ctx, sid); s != nil {
		t.Fatal("move export should delete the source row")
	}
	if s := o.lookup(sid); s != nil {
		t.Fatalf("move export should uncache the source row: %+v", s)
	}

	// import: re-inserts the paused row (the snapshot persists in the remote store).
	imported, err := o.ImportSandbox(ctx, apiKey, tok)
	if err != nil || imported != sid {
		t.Fatalf("import: imported=%q err=%v", imported, err)
	}
	got, _ := o.st.Get(ctx, sid)
	if got == nil || got.State != types.StatePaused || got.Env["FOO"] != "bar" || got.SnapshotRef != sb.SnapshotRef {
		t.Fatalf("imported row wrong: %+v", got)
	}

	// re-import is rejected (already present on this node).
	if _, err := o.ImportSandbox(ctx, apiKey, tok); err == nil {
		t.Fatal("re-import should error (sandbox already exists)")
	}
}

func TestExportPromotesLocalSnapshotState(t *testing.T) {
	dir := t.TempDir()
	o := testOrch(t)
	ctx := context.Background()
	mk := strings.Repeat("7", 64)
	raw, _ := hex.DecodeString(mk)
	apiKey, _ := apikey.Mint(raw)
	sid := "sbx-promote-ok"
	localRef := makeLocalSnapshot(t, dir, sid)
	mref := "manifest://" + strings.Repeat("c", 64)
	installPromoteStub(t, mref)

	sb := migrationSandbox(dir, sid, mk, localRef)
	if err := o.st.Put(ctx, sb); err != nil {
		t.Fatal(err)
	}
	o.cache(sb)
	events, cancel := o.Subscribe()
	defer cancel()

	if _, err := o.ExportSandbox(ctx, apiKey, sid, true, true); err != nil {
		t.Fatalf("export: %v", err)
	}
	stored, err := o.st.Get(ctx, sid)
	if err != nil || stored == nil || stored.SnapshotRef != mref {
		t.Fatalf("stored snapshot ref = %+v, err=%v", stored, err)
	}
	if cached := o.lookup(sid); cached == nil || cached.SnapshotRef != mref {
		t.Fatalf("cached snapshot ref = %+v", cached)
	}
	select {
	case ev := <-events:
		if ev.Kind != "upsert" || ev.Route.SnapshotLocation != "remote" {
			t.Fatalf("route event = %+v", ev)
		}
	default:
		t.Fatal("promote did not publish a remote upsert")
	}
	if _, err := os.Stat(filepath.Dir(localRef)); !os.IsNotExist(err) {
		t.Fatalf("redundant local snapshot directory still exists: %v", err)
	}
}

func TestExportPromoteStoreFailurePreservesLocalState(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "node.db")
	o := testOrchCfgAt(t, &config.Config{}, dbPath)
	ctx := context.Background()
	mk := strings.Repeat("8", 64)
	raw, _ := hex.DecodeString(mk)
	apiKey, _ := apikey.Mint(raw)
	sid := "sbx-promote-fail"
	localRef := makeLocalSnapshot(t, dir, sid)
	installPromoteStub(t, "manifest://"+strings.Repeat("d", 64))

	sb := migrationSandbox(dir, sid, mk, localRef)
	if err := o.st.Put(ctx, sb); err != nil {
		t.Fatal(err)
	}
	o.cache(sb)
	installStoreTrigger(t, dbPath, `CREATE TRIGGER fail_snapshot_ref BEFORE UPDATE OF snapshot_ref ON sandboxes BEGIN SELECT RAISE(ABORT, 'forced snapshot ref failure'); END`)
	events, cancel := o.Subscribe()
	defer cancel()

	if _, err := o.ExportSandbox(ctx, apiKey, sid, true, true); err == nil || !strings.Contains(err.Error(), "persist promoted snapshot ref") {
		t.Fatalf("export error = %v; want persisted-ref failure", err)
	}
	stored, err := o.st.Get(ctx, sid)
	if err != nil || stored == nil || stored.SnapshotRef != localRef {
		t.Fatalf("stored snapshot changed after failure: %+v, err=%v", stored, err)
	}
	if cached := o.lookup(sid); cached == nil || cached.SnapshotRef != localRef {
		t.Fatalf("cached snapshot changed after failure: %+v", cached)
	}
	if _, err := os.Stat(localRef); err != nil {
		t.Fatalf("local snapshot removed after failed store update: %v", err)
	}
	select {
	case ev := <-events:
		t.Fatalf("unexpected route event after failed store update: %+v", ev)
	default:
	}
}

func TestExportMoveDeleteFailurePreservesSource(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "node.db")
	runtimePath := filepath.Join(dir, "rt-e2b.erofs")
	if err := os.WriteFile(runtimePath, []byte("fake-runtime-bytes"), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{}
	cfg.Sandbox.Boot.Runtime = runtimePath
	o := testOrchCfgAt(t, cfg, dbPath)
	ctx := context.Background()
	mk := strings.Repeat("9", 64)
	raw, _ := hex.DecodeString(mk)
	apiKey, _ := apikey.Mint(raw)
	sid := "sbx-delete-fail"
	mref := "manifest://" + strings.Repeat("e", 64)
	sb := migrationSandbox(dir, sid, mk, mref)
	if err := o.st.Put(ctx, sb); err != nil {
		t.Fatal(err)
	}
	o.cache(sb)
	installStoreTrigger(t, dbPath, `CREATE TRIGGER fail_delete BEFORE DELETE ON sandboxes BEGIN SELECT RAISE(ABORT, 'forced delete failure'); END`)
	events, cancel := o.Subscribe()
	defer cancel()

	tok, err := o.ExportSandbox(ctx, apiKey, sid, false, false)
	if err == nil || !strings.Contains(err.Error(), "delete source") || tok != "" {
		t.Fatalf("move export = token %q, err=%v; want empty token and delete error", tok, err)
	}
	stored, getErr := o.st.Get(ctx, sid)
	if getErr != nil || stored == nil {
		t.Fatalf("source row lost after failed delete: %+v, err=%v", stored, getErr)
	}
	if cached := o.lookup(sid); cached == nil {
		t.Fatal("source cache removed after failed delete")
	}
	select {
	case ev := <-events:
		t.Fatalf("unexpected route event after failed delete: %+v", ev)
	default:
	}
}

func migrationSandbox(dir, sid, mk, ref string) *types.Sandbox {
	return &types.Sandbox{
		ID: sid, TemplateID: "e2b-snp-" + strings.Repeat("a", 64), State: types.StatePaused,
		ManifestKey: mk, SnapshotRef: ref, RunDir: filepath.Join(dir, "run", sid),
		BaseDir: filepath.Join(dir, "lib", sid), CreatedUnix: 1,
	}
}

func makeLocalSnapshot(t *testing.T, dir, sid string) string {
	t.Helper()
	localDir := filepath.Join(dir, "saved", sid)
	if err := os.MkdirAll(localDir, 0o755); err != nil {
		t.Fatal(err)
	}
	ref := filepath.Join(localDir, sid+".snapshot")
	if err := os.WriteFile(ref, []byte("snapshot"), 0o644); err != nil {
		t.Fatal(err)
	}
	return ref
}

func installPromoteStub(t *testing.T, mref string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "sandbox-ctl")
	script := "#!/bin/sh\nprintf '%s\\n' '" + mref + "'\n"
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
}

func installStoreTrigger(t *testing.T, dbPath, statement string) {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(statement); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}
