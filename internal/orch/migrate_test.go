package orch

import (
	"context"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/apikey"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/config"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/types"
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
	cfg.Sandbox.Boot.RuntimeE2B = rt
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

	// export (move): mints a token, deletes the source row.
	tok, err := o.ExportSandbox(ctx, apiKey, sid, false, false)
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	if s, _ := o.st.Get(ctx, sid); s != nil {
		t.Fatal("move export should delete the source row")
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
