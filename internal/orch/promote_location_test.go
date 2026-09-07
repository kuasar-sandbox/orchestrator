package orch

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// TestPromotePublishesUnderBareEntityID covers the undated publication
// contract: the location name is the bare entity id (here the sandbox row id;
// StableID-keying is covered separately) and the resolved URI carries only the
// SHA fan-out below the parent.
func TestPromotePublishesUnderBareEntityID(t *testing.T) {
	dir := t.TempDir()
	o := migrationOrchestrator(t, dir, []byte("runtime"))
	cfg := o.cfg
	cfg.Checkpoint.Remote.RefLocationParent = "file:///mnt/shared/snapshots"
	ctx := context.Background()
	mk := strings.Repeat("7", 64)
	_, apiKey := defaultTestCredentials(t, mk)
	sid := "0194a1b2-c3d4-7234-9abc-0123456789ab"
	localRef := makeLocalSnapshot(t, dir, sid)
	argsPath := filepath.Join(dir, "promote.args")
	binDir := t.TempDir()

	sb := migrationSandbox(t, dir, sid, mk, localRef)
	if err := o.st.Put(ctx, sb); err != nil {
		t.Fatal(err)
	}
	o.cache(sb)

	// The fake sandbox-ctl echoes back a located ref naming whatever
	// publication name promote passed on the command line.
	script := "#!/bin/sh\nprintf '%s\\n' \"$*\" > " + argsPath + "\n" +
		"for a in \"$@\"; do\n" +
		"  case \"$a\" in\n" +
		"    *=*)\n" +
		"      n=${a%%=*}\n" +
		"      printf '%s\\n' 'file://" + strings.Repeat("c", 64) + ".snapshot@location:'\"$n\"\n" +
		"      ;;\n" +
		"  esac\n" +
		"done\n"
	if err := os.WriteFile(filepath.Join(binDir, "sandbox-ctl"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))

	templateID, err := o.ExportSandbox(ctx, apiKey, sid, true, true)
	if err != nil {
		t.Fatal(err)
	}
	args, err := os.ReadFile(argsPath)
	if err != nil {
		t.Fatal(err)
	}
	// The publication name promote actually built is the value following
	// --to-ref-location, up to its "=".
	fields := strings.Fields(string(args))
	var locName string
	for i, f := range fields {
		if f == "--to-ref-location" && i+1 < len(fields) {
			if j := strings.Index(fields[i+1], "="); j > 0 {
				locName = fields[i+1][:j]
			}
		}
	}
	if locName == "" {
		t.Fatalf("promote args = %q, want a publication name=uri pair", args)
	}
	if locName != sid {
		t.Fatalf("publication name = %q, want the bare entity id %q", locName, sid)
	}
	locationURI, err := cfg.Checkpoint.RefLocationURI(locName)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(args), "--to-ref-location "+locName+"="+locationURI) {
		t.Fatalf("promote args = %q, want location %q resolved to %q", args, locName, locationURI)
	}
	// The URI must be the parent plus the SHA fan-out plus the name, with no
	// date-shaped segment anywhere between them.
	digest := fmt.Sprintf("%x", sha256.Sum256([]byte(locName)))
	wantURI := "file:///mnt/shared/snapshots/" + digest[:2] + "/" + digest[2:4] + "/" + locName
	if locationURI != wantURI {
		t.Fatalf("location URI = %q, want %q", locationURI, wantURI)
	}
	// And the returned template ref must carry that same publication name.
	tmpl, err := types.ParseTemplateID(templateID)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(tmpl.Ref, "@location:"+locName) {
		t.Fatalf("template ref %q does not carry the publication name %q", tmpl.Ref, locName)
	}
}
