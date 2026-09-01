package orch

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// TestPromoteBucketsByPublicationTimeNotEntityCreation covers the case where
// an entity created long before it exports must still land in a current
// (publication-date) partition: partitioning follows publication time, not
// entity creation time. The sandbox id below carries a 2025-era v7
// timestamp; the publication clock is pinned so the expected name (and
// partition) is a constant, not whatever today is.
func TestPromoteBucketsByPublicationTimeNotEntityCreation(t *testing.T) {
	dir := t.TempDir()
	cfg := &config.Config{}
	cfg.Checkpoint.LocalDir = filepath.Join(dir, "saved")
	cfg.Checkpoint.Remote.RefLocationParent = "file:///mnt/shared/snapshots"
	o := testOrchCfg(t, cfg)
	publishedAt := time.Date(2026, 8, 24, 23, 59, 0, 0, time.UTC)
	o.now = func() time.Time { return publishedAt }
	ctx := context.Background()
	mk := strings.Repeat("7", 64)
	_, apiKey := defaultTestCredentials(t, mk)
	// Old entity: 0194... is a 2025-era v7 timestamp.
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
		"    *-20*=*)\n" +
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
	// Extract the publication name promote actually built from the recorded
	// argv (the only <name>=<uri> pair).
	var locName string
	for _, f := range strings.Fields(string(args)) {
		if i := strings.Index(f, "="); i > 0 {
			candidate := f[:i]
			if strings.HasSuffix(candidate, "-20260824") {
				if _, err := cfg.Checkpoint.RefLocationURI(candidate); err == nil {
					locName = candidate
				}
			}
		}
	}
	if locName == "" {
		t.Fatalf("promote args = %q, want a publication name=uri pair dated 20260824", args)
	}
	// The bucket must be the pinned publication date, not the entity's 2025
	// timestamp: the name was built at publication time.
	if got := locName[len(locName)-8:]; got != "20260824" {
		t.Fatalf("publication date = %q, want %q", got, "20260824")
	}
	if !strings.HasPrefix(locName, sid+"-") {
		t.Fatalf("publication name %q does not start with the entity id %q", locName, sid)
	}
	locationURI, err := cfg.Checkpoint.RefLocationURI(locName)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(args), "--to-ref-location "+locName+"="+locationURI) {
		t.Fatalf("promote args = %q, want location %q resolved to %q", args, locName, locationURI)
	}
	if !strings.Contains(locationURI, "/20260824/") {
		t.Fatalf("location URI %q is not bucketed by publication date", locationURI)
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
