package orch

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
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
		"      printf '%s\\n' '{\"snapshotRef\":\"file://" + strings.Repeat("c", 64) + ".snapshot@location:'\"$n\"'\",\"sandboxRef\":\"manifest://" + strings.Repeat("e", 64) + "\",\"removedRefs\":[]}'\n" +
		"      ;;\n" +
		"  esac\n" +
		"done\n"
	if err := os.WriteFile(filepath.Join(binDir, "sandbox-ctl"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))

	templateID, err := o.exportSandboxTokenForTest(ctx, apiKey, sid, true, true)
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

// writeEchoSandboxCtl installs a fake sandbox-ctl that records its argv and
// echoes back a located ref naming whatever publication name promote passed
// on the command line.
func writeEchoSandboxCtl(t *testing.T, binDir, argsPath, ext string) {
	t.Helper()
	script := "#!/bin/sh\nprintf '%s\\n' \"$*\" > " + argsPath + "\n" +
		"for a in \"$@\"; do\n" +
		"  case \"$a\" in\n" +
		"    *=*)\n" +
		"      n=${a%%=*}\n" +
		"      printf '%s\\n' '{\"snapshotRef\":\"file://" + strings.Repeat("c", 64) + ext + "@location:'\"$n\"'\",\"sandboxRef\":\"manifest://" + strings.Repeat("e", 64) + "\",\"removedRefs\":[]}'\n" +
		"      ;;\n" +
		"  esac\n" +
		"done\n"
	if err := os.WriteFile(filepath.Join(binDir, "sandbox-ctl"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))
}

// recordedPromotePair returns the <name>=<uri> pair promote passed to
// sandbox-ctl, read back from the recorded argv.
func recordedPromotePair(t *testing.T, argsPath string) (string, string) {
	t.Helper()
	args, err := os.ReadFile(argsPath)
	if err != nil {
		t.Fatal(err)
	}
	fields := strings.Fields(string(args))
	for i, f := range fields {
		if f == "--to-ref-location" && i+1 < len(fields) {
			if j := strings.Index(fields[i+1], "="); j > 0 {
				return fields[i+1][:j], fields[i+1][j+1:]
			}
		}
	}
	t.Fatalf("promote args = %q, want a publication name=uri pair", args)
	return "", ""
}

// promoteLocationOrchestrator builds the orchestrator and fake sandbox-ctl
// harness shared by the StableID-keying tests.
func promoteLocationOrchestrator(t *testing.T, dir string) (*Orchestrator, *config.Config, string) {
	t.Helper()
	o := migrationOrchestrator(t, dir, []byte("runtime"))
	cfg := o.cfg
	cfg.Checkpoint.Remote.RefLocationParent = "file:///mnt/shared/snapshots"
	argsPath := filepath.Join(dir, "promote.args")
	writeEchoSandboxCtl(t, t.TempDir(), argsPath, ".snapshot")
	return o, cfg, argsPath
}

// TestPromoteKeysPublicationByStableID covers the rename case: a row imported
// under a new node-local id keeps its StableIDValue, and re-export must key
// the publication location by that stable id (the pre-rename directory), not
// by the row id.
func TestPromoteKeysPublicationByStableID(t *testing.T) {
	dir := t.TempDir()
	o, cfg, argsPath := promoteLocationOrchestrator(t, dir)
	ctx := context.Background()
	mk := strings.Repeat("7", 64)
	_, apiKey := defaultTestCredentials(t, mk)
	sid := "target-g1"
	localRef := makeLocalSnapshot(t, dir, sid)

	sb := migrationSandbox(t, dir, sid, mk, localRef)
	sb.StableIDValue = "logical"
	// Credentials bind the stable id; re-materialize after it is set.
	if err := materializeSandboxCredentials(sb, sandboxcfg.Credentials{}); err != nil {
		t.Fatal(err)
	}
	if err := o.st.Put(ctx, sb); err != nil {
		t.Fatal(err)
	}
	o.cache(sb)

	templateID, err := o.exportSandboxTokenForTest(ctx, apiKey, sid, true, true)
	if err != nil {
		t.Fatal(err)
	}
	locName, uri := recordedPromotePair(t, argsPath)
	if locName != "logical" {
		t.Fatalf("publication name = %q, want the stable id %q", locName, "logical")
	}
	if want := mustRefLocationURI(t, cfg, "logical"); uri != want {
		t.Fatalf("publication URI = %q, want %q", uri, want)
	}
	if other := mustRefLocationURI(t, cfg, sid); uri == other {
		t.Fatalf("publication location keyed by the row id %q", sid)
	}
	tmpl, err := types.ParseTemplateID(templateID)
	if err != nil || !strings.Contains(tmpl.Ref, "@location:logical") {
		t.Fatalf("template = %#v, %v; want ref carrying location %q", tmpl, err, "logical")
	}
}

// TestPromoteSharedStableIDPublishesToOneLocation pins the identity-preserving
// copy semantics: rows sharing a stable id publish into the one location of
// that logical entity, so their artifacts live and die together.
func TestPromoteSharedStableIDPublishesToOneLocation(t *testing.T) {
	dir := t.TempDir()
	o, _, argsPath := promoteLocationOrchestrator(t, dir)
	ctx := context.Background()
	mk := strings.Repeat("7", 64)
	_, apiKey := defaultTestCredentials(t, mk)

	var uris []string
	for _, sid := range []string{"copy-a", "copy-b"} {
		localRef := makeLocalSnapshot(t, dir, sid)
		sb := migrationSandbox(t, dir, sid, mk, localRef)
		sb.StableIDValue = "logical"
		// Credentials bind the stable id; re-materialize after it is set.
		if err := materializeSandboxCredentials(sb, sandboxcfg.Credentials{}); err != nil {
			t.Fatal(err)
		}
		if err := o.st.Put(ctx, sb); err != nil {
			t.Fatal(err)
		}
		o.cache(sb)
		if _, err := o.exportSandboxTokenForTest(ctx, apiKey, sid, true, true); err != nil {
			t.Fatal(err)
		}
		_, uri := recordedPromotePair(t, argsPath)
		uris = append(uris, uri)
	}
	if uris[0] == "" || uris[0] != uris[1] {
		t.Fatalf("shared-stable-id rows published to different locations: %q vs %q", uris[0], uris[1])
	}
}

// TestPromoteForkPublishesWithoutParentLocationRegistration pins the fork
// contract: a row whose template (and therefore retained parent chain) lives in
// ANOTHER entity's location publishes with only its own target location. The
// sandboxer publisher short-circuits already-portable refs without reading
// them (sandboxer #176), so promote deliberately registers no parent input
// location; if that publisher contract ever changes, this test documents what
// breaks.
func TestPromoteForkPublishesWithoutParentLocationRegistration(t *testing.T) {
	dir := t.TempDir()
	o, cfg, argsPath := promoteLocationOrchestrator(t, dir)
	ctx := context.Background()
	mk := strings.Repeat("7", 64)
	_, apiKey := defaultTestCredentials(t, mk)
	// The fork row: its own id keys the publication target, while its template
	// ref points into the parent entity's location directory.
	parentName := "e2e-stable-src-01"
	parentRef := "file://" + strings.Repeat("c", 64) + ".sandbox@hmac:" + strings.Repeat("c", 64) + "@location:" + parentName
	tmpl := types.TemplateID{Profile: types.ProfileBare, Kind: types.KindSbx, Ref: parentRef}.String()
	fork := "e2e-fork-01"
	localRef := makeLocalSnapshot(t, dir, fork)

	sb := migrationSandbox(t, dir, fork, mk, localRef)
	sb.Profile = types.ProfileBare
	sb.EnvdAccessToken = ""
	sb.TrafficAccessToken = ""
	sb.TemplateID = tmpl
	if err := o.st.Put(ctx, sb); err != nil {
		t.Fatal(err)
	}
	o.cache(sb)

	if _, err := o.exportSandboxTokenForTest(ctx, apiKey, fork, true, true); err != nil {
		t.Fatal(err)
	}
	args, err := os.ReadFile(argsPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(args), "--to-ref-location "+fork+"="+mustRefLocationURI(t, cfg, fork)) {
		t.Fatalf("fork promote args = %q, want its own target location", args)
	}
	// The parent's location must NOT be registered as an input: the publisher
	// resolves retained parent refs by short-circuit, not by reading them.
	if strings.Contains(string(args), "--ref-location") {
		t.Fatalf("fork promote args registered an input location: %q", args)
	}
	if strings.Contains(string(args), parentName) {
		t.Fatalf("fork promote args leaked the parent location name: %q", args)
	}
}

// TestPromoteRepublishReusesName covers the re-pause loop: after an export,
// a later pause and re-export of the same entity must derive the identical
// publication name (and directory) with no clock or persisted state involved.
func TestPromoteRepublishReusesName(t *testing.T) {
	dir := t.TempDir()
	o, _, argsPath := promoteLocationOrchestrator(t, dir)
	ctx := context.Background()
	mk := strings.Repeat("7", 64)
	_, apiKey := defaultTestCredentials(t, mk)
	sid := "0198f7a1-1234-7234-9abc-0123456789ab"
	localRef := makeLocalSnapshot(t, dir, sid)

	sb := migrationSandbox(t, dir, sid, mk, localRef)
	if err := o.st.Put(ctx, sb); err != nil {
		t.Fatal(err)
	}
	o.cache(sb)
	if _, err := o.exportSandboxTokenForTest(ctx, apiKey, sid, true, true); err != nil {
		t.Fatal(err)
	}
	first, err := os.ReadFile(argsPath)
	if err != nil {
		t.Fatal(err)
	}

	// Re-pause: the durable row points at a fresh local capture again.
	makeLocalSnapshot(t, dir, sid)
	current, err := o.st.Get(ctx, sid)
	if err != nil || current == nil {
		t.Fatalf("get paused row: %v", err)
	}
	current.ResumeSource = types.ResumeSource{SandboxRef: "manifest://eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", Kind: types.ResumeSourceSnapshot, Ref: localRef}
	if err := o.st.Put(ctx, current); err != nil {
		t.Fatal(err)
	}
	o.cache(current)
	if _, err := o.exportSandboxTokenForTest(ctx, apiKey, sid, true, true); err != nil {
		t.Fatal(err)
	}
	second, err := os.ReadFile(argsPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(first) != string(second) {
		t.Fatalf("re-export changed the publication argv:\nfirst:  %q\nsecond: %q", first, second)
	}
}
