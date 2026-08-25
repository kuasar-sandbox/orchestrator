package orch

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/migrationtoken"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
)

func TestExportImportKMT1RoundTripPreservesIdentityStateAndCredentials(t *testing.T) {
	dir := t.TempDir()
	o := migrationOrchestrator(t, dir, []byte("fake-runtime-bytes"))
	ctx := context.Background()

	mk := strings.Repeat("6", 64)
	apiSecret, apiKey := defaultTestCredentials(t, mk)
	pair := store.KeyPair{APISecret: apiSecret, ManifestKey: mk}
	if _, err := o.st.AddKeyPair(ctx, pair, "", 0, ""); err != nil {
		t.Fatal(err)
	}

	sid := "stable-sandbox-g0"
	sb := migrationSandbox(t, dir, sid, mk, "manifest://"+strings.Repeat("b", 64))
	sb.AuthSandboxIDValue = "stable-sandbox"
	sb.DeadlineUnix = 1_900_000_000
	sb.Env = map[string]string{"FOO": "bar"}
	sb.Metadata = map[string]string{
		"k": "v", sandboxcfg.NsRestore: `{"prefetch":"memory"}`,
	}
	sb.EnvdAccessToken = "source-envd-token"
	sb.TrafficAccessToken = "source-traffic-token"
	if err := os.MkdirAll(sb.RunDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := materializeSandboxCredentials(sb, sandboxcfg.Credentials{
		EnvdAccessToken: sb.EnvdAccessToken, TrafficAccessToken: sb.TrafficAccessToken,
	}); err != nil {
		t.Fatal(err)
	}
	if err := o.st.Put(ctx, sb); err != nil {
		t.Fatal(err)
	}
	o.cache(sb)

	tok, err := o.ExportSandbox(ctx, apiKey, sid, false, false)
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	if !strings.HasPrefix(tok, "kmt1.") {
		t.Fatal("exported token does not have the kmt1 prefix")
	}
	payload, err := migrationtoken.Open(migrationtoken.KeyMaterial{
		APISecret: pair.APISecret, ManifestKey: pair.ManifestKey,
	}, tok)
	if err != nil {
		t.Fatalf("open exported token: %v", err)
	}
	if payload.NodeSandboxID != sid || payload.AuthSandboxID != sb.AuthSandboxID() ||
		payload.TemplateID != sb.TemplateID || payload.Profile != string(sb.Profile) ||
		payload.SnapshotRef != sb.SnapshotRef || payload.CreatedUnix != sb.CreatedUnix ||
		payload.DeadlineUnix != sb.DeadlineUnix {
		t.Fatal("exported payload lost one or more portable sandbox fields")
	}
	assertMigrationCredentialsEqual(t, payloadCredentials(payload), sandboxCredentials(sb))

	if s, _ := o.st.Get(ctx, sid); s != nil {
		t.Fatal("move export should delete the source row")
	}
	if s := o.lookup(sid); s != nil {
		t.Fatal("move export should uncache the source row")
	}
	if _, err := os.Stat(sb.RunDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("move export retained source run directory: %v", err)
	}
	// Standalone import defaults to the source NodeSandboxID after a move.
	imported, err := o.ImportSandbox(ctx, apiKey, tok, "")
	if err != nil || imported != sid {
		t.Fatalf("import: imported=%q err=%v", imported, err)
	}
	got, _ := o.st.Get(ctx, imported)
	if got == nil || got.ID != sid || got.AuthSandboxID() != sb.AuthSandboxID() || got.Cluster != nil ||
		got.State != types.StatePaused || got.Profile != sb.Profile || got.TemplateID != sb.TemplateID ||
		got.SnapshotRef != sb.SnapshotRef || got.CreatedUnix != sb.CreatedUnix ||
		got.DeadlineUnix != sb.DeadlineUnix || !reflect.DeepEqual(got.Env, sb.Env) ||
		!reflect.DeepEqual(got.Metadata, sb.Metadata) {
		t.Fatal("imported row lost one or more portable sandbox fields")
	}
	assertMigrationCredentialsEqual(t, sandboxCredentials(got), sandboxCredentials(sb))
}

func TestMigrationTokenCarriesOnlyStrictPortableResourceMetadata(t *testing.T) {
	dir := t.TempDir()
	o := migrationOrchestrator(t, dir, []byte("runtime"))
	sb := migrationSandbox(t, dir, "portable-resource", strings.Repeat("6", 64), "manifest://"+strings.Repeat("b", 64))
	sb.Metadata = map[string]string{
		sandboxcfg.NsResource: ` { "startup" : { "memory" : "1GiB" }, "capacity" : { "memory" : "8GiB" } } `,
		"ordinary":            "preserved",
	}

	token, err := o.mintSandboxToken(sb, sb.SnapshotRef)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := migrationtoken.Open(migrationtoken.KeyMaterial{
		APISecret: sb.APISecret, ManifestKey: sb.ManifestKey,
	}, token)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := payload.Metadata[sandboxcfg.NsResource], `{"capacity":{"memory":"8GiB"},"startup":{"memory":"1GiB"}}`; got != want {
		t.Fatalf("portable resource metadata = %q, want %q", got, want)
	}
	if payload.Metadata["ordinary"] != "preserved" {
		t.Fatal("ordinary portable metadata was dropped")
	}

	sb.Metadata[sandboxcfg.NsResource] = `{"control":{"controller":"/run/foreign.sock"}}`
	if _, err := o.mintSandboxToken(sb, sb.SnapshotRef); err == nil || !strings.Contains(err.Error(), "node-managed") {
		t.Fatalf("node-owned migration resource error = %v", err)
	}
}

func TestMigrationRestoreReappliesTargetNodeResourcePolicy(t *testing.T) {
	dir := t.TempDir()
	o := migrationOrchestrator(t, dir, []byte("runtime"))
	o.cfg.ResourceListen = &config.ResourceListenConfig{Enabled: true}
	o.cfg.Sandbox.Resources = config.ResourcesConfig(sandboxcfg.NodeResourcePolicy{
		Allocatable: sandboxcfg.NodeAllocatablePolicy{Memory: "128MiB"},
		Overhead:    sandboxcfg.NodeOverheadPolicy{Memory: "64MiB"},
	})
	o.cfg.Sandbox.Network.E2B.InnerIP = "169.254.0.21/30"
	o.cfg.Sandbox.Network.E2B.Nexthop = "169.254.0.22"
	o.SetResourceControllerSocketIdentity("/target/resource.sock")

	manifestKey := strings.Repeat("7", 64)
	apiSecret, apiKey := defaultTestCredentials(t, manifestKey)
	if _, err := o.st.AddKeyPair(context.Background(), store.KeyPair{
		APISecret: apiSecret, ManifestKey: manifestKey,
	}, "", 0, ""); err != nil {
		t.Fatal(err)
	}
	source := migrationSandbox(t, dir, "source-resource", manifestKey, "manifest://"+strings.Repeat("c", 64))
	source.Metadata = map[string]string{
		sandboxcfg.NsResource: `{"capacity":{"cpu":2,"memory":"8GiB"},"allocatable":{"memory":"512MiB"},"startup":{"memory":"1GiB"}}`,
	}
	token, err := o.mintSandboxToken(source, source.SnapshotRef)
	if err != nil {
		t.Fatal(err)
	}
	targetID, err := o.ImportSandbox(context.Background(), apiKey, token, "target-resource")
	if err != nil {
		t.Fatal(err)
	}
	target, err := o.st.Get(context.Background(), targetID)
	if err != nil || target == nil {
		t.Fatalf("imported target = %+v, err=%v", target, err)
	}
	template, err := types.ParseTemplateID(target.TemplateID)
	if err != nil {
		t.Fatal(err)
	}
	preparation, err := o.prepareSandboxLaunch(context.Background(), target, template)
	if err != nil {
		t.Fatal(err)
	}
	resources, err := o.resolveRestoredResources(preparation.Spec, rtconfig.CapacityConfig{CPU: 2, Memory: "8GiB"})
	if err != nil {
		t.Fatal(err)
	}
	if resources.Capacity.CPU != 2 || resources.Capacity.Memory != "8GiB" ||
		resources.Allocatable.CPU != 2 || resources.Allocatable.Memory != "512MiB" ||
		resources.Startup == nil || resources.Startup.Memory != "1GiB" ||
		resources.Overhead == nil || resources.Overhead.Memory != "64MiB" ||
		resources.Control.Controller != "/target/resource.sock" ||
		resources.Allocatable.DeflateOnOOM == nil || !*resources.Allocatable.DeflateOnOOM {
		t.Fatalf("target-resolved migration resources = %+v", resources)
	}
}

func TestExportSandboxReturnsTypedClientErrors(t *testing.T) {
	dir := t.TempDir()
	o := migrationOrchestrator(t, dir, []byte("runtime"))
	ctx := context.Background()
	mk := strings.Repeat("6", 64)
	_, apiKey := defaultTestCredentials(t, mk)

	if _, err := o.ExportSandbox(ctx, apiKey, "missing", false, true); !errors.Is(err, api.ErrNotFound) {
		t.Fatalf("missing export error = %v, want ErrNotFound", err)
	}

	sb := migrationSandbox(t, dir, "running", mk, "manifest://"+strings.Repeat("b", 64))
	sb.State = types.StateRunning
	if err := o.st.Put(ctx, sb); err != nil {
		t.Fatal(err)
	}
	if _, err := o.ExportSandbox(ctx, "wrong-api-key", sb.ID, false, true); !errors.Is(err, api.ErrNotFound) {
		t.Fatalf("non-owner export error = %v, want ErrNotFound", err)
	}
	if _, err := o.ExportSandbox(ctx, apiKey, sb.ID, false, true); !errors.Is(err, api.ErrBadRequest) {
		t.Fatalf("running export error = %v, want ErrBadRequest", err)
	}
}

func TestImportExplicitTargetPreservesAuthSubjectAndCredentials(t *testing.T) {
	dir := t.TempDir()
	o := migrationOrchestrator(t, dir, []byte("runtime"))
	ctx := context.Background()
	mk := strings.Repeat("6", 64)
	apiSecret, apiKey := defaultTestCredentials(t, mk)
	if _, err := o.st.AddKeyPair(ctx, store.KeyPair{APISecret: apiSecret, ManifestKey: mk}, "", 0, ""); err != nil {
		t.Fatal(err)
	}
	source := migrationSandbox(t, dir, "logical-g0", mk, "manifest://"+strings.Repeat("b", 64))
	source.AuthSandboxIDValue = "logical"
	source.Metadata = map[string]string{
		"ordinary":               "preserved",
		sandboxcfg.NsCredentials: `{"service_secret":"must-not-reenter"}`,
	}
	if err := materializeSandboxCredentials(source, sandboxcfg.Credentials{}); err != nil {
		t.Fatal(err)
	}
	if err := o.st.Put(ctx, source); err != nil {
		t.Fatal(err)
	}
	token, err := o.ExportSandbox(ctx, apiKey, source.ID, false, true)
	if err != nil {
		t.Fatal(err)
	}

	targetID := "logical-g1"
	gotID, err := o.ImportSandbox(ctx, apiKey, token, targetID)
	if err != nil || gotID != targetID {
		t.Fatalf("explicit import = %q, %v", gotID, err)
	}
	got, err := o.st.Get(ctx, targetID)
	if err != nil || got == nil {
		t.Fatalf("get explicit target: %v", err)
	}
	if got.AuthSandboxID() != source.AuthSandboxID() || got.ID == source.ID {
		t.Fatal("explicit target changed the authentication subject or retained the source node ID")
	}
	if got.Metadata["ordinary"] != "preserved" {
		t.Fatal("standalone import lost ordinary metadata")
	}
	if _, found := got.Metadata[sandboxcfg.NsCredentials]; found {
		t.Fatal("credentials namespace re-entered standalone sandbox metadata")
	}
	assertMigrationCredentialsEqual(t, sandboxCredentials(got), sandboxCredentials(source))
	if retained, err := o.st.Get(ctx, source.ID); err != nil || retained == nil {
		t.Fatalf("retained-source export removed source: %v", err)
	}
}

func TestImportIsInsertOnlyAndMapsDuplicateToAlreadyExists(t *testing.T) {
	dir := t.TempDir()
	o := migrationOrchestrator(t, dir, []byte("runtime"))
	ctx := context.Background()
	mk := strings.Repeat("6", 64)
	apiSecret, apiKey := defaultTestCredentials(t, mk)
	if _, err := o.st.AddKeyPair(ctx, store.KeyPair{APISecret: apiSecret, ManifestKey: mk}, "", 0, ""); err != nil {
		t.Fatal(err)
	}
	source := migrationSandbox(t, dir, "source", mk, "manifest://"+strings.Repeat("b", 64))
	token, err := o.mintSandboxToken(source, source.SnapshotRef)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := o.ImportSandbox(ctx, apiKey, token, "target"); err != nil {
		t.Fatalf("first import: %v", err)
	}
	before, err := o.st.Get(ctx, "target")
	if err != nil || before == nil {
		t.Fatal(err)
	}
	if _, err := o.ImportSandbox(ctx, apiKey, token, "target"); !errors.Is(err, api.ErrAlreadyExists) {
		t.Fatalf("second import error = %v, want ErrAlreadyExists", err)
	}
	after, err := o.st.Get(ctx, "target")
	if err != nil || !reflect.DeepEqual(after, before) {
		t.Fatalf("duplicate import changed existing row: %v", err)
	}
}

func TestImportWithTrustedExpectationsAndClusterContext(t *testing.T) {
	dir := t.TempDir()
	o := migrationOrchestrator(t, dir, []byte("runtime"))
	source := migrationSandbox(t, dir, "logical-g0", strings.Repeat("6", 64), "manifest://"+strings.Repeat("b", 64))
	source.AuthSandboxIDValue = "logical"
	source.Metadata = map[string]string{
		"ordinary":                     "preserved",
		clusterstate.ObjectMetadataKey: "untrusted-binding",
		sandboxcfg.NsCredentials:       `{"service_secret":"must-not-reenter"}`,
	}
	if err := materializeSandboxCredentials(source, sandboxcfg.Credentials{}); err != nil {
		t.Fatal(err)
	}
	token, err := o.mintSandboxToken(source, source.SnapshotRef)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := sha256File(o.runtimeFileFor(source.Profile))
	if err != nil {
		t.Fatal(err)
	}
	cluster := &types.ClusterSandboxContext{Group: "/tenant/workloads", RouteKey: "route-1"}
	imported, err := o.importSandboxWithKey(context.Background(), store.KeyPair{
		APISecret: source.APISecret, ManifestKey: source.ManifestKey,
	}, token, "logical-g1", migrationtoken.Expectations{
		AuthSandboxID: source.AuthSandboxID(),
		TemplateID:    source.TemplateID,
		Profile:       source.Profile,
		RuntimeDigest: digest,
		SnapshotRef:   source.SnapshotRef,
	}, cluster, 0)
	if err != nil {
		t.Fatal(err)
	}
	cluster.Group = "/mutated"
	if imported.ID != "logical-g1" || imported.AuthSandboxID() != "logical" || imported.Cluster == nil ||
		imported.Cluster.Group != "/tenant/workloads" || imported.Cluster.RouteKey != "route-1" {
		t.Fatal("trusted import context was not preserved")
	}
	if imported.Metadata["ordinary"] != "preserved" {
		t.Fatal("ordinary migration metadata was not preserved")
	}
	if _, found := imported.Metadata[clusterstate.ObjectMetadataKey]; found {
		t.Fatal("untrusted execution binding metadata entered cluster import")
	}
	if _, found := imported.Metadata[sandboxcfg.NsCredentials]; found {
		t.Fatal("credentials namespace re-entered cluster sandbox metadata")
	}
}

func TestImportRejectsTenantRuntimeAndTrustedExpectationMismatch(t *testing.T) {
	dir := t.TempDir()
	o := migrationOrchestrator(t, dir, []byte("runtime-a"))
	source := migrationSandbox(t, dir, "source", strings.Repeat("6", 64), "manifest://"+strings.Repeat("b", 64))
	token, err := o.mintSandboxToken(source, source.SnapshotRef)
	if err != nil {
		t.Fatal(err)
	}
	pair := store.KeyPair{APISecret: source.APISecret, ManifestKey: source.ManifestKey}

	t.Run("API secret", func(t *testing.T) {
		wrong := pair
		wrong.APISecret = strings.Repeat("1", 64)
		_, err := o.importSandboxWithKey(context.Background(), wrong, token, "api-mismatch", migrationtoken.Expectations{}, nil, 0)
		if !errors.Is(err, migrationtoken.ErrCredentialMismatch) {
			t.Fatalf("error = %v, want credential mismatch", err)
		}
	})
	t.Run("manifest key", func(t *testing.T) {
		wrong := pair
		wrong.ManifestKey = strings.Repeat("2", 64)
		_, err := o.importSandboxWithKey(context.Background(), wrong, token, "manifest-mismatch", migrationtoken.Expectations{}, nil, 0)
		if !errors.Is(err, migrationtoken.ErrAuthentication) {
			t.Fatalf("error = %v, want authentication failure", err)
		}
	})

	for name, expected := range map[string]migrationtoken.Expectations{
		"subject":  {AuthSandboxID: "different-subject"},
		"template": {TemplateID: types.TemplateID{Profile: types.ProfileE2B, Kind: types.KindSnp, Ref: "manifest://" + strings.Repeat("c", 64)}.String()},
		"profile":  {Profile: types.ProfileBare},
		"snapshot": {SnapshotRef: "manifest://" + strings.Repeat("d", 64)},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := o.importSandboxWithKey(context.Background(), pair, token, name+"-mismatch", expected, nil, 0)
			if !errors.Is(err, migrationtoken.ErrIncompatible) {
				t.Fatalf("error = %v, want incompatible target", err)
			}
		})
	}

	if err := os.WriteFile(o.runtimeFileFor(source.Profile), []byte("runtime-b"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := o.importSandboxWithKey(context.Background(), pair, token, "runtime-mismatch", migrationtoken.Expectations{}, nil, 0); !errors.Is(err, migrationtoken.ErrIncompatible) {
		t.Fatalf("runtime mismatch error = %v, want incompatible target", err)
	}
}

func TestImportRejectsInvalidExplicitTarget(t *testing.T) {
	dir := t.TempDir()
	o := migrationOrchestrator(t, dir, []byte("runtime"))
	source := migrationSandbox(t, dir, "source", strings.Repeat("6", 64), "manifest://"+strings.Repeat("b", 64))
	token, err := o.mintSandboxToken(source, source.SnapshotRef)
	if err != nil {
		t.Fatal(err)
	}
	pair := store.KeyPair{APISecret: source.APISecret, ManifestKey: source.ManifestKey}
	for _, target := range []string{"UPPER", "has/slash", "-prefix", "suffix-", strings.Repeat("a", 58)} {
		t.Run(target, func(t *testing.T) {
			if _, err := o.importSandboxWithKey(context.Background(), pair, token, target, migrationtoken.Expectations{}, nil, 0); err == nil ||
				!strings.Contains(err.Error(), "invalid target sandbox ID") {
				t.Fatalf("target %q error = %v", target, err)
			}
		})
	}
}

func TestMintSandboxTokenRejectsProfileThatDoesNotMatchTemplate(t *testing.T) {
	o := testOrch(t)
	sb := &types.Sandbox{Profile: types.ProfileBare, TemplateID: types.TemplateID{Profile: types.ProfileE2B, Kind: types.KindSnp, Ref: "manifest://" + strings.Repeat("a", 64)}.String()}
	if _, err := o.mintSandboxToken(sb, "manifest://"+strings.Repeat("b", 64)); err == nil ||
		!strings.Contains(err.Error(), "does not match template profile") {
		t.Fatalf("mint mismatch error = %v", err)
	}
}

func TestExportPromotesLocalSnapshotState(t *testing.T) {
	dir := t.TempDir()
	o := testOrch(t)
	ctx := context.Background()
	mk := strings.Repeat("7", 64)
	_, apiKey := defaultTestCredentials(t, mk)
	sid := "sbx-promote-ok"
	localRef := makeLocalSnapshot(t, dir, sid)
	mref := "manifest://" + strings.Repeat("c", 64)
	installPromoteStub(t, mref)

	sb := migrationSandbox(t, dir, sid, mk, localRef)
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
		t.Fatalf("stored snapshot ref was not promoted: %v", err)
	}
	if cached := o.lookup(sid); cached == nil || cached.SnapshotRef != mref {
		t.Fatal("cached snapshot ref was not promoted")
	}
	select {
	case ev := <-events:
		if ev.Kind != "upsert" || ev.Route.SnapshotLocation != "remote" {
			t.Fatal("promote published the wrong route event")
		}
	default:
		t.Fatal("promote did not publish a remote upsert")
	}
	if _, err := os.Stat(filepath.Dir(localRef)); !os.IsNotExist(err) {
		t.Fatalf("redundant local snapshot directory still exists: %v", err)
	}
}

func TestExportPublishesLocatedSnapshotAndReturnsTemplate(t *testing.T) {
	dir := t.TempDir()
	cfg := &config.Config{}
	cfg.Checkpoint.Remote.RefLocationParent = "file:///mnt/shared/snapshots"
	o := testOrchCfg(t, cfg)
	ctx := context.Background()
	mk := strings.Repeat("7", 64)
	_, apiKey := defaultTestCredentials(t, mk)
	sid := "0198f7a1-1234"
	localRef := makeLocalSnapshot(t, dir, sid)
	portableRef := "file://" + strings.Repeat("c", 64) + ".bundle@location:" + sid
	argsPath := filepath.Join(dir, "promote.args")
	binDir := t.TempDir()
	script := "#!/bin/sh\nprintf '%s\\n' \"$*\" > " + argsPath + "\nprintf '%s\\n' '" + portableRef + "'\n"
	if err := os.WriteFile(filepath.Join(binDir, "sandbox-ctl"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))

	sb := migrationSandbox(t, dir, sid, mk, localRef)
	if err := o.st.Put(ctx, sb); err != nil {
		t.Fatal(err)
	}
	o.cache(sb)
	templateID, err := o.ExportSandbox(ctx, apiKey, sid, true, true)
	if err != nil {
		t.Fatal(err)
	}
	tmpl, err := types.ParseTemplateID(templateID)
	if err != nil || tmpl.Ref != portableRef || tmpl.Kind != types.KindSnp {
		t.Fatalf("template = %#v, %v", tmpl, err)
	}
	args, err := os.ReadFile(argsPath)
	if err != nil {
		t.Fatal(err)
	}
	locationURI, _ := cfg.Checkpoint.RefLocationURI(sid)
	if !strings.Contains(string(args), "--to-ref-location "+sid+"="+locationURI) {
		t.Fatalf("promote args = %q", args)
	}
}

func TestExportPromoteStoreFailurePreservesLocalState(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "node.db")
	o := testOrchCfgAt(t, &config.Config{}, dbPath)
	ctx := context.Background()
	mk := strings.Repeat("8", 64)
	_, apiKey := defaultTestCredentials(t, mk)
	sid := "sbx-promote-fail"
	localRef := makeLocalSnapshot(t, dir, sid)
	installPromoteStub(t, "manifest://"+strings.Repeat("d", 64))

	sb := migrationSandbox(t, dir, sid, mk, localRef)
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
		t.Fatalf("stored snapshot changed after failure: %v", err)
	}
	if cached := o.lookup(sid); cached == nil || cached.SnapshotRef != localRef {
		t.Fatal("cached snapshot changed after failure")
	}
	if _, err := os.Stat(localRef); err != nil {
		t.Fatalf("local snapshot removed after failed store update: %v", err)
	}
	select {
	case <-events:
		t.Fatal("unexpected route event after failed store update")
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
	_, apiKey := defaultTestCredentials(t, mk)
	sid := "sbx-delete-fail"
	localRef := makeLocalSnapshot(t, dir, sid)
	installPromoteStub(t, "manifest://"+strings.Repeat("e", 64))
	sb := migrationSandbox(t, dir, sid, mk, localRef)
	if err := o.st.Put(ctx, sb); err != nil {
		t.Fatal(err)
	}
	o.cache(sb)
	installStoreTrigger(t, dbPath, `CREATE TRIGGER fail_delete BEFORE DELETE ON sandboxes BEGIN SELECT RAISE(ABORT, 'forced delete failure'); END`)
	events, cancel := o.Subscribe()
	defer cancel()

	tok, err := o.ExportSandbox(ctx, apiKey, sid, false, false)
	if err == nil || !strings.Contains(err.Error(), "delete source") || tok != "" {
		t.Fatalf("move export returned the wrong token/error state: %v", err)
	}
	stored, getErr := o.st.Get(ctx, sid)
	if getErr != nil || stored == nil || stored.SnapshotRef != localRef {
		t.Fatalf("source row lost after failed delete: %v", getErr)
	}
	if cached := o.lookup(sid); cached == nil {
		t.Fatal("source cache removed after failed delete")
	}
	select {
	case <-events:
		t.Fatal("unexpected route event after failed delete")
	default:
	}
	if _, err := os.Stat(localRef); err != nil {
		t.Fatalf("local snapshot removed after failed source delete: %v", err)
	}
}

func migrationSandbox(t *testing.T, dir, sid, mk, ref string) *types.Sandbox {
	t.Helper()
	sb := &types.Sandbox{
		ID: sid, Profile: types.ProfileE2B, TemplateID: types.TemplateID{Profile: types.ProfileE2B, Kind: types.KindSnp, Ref: "manifest://" + strings.Repeat("a", 64)}.String(), State: types.StatePaused,
		APISecret: deriveTestAPISecret(t, mk), ManifestKey: mk, SnapshotRef: ref, RunDir: filepath.Join(dir, "run", sid),
		BaseDir: filepath.Join(dir, "lib", sid), CreatedUnix: 1,
	}
	if err := materializeSandboxCredentials(sb, sandboxcfg.Credentials{}); err != nil {
		t.Fatal(err)
	}
	return sb
}

func migrationOrchestrator(t *testing.T, dir string, runtime []byte) *Orchestrator {
	t.Helper()
	runtimePath := filepath.Join(dir, "runtime.erofs")
	if err := os.WriteFile(runtimePath, runtime, 0o644); err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{}
	cfg.Sandbox.Boot.Runtime = runtimePath
	cfg.Paths.RunRoot = filepath.Join(dir, "run")
	cfg.Paths.BaseRoot = filepath.Join(dir, "lib")
	return testOrchCfgAt(t, cfg, filepath.Join(dir, "node.db"))
}

type migrationCredentials struct {
	service string
	envd    string
	traffic string
	forward string
}

func sandboxCredentials(sb *types.Sandbox) migrationCredentials {
	return migrationCredentials{
		service: sb.ServiceSecret,
		envd:    sb.EnvdAccessToken,
		traffic: sb.TrafficAccessToken,
		forward: sb.ForwardAccessToken,
	}
}

func payloadCredentials(payload migrationtoken.MigrationTokenPayloadV1) migrationCredentials {
	return migrationCredentials{
		service: payload.ServiceSecret,
		envd:    payload.EnvdAccessToken,
		traffic: payload.TrafficAccessToken,
		forward: payload.ForwardAccessToken,
	}
}

func assertMigrationCredentialsEqual(t *testing.T, got, want migrationCredentials) {
	t.Helper()
	if got != want {
		t.Fatal("migration changed one or more persisted service credentials")
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
