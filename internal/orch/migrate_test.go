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
	"github.com/kuasar-sandbox/orchestrator/internal/configresolve"
	"github.com/kuasar-sandbox/orchestrator/internal/migrationtoken"
	"github.com/kuasar-sandbox/orchestrator/internal/nodepath"
	"github.com/kuasar-sandbox/orchestrator/internal/reflocation"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/sandboxer/pkg/artifact"
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
	sb.StableIDValue = "stable-sandbox"
	sb.DeadlineUnix = 1_900_000_000
	sb.Env = map[string]string{"FOO": "bar"}
	sb.Metadata = map[string]string{
		"k": "v", sandboxcfg.NsRestore: `{"prefetch":"memory"}`,
		sandboxcfg.NsTraffic: `{"max_inflight":{"total":9,"forward":0}}`,
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

	tok, err := o.exportSandboxTokenForTest(ctx, apiKey, sid, false, false)
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
	if payload.NodeSandboxID != sid || payload.StableID != sb.StableID() ||
		payload.TemplateID != sb.TemplateID || payload.Profile != string(sb.Profile) ||
		payload.ResumeSourceKind != sb.ResumeSource.Kind || payload.ResumeSourceRef != sb.ResumeSource.Ref ||
		payload.CreatedUnix != sb.CreatedUnix ||
		payload.DeadlineUnix != sb.DeadlineUnix {
		t.Fatal("exported payload lost one or more portable sandbox fields")
	}
	assertMigrationCredentialsEqual(t, payloadCredentials(payload), sandboxCredentials(sb))

	waitForExportDeletion(t, o, sid)
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
	if got == nil || got.ID != sid || got.StableID() != sb.StableID() || got.Cluster != nil ||
		got.State != types.StatePaused || got.Profile != sb.Profile || got.TemplateID != sb.TemplateID ||
		got.ResumeSource != sb.ResumeSource || got.CreatedUnix != sb.CreatedUnix ||
		got.DeadlineUnix != sb.DeadlineUnix || !reflect.DeepEqual(got.Env, sb.Env) ||
		!reflect.DeepEqual(got.Metadata, sb.Metadata) {
		t.Fatal("imported row lost one or more portable sandbox fields")
	}
	assertMigrationCredentialsEqual(t, sandboxCredentials(got), sandboxCredentials(sb))
}

func TestArtifactKindRoundTripsThroughTemplateAndMigrationToken(t *testing.T) {
	for _, test := range []struct {
		name            string
		sourceKind      types.ResumeSourceKind
		templateKind    types.Kind
		autoPauseMemory bool
	}{
		{name: "sandbox", sourceKind: types.ResumeSourceSandbox, templateKind: types.KindSbx, autoPauseMemory: false},
		{name: "snapshot", sourceKind: types.ResumeSourceSnapshot, templateKind: types.KindSnp, autoPauseMemory: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			o := migrationOrchestrator(t, dir, []byte("artifact-kind-runtime"))
			ctx := context.Background()
			mk := strings.Repeat("6", 64)
			apiSecret, apiKey := defaultTestCredentials(t, mk)
			if _, err := o.st.AddKeyPair(ctx, store.KeyPair{APISecret: apiSecret, ManifestKey: mk}, "", 0, ""); err != nil {
				t.Fatal(err)
			}
			sourceRef := "manifest://" + strings.Repeat(map[types.ResumeSourceKind]string{
				types.ResumeSourceSandbox:  "b",
				types.ResumeSourceSnapshot: "c",
			}[test.sourceKind], 64)
			source := migrationSandbox(t, dir, "source-"+test.name, mk, sourceRef)
			source.ResumeSource.Kind = test.sourceKind
			if test.sourceKind == types.ResumeSourceSandbox {
				source.ResumeSource.SandboxRef = ""
			}
			source.AutoPauseMemory = test.autoPauseMemory
			if err := o.st.Put(ctx, source); err != nil {
				t.Fatal(err)
			}
			o.cache(source)

			templateID, err := o.exportSandboxTokenForTest(ctx, apiKey, source.ID, true, true)
			if err != nil {
				t.Fatal(err)
			}
			template, err := types.ParseTemplateID(templateID)
			if err != nil || template.Kind != test.templateKind || template.Ref != sourceRef {
				t.Fatalf("template = %+v, %v; want kind=%s ref=%s", template, err, test.templateKind, sourceRef)
			}

			token, err := o.exportSandboxTokenForTest(ctx, apiKey, source.ID, false, true)
			if err != nil {
				t.Fatal(err)
			}
			targetID := "target-" + test.name
			if got, err := o.ImportSandbox(ctx, apiKey, token, targetID); err != nil || got != targetID {
				t.Fatalf("ImportSandbox = %q, %v", got, err)
			}
			imported, err := o.st.Get(ctx, targetID)
			if err != nil || imported == nil {
				t.Fatalf("imported row = %+v, %v", imported, err)
			}
			if imported.ResumeSource != source.ResumeSource || imported.AutoPauseMemory != test.autoPauseMemory ||
				imported.State != types.StatePaused || imported.LaunchMode != "" {
				t.Fatalf("imported lifecycle = %+v, want source=%+v auto_pause_memory=%t", imported, source.ResumeSource, test.autoPauseMemory)
			}
		})
	}
}

func TestMigrationTokenCarriesStrictPortableResourceAndTrafficMetadata(t *testing.T) {
	dir := t.TempDir()
	o := migrationOrchestrator(t, dir, []byte("runtime"))
	sb := migrationSandbox(t, dir, "portable-resource", strings.Repeat("6", 64), "manifest://"+strings.Repeat("b", 64))
	sb.Metadata = map[string]string{
		sandboxcfg.NsResource: ` { "startup" : { "memory" : "1GiB" }, "capacity" : { "memory" : "8GiB" } } `,
		sandboxcfg.NsTraffic:  ` { "max_inflight" : { "total" : 9, "forward" : 0 } } `,
		"ordinary":            "preserved",
	}

	token, err := o.mintSandboxToken(sb, sb.ResumeSource)
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
	if got, want := payload.Metadata[sandboxcfg.NsTraffic], `{"max_inflight":{"total":9,"forward":0}}`; got != want {
		t.Fatalf("portable traffic metadata = %q, want %q", got, want)
	}

	sb.Metadata[sandboxcfg.NsResource] = `{"control":{"controller":"/run/foreign.sock"}}`
	if _, err := o.mintSandboxToken(sb, sb.ResumeSource); err == nil || !strings.Contains(err.Error(), "node-managed") {
		t.Fatalf("node-owned migration resource error = %v", err)
	}
}

func TestMigrationTokenDoesNotMaterializeAbsentTrafficMetadata(t *testing.T) {
	dir := t.TempDir()
	o := migrationOrchestrator(t, dir, []byte("runtime"))
	sb := migrationSandbox(t, dir, "absent-traffic", strings.Repeat("6", 64), "manifest://"+strings.Repeat("b", 64))
	apiSecret, apiKey := defaultTestCredentials(t, sb.ManifestKey)
	if apiSecret != sb.APISecret {
		t.Fatal("test credential derivation changed")
	}
	if _, err := o.st.AddKeyPair(context.Background(), store.KeyPair{
		APISecret: apiSecret, ManifestKey: sb.ManifestKey,
	}, "", 0, ""); err != nil {
		t.Fatal(err)
	}
	sb.Metadata = map[string]string{"ordinary": "preserved"}
	token, err := o.mintSandboxToken(sb, sb.ResumeSource)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := migrationtoken.Open(migrationtoken.KeyMaterial{APISecret: sb.APISecret, ManifestKey: sb.ManifestKey}, token)
	if err != nil {
		t.Fatal(err)
	}
	if _, present := payload.Metadata[sandboxcfg.NsTraffic]; present {
		t.Fatalf("migration materialized absent traffic: %+v", payload.Metadata)
	}
	importedID, err := o.ImportSandbox(context.Background(), apiKey, token, "absent-traffic-target")
	if err != nil {
		t.Fatal(err)
	}
	imported, err := o.st.Get(context.Background(), importedID)
	if err != nil || imported == nil {
		t.Fatalf("imported sandbox = %+v, %v", imported, err)
	}
	if _, present := imported.Metadata[sandboxcfg.NsTraffic]; present {
		t.Fatalf("migration import materialized absent traffic: %+v", imported.Metadata)
	}
}

func TestMigrationTokenRejectsBareE2BTrafficServices(t *testing.T) {
	dir := t.TempDir()
	o := migrationOrchestrator(t, dir, []byte("runtime"))
	sb := migrationSandbox(t, dir, "bare-traffic", strings.Repeat("6", 64), "manifest://"+strings.Repeat("b", 64))
	sb.Profile = types.ProfileBare
	sb.TemplateID = types.TemplateID{
		Profile: types.ProfileBare,
		Kind:    types.KindSnp,
		Ref:     "manifest://" + strings.Repeat("a", 64),
	}.String()
	sb.Metadata = map[string]string{
		sandboxcfg.NsTraffic: `{"max_inflight":{"e2b:envd":1}}`,
	}
	if _, err := o.mintSandboxToken(sb, sb.ResumeSource); err == nil || !strings.Contains(err.Error(), "bare") {
		t.Fatalf("bare migration traffic error = %v", err)
	}
}

func TestMigrationRestoreReappliesTargetNodeResourcePolicy(t *testing.T) {
	dir := t.TempDir()
	o := migrationOrchestrator(t, dir, []byte("runtime"))
	o.cfg.ResourceListen = &config.ResourceListenConfig{Enabled: true}
	allocatableMemory := "128MiB"
	o.cfg.Sandbox.Resources = configresolve.PublicSandboxResources(sandboxcfg.NodeResourcePolicy{
		Allocatable: sandboxcfg.NodeAllocatablePolicy{Memory: &allocatableMemory},
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
	token, err := o.mintSandboxToken(source, source.ResumeSource)
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
	resources, err := o.resolveArtifactResources(preparation.Spec, rtconfig.CapacityConfig{CPU: 2, Memory: "8GiB"})
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

	if _, err := o.exportSandboxTokenForTest(ctx, apiKey, "missing", false, true); !errors.Is(err, api.ErrNotFound) {
		t.Fatalf("missing export error = %v, want ErrNotFound", err)
	}

	sb := migrationSandbox(t, dir, "running", mk, "manifest://"+strings.Repeat("b", 64))
	sb.State = types.StateRunning
	if err := o.st.Put(ctx, sb); err != nil {
		t.Fatal(err)
	}
	if _, err := o.exportSandboxTokenForTest(ctx, "wrong-api-key", sb.ID, false, true); !errors.Is(err, api.ErrNotFound) {
		t.Fatalf("non-owner export error = %v, want ErrNotFound", err)
	}
	if _, err := o.exportSandboxTokenForTest(ctx, apiKey, sb.ID, false, true); !errors.Is(err, api.ErrBadRequest) {
		t.Fatalf("running export error = %v, want ErrBadRequest", err)
	}
}

func TestImportExplicitTargetPreservesStableIDAndCredentials(t *testing.T) {
	dir := t.TempDir()
	o := migrationOrchestrator(t, dir, []byte("runtime"))
	ctx := context.Background()
	mk := strings.Repeat("6", 64)
	apiSecret, apiKey := defaultTestCredentials(t, mk)
	if _, err := o.st.AddKeyPair(ctx, store.KeyPair{APISecret: apiSecret, ManifestKey: mk}, "", 0, ""); err != nil {
		t.Fatal(err)
	}
	source := migrationSandbox(t, dir, "logical-g0", mk, "manifest://"+strings.Repeat("b", 64))
	source.StableIDValue = "logical"
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
	token, err := o.exportSandboxTokenForTest(ctx, apiKey, source.ID, false, true)
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
	if got.StableID() != source.StableID() || got.ID == source.ID {
		t.Fatal("explicit target changed the stable ID or retained the source node ID")
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

// TestImportRejectsLocationUnsafeStableID covers admission for the stable id
// carried by the token: the value later keys the entity's publication
// location names, so it must satisfy the opaque-id contract (which is also a
// location-name-safe subset) before the row is inserted.
func TestImportRejectsLocationUnsafeStableID(t *testing.T) {
	dir := t.TempDir()
	o := migrationOrchestrator(t, dir, []byte("runtime"))
	ctx := context.Background()
	mk := strings.Repeat("6", 64)
	apiSecret, apiKey := defaultTestCredentials(t, mk)
	if _, err := o.st.AddKeyPair(ctx, store.KeyPair{APISecret: apiSecret, ManifestKey: mk}, "", 0, ""); err != nil {
		t.Fatal(err)
	}
	// Portable manifest ref: export skips promote entirely, so the test
	// isolates the import admission check.
	source := migrationSandbox(t, dir, "logical-g0", mk, "manifest://"+strings.Repeat("b", 64))
	source.StableIDValue = "not/a valid id"
	// Credentials bind the stable id; re-materialize after it is set.
	if err := materializeSandboxCredentials(source, sandboxcfg.Credentials{}); err != nil {
		t.Fatal(err)
	}
	if err := o.st.Put(ctx, source); err != nil {
		t.Fatal(err)
	}
	token, err := o.exportSandboxTokenForTest(ctx, apiKey, source.ID, false, true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := o.ImportSandbox(ctx, apiKey, token, "other-target"); err == nil ||
		!errors.Is(err, api.ErrBadRequest) || !strings.Contains(err.Error(), "stable ID") {
		t.Fatalf("import = %v, want a bad-request stable ID rejection", err)
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
	token, err := o.mintSandboxToken(source, source.ResumeSource)
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
	source.StableIDValue = "logical"
	source.Metadata = map[string]string{
		"ordinary":                     "preserved",
		clusterstate.ObjectMetadataKey: "untrusted-binding",
		sandboxcfg.NsCredentials:       `{"service_secret":"must-not-reenter"}`,
	}
	if err := materializeSandboxCredentials(source, sandboxcfg.Credentials{}); err != nil {
		t.Fatal(err)
	}
	token, err := o.mintSandboxToken(source, source.ResumeSource)
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
		StableID:      source.StableID(),
		TemplateID:    source.TemplateID,
		Profile:       source.Profile,
		RuntimeDigest: digest,
		ResumeSource:  source.ResumeSource,
	}, cluster, 0)
	if err != nil {
		t.Fatal(err)
	}
	cluster.Group = "/mutated"
	if imported.ID != "logical-g1" || imported.StableID() != "logical" || imported.ResumeSource != source.ResumeSource || imported.Cluster == nil ||
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
	token, err := o.mintSandboxToken(source, source.ResumeSource)
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

	// Trusted target rejection must precede even the installed-runtime check;
	// neither S nor E exists in this test's artifact environment.
	if err := os.Remove(o.runtimeFileFor(source.Profile)); err != nil {
		t.Fatal(err)
	}
	for name, expected := range map[string]migrationtoken.Expectations{
		"stable-id":          {StableID: "different-stable-id"},
		"template":           {TemplateID: types.TemplateID{Profile: types.ProfileE2B, Kind: types.KindSnp, Ref: "manifest://" + strings.Repeat("c", 64)}.String()},
		"profile":            {Profile: types.ProfileBare},
		"same-s-different-e": {ResumeSource: types.ResumeSource{Kind: source.ResumeSource.Kind, Ref: source.ResumeSource.Ref, SandboxRef: "manifest://" + strings.Repeat("f", 64)}},
		"resume-source": {ResumeSource: types.ResumeSource{SandboxRef: "manifest://eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
			Kind: types.ResumeSourceSnapshot, Ref: "manifest://" + strings.Repeat("d", 64),
		}},
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
	token, err := o.mintSandboxToken(source, source.ResumeSource)
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
	if _, err := o.mintSandboxToken(sb, types.ResumeSource{SandboxRef: "manifest://eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
		Kind: types.ResumeSourceSnapshot, Ref: "manifest://" + strings.Repeat("b", 64),
	}); err == nil ||
		!strings.Contains(err.Error(), "does not match template profile") {
		t.Fatalf("mint mismatch error = %v", err)
	}
}

func TestExportKeepSourceProducesPortableResultWithoutChangingSource(t *testing.T) {
	for _, test := range []struct {
		name         string
		sourceKind   types.ResumeSourceKind
		templateKind types.Kind
		suffix       string
	}{
		{name: "sandbox", sourceKind: types.ResumeSourceSandbox, templateKind: types.KindSbx, suffix: ".sandbox"},
		{name: "snapshot", sourceKind: types.ResumeSourceSnapshot, templateKind: types.KindSnp, suffix: ".snapshot"},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			o := migrationOrchestrator(t, dir, []byte("runtime"))
			ctx := context.Background()
			mk := strings.Repeat("7", 64)
			_, apiKey := defaultTestCredentials(t, mk)
			sid := "promote-" + test.name
			localRef := makeLocalArtifact(t, dir, sid, test.suffix)
			mref := "manifest://" + strings.Repeat("c", 64)
			argsPath := filepath.Join(dir, "publish.args")
			installPromoteRecordingStub(t, mref, argsPath)

			sb := migrationSandbox(t, dir, sid, mk, localRef)
			sb.ResumeSource.Kind = test.sourceKind
			if test.sourceKind == types.ResumeSourceSandbox {
				sb.ResumeSource.SandboxRef = ""
			}
			if err := o.st.Put(ctx, sb); err != nil {
				t.Fatal(err)
			}
			o.cache(sb)
			events, cancel := o.Subscribe()
			defer cancel()

			templateID, err := o.exportSandboxTokenForTest(ctx, apiKey, sid, true, true)
			if err != nil {
				t.Fatalf("export: %v", err)
			}
			template, err := types.ParseTemplateID(templateID)
			if err != nil || template.Kind != test.templateKind || template.Ref != mref {
				t.Fatalf("template = %+v, %v", template, err)
			}
			args, err := os.ReadFile(argsPath)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(args), "publish --json --quiet") || !strings.Contains(string(args), localRef) ||
				strings.Contains(string(args), "upload-snapshot") {
				t.Fatalf("publication argv = %q", args)
			}
			// The export produces a portable result but leaves the source
			// unchanged: original local ResumeSource, no upsert event,
			// checkpoint retained (#336).
			wantSource := sb.ResumeSource
			stored, err := o.st.Get(ctx, sid)
			if err != nil || stored == nil || stored.State != types.StatePaused || stored.ResumeSource != wantSource {
				t.Fatalf("stored source changed by keep-source export: %+v, %v", stored, err)
			}
			if cached := o.lookup(sid); cached == nil || cached.ResumeSource != wantSource {
				t.Fatal("cached source changed by keep-source export")
			}
			select {
			case ev := <-events:
				t.Fatalf("keep-source export published an unexpected route event: %+v", ev)
			default:
			}
			if _, err := os.Stat(localRef); err != nil {
				t.Fatalf("keep-source removed the local artifact: %v", err)
			}
		})
	}
}

func TestExportRejectsUnownedLocalArtifactBeforePublishOrCleanup(t *testing.T) {
	dir := t.TempDir()
	o := migrationOrchestrator(t, dir, []byte("runtime"))
	ctx := context.Background()
	mk := strings.Repeat("7", 64)
	_, apiKey := defaultTestCredentials(t, mk)
	sid := "unowned-local-artifact"
	outsideDir := filepath.Join(dir, "outside", sid)
	if err := os.MkdirAll(outsideDir, 0o755); err != nil {
		t.Fatal(err)
	}
	outsideRef := filepath.Join(outsideDir, sid+".snapshot")
	if err := os.WriteFile(outsideRef, []byte("must remain"), 0o644); err != nil {
		t.Fatal(err)
	}
	sb := migrationSandbox(t, dir, sid, mk, outsideRef)
	if err := o.st.Put(ctx, sb); err != nil {
		t.Fatal(err)
	}
	o.cache(sb)
	publishCalled := false
	o.artifactPublisher = func(context.Context, *types.Sandbox, types.ResumeSource) (artifact.PublishReport, error) {
		publishCalled = true
		return artifact.PublishReport{}, nil
	}

	if _, err := o.exportSandboxTokenForTest(ctx, apiKey, sid, true, true); err == nil ||
		!strings.Contains(err.Error(), "outside its owned capture path") {
		t.Fatalf("unowned local artifact error = %v", err)
	}
	if publishCalled {
		t.Fatal("unowned local artifact reached publisher")
	}
	if _, err := os.Stat(outsideRef); err != nil {
		t.Fatalf("unowned local artifact was removed: %v", err)
	}
	stored, err := o.st.Get(ctx, sid)
	if err != nil || stored == nil || stored.ResumeSource != sb.ResumeSource {
		t.Fatalf("unowned local artifact changed durable row: %+v, %v", stored, err)
	}
}

func TestExportRejectsNonCanonicalBaseDirBeforePublishOrCleanup(t *testing.T) {
	dir := t.TempDir()
	o := migrationOrchestrator(t, dir, []byte("runtime"))
	ctx := context.Background()
	mk := strings.Repeat("7", 64)
	_, apiKey := defaultTestCredentials(t, mk)
	sid := "noncanonical-base-dir"
	badBaseDir := filepath.Join(dir, "outside", sid)
	badCheckpointDir := filepath.Join(badBaseDir, "checkpoint")
	if err := os.MkdirAll(badCheckpointDir, 0o755); err != nil {
		t.Fatal(err)
	}
	badRef := filepath.Join(badCheckpointDir, sid+".snapshot")
	if err := os.WriteFile(badRef, []byte("must remain"), 0o644); err != nil {
		t.Fatal(err)
	}
	sb := migrationSandbox(t, dir, sid, mk, badRef)
	sb.BaseDir = badBaseDir
	if err := o.st.Put(ctx, sb); err != nil {
		t.Fatal(err)
	}
	o.cache(sb)
	publishCalled := false
	o.artifactPublisher = func(context.Context, *types.Sandbox, types.ResumeSource) (artifact.PublishReport, error) {
		publishCalled = true
		return artifact.PublishReport{}, nil
	}

	if _, err := o.exportSandboxTokenForTest(ctx, apiKey, sid, true, true); err == nil ||
		!strings.Contains(err.Error(), "does not match canonical path") {
		t.Fatalf("non-canonical BaseDir error = %v", err)
	}
	if publishCalled {
		t.Fatal("non-canonical BaseDir reached publisher")
	}
	if _, err := os.Stat(badRef); err != nil {
		t.Fatalf("artifact under non-canonical BaseDir was removed: %v", err)
	}
	stored, err := o.st.Get(ctx, sid)
	if err != nil || stored == nil || stored.ResumeSource != sb.ResumeSource || stored.BaseDir != badBaseDir {
		t.Fatalf("non-canonical BaseDir changed durable row: %+v, %v", stored, err)
	}
}

func TestExportPublishesLocatedSnapshotAndReturnsTemplate(t *testing.T) {
	dir := t.TempDir()
	o := migrationOrchestrator(t, dir, []byte("runtime"))
	cfg := o.cfg
	cfg.Checkpoint.Remote.RefLocationParent = "file:///mnt/shared/snapshots"
	ctx := context.Background()
	mk := strings.Repeat("7", 64)
	_, apiKey := defaultTestCredentials(t, mk)
	sid := "0198f7a1-1234-7234-9abc-0123456789ab"
	localRef := makeLocalSnapshot(t, dir, sid)
	// The publication name is the bare sandbox id; the fake sandbox-ctl
	// echoes a ref carrying whatever name promote passed.
	argsPath := filepath.Join(dir, "promote.args")
	binDir := t.TempDir()
	script := "#!/bin/sh\nprintf '%s\\n' \"$*\" > " + argsPath + "\n" +
		"for a in \"$@\"; do\n" +
		"  case \"$a\" in\n" +
		"    *=*)\n" +
		"      n=${a%%=*}\n" +
		"      printf '%s\\n' '{\"snapshotRef\":\"file://" + strings.Repeat("c", 64) + ".bundle@location:'\"$n\"'\",\"sandboxRef\":\"manifest://" + strings.Repeat("e", 64) + "\",\"removedRefs\":[]}'\n" +
		"      ;;\n" +
		"  esac\n" +
		"done\n"
	if err := os.WriteFile(filepath.Join(binDir, "sandbox-ctl"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))

	sb := migrationSandbox(t, dir, sid, mk, localRef)
	if err := o.st.Put(ctx, sb); err != nil {
		t.Fatal(err)
	}
	o.cache(sb)
	templateID, err := o.exportSandboxTokenForTest(ctx, apiKey, sid, true, true)
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
		i := strings.Index(f, "=")
		if i <= 0 {
			continue
		}
		if f[i+1:] == mustRefLocationURI(t, cfg, f[:i]) {
			locName = f[:i]
		}
	}
	if locName == "" {
		t.Fatalf("promote args = %q, want a publication name=uri pair", args)
	}
	wantName := reflocation.PublicationName(sid)
	if locName != wantName {
		t.Fatalf("publication name = %q, want %q", locName, wantName)
	}
	tmpl, err := types.ParseTemplateID(templateID)
	if err != nil || !strings.Contains(tmpl.Ref, "@location:"+locName) {
		t.Fatalf("template = %#v, %v; want ref carrying location %q", tmpl, err, locName)
	}
}

func mustRefLocationURI(t *testing.T, cfg *config.Config, name string) string {
	t.Helper()
	uri, err := cfg.Checkpoint.RefLocationURI(name)
	if err != nil {
		t.Fatalf("RefLocationURI(%q): %v", name, err)
	}
	return uri
}

// TestExportKeepSourceSucceedsWithoutSourceWrites proves that a keep-source
// export performs no UPDATE or DELETE on the source row — even when those
// operations are forbidden, the export still succeeds and the source is
// bit-for-bit unchanged (#336).
func TestExportKeepSourceSucceedsWithoutSourceWrites(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "node.db")
	cfg := &config.Config{}
	cfg.Paths.RunRoot = filepath.Join(dir, "run")
	cfg.Paths.BaseRoot = filepath.Join(dir, "lib")
	o := testOrchCfgAt(t, cfg, dbPath)
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
	installStoreTrigger(t, dbPath, `CREATE TRIGGER fail_source_update BEFORE UPDATE ON sandboxes BEGIN SELECT RAISE(ABORT, 'forced source update failure'); END`)
	installStoreTrigger(t, dbPath, `CREATE TRIGGER fail_source_delete BEFORE DELETE ON sandboxes BEGIN SELECT RAISE(ABORT, 'forced source delete failure'); END`)
	events, cancel := o.Subscribe()
	defer cancel()

	templateID, err := o.exportSandboxTokenForTest(ctx, apiKey, sid, true, true)
	if err != nil || templateID == "" {
		t.Fatalf("keep-source export should succeed without source writes: %q, %v", templateID, err)
	}
	stored, err := o.st.Get(ctx, sid)
	if err != nil || stored == nil || stored.State != types.StatePaused ||
		stored.ResumeSource != (types.ResumeSource{SandboxRef: "manifest://eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", Kind: types.ResumeSourceSnapshot, Ref: localRef}) {
		t.Fatalf("stored source changed: %+v, %v", stored, err)
	}
	if cached := o.lookup(sid); cached == nil || cached.ResumeSource != (types.ResumeSource{SandboxRef: "manifest://eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", Kind: types.ResumeSourceSnapshot, Ref: localRef}) {
		t.Fatal("cached source changed")
	}
	if _, err := os.Stat(localRef); err != nil {
		t.Fatalf("local snapshot removed: %v", err)
	}
	select {
	case <-events:
		t.Fatal("unexpected route event after keep-source export")
	default:
	}
}

// A source that is already portable stays portable: keep-source must not swap
// in a local artifact that merely exists at the checkpoint path, even when a
// stale file from an earlier lifecycle is still there (#336).
func TestExportKeepSourceKeepsPortableSourceDespiteStaleLocalArtifact(t *testing.T) {
	dir := t.TempDir()
	o := migrationOrchestrator(t, dir, []byte("runtime"))
	ctx := context.Background()
	manifestKey := strings.Repeat("7", 64)
	_, apiKey := defaultTestCredentials(t, manifestKey)
	portableRef := "manifest://" + strings.Repeat("3", 64)
	sb := migrationSandbox(t, dir, "portable-kept-source", manifestKey, portableRef)
	stale := makeLocalArtifact(t, dir, sb.ID, ".snapshot")
	if err := o.st.Put(ctx, sb); err != nil {
		t.Fatal(err)
	}
	o.cache(sb)
	argsPath := filepath.Join(dir, "publish.args")
	installPromoteRecordingStub(t, portableRef, argsPath)

	result, err := o.exportSandboxTokenForTest(ctx, apiKey, sb.ID, true, true)
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	template, err := types.ParseTemplateID(result)
	if err != nil || template.Ref != portableRef {
		t.Fatalf("template = %+v, %v", template, err)
	}
	// A portable source needs no publication; the stub must not have run.
	if _, err := os.Stat(argsPath); !os.IsNotExist(err) {
		t.Fatalf("portable source unexpectedly ran the publisher: %v", err)
	}
	wantSource := types.ResumeSource{SandboxRef: "manifest://eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", Kind: types.ResumeSourceSnapshot, Ref: portableRef}
	stored, err := o.st.Get(ctx, sb.ID)
	if err != nil || stored == nil || stored.State != types.StatePaused || stored.ResumeSource != wantSource {
		t.Fatalf("portable source changed by keep-source export: %+v, %v", stored, err)
	}
	if cached := o.lookup(sb.ID); cached == nil || cached.ResumeSource != wantSource {
		t.Fatal("cached portable source changed by keep-source export")
	}
	if _, err := os.Stat(stale); err != nil {
		t.Fatalf("keep-source export removed the stale local artifact: %v", err)
	}
}

// Repeated keep-source exports from the same paused state all succeed without
// changing the source. KMT tokens may differ per mint and the publisher may run
// once per export; neither is a contract.
func TestExportKeepSourceRepeatableFromSamePausedState(t *testing.T) {
	dir := t.TempDir()
	o := migrationOrchestrator(t, dir, []byte("runtime"))
	ctx := context.Background()
	manifestKey := strings.Repeat("7", 64)
	_, apiKey := defaultTestCredentials(t, manifestKey)
	sid := "repeat-kept-source"
	localRef := makeLocalArtifact(t, dir, sid, ".snapshot")
	mref := "manifest://" + strings.Repeat("c", 64)
	argsPath := filepath.Join(dir, "publish.args")
	installPromoteRecordingStub(t, mref, argsPath)

	sb := migrationSandbox(t, dir, sid, manifestKey, localRef)
	if err := o.st.Put(ctx, sb); err != nil {
		t.Fatal(err)
	}
	o.cache(sb)
	wantSource := types.ResumeSource{SandboxRef: "manifest://eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", Kind: types.ResumeSourceSnapshot, Ref: localRef}

	first, err := o.exportSandboxTokenForTest(ctx, apiKey, sid, false, true)
	if err != nil || !strings.HasPrefix(first, "kmt1.") {
		t.Fatalf("first export = %q, %v", first, err)
	}
	second, err := o.exportSandboxTokenForTest(ctx, apiKey, sid, true, true)
	if err != nil {
		t.Fatalf("second export: %v", err)
	}
	template, err := types.ParseTemplateID(second)
	if err != nil || template.Ref != mref {
		t.Fatalf("second export template = %+v, %v", template, err)
	}
	stored, err := o.st.Get(ctx, sid)
	if err != nil || stored == nil || stored.State != types.StatePaused || stored.ResumeSource != wantSource {
		t.Fatalf("source changed by repeated exports: %+v, %v", stored, err)
	}
	if cached := o.lookup(sid); cached == nil || cached.ResumeSource != wantSource {
		t.Fatal("cached source changed by repeated exports")
	}
	if _, err := os.Stat(localRef); err != nil {
		t.Fatalf("repeated exports removed the local artifact: %v", err)
	}
	if _, err := os.Stat(argsPath); err != nil {
		t.Fatalf("publication did not run for the local source: %v", err)
	}
}

// A resume -> pause cycle produces a fresh local checkpoint; a following
// keep-source export still succeeds and keeps that checkpoint as the source.
func TestExportKeepSourceAfterResumePauseCycle(t *testing.T) {
	dir := t.TempDir()
	runtimePath := filepath.Join(dir, "runtime.erofs")
	if err := os.WriteFile(runtimePath, []byte("runtime"), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg := checkpointOrchestratorConfig(t, config.CheckpointLocal)
	shortDir := shortOrchestratorTestDir(t)
	cfg.Paths.RunRoot = filepath.Join(shortDir, "run")
	cfg.Paths.BaseRoot = filepath.Join(shortDir, "base")
	cfg.Sandbox.Boot.Runtime = runtimePath
	installCheckpointSandboxCtl(t)
	launcher := &countingLauncher{}
	o, ctx := newAsyncConnectTestOrchestrator(t, cfg, launcher)

	manifestKey := strings.Repeat("5", 64)
	apiSecret, apiKey := defaultTestCredentials(t, manifestKey)
	sid := "resume-pause-export"
	sb := &types.Sandbox{
		ID: sid, Profile: types.ProfileBare,
		TemplateID: types.TemplateID{
			Profile: types.ProfileBare, Kind: types.KindImg,
			Ref: "manifest://" + strings.Repeat("6", 64),
		}.String(),
		State: types.StateRunning, RunID: "cycle-run", VswitchPort: "cycle-port",
		RunDir:    nodepath.SandboxRunDir(cfg.Paths.RunRoot, sid),
		BaseDir:   nodepath.SandboxBaseDir(cfg.Paths.BaseRoot, sid),
		APISecret: apiSecret, ManifestKey: manifestKey, CreatedUnix: 1,
	}
	materializeTestSandboxCredentials(t, sb)
	if err := o.st.Put(ctx, sb); err != nil {
		t.Fatal(err)
	}
	o.cache(sb)
	checkpointRef := types.ResumeSource{Kind: types.ResumeSourceSnapshot, Ref: checkpointSnapshotRef, SandboxRef: checkpointSandboxRef}
	checkpointPath := filepath.Join(sb.BaseDir, "checkpoint", "produced.snapshot")
	if err := os.MkdirAll(filepath.Dir(checkpointPath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(checkpointPath, []byte("checkpoint"), 0o644); err != nil {
		t.Fatal(err)
	}

	// First pause from running, then resume and pause again: the durable
	// source is the fresh local checkpoint at the same node-local path.
	if err := o.Pause(ctx, sid, apiKey, orchSnapshotCapture(sandboxcfg.SnapshotPolicy{})); err != nil {
		t.Fatal(err)
	}
	if _, err := o.Connect(ctx, sid, apiKey, "", api.ConnectOptions{}); err != nil {
		t.Fatal(err)
	}
	waitForSandbox(t, o, ctx, sid, func(current *types.Sandbox) bool {
		return current.State == types.StateRunning
	}, "running after resume")
	if err := o.Pause(ctx, sid, apiKey, orchSnapshotCapture(sandboxcfg.SnapshotPolicy{})); err != nil {
		t.Fatal(err)
	}
	stored, err := o.st.Get(ctx, sid)
	if err != nil || stored == nil || stored.State != types.StatePaused || stored.ResumeSource != checkpointRef {
		t.Fatalf("re-paused source = %+v, %v", stored, err)
	}

	installPromoteStub(t, "manifest://"+strings.Repeat("c", 64))
	result, err := o.exportSandboxTokenForTest(ctx, apiKey, sid, false, true)
	if err != nil || !strings.HasPrefix(result, "kmt1.") {
		t.Fatalf("export after resume/pause cycle = %q, %v", result, err)
	}
	stored, err = o.st.Get(ctx, sid)
	if err != nil || stored == nil || stored.State != types.StatePaused || stored.ResumeSource != checkpointRef {
		t.Fatalf("source changed by post-cycle export: %+v, %v", stored, err)
	}
	if _, err := os.Stat(checkpointPath); err != nil {
		t.Fatalf("post-cycle export removed the local checkpoint: %v", err)
	}
}

func TestExportMoveDeleteAcceptanceFailurePreservesSource(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "node.db")
	runtimePath := filepath.Join(dir, "rt-e2b.erofs")
	if err := os.WriteFile(runtimePath, []byte("fake-runtime-bytes"), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{}
	cfg.Sandbox.Boot.Runtime = runtimePath
	cfg.Paths.RunRoot = filepath.Join(dir, "run")
	cfg.Paths.BaseRoot = filepath.Join(dir, "lib")
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
	installStoreTrigger(t, dbPath, `CREATE TRIGGER fail_begin_delete BEFORE UPDATE OF state ON sandboxes WHEN NEW.state='deleting' BEGIN SELECT RAISE(ABORT, 'forced delete acceptance failure'); END`)
	events, cancel := o.Subscribe()
	defer cancel()

	tok, err := o.exportSandboxTokenForTest(ctx, apiKey, sid, false, false)
	if err == nil || !strings.Contains(err.Error(), "delete source") || tok != "" {
		t.Fatalf("move export returned the wrong token/error state: %v", err)
	}
	stored, getErr := o.st.Get(ctx, sid)
	if getErr != nil || stored == nil || stored.State != types.StatePaused || stored.ResumeSource != (types.ResumeSource{SandboxRef: "manifest://eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", Kind: types.ResumeSourceSnapshot, Ref: localRef}) {
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
		APISecret: deriveTestAPISecret(t, mk), ManifestKey: mk,
		ResumeSource: types.ResumeSource{SandboxRef: "manifest://eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", Kind: types.ResumeSourceSnapshot, Ref: ref}, RunDir: nodepath.SandboxRunDir(filepath.Join(dir, "run"), sid),
		BaseDir: nodepath.SandboxBaseDir(filepath.Join(dir, "lib"), sid), CreatedUnix: 1,
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
	o := testOrchCfgAt(t, cfg, filepath.Join(dir, "node.db"))

	return o
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
	return makeLocalArtifact(t, dir, sid, ".snapshot")
}

func makeLocalArtifact(t *testing.T, dir, sid, suffix string) string {
	t.Helper()
	localDir := filepath.Join(nodepath.SandboxBaseDir(filepath.Join(dir, "lib"), sid), "checkpoint")
	if err := os.MkdirAll(localDir, 0o755); err != nil {
		t.Fatal(err)
	}
	ref := filepath.Join(localDir, sid+suffix)
	if err := os.WriteFile(ref, []byte("artifact"), 0o644); err != nil {
		t.Fatal(err)
	}
	return ref
}

func installPromoteStub(t *testing.T, mref string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "sandbox-ctl")
	script := "#!/bin/sh\n" + promoteReportScript(mref)
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
}

func installPromoteRecordingStub(t *testing.T, mref, argsPath string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "sandbox-ctl")
	script := "#!/bin/sh\nprintf '%s\\n' \"$*\" > " + argsPath + "\n" + promoteReportScript(mref)
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
}

func installStoreTrigger(t *testing.T, dbPath, statement string) {
	t.Helper()
	// Fault injection can race a cleanup worker's transaction. Use the same
	// bounded busy wait as the store so trigger removal does not fail spuriously.
	db, err := sql.Open("sqlite", "file:"+dbPath+"?_pragma=busy_timeout(5000)")
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
