package orch

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func mmdsFeatureConfig() *config.Config {
	return &config.Config{MMDS: config.MMDSConfig{
		Enabled: true,
		Routes: config.MMDSRoutesConfig{
			Enabled: true, MaxRoutesPerSandbox: 8, MaxNamespaceBytes: 4096,
			MaxStaticBodyBytes: 1024, MaxSecretValueBytes: 1024,
		},
	}}
}

func TestCreatePersistsRoutesAndInitialSecretsSeparately(t *testing.T) {
	cfg := mmdsFeatureConfig()
	o, ctx := newAsyncConnectTestOrchestrator(t, cfg, &countingLauncher{})
	blocked := &blockedCreateVS{entered: make(chan struct{}), gate: make(chan struct{})}
	o.vs = blocked
	req := createRequestFixture(t, o, "7")
	req.Metadata[sandboxcfg.NsMMDS] = `{"routes":[{"path":"/initial","type":"secret","secret":"key"},{"path":"/later","type":"secret","secret":"later"}]}`
	header := `{"secrets":{"key":"initial-value"}}`
	req.MMDSHeader = &header
	events, cancel := o.Subscribe()
	defer cancel()

	sb, err := o.Create(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-blocked.entered:
	case <-time.After(time.Second):
		t.Fatal("background launch did not enter attach")
	}
	stored, err := o.st.Get(ctx, sb.ID)
	if err != nil || stored == nil {
		t.Fatalf("stored sandbox lookup failed: found=%t err=%v", stored != nil, err)
	}
	raw := stored.Metadata[sandboxcfg.NsMMDS]
	if strings.Contains(raw, "initial-value") || raw != `{"routes":[{"path":"/initial","type":"secret","secret":"key"},{"path":"/later","type":"secret","secret":"later"}]}` {
		t.Fatal("persisted MMDS routes mismatch or contain secret material")
	}
	values, _, found, err := o.st.GetMMDSRouteSecretValues(ctx, store.MMDSRouteSecretOwnerSandbox, sb.ID, sandboxcfg.MMDSRoutesDigest(raw))
	if err != nil || !found || string(values["key"]) != "initial-value" {
		t.Fatalf("initial secret metadata mismatch: found=%t err=%v", found, err)
	}
	event := <-events
	if event.Route.MMDSRouteSecretValues == nil || string((*event.Route.MMDSRouteSecretValues)["key"]) != "initial-value" {
		t.Fatal("initial route upsert did not carry the committed secret view")
	}
	close(blocked.gate)
}

func TestBuildRegisterOwnsMMDSAndTriggerCannotOverride(t *testing.T) {
	o := testOrchCfg(t, mmdsFeatureConfig())
	ctx := context.Background()
	apiKey, _, _ := allowlistedBuildIdentity(t, o)
	header := `{"secrets":{"key":"build-initial"}}`
	b, err := o.RegisterBuild(ctx, apiKey, api.RegisterSpec{
		Profile:    types.ProfileE2B,
		Resources:  testBuildResources(),
		Metadata:   map[string]string{sandboxcfg.NsMMDS: `{"routes":[{"path":"/secret","type":"secret","secret":"key"}]}`},
		MMDSHeader: &header,
	})
	if err != nil {
		t.Fatal(err)
	}
	stored, err := o.st.GetBuild(ctx, b.BuildID)
	if err != nil || stored == nil {
		t.Fatalf("stored build lookup failed: found=%t err=%v", stored != nil, err)
	}
	raw := stored.Metadata[sandboxcfg.NsMMDS]
	if strings.Contains(raw, "build-initial") {
		t.Fatal("build metadata contains initial secret plaintext")
	}
	values, _, found, err := o.st.GetMMDSRouteSecretValues(ctx, store.MMDSRouteSecretOwnerBuild, b.BuildID, sandboxcfg.MMDSRoutesDigest(raw))
	if err != nil || !found || string(values["key"]) != "build-initial" {
		t.Fatalf("build secret metadata mismatch: found=%t err=%v", found, err)
	}

	if err := o.TriggerBuild(ctx, apiKey, b.TemplateID, b.BuildID, api.TriggerSpec{
		FromImage: "registry.example/base:latest",
	}, api.BuildAuth{}); err != nil {
		t.Fatal(err)
	}
	triggered, err := o.st.GetBuild(ctx, b.BuildID)
	if err != nil || triggered.Metadata[sandboxcfg.NsMMDS] != raw {
		t.Fatalf("trigger changed registered MMDS routes: err=%v", err)
	}
}

func TestClusterBuildRegisterExtractsMMDSSecretsBeforePersistence(t *testing.T) {
	o := testOrchCfg(t, mmdsFeatureConfig())
	ctx := context.Background()
	_, _, fingerprint := allowlistedBuildIdentity(t, o)
	cmd := clusterBuildRegisterCommand("cluster-mmds-secret", fingerprint)
	cmd.Config[sandboxcfg.NsMMDS] = `{"routes":[{"path":"/secret","type":"secret","secret":"key"}]}`
	cmd.BuildMMDSSecrets = map[string]string{"key": "cluster-initial"}
	if ack := o.HandleCommand(ctx, cmd); ack.Status != routesync.AckAccepted {
		t.Fatalf("cluster BuildRegister ack = %+v", ack)
	}
	stored, err := o.st.GetBuild(ctx, cmd.BuildID)
	if err != nil || stored == nil {
		t.Fatalf("stored cluster build = %+v, err=%v", stored, err)
	}
	raw := stored.Metadata[sandboxcfg.NsMMDS]
	if raw != `{"routes":[{"path":"/secret","type":"secret","secret":"key"}]}` ||
		strings.Contains(raw, "cluster-initial") || strings.Contains(raw, `"secrets"`) {
		t.Fatalf("cluster build persisted noncanonical or plaintext MMDS metadata: %s", raw)
	}
	digest := sandboxcfg.MMDSRoutesDigest(raw)
	if stored.RegistrationMMDSRoutesDigest != digest {
		t.Fatalf("cluster registration MMDS digest = %q, want %q", stored.RegistrationMMDSRoutesDigest, digest)
	}
	values, _, found, err := o.st.GetMMDSRouteSecretValues(
		ctx, store.MMDSRouteSecretOwnerBuild, cmd.BuildID, digest)
	if err != nil || !found || string(values["key"]) != "cluster-initial" {
		t.Fatalf("cluster build MMDS values = %+v, found=%v err=%v", values, found, err)
	}
}

func TestClusterBuildRegisterMMDSReplayUsesDurableIdentityAfterPolicyDrift(t *testing.T) {
	o := testOrchCfg(t, mmdsFeatureConfig())
	ctx := context.Background()
	_, _, fingerprint := allowlistedBuildIdentity(t, o)
	cmd := clusterBuildRegisterCommand("cluster-mmds-policy-replay", fingerprint)
	cmd.Config[sandboxcfg.NsMMDS] = `{"routes":[{"path":"/identity","type":"secret","secret":"key"}]}`
	cmd.BuildMMDSSecrets = map[string]string{"key": "initial"}
	if ack := o.HandleCommand(ctx, cmd); ack.Status != routesync.AckAccepted {
		t.Fatalf("initial BuildRegister ack = %+v", ack)
	}
	select {
	case <-o.buildEvents:
	case <-time.After(time.Second):
		t.Fatal("initial registration event was not published")
	}

	// Mutable operator policy governs only new ownership. An ACK-lost replay
	// must still reach the store's immutable row/value comparison.
	o.cfg.MMDS.Routes.Enabled = false
	o.cfg.MMDS.Routes.MaxRoutesPerSandbox = 1
	o.cfg.MMDS.Routes.MaxNamespaceBytes = 1
	o.cfg.MMDS.Routes.MaxSecretValueBytes = 1
	o.cfg.MMDS.Routes.ReservedPathPrefixes = []string{"/identity"}
	if ack := o.HandleCommand(ctx, cmd); ack.Status != routesync.AckAccepted {
		t.Fatalf("exact replay after MMDS policy drift ack = %+v", ack)
	}
	usage, err := o.st.BuildUsage(ctx)
	if err != nil || usage.RegistrationBuilds != 1 {
		t.Fatalf("replay registration usage = %+v, err=%v", usage, err)
	}

	changed := *cmd
	changed.CmdID = "changed-mmds-policy-replay"
	changed.BuildMMDSSecrets = map[string]string{"key": "changed"}
	if ack := o.HandleCommand(ctx, &changed); ack.Status != routesync.AckRejected || ack.HTTPStatus != 409 {
		t.Fatalf("changed replay ack = %+v, want immutable conflict", ack)
	}
}

func TestBuildMMDSRouteIsIncludedInFullSync(t *testing.T) {
	o := testOrchCfg(t, mmdsFeatureConfig())
	ctx := context.Background()
	apiKey, _, _ := allowlistedBuildIdentity(t, o)
	header := `{"secrets":{"key":"build-full-sync"}}`
	build, err := o.RegisterBuild(ctx, apiKey, api.RegisterSpec{
		Profile:    types.ProfileE2B,
		Resources:  testBuildResources(),
		Metadata:   map[string]string{sandboxcfg.NsMMDS: `{"routes":[{"path":"/secret","type":"secret","secret":"key"}]}`},
		MMDSHeader: &header,
	})
	if err != nil {
		t.Fatal(err)
	}
	sandboxID := "build-" + build.BuildID
	row := &types.Sandbox{
		ID: sandboxID, Profile: types.ProfileE2B, TemplateID: build.TemplateID,
		State: types.StateRunning, RunID: "builder-run", FloatingIP: "192.0.2.10",
		APISecret: build.APISecret, ManifestKey: build.ManifestKey, Metadata: build.Metadata,
	}
	o.setMMDSBuildOwner(sandboxID, build.BuildID)
	o.cache(row)
	defer o.setMMDSBuildOwner(sandboxID, "")
	defer o.uncache(sandboxID)

	var projected *routesync.RouteEntry
	if err := o.Range(ctx, func(entry routesync.RouteEntry) error {
		if entry.SandboxID == sandboxID {
			copy := entry
			projected = &copy
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if projected == nil || projected.MMDSRoutes == "" || projected.MMDSRouteSecretValues == nil ||
		string((*projected.MMDSRouteSecretValues)["key"]) != "build-full-sync" {
		t.Fatal("full sync did not include the active builder MMDS view")
	}
}
