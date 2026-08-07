package orch

import (
	"context"
	"errors"
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
		Metadata:  map[string]string{sandboxcfg.NsMMDS: `{"routes":[]}`},
	}, api.BuildAuth{}); !errors.Is(err, api.ErrBadRequest) {
		t.Fatalf("MMDS override error = %v", err)
	}
	if err := o.TriggerBuild(ctx, apiKey, b.TemplateID, b.BuildID, api.TriggerSpec{
		FromImage: "registry.example/base:latest",
		Metadata:  map[string]string{"ordinary": "trigger"},
	}, api.BuildAuth{}); err != nil {
		t.Fatal(err)
	}
	triggered, err := o.st.GetBuild(ctx, b.BuildID)
	if err != nil || triggered.Metadata[sandboxcfg.NsMMDS] != raw {
		t.Fatalf("trigger changed registered MMDS routes: err=%v", err)
	}
}

func TestBuildMMDSRouteIsIncludedInFullSync(t *testing.T) {
	o := testOrchCfg(t, mmdsFeatureConfig())
	ctx := context.Background()
	apiKey, _, _ := allowlistedBuildIdentity(t, o)
	header := `{"secrets":{"key":"build-full-sync"}}`
	build, err := o.RegisterBuild(ctx, apiKey, api.RegisterSpec{
		Profile:    types.ProfileE2B,
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
