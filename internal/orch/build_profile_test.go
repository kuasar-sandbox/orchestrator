package orch

import (
	"context"
	"encoding/hex"
	"errors"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/apikey"
	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func allowlistedBuildIdentity(t *testing.T, o *Orchestrator) (apiKey, manifestKey, fingerprint string) {
	t.Helper()
	manifestKey = strings.Repeat("5a", 32)
	raw, err := hex.DecodeString(manifestKey)
	if err != nil {
		t.Fatal(err)
	}
	apiKey, err = apikey.Mint(raw)
	if err != nil {
		t.Fatal(err)
	}
	_, fingerprint, err = o.AddManifestKey(context.Background(), manifestKey, "test", 0, "")
	if err != nil {
		t.Fatal(err)
	}
	return apiKey, manifestKey, fingerprint
}

func TestRegisterBuildPersistsBareProfile(t *testing.T) {
	o := testOrch(t)
	ctx := context.Background()
	apiKey, _, _ := allowlistedBuildIdentity(t, o)

	b, err := o.RegisterBuild(ctx, apiKey, api.RegisterSpec{
		Name: "bare-template", Tags: []string{"bare-tag"}, Profile: types.ProfileBare,
	})
	if err != nil {
		t.Fatalf("RegisterBuild: %v", err)
	}
	stored, err := o.st.GetBuild(ctx, b.BuildID)
	if err != nil || stored == nil || stored.Profile != types.ProfileBare {
		t.Fatalf("stored build = %+v, err=%v", stored, err)
	}
	if _, err := o.RegisterBuild(ctx, apiKey, api.RegisterSpec{}); !errors.Is(err, api.ErrBadRequest) {
		t.Fatalf("missing profile error = %v, want ErrBadRequest", err)
	}
}

func TestBuildRestorePrefetchRegisterAndTrigger(t *testing.T) {
	o := testOrch(t)
	ctx := context.Background()
	apiKey, _, _ := allowlistedBuildIdentity(t, o)

	b, err := o.RegisterBuild(ctx, apiKey, api.RegisterSpec{
		Profile: types.ProfileBare,
		Metadata: map[string]string{
			sandboxcfg.NsRestore: ` { "prefetch" : "memory" } `,
		},
	})
	if err != nil {
		t.Fatalf("RegisterBuild: %v", err)
	}
	stored, err := o.st.GetBuild(ctx, b.BuildID)
	if err != nil || stored.Metadata[sandboxcfg.NsRestore] != `{"prefetch":"memory"}` {
		t.Fatalf("registered restore metadata = %+v, err=%v", stored, err)
	}

	err = o.TriggerBuild(ctx, apiKey, b.TemplateID, b.BuildID, api.TriggerSpec{
		FromImage: "registry.test/base:latest",
		Metadata:  map[string]string{sandboxcfg.NsRestore: `{"prefetch":"off"}`},
	}, api.BuildAuth{})
	if err != nil {
		t.Fatalf("TriggerBuild: %v", err)
	}
	stored, err = o.st.GetBuild(ctx, b.BuildID)
	if err != nil || stored.Metadata[sandboxcfg.NsRestore] != `{"prefetch":"off"}` {
		t.Fatalf("triggered restore metadata = %+v, err=%v", stored, err)
	}
}

func TestBuildRestorePrefetchRejectsInvalidBeforePersist(t *testing.T) {
	o := testOrch(t)
	ctx := context.Background()
	apiKey, _, _ := allowlistedBuildIdentity(t, o)
	if _, err := o.RegisterBuild(ctx, apiKey, api.RegisterSpec{
		Profile:  types.ProfileBare,
		Metadata: map[string]string{sandboxcfg.NsRestore: `{"file_refs":"trust"}`},
	}); !errors.Is(err, api.ErrBadRequest) {
		t.Fatalf("RegisterBuild error = %v, want ErrBadRequest", err)
	}

	b, err := o.RegisterBuild(ctx, apiKey, api.RegisterSpec{Profile: types.ProfileBare})
	if err != nil {
		t.Fatal(err)
	}
	if err := o.TriggerBuild(ctx, apiKey, b.TemplateID, b.BuildID, api.TriggerSpec{
		FromImage: "registry.test/base:latest",
		Metadata:  map[string]string{sandboxcfg.NsRestore: `{"prefetch":"disk"}`},
	}, api.BuildAuth{}); !errors.Is(err, api.ErrBadRequest) {
		t.Fatalf("TriggerBuild error = %v, want ErrBadRequest", err)
	}
	stored, err := o.st.GetBuild(ctx, b.BuildID)
	if err != nil || stored.Status != types.BuildRegistered {
		t.Fatalf("invalid trigger changed build = %+v, err=%v", stored, err)
	}
}

func TestTriggerBareBuildRejectsCommandsAndQueuesImage(t *testing.T) {
	o := testOrch(t)
	ctx := context.Background()
	apiKey, _, _ := allowlistedBuildIdentity(t, o)
	b, err := o.RegisterBuild(ctx, apiKey, api.RegisterSpec{Profile: types.ProfileBare})
	if err != nil {
		t.Fatal(err)
	}

	for name, spec := range map[string]api.TriggerSpec{
		"start": {FromImage: "registry.test/base:latest", StartCmd: "serve"},
		"ready": {FromImage: "registry.test/base:latest", ReadyCmd: "check"},
	} {
		t.Run(name, func(t *testing.T) {
			if err := o.TriggerBuild(ctx, apiKey, b.TemplateID, b.BuildID, spec, api.BuildAuth{}); !errors.Is(err, api.ErrBadRequest) {
				t.Fatalf("TriggerBuild error = %v, want ErrBadRequest", err)
			}
		})
	}
	if err := o.TriggerBuild(ctx, apiKey, b.TemplateID, b.BuildID,
		api.TriggerSpec{FromImage: "registry.test/base:latest"}, api.BuildAuth{}); err != nil {
		t.Fatalf("TriggerBuild image-only: %v", err)
	}
	stored, err := o.st.GetBuild(ctx, b.BuildID)
	if err != nil || stored == nil || stored.Profile != types.ProfileBare || stored.Kind != types.KindImg || stored.Status != types.BuildWaiting {
		t.Fatalf("queued bare build = %+v, err=%v", stored, err)
	}
}

func TestBuildSpecCarriesBareProfileNetwork(t *testing.T) {
	cfg := &config.Config{}
	cfg.Sandbox.Network.E2B.Nexthop = "169.254.0.22"
	cfg.Sandbox.Network.Bare.Nexthop = "169.254.1.0"
	cfg.MMDS.Enabled = true
	o := testOrchCfg(t, cfg)
	b := &types.Build{BuildID: "build-bare", Profile: types.ProfileBare}
	o.pend[b.BuildID] = &pendingBuild{
		build: b, workdir: t.TempDir(), innerIP: "169.254.1.1/31",
	}

	spec, _, found, err := o.BuildSpecFor(context.Background(), "build:"+b.BuildID)
	if err != nil || !found {
		t.Fatalf("BuildSpecFor: found=%t err=%v", found, err)
	}
	if spec.Profile != string(types.ProfileBare) || spec.Net.InnerIP != "169.254.1.1/31" || spec.Net.Nexthop != "169.254.1.0" {
		t.Fatalf("bare BuildSpec profile/network = %q %+v", spec.Profile, spec.Net)
	}
	if spec.MMDSEnabled || spec.EnvdToken != "" {
		t.Fatalf("bare BuildSpec exposed e2b template controls: mmds=%t envd_token=%q", spec.MMDSEnabled, spec.EnvdToken)
	}
}

func TestRegisterClusterBuildRequiresAndPersistsProfile(t *testing.T) {
	o := testOrch(t)
	ctx := context.Background()
	_, _, fingerprint := allowlistedBuildIdentity(t, o)

	missing := &routesync.Command{BuildID: "missing-profile", TemplateRef: "transient-missing", KeyFingerprint: fingerprint}
	if err := o.registerClusterBuild(ctx, missing); err == nil {
		t.Fatal("build_register without profile was accepted")
	}
	cmd := &routesync.Command{
		BuildID: "bare-cluster-build", TemplateRef: "transient-bare",
		Profile: string(types.ProfileBare), KeyFingerprint: fingerprint,
		Config: map[string]string{sandboxcfg.NsRestore: ` { "prefetch": "memory" } `},
	}
	if err := o.registerClusterBuild(ctx, cmd); err != nil {
		t.Fatalf("registerClusterBuild: %v", err)
	}
	stored, err := o.st.GetBuild(ctx, cmd.BuildID)
	if err != nil || stored == nil || stored.Profile != types.ProfileBare ||
		stored.Metadata[sandboxcfg.NsRestore] != `{"prefetch":"memory"}` {
		t.Fatalf("cluster build = %+v, err=%v", stored, err)
	}
	if err := o.registerClusterBuild(ctx, cmd); err != nil {
		t.Fatalf("idempotent build_register replay: %v", err)
	}
	conflict := *cmd
	conflict.Profile = string(types.ProfileE2B)
	if err := o.registerClusterBuild(ctx, &conflict); err == nil {
		t.Fatal("build_register changed an existing build profile")
	}
	stored, err = o.st.GetBuild(ctx, cmd.BuildID)
	if err != nil || stored == nil || stored.Profile != types.ProfileBare || stored.Status != types.BuildRegistered {
		t.Fatalf("conflicting replay changed build = %+v, err=%v", stored, err)
	}

	invalid := &routesync.Command{
		BuildID: "invalid-restore", TemplateRef: "transient-invalid", Profile: string(types.ProfileBare),
		KeyFingerprint: fingerprint, Config: map[string]string{sandboxcfg.NsRestore: `{"file_refs":"trust"}`},
	}
	if err := o.registerClusterBuild(ctx, invalid); err == nil {
		t.Fatal("build_register accepted tenant file_refs")
	}
	if got, err := o.st.GetBuild(ctx, invalid.BuildID); err != nil || got != nil {
		t.Fatalf("invalid build_register persisted build=%+v err=%v", got, err)
	}
}
