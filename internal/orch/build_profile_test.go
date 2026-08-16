package orch

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/buildcfg"
	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func allowlistedBuildIdentity(t *testing.T, o *Orchestrator) (apiKey, manifestKey, fingerprint string) {
	t.Helper()
	manifestKey = strings.Repeat("5a", 32)
	_, apiKey = defaultTestCredentials(t, manifestKey)
	_, fingerprint, _, err := o.AddKeyPair(context.Background(), manifestKey, "", "test", 0, "")
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
		Name: "bare-template", Tags: []string{"bare-tag"}, Profile: types.ProfileBare, Resources: testBuildResources(),
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

func TestRegisterBuildValidatesPhaseResourcesAgainstNodePolicy(t *testing.T) {
	o := testOrch(t)
	apiKey, _, _ := allowlistedBuildIdentity(t, o)
	_, err := o.RegisterBuild(context.Background(), apiKey, api.RegisterSpec{
		Profile:   types.ProfileE2B,
		Resources: testBuildResources(),
		Metadata: map[string]string{
			sandboxcfg.NsResource: `{"allocatable":{"memory":"16GiB"}}`,
		},
	})
	if !errors.Is(err, api.ErrBadRequest) || !strings.Contains(err.Error(), "phase sandbox resources") {
		t.Fatalf("RegisterBuild error = %v, want node-policy resource rejection", err)
	}
	usage, usageErr := o.st.BuildUsage(context.Background())
	if usageErr != nil || usage.RegistrationBuilds != 0 {
		t.Fatalf("invalid phase resources consumed registration admission: %+v, %v", usage, usageErr)
	}
}

func TestDirectBuildRegisterRejectsClusterSystemMetadata(t *testing.T) {
	o := testOrch(t)
	apiKey, _, _ := allowlistedBuildIdentity(t, o)
	_, err := o.RegisterBuild(context.Background(), apiKey, api.RegisterSpec{
		Profile:   types.ProfileE2B,
		Resources: types.BuildResources{CPU: 1000, Memory: 1 << 30},
		Metadata: map[string]string{
			clusterstate.ObjectMetadataKey: `{"group":"tenant-injected"}`,
		},
	})
	if !errors.Is(err, api.ErrBadRequest) || !strings.Contains(err.Error(), "node-managed cluster context") {
		t.Fatalf("RegisterBuild error = %v, want node-managed ErrBadRequest", err)
	}
}

func TestTriggerBareBuildRejectsCommandsAndQueuesImage(t *testing.T) {
	o := testOrch(t)
	ctx := context.Background()
	apiKey, _, _ := allowlistedBuildIdentity(t, o)
	b, err := o.RegisterBuild(ctx, apiKey, api.RegisterSpec{Profile: types.ProfileBare, Resources: testBuildResources()})
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
		build: b, workdir: t.TempDir(),
		network: sandboxcfg.NetworkSpec{
			InnerIP: "169.254.1.1/31",
			Nexthop: "169.254.1.0",
		},
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

	missing := &routesync.Command{BuildID: "missing-profile", TemplateRef: "transient-missing", APISecretFingerprint: fingerprint}
	if err := o.registerClusterBuild(ctx, missing); err == nil {
		t.Fatal("build_register without profile was accepted")
	}
	cmd := &routesync.Command{
		BuildID: "bare-cluster-build", TemplateRef: "transient-bare",
		Profile: string(types.ProfileBare), APISecretFingerprint: fingerprint,
		BuildResources: routesync.BuildResourcesFromTypes(testBuildResources()),
		ImageRepo:      "registry.test/repo",
		RegistryAuth:   `{"auths":{"registry.test":{"auth":"opaque"}}}`,
		Config: map[string]string{
			sandboxcfg.NsResource:          ` { "capacity" : { "memory" : "8GiB" }, "allocatable" : { "memory" : "512MiB" } } `,
			clusterstate.ObjectMetadataKey: `{"group":"/test"}`,
		},
	}
	if err := o.registerClusterBuild(ctx, cmd); err != nil {
		t.Fatalf("registerClusterBuild: %v", err)
	}
	stored, err := o.st.GetBuild(ctx, cmd.BuildID)
	if err != nil || stored == nil || stored.Profile != types.ProfileBare {
		t.Fatalf("cluster build = %+v, err=%v", stored, err)
	}
	if got, want := stored.PhaseResourcePatch, `{"capacity":{"memory":"8GiB"},"allocatable":{"memory":"512MiB"}}`; got != want {
		t.Fatalf("cluster build resource = %s, want %s", got, want)
	}
	if stored.ClusterGroup != "/test" {
		t.Fatalf("durable cluster group = %q", stored.ClusterGroup)
	}
	if _, leaked := stored.Metadata[clusterstate.ObjectMetadataKey]; leaked {
		t.Fatalf("cluster ownership leaked into portable template metadata: %+v", stored.Metadata)
	}
	if stored.RegistryAuth != "" || stored.RegistrationRegistryAuth != cmd.RegistryAuth || stored.RegistrationImageRepo != cmd.ImageRepo {
		t.Fatalf("cluster registry auth was not encrypted/restored with the durable Build")
	}
	o.clusterBuildMu.Lock()
	delete(o.clusterBuilds, cmd.BuildID) // simulate controller restart
	o.clusterBuildMu.Unlock()
	if auth, cluster := o.clusterBuildCreds(stored); !cluster || auth != cmd.RegistryAuth {
		t.Fatalf("durable cluster credentials after restart = cluster %v auth %q", cluster, auth)
	}
	// A lost ACK may be retried after operator policy has tightened below this
	// already accepted Build. The retry must return the durable result rather
	// than reinterpret the existing Build as a new execution admission.
	originalExecution := o.cfg.Builder.Admission.Execution
	tinyCPU := config.CPUCores("0.001")
	tinyMemory := "1B"
	o.cfg.Builder.Admission.Execution = &config.BuildAdmissionLimitConfig{
		Resources: config.BuildAdmissionResourcesConfig{CPU: &tinyCPU, Memory: &tinyMemory},
	}
	if err := o.registerClusterBuild(ctx, cmd); err != nil {
		t.Fatalf("idempotent build_register replay after policy tightening: %v", err)
	}
	o.cfg.Builder.Admission.Execution = originalExecution
	conflict := *cmd
	conflict.Profile = string(types.ProfileE2B)
	if err := o.registerClusterBuild(ctx, &conflict); err == nil {
		t.Fatal("build_register changed an existing build profile")
	} else if status, _ := clusterCommandRejection(err); status != http.StatusConflict {
		t.Fatalf("immutable conflict status = %d, want 409", status)
	}
	stored, err = o.st.GetBuild(ctx, cmd.BuildID)
	if err != nil || stored == nil || stored.Profile != types.ProfileBare || stored.Status != types.BuildRegistered {
		t.Fatalf("conflicting replay changed build = %+v, err=%v", stored, err)
	}
	credentialConflict := *cmd
	credentialConflict.RegistryAuth = `{"auths":{"registry.test":{"auth":"other"}}}`
	if err := o.registerClusterBuild(ctx, &credentialConflict); err == nil {
		t.Fatal("build_register changed immutable registration credentials")
	}

	invalid := &routesync.Command{
		BuildID: "invalid-cluster-resource", TemplateRef: "transient-invalid",
		Profile: string(types.ProfileBare), APISecretFingerprint: fingerprint,
		BuildResources: routesync.BuildResourcesFromTypes(testBuildResources()),
		Config: map[string]string{
			sandboxcfg.NsResource:          `{"control":{}}`,
			clusterstate.ObjectMetadataKey: `{"group":"/test"}`,
		},
	}
	if err := o.registerClusterBuild(ctx, invalid); !errors.Is(err, api.ErrBadRequest) || !strings.Contains(err.Error(), "node-managed") {
		t.Fatalf("invalid cluster resource error = %v", err)
	}
	if stored, err := o.st.GetBuild(ctx, invalid.BuildID); err != nil || stored != nil {
		t.Fatalf("invalid cluster build stored = %+v, err=%v", stored, err)
	}

	duplicateAuthority := *cmd
	duplicateAuthority.BuildID = "duplicate-build-resource-authority"
	duplicateAuthority.TemplateRef = "transient-duplicate-authority"
	duplicateAuthority.Config = make(map[string]string, len(cmd.Config)+1)
	for key, value := range cmd.Config {
		duplicateAuthority.Config[key] = value
	}
	duplicateAuthority.Config[buildcfg.NsBuilder] = `{"resources":{"cpu":2,"memory":"2GiB"}}`
	if err := o.registerClusterBuild(ctx, &duplicateAuthority); !errors.Is(err, api.ErrBadRequest) ||
		!strings.Contains(err.Error(), "must be normalized into build_resources") {
		t.Fatalf("duplicate Build resource authority error = %v", err)
	}
}

func TestIMGCreateDoesNotInheritBuildPhaseResourcePatch(t *testing.T) {
	policy := sandboxcfg.NodeResourcePolicy{}
	policy.ApplyDefaults()
	cfg := &config.Config{Sandbox: config.SandboxConfig{Resources: config.ResourcesConfig(policy)}}
	o, ctx := newAsyncConnectTestOrchestrator(t, cfg, &countingLauncher{})
	blocked := &blockedCreateVS{entered: make(chan struct{}), gate: make(chan struct{})}
	o.vs = blocked
	defer close(blocked.gate)

	manifestKey := strings.Repeat("6", 64)
	apiSecret, apiKey := defaultTestCredentials(t, manifestKey)
	if _, err := o.st.AddKeyPair(ctx, store.KeyPair{APISecret: apiSecret, ManifestKey: manifestKey}, "test", 0, ""); err != nil {
		t.Fatal(err)
	}
	persistID := types.TemplateID{
		Profile: types.ProfileBare,
		Kind:    types.KindImg,
		Ref:     "manifest://" + strings.Repeat("a", 64),
	}.String()
	build := &types.Build{
		BuildID: "phase-resource-isolation", TemplateID: "transient-phase-resource-isolation",
		PersistID: persistID, APISecret: apiSecret, ManifestKey: manifestKey,
		Profile: types.ProfileBare, Kind: types.KindImg, Status: types.BuildReady,
		Resources:          types.BuildResources{CPU: 8000, Memory: 16 << 30},
		PhaseResourcePatch: `{"capacity":{"cpu":8,"memory":"8GiB"},"allocatable":{"memory":"1GiB"}}`,
		CreatedUnix:        time.Now().Unix(),
	}
	if err := o.st.PutBuild(ctx, build); err != nil {
		t.Fatal(err)
	}

	created, err := o.Create(ctx, api.CreateReq{
		APIKey: apiKey, TemplateID: build.TemplateID, TimeoutSec: 60,
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-blocked.entered:
	case <-time.After(time.Second):
		t.Fatal("IMG Create did not reach network attachment")
	}
	stored, err := o.st.Get(ctx, created.ID)
	if err != nil || stored == nil {
		t.Fatalf("created sandbox = %+v, err=%v", stored, err)
	}
	if _, leaked := stored.Metadata[sandboxcfg.NsResource]; leaked {
		t.Fatalf("phase ResourcePatch leaked into IMG Create metadata: %+v", stored.Metadata)
	}
	tmpl, err := types.ParseTemplateID(stored.TemplateID)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := o.prepareSandboxLaunch(ctx, stored, tmpl)
	if err != nil {
		t.Fatal(err)
	}
	if got := prepared.Resources.Capacity; got.CPU != policy.Capacity.CPU || got.Memory != policy.Capacity.Memory {
		t.Fatalf("IMG Create resources = %+v, want target-node defaults %+v", got, policy.Capacity)
	}
}
