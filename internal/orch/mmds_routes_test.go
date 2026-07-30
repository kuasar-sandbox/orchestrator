package orch

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

const testMMDSSpec = `{"version":1,"routes":[{"path":"/x","type":"static","data":"d"}]}`

func testMMDSTemplateRef() string {
	return types.TemplateID{
		Profile: types.ProfileBare,
		Kind:    types.KindImg,
		Ref:     "manifest://" + strings.Repeat("a", 64),
	}.String()
}

func testMMDSRoutesConfig() config.MMDSRoutesConfig {
	return config.MMDSRoutesConfig{
		Enabled:             true,
		MaxRoutesPerSandbox: 32,
		MaxNamespaceBytes:   65536,
		Secret:              config.MMDSSecretRoutesConfig{MaxPerSandbox: 16},
		Service:             config.MMDSServiceRoutesConfig{MaxPerSandbox: 16},
		Static:              config.MMDSStaticRoutesConfig{MaxBodyBytes: 16384},
	}
}

func TestPrecheckClusterRejectsMMDSWhenPolicyDisabled(t *testing.T) {
	o := testOrch(t) // zero-value config.Config: MMDS.Routes.Enabled defaults to false
	_, _, fingerprint := allowlistedBuildIdentity(t, o)
	cmd := &routesync.Command{
		SID:                  "stable-g0",
		TemplateRef:          testMMDSTemplateRef(),
		Profile:              string(types.ProfileBare),
		APISecretFingerprint: fingerprint,
		Cluster:              &routesync.ClusterSandboxContext{Group: "group-a", RouteKey: "route-a"},
		Config:               map[string]string{sandboxcfg.NsMMDS: testMMDSSpec},
	}
	if _, _, _, err := o.precheckCluster(context.Background(), cmd); err == nil {
		t.Fatal("cluster create accepted an mmds specification while the policy is disabled")
	}
}

func TestHandleCommandCreateRejectsMMDSWithHTTP400(t *testing.T) {
	o := testOrch(t)
	_, _, fingerprint := allowlistedBuildIdentity(t, o)
	ack := o.HandleCommand(context.Background(), &routesync.Command{
		Kind:                 routesync.CmdCreate,
		CmdID:                "create-invalid-mmds",
		SID:                  "stable-g0",
		TemplateRef:          testMMDSTemplateRef(),
		Profile:              string(types.ProfileBare),
		APISecretFingerprint: fingerprint,
		Cluster:              &routesync.ClusterSandboxContext{Group: "group-a", RouteKey: "route-a"},
		Config:               map[string]string{sandboxcfg.NsMMDS: testMMDSSpec},
	})
	if ack.Status != routesync.AckRejected || ack.HTTPStatus != http.StatusBadRequest ||
		ack.Reason != "bad request: cluster create: MMDS metadata: MMDS routes are disabled by policy" {
		t.Fatalf("ack = %+v, want rejected HTTP 400", ack)
	}
}

func TestPrecheckClusterAppliesMMDSPolicyAndCanonicalizes(t *testing.T) {
	cfg := &config.Config{}
	cfg.MMDS.Routes = testMMDSRoutesConfig()
	o := testOrchCfg(t, cfg)
	_, _, fingerprint := allowlistedBuildIdentity(t, o)
	cmd := &routesync.Command{
		SID:                  "stable-g0",
		TemplateRef:          testMMDSTemplateRef(),
		Profile:              string(types.ProfileBare),
		APISecretFingerprint: fingerprint,
		Cluster:              &routesync.ClusterSandboxContext{Group: "group-a", RouteKey: "route-a"},
		// Static shorthand (no explicit "type") -- must come back canonicalized.
		Config: map[string]string{sandboxcfg.NsMMDS: `{"version":1,"routes":[{"path":"/x","data":"d"}]}`},
	}
	if _, _, _, err := o.precheckCluster(context.Background(), cmd); err != nil {
		t.Fatalf("precheckCluster: %v", err)
	}
	got := cmd.Config[sandboxcfg.NsMMDS]
	if !strings.Contains(got, `"type":"static"`) {
		t.Fatalf("expected canonicalized static type in persisted config, got %q", got)
	}
	if !strings.Contains(got, `"content_type":"application/octet-stream"`) {
		t.Fatalf("expected default content_type filled in, got %q", got)
	}
}

// TestPrecheckClusterDoesNotValidateServiceTargets proves the cluster Create
// path deliberately skips validateMMDSServiceTargets (see its doc comment):
// this node's own mmds.services registry may not be the destination node's,
// so a target absent here must not be rejected -- unlike the single-node
// Create path (TestCreateRejectsUnregisteredMMDSServiceTarget).
func TestPrecheckClusterDoesNotValidateServiceTargets(t *testing.T) {
	cfg := &config.Config{}
	cfg.MMDS.Routes = testMMDSRoutesConfig()
	// No cfg.MMDS.Services entries at all -- would be rejected by
	// validateMMDSServiceTargets if precheckCluster called it.
	o := testOrchCfg(t, cfg)
	_, _, fingerprint := allowlistedBuildIdentity(t, o)

	spec := `{"version":1,"services":[{"name":"svc1","target":"broker"}],` +
		`"routes":[{"path":"/svc","type":"service","service_name":"svc1"}]}`
	cmd := &routesync.Command{
		SID:                  "stable-g0",
		TemplateRef:          testMMDSTemplateRef(),
		Profile:              string(types.ProfileBare),
		APISecretFingerprint: fingerprint,
		Cluster:              &routesync.ClusterSandboxContext{Group: "group-a", RouteKey: "route-a"},
		Config:               map[string]string{sandboxcfg.NsMMDS: spec},
	}
	if _, _, _, err := o.precheckCluster(context.Background(), cmd); err != nil {
		t.Fatalf("precheckCluster rejected an unregistered service target: %v", err)
	}
}

func TestMigrationSandboxMetadataPreservesMMDS(t *testing.T) {
	meta := map[string]string{
		sandboxcfg.NsMMDS:        testMMDSSpec,
		sandboxcfg.NsCredentials: `{"service_secret":"x"}`,
		"keep":                   "value",
	}
	got := migrationSandboxMetadata(meta)
	if got[sandboxcfg.NsMMDS] != testMMDSSpec {
		t.Fatalf("mmds specification did not survive migration metadata scrubbing: %+v", got)
	}
	if _, ok := got[sandboxcfg.NsCredentials]; ok {
		t.Fatalf("credentials leaked into migration metadata: %+v", got)
	}
	if got["keep"] != "value" {
		t.Fatalf("unrelated metadata was dropped: %+v", got)
	}
}

func TestClusterSandboxMetadataPreservesMMDS(t *testing.T) {
	config := map[string]string{
		sandboxcfg.NsMMDS:        testMMDSSpec,
		sandboxcfg.NsCredentials: `{"service_secret":"x"}`,
		"keep":                   "value",
	}
	got := clusterSandboxMetadata(config)
	if got[sandboxcfg.NsMMDS] != testMMDSSpec {
		t.Fatalf("mmds specification did not survive cluster metadata scrubbing: %+v", got)
	}
	if _, ok := got[sandboxcfg.NsCredentials]; ok {
		t.Fatalf("credentials leaked into cluster metadata: %+v", got)
	}
	if got["keep"] != "value" {
		t.Fatalf("unrelated metadata was dropped: %+v", got)
	}
}

func TestCreateRejectsMMDSWhenPolicyDisabled(t *testing.T) {
	root := t.TempDir()
	cfg := &config.Config{}
	cfg.Paths.RunRoot = filepath.Join(root, "run")
	cfg.Paths.BaseRoot = filepath.Join(root, "base")
	// MMDS.Routes.Enabled defaults to false (zero value) -- deliberately not set.
	o := testOrchCfg(t, cfg)
	o.vs = stubVS{}
	apiKey, _, _ := allowlistedBuildIdentity(t, o)

	_, err := o.Create(context.Background(), api.CreateReq{
		APIKey: apiKey, TemplateID: testMMDSTemplateRef(), TimeoutSec: 60,
		Metadata: map[string]string{sandboxcfg.NsMMDS: testMMDSSpec},
	})
	if !errors.Is(err, api.ErrBadRequest) {
		t.Fatalf("Create error = %v, want ErrBadRequest", err)
	}
	for _, path := range []string{cfg.Paths.RunRoot, cfg.Paths.BaseRoot} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("disabled-policy mmds metadata created %s: stat error = %v", path, err)
		}
	}
}

func TestCreateRejectsUnregisteredMMDSServiceTarget(t *testing.T) {
	root := t.TempDir()
	cfg := &config.Config{}
	cfg.Paths.RunRoot = filepath.Join(root, "run")
	cfg.Paths.BaseRoot = filepath.Join(root, "base")
	cfg.MMDS.Routes = testMMDSRoutesConfig()
	// No cfg.MMDS.Services entries at all -- any target is unregistered.
	o := testOrchCfg(t, cfg)
	o.vs = stubVS{}
	apiKey, _, _ := allowlistedBuildIdentity(t, o)

	spec := `{"version":1,"services":[{"name":"svc1","target":"broker"}],` +
		`"routes":[{"path":"/svc","type":"service","service_name":"svc1"}]}`
	_, err := o.Create(context.Background(), api.CreateReq{
		APIKey: apiKey, TemplateID: testMMDSTemplateRef(), TimeoutSec: 60,
		Metadata: map[string]string{sandboxcfg.NsMMDS: spec},
	})
	if !errors.Is(err, api.ErrBadRequest) {
		t.Fatalf("Create error = %v, want ErrBadRequest", err)
	}
	for _, path := range []string{cfg.Paths.RunRoot, cfg.Paths.BaseRoot} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("unregistered service target still created %s: stat error = %v", path, err)
		}
	}
}

func TestValidateMMDSServiceTargetsAcceptsRegisteredTarget(t *testing.T) {
	cfg := &config.Config{}
	cfg.MMDS.Services = map[string]config.MMDSServiceRegistryEntry{
		"broker": {Endpoint: "unix:///tmp/broker.sock"},
	}
	o := testOrchCfg(t, cfg)

	spec := sandboxcfg.MMDSSpec{Services: []sandboxcfg.MMDSServiceSpec{{Name: "svc1", Target: "broker"}}}
	if err := o.validateMMDSServiceTargets(spec); err != nil {
		t.Fatalf("validateMMDSServiceTargets: %v", err)
	}
}

func TestValidateMMDSServiceTargetsRejectsUnregisteredTarget(t *testing.T) {
	cfg := &config.Config{}
	cfg.MMDS.Services = map[string]config.MMDSServiceRegistryEntry{
		"broker": {Endpoint: "unix:///tmp/broker.sock"},
	}
	o := testOrchCfg(t, cfg)

	spec := sandboxcfg.MMDSSpec{Services: []sandboxcfg.MMDSServiceSpec{{Name: "svc1", Target: "typo-broker"}}}
	if err := o.validateMMDSServiceTargets(spec); err == nil {
		t.Fatal("expected an unregistered target to be rejected")
	}
}

func TestValidateMMDSServiceTargetsIgnoresSpecWithoutServices(t *testing.T) {
	o := testOrchCfg(t, &config.Config{})
	if err := o.validateMMDSServiceTargets(sandboxcfg.MMDSSpec{}); err != nil {
		t.Fatalf("validateMMDSServiceTargets on an empty spec: %v", err)
	}
}

func TestRegisterBuildRejectsMMDSNamespace(t *testing.T) {
	o := testOrch(t)
	ctx := context.Background()
	apiKey, _, _ := allowlistedBuildIdentity(t, o)

	_, err := o.RegisterBuild(ctx, apiKey, api.RegisterSpec{
		Name: "bare-template", Tags: []string{"bare-tag"}, Profile: types.ProfileBare,
		Metadata: map[string]string{sandboxcfg.NsMMDS: testMMDSSpec},
	})
	if !errors.Is(err, api.ErrBadRequest) {
		t.Fatalf("RegisterBuild error = %v, want ErrBadRequest", err)
	}
}

func TestTriggerBuildRejectsMMDSNamespace(t *testing.T) {
	o := testOrch(t)
	ctx := context.Background()
	apiKey, _, _ := allowlistedBuildIdentity(t, o)

	b, err := o.RegisterBuild(ctx, apiKey, api.RegisterSpec{
		Name: "bare-template", Tags: []string{"bare-tag"}, Profile: types.ProfileBare,
	})
	if err != nil {
		t.Fatalf("RegisterBuild: %v", err)
	}

	err = o.TriggerBuild(ctx, apiKey, b.TemplateID, b.BuildID, api.TriggerSpec{
		FromImage: "reg.example.com/app:tag",
		Metadata:  map[string]string{sandboxcfg.NsMMDS: testMMDSSpec},
	}, api.BuildAuth{})
	if !errors.Is(err, api.ErrBadRequest) {
		t.Fatalf("TriggerBuild error = %v, want ErrBadRequest", err)
	}
}
