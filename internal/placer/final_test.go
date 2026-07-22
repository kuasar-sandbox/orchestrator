package placer

import (
	"context"
	"net/http"
	"strings"
	"testing"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/clustercfg"
	"github.com/kuasar-sandbox/orchestrator/internal/placement"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

func TestFinalPlanMintsIndependentCapabilityAndPreservesRequest(t *testing.T) {
	templateRef := "e2b-img-" + strings.Repeat("c", 64)
	provider := keyLeaseProvider{
		group:    clusterstate.SandboxGroup{Group: "/group", TemplateRef: templateRef},
		auth:     clusterstate.Secret{Type: clusterstate.SecretInline, Value: strings.Repeat("a", 64)},
		manifest: clusterstate.Secret{Type: clusterstate.SecretInline, Value: strings.Repeat("b", 64)},
	}
	service, err := NewFinalService(provider, clustercfg.PlacementConfig{})
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := clusterstate.NewNodeRequestEnvelopeV1(
		http.MethodPost, "/sandboxes", "future=true", http.Header{"X-Future": {"kept"}},
		[]byte(`{"future":{"mode":"fast"}}`),
	)
	if err != nil {
		t.Fatal(err)
	}
	request := PlanRequest{
		Kind: PlanSandbox, Group: "/group", RouteKey: "route-1",
		Catalog: installTestCatalog(t, service, []placement.CatalogNode{{
			NodeID: "node-1", SandboxSlotCapacity: 1, Capabilities: map[string]bool{"sandbox": true},
		}}),
		Sandbox: &SandboxPlanInput{
			SandboxID: "sandbox-1", Demand: placement.SandboxDemand{SlotUnits: 1}, Request: envelope,
		},
	}
	first, err := service.Plan(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	second, err := service.Plan(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	firstSpec, err := clusterstate.ParseSandboxDispatchSpec(first.DispatchSpec)
	if err != nil {
		t.Fatal(err)
	}
	secondSpec, err := clusterstate.ParseSandboxDispatchSpec(second.DispatchSpec)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := ResolveNodeKeyLease(context.Background(), provider, "/group", 1234)
	if err != nil {
		t.Fatal(err)
	}
	if firstSpec.AuthKeyFingerprint != lease.AuthKey.Fingerprint || len(firstSpec.AccessToken) != 64 ||
		firstSpec.AccessToken == secondSpec.AccessToken || firstSpec.Request.RawQuery != envelope.RawQuery ||
		firstSpec.Request.Header["X-Future"][0] != "kept" {
		t.Fatalf("plan = %+v; second capability = %q", firstSpec, secondSpec.AccessToken)
	}
	if firstSpec.AccessToken == provider.auth.Value || firstSpec.AccessToken == provider.manifest.Value {
		t.Fatal("data-plane capability reused key material")
	}
	if lease.AuthKey.Type != routesync.KeyMaterialInline {
		t.Fatalf("node lease = %+v", lease)
	}
}

func TestFinalPlanDerivesSandboxDemandFromEffectiveGroupConfig(t *testing.T) {
	templateRef := "e2b-img-" + strings.Repeat("c", 64)
	provider := keyLeaseProvider{
		group: clusterstate.SandboxGroup{
			Group: "/group", TemplateRef: templateRef,
			Config: map[string]string{
				"kuasar-sandbox.resource": `{"capacity":{"cpu":4,"memory":"8GiB"},"allocatable":{"cpu":1,"memory":"2GiB"}}`,
			},
		},
		auth:     clusterstate.Secret{Type: clusterstate.SecretInline, Value: strings.Repeat("a", 64)},
		manifest: clusterstate.Secret{Type: clusterstate.SecretInline, Value: strings.Repeat("b", 64)},
	}
	service, err := NewFinalService(provider, clustercfg.PlacementConfig{})
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := clusterstate.NewNodeRequestEnvelopeV1(http.MethodPost, "/sandboxes", "", nil, []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	response, err := service.Plan(context.Background(), PlanRequest{
		Kind: PlanSandbox, Group: "/group", RouteKey: "route-1",
		Catalog: installTestCatalog(t, service, []placement.CatalogNode{{
			NodeID: "node-1", SandboxSlotCapacity: 4, Capabilities: map[string]bool{"sandbox": true},
		}}),
		Sandbox: &SandboxPlanInput{
			SandboxID: "sandbox-1", Demand: placement.SandboxDemand{SlotUnits: 1}, Request: envelope,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	demand, err := placement.ParseNormalizedDemand(response.NormalizedDemand)
	if err != nil || demand.Sandbox == nil {
		t.Fatalf("normalized demand = %+v, %v", demand, err)
	}
	if demand.Sandbox.FloorMemory != 2<<30 || demand.Sandbox.StartupBudgetMemory != 2<<30 ||
		demand.Sandbox.AllocatableAtSnapshot != 0 {
		t.Fatalf("derived Sandbox demand = %+v", demand.Sandbox)
	}
}

func TestFinalPlanLeavesSnapshotCapacityForNodeAdmission(t *testing.T) {
	templateRef := "e2b-snp-" + strings.Repeat("c", 64)
	provider := keyLeaseProvider{
		group: clusterstate.SandboxGroup{
			Group: "/group", TemplateRef: templateRef,
			Config: map[string]string{
				"kuasar-sandbox.resource": `{"capacity":{"cpu":8,"memory":"16GiB"}}`,
			},
		},
		auth:     clusterstate.Secret{Type: clusterstate.SecretInline, Value: strings.Repeat("a", 64)},
		manifest: clusterstate.Secret{Type: clusterstate.SecretInline, Value: strings.Repeat("b", 64)},
	}
	service, err := NewFinalService(provider, clustercfg.PlacementConfig{})
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := clusterstate.NewNodeRequestEnvelopeV1(http.MethodPost, "/sandboxes", "", nil, []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	response, err := service.Plan(context.Background(), PlanRequest{
		Kind: PlanSandbox, Group: "/group", RouteKey: "route-1",
		Catalog: installTestCatalog(t, service, []placement.CatalogNode{{
			NodeID: "node-1", SandboxSlotCapacity: 1, Capabilities: map[string]bool{"sandbox": true},
		}}),
		Sandbox: &SandboxPlanInput{
			SandboxID: "sandbox-1", Demand: placement.SandboxDemand{SlotUnits: 1}, Request: envelope,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	demand, err := placement.ParseNormalizedDemand(response.NormalizedDemand)
	if err != nil || demand.Sandbox == nil {
		t.Fatalf("normalized demand = %+v, %v", demand, err)
	}
	if demand.Sandbox.FloorMemory != 0 || demand.Sandbox.StartupBudgetMemory != 0 {
		t.Fatalf("snapshot capacity leaked into placement demand: %+v", demand.Sandbox)
	}
}

func installTestCatalog(t *testing.T, service *FinalService, nodes []placement.CatalogNode) placement.CatalogReference {
	t.Helper()
	snapshot, err := placement.NewCatalogSnapshot(
		"cluster-1", "generation-1", 1, strings.Repeat("d", 64), 1, nodes,
	)
	if err != nil {
		t.Fatal(err)
	}
	response, err := service.catalogs.install(CatalogSyncRequest{
		Reference: snapshot.Reference, Nodes: snapshot.Nodes, Final: true,
	})
	if err != nil || !response.Installed {
		t.Fatalf("install test catalog = %+v, %v", response, err)
	}
	return snapshot.Reference
}

func TestShuffleShardingFailsClosedWhenApplicableShardLabelIsAbsent(t *testing.T) {
	selectors, applicable := effectiveCatalogSelectors("/group", []placement.CatalogNode{{
		NodeID: "node-1", Labels: map[string]string{"pool": "gpu"},
	}}, nil, []clustercfg.ShuffleRule{{
		Selector: map[string]string{"pool": "gpu"}, ShardBy: "zone", N: 1,
	}})
	if !applicable || len(selectors) != 0 {
		t.Fatalf("empty applicable shard = selectors=%v applicable=%v", selectors, applicable)
	}
}

type testGroupAuthorizer struct {
	keyLeaseProvider
	called bool
}

func (p *testGroupAuthorizer) VerifyAPIKey(_ context.Context, group, apiKey string) (bool, error) {
	p.called = true
	return group == "/group" && apiKey == "caller-token", nil
}

func TestReferencedAuthKeyUsesProviderAuthorization(t *testing.T) {
	provider := &testGroupAuthorizer{keyLeaseProvider: keyLeaseProvider{
		group: clusterstate.SandboxGroup{Group: "/group"},
		auth: clusterstate.Secret{
			Type: clusterstate.SecretRef, Value: "provider://auth/group", Fingerprint: strings.Repeat("a", 24),
		},
	}}
	authorized, err := verifyProviderAPIKey(context.Background(), provider, "/group", "caller-token")
	if err != nil || !authorized || !provider.called {
		t.Fatalf("Provider authorization = %v, %v, called=%v", authorized, err, provider.called)
	}
}
