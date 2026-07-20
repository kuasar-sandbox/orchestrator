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

func TestFinalPlanSupportsReferencedAuthKeyAndMintsIndependentCapability(t *testing.T) {
	templateRef := "e2b-img-" + strings.Repeat("c", 64)
	provider := keyLeaseProvider{
		group: clusterstate.SandboxGroup{Group: "/group", TemplateRef: templateRef},
		auth: clusterstate.Secret{
			Type: clusterstate.SecretRef, Value: "provider://auth/group", Fingerprint: strings.Repeat("a", 24),
		},
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
		Nodes: []placement.CatalogNode{{NodeID: "node-1", SandboxSlotCapacity: 1}},
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
	if firstSpec.AuthKeyFingerprint != provider.auth.Fingerprint || len(firstSpec.AccessToken) != 64 ||
		firstSpec.AccessToken == secondSpec.AccessToken || firstSpec.Request.RawQuery != envelope.RawQuery ||
		firstSpec.Request.Header["X-Future"][0] != "kept" {
		t.Fatalf("referenced-key plan = %+v; second capability = %q", firstSpec, secondSpec.AccessToken)
	}
	if firstSpec.AccessToken == provider.auth.Value || firstSpec.AccessToken == provider.manifest.Value {
		t.Fatal("data-plane capability reused key material")
	}
	if lease, err := ResolveNodeKeyLease(context.Background(), provider, "/group", 1234); err != nil ||
		lease.AuthKey.Type != routesync.KeyMaterialRef {
		t.Fatalf("referenced node lease = %+v, %v", lease, err)
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
