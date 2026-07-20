package placer

import (
	"context"
	"strings"
	"testing"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

type keyLeaseProvider struct {
	group    clusterstate.SandboxGroup
	auth     clusterstate.Secret
	manifest clusterstate.Secret
}

func (p keyLeaseProvider) Get(context.Context, string) (clusterstate.SandboxGroup, bool, error) {
	return p.group, true, nil
}

func (keyLeaseProvider) GetPlacementHint(context.Context, string) (clusterstate.PlacementHint, bool, error) {
	return clusterstate.PlacementHint{}, true, nil
}

func (p keyLeaseProvider) GetManifestKey(context.Context, string) (clusterstate.Secret, bool, error) {
	return p.manifest, p.manifest.Value != "", nil
}

func (p keyLeaseProvider) GetAuthKey(context.Context, string) (clusterstate.Secret, bool, error) {
	return p.auth, p.auth.Value != "", nil
}

func TestResolveNodeKeyLeaseBuildsCompleteSeparateBundle(t *testing.T) {
	provider := keyLeaseProvider{
		group: clusterstate.SandboxGroup{
			Group: "/group", RegistryAuth: clusterstate.Secret{Type: clusterstate.SecretRef, Value: "provider://registry/group"},
		},
		auth:     clusterstate.Secret{Type: clusterstate.SecretInline, Value: strings.Repeat("a", 64)},
		manifest: clusterstate.Secret{Type: clusterstate.SecretInline, Value: strings.Repeat("b", 64)},
	}
	lease, err := ResolveNodeKeyLease(context.Background(), provider, "/group", 1234)
	if err != nil {
		t.Fatal(err)
	}
	if lease.AuthKey.Fingerprint == lease.ManifestKey.Fingerprint || lease.RegistryAuth.Ref == "" || lease.ExpiresUnix != 1234 {
		t.Fatalf("incomplete key lease: %+v", lease)
	}
	if lease.AuthKey.Type != routesync.KeyMaterialInline || lease.ManifestKey.Type != routesync.KeyMaterialInline {
		t.Fatalf("unexpected key material: %+v", lease)
	}
}

func TestResolveNodeKeyLeaseRejectsMissingOrSharedDomains(t *testing.T) {
	provider := keyLeaseProvider{
		group:    clusterstate.SandboxGroup{Group: "/group"},
		auth:     clusterstate.Secret{Value: strings.Repeat("a", 64)},
		manifest: clusterstate.Secret{Value: strings.Repeat("a", 64)},
	}
	if _, err := ResolveNodeKeyLease(context.Background(), provider, "/group", 1234); err == nil {
		t.Fatal("shared AuthKey/ManifestKey material accepted")
	}
	provider.manifest = clusterstate.Secret{}
	if _, err := ResolveNodeKeyLease(context.Background(), provider, "/group", 1234); err == nil {
		t.Fatal("missing ManifestKey accepted")
	}
}
