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

type recordOnlyKeyLeaseProvider struct {
	record clusterstate.SandboxGroupRecord
}

func (p recordOnlyKeyLeaseProvider) GetRecord(context.Context, string) (clusterstate.SandboxGroupRecord, bool, error) {
	return p.record, true, nil
}

func (recordOnlyKeyLeaseProvider) Get(context.Context, string) (clusterstate.SandboxGroup, bool, error) {
	panic("ResolveNodeKeyLease performed a non-atomic Provider read")
}

func (recordOnlyKeyLeaseProvider) GetPlacementHint(context.Context, string) (clusterstate.PlacementHint, bool, error) {
	return clusterstate.PlacementHint{}, true, nil
}

func (recordOnlyKeyLeaseProvider) GetManifestKey(context.Context, string) (clusterstate.Secret, bool, error) {
	panic("ResolveNodeKeyLease performed a non-atomic Provider read")
}

func (recordOnlyKeyLeaseProvider) GetAuthKey(context.Context, string) (clusterstate.Secret, bool, error) {
	panic("ResolveNodeKeyLease performed a non-atomic Provider read")
}

func (p keyLeaseProvider) GetRecord(context.Context, string) (clusterstate.SandboxGroupRecord, bool, error) {
	return clusterstate.SandboxGroupRecord{
		Group: p.group.Group, AuthKey: p.auth, ManifestKey: p.manifest, RegistryAuth: p.group.RegistryAuth,
	}, true, nil
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

func TestResolveNodeKeyLeaseUsesOneAtomicGroupRecord(t *testing.T) {
	provider := recordOnlyKeyLeaseProvider{record: clusterstate.SandboxGroupRecord{
		Group:       "/group",
		AuthKey:     clusterstate.Secret{Type: clusterstate.SecretInline, Value: strings.Repeat("a", 64)},
		ManifestKey: clusterstate.Secret{Type: clusterstate.SecretInline, Value: strings.Repeat("b", 64)},
	}}
	if _, err := ResolveNodeKeyLease(context.Background(), provider, "/group", 1234); err != nil {
		t.Fatal(err)
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
