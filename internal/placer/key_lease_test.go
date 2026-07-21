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
	panic("non-atomic group read")
}
func (recordOnlyKeyLeaseProvider) GetPlacementHint(context.Context, string) (clusterstate.PlacementHint, bool, error) {
	panic("non-atomic placement read")
}
func (recordOnlyKeyLeaseProvider) GetManifestKey(context.Context, string) (clusterstate.Secret, bool, error) {
	panic("non-atomic ManifestKey read")
}
func (recordOnlyKeyLeaseProvider) GetAuthKey(context.Context, string) (clusterstate.Secret, bool, error) {
	panic("non-atomic AuthKey read")
}

func (p keyLeaseProvider) GetRecord(context.Context, string) (clusterstate.SandboxGroupRecord, bool, error) {
	return clusterstate.SandboxGroupRecord{
		Group: p.group.Group, AuthKey: p.auth, ManifestKey: p.manifest,
		RegistryAuth: p.group.RegistryAuth, Config: p.group.Config, ImageRepo: p.group.ImageRepo,
		TemplateRef: p.group.TemplateRef, AllowTemplateOverride: p.group.AllowTemplateOverride,
		TargetPort: p.group.TargetPort, Metadata: p.group.Metadata,
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
			Group: "/group", RegistryAuth: clusterstate.Secret{Type: clusterstate.SecretInline, Value: "registry-json"},
		},
		auth:     clusterstate.Secret{Type: clusterstate.SecretInline, Value: strings.Repeat("a", 64)},
		manifest: clusterstate.Secret{Type: clusterstate.SecretInline, Value: strings.Repeat("b", 64)},
	}
	lease, err := ResolveNodeKeyLease(context.Background(), provider, "/group", 1234)
	if err != nil {
		t.Fatal(err)
	}
	if lease.AuthKey.Fingerprint == lease.ManifestKey.Fingerprint || lease.RegistryAuth.Value != "registry-json" ||
		lease.RegistryAuth.Type != routesync.KeyMaterialInline || lease.ExpiresUnix != 1234 {
		t.Fatalf("incomplete key lease: %+v", lease)
	}
	if lease.AuthKey.Type != routesync.KeyMaterialInline || lease.ManifestKey.Type != routesync.KeyMaterialInline {
		t.Fatalf("unexpected key material: %+v", lease)
	}
}

func TestResolveNodeKeyLeaseUsesOneGroupRecordSnapshot(t *testing.T) {
	provider := recordOnlyKeyLeaseProvider{record: clusterstate.SandboxGroupRecord{
		Group:       "/group",
		AuthKey:     clusterstate.Secret{Type: clusterstate.SecretInline, Value: strings.Repeat("a", 64)},
		ManifestKey: clusterstate.Secret{Type: clusterstate.SecretInline, Value: strings.Repeat("b", 64)},
	}}
	lease, err := ResolveNodeKeyLease(context.Background(), provider, "/group", 1234)
	if err != nil || lease.Group != "/group" || lease.ExpiresUnix != 1234 {
		t.Fatalf("atomic record lease = %+v, %v", lease, err)
	}
}

func TestResolveNodeKeyLeaseRejectsUnmaterializedReferences(t *testing.T) {
	provider := keyLeaseProvider{
		group: clusterstate.SandboxGroup{Group: "/group"},
		auth: clusterstate.Secret{
			Type: clusterstate.SecretRef, Value: "provider://auth/group", Fingerprint: strings.Repeat("a", 24),
		},
		manifest: clusterstate.Secret{Type: clusterstate.SecretInline, Value: strings.Repeat("b", 64)},
	}
	if _, err := ResolveNodeKeyLease(context.Background(), provider, "/group", 1234); err == nil {
		t.Fatal("unmaterialized AuthKey reference accepted")
	}
	provider.auth = clusterstate.Secret{Type: clusterstate.SecretInline, Value: strings.Repeat("a", 64)}
	provider.group.RegistryAuth = clusterstate.Secret{Type: clusterstate.SecretRef, Value: "provider://registry/group"}
	if _, err := ResolveNodeKeyLease(context.Background(), provider, "/group", 1234); err == nil {
		t.Fatal("unmaterialized registry credential reference accepted")
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
