package orch

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/regcreds"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestBuildTriggerCannotIncreaseRegisteredResources(t *testing.T) {
	o := testOrch(t)
	ctx := context.Background()
	apiKey := allowlistedBuildIdentity(t, o)
	b, err := o.RegisterBuild(ctx, apiKey, api.RegisterSpec{
		Profile: types.ProfileE2B, CPUCount: 2, MemoryMB: 2048,
		Metadata: map[string]string{sandboxcfg.NsNetwork: `{"hostname":"registered"}`},
	})
	if err != nil {
		t.Fatal(err)
	}
	original, err := o.st.GetBuild(ctx, b.BuildID)
	if err != nil {
		t.Fatal(err)
	}
	if err := o.TriggerBuild(ctx, apiKey, b.TemplateID, b.BuildID, api.TriggerSpec{
		FromImage: "registry.test/base:latest", CPUCount: 3, MemoryMB: 1024,
	}, api.BuildAuth{}); !errors.Is(err, api.ErrBadRequest) {
		t.Fatalf("oversized trigger error = %v", err)
	}
	unchanged, err := o.st.GetBuild(ctx, b.BuildID)
	if err != nil || unchanged.Status != types.BuildRegistered || unchanged.FromImage != original.FromImage {
		t.Fatalf("oversized trigger changed build = %+v, %v", unchanged, err)
	}

	if err := o.TriggerBuild(ctx, apiKey, b.TemplateID, b.BuildID, api.TriggerSpec{
		FromImage: "registry.test/base:latest", CPUCount: 1, MemoryMB: 1024,
		Metadata: map[string]string{
			sandboxcfg.NsResource: `{"capacity":{"cpu":99,"memory":"99GiB"}}`,
			sandboxcfg.NsNetwork:  `{"hostname":"trigger"}`,
		},
	}, api.BuildAuth{}); err != nil {
		t.Fatal(err)
	}
	stored, err := o.st.GetBuild(ctx, b.BuildID)
	if err != nil {
		t.Fatal(err)
	}
	spec, err := sandboxcfg.ParseSpec(stored.Metadata)
	if err != nil {
		t.Fatal(err)
	}
	if spec.Resource.Capacity == nil || spec.Resource.Capacity.CPU != 1 || spec.Resource.Capacity.Memory != "1024MiB" {
		t.Fatalf("bounded trigger capacity = %+v", spec.Resource.Capacity)
	}
	if stored.Metadata[sandboxcfg.NsNetwork] != `{"hostname":"trigger"}` {
		t.Fatalf("non-resource metadata was not preserved: %+v", stored.Metadata)
	}
}

func TestStandaloneBuildRegistrationFitsNodeCapacity(t *testing.T) {
	o := testOrchCfg(t, &config.Config{Builder: config.BuilderConfig{VCPU: 2, Memory: "2GiB"}})
	ctx := context.Background()
	apiKey := allowlistedBuildIdentity(t, o)
	for _, request := range []api.RegisterSpec{
		{Profile: types.ProfileE2B, CPUCount: 3, MemoryMB: 2048},
		{Profile: types.ProfileE2B, CPUCount: 2, MemoryMB: 2049},
	} {
		if _, err := o.RegisterBuild(ctx, apiKey, request); !errors.Is(err, api.ErrBadRequest) {
			t.Fatalf("oversized registration %+v: %v", request, err)
		}
	}
}

func TestBuildFromTemplateUsesTheBaseManifestKey(t *testing.T) {
	o := testOrch(t)
	ctx := context.Background()
	apiKey := allowlistedBuildIdentity(t, o)
	currentManifestKey := strings.Repeat("5a", 32)
	target, err := o.RegisterBuild(ctx, apiKey, api.RegisterSpec{
		Profile: types.ProfileE2B, CPUCount: 2, MemoryMB: 2048,
	})
	if err != nil {
		t.Fatal(err)
	}
	baseManifestKey := strings.Repeat("6c", 32)
	base := &types.Build{
		BuildID: "base-build", TemplateID: "transient-base",
		PersistID: "e2b-img-" + strings.Repeat("a", 64),
		AuthKey:   target.AuthKey, ManifestKey: baseManifestKey,
		Profile: types.ProfileE2B, CPUCount: 2, MemoryMB: 2048,
		Kind: types.KindImg, Status: types.BuildReady, CreatedUnix: 1,
	}
	if err := o.st.PutBuild(ctx, base); err != nil {
		t.Fatal(err)
	}
	if target.ManifestKey != currentManifestKey {
		t.Fatalf("registered target key = %q, want current key", target.ManifestKey)
	}
	pullToken, err := regcreds.Seal(currentManifestKey, regcreds.Creds{Username: "unused", Password: "unused"})
	if err != nil {
		t.Fatal(err)
	}
	if err := o.TriggerBuild(ctx, apiKey, target.TemplateID, target.BuildID, api.TriggerSpec{
		FromTemplate: base.PersistID,
		Steps:        []types.TemplateStep{{Type: "RUN", Args: []string{"true"}}},
	}, api.BuildAuth{PullToken: pullToken}); err != nil {
		t.Fatal(err)
	}
	stored, err := o.st.GetBuild(ctx, target.BuildID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.ManifestKey != baseManifestKey || stored.FromTemplate != base.PersistID || stored.RegistryAuth != "" {
		t.Fatalf("derived Build content identity = key %q base %q", stored.ManifestKey, stored.FromTemplate)
	}
}
