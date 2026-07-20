package orch

import (
	"context"
	"errors"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
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
