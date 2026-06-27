package cluster

import (
	"context"
	"reflect"
	"testing"

	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/groupcfg"
)

type keyProvider func(context.Context, string) (groupcfg.Key, bool, error)

func (f keyProvider) Key(ctx context.Context, group string) (groupcfg.Key, bool, error) {
	return f(ctx, group)
}

type sandboxProvider func(context.Context, string) (groupcfg.SandboxConfig, bool, error)

func (f sandboxProvider) SandboxConfig(ctx context.Context, group string) (groupcfg.SandboxConfig, bool, error) {
	return f(ctx, group)
}

type placementProvider func(context.Context, string) (groupcfg.Placement, bool, error)

func (f placementProvider) Placement(ctx context.Context, group string) (groupcfg.Placement, bool, error) {
	return f(ctx, group)
}

type imagePullProvider func(context.Context, string) (groupcfg.ImagePull, bool, error)

func (f imagePullProvider) ImagePull(ctx context.Context, group string) (groupcfg.ImagePull, bool, error) {
	return f(ctx, group)
}

func TestMemberViewOwnersStableAndCapped(t *testing.T) {
	v1 := MemberView{Version: 1, Members: []string{"m3", "m1", "m2"}}
	v2 := MemberView{Version: 1, Members: []string{"m1", "m2", "m3"}}
	a, err := v1.Owners("/g", 9)
	if err != nil {
		t.Fatal(err)
	}
	b, err := v2.Owners("/g", 3)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(a, b) {
		t.Fatalf("owners depend on input order: %v vs %v", a, b)
	}
	if len(a) != 3 {
		t.Fatalf("owners len=%d, want capped to 3", len(a))
	}
}

func TestResolverAdapter(t *testing.T) {
	a := ResolverAdapter{Resolver: groupcfg.Resolver{
		Key: keyProvider(func(context.Context, string) (groupcfg.Key, bool, error) {
			return groupcfg.Key{ProjectID: "p", ManifestKey: "mk", AuthKey: "ak"}, true, nil
		}),
		Sandbox: sandboxProvider(func(context.Context, string) (groupcfg.SandboxConfig, bool, error) {
			return groupcfg.SandboxConfig{Config: map[string]string{"A": "B"}, ImageRepo: "img", TemplateRef: "tmpl"}, true, nil
		}),
		Placement: placementProvider(func(context.Context, string) (groupcfg.Placement, bool, error) {
			return groupcfg.Placement{NodeSelectors: []map[string]string{{"zone": "z1"}}}, true, nil
		}),
		ImagePull: imagePullProvider(func(context.Context, string) (groupcfg.ImagePull, bool, error) {
			return groupcfg.ImagePull{ImageRepo: "pull", RegistryAuth: "{}"}, true, nil
		}),
	}}
	g, found, err := a.Get(context.Background(), "/g")
	if err != nil || !found {
		t.Fatalf("Get found=%v err=%v", found, err)
	}
	if g.ImageRepo != "pull" || g.TemplateRef != "tmpl" || g.RegistryAuth.Value != "{}" {
		t.Fatalf("group projection mismatch: %+v", g)
	}
	k, found, err := a.GetAuthKey(context.Background(), "/g")
	if err != nil || !found || k.Value != "ak" {
		t.Fatalf("auth key = %+v found=%v err=%v", k, found, err)
	}
	p, found, err := a.GetPlacementHint(context.Background(), "/g")
	if err != nil || !found || p.NodeSelectors[0]["zone"] != "z1" {
		t.Fatalf("placement = %+v found=%v err=%v", p, found, err)
	}
}
