package orch

import (
	"context"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/apikey"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// TestResolveTemplateAlias: a ready build's transient register id, its name, and its
// persist id all resolve to the persist id; unknown refs and other tenants do not.
func TestResolveTemplateAlias(t *testing.T) {
	o := testOrch(t)
	ctx := context.Background()
	mk := strings.Repeat("4", 64)
	raw, _ := hex.DecodeString(mk)
	apiKey, err := apikey.Mint(raw)
	if err != nil {
		t.Fatal(err)
	}
	persist := "e2b-img-" + strings.Repeat("a", 64)
	b := &types.Build{
		BuildID: "b1", TemplateID: "transient-xyz", PersistID: persist,
		AuthKey: mk, ManifestKey: strings.Repeat("5", 64), CPUCount: 2, MemoryMB: 2048,
		Profile: types.ProfileE2B, Kind: types.KindImg,
		Status: types.BuildReady, Names: []string{"my-app", persist}, Aliases: []string{persist},
		CreatedUnix: 1,
	}
	if err := o.st.PutBuild(ctx, b); err != nil {
		t.Fatal(err)
	}

	for name, ref := range map[string]string{"transient": "transient-xyz", "name": "my-app", "persist": persist} {
		if got := o.resolveTemplateAlias(ctx, apiKey, ref); got != persist {
			t.Errorf("%s: resolveTemplateAlias(%q)=%q want %q", name, ref, got, persist)
		}
	}
	currentManifestKey := strings.Repeat("6", 64)
	gotManifestKey, err := templateManifestKey(currentManifestKey, o.templateBuild(ctx, apiKey, "my-app"))
	if err != nil || gotManifestKey != b.ManifestKey {
		t.Fatalf("rotated group key selected template ManifestKey %q, %v; want %q", gotManifestKey, err, b.ManifestKey)
	}
	if got := o.resolveTemplateAlias(ctx, apiKey, "nope"); got != "" {
		t.Errorf(`unknown ref should be "", got %q`, got)
	}
	// a different tenant's key cannot resolve this build.
	other, _ := apikey.Mint([]byte(strings.Repeat("\x05", 32)))
	if got := o.resolveTemplateAlias(ctx, other, "my-app"); got != "" {
		t.Errorf(`wrong tenant should be "", got %q`, got)
	}
}

func TestTemplateManifestKeyFallbackAndValidation(t *testing.T) {
	currentManifestKey := strings.Repeat("6", 64)
	got, err := templateManifestKey(currentManifestKey, nil)
	if err != nil || got != currentManifestKey {
		t.Fatalf("self-describing template ManifestKey = %q, %v", got, err)
	}
	if _, err := templateManifestKey(currentManifestKey, &types.Build{}); err == nil {
		t.Fatal("ready template without its immutable ManifestKey was accepted")
	}
}
