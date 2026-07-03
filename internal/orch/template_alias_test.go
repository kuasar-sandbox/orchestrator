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
		ManifestKey: mk, Profile: types.ProfileE2B, Kind: types.KindImg,
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
	if got := o.resolveTemplateAlias(ctx, apiKey, "nope"); got != "" {
		t.Errorf(`unknown ref should be "", got %q`, got)
	}
	// a different tenant's key cannot resolve this build.
	other, _ := apikey.Mint([]byte(strings.Repeat("\x05", 32)))
	if got := o.resolveTemplateAlias(ctx, other, "my-app"); got != "" {
		t.Errorf(`wrong tenant should be "", got %q`, got)
	}
}
