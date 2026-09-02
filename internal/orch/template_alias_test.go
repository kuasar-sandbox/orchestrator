package orch

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// TestResolveTemplateAlias: a ready build's transient register id, its name, and its
// persist id all resolve to the persist id; unknown refs and other tenants do not.
func TestResolveTemplateAlias(t *testing.T) {
	o := testOrch(t)
	ctx := context.Background()
	mk := strings.Repeat("4", 64)
	apiSecret, apiKey := defaultTestCredentials(t, mk)
	persist := types.TemplateID{Profile: types.ProfileE2B, Kind: types.KindImg, Ref: "manifest://" + strings.Repeat("a", 64)}.String()
	b := &types.Build{
		BuildID: "b1", TemplateID: "transient-xyz", PersistID: persist,
		APISecret: apiSecret, ManifestKey: mk, Profile: types.ProfileE2B, Kind: types.KindImg,
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
	other := mintTestAPIKey(t, strings.Repeat("05", 32))
	if got := o.resolveTemplateAlias(ctx, other, "my-app"); got != "" {
		t.Errorf(`wrong tenant should be "", got %q`, got)
	}
}

func TestCanonicalTemplateIDCreatesAllArtifactKindsAfterBuildRetention(t *testing.T) {
	cfg := &config.Config{
		Paths: config.PathsConfig{
			RunRoot:  filepath.Join(t.TempDir(), "run"),
			BaseRoot: filepath.Join(t.TempDir(), "base"),
		},
		Sandbox: config.SandboxConfig{DeadTTL: "1h"},
		Builder: config.BuilderConfig{TerminalTTL: "1h"},
	}
	lc := &countingLauncher{}
	o, lifecycleCtx := newAsyncConnectTestOrchestrator(t, cfg, lc)
	blocked := &blockedCreateVS{entered: make(chan struct{}), gate: make(chan struct{})}
	o.vs = blocked
	manifestKey := strings.Repeat("d", 64)
	apiSecret, apiKey := defaultTestCredentials(t, manifestKey)
	if _, err := o.st.AddKeyPair(context.Background(), store.KeyPair{APISecret: apiSecret, ManifestKey: manifestKey}, "retention", 0, ""); err != nil {
		t.Fatal(err)
	}

	for index, kind := range []types.Kind{types.KindImg, types.KindSbx, types.KindSnp} {
		canonical := types.TemplateID{
			Profile: types.ProfileBare,
			Kind:    kind,
			Ref:     "manifest://" + strings.Repeat(string(rune('a'+index)), 64),
		}.String()
		build := &types.Build{
			BuildID:    "canonical-after-retention-" + string(rune('a'+index)),
			TemplateID: "transient-retention-" + string(rune('a'+index)), PersistID: canonical,
			APISecret: apiSecret, ManifestKey: manifestKey, Profile: types.ProfileBare, Kind: kind,
			Status: types.BuildReady, Names: []string{canonical}, Aliases: []string{canonical},
			Metadata:    map[string]string{sandboxcfg.NsNetwork: `{"hostname":"must-not-be-inherited"}`},
			CreatedUnix: 1, FinishedUnix: time.Now().Add(-2 * time.Hour).Unix(),
		}
		if err := o.st.PutBuild(context.Background(), build); err != nil {
			t.Fatal(err)
		}
		if err := o.reapTerminalHistory(context.Background(), time.Now()); err != nil {
			t.Fatal(err)
		}
		if retained, err := o.st.GetBuild(context.Background(), build.BuildID); err != nil || retained != nil {
			t.Fatalf("%s Build retained after TTL: %+v, %v", kind, retained, err)
		}

		created, err := o.Create(lifecycleCtx, api.CreateReq{
			APIKey: apiKey, TemplateID: canonical, TimeoutSec: 60,
			Metadata: map[string]string{"request-metadata": "kept"},
		})
		if err != nil || created == nil {
			t.Fatalf("Create %s canonical TemplateID after Build deletion = %+v, %v", kind, created, err)
		}
		wantMode, err := types.LaunchModeForTemplate(kind)
		if err != nil {
			t.Fatal(err)
		}
		if created.TemplateID != canonical || created.LaunchMode != wantMode || created.Metadata["request-metadata"] != "kept" {
			t.Fatalf("Create %s result = %+v", kind, created)
		}
		if _, inherited := created.Metadata[sandboxcfg.NsNetwork]; inherited {
			t.Fatalf("Create %s implicitly recovered IMG/Build metadata: %+v", kind, created.Metadata)
		}
	}

	// Release all asynchronous Create workers before the Store test cleanup.
	close(blocked.gate)
	drainCtx, cancelDrain := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelDrain()
	if err := o.DrainLaunches(drainCtx); err != nil {
		t.Fatal(err)
	}
}
