package orch

import (
	"context"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestClaimClusterCreateRejectsInflightAndStoredSandboxIDs(t *testing.T) {
	ctx := context.Background()
	o := testOrch(t)

	if err := o.claimClusterCreate(ctx, ""); err == nil {
		t.Fatal("empty sandbox id was accepted")
	}
	if err := o.claimClusterCreate(ctx, "inflight"); err != nil {
		t.Fatalf("first claim: %v", err)
	}
	if err := o.claimClusterCreate(ctx, "inflight"); err == nil {
		t.Fatal("concurrent duplicate claim was accepted")
	}
	o.releaseClusterCreate("inflight")
	if err := o.claimClusterCreate(ctx, "inflight"); err != nil {
		t.Fatalf("released claim was not reusable: %v", err)
	}
	o.releaseClusterCreate("inflight")

	manifestKey := strings.Repeat("b", 64)
	if err := o.st.Put(ctx, &types.Sandbox{
		ID: "stored", State: types.StateRunning,
		APISecret: deriveTestAPISecret(t, manifestKey), ManifestKey: manifestKey,
	}); err != nil {
		t.Fatal(err)
	}
	if err := o.claimClusterCreate(ctx, "stored"); err == nil {
		t.Fatal("stored sandbox id was accepted")
	}
	if _, claimed := o.clusterCreates["stored"]; claimed {
		t.Fatal("failed stored-id claim leaked its in-flight marker")
	}
}

func TestPrecheckClusterRejectsInvalidRestore(t *testing.T) {
	o := testOrch(t)
	_, _, fingerprint := allowlistedBuildIdentity(t, o)
	cmd := &routesync.Command{
		TemplateRef:          "bare-img-" + strings.Repeat("a", 64),
		APISecretFingerprint: fingerprint,
		Config: map[string]string{
			sandboxcfg.NsRestore: `{"prefetch":"disk"}`,
		},
	}
	if _, _, err := o.precheckCluster(context.Background(), cmd); err == nil {
		t.Fatal("cluster create accepted invalid restore policy")
	}
}
