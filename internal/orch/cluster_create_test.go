package orch

import (
	"context"
	"strings"
	"testing"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
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
		ID: "stored", Profile: types.ProfileBare, State: types.StateRunning,
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
		Profile:              string(types.ProfileBare),
		APISecretFingerprint: fingerprint,
		Cluster:              &routesync.ClusterSandboxContext{Group: "group-a", RouteKey: "route-a"},
		Config: map[string]string{
			sandboxcfg.NsRestore: `{"prefetch":"disk"}`,
		},
	}
	if _, _, err := o.precheckCluster(context.Background(), cmd); err == nil {
		t.Fatal("cluster create accepted invalid restore policy")
	}
}

func TestPrecheckClusterRequiresConsistentProfileAndContext(t *testing.T) {
	o := testOrch(t)
	_, _, fingerprint := allowlistedBuildIdentity(t, o)
	templateRef := "bare-img-" + strings.Repeat("a", 64)
	valid := func() *routesync.Command {
		return &routesync.Command{
			SID:                  "stable-g0",
			TemplateRef:          templateRef,
			Profile:              string(types.ProfileBare),
			APISecretFingerprint: fingerprint,
			Cluster:              &routesync.ClusterSandboxContext{Group: "group-a", RouteKey: "route-a", AuthSandboxID: "stable"},
		}
	}

	if _, tmpl, err := o.precheckCluster(context.Background(), valid()); err != nil {
		t.Fatalf("valid cluster create rejected: %v", err)
	} else if tmpl.Profile != types.ProfileBare {
		t.Fatalf("template profile = %q, want bare", tmpl.Profile)
	}

	tests := map[string]func(*routesync.Command){
		"missing profile": func(cmd *routesync.Command) { cmd.Profile = "" },
		"invalid profile": func(cmd *routesync.Command) { cmd.Profile = "other" },
		"missing context": func(cmd *routesync.Command) { cmd.Cluster = nil },
		"missing group":   func(cmd *routesync.Command) { cmd.Cluster.Group = "" },
		"missing route":   func(cmd *routesync.Command) { cmd.Cluster.RouteKey = "" },
		"profile mismatch": func(cmd *routesync.Command) {
			cmd.Profile = string(types.ProfileE2B)
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			cmd := valid()
			mutate(cmd)
			if _, _, err := o.precheckCluster(context.Background(), cmd); err == nil {
				t.Fatal("invalid cluster create was accepted")
			}
		})
	}
	if _, _, err := o.precheckCluster(context.Background(), nil); err == nil {
		t.Fatal("nil cluster create command was accepted")
	}
}

func TestClusterSandboxMetadataExcludesLegacyClusterIdentity(t *testing.T) {
	input := map[string]string{
		"user-key":                     "user-value",
		clusterstate.ObjectMetadataKey: `{"group":"legacy","route_key":"legacy"}`,
	}
	metadata := clusterSandboxMetadata(input)
	if metadata["user-key"] != "user-value" {
		t.Fatalf("user metadata = %q", metadata["user-key"])
	}
	if _, found := metadata[clusterstate.ObjectMetadataKey]; found {
		t.Fatal("legacy cluster identity remained in sandbox metadata")
	}
	if _, found := input[clusterstate.ObjectMetadataKey]; !found {
		t.Fatal("metadata filtering mutated the command config")
	}
}

func TestValidateClusterSandboxContext(t *testing.T) {
	stored := &types.Sandbox{
		ID:                 "stable-g0",
		Profile:            types.ProfileBare,
		Cluster:            &types.ClusterSandboxContext{Group: "group-a", RouteKey: "route-a"},
		AuthSandboxIDValue: "stable",
	}
	valid := func() *routesync.Command {
		return &routesync.Command{
			Profile: string(types.ProfileBare),
			Cluster: &routesync.ClusterSandboxContext{Group: "group-a", RouteKey: "route-a", AuthSandboxID: "stable"},
		}
	}
	if err := validateClusterSandboxContext(stored, valid()); err != nil {
		t.Fatalf("matching context rejected: %v", err)
	}

	tests := map[string]func(*types.Sandbox, *routesync.Command){
		"profile": func(_ *types.Sandbox, cmd *routesync.Command) { cmd.Profile = string(types.ProfileE2B) },
		"group":   func(_ *types.Sandbox, cmd *routesync.Command) { cmd.Cluster.Group = "group-b" },
		"route":   func(_ *types.Sandbox, cmd *routesync.Command) { cmd.Cluster.RouteKey = "route-b" },
		"auth id": func(_ *types.Sandbox, cmd *routesync.Command) { cmd.Cluster.AuthSandboxID = "other" },
		"missing stored context": func(sb *types.Sandbox, _ *routesync.Command) {
			sb.Cluster = nil
		},
		"missing command context": func(_ *types.Sandbox, cmd *routesync.Command) {
			cmd.Cluster = nil
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			sb := *stored
			cmd := valid()
			mutate(&sb, cmd)
			if err := validateClusterSandboxContext(&sb, cmd); err == nil {
				t.Fatal("mismatched cluster context was accepted")
			}
		})
	}
}

func TestHandleClusterConnectRejectsContextMismatch(t *testing.T) {
	o := testOrch(t)
	_, manifestKey, fingerprint := allowlistedBuildIdentity(t, o)
	sb := &types.Sandbox{
		ID:                 "stable-g0",
		Profile:            types.ProfileBare,
		Cluster:            &types.ClusterSandboxContext{Group: "group-a", RouteKey: "route-a"},
		AuthSandboxIDValue: "stable",
		State:              types.StatePaused,
		APISecret:          deriveTestAPISecret(t, manifestKey),
		ManifestKey:        manifestKey,
	}
	if err := o.st.Put(context.Background(), sb); err != nil {
		t.Fatal(err)
	}
	ack := o.HandleCommand(context.Background(), &routesync.Command{
		CmdID:                "connect-1",
		Kind:                 routesync.CmdConnect,
		SID:                  sb.ID,
		Profile:              string(types.ProfileBare),
		APISecretFingerprint: fingerprint,
		Cluster:              &routesync.ClusterSandboxContext{Group: "other", RouteKey: "route-a", AuthSandboxID: "stable"},
	})
	if ack.Status != routesync.AckRejected || !strings.Contains(ack.Reason, "context mismatch") {
		t.Fatalf("mismatched connect ack = %+v", ack)
	}
}
