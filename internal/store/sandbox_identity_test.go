package store

import (
	"context"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestSandboxIdentityRoundTrip(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	pair := testKeyPair("1", "2")

	cluster := &types.Sandbox{
		ID: "stable-g3", Profile: types.ProfileBare,
		Cluster:            &types.ClusterSandboxContext{Group: "/tenant", RouteKey: "worker"},
		AuthSandboxIDValue: "stable", TemplateID: "bare-img-" + strings.Repeat("a", 64),
		State: types.StateRunning, APISecret: pair.APISecret, ManifestKey: pair.ManifestKey,
		CreatedUnix: 1,
	}
	if err := st.Put(ctx, cluster); err != nil {
		t.Fatal(err)
	}
	got, err := st.Get(ctx, cluster.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Profile != types.ProfileBare || got.Cluster == nil ||
		got.Cluster.Group != "/tenant" || got.Cluster.RouteKey != "worker" ||
		got.AuthSandboxIDValue != "stable" || got.AuthSandboxID() != "stable" {
		t.Fatalf("cluster identity round trip = %+v", got)
	}

	standalone := &types.Sandbox{
		ID: "import-target", Profile: types.ProfileE2B, AuthSandboxIDValue: "source-subject",
		TemplateID: "e2b-snp-" + strings.Repeat("b", 64), State: types.StatePaused,
		APISecret: pair.APISecret, ManifestKey: pair.ManifestKey, CreatedUnix: 2,
	}
	if err := st.Put(ctx, standalone); err != nil {
		t.Fatal(err)
	}
	got, err = st.Get(ctx, standalone.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Cluster != nil || got.AuthSandboxIDValue != "source-subject" || got.AuthSandboxID() != "source-subject" {
		t.Fatalf("standalone imported identity round trip = %+v", got)
	}
}

func TestSandboxSystemIdentityIsInsertBound(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	pair := testKeyPair("3", "4")
	sb := &types.Sandbox{
		ID: "stable-g0", Profile: types.ProfileBare,
		Cluster:            &types.ClusterSandboxContext{Group: "/original", RouteKey: "route"},
		AuthSandboxIDValue: "stable", TemplateID: "bare-img-" + strings.Repeat("c", 64),
		State: types.StateRunning, APISecret: pair.APISecret, ManifestKey: pair.ManifestKey,
		CreatedUnix: 1,
	}
	if err := st.Put(ctx, sb); err != nil {
		t.Fatal(err)
	}

	sb.Profile = types.ProfileE2B
	sb.Cluster = &types.ClusterSandboxContext{Group: "/replacement", RouteKey: "other"}
	sb.AuthSandboxIDValue = "replacement"
	sb.State = types.StatePaused
	if err := st.Put(ctx, sb); err != nil {
		t.Fatal(err)
	}

	got, err := st.Get(ctx, sb.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Profile != types.ProfileBare || got.Cluster == nil ||
		got.Cluster.Group != "/original" || got.Cluster.RouteKey != "route" ||
		got.AuthSandboxIDValue != "stable" || got.State != types.StatePaused {
		t.Fatalf("lifecycle upsert rebound system identity: %+v", got)
	}
}

func TestSandboxIdentityValidation(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	pair := testKeyPair("5", "6")
	base := types.Sandbox{
		ID: "sandbox", Profile: types.ProfileBare,
		TemplateID: "bare-img-" + strings.Repeat("d", 64), State: types.StateRunning,
		APISecret: pair.APISecret, ManifestKey: pair.ManifestKey, CreatedUnix: 1,
	}

	for name, mutate := range map[string]func(*types.Sandbox){
		"empty profile":   func(sb *types.Sandbox) { sb.Profile = "" },
		"unknown profile": func(sb *types.Sandbox) { sb.Profile = "unknown" },
		"missing group": func(sb *types.Sandbox) {
			sb.Cluster = &types.ClusterSandboxContext{RouteKey: "route"}
		},
		"missing route key": func(sb *types.Sandbox) {
			sb.Cluster = &types.ClusterSandboxContext{Group: "/group"}
		},
	} {
		t.Run(name, func(t *testing.T) {
			sb := base
			mutate(&sb)
			if err := st.Put(ctx, &sb); err == nil {
				t.Fatal("invalid sandbox identity was accepted")
			}
		})
	}

	if err := st.Put(ctx, &base); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.ExecContext(ctx, `UPDATE sandboxes SET cluster_group='/group',cluster_route_key='' WHERE id=?`, base.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Get(ctx, base.ID); err == nil {
		t.Fatal("incomplete stored cluster context was accepted")
	}
	if _, err := st.db.ExecContext(ctx, `UPDATE sandboxes SET cluster_group='',profile='unknown' WHERE id=?`, base.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Get(ctx, base.ID); err == nil {
		t.Fatal("invalid stored profile was accepted")
	}
}
