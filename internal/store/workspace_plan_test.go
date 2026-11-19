package store

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/orchestrator/internal/workspace"
	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
)

func TestWorkspacePlanPrivateAndLegacyMetadata(t *testing.T) {
	ctx := context.Background()
	st := testStore(t)
	sb := sandboxInsertFixture("workspace-plan", 0)
	sb.WorkspacePlan = []workspace.Prepared{{Name: "project", Mounts: []workspace.Mount{{Path: "/data"}}, Disk: &rtconfig.DiskV1{Kind: "disk", Version: 1, DiffTemplate: "file:///private/seed"}, RefLocations: rtconfig.RefLocations{"wp": "/private/location"}}}
	sb.Metadata[workspace.Namespace] = workspace.Topology(sb.WorkspacePlan)
	if err := st.InsertSandbox(ctx, sb); err != nil {
		t.Fatal(err)
	}
	var meta, enc string
	if err := st.db.QueryRow("select metadata_json,workspace_plan_enc from sandboxes where id=?", sb.ID).Scan(&meta, &enc); err != nil {
		t.Fatal(err)
	}
	if enc == "" || strings.Contains(meta+enc, "/private") {
		t.Fatalf("plan not confidential")
	}
	got, err := st.Get(ctx, sb.ID)
	if err != nil || !reflect.DeepEqual(got.WorkspacePlan, sb.WorkspacePlan) || got.Metadata[workspace.Namespace] != workspace.Topology(sb.WorkspacePlan) {
		t.Fatalf("store round trip: %v %v", got, err)
	}
	other := sandboxInsertFixture("different-workspace-owner", 1)
	if err := st.InsertSandbox(ctx, other); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.Exec("update sandboxes set workspace_plan_enc=? where id=?", enc, other.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Get(ctx, other.ID); err == nil {
		t.Fatal("workspace plan moved between owners")
	}
	if _, err := st.db.Exec("update sandboxes set workspace_plan_enc='',metadata_json=? where id=?", mj(map[string]string{workspace.Namespace: `[{"name":"project","mounts":[{"path":"/data"}],"disk":{"migrationToken":"do-not-replay"}}]`}), sb.ID); err != nil {
		t.Fatal(err)
	}
	got, err = st.Get(ctx, sb.ID)
	if err != nil || len(got.WorkspacePlan) != 0 || got.Metadata[workspace.Namespace] != "" {
		t.Fatalf("legacy row: %+v %v", got, err)
	}
}

func TestBuildWorkspaceMetadataRequiresResolvedPlan(t *testing.T) {
	st := testStore(t)
	b := buildTriggerFixture("legacy-workspace-template", types.BuildRegistered)
	b.Metadata[workspace.Namespace] = `[{"name":"orphan","mounts":[{"path":"/stale"}],"disk":{"migrationToken":"legacy-secret"}}]`
	for _, withPlan := range []bool{false, true} {
		if withPlan {
			b.WorkspacePlan = []workspace.Prepared{{Name: "project", Mounts: []workspace.Mount{{Path: "/project"}}}}
		}
		if err := st.PutBuild(t.Context(), b); err != nil {
			t.Fatal(err)
		}
		read, err := st.GetBuild(t.Context(), b.BuildID)
		if err != nil {
			t.Fatal(err)
		}
		view, ok := read.Metadata[workspace.Namespace]
		if ok != withPlan || (withPlan && view != workspace.Topology(b.WorkspacePlan)) || read.Metadata["original"] != "metadata" {
			t.Fatalf("resolved plan %t: published build metadata %+v", withPlan, read.Metadata)
		}
	}
}

func TestWorkspacePendingDefaultPlanRoundTrip(t *testing.T) {
	st := testStore(t)
	sb := sandboxInsertFixture("pending-workspace", 0)
	sb.WorkspacePlan = []workspace.Prepared{{Name: "project", Mounts: []workspace.Mount{{Path: "/data"}}, DefaultFromSlot: true}}
	if err := st.InsertSandbox(t.Context(), sb); err != nil {
		t.Fatal(err)
	}
	read, err := st.Get(t.Context(), sb.ID)
	if err != nil || !reflect.DeepEqual(read.WorkspacePlan, sb.WorkspacePlan) {
		t.Fatalf("pending default lost during recovery: %+v %v", read, err)
	}
	if strings.Contains(read.Metadata[workspace.Namespace], "defaultFromSlot") || strings.Contains(read.Metadata[workspace.Namespace], "disk") {
		t.Fatalf("private pending choice escaped into metadata: %s", read.Metadata[workspace.Namespace])
	}
	selected, err := read.WorkspacePlan[0].ResolveDefault(16 << 20)
	if err != nil || selected.Disk == nil || selected.Disk.DiffTemplate != "mkfs://ext4?size=16777216" {
		t.Fatalf("recovered default cannot be resolved: %+v %v", selected, err)
	}
}
