package store

import (
	"context"
	"reflect"
	"strings"
	"testing"

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
	if err != nil || !reflect.DeepEqual(got.WorkspacePlan, sb.WorkspacePlan) {
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
	if err != nil || len(got.WorkspacePlan) != 0 || got.Metadata[workspace.Namespace] != sb.Metadata[workspace.Namespace] {
		t.Fatalf("legacy row: %+v %v", got, err)
	}
}
