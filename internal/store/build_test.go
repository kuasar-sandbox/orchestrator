package store

import (
	"context"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestSetBuildRunIDPreservesBuildingStatus(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	b := &types.Build{
		BuildID: "build-1", TemplateID: "transient-1",
		AuthKey:     strings.Repeat("2", 64),
		ManifestKey: strings.Repeat("1", 64),
		CPUCount:    2, MemoryMB: 2048,
		Profile: types.ProfileE2B, Kind: types.KindImg,
		Status: types.BuildWaiting, CreatedUnix: 1,
	}
	if err := st.PutBuild(ctx, b); err != nil {
		t.Fatal(err)
	}
	won, err := st.CASBuildStatus(ctx, b.BuildID, types.BuildWaiting, types.BuildBuilding)
	if err != nil || !won {
		t.Fatalf("CASBuildStatus: won=%v err=%v", won, err)
	}
	if err := st.SetBuildRunID(ctx, b.BuildID, "br-test"); err != nil {
		t.Fatal(err)
	}
	got, err := st.GetBuild(ctx, b.BuildID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != types.BuildBuilding || got.RunID != "br-test" {
		t.Fatalf("build after assignment = status %q run_id %q", got.Status, got.RunID)
	}
}
