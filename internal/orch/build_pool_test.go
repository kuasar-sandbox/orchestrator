package orch

import (
	"context"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestClaimWaitingBuildSurvivesRunIDPersistence(t *testing.T) {
	o := testOrch(t)
	ctx := context.Background()
	b := &types.Build{
		BuildID:     "build-admission-test",
		TemplateID:  "transient-build-admission-test",
		AuthKey:     strings.Repeat("b", 64),
		ManifestKey: strings.Repeat("a", 64),
		CPUCount:    2,
		MemoryMB:    2048,
		Profile:     types.ProfileE2B,
		Kind:        types.KindImg,
		Status:      types.BuildWaiting,
	}
	if err := o.st.PutBuild(ctx, b); err != nil {
		t.Fatal(err)
	}
	won, err := o.claimWaitingBuild(ctx, b)
	if err != nil || !won {
		t.Fatalf("claimWaitingBuild: won=%t err=%v", won, err)
	}
	if b.Status != types.BuildBuilding {
		t.Fatalf("in-memory status = %q, want %q", b.Status, types.BuildBuilding)
	}

	// runBuildUnit persists only the assigned run id. The claimed status must
	// remain building so a later pool scan cannot admit the build again.
	b.RunID = "br-00000000-0000-7000-8000-000000000001"
	if err := o.st.SetBuildRunID(ctx, b.BuildID, b.RunID); err != nil {
		t.Fatal(err)
	}
	got, err := o.st.GetBuild(ctx, b.BuildID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != types.BuildBuilding || got.RunID != b.RunID {
		t.Fatalf("persisted build = status %q run_id %q", got.Status, got.RunID)
	}
	waiting, err := o.st.BuildsByStatus(ctx, types.BuildWaiting)
	if err != nil {
		t.Fatal(err)
	}
	if len(waiting) != 0 {
		t.Fatalf("claimed build was requeued: %+v", waiting)
	}
}
