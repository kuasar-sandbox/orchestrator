package registry

import (
	"context"
	"strings"
	"testing"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestBuildTransientResolutionSurvivesResultAndRejectsStaleEvents(t *testing.T) {
	ctx := context.Background()
	r := testReg(t)
	r.SetPlacer(placementWithToken("n1"))
	r.SetNodeOwner(&recordingNodeOwner{runtime: map[string]*NodeRecord{"n1": {NodeID: "n1", APIEndpoint: "node-api:7443"}}})
	res, err := r.ReserveBuild(ctx, BuildReserveReq{Group: "/g", BuildID: "reused", Profile: types.ProfileE2B, Resources: testWireBuildResources()})
	if err != nil {
		t.Fatal(err)
	}
	canonical := types.TemplateID{Profile: types.ProfileE2B, Kind: types.KindImg, Ref: "manifest://" + strings.Repeat("a", 64)}.String()
	if err := r.applyBuildUpsert(ctx, "n1", &routesync.BuildEvent{BuildID: res.BuildID, TemplateID: res.TemplateID, PersistID: canonical, State: "ready"}); err != nil {
		t.Fatal(err)
	}
	projection, ok, err := r.stores.GetBuildInGroup(ctx, "/g", res.BuildID)
	if err != nil || !ok || projection.TemplateID != res.TemplateID || projection.PersistID != canonical {
		t.Fatalf("ready identity = %+v, %v", projection, err)
	}
	for i := 0; i < 2; i++ { // Resolver has no process-local transient directory/cache.
		resolved, ok, err := r.ResolveBuildByTemplate(ctx, "/g", res.TemplateID)
		if err != nil || !ok || resolved.BuildID != res.BuildID || resolved.APIEndpoint != "node-api:7443" {
			t.Fatalf("transient resolve = %+v, %v", resolved, err)
		}
	}
	if _, ok, err := r.ResolveBuildByTemplate(ctx, "/other", res.TemplateID); err != nil || ok {
		t.Fatalf("cross-group resolve = %v, %v", ok, err)
	}
	oldRef, ok, err := r.stores.GetNodeBuildRef(ctx, "n1", res.BuildID)
	if err != nil || !ok || oldRef.TemplateID != res.TemplateID {
		t.Fatalf("owner identity = %+v, %v", oldRef, err)
	}
	if err := r.applyBuildDelete(ctx, "n1", res.BuildID, res.TemplateID); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := r.ResolveBuildByTemplate(ctx, "/g", res.TemplateID); err != nil || ok {
		t.Fatalf("deleted resolve = %v, %v", ok, err)
	}
	if err := r.applyBuildUpsert(ctx, "n1", &routesync.BuildEvent{BuildID: res.BuildID, TemplateID: res.TemplateID, PersistID: canonical, State: "ready"}); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := r.stores.GetBuildInGroup(ctx, "/g", res.BuildID); err != nil || ok {
		t.Fatal("late upsert resurrected projection")
	}
	replacement, err := r.ReserveBuild(ctx, BuildReserveReq{Group: "/g", BuildID: res.BuildID, Profile: types.ProfileE2B, Resources: testWireBuildResources()})
	if err != nil {
		t.Fatal(err)
	}
	if replacement.TemplateID == res.TemplateID {
		t.Fatal("registration reused transient identity")
	}
	if err := r.applyBuildDelete(ctx, "n1", res.BuildID, res.TemplateID); err != nil {
		t.Fatal(err)
	}
	if err := r.applyNodeBuildFullSnapshot(ctx, "n1", []clusterstate.NodeBuildRef{oldRef}, map[string]struct{}{}); err != nil {
		t.Fatal(err)
	}
	if err := r.applyBuildUpsert(ctx, "n1", &routesync.BuildEvent{BuildID: res.BuildID, TemplateID: res.TemplateID, PersistID: canonical, State: "error"}); err != nil {
		t.Fatal(err)
	}
	current, ok, err := r.stores.GetBuildInGroup(ctx, "/g", res.BuildID)
	if err != nil || !ok || current.TemplateID != replacement.TemplateID || current.State != BuildRegistered || current.PersistID != "" {
		t.Fatalf("late lifecycle affected replacement = %+v, %v", current, err)
	}
	ref, ok, err := r.stores.GetNodeBuildRef(ctx, "n1", res.BuildID)
	if err != nil || !ok || ref.TemplateID != replacement.TemplateID {
		t.Fatalf("replacement owner lost = %+v, %v", ref, err)
	}
	// A lost live delete converges at the next complete snapshot.
	if err := r.applyNodeBuildFullSnapshot(ctx, "n1", []clusterstate.NodeBuildRef{ref}, map[string]struct{}{}); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := r.stores.GetBuildInGroup(ctx, "/g", res.BuildID); err != nil || ok {
		t.Fatal("full sync did not remove missing row")
	}
}

func TestBuildTransientResolutionIncompleteAndUnavailableAreErrors(t *testing.T) {
	r := testReg(t)
	ctx := context.Background()
	for _, rec := range []*BuildRecord{
		{Group: "/incomplete", BuildID: "old", NodeID: "n1", State: BuildReady, TemplateID: "e2b-img-old"},
		{Group: "/starting", BuildID: "starting", NodeID: "n1", State: BuildStarting, TemplateID: "transient-starting"},
		{Group: "/offline", BuildID: "offline", NodeID: "n1", State: BuildReady, TemplateID: "transient-offline", RegistrationTargetSet: true},
	} {
		if err := r.stores.PutBuild(ctx, rec); err != nil {
			t.Fatal(err)
		}
	}
	for _, tc := range []struct{ group, tid string }{{"/incomplete", "transient-unknown"}, {"/starting", "transient-starting"}, {"/offline", "transient-offline"}} {
		if res, ok, err := r.ResolveBuildByTemplate(ctx, tc.group, tc.tid); err == nil || ok || res != nil {
			t.Fatalf("%+v falsely resolved = %+v,%v,%v", tc, res, ok, err)
		}
	}
}
