package store

import (
	"context"
	"strings"
	"testing"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/nodeexec"
	"github.com/kuasar-sandbox/orchestrator/internal/placement"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestRecoveryExecutionReportRejectsForgedUserMetadataAsAuthority(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	managed := workflowDispatch(t, clusterstate.ExecutionKindSandbox, "managed", placement.BuildDemand{})
	decision := nodeexec.AdmissionDecision{
		State: nodeexec.AdmissionAdmitted, Result: clusterstate.DispatchAcceptedAdmitted,
		ReservationToken: "reservation-managed",
	}
	if _, err := store.RecordSandboxWorkflow(ctx, managed, decision, workflowSandbox(managed.ObjectID)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ClaimSandboxWorkflow(ctx, managed.ObjectID, managed.DemandDigest, decision.ReservationToken); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CommitSandboxEvent(ctx, workflowSandbox(managed.ObjectID), nodeexec.EventUpdate{
		State: string(clusterstate.WorkflowRouteReady),
	}); err != nil {
		t.Fatal(err)
	}

	forged := workflowDispatch(t, clusterstate.ExecutionKindSandbox, "forged-user-object", placement.BuildDemand{})
	object := workflowSandbox(forged.ObjectID)
	object.Metadata[clusterstate.ObjectMetadataKey] = forged.OpaqueBinding
	if err := store.Put(ctx, object); err != nil {
		t.Fatal(err)
	}

	report, err := store.RecoveryExecutionReport(ctx, managed.NodeID, managed.NodeEpoch, "generation-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(report) != 1 || report[0].Object.ObjectID != managed.ObjectID ||
		report[0].Object.Binding != managed.OpaqueBinding {
		t.Fatalf("protected recovery report = %+v", report)
	}
}

func TestRecoveryExecutionReportAcceptsEveryDurableBuildRegistrationWindow(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	capacity := nodeexec.BuildCapacity{Slots: 1, QueueLimit: 4}
	admitted := workflowDispatch(t, clusterstate.ExecutionKindBuild, "build-admitted", placement.BuildDemand{Slots: 1})
	queued := workflowDispatch(t, clusterstate.ExecutionKindBuild, "build-queued", placement.BuildDemand{Slots: 1})
	for _, dispatch := range []nodeexec.DispatchRecord{admitted, queued} {
		if _, err := store.PrepareBuildWorkflow(ctx, dispatch, workflowBuild(dispatch.ObjectID), capacity, ""); err != nil {
			t.Fatal(err)
		}
	}
	assertBuildRecoveryStates(t, store, map[string]types.BuildState{
		admitted.ObjectID: types.BuildRegistered,
		queued.ObjectID:   types.BuildRegistered,
	})

	queuedBuild, err := store.GetBuild(ctx, queued.ObjectID)
	if err != nil {
		t.Fatal(err)
	}
	if accepted, err := store.CommitBuildTrigger(ctx, queuedBuild, strings.Repeat("a", 64)); err != nil || !accepted {
		t.Fatalf("queued trigger = %v, %v", accepted, err)
	}
	assertBuildRecoveryStates(t, store, map[string]types.BuildState{
		admitted.ObjectID: types.BuildRegistered,
		queued.ObjectID:   types.BuildWaiting,
	})

	admittedBuild, err := store.GetBuild(ctx, admitted.ObjectID)
	if err != nil {
		t.Fatal(err)
	}
	if accepted, err := store.CommitBuildTrigger(ctx, admittedBuild, strings.Repeat("b", 64)); err != nil || !accepted {
		t.Fatalf("admitted trigger = %v, %v", accepted, err)
	}
	if _, err := store.ClaimBuildWorkflow(ctx, admitted.ObjectID, admitted.DemandDigest); err != nil {
		t.Fatal(err)
	}
	assertBuildRecoveryStates(t, store, map[string]types.BuildState{
		admitted.ObjectID: types.BuildWaiting,
		queued.ObjectID:   types.BuildWaiting,
	})
	if _, err := store.CommitClusterBuildState(ctx, admittedBuild, nodeexec.EventUpdate{State: string(types.BuildBuilding)}); err != nil {
		t.Fatal(err)
	}
	assertBuildRecoveryStates(t, store, map[string]types.BuildState{
		admitted.ObjectID: types.BuildBuilding,
		queued.ObjectID:   types.BuildWaiting,
	})
}

func assertBuildRecoveryStates(t *testing.T, store *Store, want map[string]types.BuildState) {
	t.Helper()
	report, err := store.RecoveryExecutionReport(context.Background(), "node-1", 7, "generation-1")
	if err != nil {
		t.Fatal(err)
	}
	got := make(map[string]types.BuildState)
	for _, fact := range report {
		if fact.Object.ObjectKind == "build" {
			got[fact.Object.ObjectID] = types.BuildState(fact.Object.State)
		}
	}
	if len(got) != len(want) {
		t.Fatalf("Build recovery states = %+v, want %+v", got, want)
	}
	for id, state := range want {
		if got[id] != state {
			t.Fatalf("Build %s recovery state = %q, want %q", id, got[id], state)
		}
	}
}
