package store

import (
	"context"
	"crypto/sha256"
	"errors"
	"strings"
	"testing"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/nodeexec"
	"github.com/kuasar-sandbox/orchestrator/internal/placement"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestCASExecutionBinding(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	oldOpaque := testOpaqueBinding(t, "generation-1", "s1", "n1", 7)
	newOpaque := testOpaqueBinding(t, "generation-2", "s1", "n1", 7)
	if err := st.Put(ctx, &types.Sandbox{
		ID: "s1", State: types.StatePaused,
		AuthKey: strings.Repeat("2", 64), ManifestKey: strings.Repeat("1", 64),
		Metadata: map[string]string{"user": "value", clusterstate.ObjectMetadataKey: oldOpaque},
	}); err != nil {
		t.Fatal(err)
	}
	oldDigest, err := clusterstate.ExecutionBindingDigest(oldOpaque)
	if err != nil {
		t.Fatal(err)
	}
	changed, err := st.CASExecutionBinding(ctx, clusterstate.ExecutionKindSandbox, "s1", oldDigest, newOpaque)
	if err != nil || !changed {
		t.Fatalf("CAS changed=%v err=%v", changed, err)
	}
	changed, err = st.CASExecutionBinding(ctx, clusterstate.ExecutionKindSandbox, "s1", oldDigest, newOpaque)
	if err != nil || !changed {
		t.Fatalf("idempotent CAS changed=%v err=%v", changed, err)
	}
	got, err := st.Get(ctx, "s1")
	if err != nil || got.Metadata[clusterstate.ObjectMetadataKey] != newOpaque || got.Metadata["user"] != "value" {
		t.Fatalf("sandbox after CAS = %+v err=%v", got, err)
	}

	third := testOpaqueBinding(t, "generation-3", "s1", "n1", 7)
	if changed, err := st.CASExecutionBinding(ctx, clusterstate.ExecutionKindSandbox, "s1", oldDigest, third); err != nil || changed {
		t.Fatalf("stale CAS changed=%v err=%v", changed, err)
	}
	wrongEpoch := testOpaqueBinding(t, "generation-3", "s1", "n1", 8)
	newDigest, _ := clusterstate.ExecutionBindingDigest(newOpaque)
	if _, err := st.CASExecutionBinding(ctx, clusterstate.ExecutionKindSandbox, "s1", newDigest, wrongEpoch); err == nil {
		t.Fatal("CAS changed NodeEpoch")
	}
	changedIntent := testOpaqueBinding(t, "generation-3", "s1", "n1", 7)
	decoded, err := clusterstate.DecodeExecutionBinding(changedIntent)
	if err != nil {
		t.Fatal(err)
	}
	decoded.DemandDigest = sha256.Sum256([]byte("different demand"))
	changedIntent, err = clusterstate.EncodeExecutionBinding(decoded)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.CASExecutionBinding(ctx, clusterstate.ExecutionKindSandbox, "s1", newDigest, changedIntent); err == nil {
		t.Fatal("CAS changed immutable workflow intent")
	}
}

func TestCASExecutionBindingAtomicallyRebindsWorkflowOutbox(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	dispatch := workflowDispatch(t, clusterstate.ExecutionKindSandbox, "sandbox-rebind", placement.BuildDemand{})
	decision := nodeexec.AdmissionDecision{
		State: nodeexec.AdmissionAdmitted, Result: clusterstate.DispatchAcceptedAdmitted,
		ReservationToken: "reservation-rebind",
	}
	if _, err := st.RecordSandboxWorkflow(ctx, dispatch, decision, workflowSandbox(dispatch.ObjectID)); err != nil {
		t.Fatal(err)
	}
	if _, err := st.ClaimSandboxWorkflow(ctx, dispatch.ObjectID, dispatch.DemandDigest, decision.ReservationToken); err != nil {
		t.Fatal(err)
	}
	if _, err := st.CommitSandboxEvent(ctx, workflowSandbox(dispatch.ObjectID), sandboxEvent(nodeexec.EventUpdate{
		State: string(clusterstate.WorkflowRouteReady),
	})); err != nil {
		t.Fatal(err)
	}
	select {
	case <-st.EventWake():
	default:
		t.Fatal("initial event did not wake replay")
	}
	before, err := st.GetNodeWorkflow(ctx, clusterstate.ExecutionKindSandbox, dispatch.ObjectID)
	if err != nil || before == nil || before.EventSeq == 0 {
		t.Fatalf("workflow before rebind = %+v, %v", before, err)
	}
	if err := st.AckExecutionEvent(ctx, "node-1", 7, routesync.EventAck{
		ObjectKind: "sandbox", ObjectID: dispatch.ObjectID, RegistryGeneration: before.LatestEvent.RegistryGeneration,
		BindingDigest: before.BindingDigest, EventSeq: before.EventSeq,
	}); err != nil {
		t.Fatal(err)
	}

	rebound, err := clusterstate.DecodeExecutionBinding(dispatch.OpaqueBinding)
	if err != nil {
		t.Fatal(err)
	}
	rebound.RegistryGeneration = "generation-2"
	replacement, err := clusterstate.EncodeExecutionBinding(rebound)
	if err != nil {
		t.Fatal(err)
	}
	replacementDigest, err := clusterstate.ExecutionBindingDigest(replacement)
	if err != nil {
		t.Fatal(err)
	}
	changed, err := st.CASExecutionBinding(ctx, clusterstate.ExecutionKindSandbox, dispatch.ObjectID, dispatch.BindingDigest, replacement)
	if err != nil || !changed {
		t.Fatalf("rebind changed=%v err=%v", changed, err)
	}
	select {
	case <-st.EventWake():
	default:
		t.Fatal("rebound pending event did not wake replay")
	}

	workflow, err := st.GetNodeWorkflow(ctx, clusterstate.ExecutionKindSandbox, dispatch.ObjectID)
	if err != nil {
		t.Fatal(err)
	}
	if workflow.OpaqueBinding != replacement || workflow.BindingDigest != replacementDigest ||
		workflow.LatestEvent == nil || workflow.LatestEvent.RegistryGeneration != "generation-2" ||
		workflow.LatestEvent.BindingDigest != replacementDigest || workflow.EventSeq != before.EventSeq+1 ||
		workflow.LatestEvent.EventSeq != workflow.EventSeq || workflow.AckedEventSeq != before.EventSeq {
		t.Fatalf("rebound workflow = %+v", workflow)
	}
	// A delayed ACK for the pre-rebind payload cannot acknowledge the rebound fact.
	if err := st.AckExecutionEvent(ctx, "node-1", 7, routesync.EventAck{
		ObjectKind: "sandbox", ObjectID: dispatch.ObjectID, RegistryGeneration: before.LatestEvent.RegistryGeneration,
		BindingDigest: before.BindingDigest, EventSeq: before.EventSeq,
	}); !errors.Is(err, ErrNodeWorkflowConflict) {
		t.Fatalf("stale Binding ACK error = %v", err)
	}
	pending, _, err := st.PendingExecutionEvents(ctx, "node-1", 7, routesync.EventCursor{}, 10, 1<<20)
	if err != nil || len(pending) != 1 || pending[0].ObjectID != dispatch.ObjectID ||
		pending[0].RegistryGeneration != "generation-2" || pending[0].BindingDigest != replacementDigest {
		t.Fatalf("rebound pending events = %+v, %v", pending, err)
	}
	stored, err := st.Get(ctx, dispatch.ObjectID)
	if err != nil || stored.Metadata[clusterstate.ObjectMetadataKey] != replacement {
		t.Fatalf("rebound Sandbox = %+v, %v", stored, err)
	}
}

func testOpaqueBinding(t *testing.T, generation, objectID, nodeID string, nodeEpoch uint64) string {
	t.Helper()
	opaque, err := clusterstate.EncodeExecutionBinding(clusterstate.ExecutionBinding{
		RegistryGeneration: generation, Kind: clusterstate.ExecutionKindSandbox,
		ObjectID: objectID, Group: "/g", RouteKey: "rk", NodeID: nodeID, NodeEpoch: nodeEpoch,
		DemandDigest: sha256.Sum256([]byte("demand")), DispatchSpecDigest: sha256.Sum256([]byte("dispatch")),
	})
	if err != nil {
		t.Fatal(err)
	}
	return opaque
}
