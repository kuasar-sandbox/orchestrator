package store

import (
	"context"
	"encoding/hex"
	"errors"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/nodeexec"
	"github.com/kuasar-sandbox/orchestrator/internal/placement"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/secretbox"
	"github.com/kuasar-sandbox/orchestrator/internal/session"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func workflowDispatch(
	t *testing.T,
	kind clusterstate.ExecutionKind,
	objectID string,
	buildDemand placement.BuildDemand,
) nodeexec.DispatchRecord {
	return workflowDispatchSpec(t, kind, objectID, buildDemand, objectID)
}

func workflowDispatchSpec(
	t *testing.T,
	kind clusterstate.ExecutionKind,
	objectID string,
	buildDemand placement.BuildDemand,
	specValue string,
) nodeexec.DispatchRecord {
	t.Helper()
	var (
		demand   []byte
		routeKey string
		err      error
	)
	if kind == clusterstate.ExecutionKindBuild {
		demand, err = placement.NormalizeBuildDemand(buildDemand)
	} else {
		demand, err = placement.NormalizeSandboxDemand(placement.SandboxDemand{
			SlotUnits: 1, StartupBudgetMemory: 1 << 30, FloorMemory: 512 << 20,
		})
		routeKey = "route-" + objectID
	}
	if err != nil {
		t.Fatal(err)
	}
	spec := []byte(`{"object":"` + specValue + `"}`)
	intent, err := clusterstate.NewDispatchIntent(demand, spec, "provider-v1/policy-v1")
	if err != nil {
		t.Fatal(err)
	}
	demandDigest, _ := hex.DecodeString(intent.DemandDigest)
	specDigest, _ := hex.DecodeString(intent.DispatchSpecDigest)
	binding := clusterstate.ExecutionBinding{
		RegistryGeneration: "generation-1", Kind: kind, ObjectID: objectID,
		Group: "group-1", RouteKey: routeKey, NodeID: "node-1", NodeEpoch: 7,
	}
	copy(binding.DemandDigest[:], demandDigest)
	copy(binding.DispatchSpecDigest[:], specDigest)
	opaque, err := clusterstate.EncodeExecutionBinding(binding)
	if err != nil {
		t.Fatal(err)
	}
	bindingDigest, err := clusterstate.ExecutionBindingDigest(opaque)
	if err != nil {
		t.Fatal(err)
	}
	record, err := nodeexec.DispatchRecordFromCommand(session.DispatchCommand{
		Kind: kind, Group: "group-1", RouteKey: routeKey, ObjectID: objectID,
		NodeID: "node-1", NodeEpoch: 7, SessionSeq: 11, DataEndpoint: "10.0.0.1:8443",
		Intent: intent,
		Binding: clusterstate.ExecutionBindingIntent{
			NodeID: "node-1", NodeEpoch: 7, DataEndpoint: "10.0.0.1:8443", RegistryGeneration: "generation-1",
			OpaqueBinding: opaque, BindingDigest: bindingDigest,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return record
}

func workflowBuild(id string) *types.Build {
	return &types.Build{
		BuildID: id, TemplateID: "transient-" + id,
		AuthKey: strings.Repeat("4", 64), ManifestKey: strings.Repeat("1", 64), CPUCount: 2, MemoryMB: 2048,
		Profile: types.ProfileE2B, Kind: types.KindImg, CreatedUnix: time.Now().Unix(),
		Metadata: map[string]string{"user": "kept", clusterstate.ObjectMetadataKey: "forged"},
	}
}

func workflowBuildDemand(slots uint64) placement.BuildDemand {
	return placement.BuildDemand{Slots: slots, CPU: slots, Memory: slots}
}

func workflowBuildCapacity(slots uint64, queueLimit int) nodeexec.BuildCapacity {
	return nodeexec.BuildCapacity{Slots: slots, CPU: slots, Memory: slots, QueueLimit: queueLimit}
}

func workflowDispatchEpoch(
	t *testing.T,
	kind clusterstate.ExecutionKind,
	objectID string,
	buildDemand placement.BuildDemand,
	nodeEpoch uint64,
) nodeexec.DispatchRecord {
	t.Helper()
	dispatch := workflowDispatch(t, kind, objectID, buildDemand)
	binding, err := clusterstate.DecodeExecutionBinding(dispatch.OpaqueBinding)
	if err != nil {
		t.Fatal(err)
	}
	binding.NodeEpoch = nodeEpoch
	dispatch.NodeEpoch = nodeEpoch
	dispatch.OpaqueBinding, err = clusterstate.EncodeExecutionBinding(binding)
	if err != nil {
		t.Fatal(err)
	}
	dispatch.BindingDigest, err = clusterstate.ExecutionBindingDigest(dispatch.OpaqueBinding)
	if err != nil {
		t.Fatal(err)
	}
	if err := dispatch.Validate(); err != nil {
		t.Fatal(err)
	}
	return dispatch
}

func workflowSandbox(id string) *types.Sandbox {
	return &types.Sandbox{
		ID: id, TemplateID: "e2b-img-" + strings.Repeat("2", 64),
		AuthKey: strings.Repeat("4", 64), ManifestKey: strings.Repeat("3", 64),
		EnvdAccessToken: "access", TrafficAccessToken: "traffic", CreatedUnix: time.Now().Unix(),
		Metadata: map[string]string{"user": "kept", clusterstate.ObjectMetadataKey: "forged"},
	}
}

func sandboxEvent(update nodeexec.EventUpdate) nodeexec.EventUpdate {
	presentation := clusterstate.SandboxPresentationV1{
		CPUCount: 2, MemoryMB: 2048, DiskSizeMB: 64, EnvdVersion: "0.6.1", StartedAt: 1, EndAt: 2,
	}
	update.Presentation = &presentation
	return update
}

func TestBuildAdmissionIsIdempotentConflictSafeAndBounded(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	capacity := workflowBuildCapacity(1, 1)
	first := workflowDispatch(t, clusterstate.ExecutionKindBuild, "build-1", workflowBuildDemand(1))
	admitted, err := st.PrepareBuildWorkflow(ctx, first, workflowBuild(first.ObjectID), capacity, "")
	if err != nil || admitted.Result != clusterstate.DispatchAcceptedAdmitted || !admitted.ResourceClaimed {
		t.Fatalf("admitted = %+v, %v", admitted, err)
	}
	storedFirst, err := st.GetBuild(ctx, first.ObjectID)
	if err != nil || storedFirst.Status != types.BuildRegistered ||
		storedFirst.Metadata[clusterstate.ObjectMetadataKey] != first.OpaqueBinding {
		t.Fatalf("atomic admitted Build = %+v, %v", storedFirst, err)
	}
	retry, err := st.PrepareBuildWorkflow(ctx, first, workflowBuild(first.ObjectID), capacity, "")
	if err != nil || retry.Result != admitted.Result || retry.AdmissionState != admitted.AdmissionState {
		t.Fatalf("retry = %+v, %v", retry, err)
	}
	conflict := workflowDispatchSpec(t, clusterstate.ExecutionKindBuild, "build-1", workflowBuildDemand(1), "different")
	if _, err := st.PrepareBuildWorkflow(ctx, conflict, workflowBuild(conflict.ObjectID), capacity, ""); !errors.Is(err, ErrNodeWorkflowConflict) {
		t.Fatalf("conflicting retry error = %v", err)
	}

	second := workflowDispatch(t, clusterstate.ExecutionKindBuild, "build-2", workflowBuildDemand(1))
	queued, err := st.PrepareBuildWorkflow(ctx, second, workflowBuild(second.ObjectID), capacity, "")
	if err != nil || queued.Result != clusterstate.DispatchAcceptedQueued || queued.EventSeq != 0 ||
		queued.LatestEvent != nil || queued.ObjectState != string(types.BuildRegistered) {
		t.Fatalf("queued = %+v, %v", queued, err)
	}
	storedSecond, err := st.GetBuild(ctx, second.ObjectID)
	if err != nil || storedSecond.Status != types.BuildRegistered ||
		storedSecond.Metadata[clusterstate.ObjectMetadataKey] != second.OpaqueBinding {
		t.Fatalf("atomic queued Build = %+v, %v", storedSecond, err)
	}
	third := workflowDispatch(t, clusterstate.ExecutionKindBuild, "build-3", workflowBuildDemand(1))
	rejected, err := st.PrepareBuildWorkflow(ctx, third, workflowBuild(third.ObjectID), capacity, "")
	if err != nil || rejected.Result != clusterstate.DispatchDefinitiveReject || rejected.Reason != "build_queue_full" {
		t.Fatalf("rejected = %+v, %v", rejected, err)
	}
	all, claimed, err := st.BuildAdmissionUsage(ctx, "node-1", 7)
	if err != nil || all.Slots != 2 || claimed.Slots != 1 {
		t.Fatalf("usage all=%+v claimed=%+v err=%v", all, claimed, err)
	}
	if err := st.FinalizeNodeWorkflow(ctx, clusterstate.ExecutionKindBuild, third.ObjectID, third.BindingDigest); err != nil {
		t.Fatal(err)
	}
	if got, err := st.GetNodeWorkflow(ctx, clusterstate.ExecutionKindBuild, third.ObjectID); err != nil ||
		got == nil || !got.WorkflowFinalized {
		t.Fatalf("finalized rejection marker = %+v, %v", got, err)
	}
	replayed, err := st.PrepareBuildWorkflow(ctx, third, workflowBuild(third.ObjectID), capacity, "")
	if err != nil || replayed.Result != clusterstate.DispatchDefinitiveReject {
		t.Fatalf("same-session finalized replay = %+v, %v", replayed, err)
	}
	if err := st.CompactFinalizedNodeWorkflows(ctx, third.NodeID, third.NodeEpoch, third.SessionSeq+1); err != nil {
		t.Fatal(err)
	}
	if got, err := st.GetNodeWorkflow(ctx, clusterstate.ExecutionKindBuild, third.ObjectID); err != nil || got != nil {
		t.Fatalf("new-session compacted rejection = %+v, %v", got, err)
	}
}

func TestConcurrentBuildAdmissionDoesNotOversubscribe(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	capacity := workflowBuildCapacity(1, 4)
	dispatches := []nodeexec.DispatchRecord{
		workflowDispatch(t, clusterstate.ExecutionKindBuild, "build-a", workflowBuildDemand(1)),
		workflowDispatch(t, clusterstate.ExecutionKindBuild, "build-b", workflowBuildDemand(1)),
	}
	start := make(chan struct{})
	results := make(chan clusterstate.DispatchOutcome, len(dispatches))
	errs := make(chan error, len(dispatches))
	var wg sync.WaitGroup
	for _, dispatch := range dispatches {
		dispatch := dispatch
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			record, err := st.PrepareBuildWorkflow(ctx, dispatch, workflowBuild(dispatch.ObjectID), capacity, "")
			if err != nil {
				errs <- err
				return
			}
			results <- record.Result
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	counts := map[clusterstate.DispatchOutcome]int{}
	for result := range results {
		counts[result]++
	}
	if counts[clusterstate.DispatchAcceptedAdmitted] != 1 || counts[clusterstate.DispatchAcceptedQueued] != 1 {
		t.Fatalf("outcomes = %+v", counts)
	}
	_, claimed, err := st.BuildAdmissionUsage(ctx, "node-1", 7)
	if err != nil || claimed.Slots != 1 {
		t.Fatalf("claimed = %+v, %v", claimed, err)
	}
}

func TestRegisteredBuildIsLaunchableAfterRestart(t *testing.T) {
	st := testStore(t)
	dispatch := workflowDispatch(t, clusterstate.ExecutionKindBuild, "build-registered", workflowBuildDemand(1))
	if _, err := st.PrepareBuildWorkflow(
		context.Background(), dispatch, workflowBuild(dispatch.ObjectID), workflowBuildCapacity(1, 4), "",
	); err != nil {
		t.Fatal(err)
	}
	launchable, err := st.LaunchableNodeWorkflows(
		context.Background(), clusterstate.ExecutionKindBuild, dispatch.NodeID, dispatch.NodeEpoch, "", 10,
	)
	if err != nil || len(launchable) != 1 || launchable[0].ObjectID != dispatch.ObjectID ||
		launchable[0].ObjectState != string(types.BuildRegistered) {
		t.Fatalf("launchable registered Build = %+v, %v", launchable, err)
	}
}

func TestCompactionRemovesFinalizedPriorNodeEpoch(t *testing.T) {
	st := testStore(t)
	dispatch := workflowDispatchEpoch(t, clusterstate.ExecutionKindSandbox, "sandbox-old-epoch", placement.BuildDemand{}, 6)
	record, err := st.RecordSandboxWorkflow(context.Background(), dispatch, nodeexec.AdmissionDecision{
		State: nodeexec.AdmissionRejected, Result: clusterstate.DispatchDefinitiveReject, Reason: "rejected",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.FinalizeNodeWorkflow(context.Background(), record.Kind, record.ObjectID, record.BindingDigest); err != nil {
		t.Fatal(err)
	}
	if err := st.CompactFinalizedNodeWorkflows(context.Background(), record.NodeID, 7, 1); err != nil {
		t.Fatal(err)
	}
	if got, err := st.GetNodeWorkflow(context.Background(), record.Kind, record.ObjectID); err != nil || got != nil {
		t.Fatalf("prior-epoch workflow after compaction = %+v, %v", got, err)
	}
}

func TestBuildAdmissionRollsBackJournalWhenBusinessObjectCannotPersist(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	dispatch := workflowDispatch(t, clusterstate.ExecutionKindBuild, "build-invalid", workflowBuildDemand(1))
	build := workflowBuild(dispatch.ObjectID)
	build.ManifestKey = "not-hex"
	if _, err := st.PrepareBuildWorkflow(ctx, dispatch, build, workflowBuildCapacity(1, 4), ""); err == nil {
		t.Fatal("Build Admission succeeded without a persistable business object")
	}
	workflow, err := st.GetNodeWorkflow(ctx, clusterstate.ExecutionKindBuild, dispatch.ObjectID)
	if err != nil || workflow != nil {
		t.Fatalf("failed Build left workflow = %+v, %v", workflow, err)
	}
	stored, err := st.GetBuild(ctx, dispatch.ObjectID)
	if err != nil || stored != nil {
		t.Fatalf("failed Build left business object = %+v, %v", stored, err)
	}
}

func TestSandboxAdmissionRollsBackJournalWhenKeyCopyCannotPersist(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	dispatch := workflowDispatch(t, clusterstate.ExecutionKindSandbox, "sandbox-invalid-key", placement.BuildDemand{})
	sandbox := workflowSandbox(dispatch.ObjectID)
	sandbox.ManifestKey = "not-hex"
	decision := nodeexec.AdmissionDecision{
		State: nodeexec.AdmissionAdmitted, Result: clusterstate.DispatchAcceptedAdmitted,
		ReservationToken: "reservation-invalid-key",
	}
	if _, err := st.RecordSandboxWorkflow(ctx, dispatch, decision, sandbox); err == nil {
		t.Fatal("Sandbox Admission succeeded without a persistable key copy")
	}
	workflow, err := st.GetNodeWorkflow(ctx, clusterstate.ExecutionKindSandbox, dispatch.ObjectID)
	if err != nil || workflow != nil {
		t.Fatalf("failed Sandbox key copy left workflow = %+v, %v", workflow, err)
	}
	stored, err := st.Get(ctx, dispatch.ObjectID)
	if err != nil || stored != nil {
		t.Fatalf("failed Sandbox key copy left business object = %+v, %v", stored, err)
	}
}

func TestBuildAdmissionDoesNotOverwriteExistingLocalBuild(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	dispatch := workflowDispatch(t, clusterstate.ExecutionKindBuild, "build-existing", workflowBuildDemand(1))
	existing := workflowBuild(dispatch.ObjectID)
	existing.TemplateID = "local-template"
	if err := st.PutBuild(ctx, existing); err != nil {
		t.Fatal(err)
	}
	if _, err := st.PrepareBuildWorkflow(ctx, dispatch, workflowBuild(dispatch.ObjectID),
		workflowBuildCapacity(1, 4), ""); !errors.Is(err, ErrNodeWorkflowConflict) {
		t.Fatalf("occupied Build error = %v", err)
	}
	stored, err := st.GetBuild(ctx, dispatch.ObjectID)
	if err != nil || stored.TemplateID != "local-template" {
		t.Fatalf("occupied Build was overwritten = %+v, %v", stored, err)
	}
	workflow, err := st.GetNodeWorkflow(ctx, clusterstate.ExecutionKindBuild, dispatch.ObjectID)
	if err != nil || workflow != nil {
		t.Fatalf("occupied Build left journal = %+v, %v", workflow, err)
	}
}

func TestBuildQueueTerminalizesImpossibleHeadAfterCapacityShrink(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	initial := workflowBuildCapacity(2, 4)
	running := workflowDispatch(t, clusterstate.ExecutionKindBuild, "build-running", workflowBuildDemand(2))
	impossible := workflowDispatch(t, clusterstate.ExecutionKindBuild, "build-impossible", workflowBuildDemand(2))
	following := workflowDispatch(t, clusterstate.ExecutionKindBuild, "build-following", workflowBuildDemand(1))
	for _, dispatch := range []nodeexec.DispatchRecord{running, impossible, following} {
		if _, err := st.PrepareBuildWorkflow(ctx, dispatch, workflowBuild(dispatch.ObjectID), initial, ""); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := st.ClaimBuildWorkflow(ctx, running.ObjectID, running.DemandDigest); err != nil {
		t.Fatal(err)
	}
	if _, err := st.CommitClusterBuildState(ctx, workflowBuild(running.ObjectID), nodeexec.EventUpdate{
		State: string(types.BuildError), Reason: "running_build_finished",
	}); err != nil {
		t.Fatal(err)
	}

	promoted, err := st.PromoteBuildQueue(ctx, "node-1", 7, workflowBuildCapacity(1, 4), 4)
	if err != nil || len(promoted) != 1 || promoted[0].ObjectID != following.ObjectID {
		t.Fatalf("promotion after capacity shrink = %+v, %v", promoted, err)
	}
	select {
	case <-st.WorkflowWake():
	default:
		t.Fatal("terminalized queue head did not schedule immediate reconciliation")
	}
	record, err := st.GetNodeWorkflow(ctx, clusterstate.ExecutionKindBuild, impossible.ObjectID)
	if err != nil || record == nil || record.AdmissionState != nodeexec.AdmissionTerminal ||
		record.ObjectState != string(types.BuildError) || record.EventSeq != 0 || record.LatestEvent != nil {
		t.Fatalf("impossible queue head = %+v, %v", record, err)
	}
	build, err := st.GetBuild(ctx, impossible.ObjectID)
	if err != nil || build == nil || build.Status != types.BuildError || build.Reason != "exceeds_build_capacity" {
		t.Fatalf("impossible Build = %+v, %v", build, err)
	}
}

func TestBuildLifecycleIsNodeLocalAndTerminalStateReleasesCapacity(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	capacity := workflowBuildCapacity(1, 4)
	first := workflowDispatch(t, clusterstate.ExecutionKindBuild, "build-1", workflowBuildDemand(1))
	second := workflowDispatch(t, clusterstate.ExecutionKindBuild, "build-2", workflowBuildDemand(1))
	if _, err := st.PrepareBuildWorkflow(ctx, first, workflowBuild(first.ObjectID), capacity, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := st.PrepareBuildWorkflow(ctx, second, workflowBuild(second.ObjectID), capacity, ""); err != nil {
		t.Fatal(err)
	}
	registered, err := st.GetBuild(ctx, first.ObjectID)
	if err != nil {
		t.Fatal(err)
	}
	triggerDigest := strings.Repeat("a", 64)
	if accepted, err := st.CommitBuildTrigger(ctx, registered, triggerDigest); err != nil || !accepted {
		t.Fatalf("first trigger = %v, %v", accepted, err)
	}
	if accepted, err := st.CommitBuildTrigger(ctx, registered, triggerDigest); err != nil || accepted {
		t.Fatalf("trigger retry = %v, %v", accepted, err)
	}
	if _, err := st.CommitBuildTrigger(ctx, registered, strings.Repeat("b", 64)); !errors.Is(err, ErrBuildTriggerConflict) {
		t.Fatalf("conflicting trigger error = %v", err)
	}
	if _, err := st.ClaimBuildWorkflow(ctx, first.ObjectID, first.DemandDigest); err != nil {
		t.Fatal(err)
	}
	build := workflowBuild(first.ObjectID)
	build.TemplateID = "stale-callback-must-not-rewrite-identity"
	build.Metadata["user"] = "stale"
	build.RunID = "runner-1"
	if _, err := st.CommitClusterBuildState(ctx, build, nodeexec.EventUpdate{State: string(types.BuildBuilding)}); err != nil {
		t.Fatal(err)
	}
	build.PersistID = "e2b-img-artifact"
	build.Kind = types.KindSnp
	build.StartCmd = "start-v1"
	build.ReadyCmd = "ready-v1"
	if _, err := st.CommitClusterBuildState(ctx, build, nodeexec.EventUpdate{
		State: string(types.BuildReady), ArtifactRef: build.PersistID,
	}); err != nil {
		t.Fatal(err)
	}
	conflict := cloneBuild(build)
	conflict.StartCmd = "start-v2"
	if _, err := st.CommitClusterBuildState(ctx, conflict, nodeexec.EventUpdate{
		State: string(types.BuildReady), ArtifactRef: build.PersistID,
	}); !errors.Is(err, ErrNodeWorkflowConflict) {
		t.Fatalf("duplicate BUILD_READY runtime rewrite error = %v", err)
	}
	stored, err := st.GetBuild(ctx, first.ObjectID)
	if err != nil || stored.Status != types.BuildReady || stored.PersistID != "e2b-img-artifact" ||
		stored.Kind != types.KindSnp || stored.StartCmd != "start-v1" || stored.ReadyCmd != "ready-v1" ||
		stored.TemplateID != "transient-"+first.ObjectID ||
		stored.Metadata[clusterstate.ObjectMetadataKey] != first.OpaqueBinding || stored.Metadata["user"] != "kept" {
		t.Fatalf("stored Build = %+v, %v", stored, err)
	}
	all, claimed, err := st.BuildAdmissionUsage(ctx, "node-1", 7)
	if err != nil || all.Slots != 1 || claimed.Slots != 0 {
		t.Fatalf("terminal usage all=%+v claimed=%+v err=%v", all, claimed, err)
	}
	promoted, err := st.PromoteBuildQueue(ctx, "node-1", 7, capacity, 4)
	if err != nil || len(promoted) != 1 || promoted[0].ObjectID != second.ObjectID ||
		promoted[0].Result != clusterstate.DispatchAcceptedQueued || !promoted[0].ResourceClaimed {
		t.Fatalf("promoted = %+v, %v", promoted, err)
	}
	pending, _, err := st.PendingExecutionEvents(ctx, "node-1", 7, routesync.EventCursor{}, 10, 1<<20)
	if err != nil || len(pending) != 0 {
		t.Fatalf("node-local Builds leaked into execution outbox = %+v, %v", pending, err)
	}
	if err := st.FinalizeNodeWorkflow(ctx, clusterstate.ExecutionKindBuild, first.ObjectID, first.BindingDigest); !errors.Is(err, ErrNodeWorkflowState) {
		t.Fatalf("Registry finalized accepted Build lifecycle: %v", err)
	}
	if got, err := st.GetNodeWorkflow(ctx, clusterstate.ExecutionKindBuild, first.ObjectID); err != nil ||
		got == nil || got.WorkflowFinalized || got.EventSeq != 0 || got.LatestEvent != nil {
		t.Fatalf("accepted Build lifecycle marker = %+v, %v", got, err)
	}
}

func TestSandboxJournalPreservesTokenBindingAndMonotonicOutbox(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	dispatch := workflowDispatch(t, clusterstate.ExecutionKindSandbox, "sandbox-1", placement.BuildDemand{})
	decision := nodeexec.AdmissionDecision{
		State: nodeexec.AdmissionAdmitted, Result: clusterstate.DispatchAcceptedAdmitted,
		ReservationToken: "reservation-1",
	}
	record, err := st.RecordSandboxWorkflow(ctx, dispatch, decision, workflowSandbox(dispatch.ObjectID))
	if err != nil || record.ReservationToken != "reservation-1" {
		t.Fatalf("record = %+v, %v", record, err)
	}
	if _, err := st.ClaimSandboxWorkflow(ctx, dispatch.ObjectID, dispatch.DemandDigest, "reservation-1"); err != nil {
		t.Fatal(err)
	}
	sandbox := workflowSandbox(dispatch.ObjectID)
	ready := sandboxEvent(nodeexec.EventUpdate{State: string(clusterstate.WorkflowRouteReady)})
	first, err := st.CommitSandboxEvent(ctx, sandbox, ready)
	if err != nil || first.EventSeq != 1 || first.LatestEvent.DataEndpoint != dispatch.DataEndpoint {
		t.Fatalf("ready = %+v, %v", first, err)
	}
	retry, err := st.CommitSandboxEvent(ctx, sandbox, ready)
	if err != nil || retry.EventSeq != 1 {
		t.Fatalf("ready retry = %+v, %v", retry, err)
	}
	paused, err := st.CommitSandboxEvent(ctx, sandbox, sandboxEvent(nodeexec.EventUpdate{
		State: string(clusterstate.WorkflowRoutePaused), SnapshotRef: "snapshot-new",
	}))
	if err != nil {
		t.Fatal(err)
	}
	pausedSandbox, err := st.Get(ctx, sandbox.ID)
	if err != nil || pausedSandbox == nil || pausedSandbox.State != types.StatePaused ||
		pausedSandbox.SnapshotRef != "snapshot-new" || paused.LatestEvent == nil ||
		paused.LatestEvent.SnapshotRef != "snapshot-new" || paused.LatestEvent.SnapshotLocation != "local" {
		t.Fatalf("atomic paused Sandbox/event = %+v, workflow=%+v, %v", pausedSandbox, paused, err)
	}
	resumed, err := st.CommitSandboxEvent(ctx, sandbox, ready)
	if err != nil || resumed.EventSeq != 3 {
		t.Fatalf("resumed = %+v, %v", resumed, err)
	}
	stored, err := st.Get(ctx, sandbox.ID)
	if err != nil || stored.State != types.StateRunning || stored.Metadata[clusterstate.ObjectMetadataKey] != dispatch.OpaqueBinding ||
		stored.Metadata["user"] != "kept" {
		t.Fatalf("stored Sandbox = %+v, %v", stored, err)
	}
}

func TestSandboxPresentationUpdateAdvancesLiveOutbox(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	dispatch := workflowDispatch(t, clusterstate.ExecutionKindSandbox, "sandbox-presentation", placement.BuildDemand{})
	decision := nodeexec.AdmissionDecision{
		State: nodeexec.AdmissionAdmitted, Result: clusterstate.DispatchAcceptedAdmitted,
		ReservationToken: "reservation-presentation",
	}
	if _, err := st.RecordSandboxWorkflow(ctx, dispatch, decision, workflowSandbox(dispatch.ObjectID)); err != nil {
		t.Fatal(err)
	}
	if _, err := st.ClaimSandboxWorkflow(ctx, dispatch.ObjectID, dispatch.DemandDigest, decision.ReservationToken); err != nil {
		t.Fatal(err)
	}
	update := sandboxEvent(nodeexec.EventUpdate{State: string(clusterstate.WorkflowRouteReady)})
	first, err := st.CommitSandboxEvent(ctx, workflowSandbox(dispatch.ObjectID), update)
	if err != nil || first.EventSeq != 1 {
		t.Fatalf("initial presentation event = %+v, %v", first, err)
	}
	update.Presentation.EndAt++
	second, err := st.CommitSandboxEvent(ctx, workflowSandbox(dispatch.ObjectID), update)
	if err != nil || second.EventSeq != 2 || second.LatestEvent.Presentation.EndAt != update.Presentation.EndAt {
		t.Fatalf("updated presentation event = %+v, %v", second, err)
	}
	update.Presentation.Metadata = map[string]string{"caller-mutated": "yes"}
	if len(second.LatestEvent.Presentation.Metadata) != 0 {
		t.Fatal("durable presentation aliases the caller-owned metadata map")
	}
	retry := sandboxEvent(nodeexec.EventUpdate{State: string(clusterstate.WorkflowRouteReady)})
	retry.Presentation.EndAt++
	third, err := st.CommitSandboxEvent(ctx, workflowSandbox(dispatch.ObjectID), retry)
	if err != nil || third.EventSeq != 2 {
		t.Fatalf("exact presentation retry = %+v, %v", third, err)
	}
}

func TestDeletedSandboxRemovesBusinessRowButRetainsDurableEvent(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	dispatch := workflowDispatch(t, clusterstate.ExecutionKindSandbox, "sandbox-deleted", placement.BuildDemand{})
	decision := nodeexec.AdmissionDecision{
		State: nodeexec.AdmissionAdmitted, Result: clusterstate.DispatchAcceptedAdmitted,
		ReservationToken: "reservation-deleted",
	}
	sandbox := workflowSandbox(dispatch.ObjectID)
	if _, err := st.RecordSandboxWorkflow(ctx, dispatch, decision, sandbox); err != nil {
		t.Fatal(err)
	}
	if _, err := st.ClaimSandboxWorkflow(ctx, dispatch.ObjectID, dispatch.DemandDigest, decision.ReservationToken); err != nil {
		t.Fatal(err)
	}
	if _, err := st.CommitSandboxEvent(ctx, sandbox, sandboxEvent(nodeexec.EventUpdate{
		State: string(clusterstate.WorkflowRouteReady),
	})); err != nil {
		t.Fatal(err)
	}
	deletedUpdate := sandboxEvent(nodeexec.EventUpdate{State: "DELETED"})
	deleted, err := st.CommitSandboxEvent(ctx, sandbox, deletedUpdate)
	if err != nil {
		t.Fatal(err)
	}
	if deleted.EventSeq != 2 || deleted.LatestEvent == nil || deleted.LatestEvent.State != "DELETED" {
		t.Fatalf("deleted workflow = %+v", deleted)
	}
	if stored, err := st.Get(ctx, sandbox.ID); err != nil || stored != nil {
		t.Fatalf("deleted Sandbox business row = %+v, %v", stored, err)
	}
	if exists, err := st.ExecutionObjectExists(ctx, clusterstate.ExecutionKindSandbox, sandbox.ID); err != nil || exists {
		t.Fatalf("deleted Sandbox remains occupied = %v, %v", exists, err)
	}
	pending, _, err := st.PendingExecutionEvents(
		ctx, dispatch.NodeID, dispatch.NodeEpoch, routesync.EventCursor{}, 10, routesync.MaxExecutionEventBytes,
	)
	if err != nil || len(pending) != 1 || pending[0].ObjectID != sandbox.ID || pending[0].State != "DELETED" {
		t.Fatalf("deleted Sandbox outbox = %+v, %v", pending, err)
	}
	if _, err := st.CommitSandboxEvent(ctx, sandbox, deletedUpdate); err != nil {
		t.Fatalf("idempotent deleted event: %v", err)
	}
}

func TestSandboxEventPreservesNewerBusinessObjectFields(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	dispatch := workflowDispatch(t, clusterstate.ExecutionKindSandbox, "sandbox-preserve", placement.BuildDemand{})
	decision := nodeexec.AdmissionDecision{
		State: nodeexec.AdmissionAdmitted, Result: clusterstate.DispatchAcceptedAdmitted,
		ReservationToken: "reservation-preserve",
	}
	if _, err := st.RecordSandboxWorkflow(ctx, dispatch, decision, workflowSandbox(dispatch.ObjectID)); err != nil {
		t.Fatal(err)
	}
	if _, err := st.ClaimSandboxWorkflow(ctx, dispatch.ObjectID, dispatch.DemandDigest, decision.ReservationToken); err != nil {
		t.Fatal(err)
	}
	callback := workflowSandbox(dispatch.ObjectID)
	if _, err := st.CommitSandboxEvent(ctx, callback, sandboxEvent(nodeexec.EventUpdate{
		State: string(clusterstate.WorkflowRouteReady),
	})); err != nil {
		t.Fatal(err)
	}
	current, err := st.Get(ctx, dispatch.ObjectID)
	if err != nil {
		t.Fatal(err)
	}
	current.DeadlineUnix = 123456789
	current.SnapshotRef = "manifest://newer-snapshot"
	current.RunID = "newer-run"
	current.Metadata["newer"] = "metadata"
	current.Env = map[string]string{}
	current.Env["newer"] = "env"
	if err := st.Put(ctx, current); err != nil {
		t.Fatal(err)
	}

	stale := cloneSandbox(callback)
	stale.DeadlineUnix = 1
	stale.SnapshotRef = "stale-snapshot"
	stale.RunID = "stale-run"
	stale.Metadata["newer"] = "stale"
	if _, err := st.CommitSandboxEvent(ctx, stale, sandboxEvent(nodeexec.EventUpdate{
		State: string(clusterstate.WorkflowRoutePaused),
	})); err != nil {
		t.Fatal(err)
	}
	stored, err := st.Get(ctx, dispatch.ObjectID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.State != types.StatePaused || stored.DeadlineUnix != 123456789 ||
		stored.SnapshotRef != "manifest://newer-snapshot" || stored.RunID != "newer-run" ||
		stored.Metadata["newer"] != "metadata" || stored.Env["newer"] != "env" {
		t.Fatalf("stale callback rewrote Sandbox fields: %+v", stored)
	}
}

func TestDuplicateBuildStatePersistsLaunchIdentity(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	dispatch := workflowDispatch(t, clusterstate.ExecutionKindBuild, "build-launch-fields", workflowBuildDemand(1))
	if _, err := st.PrepareBuildWorkflow(ctx, dispatch, workflowBuild(dispatch.ObjectID),
		workflowBuildCapacity(1, 4), ""); err != nil {
		t.Fatal(err)
	}
	callback := workflowBuild(dispatch.ObjectID)
	callback.RunID = "runner-1"
	callback.Names = []string{"build-name"}
	callback.Aliases = []string{"build-alias"}
	if _, err := st.CommitClusterBuildState(ctx, callback, nodeexec.EventUpdate{
		State: string(types.BuildRegistered),
	}); err != nil {
		t.Fatal(err)
	}
	stored, err := st.GetBuild(ctx, dispatch.ObjectID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.RunID != "runner-1" || !slices.Equal(stored.Names, callback.Names) ||
		!slices.Equal(stored.Aliases, callback.Aliases) {
		t.Fatalf("duplicate registered event lost launch identity: %+v", stored)
	}
	if _, err := st.CommitClusterBuildState(ctx, workflowBuild(dispatch.ObjectID), nodeexec.EventUpdate{
		State: string(types.BuildRegistered),
	}); err != nil {
		t.Fatalf("empty duplicate erased launch identity: %v", err)
	}
	conflict := workflowBuild(dispatch.ObjectID)
	conflict.RunID = "runner-2"
	if _, err := st.CommitClusterBuildState(ctx, conflict, nodeexec.EventUpdate{
		State: string(types.BuildRegistered),
	}); !errors.Is(err, ErrNodeWorkflowConflict) {
		t.Fatalf("conflicting runner identity error = %v", err)
	}
}

func TestBuildReadyMayOnlyAppendItsArtifactAlias(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	dispatch := workflowDispatch(t, clusterstate.ExecutionKindBuild, "build-ready-alias", workflowBuildDemand(1))
	if _, err := st.PrepareBuildWorkflow(ctx, dispatch, workflowBuild(dispatch.ObjectID),
		workflowBuildCapacity(1, 4), ""); err != nil {
		t.Fatal(err)
	}
	registered := workflowBuild(dispatch.ObjectID)
	registered.Names = []string{"build-name"}
	registered.Aliases = []string{"build-alias"}
	if _, err := st.CommitClusterBuildState(ctx, registered, nodeexec.EventUpdate{State: string(types.BuildRegistered)}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.CommitClusterBuildState(ctx, workflowBuild(dispatch.ObjectID), nodeexec.EventUpdate{State: string(types.BuildBuilding)}); err != nil {
		t.Fatal(err)
	}
	forged := workflowBuild(dispatch.ObjectID)
	forged.PersistID = "artifact-1"
	forged.Names = []string{"build-name", "unrelated"}
	forged.Aliases = []string{"build-alias", "artifact-1"}
	if _, err := st.CommitClusterBuildState(ctx, forged, nodeexec.EventUpdate{
		State: string(types.BuildReady), ArtifactRef: "artifact-1",
	}); !errors.Is(err, ErrNodeWorkflowConflict) {
		t.Fatalf("unrelated READY alias error = %v", err)
	}
	ready := workflowBuild(dispatch.ObjectID)
	ready.PersistID = "artifact-1"
	ready.Names = []string{"build-name", "artifact-1"}
	ready.Aliases = []string{"build-alias", "artifact-1"}
	if _, err := st.CommitClusterBuildState(ctx, ready, nodeexec.EventUpdate{
		State: string(types.BuildReady), ArtifactRef: "artifact-1",
	}); err != nil {
		t.Fatal(err)
	}
	stored, err := st.GetBuild(ctx, dispatch.ObjectID)
	if err != nil || !slices.Equal(stored.Names, ready.Names) || !slices.Equal(stored.Aliases, ready.Aliases) {
		t.Fatalf("ready aliases = %+v, err=%v", stored, err)
	}
}

func TestBuildAdmissionAndPromotionExcludePriorNodeEpoch(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	capacity := workflowBuildCapacity(1, 4)
	oldRunning := workflowDispatchEpoch(t, clusterstate.ExecutionKindBuild, "old-running", workflowBuildDemand(1), 6)
	oldQueued := workflowDispatchEpoch(t, clusterstate.ExecutionKindBuild, "old-queued", workflowBuildDemand(1), 6)
	currentRunning := workflowDispatchEpoch(t, clusterstate.ExecutionKindBuild, "current-running", workflowBuildDemand(1), 7)
	currentQueued := workflowDispatchEpoch(t, clusterstate.ExecutionKindBuild, "current-queued", workflowBuildDemand(1), 7)
	for _, dispatch := range []nodeexec.DispatchRecord{oldRunning, oldQueued, currentRunning, currentQueued} {
		record, err := st.PrepareBuildWorkflow(ctx, dispatch, workflowBuild(dispatch.ObjectID), capacity, "")
		if err != nil {
			t.Fatal(err)
		}
		if strings.HasSuffix(dispatch.ObjectID, "running") && record.AdmissionState != nodeexec.AdmissionAdmitted ||
			strings.HasSuffix(dispatch.ObjectID, "queued") && record.AdmissionState != nodeexec.AdmissionQueued {
			t.Fatalf("admission %s = %+v", dispatch.ObjectID, record)
		}
	}
	all, claimed, err := st.BuildAdmissionUsage(ctx, "node-1", 7)
	if err != nil || all.Slots != 2 || claimed.Slots != 1 {
		t.Fatalf("current epoch usage all=%+v claimed=%+v err=%v", all, claimed, err)
	}
	if _, err := st.ClaimBuildWorkflow(ctx, currentRunning.ObjectID, currentRunning.DemandDigest); err != nil {
		t.Fatal(err)
	}
	if _, err := st.CommitClusterBuildState(ctx, workflowBuild(currentRunning.ObjectID), nodeexec.EventUpdate{
		State: string(types.BuildError), Reason: "done",
	}); err != nil {
		t.Fatal(err)
	}
	promoted, err := st.PromoteBuildQueue(ctx, "node-1", 7, capacity, 4)
	if err != nil || len(promoted) != 1 || promoted[0].ObjectID != currentQueued.ObjectID {
		t.Fatalf("current epoch promotion = %+v, %v", promoted, err)
	}
	old, err := st.GetNodeWorkflow(ctx, clusterstate.ExecutionKindBuild, oldQueued.ObjectID)
	if err != nil || old.AdmissionState != nodeexec.AdmissionQueued {
		t.Fatalf("prior epoch queue was promoted = %+v, %v", old, err)
	}
}

func TestBuildQueuePromotionIsStrictFIFO(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	initial := workflowBuildCapacity(2, 4)
	blocker := workflowDispatch(t, clusterstate.ExecutionKindBuild, "blocker", workflowBuildDemand(1))
	large := workflowDispatch(t, clusterstate.ExecutionKindBuild, "large", workflowBuildDemand(2))
	small := workflowDispatch(t, clusterstate.ExecutionKindBuild, "small", workflowBuildDemand(1))
	for _, dispatch := range []nodeexec.DispatchRecord{blocker, large, small} {
		if _, err := st.PrepareBuildWorkflow(ctx, dispatch, workflowBuild(dispatch.ObjectID), initial, ""); err != nil {
			t.Fatal(err)
		}
	}
	promoted, err := st.PromoteBuildQueue(ctx, "node-1", 7, initial, 4)
	if err != nil || len(promoted) != 0 {
		t.Fatalf("strict FIFO promotion = %+v, %v", promoted, err)
	}
	smallRecord, err := st.GetNodeWorkflow(ctx, clusterstate.ExecutionKindBuild, small.ObjectID)
	if err != nil || smallRecord.AdmissionState != nodeexec.AdmissionQueued {
		t.Fatalf("small skipped FIFO head = %+v, %v", smallRecord, err)
	}
}

func TestNodeWorkflowBuildQueueSurvivesStoreRestartWithoutOutbox(t *testing.T) {
	ctx := context.Background()
	box, err := secretbox.NewFromColonHex(strings.Repeat("0", 64))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "restart.db")
	st, err := Open(path, box)
	if err != nil {
		t.Fatal(err)
	}
	capacity := workflowBuildCapacity(1, 4)
	first := workflowDispatch(t, clusterstate.ExecutionKindBuild, "build-running", workflowBuildDemand(1))
	queued := workflowDispatch(t, clusterstate.ExecutionKindBuild, "build-queued", workflowBuildDemand(1))
	if _, err := st.PrepareBuildWorkflow(ctx, first, workflowBuild(first.ObjectID), capacity, ""); err != nil {
		t.Fatal(err)
	}
	before, err := st.PrepareBuildWorkflow(ctx, queued, workflowBuild(queued.ObjectID), capacity, "")
	if err != nil || before.AdmissionState != nodeexec.AdmissionQueued || before.EventSeq != 0 || before.LatestEvent != nil {
		t.Fatalf("before restart = %+v, %v", before, err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := Open(path, box)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.Close() })
	after, err := restarted.GetNodeWorkflow(ctx, clusterstate.ExecutionKindBuild, queued.ObjectID)
	if err != nil || after.AdmissionState != nodeexec.AdmissionQueued || after.Result != clusterstate.DispatchAcceptedQueued ||
		after.EventSeq != 0 || after.LatestEvent != nil || after.ObjectState != string(types.BuildRegistered) {
		t.Fatalf("after restart = %+v, %v", after, err)
	}
	pending, _, err := restarted.PendingExecutionEvents(ctx, "node-1", 7, routesync.EventCursor{}, 10, 1<<20)
	if err != nil || len(pending) != 0 {
		t.Fatalf("restarted outbox = %+v, %v", pending, err)
	}
}
