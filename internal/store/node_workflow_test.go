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

func TestBuildAdmissionIsIdempotentConflictSafeAndBounded(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	capacity := nodeexec.BuildCapacity{Slots: 1, QueueLimit: 1}
	first := workflowDispatch(t, clusterstate.ExecutionKindBuild, "build-1", placement.BuildDemand{Slots: 1})
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
	conflict := workflowDispatchSpec(t, clusterstate.ExecutionKindBuild, "build-1", placement.BuildDemand{Slots: 1}, "different")
	if _, err := st.PrepareBuildWorkflow(ctx, conflict, workflowBuild(conflict.ObjectID), capacity, ""); !errors.Is(err, ErrNodeWorkflowConflict) {
		t.Fatalf("conflicting retry error = %v", err)
	}

	second := workflowDispatch(t, clusterstate.ExecutionKindBuild, "build-2", placement.BuildDemand{Slots: 1})
	queued, err := st.PrepareBuildWorkflow(ctx, second, workflowBuild(second.ObjectID), capacity, "")
	if err != nil || queued.Result != clusterstate.DispatchAcceptedQueued || queued.EventSeq != 1 ||
		queued.LatestEvent == nil || queued.LatestEvent.State != string(clusterstate.BuildQueued) {
		t.Fatalf("queued = %+v, %v", queued, err)
	}
	storedSecond, err := st.GetBuild(ctx, second.ObjectID)
	if err != nil || storedSecond.Status != types.BuildWaiting ||
		storedSecond.Metadata[clusterstate.ObjectMetadataKey] != second.OpaqueBinding {
		t.Fatalf("atomic queued Build = %+v, %v", storedSecond, err)
	}
	third := workflowDispatch(t, clusterstate.ExecutionKindBuild, "build-3", placement.BuildDemand{Slots: 1})
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
	if got, err := st.GetNodeWorkflow(ctx, clusterstate.ExecutionKindBuild, third.ObjectID); err != nil || got != nil {
		t.Fatalf("finalized rejection = %+v, %v", got, err)
	}
}

func TestConcurrentBuildAdmissionDoesNotOversubscribe(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	capacity := nodeexec.BuildCapacity{Slots: 1, QueueLimit: 4}
	dispatches := []nodeexec.DispatchRecord{
		workflowDispatch(t, clusterstate.ExecutionKindBuild, "build-a", placement.BuildDemand{Slots: 1}),
		workflowDispatch(t, clusterstate.ExecutionKindBuild, "build-b", placement.BuildDemand{Slots: 1}),
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

func TestBuildAdmissionRollsBackJournalWhenBusinessObjectCannotPersist(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	dispatch := workflowDispatch(t, clusterstate.ExecutionKindBuild, "build-invalid", placement.BuildDemand{Slots: 1})
	build := workflowBuild(dispatch.ObjectID)
	build.ManifestKey = "not-hex"
	if _, err := st.PrepareBuildWorkflow(ctx, dispatch, build, nodeexec.BuildCapacity{Slots: 1, QueueLimit: 4}, ""); err == nil {
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

func TestBuildAdmissionDoesNotOverwriteExistingLocalBuild(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	dispatch := workflowDispatch(t, clusterstate.ExecutionKindBuild, "build-existing", placement.BuildDemand{Slots: 1})
	existing := workflowBuild(dispatch.ObjectID)
	existing.TemplateID = "local-template"
	if err := st.PutBuild(ctx, existing); err != nil {
		t.Fatal(err)
	}
	if _, err := st.PrepareBuildWorkflow(ctx, dispatch, workflowBuild(dispatch.ObjectID),
		nodeexec.BuildCapacity{Slots: 1, QueueLimit: 4}, ""); !errors.Is(err, ErrNodeWorkflowConflict) {
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
	initial := nodeexec.BuildCapacity{Slots: 2, QueueLimit: 4}
	running := workflowDispatch(t, clusterstate.ExecutionKindBuild, "build-running", placement.BuildDemand{Slots: 2})
	impossible := workflowDispatch(t, clusterstate.ExecutionKindBuild, "build-impossible", placement.BuildDemand{Slots: 2})
	following := workflowDispatch(t, clusterstate.ExecutionKindBuild, "build-following", placement.BuildDemand{Slots: 1})
	for _, dispatch := range []nodeexec.DispatchRecord{running, impossible, following} {
		if _, err := st.PrepareBuildWorkflow(ctx, dispatch, workflowBuild(dispatch.ObjectID), initial, ""); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := st.FailPendingNodeWorkflow(ctx, clusterstate.ExecutionKindBuild,
		running.ObjectID, running.DemandDigest, "running_build_finished"); err != nil {
		t.Fatal(err)
	}

	promoted, err := st.PromoteBuildQueue(ctx, "node-1", 7, nodeexec.BuildCapacity{Slots: 1, QueueLimit: 4}, 4)
	if err != nil || len(promoted) != 1 || promoted[0].ObjectID != following.ObjectID {
		t.Fatalf("promotion after capacity shrink = %+v, %v", promoted, err)
	}
	record, err := st.GetNodeWorkflow(ctx, clusterstate.ExecutionKindBuild, impossible.ObjectID)
	if err != nil || record == nil || record.AdmissionState != nodeexec.AdmissionTerminal ||
		record.LatestEvent == nil || record.LatestEvent.State != string(clusterstate.BuildError) ||
		record.LatestEvent.Reason != "exceeds_build_capacity" {
		t.Fatalf("impossible queue head = %+v, %v", record, err)
	}
	build, err := st.GetBuild(ctx, impossible.ObjectID)
	if err != nil || build == nil || build.Status != types.BuildError || build.Reason != "exceeds_build_capacity" {
		t.Fatalf("impossible Build = %+v, %v", build, err)
	}
}

func TestBuildTerminalEventReleasesCapacityAndAckCannotDropNewerEvent(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	capacity := nodeexec.BuildCapacity{Slots: 1, QueueLimit: 4}
	first := workflowDispatch(t, clusterstate.ExecutionKindBuild, "build-1", placement.BuildDemand{Slots: 1})
	second := workflowDispatch(t, clusterstate.ExecutionKindBuild, "build-2", placement.BuildDemand{Slots: 1})
	if _, err := st.PrepareBuildWorkflow(ctx, first, workflowBuild(first.ObjectID), capacity, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := st.PrepareBuildWorkflow(ctx, second, workflowBuild(second.ObjectID), capacity, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := st.ClaimBuildWorkflow(ctx, first.ObjectID, first.DemandDigest); err != nil {
		t.Fatal(err)
	}
	build := workflowBuild(first.ObjectID)
	build.TemplateID = "stale-callback-must-not-rewrite-identity"
	build.Metadata["user"] = "stale"
	for _, update := range []nodeexec.EventUpdate{
		{State: string(clusterstate.BuildRegistered)},
		{State: string(clusterstate.BuildBuilding)},
		{State: string(clusterstate.BuildReady), ArtifactRef: "e2b-img-artifact"},
	} {
		if _, err := st.CommitBuildEvent(ctx, build, update); err != nil {
			t.Fatal(err)
		}
	}
	stored, err := st.GetBuild(ctx, first.ObjectID)
	if err != nil || stored.Status != types.BuildReady || stored.PersistID != "e2b-img-artifact" ||
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
	firstBatch, cursor, err := st.PendingExecutionEvents(ctx, "node-1", 7, routesync.EventCursor{}, 1, 1<<20)
	if err != nil || len(firstBatch) != 1 {
		t.Fatalf("first replay batch = %+v cursor=%+v err=%v", firstBatch, cursor, err)
	}
	secondBatch, _, err := st.PendingExecutionEvents(ctx, "node-1", 7, cursor, 1, 1<<20)
	if err != nil || len(secondBatch) != 1 || secondBatch[0].ObjectID == firstBatch[0].ObjectID {
		t.Fatalf("cursor replay batch = %+v after %+v err=%v", secondBatch, firstBatch, err)
	}

	pending, _, err := st.PendingExecutionEvents(ctx, "node-1", 7, routesync.EventCursor{}, 10, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	events := map[string]routesync.ExecutionEvent{}
	for _, event := range pending {
		events[event.ObjectID] = event
	}
	if events[first.ObjectID].EventSeq != 3 || events[second.ObjectID].EventSeq != 2 ||
		events[second.ObjectID].State != string(clusterstate.BuildRegistered) {
		t.Fatalf("pending events = %+v", events)
	}
	firstEvent := events[first.ObjectID]
	if err := st.AckExecutionEvent(ctx, "node-1", 7, routesync.EventAck{
		ObjectKind: "build", ObjectID: first.ObjectID, RegistryGeneration: firstEvent.RegistryGeneration,
		BindingDigest: firstEvent.BindingDigest, EventSeq: 2,
	}); err != nil {
		t.Fatal(err)
	}
	pending, _, err = st.PendingExecutionEvents(ctx, "node-1", 7, routesync.EventCursor{}, 10, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if findEventSeq(pending, first.ObjectID) != 3 {
		t.Fatalf("ACK(2) discarded event 3: %+v", pending)
	}
	if err := st.FinalizeNodeWorkflow(ctx, clusterstate.ExecutionKindBuild, first.ObjectID, first.BindingDigest); err != nil {
		t.Fatal(err)
	}
	if err := st.AckExecutionEvent(ctx, "node-1", 7, routesync.EventAck{
		ObjectKind: "build", ObjectID: first.ObjectID, RegistryGeneration: firstEvent.RegistryGeneration,
		BindingDigest: firstEvent.BindingDigest, EventSeq: 3,
	}); err != nil {
		t.Fatal(err)
	}
	if got, err := st.GetNodeWorkflow(ctx, clusterstate.ExecutionKindBuild, first.ObjectID); err != nil || got != nil {
		t.Fatalf("finalized+acked workflow = %+v, %v", got, err)
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
	record, err := st.RecordSandboxWorkflow(ctx, dispatch, decision)
	if err != nil || record.ReservationToken != "reservation-1" {
		t.Fatalf("record = %+v, %v", record, err)
	}
	if _, err := st.ClaimSandboxWorkflow(ctx, dispatch.ObjectID, dispatch.DemandDigest, "reservation-1"); err != nil {
		t.Fatal(err)
	}
	sandbox := workflowSandbox(dispatch.ObjectID)
	ready := nodeexec.EventUpdate{State: string(clusterstate.WorkflowRouteReady)}
	first, err := st.CommitSandboxEvent(ctx, sandbox, ready)
	if err != nil || first.EventSeq != 1 || first.LatestEvent.DataEndpoint != dispatch.DataEndpoint {
		t.Fatalf("ready = %+v, %v", first, err)
	}
	retry, err := st.CommitSandboxEvent(ctx, sandbox, ready)
	if err != nil || retry.EventSeq != 1 {
		t.Fatalf("ready retry = %+v, %v", retry, err)
	}
	if _, err := st.CommitSandboxEvent(ctx, sandbox, nodeexec.EventUpdate{State: string(clusterstate.WorkflowRoutePaused)}); err != nil {
		t.Fatal(err)
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

func TestSandboxEventPreservesNewerBusinessObjectFields(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	dispatch := workflowDispatch(t, clusterstate.ExecutionKindSandbox, "sandbox-preserve", placement.BuildDemand{})
	decision := nodeexec.AdmissionDecision{
		State: nodeexec.AdmissionAdmitted, Result: clusterstate.DispatchAcceptedAdmitted,
		ReservationToken: "reservation-preserve",
	}
	if _, err := st.RecordSandboxWorkflow(ctx, dispatch, decision); err != nil {
		t.Fatal(err)
	}
	if _, err := st.ClaimSandboxWorkflow(ctx, dispatch.ObjectID, dispatch.DemandDigest, decision.ReservationToken); err != nil {
		t.Fatal(err)
	}
	callback := workflowSandbox(dispatch.ObjectID)
	if _, err := st.CommitSandboxEvent(ctx, callback, nodeexec.EventUpdate{
		State: string(clusterstate.WorkflowRouteReady),
	}); err != nil {
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
	if _, err := st.CommitSandboxEvent(ctx, stale, nodeexec.EventUpdate{
		State: string(clusterstate.WorkflowRoutePaused),
	}); err != nil {
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

func TestDuplicateBuildRegisteredEventPersistsLaunchIdentity(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	dispatch := workflowDispatch(t, clusterstate.ExecutionKindBuild, "build-launch-fields", placement.BuildDemand{Slots: 1})
	if _, err := st.PrepareBuildWorkflow(ctx, dispatch, workflowBuild(dispatch.ObjectID),
		nodeexec.BuildCapacity{Slots: 1, QueueLimit: 4}, ""); err != nil {
		t.Fatal(err)
	}
	callback := workflowBuild(dispatch.ObjectID)
	callback.RunID = "runner-1"
	callback.Names = []string{"build-name"}
	callback.Aliases = []string{"build-alias"}
	if _, err := st.CommitBuildEvent(ctx, callback, nodeexec.EventUpdate{
		State: string(clusterstate.BuildRegistered),
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
	if _, err := st.CommitBuildEvent(ctx, workflowBuild(dispatch.ObjectID), nodeexec.EventUpdate{
		State: string(clusterstate.BuildRegistered),
	}); err != nil {
		t.Fatalf("empty duplicate erased launch identity: %v", err)
	}
	conflict := workflowBuild(dispatch.ObjectID)
	conflict.RunID = "runner-2"
	if _, err := st.CommitBuildEvent(ctx, conflict, nodeexec.EventUpdate{
		State: string(clusterstate.BuildRegistered),
	}); !errors.Is(err, ErrNodeWorkflowConflict) {
		t.Fatalf("conflicting runner identity error = %v", err)
	}
}

func TestBuildReadyMayOnlyAppendItsArtifactAlias(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	dispatch := workflowDispatch(t, clusterstate.ExecutionKindBuild, "build-ready-alias", placement.BuildDemand{Slots: 1})
	if _, err := st.PrepareBuildWorkflow(ctx, dispatch, workflowBuild(dispatch.ObjectID),
		nodeexec.BuildCapacity{Slots: 1, QueueLimit: 4}, ""); err != nil {
		t.Fatal(err)
	}
	registered := workflowBuild(dispatch.ObjectID)
	registered.Names = []string{"build-name"}
	registered.Aliases = []string{"build-alias"}
	if _, err := st.CommitBuildEvent(ctx, registered, nodeexec.EventUpdate{State: string(clusterstate.BuildRegistered)}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.CommitBuildEvent(ctx, workflowBuild(dispatch.ObjectID), nodeexec.EventUpdate{State: string(clusterstate.BuildBuilding)}); err != nil {
		t.Fatal(err)
	}
	forged := workflowBuild(dispatch.ObjectID)
	forged.Names = []string{"build-name", "unrelated"}
	forged.Aliases = []string{"build-alias", "artifact-1"}
	if _, err := st.CommitBuildEvent(ctx, forged, nodeexec.EventUpdate{
		State: string(clusterstate.BuildReady), ArtifactRef: "artifact-1",
	}); !errors.Is(err, ErrNodeWorkflowConflict) {
		t.Fatalf("unrelated READY alias error = %v", err)
	}
	ready := workflowBuild(dispatch.ObjectID)
	ready.Names = []string{"build-name", "artifact-1"}
	ready.Aliases = []string{"build-alias", "artifact-1"}
	if _, err := st.CommitBuildEvent(ctx, ready, nodeexec.EventUpdate{
		State: string(clusterstate.BuildReady), ArtifactRef: "artifact-1",
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
	capacity := nodeexec.BuildCapacity{Slots: 1, QueueLimit: 4}
	oldRunning := workflowDispatchEpoch(t, clusterstate.ExecutionKindBuild, "old-running", placement.BuildDemand{Slots: 1}, 6)
	oldQueued := workflowDispatchEpoch(t, clusterstate.ExecutionKindBuild, "old-queued", placement.BuildDemand{Slots: 1}, 6)
	currentRunning := workflowDispatchEpoch(t, clusterstate.ExecutionKindBuild, "current-running", placement.BuildDemand{Slots: 1}, 7)
	currentQueued := workflowDispatchEpoch(t, clusterstate.ExecutionKindBuild, "current-queued", placement.BuildDemand{Slots: 1}, 7)
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
	if _, err := st.CommitBuildEvent(ctx, workflowBuild(currentRunning.ObjectID), nodeexec.EventUpdate{
		State: string(clusterstate.BuildError), Reason: "done",
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
	initial := nodeexec.BuildCapacity{Slots: 2, QueueLimit: 4}
	blocker := workflowDispatch(t, clusterstate.ExecutionKindBuild, "blocker", placement.BuildDemand{Slots: 1})
	large := workflowDispatch(t, clusterstate.ExecutionKindBuild, "large", placement.BuildDemand{Slots: 2})
	small := workflowDispatch(t, clusterstate.ExecutionKindBuild, "small", placement.BuildDemand{Slots: 1})
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

func TestNodeWorkflowQueueAndOutboxSurviveStoreRestart(t *testing.T) {
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
	capacity := nodeexec.BuildCapacity{Slots: 1, QueueLimit: 4}
	first := workflowDispatch(t, clusterstate.ExecutionKindBuild, "build-running", placement.BuildDemand{Slots: 1})
	queued := workflowDispatch(t, clusterstate.ExecutionKindBuild, "build-queued", placement.BuildDemand{Slots: 1})
	if _, err := st.PrepareBuildWorkflow(ctx, first, workflowBuild(first.ObjectID), capacity, ""); err != nil {
		t.Fatal(err)
	}
	before, err := st.PrepareBuildWorkflow(ctx, queued, workflowBuild(queued.ObjectID), capacity, "")
	if err != nil || before.AdmissionState != nodeexec.AdmissionQueued || before.EventSeq != 1 {
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
		after.EventSeq != 1 || after.LatestEvent == nil {
		t.Fatalf("after restart = %+v, %v", after, err)
	}
	pending, _, err := restarted.PendingExecutionEvents(ctx, "node-1", 7, routesync.EventCursor{}, 10, 1<<20)
	if err != nil || findEventSeq(pending, queued.ObjectID) != 1 {
		t.Fatalf("restarted outbox = %+v, %v", pending, err)
	}
}

func findEventSeq(events []routesync.ExecutionEvent, objectID string) uint64 {
	for _, event := range events {
		if event.ObjectID == objectID {
			return event.EventSeq
		}
	}
	return 0
}
