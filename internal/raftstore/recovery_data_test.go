package raftstore

import (
	"bytes"
	"fmt"
	"testing"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/routeapi"
)

func TestDataRecoveryRequiresRebindBeforeProjectionActivation(t *testing.T) {
	source := testRegistryLayout(4, "generation-source")
	target := testRegistryLayout(4, "generation-target")
	group, routeKey := "/recovery", "route"
	state, initial := initializedRouteShard(t, target, group, routeKey)
	recovery, recoveryIdentity, finalIdentity := beginRecoveryFixture(t, &state, initial, source, target, 2)
	record := recoveryRouteRecord(t, source, target, recovery, group, routeKey, "sandbox-recovered", "report-1")

	applyDataOK(t, &state, 3, DataCommand{
		Type: DataStageRecovery, Identity: recoveryIdentity, RecoveryRecord: &record,
	})
	if len(state.Routes) != 0 {
		t.Fatal("staged recovery exposed a Route before Binding rebind")
	}
	update := recoveryUpdate(record)
	result := ApplyDataCommand(&state, 4, DataCommand{
		Type: DataActivateRecovery, Identity: recoveryIdentity, RecoveryUpdate: &update,
	})
	if !result.Conflict || len(state.Routes) != 0 {
		t.Fatalf("activation before rebind = %+v, routes=%d", result, len(state.Routes))
	}

	applyDataOK(t, &state, 5, DataCommand{
		Type: DataAckRecovery, Identity: recoveryIdentity, RecoveryUpdate: &update,
	})
	applyDataOK(t, &state, 6, DataCommand{
		Type: DataStageRecovery, Identity: recoveryIdentity, RecoveryRecord: &record,
	})
	key := recoveryRecordKey(record)
	if state.RecoveryRecords[key].State != RecoveryObjectRebound {
		t.Fatal("duplicate source report regressed an acknowledged rebind")
	}
	applyDataOK(t, &state, 7, DataCommand{
		Type: DataActivateRecovery, Identity: recoveryIdentity, RecoveryUpdate: &update,
	})
	stored := state.Routes[routeMapKey(group, routeKey)]
	if stored.State != clusterstate.WorkflowRouteReady || stored.Revision.LogIndex != 7 {
		t.Fatalf("activated recovery Route = %+v", stored)
	}

	final := RecoveryFinalization{
		RecoveryEpoch: recovery.RecoveryEpoch, SourceRegistryGeneration: recovery.SourceRegistryGeneration,
		SourceRegistryLayoutDigest: recovery.SourceRegistryLayoutDigest,
	}
	applyDataOK(t, &state, 8, DataCommand{
		Type: DataFinalizeRecovery, Identity: finalIdentity, RecoveryFinal: &final,
	})
	if state.Recovery != nil || len(state.RecoveryRecords) != 0 || len(state.RecoveryClaims) != 0 ||
		!state.Accepts(finalIdentity) || state.Accepts(recoveryIdentity) {
		t.Fatalf("finalized recovery retained staging authority: %+v", state)
	}
	if err := state.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestDataRecoveryQuarantinesConflictingLogicalClaims(t *testing.T) {
	source := testRegistryLayout(4, "generation-source-conflict")
	target := testRegistryLayout(4, "generation-target-conflict")
	group, routeKey := "/recovery", "conflict"
	state, initial := initializedRouteShard(t, target, group, routeKey)
	recovery, identity, _ := beginRecoveryFixture(t, &state, initial, source, target, 2)
	first := recoveryRouteRecord(t, source, target, recovery, group, routeKey, "sandbox-a", "report-a")
	second := recoveryRouteRecord(t, source, target, recovery, group, routeKey, "sandbox-b", "report-b")

	applyDataOK(t, &state, 3, DataCommand{Type: DataStageRecovery, Identity: identity, RecoveryRecord: &first})
	applyDataOK(t, &state, 4, DataCommand{Type: DataStageRecovery, Identity: identity, RecoveryRecord: &second})
	for _, record := range state.RecoveryRecords {
		if record.State != RecoveryObjectQuarantined || len(record.ConflictingReportDigests) != 2 {
			t.Fatalf("conflicting recovery claim was not quarantined: %+v", record)
		}
	}
	update := recoveryUpdate(first)
	result := ApplyDataCommand(&state, 5, DataCommand{
		Type: DataAckRecovery, Identity: identity, RecoveryUpdate: &update,
	})
	if !result.Conflict {
		t.Fatal("quarantined recovery object accepted a rebind acknowledgement")
	}
	if err := state.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestDataRecoveryNodeStagingResetReplacesPartialReport(t *testing.T) {
	source := testRegistryLayout(4, "generation-source-reset")
	target := testRegistryLayout(4, "generation-target-reset")
	group, routeKey := "/recovery", "reset"
	state, initial := initializedRouteShard(t, target, group, routeKey)
	recovery, identity, _ := beginRecoveryFixture(t, &state, initial, source, target, 2)
	first := recoveryRouteRecord(t, source, target, recovery, group, routeKey, "sandbox-old-a", "report-old")
	second := recoveryRouteRecord(t, source, target, recovery, group, routeKey, "sandbox-old-b", "report-old")

	applyDataOK(t, &state, 3, DataCommand{Type: DataStageRecovery, Identity: identity, RecoveryRecord: &first})
	applyDataOK(t, &state, 4, DataCommand{Type: DataStageRecovery, Identity: identity, RecoveryRecord: &second})
	applyDataOK(t, &state, 5, DataCommand{
		Type: DataResetRecoveryNode, Identity: identity,
		RecoveryReset: &RecoveryNodeStagingReset{
			RecoveryEpoch: recovery.RecoveryEpoch, NodeID: first.NodeID,
			NodeEpoch: first.NodeEpoch, SessionSeq: 12,
		},
	})
	if len(state.RecoveryRecords) != 0 || len(state.RecoveryClaims) != 0 {
		t.Fatalf("staging reset retained partial report: records=%d claims=%d", len(state.RecoveryRecords), len(state.RecoveryClaims))
	}

	current := first
	current.SessionSeq = 12
	current.ReportDigest = digestFor("report-current")
	applyDataOK(t, &state, 6, DataCommand{Type: DataStageRecovery, Identity: identity, RecoveryRecord: &current})
	if len(state.RecoveryRecords) != 1 || len(state.RecoveryClaims) != 1 ||
		state.RecoveryRecords[recoveryRecordKey(current)].ReportDigest != current.ReportDigest {
		t.Fatalf("replacement report was not staged exactly: %+v", state.RecoveryRecords)
	}
	if err := state.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestDataRecoveryRebindProjectionRetryIsExactlyIdempotent(t *testing.T) {
	source := testRegistryLayout(4, "generation-source-retry")
	target := testRegistryLayout(4, "generation-target-retry")
	group, routeKey := "/recovery", "retry"
	state, initial := initializedRouteShard(t, target, group, routeKey)
	recovery, identity, _ := beginRecoveryFixture(t, &state, initial, source, target, 2)
	record := recoveryRouteRecord(t, source, target, recovery, group, routeKey, "sandbox-retry", "report-retry")
	applyDataOK(t, &state, 3, DataCommand{Type: DataStageRecovery, Identity: identity, RecoveryRecord: &record})

	update := recoveryUpdate(record)
	projection := cloneRouteRecord(*record.Route)
	update.Route = &projection
	applyDataOK(t, &state, 4, DataCommand{Type: DataAckRecovery, Identity: identity, RecoveryUpdate: &update})
	applyDataOK(t, &state, 5, DataCommand{Type: DataAckRecovery, Identity: identity, RecoveryUpdate: &update})

	newer := cloneRouteRecord(projection)
	newer.Ready.LastEventSeq++
	update.Route = &newer
	applyDataOK(t, &state, 6, DataCommand{Type: DataAckRecovery, Identity: identity, RecoveryUpdate: &update})

	changed := cloneRouteRecord(newer)
	changed.Ready.TrafficAccessToken = "changed-without-new-event"
	update.Route = &changed
	result := ApplyDataCommand(&state, 7, DataCommand{
		Type: DataAckRecovery, Identity: identity, RecoveryUpdate: &update,
	})
	if !result.Conflict {
		t.Fatal("rebind retry changed its committed projection without a newer event")
	}
	stored := state.RecoveryRecords[recoveryRecordKey(record)]
	if stored.State != RecoveryObjectRebound || stored.Route.Ready.LastEventSeq != newer.Ready.LastEventSeq ||
		stored.Route.Ready.TrafficAccessToken != newer.Ready.TrafficAccessToken {
		t.Fatalf("committed rebind projection changed after conflicting retry: %+v", stored)
	}
}

func TestPebbleRecoveryProgressSurvivesRestartAndSnapshot(t *testing.T) {
	engine, err := OpenPebbleStateEngine(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	source := testRegistryLayout(4, "generation-source-disk")
	target := testRegistryLayout(4, "generation-target-disk")
	group, routeKey := "/recovery", "disk"
	initial := routeShardIdentity(t, target, group, routeKey)
	shardID := DataRaftShardID(initial.ShardID)
	machine := engine.NewStateMachine(shardID, 1)
	if _, err := machine.Open(make(chan struct{})); err != nil {
		t.Fatal(err)
	}
	bootstrap, err := NewDataShardBootstrap(target, initial.ShardID)
	if err != nil {
		t.Fatal(err)
	}
	applyDiskData(t, machine, 1, DataCommand{
		Type: DataInitializeShard, Identity: initial, Bootstrap: &bootstrap, ReplicaIDs: bootstrap.ReplicaIDs,
	})
	recovery, recoveryIdentity, finalIdentity := recoveryFixture(t, initial, source, target, 2)
	applyDiskData(t, machine, 2, DataCommand{
		Type: DataBeginRecovery, Identity: recoveryIdentity, RecoveryStart: &recovery,
	})
	record := recoveryRouteRecord(t, source, target, recovery, group, routeKey, "sandbox-disk-recovery", "report-disk")
	applyDiskData(t, machine, 3, DataCommand{
		Type: DataStageRecovery, Identity: recoveryIdentity, RecoveryRecord: &record,
	})
	if err := machine.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := machine.Close(); err != nil {
		t.Fatal(err)
	}

	machine = engine.NewStateMachine(shardID, 1)
	if index, err := machine.Open(make(chan struct{})); err != nil || index != 3 {
		t.Fatalf("reopen staged recovery = %d, %v", index, err)
	}
	update := recoveryUpdate(record)
	applyDiskData(t, machine, 4, DataCommand{
		Type: DataAckRecovery, Identity: recoveryIdentity, RecoveryUpdate: &update,
	})
	context, err := machine.PrepareSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	var snapshot bytes.Buffer
	if err := machine.SaveSnapshot(context, &snapshot, make(chan struct{})); err != nil {
		t.Fatal(err)
	}

	targetReplica := engine.NewStateMachine(shardID, 99)
	if _, err := targetReplica.Open(make(chan struct{})); err != nil {
		t.Fatal(err)
	}
	if err := targetReplica.RecoverFromSnapshot(bytes.NewReader(snapshot.Bytes()), make(chan struct{})); err != nil {
		t.Fatal(err)
	}
	applyDiskData(t, targetReplica, 5, DataCommand{
		Type: DataActivateRecovery, Identity: recoveryIdentity, RecoveryUpdate: &update,
	})
	final := RecoveryFinalization{
		RecoveryEpoch: recovery.RecoveryEpoch, SourceRegistryGeneration: recovery.SourceRegistryGeneration,
		SourceRegistryLayoutDigest: recovery.SourceRegistryLayoutDigest,
	}
	applyDiskData(t, targetReplica, 6, DataCommand{
		Type: DataFinalizeRecovery, Identity: finalIdentity, RecoveryFinal: &final,
	})
	value, err := targetReplica.Lookup(DataLookup{Route: &routeapi.ReadRouteRequest{
		RequestIdentity: routeIdentity(finalIdentity), Group: group, RouteKey: routeKey,
	}})
	if err != nil {
		t.Fatal(err)
	}
	result := value.(DataLookupResult).Route
	if result == nil || result.Outcome != routeapi.ReadReady || result.RouteRevision != 5 ||
		result.Route == nil || result.Route.SandboxID != "sandbox-disk-recovery" {
		t.Fatalf("recovered on-disk READY Route = %+v", result)
	}
}

func TestPebbleRecoveryLookupIsBoundedAndPaged(t *testing.T) {
	engine, err := OpenPebbleStateEngine(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	source := testRegistryLayout(4, "generation-source-lookup")
	target := testRegistryLayout(4, "generation-target-lookup")
	group := "/recovery/lookup"
	firstRouteKey := routeKeyForShard(t, target, group, 0, "route")
	initial := routeShardIdentity(t, target, group, firstRouteKey)
	machine := engine.NewStateMachine(DataRaftShardID(initial.ShardID), 1)
	if _, err := machine.Open(make(chan struct{})); err != nil {
		t.Fatal(err)
	}
	bootstrap, err := NewDataShardBootstrap(target, initial.ShardID)
	if err != nil {
		t.Fatal(err)
	}
	applyDiskData(t, machine, 1, DataCommand{
		Type: DataInitializeShard, Identity: initial, Bootstrap: &bootstrap, ReplicaIDs: bootstrap.ReplicaIDs,
	})
	recovery, recoveryIdentity, _ := recoveryFixture(t, initial, source, target, 2)
	applyDiskData(t, machine, 2, DataCommand{
		Type: DataBeginRecovery, Identity: recoveryIdentity, RecoveryStart: &recovery,
	})
	for index := 0; index < 3; index++ {
		routeKey := routeKeyForShard(t, target, group, initial.ShardID, fmt.Sprintf("route-%d", index))
		record := recoveryRouteRecord(
			t, source, target, recovery, group, routeKey,
			fmt.Sprintf("sandbox-%d", index), fmt.Sprintf("report-%d", index),
		)
		applyDiskData(t, machine, uint64(index+3), DataCommand{
			Type: DataStageRecovery, Identity: recoveryIdentity, RecoveryRecord: &record,
		})
	}
	lookup := func(after string) RecoveryLookupResult {
		t.Helper()
		value, err := machine.Lookup(DataLookup{Recovery: &RecoveryLookup{
			Identity: recoveryIdentity, AfterKey: after, Limit: 2,
		}})
		if err != nil {
			t.Fatal(err)
		}
		result := value.(DataLookupResult).Recovery
		if result == nil {
			t.Fatal("on-disk recovery lookup returned no result")
		}
		return *result
	}
	first := lookup("")
	if !first.Available || len(first.Records) != 2 || first.NextKey == "" {
		t.Fatalf("first on-disk recovery page = %+v", first)
	}
	second := lookup(first.NextKey)
	if !second.Available || len(second.Records) != 1 || second.NextKey != "" {
		t.Fatalf("second on-disk recovery page = %+v", second)
	}
}

func beginRecoveryFixture(
	t *testing.T,
	state *DataState,
	initial ShardRequestIdentity,
	source RegistryLayout,
	target RegistryLayout,
	index uint64,
) (DataRecoveryState, ShardRequestIdentity, ShardRequestIdentity) {
	t.Helper()
	recovery, recoveryIdentity, finalIdentity := recoveryFixture(t, initial, source, target, index)
	applyDataOK(t, state, index, DataCommand{
		Type: DataBeginRecovery, Identity: recoveryIdentity, RecoveryStart: &recovery,
	})
	return recovery, recoveryIdentity, finalIdentity
}

func recoveryFixture(
	t *testing.T,
	initial ShardRequestIdentity,
	source RegistryLayout,
	target RegistryLayout,
	recoveryEpoch uint64,
) (DataRecoveryState, ShardRequestIdentity, ShardRequestIdentity) {
	t.Helper()
	sourceDigest, err := source.Digest()
	if err != nil {
		t.Fatal(err)
	}
	targetDigest, err := target.Digest()
	if err != nil {
		t.Fatal(err)
	}
	targetPermit := PermitIdentity{
		ClusterID: target.ClusterID, RegistryGeneration: target.RegistryGeneration,
		SystemEpoch: recoveryEpoch, RegistryLayoutDigest: targetDigest,
	}
	recovery := DataRecoveryState{
		RecoveryEpoch: recoveryEpoch, SourceClusterID: source.ClusterID,
		SourceRegistryGeneration: source.RegistryGeneration, SourceRegistryLayoutDigest: sourceDigest,
		Target: targetPermit,
	}
	recoveryIdentity := ShardRequestIdentity{PermitIdentity: targetPermit, ShardID: initial.ShardID}
	finalPermit := targetPermit
	finalPermit.SystemEpoch++
	finalIdentity := ShardRequestIdentity{PermitIdentity: finalPermit, ShardID: initial.ShardID}
	return recovery, recoveryIdentity, finalIdentity
}

func recoveryRouteRecord(
	t *testing.T,
	source RegistryLayout,
	target RegistryLayout,
	recovery DataRecoveryState,
	group string,
	routeKey string,
	sandboxID string,
	report string,
) RecoveryObjectRecord {
	t.Helper()
	intent := testDispatchIntent(t)
	sourceBinding := testBinding(
		t, source, clusterstate.ExecutionKindSandbox, sandboxID, group, routeKey, "node-1", intent,
	)
	targetBinding := testBinding(
		t, target, clusterstate.ExecutionKindSandbox, sandboxID, group, routeKey, "node-1", intent,
	)
	spec, err := clusterstate.ParseSandboxDispatchSpec(intent.DispatchSpec)
	if err != nil {
		t.Fatal(err)
	}
	route := clusterstate.RouteWorkflowRecord{
		Group: group, RouteKey: routeKey, State: clusterstate.WorkflowRouteReady,
		Ready: &clusterstate.ReadyRoute{
			SandboxID: sandboxID, NodeID: targetBinding.NodeID, NodeEpoch: targetBinding.NodeEpoch,
			DataEndpoint: targetBinding.DataEndpoint, TargetPort: spec.TargetPort, AccessToken: spec.AccessToken,
			TrafficAccessToken: "traffic-token", TemplateRef: spec.TemplateRef,
			RegistryGeneration: targetBinding.RegistryGeneration,
			OpaqueBinding:      targetBinding.OpaqueBinding,
			BindingDigest:      targetBinding.BindingDigest, LastEventSeq: 9, Intent: intent,
			Presentation: testSandboxPresentation(),
		},
	}
	return RecoveryObjectRecord{
		RecoveryEpoch: recovery.RecoveryEpoch, Kind: clusterstate.ExecutionKindSandbox,
		Group: group, RouteKey: routeKey, ObjectID: sandboxID,
		NodeID: targetBinding.NodeID, NodeEpoch: targetBinding.NodeEpoch, SessionSeq: 11, EventSeq: 9,
		ReportDigest: digestFor(report), SourceRegistryGeneration: source.RegistryGeneration,
		SourceRegistryLayoutDigest: recovery.SourceRegistryLayoutDigest, SourceOpaqueBinding: sourceBinding.OpaqueBinding,
		SourceBindingDigest: sourceBinding.BindingDigest, TargetBinding: targetBinding,
		Route: &route, State: RecoveryObjectStaged,
	}
}

func recoveryUpdate(record RecoveryObjectRecord) RecoveryObjectUpdate {
	return RecoveryObjectUpdate{
		Kind: record.Kind, Group: record.Group, RouteKey: record.RouteKey, ObjectID: record.ObjectID,
		RecoveryEpoch: record.RecoveryEpoch, ReportDigest: record.ReportDigest,
		SourceBindingDigest: record.SourceBindingDigest,
		TargetBindingDigest: record.TargetBinding.BindingDigest,
	}
}
