package raftstore

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/routeapi"
	sm "github.com/lni/dragonboat/v4/statemachine"
)

func TestSnapshotRejectsRouteChangeBeyondLastApplied(t *testing.T) {
	registryLayout := testRegistryLayout(4, "generation-future-change")
	identity := routeShardIdentity(t, registryLayout, "/future", "route")
	state := initializeDataShard(t, registryLayout, identity)
	state.LastApplied = 5
	bucket, _, err := clusterstate.RouteShardFor(
		"/future", "route", registryLayout.RouteBucketCount, registryLayout.VirtualShardCount,
	)
	if err != nil {
		t.Fatal(err)
	}
	change := RouteChange{Revision: 6, Bucket: bucket, Group: "/future", RouteKey: "route", State: clusterstate.WorkflowRouteStarting}
	value, err := json.Marshal(change)
	if err != nil {
		t.Fatal(err)
	}
	revision := make([]byte, 8)
	binary.BigEndian.PutUint64(revision, change.Revision)
	machine := &diskStateMachine{shardID: DataRaftShardID(identity.ShardID)}
	metadataSeen := true
	if err := machine.validateSnapshotRecord(
		append([]byte{stateRouteChangeTable}, revision...), value, state.LastApplied, &metadataSeen, &state,
	); err == nil {
		t.Fatal("snapshot accepted a Route change beyond LastApplied")
	}
}

func TestPebblePlacementRetryLoadsCommittedFence(t *testing.T) {
	engine, err := OpenPebbleStateEngine(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	registryLayout := testRegistryLayout(4, "generation-placement-retry")
	group, routeKey := "/g", "rk-placement-retry"
	identity := routeShardIdentity(t, registryLayout, group, routeKey)
	machine := engine.NewStateMachine(DataRaftShardID(identity.ShardID), 1)
	if _, err := machine.Open(make(chan struct{})); err != nil {
		t.Fatal(err)
	}
	bootstrap, _ := NewDataShardBootstrap(registryLayout, identity.ShardID)
	applyDiskData(t, machine, 1, DataCommand{
		Type: DataInitializeShard, Identity: identity, Bootstrap: &bootstrap,
		ReplicaIDs: append([]uint64(nil), bootstrap.ReplicaIDs...),
	})
	starting := routeStarting(t, registryLayout, group, routeKey, "sandbox-1", 1, false)
	starting.Starting.DefinitivelyRejected = []uint32{0, 1}
	applyDiskData(t, machine, 2, DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{Absent: true}, Route: &starting,
	})
	failure := clusterstate.RoutePlacementFailureState{
		SandboxID: starting.Starting.SandboxID, PlacementRound: starting.Starting.PlacementRound,
		CandidatePool:        append([]clusterstate.PlacementCandidate(nil), starting.Starting.CandidatePool...),
		DefinitivelyRejected: append([]uint32(nil), starting.Starting.DefinitivelyRejected...),
		Intent:               starting.Starting.Intent, Reason: "placement candidate pool exhausted",
	}
	tombstone := clusterstate.RouteWorkflowRecord{
		Group: group, RouteKey: routeKey, State: clusterstate.WorkflowRouteTombstone,
		Tombstone: &clusterstate.RouteTombstoneState{PlacementFailure: &failure},
	}
	applyDiskData(t, machine, 3, DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{LogIndex: 2}, Route: &tombstone,
	})
	fence, err := clusterstate.NewPlacementFailureFence(group, routeKey, registryLayout.RegistryGeneration, failure)
	if err != nil {
		t.Fatal(err)
	}
	applyDiskData(t, machine, 4, DataCommand{
		Type: DataPutFence, Identity: identity, Expect: RevisionExpectation{Absent: true}, Fence: &fence,
	})
	next := routeStarting(t, registryLayout, group, routeKey, "sandbox-2", 2, false)
	applyDiskData(t, machine, 5, DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{LogIndex: 3}, Route: &next,
	})
}

func TestPendingDiskScanSeeksFromQualifiedCursor(t *testing.T) {
	prefix := []byte("slot/")
	buildPrefix := stateTablePrefix(prefix, stateBuildTable)
	routePrefix := stateTablePrefix(prefix, stateRouteTable)

	lower, scan := pendingTableLowerBound(prefix, buildPrefix, stateBuildTable, 'b', "bbuild-key")
	want := stateRowKey(prefix, stateBuildTable, "build-key")
	if !scan || !bytes.Equal(lower, want) {
		t.Fatalf("Build lower bound = %q scan=%t, want %q", lower, scan, want)
	}
	if _, scan := pendingTableLowerBound(prefix, buildPrefix, stateBuildTable, 'b', "rroute-key"); scan {
		t.Fatal("Build table was rescanned after the cursor entered the Route table")
	}
	lower, scan = pendingTableLowerBound(prefix, routePrefix, stateRouteTable, 'r', "bbuild-key")
	if !scan || !bytes.Equal(lower, routePrefix) {
		t.Fatalf("Route lower bound after Build cursor = %q scan=%t", lower, scan)
	}
	lower, scan = pendingTableLowerBound(prefix, routePrefix, stateRouteTable, 'r', "rroute-key")
	want = stateRowKey(prefix, stateRouteTable, "route-key")
	if !scan || !bytes.Equal(lower, want) {
		t.Fatalf("Route lower bound = %q scan=%t, want %q", lower, scan, want)
	}
}

func TestPebbleStateMachinePersistsAndRestoresAcrossReplicaIDs(t *testing.T) {
	root := t.TempDir()
	engine, err := OpenPebbleStateEngine(root)
	if err != nil {
		t.Fatal(err)
	}
	registryLayout := testRegistryLayout(4, "generation-disk")
	identity := routeShardIdentity(t, registryLayout, "/disk", "ready")
	shardID := DataRaftShardID(identity.ShardID)
	machine := engine.NewStateMachine(shardID, 1)
	if index, err := machine.Open(make(chan struct{})); err != nil || index != 0 {
		t.Fatalf("initial Open = %d, %v", index, err)
	}
	bootstrap, err := NewDataShardBootstrap(registryLayout, identity.ShardID)
	if err != nil {
		t.Fatal(err)
	}
	applyDiskData(t, machine, 1, DataCommand{
		Type: DataInitializeShard, Identity: identity, Bootstrap: &bootstrap,
		ReplicaIDs: append([]uint64(nil), bootstrap.ReplicaIDs...),
	})
	starting := routeStarting(t, registryLayout, "/disk", "ready", "sandbox-disk", 1, true)
	applyDiskData(t, machine, 2, DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{Absent: true}, Route: &starting,
	})
	ready := readyRecord(starting, 1)
	applyDiskData(t, machine, 3, DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{LogIndex: 2}, Route: &ready,
	})
	assertDiskReady(t, machine, identity, "/disk", "ready")
	if err := machine.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := machine.Close(); err != nil {
		t.Fatal(err)
	}
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}

	engine, err = OpenPebbleStateEngine(root)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	machine = engine.NewStateMachine(shardID, 1)
	if index, err := machine.Open(make(chan struct{})); err != nil || index != 3 {
		t.Fatalf("restart Open = %d, %v", index, err)
	}
	assertDiskReady(t, machine, identity, "/disk", "ready")

	snapshotContext, err := machine.PrepareSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	var snapshot bytes.Buffer
	if err := machine.SaveSnapshot(snapshotContext, &snapshot, make(chan struct{})); err != nil {
		t.Fatal(err)
	}
	target := engine.NewStateMachine(shardID, 99)
	if index, err := target.Open(make(chan struct{})); err != nil || index != 0 {
		t.Fatalf("joining replica Open = %d, %v", index, err)
	}
	if err := target.RecoverFromSnapshot(bytes.NewReader(snapshot.Bytes()), make(chan struct{})); err != nil {
		t.Fatal(err)
	}
	assertDiskReady(t, target, identity, "/disk", "ready")

	corrupt := append([]byte(nil), snapshot.Bytes()[:snapshot.Len()-5]...)
	if err := target.RecoverFromSnapshot(bytes.NewReader(corrupt), make(chan struct{})); err == nil {
		t.Fatal("truncated snapshot replaced the active state slot")
	}
	assertDiskReady(t, target, identity, "/disk", "ready")
}

func TestPebbleSystemStateMachinePersistsPermitState(t *testing.T) {
	engine, err := OpenPebbleStateEngine(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	machine := engine.NewStateMachine(SystemRaftShardID, 1)
	if index, err := machine.Open(make(chan struct{})); err != nil || index != 0 {
		t.Fatalf("initial Open = %d, %v", index, err)
	}
	registryLayout := testRegistryLayout(4, "generation-system-disk")
	digest, _ := registryLayout.Digest()
	applyDiskSystem(t, machine, 1, SystemCommand{Type: SystemBootstrap, RegistryLayout: &registryLayout, Digest: digest})
	applyDiskSystem(t, machine, 2, SystemCommand{
		Type: SystemSetGates, Gates: &GateUpdate{Serve: true, Write: true, Cutover: true},
	})
	result := applyDiskSystem(t, machine, 3, SystemCommand{Type: SystemRefreshPermit})
	if result.PermitGrant == nil || !result.PermitGrant.ServeGate || !result.PermitGrant.WriteGate {
		t.Fatalf("persisted permit grant = %+v", result.PermitGrant)
	}
	value, err := machine.Lookup(SystemStateLookup{})
	if err != nil {
		t.Fatal(err)
	}
	state := value.(SystemState)
	if state.LastApplied != 3 || state.ActiveRegistryLayoutDigest != digest {
		t.Fatalf("persisted System state = %+v", state)
	}
	raw, err := EncodeSystemCommand(SystemCommand{Type: SystemSetGates, Gates: &GateUpdate{Write: true}})
	if err != nil {
		t.Fatal(err)
	}
	entries, err := machine.Update([]sm.Entry{{Index: 4, Cmd: raw}})
	if err != nil {
		t.Fatal(err)
	}
	var conflict SystemApplyResult
	if err := json.Unmarshal(entries[0].Result.Data, &conflict); err != nil || !conflict.Conflict {
		t.Fatalf("conflicting System command = %+v, %v", conflict, err)
	}
	if err := machine.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := machine.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := engine.NewStateMachine(SystemRaftShardID, 1)
	if index, err := reopened.Open(make(chan struct{})); err != nil || index != 4 {
		t.Fatalf("Open after conflict = %d, %v", index, err)
	}
}

func TestPebbleInitialBootstrapClearsUncommittedSnapshotSlot(t *testing.T) {
	engine, err := OpenPebbleStateEngine(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	registryLayout := testRegistryLayout(4, "generation-clean-slot")
	identity := routeShardIdentity(t, registryLayout, "/disk", "missing")
	shardID := DataRaftShardID(identity.ShardID)
	staleKey := stateRowKey(stateSlotPrefix(shardID, 1, 1), stateRouteTable, routeMapKey("/stale", "route"))
	if err := engine.db.Set(staleKey, []byte(`{"partial":true}`), nil); err != nil {
		t.Fatal(err)
	}
	machine := engine.NewStateMachine(shardID, 1)
	if index, err := machine.Open(make(chan struct{})); err != nil || index != 0 {
		t.Fatalf("Open with inactive residue = %d, %v", index, err)
	}
	bootstrap, _ := NewDataShardBootstrap(registryLayout, identity.ShardID)
	applyDiskData(t, machine, 1, DataCommand{
		Type: DataInitializeShard, Identity: identity, Bootstrap: &bootstrap,
		ReplicaIDs: append([]uint64(nil), bootstrap.ReplicaIDs...),
	})
	if _, found, err := getStateValue(engine.db, staleKey); err != nil || found {
		t.Fatalf("inactive snapshot residue remains after bootstrap: found=%v err=%v", found, err)
	}
}

func TestPebbleRouteChangefeedCompactionForcesReset(t *testing.T) {
	engine, err := OpenPebbleStateEngine(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	registryLayout := testRegistryLayout(4, "generation-changefeed")
	group, routeKey := "/changefeed", "route"
	identity := routeShardIdentity(t, registryLayout, group, routeKey)
	machine := engine.NewStateMachine(DataRaftShardID(identity.ShardID), 1)
	if _, err := machine.Open(make(chan struct{})); err != nil {
		t.Fatal(err)
	}
	bootstrap, _ := NewDataShardBootstrap(registryLayout, identity.ShardID)
	applyDiskData(t, machine, 1, DataCommand{
		Type: DataInitializeShard, Identity: identity, Bootstrap: &bootstrap,
		ReplicaIDs: append([]uint64(nil), bootstrap.ReplicaIDs...),
	})
	starting := routeStarting(t, registryLayout, group, routeKey, "sandbox-changefeed", 1, true)
	applyDiskData(t, machine, 2, DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{Absent: true}, Route: &starting,
	})
	raw, err := EncodeDataCommand(DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{LogIndex: 999}, Route: &starting,
	})
	if err != nil {
		t.Fatal(err)
	}
	entries, err := machine.Update([]sm.Entry{{Index: RouteChangefeedRetentionRevisions + 2, Cmd: raw}})
	if err != nil {
		t.Fatal(err)
	}
	var conflict DataApplyResult
	if err := json.Unmarshal(entries[0].Result.Data, &conflict); err != nil || !conflict.Conflict {
		t.Fatalf("compaction-driving conflict = %+v, %v", conflict, err)
	}
	bucket, _, _ := clusterstate.RouteShardFor(group, routeKey, registryLayout.RouteBucketCount, registryLayout.VirtualShardCount)
	value, err := machine.Lookup(DataLookup{Changefeed: &RouteChangefeedLookup{
		Identity: identity, Group: group, Bucket: bucket, AfterRevision: 1, Limit: 10,
	}})
	if err != nil {
		t.Fatal(err)
	}
	changefeed := value.(DataLookupResult).Changefeed
	if changefeed == nil || !changefeed.Available || !changefeed.Reset ||
		changefeed.FloorRevision != 2 || changefeed.HeadRevision != RouteChangefeedRetentionRevisions+2 {
		t.Fatalf("compacted on-disk Route changefeed = %+v", changefeed)
	}
}

func TestPebbleStateMachineAppliesOneDragonboatBatchAtomically(t *testing.T) {
	engine, err := OpenPebbleStateEngine(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	registryLayout := testRegistryLayout(4, "generation-batch")
	identity := routeShardIdentity(t, registryLayout, "/batch", "ready")
	machine := engine.NewStateMachine(DataRaftShardID(identity.ShardID), 1)
	if _, err := machine.Open(make(chan struct{})); err != nil {
		t.Fatal(err)
	}
	bootstrap, _ := NewDataShardBootstrap(registryLayout, identity.ShardID)
	starting := routeStarting(t, registryLayout, "/batch", "ready", "sandbox-batch", 1, true)
	ready := readyRecord(starting, 1)
	commands := []DataCommand{
		{Type: DataInitializeShard, Identity: identity, Bootstrap: &bootstrap, ReplicaIDs: bootstrap.ReplicaIDs},
		{Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{Absent: true}, Route: &starting},
		{Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{LogIndex: 2}, Route: &ready},
	}
	entries := make([]sm.Entry, len(commands))
	for index, command := range commands {
		raw, err := EncodeDataCommand(command)
		if err != nil {
			t.Fatal(err)
		}
		entries[index] = sm.Entry{Index: uint64(index + 1), Cmd: raw}
	}
	entries, err = machine.Update(entries)
	if err != nil {
		t.Fatal(err)
	}
	for index, entry := range entries {
		var result DataApplyResult
		if err := json.Unmarshal(entry.Result.Data, &result); err != nil || !result.Applied {
			t.Fatalf("batched entry %d = %+v, %v", index, result, err)
		}
	}
	assertDiskReady(t, machine, identity, "/batch", "ready")
}

func TestPebbleSnapshotRecoverySpansMultipleSyncedBatches(t *testing.T) {
	engine, err := OpenPebbleStateEngine(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	registryLayout := testRegistryLayout(4, "generation-large-snapshot")
	group := "/large-snapshot"
	identity := routeShardIdentity(t, registryLayout, group, "seed")
	shardID := DataRaftShardID(identity.ShardID)
	machine := engine.NewStateMachine(shardID, 1)
	if _, err := machine.Open(make(chan struct{})); err != nil {
		t.Fatal(err)
	}
	bootstrap, _ := NewDataShardBootstrap(registryLayout, identity.ShardID)
	applyDiskData(t, machine, 1, DataCommand{
		Type: DataInitializeShard, Identity: identity, Bootstrap: &bootstrap,
		ReplicaIDs: append([]uint64(nil), bootstrap.ReplicaIDs...),
	})
	baseIntent := testDispatchIntent(t)
	largeSpec, err := clusterstate.ParseSandboxDispatchSpec(baseIntent.DispatchSpec)
	if err != nil {
		t.Fatal(err)
	}
	largeSpec.RequestedConfig = map[string]string{"padding": strings.Repeat("x", clusterstate.MaxDispatchSpecBytes-1024)}
	encodedSpec, err := clusterstate.MarshalSandboxDispatchSpec(largeSpec)
	if err != nil {
		t.Fatal(err)
	}
	intent, err := clusterstate.NewDispatchIntent(
		baseIntent.NormalizedDemand, encodedSpec, baseIntent.ProviderPolicyVersion,
	)
	if err != nil {
		t.Fatal(err)
	}

	const routeCount = 300
	entries := make([]sm.Entry, 0, routeCount)
	routeKeys := make([]string, 0, routeCount)
	for candidate := 0; len(entries) < routeCount; candidate++ {
		routeKey := fmt.Sprintf("route-%06d", candidate)
		_, candidateShard, err := clusterstate.RouteShardFor(
			group, routeKey, registryLayout.RouteBucketCount, registryLayout.VirtualShardCount,
		)
		if err != nil {
			t.Fatal(err)
		}
		if candidateShard != identity.ShardID {
			continue
		}
		starting := routeStarting(t, registryLayout, group, routeKey, fmt.Sprintf("sandbox-%06d", candidate), 1, false)
		starting.Starting.Intent = intent
		selected := uint32(0)
		binding := testBinding(
			t, registryLayout, clusterstate.ExecutionKindSandbox, starting.Starting.SandboxID,
			group, routeKey, starting.Starting.CandidatePool[0].NodeID, intent,
		)
		starting.Starting.SelectedCandidate = &selected
		starting.Starting.Binding = &binding
		raw, err := EncodeDataCommand(DataCommand{
			Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{Absent: true}, Route: &starting,
		})
		if err != nil {
			t.Fatal(err)
		}
		entries = append(entries, sm.Entry{Index: uint64(len(entries) + 2), Cmd: raw})
		routeKeys = append(routeKeys, routeKey)
	}
	entries, err = machine.Update(entries)
	if err != nil {
		t.Fatal(err)
	}
	for index, entry := range entries {
		var result DataApplyResult
		if err := json.Unmarshal(entry.Result.Data, &result); err != nil || !result.Applied {
			t.Fatalf("large snapshot row %d = %+v, %v", index, result, err)
		}
	}
	if err := machine.Sync(); err != nil {
		t.Fatal(err)
	}

	snapshotContext, err := machine.PrepareSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	var snapshot bytes.Buffer
	if err := machine.SaveSnapshot(snapshotContext, &snapshot, make(chan struct{})); err != nil {
		t.Fatal(err)
	}
	if snapshot.Len() <= stateRecoveryBatch {
		t.Fatalf("snapshot size = %d, want more than one %d-byte recovery batch", snapshot.Len(), stateRecoveryBatch)
	}
	target := engine.NewStateMachine(shardID, 2)
	if _, err := target.Open(make(chan struct{})); err != nil {
		t.Fatal(err)
	}
	if err := target.RecoverFromSnapshot(bytes.NewReader(snapshot.Bytes()), make(chan struct{})); err != nil {
		t.Fatal(err)
	}
	after, recovered := "", 0
	for {
		value, err := target.Lookup(DataLookup{Pending: &PendingLookup{
			Identity: identity, AfterKey: after, Limit: routeCount + 1,
		}})
		if err != nil {
			t.Fatal(err)
		}
		pending := value.(DataLookupResult).Pending
		if pending == nil || len(pending.Workflows) == 0 {
			t.Fatalf("recovered pending workflows = %+v", pending)
		}
		encoded, err := json.Marshal(pending)
		if err != nil || len(encoded) > MaxPendingLookupResponseBytes {
			t.Fatalf("pending page bytes = %d, err=%v", len(encoded), err)
		}
		for _, workflow := range pending.Workflows {
			if workflow.Route == nil {
				t.Fatal("recovered snapshot returned a non-Route workflow")
			}
		}
		recovered += len(pending.Workflows)
		if pending.NextKey == "" {
			break
		}
		after = pending.NextKey
	}
	if recovered != routeCount || len(routeKeys) != routeCount {
		t.Fatalf("recovered %d pending workflows, want %d", recovered, routeCount)
	}
}

func applyDiskSystem(t *testing.T, machine sm.IOnDiskStateMachine, index uint64, command SystemCommand) SystemApplyResult {
	t.Helper()
	raw, err := EncodeSystemCommand(command)
	if err != nil {
		t.Fatal(err)
	}
	entries, err := machine.Update([]sm.Entry{{Index: index, Cmd: raw}})
	if err != nil {
		t.Fatal(err)
	}
	var result SystemApplyResult
	if err := json.Unmarshal(entries[0].Result.Data, &result); err != nil {
		t.Fatal(err)
	}
	if !result.Applied || result.Conflict {
		t.Fatalf("System command %s at %d = %+v", command.Type, index, result)
	}
	return result
}

func applyDiskData(t *testing.T, machine sm.IOnDiskStateMachine, index uint64, command DataCommand) DataApplyResult {
	t.Helper()
	raw, err := EncodeDataCommand(command)
	if err != nil {
		t.Fatal(err)
	}
	entries, err := machine.Update([]sm.Entry{{Index: index, Cmd: raw}})
	if err != nil {
		t.Fatal(err)
	}
	var result DataApplyResult
	if err := json.Unmarshal(entries[0].Result.Data, &result); err != nil {
		t.Fatal(err)
	}
	if !result.Applied || result.Conflict {
		t.Fatalf("data command %s at %d = %+v", command.Type, index, result)
	}
	return result
}

func assertDiskReady(
	t *testing.T,
	machine sm.IOnDiskStateMachine,
	identity ShardRequestIdentity,
	group string,
	routeKey string,
) {
	t.Helper()
	request := routeapi.ReadRouteRequest{
		RequestIdentity: routeIdentity(identity), Group: group, RouteKey: routeKey,
	}
	value, err := machine.Lookup(DataLookup{Route: &request})
	if err != nil {
		t.Fatal(err)
	}
	result := value.(DataLookupResult)
	if result.Route == nil || result.Route.Outcome != routeapi.ReadReady || result.Route.RouteRevision != 3 {
		t.Fatalf("on-disk READY lookup = %+v", result.Route)
	}
	value, err = machine.Lookup(DataLookup{Workflow: &WorkflowLookup{
		Identity: identity, Group: group, RouteKey: routeKey,
	}})
	if err != nil {
		t.Fatal(err)
	}
	workflow := value.(DataLookupResult).Workflow
	if workflow == nil || !workflow.Available || workflow.Route == nil ||
		workflow.Route.State != clusterstate.WorkflowRouteReady || workflow.Route.Revision.LogIndex != 3 {
		t.Fatalf("on-disk workflow lookup = %+v", workflow)
	}
	bucket, _, err := clusterstate.RouteShardFor(group, routeKey, 16, 4)
	if err != nil {
		t.Fatal(err)
	}
	value, err = machine.Lookup(DataLookup{RouteBucket: &RouteBucketLookup{
		Identity: identity, Group: group, Bucket: bucket, Limit: 10,
	}})
	if err != nil {
		t.Fatal(err)
	}
	bucketResult := value.(DataLookupResult).RouteBucket
	if bucketResult == nil || !bucketResult.Available || bucketResult.SnapshotRevision != 3 ||
		len(bucketResult.Routes) != 1 || bucketResult.Routes[0].RouteKey != routeKey {
		t.Fatalf("on-disk Route bucket lookup = %+v", bucketResult)
	}
	value, err = machine.Lookup(DataLookup{Changefeed: &RouteChangefeedLookup{
		Identity: identity, Group: group, Bucket: bucket, AfterRevision: 1, Limit: 10,
	}})
	if err != nil {
		t.Fatal(err)
	}
	changefeed := value.(DataLookupResult).Changefeed
	if changefeed == nil || !changefeed.Available || changefeed.Reset || changefeed.CursorRevision != 3 ||
		len(changefeed.Changes) != 2 || changefeed.Changes[0].Revision != 2 || changefeed.Changes[1].Revision != 3 {
		t.Fatalf("on-disk Route changefeed = %+v", changefeed)
	}
}
