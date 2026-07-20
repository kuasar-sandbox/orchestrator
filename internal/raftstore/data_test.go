package raftstore

import (
	"crypto/sha256"
	"testing"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
)

func TestDataShardBootstrapRequiresExactRegistryLayoutIdentity(t *testing.T) {
	registryLayout := testRegistryLayout(4, "generation-1")
	identity := routeShardIdentity(t, registryLayout, "/g", "rk")
	replicas := replicaIDsForPlacement(registryLayout.DataShards[identity.ShardID])
	bootstrap, _ := NewDataShardBootstrap(registryLayout, identity.ShardID)
	state := DataState{}

	wrong := identity
	wrong.RegistryLayoutDigest = digestFor("another-registryLayout")
	result := ApplyDataCommand(&state, 1, DataCommand{
		Type: DataInitializeShard, Identity: wrong, Bootstrap: &bootstrap, ReplicaIDs: replicas,
	})
	if !result.Conflict || state.Initialized || state.LastApplied != 0 {
		t.Fatalf("wrong registryLayout bootstrap = %+v, state=%+v", result, state)
	}

	result = ApplyDataCommand(&state, 2, DataCommand{
		Type: DataInitializeShard, Identity: identity, Bootstrap: &bootstrap, ReplicaIDs: replicas,
	})
	if !result.Applied || state.LastApplied != 2 || !state.Accepts(identity) {
		t.Fatalf("exact bootstrap = %+v, state=%+v", result, state)
	}
	if err := state.Validate(); err != nil {
		t.Fatal(err)
	}

	corrupt := state
	corrupt.Routes = map[string]clusterstate.RouteWorkflowRecord{"wrong-key": routeStarting(t, registryLayout, "/g", "rk", "sandbox-1", 1, false)}
	corrupt.Routes["wrong-key"] = withRevision(corrupt.Routes["wrong-key"], corrupt, 2)
	if err := corrupt.Validate(); err == nil {
		t.Fatal("snapshot validation accepted a Route under the wrong map key")
	}
}

func TestDataCASConflictAdvancesAppliedIndexWithoutChangingRow(t *testing.T) {
	registryLayout := testRegistryLayout(4, "generation-1")
	state, identity := initializedRouteShard(t, registryLayout, "/g", "rk")
	starting := routeStarting(t, registryLayout, "/g", "rk", "sandbox-1", 1, false)
	applyDataOK(t, &state, 2, DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{Absent: true}, Route: &starting,
	})

	starting.Starting.CandidatePool[0].NodeID = "caller-mutated"
	stored := state.Routes[routeMapKey("/g", "rk")]
	if stored.Starting.CandidatePool[0].NodeID == "caller-mutated" {
		t.Fatal("stored Route aliases command-owned slices")
	}

	update := cloneRouteRecord(stored)
	result := ApplyDataCommand(&state, 3, DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{LogIndex: 99}, Route: &update,
	})
	stored = state.Routes[routeMapKey("/g", "rk")]
	if !result.Conflict || result.CurrentRevision != 2 || state.LastApplied != 3 || stored.Revision.LogIndex != 2 {
		t.Fatalf("stale CAS = %+v, applied=%d, row=%+v", result, state.LastApplied, stored.Revision)
	}

	selected := routeStarting(t, registryLayout, "/g", "rk", "sandbox-1", 1, true)
	applyDataOK(t, &state, 4, DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{LogIndex: 2}, Route: &selected,
	})
	if got := state.Routes[routeMapKey("/g", "rk")].Revision.LogIndex; got != 4 {
		t.Fatalf("Route revision = %d, want committed log index 4", got)
	}
}

func TestStartingCandidateRejectionAndSandboxRoundAdvance(t *testing.T) {
	registryLayout := testRegistryLayout(4, "generation-1")
	state, identity := initializedRouteShard(t, registryLayout, "/g", "rk")
	selected := routeStarting(t, registryLayout, "/g", "rk", "sandbox-1", 1, true)
	applyDataOK(t, &state, 2, DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{Absent: true}, Route: &selected,
	})

	rejectedFirst := cloneRouteRecord(state.Routes[routeMapKey("/g", "rk")])
	rejectedFirst.Starting.SelectedCandidate = nil
	rejectedFirst.Starting.Binding = nil
	rejectedFirst.Starting.DefinitivelyRejected = []uint32{0}
	applyDataOK(t, &state, 3, DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{LogIndex: 2}, Route: &rejectedFirst,
	})

	selectedSecond := cloneRouteRecord(state.Routes[routeMapKey("/g", "rk")])
	second := uint32(1)
	binding := testBinding(t, registryLayout, clusterstate.ExecutionKindSandbox, "sandbox-1", "/g", "rk", "node-2", selectedSecond.Starting.Intent)
	selectedSecond.Starting.SelectedCandidate = &second
	selectedSecond.Starting.Binding = &binding
	applyDataOK(t, &state, 4, DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{LogIndex: 3}, Route: &selectedSecond,
	})

	exhausted := cloneRouteRecord(state.Routes[routeMapKey("/g", "rk")])
	exhausted.Starting.SelectedCandidate = nil
	exhausted.Starting.Binding = nil
	exhausted.Starting.DefinitivelyRejected = []uint32{0, 1}
	applyDataOK(t, &state, 5, DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{LogIndex: 4}, Route: &exhausted,
	})

	nextRound := routeStarting(t, registryLayout, "/g", "rk", "sandbox-2", 2, false)
	applyDataOK(t, &state, 6, DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{LogIndex: 5}, Route: &nextRound,
	})

	skipped := cloneRouteRecord(state.Routes[routeMapKey("/g", "rk")])
	skipped.Starting.DefinitivelyRejected = []uint32{0}
	result := ApplyDataCommand(&state, 7, DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{LogIndex: 6}, Route: &skipped,
	})
	if !result.Conflict {
		t.Fatal("unselected candidate was marked rejected without a dispatch result")
	}
}

func TestBuildStartingCommitsDefinitiveCandidateRejection(t *testing.T) {
	registryLayout := testRegistryLayout(4, "generation-1")
	state, identity := initializedBuildShard(t, registryLayout, "/g", "build-reject")
	selected := buildStarting(t, registryLayout, "/g", "build-reject", true)
	applyDataOK(t, &state, 2, DataCommand{
		Type: DataPutBuild, Identity: identity, Expect: RevisionExpectation{Absent: true}, Build: &selected,
	})
	rejected := cloneBuildRecord(state.Builds[buildMapKey("/g", "build-reject")])
	rejected.Starting.SelectedCandidate = nil
	rejected.Starting.Binding = nil
	rejected.Starting.DefinitivelyRejected = []uint32{0}
	applyDataOK(t, &state, 3, DataCommand{
		Type: DataPutBuild, Identity: identity, Expect: RevisionExpectation{LogIndex: 2}, Build: &rejected,
	})
}

func TestRouteAutoResumeReplacementAndFenceCompaction(t *testing.T) {
	registryLayout := testRegistryLayout(4, "generation-1")
	state, identity := initializedRouteShard(t, registryLayout, "/g", "rk")
	starting := routeStarting(t, registryLayout, "/g", "rk", "sandbox-1", 1, true)
	applyDataOK(t, &state, 2, DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{Absent: true}, Route: &starting,
	})

	ready := readyRecord(starting, 1)
	applyDataOK(t, &state, 3, DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{LogIndex: 2}, Route: &ready,
	})
	paused := pausedRecord(ready, 2)
	applyDataOK(t, &state, 4, DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{LogIndex: 3}, Route: &paused,
	})

	wrongResume := readyRecord(starting, 3)
	wrongResume.Ready.NodeEpoch++
	result := ApplyDataCommand(&state, 5, DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{LogIndex: 4}, Route: &wrongResume,
	})
	if !result.Conflict || state.LastApplied != 5 || state.Routes[routeMapKey("/g", "rk")].State != clusterstate.WorkflowRoutePaused {
		t.Fatalf("wrong auto-resume = %+v", result)
	}

	resumed := readyRecord(starting, 3)
	applyDataOK(t, &state, 6, DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{LogIndex: 4}, Route: &resumed,
	})
	deleting := deletingRecord(resumed)
	applyDataOK(t, &state, 7, DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{LogIndex: 6}, Route: &deleting,
	})
	tombstone, fence := terminalRouteAndFence(deleting, 4)
	applyDataOK(t, &state, 8, DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{LogIndex: 7}, Route: &tombstone,
	})

	replacement := routeStarting(t, registryLayout, "/g", "rk", "sandbox-2", 2, false)
	result = ApplyDataCommand(&state, 9, DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{LogIndex: 8}, Route: &replacement,
	})
	if !result.Conflict {
		t.Fatal("replacement was committed without an old-execution fence")
	}

	applyDataOK(t, &state, 10, DataCommand{
		Type: DataPutFence, Identity: identity, Expect: RevisionExpectation{Absent: true}, Fence: &fence,
	})
	applyDataOK(t, &state, 11, DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{LogIndex: 8}, Route: &replacement,
	})

	fenceKey := fenceMapKey("/g", "rk", "sandbox-1")
	storedFence := state.Fences[fenceKey]
	incomplete := fenceCompaction(storedFence, state.ReplicaIDs[:2])
	result = ApplyDataCommand(&state, 12, DataCommand{
		Type: DataCompactFence, Identity: identity, Compaction: &incomplete,
	})
	if !result.Conflict || state.LastApplied != 12 {
		t.Fatalf("incomplete fence compaction = %+v", result)
	}
	if _, found := state.Fences[fenceKey]; !found {
		t.Fatal("incomplete proof compacted the execution fence")
	}

	complete := fenceCompaction(storedFence, state.ReplicaIDs)
	applyDataOK(t, &state, 13, DataCommand{Type: DataCompactFence, Identity: identity, Compaction: &complete})
	if _, found := state.Fences[fenceKey]; found {
		t.Fatal("complete proof did not compact the execution fence")
	}
	if err := state.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestBuildStateProgressionRejectsSkippedAndRepeatedEvents(t *testing.T) {
	registryLayout := testRegistryLayout(4, "generation-1")
	state, identity := initializedBuildShard(t, registryLayout, "/g", "build-1")
	starting := buildStarting(t, registryLayout, "/g", "build-1", true)
	applyDataOK(t, &state, 2, DataCommand{
		Type: DataPutBuild, Identity: identity, Expect: RevisionExpectation{Absent: true}, Build: &starting,
	})
	queued := buildProjectionRecord(starting, clusterstate.BuildQueued, 1)
	applyDataOK(t, &state, 3, DataCommand{
		Type: DataPutBuild, Identity: identity, Expect: RevisionExpectation{LogIndex: 2}, Build: &queued,
	})

	ready := buildProjectionRecord(starting, clusterstate.BuildReady, 2)
	ready.Projection.ArtifactRef = "artifact://build-1"
	result := ApplyDataCommand(&state, 4, DataCommand{
		Type: DataPutBuild, Identity: identity, Expect: RevisionExpectation{LogIndex: 3}, Build: &ready,
	})
	if !result.Conflict {
		t.Fatal("BUILD_QUEUED skipped directly to BUILD_READY")
	}

	registered := buildProjectionRecord(starting, clusterstate.BuildRegistered, 2)
	applyDataOK(t, &state, 5, DataCommand{
		Type: DataPutBuild, Identity: identity, Expect: RevisionExpectation{LogIndex: 3}, Build: &registered,
	})
	building := buildProjectionRecord(starting, clusterstate.BuildBuilding, 2)
	result = ApplyDataCommand(&state, 6, DataCommand{
		Type: DataPutBuild, Identity: identity, Expect: RevisionExpectation{LogIndex: 5}, Build: &building,
	})
	if !result.Conflict {
		t.Fatal("a repeated event sequence advanced the Build state")
	}
	building.Projection.LastEventSeq = 3
	applyDataOK(t, &state, 7, DataCommand{
		Type: DataPutBuild, Identity: identity, Expect: RevisionExpectation{LogIndex: 5}, Build: &building,
	})
	ready = buildProjectionRecord(starting, clusterstate.BuildReady, 4)
	ready.Projection.ArtifactRef = "artifact://build-1"
	applyDataOK(t, &state, 8, DataCommand{
		Type: DataPutBuild, Identity: identity, Expect: RevisionExpectation{LogIndex: 7}, Build: &ready,
	})
	if err := state.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestDataServingEpochTransitionPersistsBothReplicaSets(t *testing.T) {
	registryLayout := testRegistryLayout(4, "generation-1")
	state, identity := initializedRouteShard(t, registryLayout, "/g", "rk")
	next := identity.PermitIdentity
	next.SystemEpoch++
	next.RegistryLayoutDigest = digestFor("registryLayout-next")
	nextReplicas := []uint64{2, 3, 4}
	applyDataOK(t, &state, 2, DataCommand{
		Type: DataPrepareEpoch, Identity: identity, Epoch: &next, ReplicaIDs: nextReplicas,
	})
	nextIdentity := ShardRequestIdentity{PermitIdentity: next, ShardID: identity.ShardID}
	if !state.Accepts(identity) || !state.Accepts(nextIdentity) {
		t.Fatal("prepared transition did not accept both committed serving epochs")
	}
	if got := compactionReplicaIDs(state); len(got) != 4 || got[0] != 1 || got[3] != 4 {
		t.Fatalf("transition compaction replicas = %+v", got)
	}
	old := identity.PermitIdentity
	applyDataOK(t, &state, 3, DataCommand{
		Type: DataRetireEpoch, Identity: nextIdentity, Epoch: &old,
	})
	if state.Accepts(identity) || !state.Accepts(nextIdentity) || len(state.PreparedReplicaIDs) != 0 {
		t.Fatalf("retired epoch state = %+v", state)
	}
	if got := state.ReplicaIDs; len(got) != 3 || got[0] != 2 || got[2] != 4 {
		t.Fatalf("active replicas after retirement = %+v", got)
	}
}

func TestDataServingEpochCanAdvanceWithinOneRegistryLayoutForRecovery(t *testing.T) {
	registryLayout := testRegistryLayout(4, "generation-1")
	state, identity := initializedRouteShard(t, registryLayout, "/g", "rk")
	recovery := identity.PermitIdentity
	recovery.SystemEpoch++
	replicas := append([]uint64(nil), state.ReplicaIDs...)
	applyDataOK(t, &state, 2, DataCommand{
		Type: DataPrepareEpoch, Identity: identity, Epoch: &recovery, ReplicaIDs: replicas,
	})
	if len(state.ServingEpochs) != 2 || state.ServingEpochs[1].RegistryLayoutDigest != identity.RegistryLayoutDigest {
		t.Fatalf("recovery epoch was not prepared under the active registryLayout: %+v", state.ServingEpochs)
	}
	recoveryIdentity := ShardRequestIdentity{PermitIdentity: recovery, ShardID: identity.ShardID}
	previous := identity.PermitIdentity
	applyDataOK(t, &state, 3, DataCommand{
		Type: DataRetireEpoch, Identity: recoveryIdentity, Epoch: &previous,
	})
	if len(state.ServingEpochs) != 1 || state.ServingEpochs[0] != recovery {
		t.Fatalf("pre-recovery epoch remained active: %+v", state.ServingEpochs)
	}
}

func initializedRouteShard(t *testing.T, registryLayout RegistryLayout, group, routeKey string) (DataState, ShardRequestIdentity) {
	t.Helper()
	identity := routeShardIdentity(t, registryLayout, group, routeKey)
	return initializeDataShard(t, registryLayout, identity), identity
}

func initializedBuildShard(t *testing.T, registryLayout RegistryLayout, group, buildID string) (DataState, ShardRequestIdentity) {
	t.Helper()
	_, shardID, err := clusterstate.BuildShardFor(group, buildID, registryLayout.BuildBucketCount, registryLayout.VirtualShardCount)
	if err != nil {
		t.Fatal(err)
	}
	identity := registryLayoutShardIdentity(t, registryLayout, shardID)
	return initializeDataShard(t, registryLayout, identity), identity
}

func routeShardIdentity(t *testing.T, registryLayout RegistryLayout, group, routeKey string) ShardRequestIdentity {
	t.Helper()
	_, shardID, err := clusterstate.RouteShardFor(group, routeKey, registryLayout.RouteBucketCount, registryLayout.VirtualShardCount)
	if err != nil {
		t.Fatal(err)
	}
	return registryLayoutShardIdentity(t, registryLayout, shardID)
}

func registryLayoutShardIdentity(t *testing.T, registryLayout RegistryLayout, shardID uint32) ShardRequestIdentity {
	t.Helper()
	digest, err := registryLayout.Digest()
	if err != nil {
		t.Fatal(err)
	}
	return ShardRequestIdentity{PermitIdentity: PermitIdentity{
		ClusterID: registryLayout.ClusterID, RegistryGeneration: registryLayout.RegistryGeneration,
		SystemEpoch: 1, RegistryLayoutDigest: digest,
	}, ShardID: shardID}
}

func initializeDataShard(t *testing.T, registryLayout RegistryLayout, identity ShardRequestIdentity) DataState {
	t.Helper()
	state := DataState{}
	bootstrap, err := NewDataShardBootstrap(registryLayout, identity.ShardID)
	if err != nil {
		t.Fatal(err)
	}
	applyDataOK(t, &state, 1, DataCommand{
		Type: DataInitializeShard, Identity: identity, Bootstrap: &bootstrap,
		ReplicaIDs: replicaIDsForPlacement(registryLayout.DataShards[identity.ShardID]),
	})
	return state
}

func applyDataOK(t *testing.T, state *DataState, index uint64, command DataCommand) {
	t.Helper()
	result := ApplyDataCommand(state, index, command)
	if !result.Applied || result.Conflict || result.Revision != index {
		t.Fatalf("apply index %d command %s = %+v", index, command.Type, result)
	}
}

func routeStarting(t *testing.T, registryLayout RegistryLayout, group, routeKey, sandboxID string, round uint64, selected bool) clusterstate.RouteWorkflowRecord {
	t.Helper()
	intent := testDispatchIntent(t)
	candidates := []clusterstate.PlacementCandidate{{NodeID: "node-1"}, {NodeID: "node-2"}}
	starting := &clusterstate.RouteStartingState{
		SandboxID: sandboxID, PlacementRound: round, CandidatePool: candidates, Intent: intent,
	}
	if selected {
		index := uint32(0)
		binding := testBinding(t, registryLayout, clusterstate.ExecutionKindSandbox, sandboxID, group, routeKey, "node-1", intent)
		starting.SelectedCandidate, starting.Binding = &index, &binding
	}
	return clusterstate.RouteWorkflowRecord{Group: group, RouteKey: routeKey, State: clusterstate.WorkflowRouteStarting, Starting: starting}
}

func withRevision(record clusterstate.RouteWorkflowRecord, state DataState, index uint64) clusterstate.RouteWorkflowRecord {
	record.Revision = revisionFor(state, index)
	return record
}

func readyRecord(starting clusterstate.RouteWorkflowRecord, eventSeq uint64) clusterstate.RouteWorkflowRecord {
	binding := starting.Starting.Binding
	return clusterstate.RouteWorkflowRecord{
		Group: starting.Group, RouteKey: starting.RouteKey, State: clusterstate.WorkflowRouteReady,
		Ready: &clusterstate.ReadyRoute{
			SandboxID: starting.Starting.SandboxID, NodeID: binding.NodeID, NodeEpoch: binding.NodeEpoch,
			DataEndpoint: binding.DataEndpoint, TargetPort: 8080, AccessToken: "access-token",
			TemplateRef: "template-1", RegistryGeneration: binding.RegistryGeneration,
			BindingDigest: binding.BindingDigest, LastEventSeq: eventSeq,
		},
	}
}

func pausedRecord(ready clusterstate.RouteWorkflowRecord, eventSeq uint64) clusterstate.RouteWorkflowRecord {
	projection := *ready.Ready
	projection.LastEventSeq = eventSeq
	intent := testDispatchIntentNoFail()
	return clusterstate.RouteWorkflowRecord{
		Group: ready.Group, RouteKey: ready.RouteKey, State: clusterstate.WorkflowRoutePaused,
		Paused: &clusterstate.PausedRouteState{Execution: projection, SnapshotRef: "snapshot-1", ResumeIntent: intent},
	}
}

func deletingRecord(ready clusterstate.RouteWorkflowRecord) clusterstate.RouteWorkflowRecord {
	spec := []byte(`{"delete":true}`)
	digest := sha256.Sum256(spec)
	return clusterstate.RouteWorkflowRecord{
		Group: ready.Group, RouteKey: ready.RouteKey, State: clusterstate.WorkflowRouteDeleting,
		Deleting: &clusterstate.DeletingRouteState{
			Execution: *ready.Ready, DeleteSpec: spec, DeleteSpecDigest: hexDigestBytes(digest),
			LastEventSeq: ready.Ready.LastEventSeq,
		},
	}
}

func terminalRouteAndFence(deleting clusterstate.RouteWorkflowRecord, eventSeq uint64) (clusterstate.RouteWorkflowRecord, clusterstate.ExecutionFence) {
	execution := deleting.Deleting.Execution
	proof := clusterstate.TerminalProof{
		Kind: clusterstate.ProofNodeTerminal, ProofDigest: digestFor("terminal-sandbox-1"),
		FencedNodeID: execution.NodeID, FencedNodeEpoch: execution.NodeEpoch,
	}
	tombstone := clusterstate.RouteWorkflowRecord{
		Group: deleting.Group, RouteKey: deleting.RouteKey, State: clusterstate.WorkflowRouteTombstone,
		Tombstone: &clusterstate.RouteTombstoneState{
			SandboxID: execution.SandboxID, NodeID: execution.NodeID, NodeEpoch: execution.NodeEpoch,
			BindingDigest: execution.BindingDigest, LastEventSeq: eventSeq, Proof: proof, TerminalReason: "deleted",
		},
	}
	fence := clusterstate.ExecutionFence{
		Group: deleting.Group, RouteKey: deleting.RouteKey, SandboxID: execution.SandboxID,
		NodeID: execution.NodeID, NodeEpoch: execution.NodeEpoch, RegistryGeneration: execution.RegistryGeneration,
		BindingDigest: execution.BindingDigest, LastEventSeq: eventSeq, FinalOutboxWatermark: eventSeq, Proof: proof,
	}
	return tombstone, fence
}

func fenceCompaction(fence clusterstate.ExecutionFence, replicas []uint64) FenceCompactionAuthorization {
	proofs := make([]ReplicaAppliedProof, len(replicas))
	for index, replicaID := range replicas {
		proofs[index] = ReplicaAppliedProof{ReplicaID: replicaID, AppliedIndex: fence.Revision.LogIndex + 1}
	}
	return FenceCompactionAuthorization{
		Group: fence.Group, RouteKey: fence.RouteKey, SandboxID: fence.SandboxID,
		FenceRevision: fence.Revision.LogIndex, TerminalProofDigest: fence.Proof.ProofDigest,
		FinalOutboxWatermarkAcked: true, ReplicaApplied: proofs, RetentionProofDigest: digestFor("retention-window"),
	}
}

func buildStarting(t *testing.T, registryLayout RegistryLayout, group, buildID string, selected bool) clusterstate.BuildRecord {
	t.Helper()
	intent := testDispatchIntent(t)
	starting := &clusterstate.BuildStartingState{
		BuildID: buildID, CandidatePool: []clusterstate.PlacementCandidate{{NodeID: "node-1"}, {NodeID: "node-2"}}, Intent: intent,
	}
	if selected {
		index := uint32(0)
		binding := testBinding(t, registryLayout, clusterstate.ExecutionKindBuild, buildID, group, "", "node-1", intent)
		starting.SelectedCandidate, starting.Binding = &index, &binding
	}
	return clusterstate.BuildRecord{Group: group, BuildID: buildID, State: clusterstate.BuildStarting, Starting: starting}
}

func buildProjectionRecord(starting clusterstate.BuildRecord, state clusterstate.BuildWorkflowState, eventSeq uint64) clusterstate.BuildRecord {
	binding := starting.Starting.Binding
	return clusterstate.BuildRecord{
		Group: starting.Group, BuildID: starting.BuildID, State: state,
		Projection: &clusterstate.BuildProjection{
			BuildID: starting.BuildID, NodeID: binding.NodeID, NodeEpoch: binding.NodeEpoch,
			RegistryGeneration: binding.RegistryGeneration, BindingDigest: binding.BindingDigest,
			TemplateRef: "template-1", LastEventSeq: eventSeq,
		},
	}
}

func testDispatchIntent(t *testing.T) clusterstate.DispatchIntent {
	t.Helper()
	intent, err := clusterstate.NewDispatchIntent([]byte(`{"slot_units":1}`), []byte(`{"template":"template-1"}`), "provider-v1/policy-v1")
	if err != nil {
		t.Fatal(err)
	}
	return intent
}

func testDispatchIntentNoFail() clusterstate.DispatchIntent {
	intent, err := clusterstate.NewDispatchIntent([]byte(`{"slot_units":1}`), []byte(`{"template":"template-1"}`), "provider-v1/policy-v1")
	if err != nil {
		panic(err)
	}
	return intent
}

func testBinding(
	t *testing.T,
	registryLayout RegistryLayout,
	kind clusterstate.ExecutionKind,
	objectID, group, routeKey, nodeID string,
	intent clusterstate.DispatchIntent,
) clusterstate.ExecutionBindingIntent {
	t.Helper()
	demandDigest := sha256.Sum256(intent.NormalizedDemand)
	specDigest := sha256.Sum256(intent.DispatchSpec)
	opaque, err := clusterstate.EncodeExecutionBinding(clusterstate.ExecutionBinding{
		RegistryGeneration: registryLayout.RegistryGeneration, Kind: kind, ObjectID: objectID, Group: group,
		RouteKey: routeKey, NodeID: nodeID, NodeEpoch: 7, DemandDigest: demandDigest, DispatchSpecDigest: specDigest,
	})
	if err != nil {
		t.Fatal(err)
	}
	digest, err := clusterstate.ExecutionBindingDigest(opaque)
	if err != nil {
		t.Fatal(err)
	}
	return clusterstate.ExecutionBindingIntent{
		NodeID: nodeID, NodeEpoch: 7, DataEndpoint: nodeID + ":8443",
		RegistryGeneration: registryLayout.RegistryGeneration, OpaqueBinding: opaque, BindingDigest: digest,
	}
}

func hexDigestBytes(value [sha256.Size]byte) string {
	const alphabet = "0123456789abcdef"
	encoded := make([]byte, len(value)*2)
	for index, b := range value {
		encoded[index*2] = alphabet[b>>4]
		encoded[index*2+1] = alphabet[b&0x0f]
	}
	return string(encoded)
}
