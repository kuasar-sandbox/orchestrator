package raftstore

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	dragonboat "github.com/lni/dragonboat/v4"
	sm "github.com/lni/dragonboat/v4/statemachine"
)

func TestRuntimeReconcilesAndFinalizesManifestTransition(t *testing.T) {
	oldManifest := testManifest(1, "generation-1")
	oldManifest.ServePermitMaxMillis = 25
	oldDigest, err := oldManifest.Digest()
	if err != nil {
		t.Fatal(err)
	}
	nextManifest := oldManifest
	nextManifest.ManifestVersion = 2
	nextManifest.PreviousManifestDigest = oldDigest
	nextManifest.Members = append(append([]RegistryMember(nil), oldManifest.Members...), RegistryMember{
		MemberID: "registry-d", InternalEndpoint: "https://registry-d:9443", RaftEndpoint: "registry-d:63001",
	})
	desired := []ReplicaPlacement{
		{MemberID: "registry-a", ReplicaID: 1},
		{MemberID: "registry-b", ReplicaID: 2},
		{MemberID: "registry-d", ReplicaID: 4},
	}
	nextManifest.SystemReplicas = append([]ReplicaPlacement(nil), desired...)
	nextManifest.DataShards = []ShardPlacement{{ShardID: 0, Replicas: append([]ReplicaPlacement(nil), desired...)}}
	if err := nextManifest.Validate(); err != nil {
		t.Fatal(err)
	}
	nextDigest, err := nextManifest.Digest()
	if err != nil {
		t.Fatal(err)
	}

	system, _ := applySystem(t, SystemState{}, 1, SystemCommand{
		Type: SystemBootstrap, Manifest: &oldManifest, Digest: oldDigest,
	})
	transition := &ManifestTransition{
		Version: 2, Digest: nextDigest, PreviousDigest: oldDigest, NextSystemEpoch: 2,
		Shards: []ShardTransition{
			{ShardID: ^uint32(0), Stage: TransitionPending},
			{ShardID: 0, Stage: TransitionPending},
		},
	}
	system, _ = applySystem(t, system, 2, SystemCommand{
		Type: SystemBeginTransition, Transition: transition,
	})
	dataIdentity := manifestShardIdentity(t, oldManifest, 0)
	data := initializeDataShard(t, oldManifest, dataIdentity)
	systemIndex, dataIndex := system.LastApplied, data.LastApplied

	host := newFakeNodeHost()
	for _, shardID := range []uint64{SystemRaftShardID, DataRaftShardID(0)} {
		host.memberships[shardID] = &dragonboat.Membership{
			ConfigChangeID: 10,
			Nodes: map[uint64]string{
				1: "registry-a:63001", 2: "registry-b:63001", 3: "registry-c:63001",
			},
			NonVotings: map[uint64]string{}, Witnesses: map[uint64]string{}, Removed: map[uint64]struct{}{},
		}
	}
	host.read = func(shardID uint64, _ any) (any, error) {
		if shardID == SystemRaftShardID {
			return system, nil
		}
		return data, nil
	}
	host.propose = func(raw []byte) (sm.Result, error) {
		if command, decodeErr := DecodeSystemCommand(raw); decodeErr == nil {
			systemIndex++
			var result SystemApplyResult
			system, result = ApplySystemCommand(system, systemIndex, command)
			encoded, marshalErr := json.Marshal(result)
			return sm.Result{Data: encoded}, marshalErr
		}
		command, decodeErr := DecodeDataCommand(raw)
		if decodeErr != nil {
			return sm.Result{}, decodeErr
		}
		dataIndex++
		result := ApplyDataCommand(&data, dataIndex, command)
		encoded, marshalErr := json.Marshal(result)
		return sm.Result{Data: encoded}, marshalErr
	}
	permitCache := NewPermitCache(time.Now)
	if err := permitCache.Install(PermitGrant{
		PermitIdentity: system.Identity(), CommitIndex: system.LastApplied,
		MaxLifetimeMillis: oldManifest.ServePermitMaxMillis,
		ServeGate:         true, WriteGate: true, CutoverGate: true, RecoveryClosed: true,
	}, time.Now()); err != nil {
		t.Fatal(err)
	}
	failPromotionConfirmation := map[uint64]bool{
		SystemRaftShardID: true, DataRaftShardID(0): true,
	}
	runtime := &Runtime{
		manifest: nextManifest, manifestDigest: nextDigest, member: nextManifest.Members[0],
		nodeHost: host, permitCache: permitCache,
		enrollmentStore: EnrollmentStore{Path: t.TempDir() + "/enrollment.json"},
		enrollment: LocalEnrollment{
			Version: localEnrollmentVersion, ClusterID: nextManifest.ClusterID,
			StorageGeneration: nextManifest.StorageGeneration, MemberID: nextManifest.Members[0].MemberID,
			DeploymentID: 1, RaftAddress: nextManifest.Members[0].RaftEndpoint,
			NodeHostDir: "/nodehost", StateEngineDir: "/state",
			RuntimeConfigDigest: digestFor("runtime"), ManifestVersion: oldManifest.ManifestVersion,
			ManifestDigest: oldDigest, Mode: EnrollmentBootstrap,
			Replicas: []LocalReplicaEnrollment{
				{ShardID: SystemRaftShardID, ReplicaID: 1, StartPlan: ReplicaInitial, LocalState: ReplicaActive},
				{ShardID: DataRaftShardID(0), ReplicaID: 1, StartPlan: ReplicaInitial, LocalState: ReplicaActive},
			},
		},
		transitionClient: ReplicaTransitionClientFuncs{
			Probe: func(_ context.Context, request ReplicaCatchUpRequest) (ReplicaCatchUpProof, error) {
				return ReplicaCatchUpProof{
					ShardID: request.ShardID, ReplicaID: request.ReplicaID, MemberID: request.MemberID,
					ManifestDigest: request.ManifestDigest, AppliedIndex: request.MinimumAppliedIndex,
				}, nil
			},
			Confirm: func(_ context.Context, request ReplicaPromotionRequest) error {
				if request.MemberID == "registry-d" && failPromotionConfirmation[request.ShardID] {
					delete(failPromotionConfirmation, request.ShardID)
					return context.DeadlineExceeded
				}
				return nil
			},
		},
	}
	ctx := context.Background()
	for _, shardID := range []uint64{SystemRaftShardID, DataRaftShardID(0)} {
		state, err := runtime.ReconcileManifestTransitionShard(ctx, shardID)
		if err != nil || transitionStageForTest(t, state, shardID) != TransitionCatchingUp {
			t.Fatalf("prepare shard %d = %v, %v", shardID, state, err)
		}
		state, err = runtime.ReconcileManifestTransitionShard(ctx, shardID)
		if err == nil || transitionStageForTest(t, system, shardID) != TransitionCatchingUp {
			t.Fatalf("unconfirmed promotion shard %d = %v, %v", shardID, state, err)
		}
		state, err = runtime.ReconcileManifestTransitionShard(ctx, shardID)
		if err != nil || transitionStageForTest(t, state, shardID) != TransitionPromoted {
			t.Fatalf("promote shard %d = %v, %v", shardID, state, err)
		}
		state, err = runtime.ReconcileManifestTransitionShard(ctx, shardID)
		if err != nil || transitionStageForTest(t, state, shardID) != TransitionOldRemoved {
			t.Fatalf("remove shard %d = %v, %v", shardID, state, err)
		}
		state, err = runtime.ReconcileManifestTransitionShard(ctx, shardID)
		if err != nil || transitionStageForTest(t, state, shardID) != TransitionComplete {
			t.Fatalf("complete shard %d = %v, %v", shardID, state, err)
		}
	}

	activated, err := runtime.ActivateManifestTransition(ctx)
	if err != nil || activated.Transition == nil || !activated.Transition.Activated {
		t.Fatalf("activate = %+v, %v", activated, err)
	}
	drained, err := runtime.ConfirmManifestTransitionPermitDrain(ctx)
	if err != nil || drained.Transition == nil || !drained.Transition.PreviousPermitDrainComplete {
		t.Fatalf("drain = %+v, %v", drained, err)
	}
	if err := permitCache.Install(PermitGrant{
		PermitIdentity: drained.Identity(), CommitIndex: drained.LastApplied,
		MaxLifetimeMillis: nextManifest.ServePermitMaxMillis,
		ServeGate:         true, WriteGate: true, CutoverGate: true, RecoveryClosed: true,
	}, time.Now()); err != nil {
		t.Fatal(err)
	}
	for _, shardID := range []uint64{SystemRaftShardID, DataRaftShardID(0)} {
		state, err := runtime.ReconcileManifestTransitionShard(ctx, shardID)
		if err != nil || transitionStageForTest(t, state, shardID) != TransitionEpochRetired {
			t.Fatalf("retire shard %d = %v, %v", shardID, state, err)
		}
	}
	finalized, err := runtime.FinalizeManifestTransition(ctx)
	if err != nil || finalized.Transition != nil || finalized.ActiveManifestDigest != nextDigest {
		t.Fatalf("finalize = %+v, %v", finalized, err)
	}
	if err := validateRetiredEpoch(data, activated.Identity(), []uint64{1, 2, 4}); err != nil {
		t.Fatal(err)
	}
	for _, shardID := range []uint64{SystemRaftShardID, DataRaftShardID(0)} {
		if err := runtime.verifyDesiredMembership(ctx, shardID); err != nil {
			t.Fatalf("final membership shard %d: %v", shardID, err)
		}
	}
}

func transitionStageForTest(t *testing.T, state SystemState, raftShardID uint64) TransitionStage {
	t.Helper()
	if state.Transition == nil {
		t.Fatal("manifest transition disappeared")
	}
	shardID, err := transitionProgressID(raftShardID)
	if err != nil {
		t.Fatal(err)
	}
	position := transitionPosition(state.Transition.Shards, shardID)
	if position < 0 {
		t.Fatal("manifest transition does not track shard")
	}
	return state.Transition.Shards[position].Stage
}
