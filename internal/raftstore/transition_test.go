package raftstore

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	dragonboat "github.com/lni/dragonboat/v4"
	sm "github.com/lni/dragonboat/v4/statemachine"
)

func TestRuntimeReconcilesAndFinalizesRegistryLayoutTransition(t *testing.T) {
	oldRegistryLayout := testRegistryLayout(1, "generation-1")
	oldRegistryLayout.ServePermitMaxMillis = 25
	oldDigest, err := oldRegistryLayout.Digest()
	if err != nil {
		t.Fatal(err)
	}
	nextRegistryLayout := oldRegistryLayout
	nextRegistryLayout.RegistryLayoutVersion = 2
	nextRegistryLayout.PreviousRegistryLayoutVersion = 1
	nextRegistryLayout.PreviousRegistryLayoutDigest = oldDigest
	nextRegistryLayout.Members = append(append([]RegistryMember(nil), oldRegistryLayout.Members...),
		RegistryMember{MemberID: "registry-d", InternalEndpoint: "https://registry-d:9443", RaftEndpoint: "registry-d:63001"},
		RegistryMember{MemberID: "registry-e", InternalEndpoint: "https://registry-e:9443", RaftEndpoint: "registry-e:63001"},
		RegistryMember{MemberID: "registry-f", InternalEndpoint: "https://registry-f:9443", RaftEndpoint: "registry-f:63001"},
	)
	desired := []ReplicaPlacement{
		{MemberID: "registry-d", ReplicaID: 4},
		{MemberID: "registry-e", ReplicaID: 5},
		{MemberID: "registry-f", ReplicaID: 6},
	}
	nextRegistryLayout.SystemReplicas = append([]ReplicaPlacement(nil), desired...)
	nextRegistryLayout.DataShards = []ShardPlacement{{ShardID: 0, Replicas: append([]ReplicaPlacement(nil), desired...)}}
	if err := nextRegistryLayout.Validate(); err != nil {
		t.Fatal(err)
	}
	nextDigest, err := nextRegistryLayout.Digest()
	if err != nil {
		t.Fatal(err)
	}

	system, _ := applySystem(t, SystemState{}, 1, SystemCommand{
		Type: SystemBootstrap, RegistryLayout: &oldRegistryLayout, Digest: oldDigest,
	})
	transition := &RegistryLayoutTransition{
		Version: 2, Digest: nextDigest, PreviousDigest: oldDigest, NextSystemEpoch: 2,
		Shards: []ShardTransition{
			{ShardID: ^uint32(0), Stage: TransitionPending},
			{ShardID: 0, Stage: TransitionPending},
		},
	}
	system, _ = applySystem(t, system, 2, SystemCommand{
		Type: SystemBeginTransition, Transition: transition,
	})
	dataIdentity := registryLayoutShardIdentity(t, oldRegistryLayout, 0)
	data := initializeDataShard(t, oldRegistryLayout, dataIdentity)
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
		MaxLifetimeMillis: oldRegistryLayout.ServePermitMaxMillis,
		ServeGate:         true, WriteGate: true, CutoverGate: true, RecoveryClosed: true,
	}, time.Now()); err != nil {
		t.Fatal(err)
	}
	failPromotionConfirmation := map[uint64]bool{
		SystemRaftShardID: true, DataRaftShardID(0): true,
	}
	runtime := &Runtime{
		registryLayout: nextRegistryLayout, registryLayoutDigest: nextDigest, member: nextRegistryLayout.Members[0],
		nodeHost: host, permitCache: permitCache,
		enrollmentStore: EnrollmentStore{Path: t.TempDir() + "/enrollment.json"},
		enrollment: LocalEnrollment{
			Version: localEnrollmentVersion, ClusterID: nextRegistryLayout.ClusterID,
			RegistryGeneration: nextRegistryLayout.RegistryGeneration, MemberID: nextRegistryLayout.Members[0].MemberID,
			DeploymentID: 1, RaftAddress: nextRegistryLayout.Members[0].RaftEndpoint,
			NodeHostDir: "/nodehost", StateEngineDir: "/state",
			RuntimeConfigDigest: digestFor("runtime"), RegistryLayoutVersion: oldRegistryLayout.RegistryLayoutVersion,
			RegistryLayoutDigest: oldDigest, Mode: EnrollmentBootstrap,
			Replicas: []LocalReplicaEnrollment{
				{ShardID: SystemRaftShardID, ReplicaID: 1, StartPlan: ReplicaInitial, LocalState: ReplicaActive},
				{ShardID: DataRaftShardID(0), ReplicaID: 1, StartPlan: ReplicaInitial, LocalState: ReplicaActive},
			},
		},
		transitionClient: ReplicaTransitionClientFuncs{
			Probe: func(_ context.Context, request ReplicaCatchUpRequest) (ReplicaCatchUpProof, error) {
				return ReplicaCatchUpProof{
					ShardID: request.ShardID, ReplicaID: request.ReplicaID, MemberID: request.MemberID,
					RegistryLayoutDigest: request.RegistryLayoutDigest, AppliedIndex: request.MinimumAppliedIndex,
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
		state, err := runtime.ReconcileRegistryLayoutTransitionShard(ctx, shardID)
		if err != nil || transitionStageForTest(t, state, shardID) != TransitionCatchingUp {
			t.Fatalf("prepare shard %d = %v, %v", shardID, state, err)
		}
		state, err = runtime.ReconcileRegistryLayoutTransitionShard(ctx, shardID)
		if err == nil || transitionStageForTest(t, system, shardID) != TransitionCatchingUp {
			t.Fatalf("unconfirmed promotion shard %d = %v, %v", shardID, state, err)
		}
		state, err = runtime.ReconcileRegistryLayoutTransitionShard(ctx, shardID)
		if err != nil || transitionStageForTest(t, state, shardID) != TransitionPromoted {
			t.Fatalf("promote shard %d = %v, %v", shardID, state, err)
		}
		state, err = runtime.ReconcileRegistryLayoutTransitionShard(ctx, shardID)
		if err != nil || transitionStageForTest(t, state, shardID) != TransitionComplete {
			t.Fatalf("complete shard %d = %v, %v", shardID, state, err)
		}
		if _, reachable := host.memberships[shardID].Nodes[3]; !reachable {
			t.Fatalf("predecessor replica was removed before layout activation on shard %d", shardID)
		}
	}

	activated, err := runtime.ActivateRegistryLayoutTransition(ctx)
	if err != nil || activated.Transition == nil || !activated.Transition.Activated {
		t.Fatalf("activate = %+v, %v", activated, err)
	}
	drained, err := runtime.ConfirmRegistryLayoutTransitionPermitDrain(ctx)
	if err != nil || drained.Transition == nil || !drained.Transition.PreviousPermitDrainComplete {
		t.Fatalf("drain = %+v, %v", drained, err)
	}
	if err := permitCache.Install(PermitGrant{
		PermitIdentity: drained.Identity(), CommitIndex: drained.LastApplied,
		MaxLifetimeMillis: nextRegistryLayout.ServePermitMaxMillis,
		ServeGate:         true, WriteGate: true, CutoverGate: true, RecoveryClosed: true,
	}, time.Now()); err != nil {
		t.Fatal(err)
	}
	for _, shardID := range []uint64{SystemRaftShardID, DataRaftShardID(0)} {
		state, err := runtime.ReconcileRegistryLayoutTransitionShard(ctx, shardID)
		if err != nil || transitionStageForTest(t, state, shardID) != TransitionEpochRetired {
			t.Fatalf("retire shard %d = %v, %v", shardID, state, err)
		}
	}
	finalized, err := runtime.FinalizeRegistryLayoutTransition(ctx)
	if err != nil || finalized.Transition != nil || finalized.ActiveRegistryLayoutDigest != nextDigest {
		t.Fatalf("finalize = %+v, %v", finalized, err)
	}
	retried, err := runtime.FinalizeRegistryLayoutTransition(ctx)
	if err != nil || retried.Transition != nil || retried.ActiveRegistryLayoutDigest != nextDigest {
		t.Fatalf("retry finalized transition = %+v, %v", retried, err)
	}
	redrained, err := runtime.ConfirmRegistryLayoutTransitionPermitDrain(ctx)
	if err != nil || redrained.Transition != nil || redrained.ActiveRegistryLayoutDigest != nextDigest {
		t.Fatalf("retry drain after finalized transition = %+v, %v", redrained, err)
	}
	if err := validateRetiredEpoch(data, activated.Identity(), []uint64{4, 5, 6}); err != nil {
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
		t.Fatal("registryLayout transition disappeared")
	}
	shardID, err := transitionProgressID(raftShardID)
	if err != nil {
		t.Fatal(err)
	}
	position := transitionPosition(state.Transition.Shards, shardID)
	if position < 0 {
		t.Fatal("registryLayout transition does not track shard")
	}
	return state.Transition.Shards[position].Stage
}

func TestTransitionAdvanceAmbiguityAcceptsLaterOrFinalizedState(t *testing.T) {
	const (
		version = uint64(2)
		shardID = uint32(0)
	)
	digest := digestFor("next-registry-layout")
	state := SystemState{Transition: &RegistryLayoutTransition{
		Version: version, Digest: digest,
		Shards: []ShardTransition{
			{ShardID: ^uint32(0), Stage: TransitionPending},
			{ShardID: shardID, Stage: TransitionComplete},
		},
	}}
	if !transitionAdvanceCommitted(state, version, digest, shardID, TransitionCatchingUp) {
		t.Fatal("later committed shard stage did not resolve the earlier ambiguous advance")
	}
	if transitionAdvanceCommitted(state, version, digestFor("other-layout"), shardID, TransitionCatchingUp) {
		t.Fatal("another Registry Layout resolved the ambiguous advance")
	}
	state.Transition = nil
	state.ActiveRegistryLayoutVersion = version
	state.ActiveRegistryLayoutDigest = digest
	if !transitionAdvanceCommitted(state, version, digest, shardID, TransitionCatchingUp) {
		t.Fatal("finalized target Registry Layout did not resolve the ambiguous advance")
	}
}

func TestRegistryLayoutActivationAcceptsOnlyExactActivatedOrFinalizedTarget(t *testing.T) {
	previous := testRegistryLayout(1, "generation-activation")
	previousDigest, err := previous.Digest()
	if err != nil {
		t.Fatal(err)
	}
	target := previous
	target.RegistryLayoutVersion = 2
	target.PreviousRegistryLayoutVersion = 1
	target.PreviousRegistryLayoutDigest = previousDigest
	targetDigest, err := target.Digest()
	if err != nil {
		t.Fatal(err)
	}
	runtime := Runtime{registryLayout: target, registryLayoutDigest: targetDigest}
	state := SystemState{
		ActiveRegistryLayoutVersion: 2, ActiveRegistryLayoutDigest: targetDigest,
		Transition: &RegistryLayoutTransition{
			Version: 2, Digest: targetDigest, PreviousDigest: previousDigest, Activated: true,
		},
	}
	if !runtime.registryLayoutActivationCommitted(state) {
		t.Fatal("exact activated target was not recognized")
	}
	state.Transition = nil
	if !runtime.registryLayoutActivationCommitted(state) {
		t.Fatal("exact finalized target was not recognized")
	}
	state.ActiveRegistryLayoutDigest = digestFor("other-layout")
	if runtime.registryLayoutActivationCommitted(state) {
		t.Fatal("another finalized Registry Layout was accepted")
	}
	initial := testRegistryLayout(1, "generation-initial")
	initialDigest, err := initial.Digest()
	if err != nil {
		t.Fatal(err)
	}
	runtime = Runtime{registryLayout: initial, registryLayoutDigest: initialDigest}
	if runtime.registryLayoutActivationCommitted(SystemState{
		ActiveRegistryLayoutVersion: 1, ActiveRegistryLayoutDigest: initialDigest,
	}) {
		t.Fatal("initial Registry Layout was mistaken for a finalized transition")
	}
}
