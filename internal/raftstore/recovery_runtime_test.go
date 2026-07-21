package raftstore

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	sm "github.com/lni/dragonboat/v4/statemachine"
)

func TestRecoveryRequiresExactCommittedActiveRegistryLayout(t *testing.T) {
	active := recoveryTestRegistryLayout(t, 1)
	activeDigest, _ := active.Digest()
	state, _ := applySystem(t, SystemState{}, 1, SystemCommand{
		Type: SystemBootstrap, RegistryLayout: &active, Digest: activeDigest,
	})
	next := active
	next.RegistryLayoutVersion = 2
	next.PreviousRegistryLayoutVersion = 1
	next.PreviousRegistryLayoutDigest = activeDigest
	if err := next.Validate(); err != nil {
		t.Fatal(err)
	}
	nextDigest, _ := next.Digest()
	runtime := &Runtime{registryLayout: next, registryLayoutDigest: nextDigest}
	if err := runtime.authorizeRegistryLayoutState(state); err != nil {
		t.Fatalf("next artifact should be accepted as staged runtime input: %v", err)
	}
	if err := runtime.requireExactActiveRegistryLayout(state); err == nil {
		t.Fatal("recovery accepted placements from an uncommitted next Registry Layout")
	}
}

func TestRuntimeWaitsForRecoveryPermitDrain(t *testing.T) {
	registryLayout := recoveryTestRegistryLayout(t, 1)
	registryLayout.ServePermitMaxMillis = 5
	digest, _ := registryLayout.Digest()
	state, _ := applySystem(t, SystemState{}, 1, SystemCommand{
		Type: SystemBootstrap, RegistryLayout: &registryLayout, Digest: digest,
	})
	host := newFakeNodeHost()
	host.read = func(shardID uint64, _ any) (any, error) {
		if shardID != SystemRaftShardID {
			return nil, errors.New("unexpected data-shard read before permit drain")
		}
		return cloneSystemState(state), nil
	}
	host.propose = func(raw []byte) (sm.Result, error) {
		command, err := DecodeSystemCommand(raw)
		if err != nil {
			return sm.Result{}, err
		}
		var result SystemApplyResult
		state, result = ApplySystemCommand(state, state.LastApplied+1, command)
		encoded, _ := json.Marshal(result)
		return sm.Result{Data: encoded}, nil
	}
	runtime := &Runtime{
		registryLayout: registryLayout, registryLayoutDigest: digest,
		nodeHost: host, member: registryLayout.Members[0],
	}
	recovery := newRecoveryEpoch(registryLayout, digest)
	opened, err := runtime.BeginRecovery(context.Background(), recovery)
	if err != nil || opened.Recovery == nil || opened.Recovery.PermitDrainComplete {
		t.Fatalf("opened recovery = %+v, %v", opened, err)
	}
	if err := runtime.PrepareRecoveryDataShards(context.Background(), 1); err == nil {
		t.Fatal("data-shard recovery started while an old write Permit could remain valid")
	}
	started := time.Now()
	confirmed, err := runtime.ConfirmRecoveryPermitDrain(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed < 5*time.Millisecond {
		t.Fatalf("permit drain completed after only %s", elapsed)
	}
	if confirmed.Recovery == nil || !confirmed.Recovery.PermitDrainComplete ||
		confirmed.Recovery.PermitDrainIndex == 0 {
		t.Fatalf("confirmed recovery = %+v", confirmed.Recovery)
	}
}

func TestRecoveryRoutesShardWorkToPlacedRegistryMember(t *testing.T) {
	registryLayout := recoveryTestRegistryLayout(t, 2)
	registryLayout.Members = append(registryLayout.Members, RegistryMember{
		MemberID: "registry-d", InternalEndpoint: "https://registry-d:9443", RaftEndpoint: "registry-d:63001",
	})
	registryLayout.DataShards[1].Replicas = []ReplicaPlacement{
		{MemberID: "registry-b", ReplicaID: 12},
		{MemberID: "registry-c", ReplicaID: 13},
		{MemberID: "registry-d", ReplicaID: 14},
	}
	if err := registryLayout.Validate(); err != nil {
		t.Fatal(err)
	}
	digest, _ := registryLayout.Digest()
	authorization := recoveryAuthorizationForTest(registryLayout, digest, RecoveryPreparing)
	var routed RecoveryShardRequest
	runtime := &Runtime{
		registryLayout: registryLayout, registryLayoutDigest: digest, member: registryLayout.Members[0],
		recoveryClient: RecoveryShardClientFunc(func(_ context.Context, request RecoveryShardRequest) (RecoveryShardProof, error) {
			routed = request
			return RecoveryShardProof{
				MemberID: request.MemberID, ShardID: request.ShardID, Action: request.Action,
				RegistryLayoutDigest: digest, RecoveryEpoch: request.Authorization.Recovery.Epoch, AppliedIndex: 9,
			}, nil
		}),
	}
	proof, err := runtime.routeRecoveryShard(context.Background(), authorization, 1, RecoveryShardVerifyPrepared)
	if err != nil {
		t.Fatal(err)
	}
	if routed.MemberID != "registry-b" || routed.ShardID != 1 || proof.MemberID != "registry-b" {
		t.Fatalf("recovery was not routed to an exact hosted replica: request=%+v proof=%+v", routed, proof)
	}
}

func TestRecoveryCannotLeavePreparingBeforeEveryShardIsPrepared(t *testing.T) {
	registryLayout := recoveryTestRegistryLayout(t, 2)
	registryLayout.Members = append(registryLayout.Members, RegistryMember{
		MemberID: "registry-d", InternalEndpoint: "https://registry-d:9443", RaftEndpoint: "registry-d:63001",
	})
	digest, _ := registryLayout.Digest()
	state := recoverySystemForTest(t, registryLayout, digest, RecoveryPreparing)
	host := newFakeNodeHost()
	host.read = func(shardID uint64, _ any) (any, error) {
		if shardID != SystemRaftShardID {
			return nil, errors.New("unexpected local data-shard read")
		}
		return cloneSystemState(state), nil
	}
	proposals := 0
	host.propose = func(_ []byte) (sm.Result, error) {
		proposals++
		return sm.Result{}, errors.New("unexpected recovery phase proposal")
	}
	runtime := &Runtime{
		registryLayout: registryLayout, registryLayoutDigest: digest,
		member: registryLayout.Members[3], nodeHost: host,
		recoveryClient: RecoveryShardClientFunc(func(_ context.Context, request RecoveryShardRequest) (RecoveryShardProof, error) {
			return RecoveryShardProof{}, errors.New("shard is not prepared")
		}),
	}
	if _, err := runtime.AdvanceRecovery(context.Background(), RecoveryCollecting); err == nil {
		t.Fatal("recovery entered COLLECTING with unprepared data shards")
	}
	if proposals != 0 {
		t.Fatalf("recovery phase proposal count = %d, want 0", proposals)
	}
}

func recoveryTestRegistryLayout(t *testing.T, shards uint32) RegistryLayout {
	t.Helper()
	registryLayout := testRegistryLayout(shards, "generation-2")
	registryLayout.Predecessor = &PredecessorProof{
		RegistryGeneration: "generation-1", RegistryLayoutDigest: digestFor("source-registryLayout"),
		ServePermitMaxMillis: registryLayout.ServePermitMaxMillis,
		Kind:                 RolloverExternalFence, ProofDigest: digestFor("complete-hard-fence"),
	}
	if err := registryLayout.Validate(); err != nil {
		t.Fatal(err)
	}
	return registryLayout
}

func newRecoveryEpoch(registryLayout RegistryLayout, digest string) RecoveryEpoch {
	return RecoveryEpoch{
		Epoch: 2, SourceClusterID: registryLayout.ClusterID,
		SourceRegistryGeneration:   registryLayout.Predecessor.RegistryGeneration,
		SourceRegistryLayoutDigest: registryLayout.Predecessor.RegistryLayoutDigest,
		TargetRegistryGeneration:   registryLayout.RegistryGeneration,
		TargetRegistryLayoutDigest: digest, Phase: RecoveryPreparing,
	}
}

func recoveryAuthorizationForTest(
	registryLayout RegistryLayout,
	digest string,
	phase RecoveryPhase,
) RecoveryShardAuthorization {
	recovery := newRecoveryEpoch(registryLayout, digest)
	recovery.Phase = phase
	recovery.PermitDrainComplete = true
	recovery.PermitDrainIndex = 3
	return RecoveryShardAuthorization{
		Recovery: recovery, ActiveRegistryLayoutVersion: registryLayout.RegistryLayoutVersion, SystemCommitIndex: 4,
	}
}

func recoverySystemForTest(
	t *testing.T,
	registryLayout RegistryLayout,
	digest string,
	phase RecoveryPhase,
) SystemState {
	t.Helper()
	state, _ := applySystem(t, SystemState{}, 1, SystemCommand{
		Type: SystemBootstrap, RegistryLayout: &registryLayout, Digest: digest,
	})
	recovery := newRecoveryEpoch(registryLayout, digest)
	state, _ = applySystem(t, state, 2, SystemCommand{Type: SystemBeginRecovery, Recovery: &recovery})
	state, _ = applySystem(t, state, 3, SystemCommand{
		Type: SystemConfirmRecoveryDrain,
		RecoveryDrain: &RecoveryDrainConfirmation{
			RecoveryEpoch: recovery.Epoch, TargetRegistryGeneration: recovery.TargetRegistryGeneration,
			TargetRegistryLayoutDigest: recovery.TargetRegistryLayoutDigest,
			WaitedMillis:               registryLayout.ServePermitMaxMillis,
		},
	})
	for state.Recovery.Phase != phase {
		next := map[RecoveryPhase]RecoveryPhase{
			RecoveryPreparing: RecoveryCollecting, RecoveryCollecting: RecoveryReconciling,
			RecoveryReconciling: RecoveryFinalizing,
		}[state.Recovery.Phase]
		state, _ = applySystem(t, state, state.LastApplied+1, SystemCommand{
			Type:            SystemAdvanceRecovery,
			RecoveryAdvance: &RecoveryAdvance{From: state.Recovery.Phase, To: next},
		})
	}
	return state
}
