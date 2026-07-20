package raftstore

import (
	"errors"
	"testing"
	"time"
)

func applySystem(t *testing.T, state SystemState, index uint64, command SystemCommand) (SystemState, SystemApplyResult) {
	t.Helper()
	next, result := ApplySystemCommand(state, index, command)
	if result.Conflict || !result.Applied {
		t.Fatalf("apply %s at %d = %+v", command.Type, index, result)
	}
	return next, result
}

func TestSystemGroupBootstrapPermitAndPermanentClosure(t *testing.T) {
	registryLayout := testRegistryLayout(4, "generation-1")
	digest, _ := registryLayout.Digest()
	state, _ := applySystem(t, SystemState{}, 1, SystemCommand{Type: SystemBootstrap, RegistryLayout: &registryLayout, Digest: digest})
	state, _ = applySystem(t, state, 2, SystemCommand{
		Type: SystemSetGates, Gates: &GateUpdate{Serve: true, Write: true, Cutover: true},
	})
	state, result := applySystem(t, state, 3, SystemCommand{Type: SystemRefreshPermit})
	if result.PermitGrant == nil || !result.PermitGrant.WriteGate || result.PermitGrant.CommitIndex != 3 {
		t.Fatalf("permit grant = %+v", result.PermitGrant)
	}
	permit, err := NewServePermit(*result.PermitGrant, time.Now())
	if err != nil || permit.Authorize(time.Now(), state.Identity(), PermitHolderDispatch) != nil {
		t.Fatalf("authorized permit = %+v, %v", permit, err)
	}
	successor := testRegistryLayout(4, "generation-2")
	successor.Predecessor = &PredecessorProof{
		RegistryGeneration: registryLayout.RegistryGeneration, RegistryLayoutDigest: digest,
		ServePermitMaxMillis: registryLayout.ServePermitMaxMillis, Kind: RolloverConsensusClosure,
	}
	intentDigest, err := successor.RolloverIntentDigest()
	if err != nil {
		t.Fatal(err)
	}
	closure := &RegistryGenerationClosure{
		TargetRegistryGeneration:         successor.RegistryGeneration,
		TargetRegistryLayoutIntentDigest: intentDigest, Kind: RolloverConsensusClosure,
	}
	state, _ = applySystem(t, state, 4, SystemCommand{Type: SystemCloseRegistryGeneration, Closure: closure})
	if !state.Retired || state.ServeGate || state.WriteGate || state.Closure == nil || state.Closure.CommitIndex != 4 ||
		!isSHA256(state.Closure.ProofDigest) || state.Closure.ProofDigest != registryGenerationClosureProofDigest(state, *state.Closure) {
		t.Fatalf("closed generation = %+v", state)
	}
	proof, err := state.ConsensusPredecessorProof()
	if err != nil {
		t.Fatal(err)
	}
	successor.Predecessor = &proof
	if err := successor.Validate(); err != nil {
		t.Fatalf("successor did not accept committed closure: %v", err)
	}
	_, rejected := ApplySystemCommand(state, 5, SystemCommand{Type: SystemRefreshPermit})
	if !rejected.Conflict {
		t.Fatal("retired generation renewed a permit")
	}
	state.LastApplied = 5 // committed rejected entries still advance the state-machine applied index
	if err := state.Validate(); err != nil {
		t.Fatalf("late rejected entry invalidated permanent closure: %v", err)
	}
	if _, err := state.ConsensusPredecessorProof(); err != nil {
		t.Fatalf("late rejected entry hid permanent closure proof: %v", err)
	}
}

func TestSuccessorGenerationRequiresPermitDrainUnlessHardFenced(t *testing.T) {
	predecessor := testRegistryLayout(4, "generation-1")
	predecessorDigest, _ := predecessor.Digest()
	registryLayout, _ := finalizeConsensusSuccessor(t, predecessor, predecessorDigest, testRegistryLayout(4, "generation-2"))
	digest, _ := registryLayout.Digest()
	state, _ := applySystem(t, SystemState{}, 1, SystemCommand{Type: SystemBootstrap, RegistryLayout: &registryLayout, Digest: digest})
	_, rejected := ApplySystemCommand(state, 2, SystemCommand{
		Type: SystemSetGates, Gates: &GateUpdate{Serve: true, Write: true, Cutover: true},
	})
	if !rejected.Conflict {
		t.Fatal("successor served before predecessor permit drain")
	}
	_, rejected = ApplySystemCommand(state, 3, SystemCommand{Type: SystemConfirmDrain, Drain: &DrainConfirmation{
		PredecessorProofDigest: registryLayout.Predecessor.ProofDigest,
		WaitedMillis:           registryLayout.Predecessor.ServePermitMaxMillis - 1,
		EvidenceDigest:         digestFor("short-wait"),
	}})
	if !rejected.Conflict {
		t.Fatal("short predecessor permit wait was accepted")
	}
	state, _ = applySystem(t, state, 4, SystemCommand{Type: SystemConfirmDrain, Drain: &DrainConfirmation{
		PredecessorProofDigest: registryLayout.Predecessor.ProofDigest,
		WaitedMillis:           registryLayout.Predecessor.ServePermitMaxMillis,
		EvidenceDigest:         digestFor("monotonic-wait"),
	}})
	state, _ = applySystem(t, state, 5, SystemCommand{
		Type: SystemSetGates, Gates: &GateUpdate{Serve: true, Write: true, Cutover: true},
	})
	if !state.ServeGate {
		t.Fatal("drained successor did not open")
	}

	hardFenced := testRegistryLayout(4, "generation-3")
	hardFenced.Predecessor = &PredecessorProof{
		RegistryGeneration: "generation-2", RegistryLayoutDigest: digest, ServePermitMaxMillis: 5000,
		Kind:        RolloverExternalFence,
		ProofDigest: digestFor("external-hard-fence"),
	}
	hardDigest, _ := hardFenced.Digest()
	hardState, _ := applySystem(t, SystemState{}, 1, SystemCommand{Type: SystemBootstrap, RegistryLayout: &hardFenced, Digest: hardDigest})
	if !hardState.PredecessorDrainComplete {
		t.Fatal("complete hard fence did not satisfy predecessor drain")
	}
}

func TestSystemGroupRejectsNonInitialRegistryLayoutBootstrap(t *testing.T) {
	registryLayout := testRegistryLayout(2, "generation-1")
	registryLayout.RegistryLayoutVersion = 2
	registryLayout.PreviousRegistryLayoutVersion = 1
	registryLayout.PreviousRegistryLayoutDigest = digestFor("registryLayout-1")
	digest, err := registryLayout.Digest()
	if err != nil {
		t.Fatal(err)
	}
	_, result := ApplySystemCommand(SystemState{}, 1, SystemCommand{
		Type: SystemBootstrap, RegistryLayout: &registryLayout, Digest: digest,
	})
	if !result.Conflict {
		t.Fatal("System Group accepted a non-initial registryLayout as empty bootstrap")
	}
}

func TestRecoveryEpochClosesNormalServiceAndAdvancesSystemEpoch(t *testing.T) {
	registryLayout := testRegistryLayout(2, "generation-2")
	registryLayout.Predecessor = &PredecessorProof{
		RegistryGeneration: "generation-1", RegistryLayoutDigest: digestFor("source-registryLayout"),
		ServePermitMaxMillis: 5000, Kind: RolloverExternalFence, ProofDigest: digestFor("hard-fence"),
	}
	digest, _ := registryLayout.Digest()
	state, _ := applySystem(t, SystemState{}, 1, SystemCommand{Type: SystemBootstrap, RegistryLayout: &registryLayout, Digest: digest})
	state, _ = applySystem(t, state, 2, SystemCommand{
		Type: SystemSetGates, Gates: &GateUpdate{Serve: true, Write: true, Cutover: true},
	})
	recovery := &RecoveryEpoch{
		Epoch: 2, SourceClusterID: "cluster-1", SourceRegistryGeneration: "generation-1",
		SourceRegistryLayoutDigest: digestFor("source-registryLayout"), TargetRegistryGeneration: "generation-2",
		TargetRegistryLayoutDigest: digest, Phase: RecoveryPreparing,
	}
	state, _ = applySystem(t, state, 3, SystemCommand{Type: SystemBeginRecovery, Recovery: recovery})
	if state.Recovery == nil || state.ServeGate || state.SystemEpoch != 2 {
		t.Fatalf("open recovery state = %+v", state)
	}
	phases := []RecoveryPhase{RecoveryCollecting, RecoveryReconciling, RecoveryFinalizing, RecoveryClosed}
	from := RecoveryPreparing
	index := uint64(4)
	for _, to := range phases {
		state, _ = applySystem(t, state, index, SystemCommand{
			Type: SystemAdvanceRecovery, RecoveryAdvance: &RecoveryAdvance{From: from, To: to},
		})
		from = to
		index++
	}
	if state.Recovery != nil || state.SystemEpoch != 3 || state.ServeGate {
		t.Fatalf("closed recovery state = %+v", state)
	}
}

func TestRecoveryEpochMustNameTheCommittedPredecessor(t *testing.T) {
	registryLayout := testRegistryLayout(2, "generation-2")
	registryLayout.Predecessor = &PredecessorProof{
		RegistryGeneration: "generation-1", RegistryLayoutDigest: digestFor("source-registryLayout"),
		ServePermitMaxMillis: 5000, Kind: RolloverExternalFence, ProofDigest: digestFor("hard-fence"),
	}
	digest, _ := registryLayout.Digest()
	state, _ := applySystem(t, SystemState{}, 1, SystemCommand{Type: SystemBootstrap, RegistryLayout: &registryLayout, Digest: digest})

	for name, mutate := range map[string]func(*RecoveryEpoch){
		"wrong epoch":          func(recovery *RecoveryEpoch) { recovery.Epoch++ },
		"wrong cluster":        func(recovery *RecoveryEpoch) { recovery.SourceClusterID = "other-cluster" },
		"wrong generation":     func(recovery *RecoveryEpoch) { recovery.SourceRegistryGeneration = "generation-0" },
		"wrong registryLayout": func(recovery *RecoveryEpoch) { recovery.SourceRegistryLayoutDigest = digestFor("other-registryLayout") },
	} {
		t.Run(name, func(t *testing.T) {
			recovery := RecoveryEpoch{
				Epoch: 2, SourceClusterID: "cluster-1", SourceRegistryGeneration: "generation-1",
				SourceRegistryLayoutDigest: digestFor("source-registryLayout"), TargetRegistryGeneration: "generation-2",
				TargetRegistryLayoutDigest: digest, Phase: RecoveryPreparing,
			}
			mutate(&recovery)
			_, result := ApplySystemCommand(state, 2, SystemCommand{Type: SystemBeginRecovery, Recovery: &recovery})
			if !result.Conflict {
				t.Fatal("recovery with uncommitted source identity was accepted")
			}
		})
	}
}

func TestRegistryLayoutTransitionActivatesOnlyAfterEveryShardCompletes(t *testing.T) {
	registryLayout := testRegistryLayout(2, "generation-1")
	digest, _ := registryLayout.Digest()
	state, _ := applySystem(t, SystemState{}, 1, SystemCommand{Type: SystemBootstrap, RegistryLayout: &registryLayout, Digest: digest})
	transition := &RegistryLayoutTransition{
		Version: 2, Digest: digestFor("registryLayout-2"), PreviousDigest: digest, NextSystemEpoch: 2,
		Shards: []ShardTransition{
			{ShardID: ^uint32(0), Stage: TransitionPending},
			{ShardID: 0, Stage: TransitionPending},
			{ShardID: 1, Stage: TransitionPending},
		},
	}
	state, _ = applySystem(t, state, 2, SystemCommand{Type: SystemBeginTransition, Transition: transition})
	_, rejected := ApplySystemCommand(state, 3, SystemCommand{Type: SystemActivateTransition})
	if !rejected.Conflict {
		t.Fatal("incomplete transition activated")
	}
	index := uint64(4)
	for _, shardID := range []uint32{^uint32(0), 0, 1} {
		for _, edge := range [][2]TransitionStage{
			{TransitionPending, TransitionCatchingUp},
			{TransitionCatchingUp, TransitionPromoted},
			{TransitionPromoted, TransitionOldRemoved},
			{TransitionOldRemoved, TransitionComplete},
		} {
			state, _ = applySystem(t, state, index, SystemCommand{
				Type:    SystemAdvanceTransition,
				Advance: &TransitionAdvance{ShardID: shardID, From: edge[0], To: edge[1]},
			})
			index++
		}
	}
	state, _ = applySystem(t, state, index, SystemCommand{Type: SystemActivateTransition})
	activationIndex := index
	if state.Transition == nil || !state.Transition.Activated ||
		state.Transition.ActivationIndex != activationIndex || state.ActiveRegistryLayoutVersion != 2 ||
		state.SystemEpoch != 2 || state.ActiveRegistryLayoutDigest != transition.Digest {
		t.Fatalf("activated transition = %+v", state)
	}
	index++
	_, rejected = ApplySystemCommand(state, index, SystemCommand{
		Type: SystemAdvanceTransition,
		Advance: &TransitionAdvance{
			ShardID: ^uint32(0), From: TransitionComplete, To: TransitionEpochRetired,
		},
	})
	if !rejected.Conflict {
		t.Fatal("old epoch retired before its Serve Permits drained")
	}
	state, _ = applySystem(t, state, index, SystemCommand{
		Type: SystemConfirmTransitionDrain,
		TransitionDrain: &TransitionDrainConfirmation{
			PreviousRegistryLayoutDigest: transition.PreviousDigest,
			PreviousSystemEpoch:          1, ActivationIndex: activationIndex,
			WaitedMillis: registryLayout.ServePermitMaxMillis,
		},
	})
	index++
	for _, shardID := range []uint32{^uint32(0), 0, 1} {
		state, _ = applySystem(t, state, index, SystemCommand{
			Type: SystemAdvanceTransition,
			Advance: &TransitionAdvance{
				ShardID: shardID, From: TransitionComplete, To: TransitionEpochRetired,
			},
		})
		index++
	}
	state, _ = applySystem(t, state, index, SystemCommand{Type: SystemFinalizeTransition})
	if state.Transition != nil || state.ActiveRegistryLayoutVersion != 2 || state.SystemEpoch != 2 ||
		state.ActiveRegistryLayoutDigest != transition.Digest {
		t.Fatalf("finalized transition = %+v", state)
	}
}

func TestPermitIdentityMismatchIsTyped(t *testing.T) {
	identity := PermitIdentity{
		ClusterID: "cluster-1", RegistryGeneration: "generation-1", SystemEpoch: 1,
		RegistryLayoutDigest: digestFor("registryLayout"),
	}
	grant := PermitGrant{
		PermitIdentity: identity, CommitIndex: 1, MaxLifetimeMillis: 100,
		ServeGate: true, WriteGate: true, CutoverGate: true, RecoveryClosed: true,
	}
	permit, _ := NewServePermit(grant, time.Now())
	identity.RegistryGeneration = "generation-2"
	if err := permit.Authorize(time.Now(), identity, PermitRegistryRead); !errors.Is(err, ErrPermitMismatch) {
		t.Fatalf("mismatch error = %v", err)
	}
}
