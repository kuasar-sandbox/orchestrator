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
	manifest := testManifest(4, "generation-1")
	digest, _ := manifest.Digest()
	state, _ := applySystem(t, SystemState{}, 1, SystemCommand{Type: SystemBootstrap, Manifest: &manifest, Digest: digest})
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
	closure := &GenerationClosure{
		TargetStorageGeneration: "generation-2", TargetManifestDigest: digestFor("generation-2-manifest"),
		Kind: RolloverConsensusClosure, ProofDigest: digestFor("closure"),
	}
	state, _ = applySystem(t, state, 4, SystemCommand{Type: SystemCloseGeneration, Closure: closure})
	if !state.Retired || state.ServeGate || state.WriteGate || state.Closure == nil || state.Closure.CommitIndex != 4 {
		t.Fatalf("closed generation = %+v", state)
	}
	_, rejected := ApplySystemCommand(state, 5, SystemCommand{Type: SystemRefreshPermit})
	if !rejected.Conflict {
		t.Fatal("retired generation renewed a permit")
	}
}

func TestSuccessorGenerationRequiresPermitDrainUnlessHardFenced(t *testing.T) {
	manifest := testManifest(4, "generation-2")
	manifest.Predecessor = &PredecessorProof{
		StorageGeneration: "generation-1", ManifestDigest: digestFor("old-manifest"),
		ServePermitMaxMillis: 5000, Kind: RolloverConsensusClosure, ProofDigest: digestFor("closure"),
	}
	digest, _ := manifest.Digest()
	state, _ := applySystem(t, SystemState{}, 1, SystemCommand{Type: SystemBootstrap, Manifest: &manifest, Digest: digest})
	_, rejected := ApplySystemCommand(state, 2, SystemCommand{
		Type: SystemSetGates, Gates: &GateUpdate{Serve: true, Write: true, Cutover: true},
	})
	if !rejected.Conflict {
		t.Fatal("successor served before predecessor permit drain")
	}
	_, rejected = ApplySystemCommand(state, 3, SystemCommand{Type: SystemConfirmDrain, Drain: &DrainConfirmation{
		PredecessorProofDigest: manifest.Predecessor.ProofDigest,
		WaitedMillis:           manifest.Predecessor.ServePermitMaxMillis - 1,
		EvidenceDigest:         digestFor("short-wait"),
	}})
	if !rejected.Conflict {
		t.Fatal("short predecessor permit wait was accepted")
	}
	state, _ = applySystem(t, state, 4, SystemCommand{Type: SystemConfirmDrain, Drain: &DrainConfirmation{
		PredecessorProofDigest: manifest.Predecessor.ProofDigest,
		WaitedMillis:           manifest.Predecessor.ServePermitMaxMillis,
		EvidenceDigest:         digestFor("monotonic-wait"),
	}})
	state, _ = applySystem(t, state, 5, SystemCommand{
		Type: SystemSetGates, Gates: &GateUpdate{Serve: true, Write: true, Cutover: true},
	})
	if !state.ServeGate {
		t.Fatal("drained successor did not open")
	}

	hardFenced := testManifest(4, "generation-3")
	hardFenced.Predecessor = &PredecessorProof{
		StorageGeneration: "generation-2", ManifestDigest: digest, ServePermitMaxMillis: 5000,
		Kind:        RolloverExternalFence,
		ProofDigest: digestFor("external-hard-fence"),
	}
	hardDigest, _ := hardFenced.Digest()
	hardState, _ := applySystem(t, SystemState{}, 1, SystemCommand{Type: SystemBootstrap, Manifest: &hardFenced, Digest: hardDigest})
	if !hardState.PredecessorDrainComplete {
		t.Fatal("complete hard fence did not satisfy predecessor drain")
	}
}

func TestRecoveryEpochClosesNormalServiceAndAdvancesSystemEpoch(t *testing.T) {
	manifest := testManifest(2, "generation-2")
	manifest.Predecessor = &PredecessorProof{
		StorageGeneration: "generation-1", ManifestDigest: digestFor("source-manifest"),
		ServePermitMaxMillis: 5000, Kind: RolloverExternalFence, ProofDigest: digestFor("hard-fence"),
	}
	digest, _ := manifest.Digest()
	state, _ := applySystem(t, SystemState{}, 1, SystemCommand{Type: SystemBootstrap, Manifest: &manifest, Digest: digest})
	state, _ = applySystem(t, state, 2, SystemCommand{
		Type: SystemSetGates, Gates: &GateUpdate{Serve: true, Write: true, Cutover: true},
	})
	recovery := &RecoveryEpoch{
		Epoch: 2, SourceClusterID: "cluster-1", SourceStorageGeneration: "generation-1",
		SourceManifestDigest: digestFor("source-manifest"), TargetStorageGeneration: "generation-2",
		TargetManifestDigest: digest, Phase: RecoveryPreparing,
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
	manifest := testManifest(2, "generation-2")
	manifest.Predecessor = &PredecessorProof{
		StorageGeneration: "generation-1", ManifestDigest: digestFor("source-manifest"),
		ServePermitMaxMillis: 5000, Kind: RolloverExternalFence, ProofDigest: digestFor("hard-fence"),
	}
	digest, _ := manifest.Digest()
	state, _ := applySystem(t, SystemState{}, 1, SystemCommand{Type: SystemBootstrap, Manifest: &manifest, Digest: digest})

	for name, mutate := range map[string]func(*RecoveryEpoch){
		"wrong epoch":      func(recovery *RecoveryEpoch) { recovery.Epoch++ },
		"wrong cluster":    func(recovery *RecoveryEpoch) { recovery.SourceClusterID = "other-cluster" },
		"wrong generation": func(recovery *RecoveryEpoch) { recovery.SourceStorageGeneration = "generation-0" },
		"wrong manifest":   func(recovery *RecoveryEpoch) { recovery.SourceManifestDigest = digestFor("other-manifest") },
	} {
		t.Run(name, func(t *testing.T) {
			recovery := RecoveryEpoch{
				Epoch: 2, SourceClusterID: "cluster-1", SourceStorageGeneration: "generation-1",
				SourceManifestDigest: digestFor("source-manifest"), TargetStorageGeneration: "generation-2",
				TargetManifestDigest: digest, Phase: RecoveryPreparing,
			}
			mutate(&recovery)
			_, result := ApplySystemCommand(state, 2, SystemCommand{Type: SystemBeginRecovery, Recovery: &recovery})
			if !result.Conflict {
				t.Fatal("recovery with uncommitted source identity was accepted")
			}
		})
	}
}

func TestManifestTransitionActivatesOnlyAfterEveryShardCompletes(t *testing.T) {
	manifest := testManifest(2, "generation-1")
	digest, _ := manifest.Digest()
	state, _ := applySystem(t, SystemState{}, 1, SystemCommand{Type: SystemBootstrap, Manifest: &manifest, Digest: digest})
	transition := &ManifestTransition{
		Version: 2, Digest: digestFor("manifest-2"), PreviousDigest: digest, NextSystemEpoch: 2,
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
	if state.Transition != nil || state.ActiveManifestVersion != 2 || state.SystemEpoch != 2 ||
		state.ActiveManifestDigest != transition.Digest {
		t.Fatalf("activated transition = %+v", state)
	}
}

func TestPermitIdentityMismatchIsTyped(t *testing.T) {
	identity := PermitIdentity{
		ClusterID: "cluster-1", StorageGeneration: "generation-1", SystemEpoch: 1,
		ManifestDigest: digestFor("manifest"),
	}
	grant := PermitGrant{
		PermitIdentity: identity, CommitIndex: 1, MaxLifetimeMillis: 100,
		ServeGate: true, WriteGate: true, CutoverGate: true, RecoveryClosed: true,
	}
	permit, _ := NewServePermit(grant, time.Now())
	identity.StorageGeneration = "generation-2"
	if err := permit.Authorize(time.Now(), identity, PermitRegistryRead); !errors.Is(err, ErrPermitMismatch) {
		t.Fatalf("mismatch error = %v", err)
	}
}
