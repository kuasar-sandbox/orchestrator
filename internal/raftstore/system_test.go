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
	successor := testManifest(4, "generation-2")
	successor.Predecessor = &PredecessorProof{
		StorageGeneration: manifest.StorageGeneration, ManifestDigest: digest,
		ServePermitMaxMillis: manifest.ServePermitMaxMillis, Kind: RolloverConsensusClosure,
	}
	intentDigest, err := successor.RolloverIntentDigest()
	if err != nil {
		t.Fatal(err)
	}
	closure := &GenerationClosure{
		TargetStorageGeneration:    successor.StorageGeneration,
		TargetManifestIntentDigest: intentDigest, Kind: RolloverConsensusClosure,
	}
	state, _ = applySystem(t, state, 4, SystemCommand{Type: SystemCloseGeneration, Closure: closure})
	if !state.Retired || state.ServeGate || state.WriteGate || state.Closure == nil || state.Closure.CommitIndex != 4 ||
		!isSHA256(state.Closure.ProofDigest) || state.Closure.ProofDigest != generationClosureProofDigest(state, *state.Closure) {
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
	predecessor := testManifest(4, "generation-1")
	predecessorDigest, _ := predecessor.Digest()
	manifest, _ := finalizeConsensusSuccessor(t, predecessor, predecessorDigest, testManifest(4, "generation-2"))
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

func TestSystemGroupRejectsNonInitialManifestBootstrap(t *testing.T) {
	manifest := testManifest(2, "generation-1")
	manifest.ManifestVersion = 2
	manifest.PreviousManifestDigest = digestFor("manifest-1")
	digest, err := manifest.Digest()
	if err != nil {
		t.Fatal(err)
	}
	_, result := ApplySystemCommand(SystemState{}, 1, SystemCommand{
		Type: SystemBootstrap, Manifest: &manifest, Digest: digest,
	})
	if !result.Conflict {
		t.Fatal("System Group accepted a non-initial manifest as empty bootstrap")
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

func TestNodeRegistrationRequiresExplicitEnrollmentAndFencesEpochRollback(t *testing.T) {
	manifest := testManifest(2, "generation-1")
	digest, _ := manifest.Digest()
	state, _ := applySystem(t, SystemState{}, 1, SystemCommand{
		Type: SystemBootstrap, Manifest: &manifest, Digest: digest,
	})
	registration := &NodeRegistrationCommand{
		NodeID: "node-1", EnrollmentID: "enrollment-1", NodeEpoch: 1,
		DataEndpoint: "10.0.0.1:8443", RuntimeDigest: "runtime-v1",
		Labels: map[string]string{"pool": "default"}, FailureDomain: "zone-a",
		LoadModelVersion: 1, SandboxSlots: 64, BuildSlots: 2,
		BuildCPU: 4000, BuildMemory: 8 << 30, BuildStorage: 100 << 30,
	}
	if _, result := ApplySystemCommand(state, 2, SystemCommand{
		Type: SystemAcceptNodeRegistration, Registration: registration,
	}); !result.Conflict {
		t.Fatal("ordinary registration created an unknown enrollment")
	}
	state, _ = applySystem(t, state, 3, SystemCommand{Type: SystemEnrollNode, Enrollment: &NodeEnrollmentCommand{
		NodeID: registration.NodeID, EnrollmentID: registration.EnrollmentID,
		NodeEpoch: registration.NodeEpoch, DataEndpoint: registration.DataEndpoint,
	}})
	state, _ = applySystem(t, state, 4, SystemCommand{
		Type: SystemAcceptNodeRegistration, Registration: registration,
	})
	record := state.NodeEnrollments[registration.NodeID]
	if record.Catalog == nil || record.Catalog.SandboxSlots != 64 || record.MaxNodeEpoch != 1 {
		t.Fatalf("accepted enrollment = %+v", record)
	}

	wrongEndpoint := *registration
	wrongEndpoint.DataEndpoint = "10.0.0.2:8443"
	if _, result := ApplySystemCommand(state, 5, SystemCommand{
		Type: SystemAcceptNodeRegistration, Registration: &wrongEndpoint,
	}); !result.Conflict {
		t.Fatal("same NodeEpoch changed data endpoint")
	}
	newEpoch := wrongEndpoint
	newEpoch.NodeEpoch = 2
	state, _ = applySystem(t, state, 6, SystemCommand{
		Type: SystemAcceptNodeRegistration, Registration: &newEpoch,
	})
	if state.NodeEnrollments[registration.NodeID].MaxNodeEpoch != 2 ||
		state.NodeEnrollments[registration.NodeID].DataEndpoint != newEpoch.DataEndpoint {
		t.Fatalf("new NodeEpoch was not committed: %+v", state.NodeEnrollments[registration.NodeID])
	}
	if _, result := ApplySystemCommand(state, 7, SystemCommand{
		Type: SystemAcceptNodeRegistration, Registration: registration,
	}); !result.Conflict {
		t.Fatal("older NodeEpoch registered after a newer epoch")
	}
	state, _ = applySystem(t, state, 8, SystemCommand{Type: SystemRetireNode, Retirement: &NodeRetirementCommand{
		NodeID: registration.NodeID, EnrollmentID: registration.EnrollmentID, LastNodeEpoch: 2,
	}})
	if !state.NodeEnrollments[registration.NodeID].Retired {
		t.Fatal("node enrollment retirement was not permanent")
	}
	if _, result := ApplySystemCommand(state, 9, SystemCommand{Type: SystemEnrollNode, Enrollment: &NodeEnrollmentCommand{
		NodeID: registration.NodeID, EnrollmentID: "replacement", NodeEpoch: 1, DataEndpoint: "10.0.0.3:8443",
	}}); !result.Conflict {
		t.Fatal("retired node ID was re-enrolled")
	}
}

func TestRecoveryNodeProgressIsDurableAndPhaseBound(t *testing.T) {
	manifest := testManifest(2, "generation-2")
	manifest.Predecessor = &PredecessorProof{
		StorageGeneration: "generation-1", ManifestDigest: digestFor("source-manifest"),
		ServePermitMaxMillis: 5000, Kind: RolloverExternalFence, ProofDigest: digestFor("hard-fence"),
	}
	digest, _ := manifest.Digest()
	state, _ := applySystem(t, SystemState{}, 1, SystemCommand{
		Type: SystemBootstrap, Manifest: &manifest, Digest: digest,
	})
	state, _ = applySystem(t, state, 2, SystemCommand{Type: SystemEnrollNode, Enrollment: &NodeEnrollmentCommand{
		NodeID: "node-1", EnrollmentID: "enrollment-1", NodeEpoch: 7, DataEndpoint: "10.0.0.1:8443",
	}})
	state, _ = applySystem(t, state, 3, SystemCommand{Type: SystemAcceptNodeRegistration, Registration: &NodeRegistrationCommand{
		NodeID: "node-1", EnrollmentID: "enrollment-1", NodeEpoch: 7, DataEndpoint: "10.0.0.1:8443",
		LoadModelVersion: 1, SandboxSlots: 64,
	}})
	recovery := &RecoveryEpoch{
		Epoch: 2, SourceClusterID: "cluster-1", SourceStorageGeneration: "generation-1",
		SourceManifestDigest: digestFor("source-manifest"), TargetStorageGeneration: "generation-2",
		TargetManifestDigest: digest, Phase: RecoveryPreparing,
		Nodes: map[string]RecoveryNodeProgress{"node-1": {
			NodeID: "node-1", EnrollmentID: "enrollment-1", NodeEpoch: 7, State: RecoveryNodeExpected,
		}},
	}
	state, _ = applySystem(t, state, 4, SystemCommand{Type: SystemBeginRecovery, Recovery: recovery})
	state, _ = applySystem(t, state, 5, SystemCommand{Type: SystemAdvanceRecovery, RecoveryAdvance: &RecoveryAdvance{
		From: RecoveryPreparing, To: RecoveryCollecting,
	}})
	if _, result := ApplySystemCommand(state, 6, SystemCommand{Type: SystemAdvanceRecovery, RecoveryAdvance: &RecoveryAdvance{
		From: RecoveryCollecting, To: RecoveryReconciling,
	}}); !result.Conflict {
		t.Fatal("recovery advanced before the expected node reported")
	}
	state, _ = applySystem(t, state, 7, SystemCommand{Type: SystemUpdateRecoveryNode, RecoveryNode: &RecoveryNodeUpdate{
		NodeID: "node-1", EnrollmentID: "enrollment-1", NodeEpoch: 7, SessionSeq: 11,
		From: RecoveryNodeExpected, To: RecoveryNodeCollecting,
	}})
	state, _ = applySystem(t, state, 8, SystemCommand{Type: SystemUpdateRecoveryNode, RecoveryNode: &RecoveryNodeUpdate{
		NodeID: "node-1", EnrollmentID: "enrollment-1", NodeEpoch: 7, SessionSeq: 11,
		From: RecoveryNodeCollecting, To: RecoveryNodeReported,
		ReportDigest: digestFor("report"), ReportedObjects: 3,
	}})
	state, _ = applySystem(t, state, 9, SystemCommand{Type: SystemAdvanceRecovery, RecoveryAdvance: &RecoveryAdvance{
		From: RecoveryCollecting, To: RecoveryReconciling,
	}})
	state, _ = applySystem(t, state, 10, SystemCommand{Type: SystemUpdateRecoveryNode, RecoveryNode: &RecoveryNodeUpdate{
		NodeID: "node-1", EnrollmentID: "enrollment-1", NodeEpoch: 7, SessionSeq: 11,
		From: RecoveryNodeReported, To: RecoveryNodeReconciled,
		ReportDigest: digestFor("report"), ReportedObjects: 3, ResolvedObjects: 2, ConflictObjects: 1,
	}})
	state, _ = applySystem(t, state, 11, SystemCommand{Type: SystemAdvanceRecovery, RecoveryAdvance: &RecoveryAdvance{
		From: RecoveryReconciling, To: RecoveryFinalizing,
	}})
	state, _ = applySystem(t, state, 12, SystemCommand{Type: SystemAdvanceRecovery, RecoveryAdvance: &RecoveryAdvance{
		From: RecoveryFinalizing, To: RecoveryClosed,
	}})
	if state.Recovery != nil || state.SystemEpoch != 3 {
		t.Fatalf("recovery did not close after durable reconciliation: %+v", state)
	}
}

func TestRecoveryNodeReportIdentityIsImmutable(t *testing.T) {
	current := RecoveryNodeProgress{
		NodeID: "node-1", EnrollmentID: "enrollment-1", NodeEpoch: 7, SessionSeq: 11,
		State: RecoveryNodeReported, ReportDigest: digestFor("report-1"), ReportedObjects: 3,
		LastAppliedIndex: 1,
	}
	changedReport := RecoveryNodeUpdate{
		NodeID: current.NodeID, EnrollmentID: current.EnrollmentID, NodeEpoch: current.NodeEpoch,
		SessionSeq: current.SessionSeq, To: RecoveryNodeReported,
		ReportDigest: digestFor("report-2"), ReportedObjects: current.ReportedObjects,
	}
	if validRecoveryNodeAdvance(current, changedReport) {
		t.Fatal("an accepted recovery report changed identity in place")
	}
	incompleteQuarantine := RecoveryNodeUpdate{
		NodeID: current.NodeID, EnrollmentID: current.EnrollmentID, NodeEpoch: current.NodeEpoch,
		SessionSeq: current.SessionSeq, To: RecoveryNodeQuarantined,
		ReportDigest: current.ReportDigest, ReportedObjects: current.ReportedObjects, ConflictObjects: 1,
	}
	if validRecoveryNodeAdvance(current, incompleteQuarantine) {
		t.Fatal("an unresolved report was treated as fully quarantined")
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
	activationIndex := index
	if state.Transition == nil || !state.Transition.Activated ||
		state.Transition.ActivationIndex != activationIndex || state.ActiveManifestVersion != 2 ||
		state.SystemEpoch != 2 || state.ActiveManifestDigest != transition.Digest {
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
			PreviousManifestDigest: transition.PreviousDigest,
			PreviousSystemEpoch:    1, ActivationIndex: activationIndex,
			WaitedMillis: manifest.ServePermitMaxMillis,
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
	if state.Transition != nil || state.ActiveManifestVersion != 2 || state.SystemEpoch != 2 ||
		state.ActiveManifestDigest != transition.Digest {
		t.Fatalf("finalized transition = %+v", state)
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
