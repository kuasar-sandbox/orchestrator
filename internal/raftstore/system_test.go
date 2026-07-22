package raftstore

import (
	"errors"
	"strings"
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
	_, rejected = ApplySystemCommand(state, 5, SystemCommand{
		Type: SystemSetGates, Gates: &GateUpdate{Serve: true, Write: true, Cutover: true},
	})
	if !rejected.Conflict {
		t.Fatal("successor served before generation recovery")
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
	if _, result := ApplySystemCommand(hardState, 2, SystemCommand{
		Type: SystemSetGates, Gates: &GateUpdate{Serve: true, Write: true, Cutover: true},
	}); !result.Conflict {
		t.Fatal("hard-fenced successor served before generation recovery")
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

func TestRecoveryEpochClosesNormalServiceOnThePreparedSystemEpoch(t *testing.T) {
	registryLayout := testRegistryLayout(2, "generation-2")
	registryLayout.Predecessor = &PredecessorProof{
		RegistryGeneration: "generation-1", RegistryLayoutDigest: digestFor("source-registryLayout"),
		ServePermitMaxMillis: 5000, Kind: RolloverExternalFence, ProofDigest: digestFor("hard-fence"),
	}
	digest, _ := registryLayout.Digest()
	state, _ := applySystem(t, SystemState{}, 1, SystemCommand{Type: SystemBootstrap, RegistryLayout: &registryLayout, Digest: digest})
	if _, result := ApplySystemCommand(state, 2, SystemCommand{
		Type: SystemSetGates, Gates: &GateUpdate{Serve: true, Write: true, Cutover: true},
	}); !result.Conflict {
		t.Fatal("successor served before recovery")
	}
	recovery := &RecoveryEpoch{
		Epoch: 2, SourceClusterID: "cluster-1", SourceRegistryGeneration: "generation-1",
		SourceRegistryLayoutDigest: digestFor("source-registryLayout"), TargetRegistryGeneration: "generation-2",
		TargetRegistryLayoutDigest: digest, Phase: RecoveryPreparing,
	}
	state, _ = applySystem(t, state, 3, SystemCommand{Type: SystemBeginRecovery, Recovery: recovery})
	if state.Recovery == nil || state.ServeGate || state.SystemEpoch != 2 {
		t.Fatalf("open recovery state = %+v", state)
	}
	_, result := ApplySystemCommand(state, 4, SystemCommand{
		Type:            SystemAdvanceRecovery,
		RecoveryAdvance: &RecoveryAdvance{From: RecoveryPreparing, To: RecoveryCollecting},
	})
	if !result.Conflict {
		t.Fatal("recovery advanced while a previously issued Permit could remain valid")
	}
	state, _ = applySystem(t, state, 5, SystemCommand{
		Type: SystemConfirmRecoveryDrain,
		RecoveryDrain: &RecoveryDrainConfirmation{
			RecoveryEpoch: 2, TargetRegistryGeneration: "generation-2",
			TargetRegistryLayoutDigest: digest, WaitedMillis: registryLayout.ServePermitMaxMillis,
		},
	})
	phases := []RecoveryPhase{RecoveryCollecting, RecoveryReconciling, RecoveryFinalizing, RecoveryClosed}
	from := RecoveryPreparing
	index := uint64(6)
	for _, to := range phases {
		state, _ = applySystem(t, state, index, SystemCommand{
			Type: SystemAdvanceRecovery, RecoveryAdvance: &RecoveryAdvance{From: from, To: to},
		})
		from = to
		index++
	}
	if state.Recovery != nil || state.RecoveryCompletion == nil || state.SystemEpoch != 3 || state.ServeGate ||
		state.RecoveryCompletion.Epoch != 2 || state.RecoveryCompletion.FinalizedIndex != 9 {
		t.Fatalf("closed recovery state = %+v", state)
	}
	state, _ = applySystem(t, state, 10, SystemCommand{
		Type: SystemSetGates, Gates: &GateUpdate{Serve: true, Write: true, Cutover: true},
	})
	if !state.ServeGate || !state.WriteGate || !state.CutoverGate {
		t.Fatal("completed successor recovery did not open serving gates")
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

func TestNodeRegistrationRequiresExplicitEnrollmentAndFencesEpochRollback(t *testing.T) {
	registryLayout := testRegistryLayout(2, "generation-1")
	digest, _ := registryLayout.Digest()
	state, _ := applySystem(t, SystemState{}, 1, SystemCommand{
		Type: SystemBootstrap, RegistryLayout: &registryLayout, Digest: digest,
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
	for name, mutate := range map[string]func(*NodeRegistrationCommand){
		"runtime":    func(value *NodeRegistrationCommand) { value.RuntimeDigest = "runtime-v2" },
		"labels":     func(value *NodeRegistrationCommand) { value.Labels = map[string]string{"pool": "other"} },
		"capability": func(value *NodeRegistrationCommand) { value.Capabilities = map[string]bool{"build": true} },
		"capacity":   func(value *NodeRegistrationCommand) { value.SandboxSlots++ },
	} {
		t.Run(name, func(t *testing.T) {
			changed := *registration
			mutate(&changed)
			if _, result := ApplySystemCommand(state, 5, SystemCommand{
				Type: SystemAcceptNodeRegistration, Registration: &changed,
			}); !result.Conflict {
				t.Fatal("same NodeEpoch changed stable catalog fields")
			}
		})
	}
	draining := *registration
	draining.Draining = true
	state, _ = applySystem(t, state, 5, SystemCommand{
		Type: SystemAcceptNodeRegistration, Registration: &draining,
	})
	if !state.NodeEnrollments[registration.NodeID].Catalog.Draining {
		t.Fatal("same NodeEpoch could not update the mutable draining hint")
	}

	wrongEndpoint := *registration
	wrongEndpoint.DataEndpoint = "10.0.0.2:8443"
	if _, result := ApplySystemCommand(state, 6, SystemCommand{
		Type: SystemAcceptNodeRegistration, Registration: &wrongEndpoint,
	}); !result.Conflict {
		t.Fatal("same NodeEpoch changed data endpoint")
	}
	newEpoch := wrongEndpoint
	newEpoch.NodeEpoch = 2
	state, _ = applySystem(t, state, 7, SystemCommand{
		Type: SystemAcceptNodeRegistration, Registration: &newEpoch,
	})
	if state.NodeEnrollments[registration.NodeID].MaxNodeEpoch != 2 ||
		state.NodeEnrollments[registration.NodeID].DataEndpoint != newEpoch.DataEndpoint {
		t.Fatalf("new NodeEpoch was not committed: %+v", state.NodeEnrollments[registration.NodeID])
	}
	if _, result := ApplySystemCommand(state, 8, SystemCommand{
		Type: SystemAcceptNodeRegistration, Registration: registration,
	}); !result.Conflict {
		t.Fatal("older NodeEpoch registered after a newer epoch")
	}
	state, _ = applySystem(t, state, 9, SystemCommand{Type: SystemRetireNode, Retirement: &NodeRetirementCommand{
		NodeID: registration.NodeID, EnrollmentID: registration.EnrollmentID, LastNodeEpoch: 2,
	}})
	if !state.NodeEnrollments[registration.NodeID].Retired {
		t.Fatal("node enrollment retirement was not permanent")
	}
	state, result := ApplySystemCommand(state, 10, SystemCommand{Type: SystemRetireNode, Retirement: &NodeRetirementCommand{
		NodeID: registration.NodeID, EnrollmentID: registration.EnrollmentID, LastNodeEpoch: 2,
	}})
	if result.Conflict || !result.Applied || !state.NodeEnrollments[registration.NodeID].Retired {
		t.Fatalf("exact node retirement retry = %+v, %+v", state.NodeEnrollments[registration.NodeID], result)
	}
	if _, result := ApplySystemCommand(state, 11, SystemCommand{Type: SystemEnrollNode, Enrollment: &NodeEnrollmentCommand{
		NodeID: registration.NodeID, EnrollmentID: "replacement", NodeEpoch: 1, DataEndpoint: "10.0.0.3:8443",
	}}); !result.Conflict {
		t.Fatal("retired node ID was re-enrolled")
	}
}

func TestExactNodeEnrollmentRetryIsIdempotent(t *testing.T) {
	registryLayout := testRegistryLayout(1, "generation-enrollment-retry")
	digest, _ := registryLayout.Digest()
	state, _ := applySystem(t, SystemState{}, 1, SystemCommand{
		Type: SystemBootstrap, RegistryLayout: &registryLayout, Digest: digest,
	})
	enrollment := &NodeEnrollmentCommand{
		NodeID: "node-1", EnrollmentID: "enrollment-1", NodeEpoch: 7, DataEndpoint: "10.0.0.1:8443",
	}
	state, _ = applySystem(t, state, 2, SystemCommand{Type: SystemEnrollNode, Enrollment: enrollment})
	original := state.NodeEnrollments[enrollment.NodeID]
	state, result := ApplySystemCommand(state, 3, SystemCommand{Type: SystemEnrollNode, Enrollment: enrollment})
	if result.Conflict || !result.Applied || state.NodeEnrollments[enrollment.NodeID] != original {
		t.Fatalf("exact enrollment retry = %+v, %+v", state.NodeEnrollments[enrollment.NodeID], result)
	}
	mismatch := *enrollment
	mismatch.NodeEpoch++
	if _, result := ApplySystemCommand(state, 4, SystemCommand{
		Type: SystemEnrollNode, Enrollment: &mismatch,
	}); !result.Conflict {
		t.Fatal("mismatched enrollment retry was accepted")
	}
}

func TestNodeCatalogRecordHasSyncableByteBound(t *testing.T) {
	record := NodeCatalogRecord{
		LoadModelVersion: 1, SandboxSlots: 1, CatalogVersion: 1, LastRegistrationIndex: 1,
		Labels: map[string]string{"large": strings.Repeat("x", MaxNodeCatalogRecordBytes)},
	}
	if err := record.Validate(1); err == nil {
		t.Fatal("unsyncable node catalog record was accepted")
	}
}

func TestRecoveryNodeProgressIsDurableAndPhaseBound(t *testing.T) {
	registryLayout := testRegistryLayout(2, "generation-2")
	registryLayout.Predecessor = &PredecessorProof{
		RegistryGeneration: "generation-1", RegistryLayoutDigest: digestFor("source-registryLayout"),
		ServePermitMaxMillis: 5000, Kind: RolloverExternalFence, ProofDigest: digestFor("hard-fence"),
	}
	digest, _ := registryLayout.Digest()
	state, _ := applySystem(t, SystemState{}, 1, SystemCommand{
		Type: SystemBootstrap, RegistryLayout: &registryLayout, Digest: digest,
	})
	state, _ = applySystem(t, state, 2, SystemCommand{Type: SystemEnrollNode, Enrollment: &NodeEnrollmentCommand{
		NodeID: "node-1", EnrollmentID: "enrollment-1", NodeEpoch: 7, DataEndpoint: "10.0.0.1:8443",
	}})
	state, _ = applySystem(t, state, 3, SystemCommand{Type: SystemAcceptNodeRegistration, Registration: &NodeRegistrationCommand{
		NodeID: "node-1", EnrollmentID: "enrollment-1", NodeEpoch: 7, DataEndpoint: "10.0.0.1:8443",
		LoadModelVersion: 1, SandboxSlots: 64,
	}})
	recovery := &RecoveryEpoch{
		Epoch: 2, SourceClusterID: "cluster-1", SourceRegistryGeneration: "generation-1",
		SourceRegistryLayoutDigest: digestFor("source-registryLayout"), TargetRegistryGeneration: "generation-2",
		TargetRegistryLayoutDigest: digest, Phase: RecoveryPreparing,
		Nodes: map[string]RecoveryNodeProgress{"node-1": {
			NodeID: "node-1", EnrollmentID: "enrollment-1", NodeEpoch: 7, State: RecoveryNodeExpected,
		}},
	}
	state, _ = applySystem(t, state, 4, SystemCommand{Type: SystemBeginRecovery, Recovery: recovery})
	if _, result := ApplySystemCommand(state, 5, SystemCommand{Type: SystemRetireNode, Retirement: &NodeRetirementCommand{
		NodeID: "node-1", EnrollmentID: "enrollment-1", LastNodeEpoch: 7,
	}}); !result.Conflict {
		t.Fatal("node retirement changed the frozen recovery enrollment set")
	}
	state, _ = applySystem(t, state, 5, SystemCommand{Type: SystemConfirmRecoveryDrain, RecoveryDrain: &RecoveryDrainConfirmation{
		RecoveryEpoch: 2, TargetRegistryGeneration: "generation-2",
		TargetRegistryLayoutDigest: digest, WaitedMillis: registryLayout.ServePermitMaxMillis,
	}})
	state, _ = applySystem(t, state, 6, SystemCommand{Type: SystemAdvanceRecovery, RecoveryAdvance: &RecoveryAdvance{
		From: RecoveryPreparing, To: RecoveryCollecting,
	}})
	if _, result := ApplySystemCommand(state, 7, SystemCommand{Type: SystemAdvanceRecovery, RecoveryAdvance: &RecoveryAdvance{
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
		From: RecoveryNodeReported, To: RecoveryNodeQuarantined,
		ReportDigest: digestFor("report"), ReportedObjects: 3, ResolvedObjects: 2, ConflictObjects: 1,
		ResolutionProofDigest: digestFor("conflict-proof"), ResolutionReason: "one object is quarantined",
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
