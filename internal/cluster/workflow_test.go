package cluster

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
)

func TestRouteWorkflowTypesValidateFrozenIntent(t *testing.T) {
	intent, err := NewDispatchIntent([]byte(`{"memory":268435456}`), []byte(`{"template":"e2b-snp-t1"}`), "provider-v1/policy-v2")
	if err != nil {
		t.Fatal(err)
	}
	binding := workflowBinding(t, ExecutionKindSandbox, "s1", "rk", intent)
	selected := uint32(0)
	starting := RouteWorkflowRecord{
		Group: "/g", RouteKey: "rk", State: WorkflowRouteStarting,
		Revision: Revision{StorageGeneration: "g1", ShardID: 7, LogIndex: 11},
		Starting: &RouteStartingState{
			SandboxID: "s1", PlacementRound: 1,
			CandidatePool:     []PlacementCandidate{{NodeID: "n1"}, {NodeID: "n2"}},
			SelectedCandidate: &selected, Intent: intent, Binding: &binding,
		},
	}
	if err := starting.Validate(); err != nil {
		t.Fatal(err)
	}
	starting.Ready = readyRoute()
	if err := starting.Validate(); err == nil {
		t.Fatal("Route union accepted two states")
	}
	starting.Ready = nil
	starting.Starting.DefinitivelyRejected = []uint32{0}
	if err := starting.Validate(); err == nil {
		t.Fatal("selected candidate was also definitively rejected")
	}
	starting.Starting.DefinitivelyRejected = nil
	starting.Group = "/another-group"
	if err := starting.Validate(); err == nil {
		t.Fatal("Binding from another group was accepted")
	}
	starting.Group = "/g"
	starting.Starting.Intent.DispatchSpec = []byte("mutated")
	if err := starting.Validate(); err == nil {
		t.Fatal("Binding for another immutable dispatch intent was accepted")
	}
}

func TestReadyRouteAndRevisionValidation(t *testing.T) {
	record := RouteWorkflowRecord{
		Group: "/g", RouteKey: "rk", State: WorkflowRouteReady,
		Revision: Revision{StorageGeneration: "g1", ShardID: 9, LogIndex: 100},
		Ready:    readyRoute(),
	}
	if err := record.Validate(); err != nil {
		t.Fatal(err)
	}
	if !record.Revision.AtLeast(Revision{StorageGeneration: "g1", ShardID: 9, LogIndex: 99}) {
		t.Fatal("newer revision did not satisfy minimum")
	}
	if record.Revision.AtLeast(Revision{StorageGeneration: "g2", ShardID: 9, LogIndex: 1}) {
		t.Fatal("revision compared across storage generations")
	}
	record.Ready.LastEventSeq = 0
	if err := record.Validate(); err == nil {
		t.Fatal("READY accepted without durable event watermark")
	}
	record.Ready.LastEventSeq = 3
	record.Ready.StorageGeneration = "g2"
	if err := record.Validate(); err == nil {
		t.Fatal("READY from another storage generation was accepted")
	}
}

func TestDispatchIntentRejectsMutationAndOversize(t *testing.T) {
	intent, err := NewDispatchIntent([]byte("demand"), []byte("spec"), "v1")
	if err != nil {
		t.Fatal(err)
	}
	intent.DispatchSpec[0] ^= 1
	if err := intent.Validate(); err == nil {
		t.Fatal("mutated dispatch spec accepted")
	}
	if _, err := NewDispatchIntent([]byte("demand"), []byte(strings.Repeat("x", MaxDispatchSpecBytes+1)), "v1"); err == nil {
		t.Fatal("oversized dispatch spec accepted")
	}
}

func TestPlacementFailuresDoNotInventExecutionProof(t *testing.T) {
	intent, err := NewDispatchIntent([]byte("demand"), []byte("spec"), "v1")
	if err != nil {
		t.Fatal(err)
	}
	candidates := []PlacementCandidate{{NodeID: "n1"}, {NodeID: "n2"}}
	rejected := []uint32{0, 1}
	route := RouteWorkflowRecord{
		Group: "/g", RouteKey: "rk", State: WorkflowRouteTombstone,
		Revision: Revision{StorageGeneration: "g1", ShardID: 1, LogIndex: 10},
		Tombstone: &RouteTombstoneState{PlacementFailure: &RoutePlacementFailureState{
			SandboxID: "s1", PlacementRound: 2, CandidatePool: candidates,
			DefinitivelyRejected: rejected, Intent: intent, Reason: "candidate rounds exhausted",
		}},
	}
	if err := route.Validate(); err != nil {
		t.Fatal(err)
	}
	route.Tombstone.PlacementFailure.DefinitivelyRejected = []uint32{0}
	if err := route.Validate(); err == nil {
		t.Fatal("Route placement failure accepted a non-exhausted pool")
	}

	build := BuildRecord{
		Group: "/g", BuildID: "b1", State: BuildError,
		Revision: Revision{StorageGeneration: "g1", ShardID: 2, LogIndex: 20},
		Failure: &BuildPlacementFailureState{
			BuildID: "b1", CandidatePool: candidates, DefinitivelyRejected: rejected,
			Intent: intent, Reason: "candidate pool exhausted",
		},
	}
	if err := build.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestDispatchOutcomeRejectsUnknownValue(t *testing.T) {
	for _, outcome := range []DispatchOutcome{
		DispatchAcceptedAdmitted, DispatchAcceptedQueued, DispatchDefinitiveReject,
		DispatchSessionMoved, DispatchConflict, DispatchWrongBinding, DispatchUnknown,
	} {
		if err := outcome.Validate(); err != nil {
			t.Fatalf("%s: %v", outcome, err)
		}
	}
	if err := DispatchOutcome("TIMEOUT").Validate(); err == nil {
		t.Fatal("unknown dispatch outcome was accepted")
	}
}

func TestExecutionFenceCompactionRequiresEveryProof(t *testing.T) {
	proofDigest := sha256.Sum256([]byte("terminal proof"))
	bindingDigest := sha256.Sum256([]byte("binding"))
	fence := ExecutionFence{
		Group: "/g", RouteKey: "rk", SandboxID: "s1", NodeID: "n1", NodeEpoch: 7,
		StorageGeneration: "g1", BindingDigest: strings.ToLower(strings.Repeat("0", 64)),
		LastEventSeq: 9, FinalOutboxWatermark: 9,
		Proof: TerminalProof{
			Kind: ProofNodeTerminal, ProofDigest: hexDigest(proofDigest), FencedNodeID: "n1", FencedNodeEpoch: 7,
		},
		Revision: Revision{StorageGeneration: "g1", ShardID: 1, LogIndex: 20},
	}
	fence.BindingDigest = hexDigest(bindingDigest)
	all := FenceCompactionProof{
		TerminalProofCommitted: true, FinalOutboxWatermarkAcked: true,
		AllReplicasApplied: true, MinimumRetentionElapsed: true,
	}
	if !CanCompactExecutionFence(fence, all) {
		t.Fatal("complete fence proof did not compact")
	}
	all.AllReplicasApplied = false
	if CanCompactExecutionFence(fence, all) {
		t.Fatal("fence compacted before every replica applied")
	}
	all.AllReplicasApplied = true
	all.FinalOutboxWatermarkAcked = false
	if CanCompactExecutionFence(fence, all) {
		t.Fatal("fence compacted without outbox ACK or epoch fence")
	}
	all.NodeEpochPermanentlyFenced = true
	if !CanCompactExecutionFence(fence, all) {
		t.Fatal("permanent NodeEpoch fence did not replace final outbox ACK")
	}
}

func workflowBinding(t *testing.T, kind ExecutionKind, objectID, routeKey string, intent DispatchIntent) ExecutionBindingIntent {
	t.Helper()
	var demand, dispatch [sha256.Size]byte
	demandBytes, _ := hex.DecodeString(intent.DemandDigest)
	dispatchBytes, _ := hex.DecodeString(intent.DispatchSpecDigest)
	copy(demand[:], demandBytes)
	copy(dispatch[:], dispatchBytes)
	opaque, err := EncodeExecutionBinding(ExecutionBinding{
		StorageGeneration: "g1", Kind: kind, ObjectID: objectID, Group: "/g", RouteKey: routeKey,
		NodeID: "n1", NodeEpoch: 7, DemandDigest: demand, DispatchSpecDigest: dispatch,
	})
	if err != nil {
		t.Fatal(err)
	}
	digest, err := ExecutionBindingDigest(opaque)
	if err != nil {
		t.Fatal(err)
	}
	return ExecutionBindingIntent{
		NodeID: "n1", NodeEpoch: 7, DataEndpoint: "10.0.0.1:8443",
		StorageGeneration: "g1", OpaqueBinding: opaque, BindingDigest: digest,
	}
}

func readyRoute() *ReadyRoute {
	binding := sha256.Sum256([]byte("binding"))
	return &ReadyRoute{
		SandboxID: "s1", NodeID: "n1", NodeEpoch: 7, DataEndpoint: "10.0.0.1:8443",
		AccessToken: "token", TemplateRef: "e2b-snp-t1", StorageGeneration: "g1",
		BindingDigest: hexDigest(binding), LastEventSeq: 3,
	}
}

func hexDigest(value [sha256.Size]byte) string {
	return hex.EncodeToString(value[:])
}
