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
		Revision: Revision{RegistryGeneration: "g1", ShardID: 7, LogIndex: 11},
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
		Revision: Revision{RegistryGeneration: "g1", ShardID: 9, LogIndex: 100},
		Ready:    readyRoute(),
	}
	if err := record.Validate(); err != nil {
		t.Fatal(err)
	}
	if !record.Revision.AtLeast(Revision{RegistryGeneration: "g1", ShardID: 9, LogIndex: 99}) {
		t.Fatal("newer revision did not satisfy minimum")
	}
	if record.Revision.AtLeast(Revision{RegistryGeneration: "g2", ShardID: 9, LogIndex: 1}) {
		t.Fatal("revision compared across Registry History Generations")
	}
	record.Ready.LastEventSeq = 0
	if err := record.Validate(); err == nil {
		t.Fatal("READY accepted without durable event watermark")
	}
	record.Ready.LastEventSeq = 3
	record.Ready.RegistryGeneration = "g2"
	if err := record.Validate(); err == nil {
		t.Fatal("READY from another Registry History Generation was accepted")
	}
}

func TestRouteTombstoneCarriesExactExecutionFence(t *testing.T) {
	binding := sha256.Sum256([]byte("binding"))
	record := RouteWorkflowRecord{
		Group: "/g", RouteKey: "rk", State: WorkflowRouteTombstone,
		Revision: Revision{RegistryGeneration: "g1", ShardID: 9, LogIndex: 101},
		Tombstone: &RouteTombstoneState{
			SandboxID: "s1", NodeID: "n1", NodeEpoch: 7, RegistryGeneration: "g1",
			BindingDigest: hexDigest(binding), LastEventSeq: 4, TerminalReason: "deleted",
			Proof:           TerminalProof{Kind: ProofNodeTerminal, ProofDigest: hexDigest(sha256.Sum256([]byte("proof"))), FencedNodeID: "n1", FencedNodeEpoch: 7},
			FailureRevision: Revision{RegistryGeneration: "g1", ShardID: 9, LogIndex: 100},
		},
	}
	if err := record.Validate(); err != nil {
		t.Fatal(err)
	}
	record.Tombstone.RegistryGeneration = "g2"
	if err := record.Validate(); err == nil {
		t.Fatal("Route tombstone from another Registry History Generation was accepted")
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
		Revision: Revision{RegistryGeneration: "g1", ShardID: 1, LogIndex: 10},
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
		Revision: Revision{RegistryGeneration: "g1", ShardID: 2, LogIndex: 20},
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

func workflowBinding(t *testing.T, kind ExecutionKind, objectID, routeKey string, intent DispatchIntent) ExecutionBindingIntent {
	t.Helper()
	var demand, dispatch [sha256.Size]byte
	demandBytes, _ := hex.DecodeString(intent.DemandDigest)
	dispatchBytes, _ := hex.DecodeString(intent.DispatchSpecDigest)
	copy(demand[:], demandBytes)
	copy(dispatch[:], dispatchBytes)
	opaque, err := EncodeExecutionBinding(ExecutionBinding{
		RegistryGeneration: "g1", Kind: kind, ObjectID: objectID, Group: "/g", RouteKey: routeKey,
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
		RegistryGeneration: "g1", OpaqueBinding: opaque, BindingDigest: digest,
	}
}

func readyRoute() *ReadyRoute {
	binding := sha256.Sum256([]byte("binding"))
	return &ReadyRoute{
		SandboxID: "s1", NodeID: "n1", NodeEpoch: 7, DataEndpoint: "10.0.0.1:8443",
		AccessToken: "token", TemplateRef: "e2b-snp-t1", RegistryGeneration: "g1",
		BindingDigest: hexDigest(binding), LastEventSeq: 3,
	}
}

func TestReadyRouteRequiresDataPlaneCapability(t *testing.T) {
	route := readyRoute()
	route.AccessToken = ""
	if err := route.Validate(); err == nil {
		t.Fatal("READY route without an access token was accepted")
	}
}

func hexDigest(value [sha256.Size]byte) string {
	return hex.EncodeToString(value[:])
}
