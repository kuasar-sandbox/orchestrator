package cluster

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestRouteWorkflowTypesValidateFrozenIntent(t *testing.T) {
	intent := workflowSandboxIntent(t)
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
	proof := TerminalProof{Kind: ProofNodeTerminal, FencedNodeID: "n1", FencedNodeEpoch: 7}
	var err error
	proof.ProofDigest, err = NodeTerminalProofDigest(proof, "g1", "s1", hexDigest(binding), 4)
	if err != nil {
		t.Fatal(err)
	}
	record := RouteWorkflowRecord{
		Group: "/g", RouteKey: "rk", State: WorkflowRouteTombstone,
		Revision: Revision{RegistryGeneration: "g1", ShardID: 9, LogIndex: 101},
		Tombstone: &RouteTombstoneState{
			SandboxID: "s1", NodeID: "n1", NodeEpoch: 7, RegistryGeneration: "g1",
			BindingDigest: hexDigest(binding), LastEventSeq: 4, TerminalReason: "deleted",
			Proof:           proof,
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
	if _, err := NewDispatchIntent([]byte(strings.Repeat("x", MaxNormalizedDemandBytes+1)), []byte("spec"), "v1"); err == nil {
		t.Fatal("oversized normalized demand accepted")
	}
}

func TestPlacementFailuresDoNotInventExecutionProof(t *testing.T) {
	intent := workflowSandboxIntent(t)
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
	route.Tombstone.FenceCompacted = true
	if err := route.Validate(); err != nil {
		t.Fatalf("compacted placement-failure tombstone: %v", err)
	}
	route.Tombstone.FenceCompacted = false
	route.Tombstone.PlacementFailure.DefinitivelyRejected = []uint32{0}
	if err := route.Validate(); err == nil {
		t.Fatal("Route placement failure accepted a non-exhausted pool")
	}

	build := BuildRecord{
		Group: "/g", BuildID: "b1", State: BuildTombstone,
		Revision: Revision{RegistryGeneration: "g1", ShardID: 2, LogIndex: 20},
		Tombstone: &BuildTombstoneState{
			PlacementFailure: BuildPlacementFailureState{
				BuildID: "b1", CandidatePool: candidates, DefinitivelyRejected: rejected,
				Intent: workflowBuildIntent(t), Reason: "candidate pool exhausted",
			},
		},
	}
	if err := build.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestPlacementFailureReasonIsBoundedUTF8(t *testing.T) {
	route := RoutePlacementFailureState{
		SandboxID: "s1", PlacementRound: 1,
		CandidatePool:        []PlacementCandidate{{NodeID: "n1"}},
		DefinitivelyRejected: []uint32{0}, Intent: workflowSandboxIntent(t), Reason: "exhausted",
	}
	build := BuildPlacementFailureState{
		BuildID: "b1", CandidatePool: []PlacementCandidate{{NodeID: "n1"}},
		DefinitivelyRejected: []uint32{0}, Intent: workflowBuildIntent(t), Reason: "exhausted",
	}
	for name, reason := range map[string]string{
		"oversized":     strings.Repeat("x", MaxPlacementFailureReasonBytes+1),
		"invalid UTF-8": string([]byte{0xff}),
	} {
		t.Run(name, func(t *testing.T) {
			route.Reason = reason
			if err := route.Validate(); err == nil {
				t.Fatal("Route placement failure accepted an invalid reason")
			}
			build.Reason = reason
			if err := build.Validate(); err == nil {
				t.Fatal("Build placement failure accepted an invalid reason")
			}
		})
	}
}

func TestPlacementFailureFenceProofIsBoundToRouteIdentity(t *testing.T) {
	failure := RoutePlacementFailureState{
		SandboxID: "s1", PlacementRound: 2,
		CandidatePool:        []PlacementCandidate{{NodeID: "n1"}, {NodeID: "n2"}},
		DefinitivelyRejected: []uint32{0, 1}, Intent: workflowSandboxIntent(t), Reason: "placement exhausted",
	}
	fence, err := NewPlacementFailureFence("/g", "rk", "g1", failure)
	if err != nil {
		t.Fatal(err)
	}
	fence.Revision = Revision{RegistryGeneration: "g1", ShardID: 1, LogIndex: 10}
	if err := fence.Validate(); err != nil {
		t.Fatal(err)
	}

	for name, mutate := range map[string]func(*ExecutionFence){
		"group":      func(f *ExecutionFence) { f.Group = "/another-group" },
		"route key":  func(f *ExecutionFence) { f.RouteKey = "another-route" },
		"generation": func(f *ExecutionFence) { f.RegistryGeneration, f.Revision.RegistryGeneration = "g2", "g2" },
	} {
		t.Run(name, func(t *testing.T) {
			transplanted := fence
			mutate(&transplanted)
			if err := transplanted.Validate(); err == nil {
				t.Fatal("placement-failure proof was accepted for another Route identity")
			}
		})
	}
}

func TestBuildProjectionRequiresCanonicalDataEndpoint(t *testing.T) {
	build := registeredBuildRecord(t)
	build.Projection.DataEndpoint = "https://node-1:8443"
	if err := build.Validate(); err == nil {
		t.Fatal("non-TCP Build data endpoint accepted")
	}
}

func TestWorkflowFinalizationRequiresCanonicalDataEndpoint(t *testing.T) {
	binding := workflowBinding(t, ExecutionKindSandbox, "s1", "rk", workflowSandboxIntent(t))
	intent, err := NewWorkflowFinalizationIntent("s1", binding, nil)
	if err != nil {
		t.Fatal(err)
	}
	intent.DataEndpoint = "https://node-1:8443"
	if err := intent.Validate(); err == nil {
		t.Fatal("non-TCP workflow finalization endpoint accepted")
	}
}

func TestRouteFinalizationIsBoundToOwningWorkflow(t *testing.T) {
	intent := workflowSandboxIntent(t)
	priorBinding := workflowBinding(t, ExecutionKindSandbox, "prior-sandbox", "rk", intent)
	finalization, err := NewWorkflowFinalizationIntent("prior-sandbox", priorBinding, nil)
	if err != nil {
		t.Fatal(err)
	}
	record := RouteWorkflowRecord{
		Group: "/g", RouteKey: "rk", State: WorkflowRouteReady,
		Revision:      Revision{RegistryGeneration: "g1", ShardID: 1, LogIndex: 10},
		Ready:         readyRoute(),
		Finalizations: []WorkflowFinalizationIntent{finalization},
	}
	if err := record.Validate(); err != nil {
		t.Fatalf("prior execution from the same Route was rejected: %v", err)
	}

	otherBinding := workflowBinding(t, ExecutionKindSandbox, "prior-sandbox", "other-route", intent)
	foreign, err := NewWorkflowFinalizationIntent("prior-sandbox", otherBinding, nil)
	if err != nil {
		t.Fatal(err)
	}
	record.Finalizations = []WorkflowFinalizationIntent{foreign}
	if err := record.Validate(); err == nil {
		t.Fatal("finalization from another Route was accepted")
	}
}

func TestDispatchIntentBoundsProviderPolicyVersion(t *testing.T) {
	intent := workflowSandboxIntent(t)
	intent.ProviderPolicyVersion = strings.Repeat("v", MaxProviderPolicyVersionBytes+1)
	if err := intent.Validate(); err == nil {
		t.Fatal("oversized provider policy version accepted")
	}
	intent.ProviderPolicyVersion = string([]byte{'v', 0xff})
	if err := intent.Validate(); err == nil {
		t.Fatal("invalid UTF-8 provider policy version accepted")
	}
}

func TestBuildFinalizationMustIdentifyContainingBuild(t *testing.T) {
	build := registeredBuildRecord(t)
	binding := workflowBinding(t, ExecutionKindBuild, "another-build", "", workflowBuildIntent(t))
	finalization, err := NewWorkflowFinalizationIntent("another-build", binding, nil)
	if err != nil {
		t.Fatal(err)
	}
	build.Finalizations = []WorkflowFinalizationIntent{finalization}
	if err := build.Validate(); err == nil {
		t.Fatal("Build finalization for another object accepted")
	}
}

func TestBuildProjectionBindingMustCoverContainingWorkflow(t *testing.T) {
	build := registeredBuildRecord(t)
	build.Group = "/another-group"
	if err := build.Validate(); err == nil {
		t.Fatal("Build projection from another group was accepted")
	}
}

func registeredBuildRecord(t *testing.T) BuildRecord {
	t.Helper()
	intent := workflowBuildIntent(t)
	binding := workflowBinding(t, ExecutionKindBuild, "b1", "", intent)
	return BuildRecord{
		Group: "/g", BuildID: "b1", State: BuildRegistered,
		Revision: Revision{RegistryGeneration: "g1", ShardID: 2, LogIndex: 20},
		Projection: &BuildProjection{
			BuildID: "b1", NodeID: "n1", NodeEpoch: 7, DataEndpoint: "10.0.0.1:8443",
			RegistryGeneration: "g1", OpaqueBinding: binding.OpaqueBinding, BindingDigest: binding.BindingDigest,
			Intent: intent, TemplateRef: "template-1",
		},
	}
}

func TestPlacementCandidatePoolIsBoundedBeforePersistence(t *testing.T) {
	candidates := make([]PlacementCandidate, MaxPlacementCandidates+1)
	for index := range candidates {
		candidates[index].NodeID = fmt.Sprintf("node-%d", index)
	}
	if err := validateCandidates(candidates, nil, nil); err == nil {
		t.Fatal("oversized candidate pool was accepted")
	}
	if err := validateCandidates([]PlacementCandidate{{NodeID: "node-1"}}, nil, make([]uint32, 2)); err == nil {
		t.Fatal("oversized rejected-candidate set was accepted")
	}
	if err := validateCandidates([]PlacementCandidate{{
		NodeID: "node-1", FailureDomain: strings.Repeat("z", MaxPlacementCandidateMetadataBytes+1),
	}}, nil, nil); err == nil {
		t.Fatal("oversized candidate metadata was accepted")
	}
}

func TestExecutionFenceRequiresFinalOutboxCoverage(t *testing.T) {
	proof := TerminalProof{
		Kind: ProofNodeTerminal, FencedNodeID: "node-1", FencedNodeEpoch: 7,
	}
	bindingDigest := hexDigest(sha256.Sum256([]byte("binding")))
	var err error
	proof.ProofDigest, err = NodeTerminalProofDigest(proof, "g1", "sandbox-1", bindingDigest, 4)
	if err != nil {
		t.Fatal(err)
	}
	fence := ExecutionFence{
		Group: "/g", RouteKey: "rk", SandboxID: "sandbox-1", NodeID: "node-1", NodeEpoch: 7,
		RegistryGeneration: "g1", BindingDigest: bindingDigest,
		LastEventSeq: 4, FinalOutboxWatermark: 3, Proof: proof,
		Revision: Revision{RegistryGeneration: "g1", ShardID: 1, LogIndex: 9},
	}
	if err := fence.Validate(); err == nil {
		t.Fatal("execution fence accepted an outbox watermark below its last event")
	}
	fence.FinalOutboxWatermark = fence.LastEventSeq
	if err := fence.Validate(); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*ExecutionFence){
		"sandbox":   func(value *ExecutionFence) { value.SandboxID = "sandbox-2" },
		"binding":   func(value *ExecutionFence) { value.BindingDigest = hexDigest(sha256.Sum256([]byte("other"))) },
		"watermark": func(value *ExecutionFence) { value.LastEventSeq++ },
	} {
		t.Run(name, func(t *testing.T) {
			changed := fence
			mutate(&changed)
			if err := changed.Validate(); err == nil {
				t.Fatal("node-terminal proof was accepted for another execution")
			}
		})
	}
}

func TestNewerNodeEpochProofIsBoundToExactExecutionState(t *testing.T) {
	ready := readyRoute()
	proof := TerminalProof{
		Kind: ProofNewerNodeEpoch, FencedNodeID: ready.NodeID, FencedNodeEpoch: ready.NodeEpoch,
		ObservedNodeEpoch: ready.NodeEpoch + 1, SystemEpoch: 4, SystemCommitIndex: 30,
		EnrollmentID: "enrollment-2", EnrollmentCommitIndex: 29,
	}
	var err error
	proof.ProofDigest, err = NewerNodeEpochProofDigest(
		proof, ready.RegistryGeneration, ready.SandboxID, ready.BindingDigest, ready.LastEventSeq,
	)
	if err != nil {
		t.Fatal(err)
	}
	record := RouteWorkflowRecord{
		Group: "/g", RouteKey: "rk", State: WorkflowRouteTombstone,
		Revision: Revision{RegistryGeneration: "g1", ShardID: 7, LogIndex: 40},
		Tombstone: &RouteTombstoneState{
			SandboxID: ready.SandboxID, NodeID: ready.NodeID, NodeEpoch: ready.NodeEpoch,
			RegistryGeneration: ready.RegistryGeneration,
			BindingDigest:      ready.BindingDigest, LastEventSeq: ready.LastEventSeq,
			Proof: proof, TerminalReason: "node epoch advanced",
			FailureRevision: Revision{RegistryGeneration: "g1", ShardID: 7, LogIndex: 40},
		},
	}
	if err := record.Validate(); err != nil {
		t.Fatal(err)
	}
	record.Tombstone.LastEventSeq++
	if err := record.Validate(); err == nil {
		t.Fatal("newer-NodeEpoch proof was reused for another execution watermark")
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

func workflowSandboxIntent(t *testing.T) DispatchIntent {
	t.Helper()
	templateRef := "e2b-img-" + strings.Repeat("c", 64)
	spec, err := MarshalSandboxDispatchSpec(SandboxDispatchSpecV1{
		Version: DispatchSpecVersionV1, TemplateRef: templateRef,
		AuthKeyFingerprint: strings.Repeat("a", 24), ManifestKeyFingerprint: strings.Repeat("b", 24),
		AccessToken: "token", TargetPort: 3000,
		Request: NodeRequestEnvelopeV1{
			Version: NodeRequestEnvelopeVersionV1, Method: "POST", Path: "/sandboxes",
			Body: []byte(`{"metadata":null,"templateID":"` + templateRef + `","timeout":0}`),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	intent, err := NewDispatchIntent([]byte("demand"), spec, "provider-v1")
	if err != nil {
		t.Fatal(err)
	}
	return intent
}

func workflowBuildIntent(t *testing.T) DispatchIntent {
	t.Helper()
	spec, err := MarshalBuildDispatchSpec(BuildDispatchSpecV1{
		Version: DispatchSpecVersionV1, TemplateID: "template-1",
		AuthKeyFingerprint: strings.Repeat("b", 24), ManifestKeyFingerprint: strings.Repeat("c", 24),
		Profile: types.ProfileBare, CPUCount: 1, MemoryMB: 512,
		Request: NodeRequestEnvelopeV1{Version: NodeRequestEnvelopeVersionV1, Method: "POST", Path: "/v3/templates", Body: []byte(`{"cpuCount":1,"memoryMB":512,"metadata":null,"name":"","profile":"bare","tags":null}`)},
	})
	if err != nil {
		t.Fatal(err)
	}
	intent, err := NewDispatchIntent([]byte("demand"), spec, "provider-v1")
	if err != nil {
		t.Fatal(err)
	}
	return intent
}

func readyRoute() *ReadyRoute {
	templateRef := "e2b-img-" + strings.Repeat("c", 64)
	spec, _ := MarshalSandboxDispatchSpec(SandboxDispatchSpecV1{
		Version: DispatchSpecVersionV1, TemplateRef: templateRef,
		AuthKeyFingerprint: strings.Repeat("a", 24), ManifestKeyFingerprint: strings.Repeat("b", 24),
		AccessToken: "token", TargetPort: 3000,
		Request: NodeRequestEnvelopeV1{
			Version: NodeRequestEnvelopeVersionV1, Method: "POST", Path: "/sandboxes",
			Body: []byte(`{"metadata":null,"templateID":"` + templateRef + `","timeout":0}`),
		},
	})
	intent, _ := NewDispatchIntent([]byte("demand"), spec, "provider-v1")
	binding := projectionBinding(ExecutionKindSandbox, "s1", "rk", intent)
	return &ReadyRoute{
		SandboxID: "s1", NodeID: "n1", NodeEpoch: 7, DataEndpoint: "10.0.0.1:8443",
		TargetPort: 3000, AccessToken: "token", TrafficAccessToken: "traffic-token",
		TemplateRef: templateRef, RegistryGeneration: "g1",
		OpaqueBinding: binding.OpaqueBinding, BindingDigest: binding.BindingDigest, LastEventSeq: 3, Intent: intent,
		Presentation: SandboxPresentationV1{
			CPUCount: 2, MemoryMB: 2048, DiskSizeMB: 64, EnvdVersion: "0.6.1", StartedAt: 1, EndAt: 2,
		},
	}
}

func projectionBinding(kind ExecutionKind, objectID, routeKey string, intent DispatchIntent) ExecutionBindingIntent {
	var demand, dispatch [sha256.Size]byte
	demandBytes, _ := hex.DecodeString(intent.DemandDigest)
	dispatchBytes, _ := hex.DecodeString(intent.DispatchSpecDigest)
	copy(demand[:], demandBytes)
	copy(dispatch[:], dispatchBytes)
	opaque, _ := EncodeExecutionBinding(ExecutionBinding{
		RegistryGeneration: "g1", Kind: kind, ObjectID: objectID, Group: "/g", RouteKey: routeKey,
		NodeID: "n1", NodeEpoch: 7, DemandDigest: demand, DispatchSpecDigest: dispatch,
	})
	digest, _ := ExecutionBindingDigest(opaque)
	return ExecutionBindingIntent{
		NodeID: "n1", NodeEpoch: 7, DataEndpoint: "10.0.0.1:8443",
		RegistryGeneration: "g1", OpaqueBinding: opaque, BindingDigest: digest,
	}
}

func TestReadyRouteBindingMustCoverContainingWorkflow(t *testing.T) {
	record := RouteWorkflowRecord{
		Group: "/g", RouteKey: "another-route", State: WorkflowRouteReady,
		Revision: Revision{RegistryGeneration: "g1", ShardID: 1, LogIndex: 1}, Ready: readyRoute(),
	}
	if err := record.Validate(); err == nil {
		t.Fatal("READY projection from another Route was accepted")
	}
}

func TestReadyRouteRequiresDataPlaneCapability(t *testing.T) {
	route := readyRoute()
	route.AccessToken = ""
	if err := route.Validate(); err == nil {
		t.Fatal("READY route without an access token was accepted")
	}
}

func TestReadyRouteRejectsInvalidConnectHeaderValues(t *testing.T) {
	for name, mutate := range map[string]func(*ReadyRoute){
		"sandbox ID":          func(route *ReadyRoute) { route.SandboxID = "bad\r\nid" },
		"node ID":             func(route *ReadyRoute) { route.NodeID = "bad\nnode" },
		"Registry generation": func(route *ReadyRoute) { route.RegistryGeneration = " bad" },
	} {
		t.Run(name, func(t *testing.T) {
			route := readyRoute()
			mutate(route)
			if err := route.Validate(); err == nil {
				t.Fatal("READY accepted a value that cannot be written as a CONNECT header")
			}
		})
	}

	route := readyRoute()
	spec, err := ParseSandboxDispatchSpec(route.Intent.DispatchSpec)
	if err != nil {
		t.Fatal(err)
	}
	spec.AccessToken = "bad\r\ntoken"
	encoded, err := MarshalSandboxDispatchSpec(spec)
	if err != nil {
		t.Fatal(err)
	}
	route.Intent, err = NewDispatchIntent(route.Intent.NormalizedDemand, encoded, route.Intent.ProviderPolicyVersion)
	if err != nil {
		t.Fatal(err)
	}
	route.AccessToken = spec.AccessToken
	if err := route.Validate(); err == nil {
		t.Fatal("READY accepted an access token that cannot be written as a CONNECT header")
	}
}

func hexDigest(value [sha256.Size]byte) string {
	return hex.EncodeToString(value[:])
}
