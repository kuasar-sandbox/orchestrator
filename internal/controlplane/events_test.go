package controlplane

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"strings"
	"testing"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/placement"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestSandboxEventsConvergeWithoutProjectionRegression(t *testing.T) {
	store, converger, fixture := newEventFixture(t, clusterstate.ExecutionKindSandbox, "sandbox-1", "route-1")
	ctx := context.Background()
	starting := clusterstate.RouteWorkflowRecord{
		Group: fixture.group, RouteKey: fixture.routeKey, State: clusterstate.WorkflowRouteStarting,
		Starting: &clusterstate.RouteStartingState{
			SandboxID: fixture.objectID, PlacementRound: 1,
			CandidatePool:     []clusterstate.PlacementCandidate{{NodeID: fixture.binding.NodeID}},
			SelectedCandidate: uint32Pointer(0), Intent: fixture.intent, Binding: &fixture.binding,
		},
	}
	if _, err := store.CreateRouteWorkflow(ctx, starting); err != nil {
		t.Fatal(err)
	}
	ready := fixture.event(1, string(clusterstate.WorkflowRouteReady))
	ready.TargetPort = 3000
	ready.AccessToken = "access-token"
	ready.TrafficAccessToken = "traffic-token"
	if err := converger.ConvergeExecutionEvent(ctx, ready); err != nil {
		t.Fatal(err)
	}
	paused := fixture.event(2, string(clusterstate.WorkflowRoutePaused))
	paused.SnapshotRef = "snapshot-1"
	if err := converger.ConvergeExecutionEvent(ctx, paused); err != nil {
		t.Fatal(err)
	}
	resumed := fixture.event(3, string(clusterstate.WorkflowRouteReady))
	if err := converger.ConvergeExecutionEvent(ctx, resumed); err != nil {
		t.Fatal(err)
	}
	if err := converger.ConvergeExecutionEvent(ctx, paused); err != nil {
		t.Fatalf("older duplicate event: %v", err)
	}
	record, err := store.ReadRouteWorkflow(ctx, fixture.group, fixture.routeKey)
	if err != nil {
		t.Fatal(err)
	}
	if record == nil || record.State != clusterstate.WorkflowRouteReady || record.Ready == nil ||
		record.Ready.LastEventSeq != 3 || record.Ready.TargetPort != 3000 ||
		record.Ready.AccessToken != "access-token" || record.Ready.TemplateRef != fixture.templateRef {
		t.Fatalf("converged Route = %+v", record)
	}

	changed := fixture.event(4, string(clusterstate.WorkflowRouteReady))
	changed.TemplateRef = "another-template"
	if err := converger.ConvergeExecutionEvent(ctx, changed); err == nil {
		t.Fatal("same execution changed immutable READY projection")
	}
}

func TestTerminalReplayRepairsFenceBeforeAcknowledgement(t *testing.T) {
	store, converger, fixture := newEventFixture(t, clusterstate.ExecutionKindSandbox, "sandbox-1", "route-1")
	ctx := context.Background()
	starting := clusterstate.RouteWorkflowRecord{
		Group: fixture.group, RouteKey: fixture.routeKey, State: clusterstate.WorkflowRouteStarting,
		Starting: &clusterstate.RouteStartingState{
			SandboxID: fixture.objectID, PlacementRound: 1,
			CandidatePool:     []clusterstate.PlacementCandidate{{NodeID: fixture.binding.NodeID}},
			SelectedCandidate: uint32Pointer(0), Intent: fixture.intent, Binding: &fixture.binding,
		},
	}
	if _, err := store.CreateRouteWorkflow(ctx, starting); err != nil {
		t.Fatal(err)
	}
	ready := fixture.event(1, string(clusterstate.WorkflowRouteReady))
	if err := converger.ConvergeExecutionEvent(ctx, ready); err != nil {
		t.Fatal(err)
	}
	fixture.runtime.failFenceOnce = true
	terminal := fixture.event(2, "ERROR")
	terminal.Reason = "runtime failed"
	if err := converger.ConvergeExecutionEvent(ctx, terminal); err == nil {
		t.Fatal("injected fence failure was hidden")
	}
	record, err := store.ReadRouteWorkflow(ctx, fixture.group, fixture.routeKey)
	if err != nil {
		t.Fatal(err)
	}
	if record == nil || record.State != clusterstate.WorkflowRouteTombstone || fixture.runtime.fenceCount() != 0 {
		t.Fatalf("crash-window state = %+v, fences=%d", record, fixture.runtime.fenceCount())
	}
	if err := converger.ConvergeExecutionEvent(ctx, terminal); err != nil {
		t.Fatalf("terminal replay did not repair fence: %v", err)
	}
	if fixture.runtime.fenceCount() != 1 {
		t.Fatalf("fences=%d, want 1", fixture.runtime.fenceCount())
	}
}

func TestDelayedSandboxEventsAdvanceDeletingWatermarkWithoutStateRegression(t *testing.T) {
	store, converger, fixture := newEventFixture(t, clusterstate.ExecutionKindSandbox, "sandbox-1", "route-1")
	ctx := context.Background()
	starting := clusterstate.RouteWorkflowRecord{
		Group: fixture.group, RouteKey: fixture.routeKey, State: clusterstate.WorkflowRouteStarting,
		Starting: &clusterstate.RouteStartingState{
			SandboxID: fixture.objectID, PlacementRound: 1,
			CandidatePool:     []clusterstate.PlacementCandidate{{NodeID: fixture.binding.NodeID}},
			SelectedCandidate: uint32Pointer(0), Intent: fixture.intent, Binding: &fixture.binding,
		},
	}
	if _, err := store.CreateRouteWorkflow(ctx, starting); err != nil {
		t.Fatal(err)
	}
	if err := converger.ConvergeExecutionEvent(ctx, fixture.event(1, string(clusterstate.WorkflowRouteReady))); err != nil {
		t.Fatal(err)
	}
	ready, err := store.ReadRouteWorkflow(ctx, fixture.group, fixture.routeKey)
	if err != nil || ready == nil || ready.Ready == nil {
		t.Fatalf("READY Route = %+v, %v", ready, err)
	}
	deleteSpec := []byte(`{"delete":true}`)
	deleteDigest := sha256.Sum256(deleteSpec)
	deleting := clusterstate.RouteWorkflowRecord{
		Group: fixture.group, RouteKey: fixture.routeKey, State: clusterstate.WorkflowRouteDeleting,
		Deleting: &clusterstate.DeletingRouteState{
			Execution: *ready.Ready, DeleteSpec: deleteSpec,
			DeleteSpecDigest: hex.EncodeToString(deleteDigest[:]), LastEventSeq: ready.Ready.LastEventSeq,
		},
	}
	if _, err := store.CommitRouteWorkflow(ctx, ready.Revision, deleting); err != nil {
		t.Fatal(err)
	}
	if err := converger.ConvergeExecutionEvent(ctx, fixture.event(2, string(clusterstate.WorkflowRouteReady))); err != nil {
		t.Fatal(err)
	}
	paused := fixture.event(3, string(clusterstate.WorkflowRoutePaused))
	paused.SnapshotRef = "late-snapshot"
	if err := converger.ConvergeExecutionEvent(ctx, paused); err != nil {
		t.Fatal(err)
	}
	record, err := store.ReadRouteWorkflow(ctx, fixture.group, fixture.routeKey)
	if err != nil || record == nil || record.State != clusterstate.WorkflowRouteDeleting || record.Deleting == nil ||
		record.Deleting.LastEventSeq != 3 || record.Deleting.Execution.LastEventSeq != 1 {
		t.Fatalf("DELETING Route regressed = %+v, %v", record, err)
	}
}

func TestBuildLifecycleEventIsRejectedAfterRegistration(t *testing.T) {
	store, converger, fixture := newEventFixture(t, clusterstate.ExecutionKindBuild, "build-1", "")
	ctx := context.Background()
	starting := clusterstate.BuildRecord{
		Group: fixture.group, BuildID: fixture.objectID, State: clusterstate.BuildStarting,
		Starting: &clusterstate.BuildStartingState{
			BuildID:           fixture.objectID,
			CandidatePool:     []clusterstate.PlacementCandidate{{NodeID: fixture.binding.NodeID}},
			SelectedCandidate: uint32Pointer(0), Intent: fixture.intent, Binding: &fixture.binding,
		},
	}
	created, err := store.CreateBuildWorkflow(ctx, starting)
	if err != nil {
		t.Fatal(err)
	}
	registered := clusterstate.BuildRecord{
		Group: fixture.group, BuildID: fixture.objectID, State: clusterstate.BuildRegistered,
		Projection: &clusterstate.BuildProjection{
			BuildID: fixture.objectID, NodeID: fixture.binding.NodeID, NodeEpoch: fixture.binding.NodeEpoch,
			DataEndpoint: fixture.binding.DataEndpoint, RegistryGeneration: fixture.binding.RegistryGeneration,
			BindingDigest: fixture.binding.BindingDigest, Intent: fixture.intent, TemplateRef: fixture.templateRef,
		},
	}
	registered, err = store.CommitBuildWorkflow(ctx, created.Revision, registered)
	if err != nil {
		t.Fatal(err)
	}
	lifecycle := fixture.event(1, string(types.BuildBuilding))
	if err := converger.ConvergeExecutionEvent(ctx, lifecycle); err == nil {
		t.Fatal("Registry accepted a node-local Build lifecycle event")
	}
	record, err := store.ReadBuildWorkflow(ctx, fixture.group, fixture.objectID)
	if err != nil {
		t.Fatal(err)
	}
	if record == nil || record.State != clusterstate.BuildRegistered || record.Projection == nil ||
		record.Revision != registered.Revision {
		t.Fatalf("Build registration changed after lifecycle event: %+v", record)
	}
}

type eventFixture struct {
	runtime     *memoryConsensus
	group       string
	routeKey    string
	objectID    string
	intent      clusterstate.DispatchIntent
	binding     clusterstate.ExecutionBindingIntent
	objectKind  string
	templateRef string
}

func newEventFixture(
	t *testing.T,
	kind clusterstate.ExecutionKind,
	objectID, routeKey string,
) (*RaftStore, *EventConverger, eventFixture) {
	t.Helper()
	registryLayout := testControlRegistryLayout()
	runtime, err := newMemoryConsensus(registryLayout)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := registryLayout.Digest()
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewRaftStore(runtime, registryLayout, digest)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.RefreshPermit(context.Background()); err != nil {
		t.Fatal(err)
	}
	converger, err := NewEventConverger(store)
	if err != nil {
		t.Fatal(err)
	}
	var normalizedDemand, dispatchSpec []byte
	templateRef := "template-1"
	if kind == clusterstate.ExecutionKindSandbox {
		templateRef = "e2b-img-" + strings.Repeat("c", 64)
		normalizedDemand, err = placement.NormalizeSandboxDemand(placement.SandboxDemand{SlotUnits: 1})
		if err == nil {
			var request clusterstate.NodeRequestEnvelopeV1
			request, err = clusterstate.NewNodeRequestEnvelopeV1(
				http.MethodPost, "/sandboxes", "", nil,
				[]byte(`{"templateID":"`+templateRef+`"}`),
			)
			if err != nil {
				t.Fatal(err)
			}
			dispatchSpec, err = clusterstate.MarshalSandboxDispatchSpec(clusterstate.SandboxDispatchSpecV1{
				Version: clusterstate.DispatchSpecVersionV1, TemplateRef: templateRef,
				AuthKeyFingerprint: strings.Repeat("a", 24), ManifestKeyFingerprint: strings.Repeat("b", 24),
				AccessToken: "access-token", TargetPort: 3000, Request: request,
			})
		}
	} else {
		normalizedDemand, err = placement.NormalizeBuildDemand(placement.BuildDemand{Slots: 1})
		if err == nil {
			var request clusterstate.NodeRequestEnvelopeV1
			request, err = clusterstate.NewNodeRequestEnvelopeV1(http.MethodPost, "/v3/templates", "", nil, []byte(`{"cpuCount":1,"memoryMB":512}`))
			if err != nil {
				t.Fatal(err)
			}
			dispatchSpec, err = clusterstate.MarshalBuildDispatchSpec(clusterstate.BuildDispatchSpecV1{
				Version: clusterstate.DispatchSpecVersionV1, TemplateID: templateRef,
				AuthKeyFingerprint: strings.Repeat("b", 24), ManifestKeyFingerprint: strings.Repeat("c", 24),
				Profile: types.ProfileBare, CPUCount: 1, MemoryMB: 512, Request: request,
			})
		}
	}
	if err != nil {
		t.Fatal(err)
	}
	intent, err := clusterstate.NewDispatchIntent(normalizedDemand, dispatchSpec, "provider-v1")
	if err != nil {
		t.Fatal(err)
	}
	var demandDigest, specDigest [sha256.Size]byte
	demandBytes, _ := hex.DecodeString(intent.DemandDigest)
	specBytes, _ := hex.DecodeString(intent.DispatchSpecDigest)
	copy(demandDigest[:], demandBytes)
	copy(specDigest[:], specBytes)
	opaque, err := clusterstate.EncodeExecutionBinding(clusterstate.ExecutionBinding{
		RegistryGeneration: registryLayout.RegistryGeneration, Kind: kind, ObjectID: objectID,
		Group: "/group", RouteKey: routeKey, NodeID: "node-1", NodeEpoch: 7,
		DemandDigest: demandDigest, DispatchSpecDigest: specDigest,
	})
	if err != nil {
		t.Fatal(err)
	}
	bindingDigest, err := clusterstate.ExecutionBindingDigest(opaque)
	if err != nil {
		t.Fatal(err)
	}
	objectKind := "sandbox"
	if kind == clusterstate.ExecutionKindBuild {
		objectKind = "build"
	}
	fixture := eventFixture{
		runtime: runtime, group: "/group", routeKey: routeKey, objectID: objectID,
		intent: intent, objectKind: objectKind, templateRef: templateRef,
		binding: clusterstate.ExecutionBindingIntent{
			NodeID: "node-1", NodeEpoch: 7, DataEndpoint: "10.0.0.1:8443",
			RegistryGeneration: registryLayout.RegistryGeneration, OpaqueBinding: opaque, BindingDigest: bindingDigest,
		},
	}
	return store, converger, fixture
}

func (f eventFixture) event(sequence uint64, state string) routesync.ExecutionEvent {
	event := routesync.ExecutionEvent{
		ObjectKind: f.objectKind, ObjectID: f.objectID, NodeID: f.binding.NodeID,
		NodeEpoch: f.binding.NodeEpoch, RegistryGeneration: f.binding.RegistryGeneration,
		Binding: f.binding.OpaqueBinding, BindingDigest: f.binding.BindingDigest,
		EventSeq: sequence, State: state, DataEndpoint: f.binding.DataEndpoint,
		TemplateRef: f.templateRef,
	}
	if f.objectKind == "sandbox" {
		event.TargetPort = 3000
		event.AccessToken = "access-token"
		event.TrafficAccessToken = "traffic-token"
	}
	return event
}

func uint32Pointer(value uint32) *uint32 { return &value }
