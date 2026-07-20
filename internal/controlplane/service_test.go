package controlplane

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/placement"
	"github.com/kuasar-sandbox/orchestrator/internal/placer"
	"github.com/kuasar-sandbox/orchestrator/internal/raftstore"
	"github.com/kuasar-sandbox/orchestrator/internal/routeapi"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/session"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestRegistryReserveKeepsCommittedStartingIntentAndBinding(t *testing.T) {
	service, store, planner, dispatcher := newRegistryServiceFixture(t, clusterstate.DispatchUnknown)
	request := sandboxMutationRequest(t, store)

	first, err := service.ReserveSandbox(context.Background(), request)
	if err != nil || first.Outcome != routeapi.MutationPending {
		t.Fatalf("first Reserve = %+v, %v", first, err)
	}
	record, err := store.ReadRouteWorkflow(context.Background(), request.Group, request.RouteKey)
	if err != nil || record == nil || record.Starting == nil || record.Starting.SelectedCandidate == nil {
		t.Fatalf("committed STARTING = %+v, %v", record, err)
	}
	firstSID := record.Starting.SandboxID
	firstBinding := *record.Starting.Binding

	second, err := service.ReserveSandbox(context.Background(), request)
	if err != nil || second.Outcome != routeapi.MutationPending {
		t.Fatalf("second Reserve = %+v, %v", second, err)
	}
	after, err := store.ReadRouteWorkflow(context.Background(), request.Group, request.RouteKey)
	if err != nil || after.Starting.SandboxID != firstSID || *after.Starting.Binding != firstBinding {
		t.Fatalf("STARTING was reconstructed or rebound: %+v, %v", after, err)
	}
	if planner.callCount() != 1 {
		t.Fatalf("Planner calls = %d, want one committed plan", planner.callCount())
	}
	calls := dispatcher.snapshot()
	if len(calls) != 2 || calls[0].ObjectID != calls[1].ObjectID || calls[0].Binding != calls[1].Binding {
		t.Fatalf("dispatch retries were not pinned to one execution: %+v", calls)
	}

	changed := request
	changed.Input.Config = map[string]string{"request": "changed"}
	conflict, err := service.ReserveSandbox(context.Background(), changed)
	if err != nil || conflict.Outcome != routeapi.MutationConflict {
		t.Fatalf("changed Reserve = %+v, %v", conflict, err)
	}
	if planner.callCount() != 1 || len(dispatcher.snapshot()) != 2 {
		t.Fatal("conflicting retry invoked placement or dispatch")
	}
}

func TestRegistryReadyPositiveReadStillChecksImmutableSandboxRequest(t *testing.T) {
	service, store, planner, dispatcher := newRegistryServiceFixture(t, clusterstate.DispatchAcceptedAdmitted)
	request := sandboxMutationRequest(t, store)
	if _, err := service.ReserveSandbox(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	record, err := store.ReadRouteWorkflow(context.Background(), request.Group, request.RouteKey)
	if err != nil || record == nil || record.Starting == nil || record.Starting.Binding == nil {
		t.Fatalf("STARTING = %+v, %v", record, err)
	}
	binding := *record.Starting.Binding
	converger, err := NewEventConverger(store)
	if err != nil {
		t.Fatal(err)
	}
	if err := converger.ConvergeExecutionEvent(context.Background(), routesync.ExecutionEvent{
		ObjectKind: "sandbox", ObjectID: record.Starting.SandboxID,
		NodeID: binding.NodeID, NodeEpoch: binding.NodeEpoch,
		RegistryGeneration: binding.RegistryGeneration, Binding: binding.OpaqueBinding,
		BindingDigest: binding.BindingDigest, EventSeq: 1,
		State: string(clusterstate.WorkflowRouteReady), DataEndpoint: binding.DataEndpoint,
		TargetPort: 3000, AccessToken: "access", TrafficAccessToken: "traffic",
		TemplateRef: "e2b-img-" + strings.Repeat("c", 64),
	}); err != nil {
		t.Fatal(err)
	}

	same, err := service.ReserveSandbox(context.Background(), request)
	if err != nil || same.Outcome != routeapi.MutationReady {
		t.Fatalf("same READY Reserve = %+v, %v", same, err)
	}
	changed := request
	changed.Input.TargetRuntimeDigest = "runtime-v2"
	conflict, err := service.ReserveSandbox(context.Background(), changed)
	if err != nil || conflict.Outcome != routeapi.MutationConflict {
		t.Fatalf("changed READY Reserve = %+v, %v", conflict, err)
	}
	if planner.callCount() != 1 || len(dispatcher.snapshot()) != 1 {
		t.Fatal("READY positive read invoked placement or dispatch")
	}
}

func TestRegistryBuildIDConflictSurvivesRegisteredProjection(t *testing.T) {
	service, store, planner, _ := newRegistryServiceFixture(t, clusterstate.DispatchAcceptedQueued)
	request := buildMutationRequest(t, store)
	if response, err := service.RegisterBuild(context.Background(), request); err != nil ||
		response.Outcome != routeapi.MutationReady || response.State != clusterstate.BuildRegistered {
		t.Fatalf("RegisterBuild = %+v, %v", response, err)
	}
	record, err := store.ReadBuildWorkflow(context.Background(), request.Group, request.BuildID)
	if err != nil || record == nil || record.State != clusterstate.BuildRegistered || record.Projection == nil {
		t.Fatalf("BUILD_REGISTERED = %+v, %v", record, err)
	}

	same, err := service.RegisterBuild(context.Background(), request)
	if err != nil || same.Outcome != routeapi.MutationReady || same.State != clusterstate.BuildRegistered {
		t.Fatalf("same registered Build = %+v, %v", same, err)
	}
	changed := request
	changed.Input.TemplateID = "another-template"
	conflict, err := service.RegisterBuild(context.Background(), changed)
	if err != nil || conflict.Outcome != routeapi.MutationConflict {
		t.Fatalf("changed registered Build = %+v, %v", conflict, err)
	}
	if planner.callCount() != 1 {
		t.Fatalf("Planner calls = %d after Build retry", planner.callCount())
	}
}

type serviceConsensus struct {
	*memoryConsensus
	system raftstore.SystemState
}

func (s *serviceConsensus) ReadSystemStrong(context.Context) (raftstore.SystemState, error) {
	return s.system, nil
}

type servicePlanner struct {
	mu    sync.Mutex
	calls []placer.PlanRequest
}

func (p *servicePlanner) Plan(_ context.Context, request placer.PlanRequest) (placer.PlanResponse, error) {
	p.mu.Lock()
	p.calls = append(p.calls, request)
	p.mu.Unlock()
	candidates := make([]clusterstate.PlacementCandidate, placement.DefaultCandidateCount)
	for index := range candidates {
		candidates[index] = clusterstate.PlacementCandidate{
			NodeID: fmt.Sprintf("node-%d", index+1), RuntimeDigest: request.TargetRuntimeDigest,
		}
	}
	switch request.Kind {
	case placer.PlanSandbox:
		demand, err := placement.NormalizeSandboxDemand(request.Sandbox.Demand)
		if err != nil {
			return placer.PlanResponse{}, err
		}
		spec, err := clusterstate.MarshalSandboxDispatchSpec(clusterstate.SandboxDispatchSpecV1{
			Version: clusterstate.DispatchSpecVersionV1, TemplateRef: "e2b-img-" + strings.Repeat("c", 64),
			KeyFingerprint: strings.Repeat("a", 24), TargetRuntimeDigest: request.TargetRuntimeDigest,
			RequestedConfig: clusterstate.WithoutSystemMetadata(request.Sandbox.Config),
			Config:          clusterstate.WithoutSystemMetadata(request.Sandbox.Config),
			AccessToken:     "access", TargetPort: 3000, TimeoutSeconds: request.Sandbox.TimeoutSeconds,
		})
		return placer.PlanResponse{Candidates: candidates, NormalizedDemand: demand, DispatchSpec: spec, ProviderPolicyVersion: "policy-v1"}, err
	case placer.PlanBuild:
		demand, err := placement.NormalizeBuildDemand(request.Build.Demand)
		if err != nil {
			return placer.PlanResponse{}, err
		}
		spec, err := clusterstate.MarshalBuildDispatchSpec(clusterstate.BuildDispatchSpecV1{
			Version: clusterstate.DispatchSpecVersionV1, TemplateID: request.Build.TemplateID,
			KeyFingerprint: strings.Repeat("b", 24), TargetRuntimeDigest: request.TargetRuntimeDigest,
			Profile: request.Build.Profile, Names: request.Build.Names, Aliases: request.Build.Aliases,
			Metadata: clusterstate.WithoutSystemMetadata(request.Build.Metadata), Builder: request.Build.Builder,
		})
		return placer.PlanResponse{Candidates: candidates, NormalizedDemand: demand, DispatchSpec: spec, ProviderPolicyVersion: "policy-v1"}, err
	default:
		return placer.PlanResponse{}, fmt.Errorf("unexpected plan kind %q", request.Kind)
	}
}

func (p *servicePlanner) callCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.calls)
}

type serviceProber struct{}

func (serviceProber) ProbePair(_ context.Context, _ session.ServeIdentity, requests []placement.PlacementProbeRequest) []session.ProbeResult {
	results := make([]session.ProbeResult, len(requests))
	for index, request := range requests {
		results[index].Response = placement.PlacementProbeResponse{
			Class: placement.ProbeImmediate, NodeID: request.NodeID,
			NodeEpoch: 7, SessionSeq: 11, DataEndpoint: request.NodeID + ":8443",
			SampleSeq: 1, LoadModelVersion: request.LoadModelVersion, RatePPM: uint32(index + 1),
		}
	}
	return results
}

type serviceDispatcher struct {
	mu      sync.Mutex
	outcome clusterstate.DispatchOutcome
	calls   []session.DispatchCommand
}

func (d *serviceDispatcher) AdmitAndDispatch(_ context.Context, command session.DispatchCommand) (session.DispatchReply, error) {
	d.mu.Lock()
	d.calls = append(d.calls, command)
	d.mu.Unlock()
	return session.DispatchReply{Outcome: d.outcome}, nil
}

func (d *serviceDispatcher) snapshot() []session.DispatchCommand {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]session.DispatchCommand(nil), d.calls...)
}

type serviceCommandSender struct{}

func (serviceCommandSender) SendNodeCommand(
	context.Context, session.ServeIdentity, string, uint64, string, *routesync.Command,
) (routesync.CmdAck, bool, error) {
	return routesync.CmdAck{}, false, nil
}

func newRegistryServiceFixture(
	t *testing.T,
	outcome clusterstate.DispatchOutcome,
) (*RegistryService, *RaftStore, *servicePlanner, *serviceDispatcher) {
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
	consensus := &serviceConsensus{memoryConsensus: runtime, system: raftstore.SystemState{
		Initialized: true, ClusterID: registryLayout.ClusterID, RegistryGeneration: registryLayout.RegistryGeneration,
		SystemEpoch: 1, ActiveRegistryLayoutDigest: digest, NodeEnrollments: map[string]raftstore.NodeEnrollmentRecord{},
	}}
	store, err := NewRaftStore(consensus, registryLayout, digest)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.RefreshPermit(context.Background()); err != nil {
		t.Fatal(err)
	}
	planner := &servicePlanner{}
	dispatcher := &serviceDispatcher{outcome: outcome}
	nextID := 0
	config := DefaultRegistryServiceConfig()
	config.ParkTimeout = time.Millisecond
	config.PollInterval = 100 * time.Microsecond
	config.NewObjectID = func() (string, error) {
		nextID++
		return fmt.Sprintf("sandbox-%d", nextID), nil
	}
	service, err := NewRegistryService(store, planner, serviceProber{}, dispatcher, serviceCommandSender{}, config)
	if err != nil {
		t.Fatal(err)
	}
	return service, store, planner, dispatcher
}

func sandboxMutationRequest(t *testing.T, store *RaftStore) routeapi.ReserveSandboxRequest {
	t.Helper()
	identity, err := store.RouteRequestIdentity("/group", "route-1")
	if err != nil {
		t.Fatal(err)
	}
	return routeapi.ReserveSandboxRequest{
		RequestIdentity: identity, Group: "/group", RouteKey: "route-1",
		Input: routeapi.SandboxInput{
			Config: map[string]string{"request": "value"}, TimeoutSeconds: 30,
			Demand:              placement.SandboxDemand{SlotUnits: 1, StartupBudgetMemory: 1024},
			TargetRuntimeDigest: "runtime-v1",
		},
	}
}

func buildMutationRequest(t *testing.T, store *RaftStore) routeapi.RegisterBuildRequest {
	t.Helper()
	identity, err := store.BuildRequestIdentity("/group", "build-1")
	if err != nil {
		t.Fatal(err)
	}
	return routeapi.RegisterBuildRequest{
		RequestIdentity: identity, Group: "/group", BuildID: "build-1",
		Input: routeapi.BuildInput{
			TemplateID: "transient-template", Profile: types.ProfileBare,
			Names: []string{"name"}, Aliases: []string{"alias"},
			Metadata:            map[string]string{"request": "value"},
			Demand:              placement.BuildDemand{Slots: 1, CPU: 100, Memory: 1024, Storage: 4096},
			TargetRuntimeDigest: "runtime-v1",
		},
	}
}
