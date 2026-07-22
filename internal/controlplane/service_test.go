package controlplane

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
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
	wantLease, err := serviceKeyLease(request.Group).Ref()
	if err != nil {
		t.Fatal(err)
	}
	if calls[0].KeyLeaseRef != wantLease || calls[1].KeyLeaseRef != wantLease {
		t.Fatalf("dispatch retries were not fenced by the acknowledged key lease: %+v", calls)
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

func TestSandboxRetryAcceptsPlacementDerivedDemandOnly(t *testing.T) {
	persisted := placement.SandboxDemand{
		SlotUnits: 1, StartupBudgetMemory: 2 << 30, FloorMemory: 2 << 30,
	}
	if !sandboxDemandMatches(placement.SandboxDemand{SlotUnits: 1}, persisted) {
		t.Fatal("Router retry without internal memory fields did not match persisted derived demand")
	}
	if sandboxDemandMatches(placement.SandboxDemand{SlotUnits: 1, FloorMemory: 1 << 30}, persisted) {
		t.Fatal("explicit conflicting memory demand matched persisted intent")
	}
	if sandboxDemandMatches(placement.SandboxDemand{SlotUnits: 2}, persisted) {
		t.Fatal("different slot demand matched persisted intent")
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
		Presentation: &clusterstate.SandboxPresentationV1{
			CPUCount: 2, MemoryMB: 2048, DiskSizeMB: 64, EnvdVersion: "0.6.1", StartedAt: 1, EndAt: 2,
		},
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
	changed = request
	changed.Input.TemplateRef = "e2b-img-" + strings.Repeat("d", 64)
	conflict, err = service.ReserveSandbox(context.Background(), changed)
	if err != nil || conflict.Outcome != routeapi.MutationConflict {
		t.Fatalf("changed template READY Reserve = %+v, %v", conflict, err)
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

func TestNextSandboxRoundPreservesTargetRuntimeDigest(t *testing.T) {
	service, store, planner, _ := newRegistryServiceFixture(t, clusterstate.DispatchAcceptedQueued)
	request := sandboxMutationRequest(t, store)
	first, err := service.planSandbox(
		context.Background(), request.Group, request.RouteKey, "sandbox-first", request.Input, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := (sandboxRoundSource{service: service}).NextSandboxRound(
		context.Background(), request.Group, request.RouteKey, 2, first.Intent, []string{"node-1"},
	); err != nil {
		t.Fatal(err)
	}
	planner.mu.Lock()
	defer planner.mu.Unlock()
	if len(planner.calls) != 2 {
		t.Fatalf("placement calls = %d, want 2", len(planner.calls))
	}
	if got := planner.calls[1].TargetRuntimeDigest; got != request.Input.TargetRuntimeDigest {
		t.Fatalf("next placement runtime digest = %q, want %q", got, request.Input.TargetRuntimeDigest)
	}
}

type serviceConsensus struct {
	*memoryConsensus
	system raftstore.SystemState
}

func (s *serviceConsensus) ReadSystemStrong(context.Context) (raftstore.SystemState, error) {
	return s.system, nil
}

type blockingRecoveryConsensus struct {
	*serviceConsensus
	refreshes   chan struct{}
	scanStarted chan struct{}
	scanOnce    sync.Once
}

func (s *blockingRecoveryConsensus) RefreshPermit(ctx context.Context) (raftstore.PermitGrant, error) {
	select {
	case s.refreshes <- struct{}{}:
	default:
	}
	return s.memoryConsensus.RefreshPermit(ctx)
}

func (s *blockingRecoveryConsensus) ReadData(
	ctx context.Context,
	query raftstore.DataLookup,
) (raftstore.DataLookupResult, error) {
	if query.Pending != nil {
		s.scanOnce.Do(func() { close(s.scanStarted) })
		<-ctx.Done()
		return raftstore.DataLookupResult{}, ctx.Err()
	}
	return s.memoryConsensus.ReadData(ctx, query)
}

func TestRegistryPermitRefreshIsIndependentOfRecoveryScan(t *testing.T) {
	registryLayout := testControlRegistryLayout()
	memory, err := newMemoryConsensus(registryLayout)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := registryLayout.Digest()
	if err != nil {
		t.Fatal(err)
	}
	consensus := &blockingRecoveryConsensus{
		serviceConsensus: &serviceConsensus{memoryConsensus: memory, system: raftstore.SystemState{
			Initialized: true, ClusterID: registryLayout.ClusterID, RegistryGeneration: registryLayout.RegistryGeneration,
			SystemEpoch: 1, ActiveRegistryLayoutDigest: digest, NodeEnrollments: map[string]raftstore.NodeEnrollmentRecord{},
		}},
		refreshes: make(chan struct{}, 16), scanStarted: make(chan struct{}),
	}
	store, err := NewRaftStore(consensus, registryLayout, digest)
	if err != nil {
		t.Fatal(err)
	}
	config := DefaultRegistryServiceConfig()
	config.PermitRefreshInterval = 10 * time.Millisecond
	config.RecoveryScanInterval = time.Millisecond
	config.RecoveryShardsPerScan = 1
	service, err := NewRegistryService(
		store, &servicePlanner{}, serviceProber{}, &serviceDispatcher{outcome: clusterstate.DispatchUnknown},
		serviceCommandSender{}, config,
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- service.Run(ctx) }()
	defer cancel()
	select {
	case <-consensus.refreshes:
	case <-time.After(time.Second):
		t.Fatal("initial Permit refresh did not run")
	}
	select {
	case <-consensus.scanStarted:
	case <-time.After(time.Second):
		t.Fatal("recovery scan did not block")
	}
	for refresh := 0; refresh < 2; refresh++ {
		select {
		case <-consensus.refreshes:
		case <-time.After(time.Second):
			t.Fatal("blocked recovery scan stopped Permit refresh")
		}
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Registry service did not stop after cancellation")
	}
}

type servicePlanner struct {
	mu     sync.Mutex
	calls  []placer.PlanRequest
	mutate func(placer.PlanRequest, *placer.PlanResponse)
}

func (p *servicePlanner) Plan(
	_ context.Context,
	_ placement.CatalogSnapshot,
	request placer.PlanRequest,
) (placer.PlanResponse, error) {
	p.mu.Lock()
	p.calls = append(p.calls, request)
	p.mu.Unlock()
	candidates := make([]clusterstate.PlacementCandidate, placement.DefaultCandidateCount)
	for index := range candidates {
		candidates[index] = clusterstate.PlacementCandidate{
			NodeID: fmt.Sprintf("node-%d", index+1), RuntimeDigest: request.TargetRuntimeDigest,
			CatalogDigest: strings.Repeat("a", 64),
		}
	}
	switch request.Kind {
	case placer.PlanSandbox:
		lease := serviceKeyLease(request.Group)
		demand, err := placement.NormalizeSandboxDemand(request.Sandbox.Demand)
		if err != nil {
			return placer.PlanResponse{}, err
		}
		nodeRequest, err := api.RewriteSandboxCreateEnvelope(
			request.Sandbox.Request, "e2b-img-"+strings.Repeat("c", 64),
			request.Sandbox.TimeoutSeconds, clusterstate.WithoutSystemMetadata(request.Sandbox.Config),
		)
		if err != nil {
			return placer.PlanResponse{}, err
		}
		spec, err := clusterstate.MarshalSandboxDispatchSpec(clusterstate.SandboxDispatchSpecV1{
			Version: clusterstate.DispatchSpecVersionV1, TemplateRef: "e2b-img-" + strings.Repeat("c", 64),
			AuthKeyFingerprint: lease.AuthKey.Fingerprint, ManifestKeyFingerprint: lease.ManifestKey.Fingerprint,
			TargetRuntimeDigest: request.TargetRuntimeDigest,
			RequestedConfig:     clusterstate.WithoutSystemMetadata(request.Sandbox.Config),
			Config:              clusterstate.WithoutSystemMetadata(request.Sandbox.Config),
			AccessToken:         "access", TargetPort: 3000, TimeoutSeconds: request.Sandbox.TimeoutSeconds,
			Request: nodeRequest,
		})
		response := placer.PlanResponse{Candidates: candidates, NormalizedDemand: demand, DispatchSpec: spec, ProviderPolicyVersion: "policy-v1"}
		if p.mutate != nil {
			p.mutate(request, &response)
		}
		return response, err
	case placer.PlanBuild:
		lease := serviceKeyLease(request.Group)
		demand, err := placement.NormalizeBuildDemand(request.Build.Demand)
		if err != nil {
			return placer.PlanResponse{}, err
		}
		nodeRequest, err := api.RewriteBuildRegisterEnvelope(request.Build.Request, api.RegisterSpec{
			Name: request.Build.Names[0], Tags: request.Build.Aliases, Profile: request.Build.Profile,
			CPUCount: request.Build.CPUCount, MemoryMB: request.Build.MemoryMB,
			Metadata: clusterstate.WithoutSystemMetadata(request.Build.Metadata),
		})
		if err != nil {
			return placer.PlanResponse{}, err
		}
		spec, err := clusterstate.MarshalBuildDispatchSpec(clusterstate.BuildDispatchSpecV1{
			Version: clusterstate.DispatchSpecVersionV1, TemplateID: request.Build.TemplateID,
			AuthKeyFingerprint: lease.AuthKey.Fingerprint, ManifestKeyFingerprint: lease.ManifestKey.Fingerprint,
			TargetRuntimeDigest: request.TargetRuntimeDigest,
			Profile:             request.Build.Profile, Names: request.Build.Names, Aliases: request.Build.Aliases,
			Metadata: clusterstate.WithoutSystemMetadata(request.Build.Metadata),
			CPUCount: request.Build.CPUCount, MemoryMB: request.Build.MemoryMB, Request: nodeRequest,
		})
		response := placer.PlanResponse{Candidates: candidates, NormalizedDemand: demand, DispatchSpec: spec, ProviderPolicyVersion: "policy-v1"}
		if p.mutate != nil {
			p.mutate(request, &response)
		}
		return response, err
	default:
		return placer.PlanResponse{}, fmt.Errorf("unexpected plan kind %q", request.Kind)
	}
}

func TestRegistryRejectsPlacerRuntimeAndDemandDrift(t *testing.T) {
	t.Run("candidate runtime", func(t *testing.T) {
		service, store, planner, _ := newRegistryServiceFixture(t, clusterstate.DispatchUnknown)
		planner.mutate = func(_ placer.PlanRequest, response *placer.PlanResponse) {
			response.Candidates[0].RuntimeDigest = "wrong-runtime"
		}
		if _, err := service.ReserveSandbox(context.Background(), sandboxMutationRequest(t, store)); err == nil {
			t.Fatal("Placer candidate with a mismatched runtime fence was accepted")
		}
	})
	t.Run("normalized Build demand", func(t *testing.T) {
		service, store, planner, _ := newRegistryServiceFixture(t, clusterstate.DispatchUnknown)
		planner.mutate = func(request placer.PlanRequest, response *placer.PlanResponse) {
			demand := request.Build.Demand
			demand.Storage++
			response.NormalizedDemand, _ = placement.NormalizeBuildDemand(demand)
		}
		if _, err := service.RegisterBuild(context.Background(), buildMutationRequest(t, store)); err == nil {
			t.Fatal("Placer Build demand that differs from the immutable request was accepted")
		}
	})
}

func TestRouteListResponseIsBoundedBySerializedBytes(t *testing.T) {
	metadata := make(map[string]string, 4)
	for index := 0; index < 4; index++ {
		metadata[fmt.Sprintf("key-%d", index)] = strings.Repeat("\x01", clusterstate.MaxPresentationMetadataBytes/4-8)
	}
	routes := make([]routeapi.ListedRoute, 100)
	for index := range routes {
		routes[index] = routeapi.ListedRoute{
			RouteKey: fmt.Sprintf("route-%03d", index), State: clusterstate.WorkflowRouteReady,
			NodeID: "node-1", TemplateRef: "template-1",
			Presentation: clusterstate.SandboxPresentationV1{
				CPUCount: 1, MemoryMB: 512, DiskSizeMB: 64, EnvdVersion: "0.6.1",
				StartedAt: 1, EndAt: 2, Metadata: metadata,
			},
		}
	}
	response, err := boundedRouteListResponse(routes, 7, 99, "")
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) > routeapi.MaxListRoutesResponseBytes || len(response.Routes) == 0 ||
		len(response.Routes) >= len(routes) || response.NextRouteKey != response.Routes[len(response.Routes)-1].RouteKey {
		t.Fatalf("bounded response = bytes=%d routes=%d next=%q", len(encoded), len(response.Routes), response.NextRouteKey)
	}
	payloadBytes := 0
	for index, route := range response.Routes {
		raw, _ := json.Marshal(route)
		payloadBytes += len(raw)
		if index != 0 {
			payloadBytes++
		}
	}
	calculated, err := routeListResponseBytes(payloadBytes, response.Bucket, response.SnapshotRevision, response.NextRouteKey)
	if err != nil || calculated != len(encoded) {
		t.Fatalf("response byte accounting = %d, want %d, err=%v", calculated, len(encoded), err)
	}
}

func (p *servicePlanner) ResolveKeyLease(
	_ context.Context,
	request placer.KeyLeaseRequest,
) (routesync.NodeKeyLeaseV1, error) {
	lease := serviceKeyLease(request.Group)
	if request.AuthKeyFingerprint != lease.AuthKey.Fingerprint ||
		request.ManifestKeyFingerprint != lease.ManifestKey.Fingerprint {
		return routesync.NodeKeyLeaseV1{}, fmt.Errorf("unexpected key lease request: %+v", request)
	}
	return lease, nil
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

func (serviceCommandSender) InstallKeyLease(
	_ context.Context,
	_ session.ServeIdentity,
	_ string,
	_ uint64,
	_ string,
	lease routesync.NodeKeyLeaseV1,
) (routesync.NodeKeyLeaseRefV1, bool, error) {
	ref, err := lease.Ref()
	return ref, err == nil, err
}

type recordingKeyLeaseSender struct {
	mu    sync.Mutex
	calls int
}

func (*recordingKeyLeaseSender) SendNodeCommand(
	context.Context, session.ServeIdentity, string, uint64, string, *routesync.Command,
) (routesync.CmdAck, bool, error) {
	return routesync.CmdAck{}, false, nil
}

func (s *recordingKeyLeaseSender) InstallKeyLease(
	_ context.Context,
	_ session.ServeIdentity,
	_ string,
	_ uint64,
	_ string,
	lease routesync.NodeKeyLeaseV1,
) (routesync.NodeKeyLeaseRefV1, bool, error) {
	s.mu.Lock()
	s.calls++
	s.mu.Unlock()
	ref, err := lease.Ref()
	return ref, err == nil, err
}

func (s *recordingKeyLeaseSender) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func TestRegistryServiceRenewsActiveBindingKeyLeaseBeforeExpiry(t *testing.T) {
	service, store, _, _ := newRegistryServiceFixture(t, clusterstate.DispatchAcceptedAdmitted)
	sender := &recordingKeyLeaseSender{}
	service.commands = sender
	identity, err := store.ServeIdentity()
	if err != nil {
		t.Fatal(err)
	}
	lease := serviceKeyLease("/group")
	binding := raftstore.LeaseBinding{
		Group: "/group", NodeID: "node-1", NodeEpoch: 7, DataEndpoint: "node-1:8443",
		RegistryGeneration: identity.RegistryGeneration,
		AuthKeyFingerprint: lease.AuthKey.Fingerprint, ManifestKeyFingerprint: lease.ManifestKey.Fingerprint,
	}
	if err := service.renewKeyLease(context.Background(), identity, binding); err != nil {
		t.Fatal(err)
	}
	if err := service.renewKeyLease(context.Background(), identity, binding); err != nil {
		t.Fatal(err)
	}
	if sender.callCount() != 1 {
		t.Fatalf("key_put calls before renewal window = %d", sender.callCount())
	}
	cacheKey := leaseBindingCacheKey(binding)
	service.leaseMu.Lock()
	service.leaseExpiries[cacheKey] = time.Now().Add(placer.NodeKeyLeaseRenewBefore / 2).Unix()
	service.leaseMu.Unlock()
	if err := service.renewKeyLease(context.Background(), identity, binding); err != nil {
		t.Fatal(err)
	}
	if sender.callCount() != 2 {
		t.Fatalf("key_put calls after entering renewal window = %d", sender.callCount())
	}
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
	nodeRequest, err := clusterstate.NewNodeRequestEnvelopeV1(
		http.MethodPost, "/sandboxes", "feature=1", map[string][]string{"X-Node-Extension": {"keep"}},
		[]byte(`{"templateID":"caller-template","metadata":{"request":"value"},"extension":{"keep":true}}`),
	)
	if err != nil {
		t.Fatal(err)
	}
	return routeapi.ReserveSandboxRequest{
		RequestIdentity: identity, Group: "/group", RouteKey: "route-1",
		Input: routeapi.SandboxInput{
			Config: map[string]string{"request": "value"}, TimeoutSeconds: 30,
			Demand:              placement.SandboxDemand{SlotUnits: 1, StartupBudgetMemory: 1024},
			TargetRuntimeDigest: "runtime-v1",
			Request:             nodeRequest,
		},
	}
}

func buildMutationRequest(t *testing.T, store *RaftStore) routeapi.RegisterBuildRequest {
	t.Helper()
	identity, err := store.BuildRequestIdentity("/group", "build-1")
	if err != nil {
		t.Fatal(err)
	}
	nodeRequest, err := clusterstate.NewNodeRequestEnvelopeV1(
		http.MethodPost, "/v3/templates", "feature=1", map[string][]string{"X-Node-Extension": {"keep"}},
		[]byte(`{"name":"name","tags":["alias"],"profile":"bare","cpu_count":100,"memory_mb":1024,"metadata":{"request":"value"},"extension":{"keep":true}}`),
	)
	if err != nil {
		t.Fatal(err)
	}
	return routeapi.RegisterBuildRequest{
		RequestIdentity: identity, Group: "/group", BuildID: "build-1",
		Input: routeapi.BuildInput{
			TemplateID: "transient-template", Profile: types.ProfileBare,
			Names: []string{"name"}, Aliases: []string{"alias"},
			CPUCount: 100, MemoryMB: 1024,
			Metadata:            map[string]string{"request": "value"},
			Demand:              placement.BuildDemand{Slots: 1, CPU: 100_000, Memory: 1024 << 20, Storage: 4096},
			TargetRuntimeDigest: "runtime-v1",
			Request:             nodeRequest,
		},
	}
}

func serviceKeyLease(group string) routesync.NodeKeyLeaseV1 {
	material := func(value string) routesync.NodeKeyMaterialV1 {
		raw, _ := hex.DecodeString(value)
		digest := sha256.Sum256(raw)
		return routesync.NodeKeyMaterialV1{
			Type: routesync.KeyMaterialInline, Value: value, Fingerprint: hex.EncodeToString(digest[:12]),
		}
	}
	return routesync.NodeKeyLeaseV1{
		Version: routesync.NodeKeyLeaseVersionV1, Group: group, KeyRevision: 1,
		AuthKey: material(strings.Repeat("a", 64)), ManifestKey: material(strings.Repeat("b", 64)),
		ExpiresUnix: time.Now().Add(placer.DefaultNodeKeyLeaseTTL).Unix(),
	}
}
