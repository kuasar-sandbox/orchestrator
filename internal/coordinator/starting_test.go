package coordinator

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/placement"
	"github.com/kuasar-sandbox/orchestrator/internal/session"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

type fixedTie bool

func (t fixedTie) ChooseSecond() (bool, error) { return bool(t), nil }

type workflowStoreStub struct {
	route  cluster.RouteWorkflowRecord
	build  cluster.BuildRecord
	fences map[string]cluster.ExecutionFence
	events []string
	fail   error
}

func (s *workflowStoreStub) CommitRouteWorkflow(_ context.Context, expected cluster.Revision, next cluster.RouteWorkflowRecord) (cluster.RouteWorkflowRecord, error) {
	if s.fail != nil {
		return cluster.RouteWorkflowRecord{}, s.fail
	}
	if expected != s.route.Revision {
		return cluster.RouteWorkflowRecord{}, errors.New("Route CAS conflict")
	}
	next.Revision.LogIndex++
	s.events = append(s.events, describeRouteCommit(next))
	s.route = next
	return next, nil
}

func (s *workflowStoreStub) CommitBuildWorkflow(_ context.Context, expected cluster.Revision, next cluster.BuildRecord) (cluster.BuildRecord, error) {
	if s.fail != nil {
		return cluster.BuildRecord{}, s.fail
	}
	if expected != s.build.Revision {
		return cluster.BuildRecord{}, errors.New("Build CAS conflict")
	}
	next.Revision.LogIndex++
	s.events = append(s.events, describeBuildCommit(next))
	s.build = next
	return next, nil
}

func (s *workflowStoreStub) EnsureExecutionFence(_ context.Context, fence cluster.ExecutionFence) error {
	if s.fail != nil {
		return s.fail
	}
	if s.route.State != cluster.WorkflowRouteTombstone || s.route.Tombstone.PlacementFailure == nil {
		return errors.New("placement fence was not preceded by a tombstone")
	}
	if s.fences == nil {
		s.fences = make(map[string]cluster.ExecutionFence)
	}
	fence.Revision = s.route.Revision
	fence.Revision.LogIndex++
	s.fences[fence.SandboxID] = fence
	s.events = append(s.events, "commit:placement-fence:"+fence.SandboxID)
	return nil
}

func describeRouteCommit(record cluster.RouteWorkflowRecord) string {
	if record.State == cluster.WorkflowRouteTombstone {
		return "commit:route-terminal"
	}
	if record.Starting.SelectedCandidate != nil {
		return "commit:select:" + record.Starting.CandidatePool[*record.Starting.SelectedCandidate].NodeID
	}
	return fmt.Sprintf("commit:route-unselected:%d", len(record.Starting.DefinitivelyRejected))
}

func describeBuildCommit(record cluster.BuildRecord) string {
	if record.State == cluster.BuildTombstone {
		return "commit:build-terminal"
	}
	if record.Starting.SelectedCandidate != nil {
		return "commit:select:" + record.Starting.CandidatePool[*record.Starting.SelectedCandidate].NodeID
	}
	return fmt.Sprintf("commit:build-unselected:%d", len(record.Starting.DefinitivelyRejected))
}

type pairProberStub struct {
	responses map[string]placement.PlacementProbeResponse
	calls     [][]string
	events    *[]string
	after     func()
}

func (p *pairProberStub) ProbePair(_ context.Context, _ session.ServeIdentity, requests []placement.PlacementProbeRequest) []session.ProbeResult {
	ids := make([]string, len(requests))
	results := make([]session.ProbeResult, len(requests))
	for index, request := range requests {
		ids[index] = request.NodeID
		response, found := p.responses[request.NodeID]
		if !found {
			response = probeResponse(request.NodeID, placement.ProbeStale, 0)
		}
		results[index].Response = response
	}
	p.calls = append(p.calls, ids)
	if p.events != nil {
		*p.events = append(*p.events, "probe:"+strings.Join(ids, ","))
	}
	if p.after != nil {
		p.after()
	}
	return results
}

type dispatcherStub struct {
	store    *workflowStoreStub
	results  map[string][]session.DispatchReply
	errors   map[string]error
	requests []session.DispatchCommand
}

func (d *dispatcherStub) AdmitAndDispatch(_ context.Context, request session.DispatchCommand) (session.DispatchReply, error) {
	d.requests = append(d.requests, request)
	d.store.events = append(d.store.events, "dispatch:"+request.NodeID)
	if request.Kind == cluster.ExecutionKindSandbox {
		if d.store.route.Starting == nil || d.store.route.Starting.SelectedCandidate == nil ||
			d.store.route.Starting.Binding == nil || d.store.route.Starting.Binding.BindingDigest != request.Binding.BindingDigest {
			return session.DispatchReply{}, errors.New("dispatch happened before selected Route intent was committed")
		}
	} else if d.store.build.Starting == nil || d.store.build.Starting.SelectedCandidate == nil ||
		d.store.build.Starting.Binding == nil || d.store.build.Starting.Binding.BindingDigest != request.Binding.BindingDigest {
		return session.DispatchReply{}, errors.New("dispatch happened before selected Build intent was committed")
	}
	if err := d.errors[request.NodeID]; err != nil {
		return session.DispatchReply{}, err
	}
	results := d.results[request.NodeID]
	if len(results) == 0 {
		return session.DispatchReply{Outcome: cluster.DispatchAcceptedAdmitted}, nil
	}
	result := results[0]
	d.results[request.NodeID] = results[1:]
	return result, nil
}

type roundSourceStub struct {
	sandboxID  string
	candidates []cluster.PlacementCandidate
	intent     *cluster.DispatchIntent
	calls      int
}

func (s *roundSourceStub) NextSandboxRound(_ context.Context, _, _ string, _ uint64, previous cluster.DispatchIntent) (SandboxRound, error) {
	s.calls++
	intent := previous
	if s.intent != nil {
		intent = *s.intent
	}
	return SandboxRound{
		SandboxID: s.sandboxID, Candidates: append([]cluster.PlacementCandidate(nil), s.candidates...),
		Intent: cloneIntent(intent),
	}, nil
}

func TestRouteCoordinatorCommitsSelectionBeforeDispatch(t *testing.T) {
	record := routeStartingRecord(t, "s1", 1, candidatePool("n1", "n2", "n3", "n4"), nil)
	store := &workflowStoreStub{route: record}
	prober := &pairProberStub{responses: map[string]placement.PlacementProbeResponse{
		"n1": probeResponse("n1", placement.ProbeImmediate, 100),
		"n2": probeResponse("n2", placement.ProbeImmediate, 200),
	}, events: &store.events}
	dispatcher := &dispatcherStub{store: store, results: map[string][]session.DispatchReply{}}
	coordinator := newTestCoordinator(t, store, prober, dispatcher, nil)
	result, err := coordinator.RunRoute(context.Background(), record)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != RunWaitingForEvent || result.Route == nil || result.Route.Starting.Binding.DataEndpoint != "n1:8443" {
		t.Fatalf("result = %+v", result)
	}
	want := []string{"probe:n1,n2", "commit:select:n1", "dispatch:n1"}
	if !reflect.DeepEqual(store.events, want) {
		t.Fatalf("events = %v, want %v", store.events, want)
	}
}

func TestDefinitiveRejectIsCommittedBeforeFreshPairLoserFallback(t *testing.T) {
	record := routeStartingRecord(t, "s1", 1, candidatePool("n1", "n2", "n3", "n4"), nil)
	store := &workflowStoreStub{route: record}
	prober := &pairProberStub{responses: map[string]placement.PlacementProbeResponse{
		"n1": probeResponse("n1", placement.ProbeImmediate, 100),
		"n2": probeResponse("n2", placement.ProbeImmediate, 200),
	}, events: &store.events}
	dispatcher := &dispatcherStub{store: store, results: map[string][]session.DispatchReply{
		"n1": {{Outcome: cluster.DispatchDefinitiveReject, Reason: "queue closed"}},
		"n2": {{Outcome: cluster.DispatchAcceptedQueued}},
	}}
	coordinator := newTestCoordinator(t, store, prober, dispatcher, nil)
	result, err := coordinator.RunRoute(context.Background(), record)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != RunWaitingForEvent || result.Route == nil || *result.Route.Starting.SelectedCandidate != 1 ||
		!reflect.DeepEqual(result.Route.Starting.DefinitivelyRejected, []uint32{0}) {
		t.Fatalf("result = %+v", result)
	}
	want := []string{
		"probe:n1,n2", "commit:select:n1", "dispatch:n1",
		"commit:route-unselected:1", "commit:select:n2", "dispatch:n2",
	}
	if !reflect.DeepEqual(store.events, want) {
		t.Fatalf("events = %v, want %v", store.events, want)
	}
	if len(prober.calls) != 1 {
		t.Fatalf("fresh loser was re-probed: %v", prober.calls)
	}
}

func TestAmbiguousDispatchPinsSelectedExecution(t *testing.T) {
	record := routeStartingRecord(t, "s1", 1, candidatePool("n1", "n2"), nil)
	store := &workflowStoreStub{route: record}
	prober := &pairProberStub{responses: map[string]placement.PlacementProbeResponse{
		"n1": probeResponse("n1", placement.ProbeImmediate, 100),
		"n2": probeResponse("n2", placement.ProbeWouldQueue, 50),
	}}
	dispatcher := &dispatcherStub{store: store, results: map[string][]session.DispatchReply{}, errors: map[string]error{"n1": errors.New("ACK lost")}}
	coordinator := newTestCoordinator(t, store, prober, dispatcher, nil)
	result, err := coordinator.RunRoute(context.Background(), record)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != RunPinnedUnknown || result.Outcome != cluster.DispatchUnknown ||
		result.Route == nil || *result.Route.Starting.SelectedCandidate != 0 || len(result.Route.Starting.DefinitivelyRejected) != 0 {
		t.Fatalf("result = %+v", result)
	}
	if len(dispatcher.requests) != 1 {
		t.Fatalf("ambiguous dispatch advanced to another node: %+v", dispatcher.requests)
	}
}

func TestLeaderRecoveryRetriesPersistedSelectionBeforeProbe(t *testing.T) {
	record := routeStartingRecord(t, "s1", 1, candidatePool("n1", "n2"), nil)
	probe := probeResponse("n2", placement.ProbeImmediate, 1)
	binding, err := makeBinding("g1", cluster.ExecutionKindSandbox, record.Group, record.RouteKey, record.Starting.SandboxID, record.Starting.Intent, probe)
	if err != nil {
		t.Fatal(err)
	}
	record.Starting.SelectedCandidate = uint32Pointer(1)
	record.Starting.Binding = &binding
	if err := record.Validate(); err != nil {
		t.Fatal(err)
	}
	store := &workflowStoreStub{route: record}
	prober := &pairProberStub{responses: map[string]placement.PlacementProbeResponse{}}
	dispatcher := &dispatcherStub{store: store, results: map[string][]session.DispatchReply{}}
	coordinator := newTestCoordinator(t, store, prober, dispatcher, nil)
	result, err := coordinator.RunRoute(context.Background(), record)
	if err != nil || result.Status != RunWaitingForEvent {
		t.Fatalf("result = %+v, %v", result, err)
	}
	if len(prober.calls) != 0 || len(dispatcher.requests) != 1 || dispatcher.requests[0].NodeID != "n2" {
		t.Fatalf("probes=%v dispatches=%+v", prober.calls, dispatcher.requests)
	}
}

func TestSessionMovedKeepsSameSelectedCandidate(t *testing.T) {
	record := routeStartingRecord(t, "s1", 1, candidatePool("n1", "n2"), nil)
	store := &workflowStoreStub{route: record}
	prober := &pairProberStub{responses: map[string]placement.PlacementProbeResponse{
		"n1": probeResponse("n1", placement.ProbeImmediate, 1),
		"n2": probeResponse("n2", placement.ProbeWouldQueue, 1),
	}}
	dispatcher := &dispatcherStub{store: store, results: map[string][]session.DispatchReply{
		"n1": {{Outcome: cluster.DispatchSessionMoved}},
	}}
	coordinator := newTestCoordinator(t, store, prober, dispatcher, nil)
	result, err := coordinator.RunRoute(context.Background(), record)
	if err != nil || result.Status != RunRetrySelected || result.Route == nil || *result.Route.Starting.SelectedCandidate != 0 ||
		len(result.Route.Starting.DefinitivelyRejected) != 0 {
		t.Fatalf("result = %+v, %v", result, err)
	}
}

func TestSandboxRoundLimitAndBuildPoolExhaustionCommitTerminalState(t *testing.T) {
	rejected := []uint32{0, 1}
	route := routeStartingRecord(t, "s2", 2, candidatePool("n1", "n2"), rejected)
	store := &workflowStoreStub{route: route}
	prober := &pairProberStub{responses: map[string]placement.PlacementProbeResponse{}}
	dispatcher := &dispatcherStub{store: store, results: map[string][]session.DispatchReply{}}
	coordinator := newTestCoordinator(t, store, prober, dispatcher, nil)
	result, err := coordinator.RunRoute(context.Background(), route)
	if err != nil || result.Status != RunTerminal || result.Route == nil || result.Route.State != cluster.WorkflowRouteTombstone ||
		result.Route.Tombstone.PlacementFailure == nil {
		t.Fatalf("Route result = %+v, %v", result, err)
	}

	build := buildStartingRecord(t, "b1", candidatePool("n1", "n2"), rejected)
	store.build = build
	result, err = coordinator.RunBuild(context.Background(), build)
	if err != nil || result.Status != RunTerminal || result.Build == nil || result.Build.State != cluster.BuildTombstone ||
		result.Build.Tombstone == nil {
		t.Fatalf("Build result = %+v, %v", result, err)
	}
}

func TestExhaustedSandboxRoundCommitsNewIDBeforeNewPlacement(t *testing.T) {
	record := routeStartingRecord(t, "s1", 1, candidatePool("n1", "n2"), []uint32{0, 1})
	priorBinding, err := makeBinding(
		"g1", cluster.ExecutionKindSandbox, record.Group, record.RouteKey, "prior-sandbox",
		record.Starting.Intent, probeResponse("prior-node", placement.ProbeImmediate, 1),
	)
	if err != nil {
		t.Fatal(err)
	}
	priorFinalization, err := cluster.NewWorkflowFinalizationIntent("prior-sandbox", priorBinding, nil)
	if err != nil {
		t.Fatal(err)
	}
	record.Finalizations = []cluster.WorkflowFinalizationIntent{priorFinalization}
	store := &workflowStoreStub{route: record}
	rounds := &roundSourceStub{sandboxID: "s2", candidates: candidatePool("n3", "n4")}
	prober := &pairProberStub{responses: map[string]placement.PlacementProbeResponse{
		"n3": probeResponse("n3", placement.ProbeImmediate, 1),
		"n4": probeResponse("n4", placement.ProbeWouldQueue, 1),
	}}
	dispatcher := &dispatcherStub{store: store, results: map[string][]session.DispatchReply{}}
	coordinator := newTestCoordinator(t, store, prober, dispatcher, rounds)
	result, err := coordinator.RunRoute(context.Background(), record)
	if err != nil || result.Status != RunWaitingForEvent || result.Route.Starting.SandboxID != "s2" ||
		result.Route.Starting.PlacementRound != 2 || rounds.calls != 1 {
		t.Fatalf("result = %+v, rounds=%d, err=%v", result, rounds.calls, err)
	}
	if len(dispatcher.requests) != 1 || dispatcher.requests[0].ObjectID != "s2" {
		t.Fatalf("dispatches = %+v", dispatcher.requests)
	}
	if len(result.Route.Finalizations) != 1 || result.Route.Finalizations[0].BindingDigest != priorFinalization.BindingDigest {
		t.Fatalf("prior finalization was lost across placement rounds: %+v", result.Route.Finalizations)
	}
	wantPrefix := []string{"commit:route-terminal", "commit:placement-fence:s1"}
	if len(store.events) < len(wantPrefix) || !reflect.DeepEqual(store.events[:len(wantPrefix)], wantPrefix) {
		t.Fatalf("round handoff events = %v", store.events)
	}
}

func TestCommitFailurePreventsDispatch(t *testing.T) {
	record := routeStartingRecord(t, "s1", 1, candidatePool("n1", "n2"), nil)
	store := &workflowStoreStub{route: record, fail: errors.New("quorum unavailable")}
	prober := &pairProberStub{responses: map[string]placement.PlacementProbeResponse{
		"n1": probeResponse("n1", placement.ProbeImmediate, 1),
		"n2": probeResponse("n2", placement.ProbeWouldQueue, 1),
	}}
	dispatcher := &dispatcherStub{store: store, results: map[string][]session.DispatchReply{}}
	coordinator := newTestCoordinator(t, store, prober, dispatcher, nil)
	if _, err := coordinator.RunRoute(context.Background(), record); err == nil {
		t.Fatal("selection commit failure was ignored")
	}
	if len(dispatcher.requests) != 0 {
		t.Fatalf("dispatch occurred without committed selection: %+v", dispatcher.requests)
	}
}

func TestProbeRPCLatencyExpiresOtherwiseUsableSamples(t *testing.T) {
	record := routeStartingRecord(t, "s1", 1, candidatePool("n1", "n2"), nil)
	store := &workflowStoreStub{route: record}
	now := time.Unix(10, 0)
	prober := &pairProberStub{
		responses: map[string]placement.PlacementProbeResponse{
			"n1": probeResponse("n1", placement.ProbeImmediate, 1),
			"n2": probeResponse("n2", placement.ProbeImmediate, 2),
		},
		after: func() { now = now.Add(placement.MaximumProbeSampleAge) },
	}
	dispatcher := &dispatcherStub{store: store, results: map[string][]session.DispatchReply{}}
	coordinator, err := NewStartingCoordinator(Config{
		ServeIdentity:       coordinatorServeIdentity(),
		PlacementRoundLimit: 2, Clock: func() time.Time { return now }, TieBreaker: fixedTie(false),
	}, prober, dispatcher, store, store, nil)
	if err != nil {
		t.Fatal(err)
	}
	result, err := coordinator.RunRoute(context.Background(), record)
	if err != nil || result.Status != RunNoUsableProbe {
		t.Fatalf("result = %+v, err=%v", result, err)
	}
	if len(dispatcher.requests) != 0 || len(store.events) != 0 {
		t.Fatalf("expired Probe caused side effects: dispatches=%d events=%v", len(dispatcher.requests), store.events)
	}
}

func newTestCoordinator(t *testing.T, store *workflowStoreStub, prober *pairProberStub, dispatcher *dispatcherStub, rounds SandboxRoundSource) *StartingCoordinator {
	t.Helper()
	coordinator, err := NewStartingCoordinator(Config{
		ServeIdentity:       coordinatorServeIdentity(),
		PlacementRoundLimit: 2, Clock: time.Now, TieBreaker: fixedTie(false),
	}, prober, dispatcher, store, store, rounds)
	if err != nil {
		t.Fatal(err)
	}
	return coordinator
}

func coordinatorServeIdentity() session.ServeIdentity {
	return session.ServeIdentity{
		ClusterID: "c1", RegistryGeneration: "g1", SystemEpoch: 1,
		RegistryLayoutDigest: strings.Repeat("a", 64),
	}
}

func routeStartingRecord(t *testing.T, sandboxID string, round uint64, candidates []cluster.PlacementCandidate, rejected []uint32) cluster.RouteWorkflowRecord {
	t.Helper()
	demand, err := placement.NormalizeSandboxDemand(placement.SandboxDemand{SlotUnits: 1})
	if err != nil {
		t.Fatal(err)
	}
	spec, err := cluster.MarshalSandboxDispatchSpec(cluster.SandboxDispatchSpecV1{
		Version: cluster.DispatchSpecVersionV1, TemplateRef: "e2b-img-" + strings.Repeat("c", 64),
		AuthKeyFingerprint: strings.Repeat("a", 24), ManifestKeyFingerprint: strings.Repeat("b", 24),
		AccessToken: "access-token", TargetPort: 3000,
		Request: cluster.NodeRequestEnvelopeV1{Version: cluster.NodeRequestEnvelopeVersionV1, Method: "POST", Path: "/sandboxes", Body: []byte("{}")},
	})
	if err != nil {
		t.Fatal(err)
	}
	intent, err := cluster.NewDispatchIntent(demand, spec, "provider-v1/policy-v1")
	if err != nil {
		t.Fatal(err)
	}
	record := cluster.RouteWorkflowRecord{
		Group: "/g", RouteKey: "rk", State: cluster.WorkflowRouteStarting,
		Revision: cluster.Revision{RegistryGeneration: "g1", ShardID: 7, LogIndex: 10},
		Starting: &cluster.RouteStartingState{
			SandboxID: sandboxID, PlacementRound: round, CandidatePool: candidates,
			DefinitivelyRejected: rejected, Intent: intent,
		},
	}
	if err := record.Validate(); err != nil {
		t.Fatal(err)
	}
	return record
}

func buildStartingRecord(t *testing.T, buildID string, candidates []cluster.PlacementCandidate, rejected []uint32) cluster.BuildRecord {
	t.Helper()
	demand, err := placement.NormalizeBuildDemand(placement.BuildDemand{Slots: 1, CPU: 1000})
	if err != nil {
		t.Fatal(err)
	}
	spec, err := cluster.MarshalBuildDispatchSpec(cluster.BuildDispatchSpecV1{
		Version: cluster.DispatchSpecVersionV1, TemplateID: "template-1",
		AuthKeyFingerprint: strings.Repeat("b", 24), ManifestKeyFingerprint: strings.Repeat("c", 24),
		Profile: types.ProfileBare, CPUCount: 1, MemoryMB: 512,
		Request: cluster.NodeRequestEnvelopeV1{Version: cluster.NodeRequestEnvelopeVersionV1, Method: "POST", Path: "/v3/templates", Body: []byte("{}")},
	})
	if err != nil {
		t.Fatal(err)
	}
	intent, err := cluster.NewDispatchIntent(demand, spec, "provider-v1/policy-v1")
	if err != nil {
		t.Fatal(err)
	}
	record := cluster.BuildRecord{
		Group: "/g", BuildID: buildID, State: cluster.BuildStarting,
		Revision: cluster.Revision{RegistryGeneration: "g1", ShardID: 8, LogIndex: 20},
		Starting: &cluster.BuildStartingState{
			BuildID: buildID, CandidatePool: candidates, DefinitivelyRejected: rejected, Intent: intent,
		},
	}
	if err := record.Validate(); err != nil {
		t.Fatal(err)
	}
	return record
}

func candidatePool(ids ...string) []cluster.PlacementCandidate {
	candidates := make([]cluster.PlacementCandidate, len(ids))
	for index, id := range ids {
		candidates[index] = cluster.PlacementCandidate{NodeID: id, RuntimeDigest: "runtime-v1"}
	}
	return candidates
}

func probeResponse(nodeID string, class placement.ProbeClass, rate uint32) placement.PlacementProbeResponse {
	return placement.PlacementProbeResponse{
		Class: class, NodeID: nodeID, NodeEpoch: 7, SessionSeq: 11, DataEndpoint: nodeID + ":8443",
		SampleSeq: 3, SampleAge: 10 * time.Millisecond, LoadModelVersion: placement.LoadModelVersion, RatePPM: rate,
	}
}
