package raftstore

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/routeapi"
	sm "github.com/lni/dragonboat/v4/statemachine"
)

func TestRuntimeConfigureServiceGatesUsesDedicatedWorkflow(t *testing.T) {
	registryLayout := testRegistryLayout(1, "generation-gates")
	digest, _ := registryLayout.Digest()
	state, _ := applySystem(t, SystemState{}, 1, SystemCommand{
		Type: SystemBootstrap, RegistryLayout: &registryLayout, Digest: digest,
	})
	host := newFakeNodeHost()
	host.read = func(_ uint64, _ any) (any, error) { return state, nil }
	host.propose = func(raw []byte) (sm.Result, error) {
		command, err := DecodeSystemCommand(raw)
		if err != nil {
			return sm.Result{}, err
		}
		var result SystemApplyResult
		state, result = ApplySystemCommand(state, state.LastApplied+1, command)
		encoded, _ := json.Marshal(result)
		return sm.Result{Data: encoded}, nil
	}
	runtime := &Runtime{
		registryLayout: registryLayout, registryLayoutDigest: digest, nodeHost: host,
		permitCache: NewPermitCache(time.Now),
		enrollment: LocalEnrollment{Replicas: []LocalReplicaEnrollment{{
			ShardID: SystemRaftShardID, ReplicaID: 1, StartPlan: ReplicaInitial, LocalState: ReplicaActive,
		}}},
	}
	opened, err := runtime.ConfigureServiceGates(context.Background(), GateUpdate{Serve: true, Write: true, Cutover: true})
	if err != nil || !opened.ServeGate || !opened.WriteGate || !opened.CutoverGate {
		t.Fatalf("configured gates = %+v, %v", opened, err)
	}
}

func TestLeaderHintUsesThePermitRegistryLayout(t *testing.T) {
	startup := testRegistryLayout(1, "generation-hint")
	startupDigest, err := startup.Digest()
	if err != nil {
		t.Fatal(err)
	}
	target := cloneRegistryLayout(startup)
	target.RegistryLayoutVersion++
	target.PreviousRegistryLayoutVersion = startup.RegistryLayoutVersion
	target.PreviousRegistryLayoutDigest = startupDigest
	oldLeader := startup.DataShards[0].Replicas[0]
	target.DataShards[0].Replicas[0].ReplicaID += 100
	targetDigest, err := target.Digest()
	if err != nil {
		t.Fatal(err)
	}
	host := newFakeNodeHost()
	host.leaderID, host.leaderTerm = oldLeader.ReplicaID, 9
	runtime := &Runtime{
		registryLayout: target, registryLayoutDigest: targetDigest,
		startupRegistryLayout: startup, nodeHost: host,
	}
	result := DataLookupResult{Route: &routeapi.ReadRouteResponse{Outcome: routeapi.ReadNeedLeader}}
	runtime.attachLeaderHint(0, ShardRequestIdentity{PermitIdentity: PermitIdentity{
		RegistryLayoutDigest: startupDigest,
	}}, &result)
	member, found := registryLayoutMember(startup, oldLeader.MemberID)
	if !found {
		t.Fatal("startup leader member is missing")
	}
	if result.Route.LeaderHint == nil || result.Route.LeaderHint.MemberID != member.MemberID ||
		result.Route.LeaderHint.Endpoint != member.InternalEndpoint || result.Route.LeaderHint.Term != 9 {
		t.Fatalf("predecessor leader hint = %+v", result.Route.LeaderHint)
	}
}

func TestRuntimeResolvesAmbiguousDataMutationByExactStrongRead(t *testing.T) {
	registryLayout := testRegistryLayout(1, "generation-ambiguous")
	identity := routeShardIdentity(t, registryLayout, "/g", "rk")
	state := initializeDataShard(t, registryLayout, identity)
	starting := routeStarting(t, registryLayout, "/g", "rk", "sandbox-1", 1, false)
	command := DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{Absent: true}, Route: &starting,
	}
	host := newFakeNodeHost()
	host.propose = func(raw []byte) (sm.Result, error) {
		committed, err := DecodeDataCommand(raw)
		if err != nil {
			return sm.Result{}, err
		}
		result := ApplyDataCommand(&state, 2, committed)
		if !result.Applied {
			t.Fatalf("ambiguous mutation did not commit: %+v", result)
		}
		return sm.Result{}, context.DeadlineExceeded
	}
	host.read = func(_ uint64, query any) (any, error) {
		return LookupDataMutation(state, query.(DataMutationLookup))
	}
	digest, _ := registryLayout.Digest()
	runtime := &Runtime{
		registryLayout: registryLayout, registryLayoutDigest: digest, nodeHost: host,
		permitCache: NewPermitCache(time.Now), member: registryLayout.Members[0],
		enrollment: LocalEnrollment{Replicas: []LocalReplicaEnrollment{{
			ShardID: DataRaftShardID(identity.ShardID), ReplicaID: 1, StartPlan: ReplicaInitial, LocalState: ReplicaActive,
		}}},
	}
	if err := runtime.permitCache.Install(PermitGrant{
		PermitIdentity: identity.PermitIdentity, CommitIndex: 1, MaxLifetimeMillis: 1_000,
		ServeGate: true, WriteGate: true, CutoverGate: true, RecoveryClosed: true,
	}, time.Now()); err != nil {
		t.Fatal(err)
	}
	result, err := runtime.ApplyData(context.Background(), command)
	if err != nil || !result.Applied || result.Revision != 2 {
		t.Fatalf("resolved mutation = %+v, %v", result, err)
	}
}

func TestRuntimeResolvesCommittedDataMutationAfterCallerCancellation(t *testing.T) {
	registryLayout := testRegistryLayout(1, "generation-canceled-mutation")
	identity := routeShardIdentity(t, registryLayout, "/g", "rk-canceled")
	state := initializeDataShard(t, registryLayout, identity)
	starting := routeStarting(t, registryLayout, "/g", "rk-canceled", "sandbox-canceled", 1, false)
	command := DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{Absent: true}, Route: &starting,
	}
	caller, cancelCaller := context.WithCancel(context.Background())
	host := newFakeNodeHost()
	host.proposeCtx = func(_ context.Context, raw []byte) (sm.Result, error) {
		committed, err := DecodeDataCommand(raw)
		if err != nil {
			return sm.Result{}, err
		}
		result := ApplyDataCommand(&state, 2, committed)
		if !result.Applied {
			t.Fatalf("mutation did not commit before cancellation: %+v", result)
		}
		cancelCaller()
		return sm.Result{}, context.Canceled
	}
	host.readCtx = func(ctx context.Context, _ uint64, query any) (any, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return LookupDataMutation(state, query.(DataMutationLookup))
	}
	digest, _ := registryLayout.Digest()
	runtime := &Runtime{
		registryLayout: registryLayout, registryLayoutDigest: digest, nodeHost: host,
		permitCache: NewPermitCache(time.Now), member: registryLayout.Members[0],
		enrollment: LocalEnrollment{Replicas: []LocalReplicaEnrollment{{
			ShardID: DataRaftShardID(identity.ShardID), ReplicaID: 1,
			StartPlan: ReplicaInitial, LocalState: ReplicaActive,
		}}},
	}
	if err := runtime.permitCache.Install(PermitGrant{
		PermitIdentity: identity.PermitIdentity, CommitIndex: 1, MaxLifetimeMillis: 1_000,
		ServeGate: true, WriteGate: true, CutoverGate: true, RecoveryClosed: true,
	}, time.Now()); err != nil {
		t.Fatal(err)
	}
	result, err := runtime.ApplyData(caller, command)
	if err != nil || !result.Applied || result.Revision != 2 {
		t.Fatalf("canceled caller ambiguity resolution = %+v, %v", result, err)
	}
}

func TestRuntimeResolvesCommittedGateUpdateAfterCallerCancellation(t *testing.T) {
	registryLayout := testRegistryLayout(1, "generation-canceled-gates")
	digest, _ := registryLayout.Digest()
	state, _ := applySystem(t, SystemState{}, 1, SystemCommand{
		Type: SystemBootstrap, RegistryLayout: &registryLayout, Digest: digest,
	})
	caller, cancelCaller := context.WithCancel(context.Background())
	host := newFakeNodeHost()
	host.proposeCtx = func(_ context.Context, raw []byte) (sm.Result, error) {
		command, err := DecodeSystemCommand(raw)
		if err != nil {
			return sm.Result{}, err
		}
		var result SystemApplyResult
		state, result = ApplySystemCommand(state, state.LastApplied+1, command)
		if !result.Applied {
			t.Fatalf("gate update did not commit: %+v", result)
		}
		cancelCaller()
		return sm.Result{}, context.Canceled
	}
	host.readCtx = func(ctx context.Context, _ uint64, _ any) (any, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return state, nil
	}
	runtime := &Runtime{
		registryLayout: registryLayout, registryLayoutDigest: digest, nodeHost: host,
		permitCache: NewPermitCache(time.Now),
		enrollment: LocalEnrollment{Replicas: []LocalReplicaEnrollment{{
			ShardID: SystemRaftShardID, ReplicaID: 1, StartPlan: ReplicaInitial, LocalState: ReplicaActive,
		}}},
	}
	opened, err := runtime.ConfigureServiceGates(caller, GateUpdate{Serve: true, Write: true, Cutover: true})
	if err != nil || !opened.ServeGate || !opened.WriteGate || !opened.CutoverGate {
		t.Fatalf("canceled gate ambiguity resolution = %+v, %v", opened, err)
	}
}

func TestRuntimeRequiresTrustedProofWorkflowForExecutionTombstoneAndFence(t *testing.T) {
	registryLayout := testRegistryLayout(1, "generation-terminal-proof")
	identity := routeShardIdentity(t, registryLayout, "/g", "rk-terminal-proof")
	state := initializeDataShard(t, registryLayout, identity)
	starting := routeStarting(t, registryLayout, "/g", "rk-terminal-proof", "sandbox-terminal", 1, true)
	applyDataOK(t, &state, 2, DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{Absent: true}, Route: &starting,
	})
	ready := readyRecord(starting, 1)
	applyDataOK(t, &state, 3, DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{LogIndex: 2}, Route: &ready,
	})
	deleting := deletingRecord(ready)
	applyDataOK(t, &state, 4, DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{LogIndex: 3}, Route: &deleting,
	})
	tombstone, fence := terminalRouteAndFence(deleting, 2)
	runtime := runtimeWithDataState(t, registryLayout, identity, &state)
	tombstoneCommand := DataCommand{
		Type: DataPutRoute, Identity: identity,
		Expect: RevisionExpectation{LogIndex: 4}, Route: &tombstone,
	}
	if _, err := runtime.ApplyData(context.Background(), tombstoneCommand); err == nil {
		t.Fatal("generic Route mutation accepted an unverified execution tombstone")
	}
	if _, err := runtime.ApplyProvenExecutionMutation(context.Background(), tombstoneCommand); err == nil {
		t.Fatal("dedicated tombstone workflow accepted no trusted verifier")
	}
	verified := 0
	runtime.terminalVerifier = TerminalProofVerifierFunc(func(_ context.Context, request TerminalProofRequest) error {
		verified++
		if request.Tombstone.Proof.Kind != clusterstate.ProofNodeTerminal ||
			request.CurrentRoute.Revision.LogIndex == 0 {
			return errors.New("wrong proof source")
		}
		return nil
	})
	result, err := runtime.ApplyProvenExecutionMutation(context.Background(), tombstoneCommand)
	if err != nil || !result.Applied || state.Routes[routeMapKey("/g", "rk-terminal-proof")].Tombstone == nil {
		t.Fatalf("proven tombstone = %+v, %v", result, err)
	}
	fenceCommand := DataCommand{
		Type: DataPutFence, Identity: identity,
		Expect: RevisionExpectation{Absent: true}, Fence: &fence,
	}
	if _, err := runtime.ApplyData(context.Background(), fenceCommand); err == nil {
		t.Fatal("generic data mutation accepted an execution fence")
	}
	result, err = runtime.ApplyProvenExecutionMutation(context.Background(), fenceCommand)
	if err != nil || !result.Applied || len(state.Fences) != 1 || verified != 2 {
		t.Fatalf("proven fence = %+v, verifier calls=%d, %v", result, verified, err)
	}
}

func TestRuntimeRejectsTerminalMutationWhenTrustedSourceDisagrees(t *testing.T) {
	registryLayout := testRegistryLayout(1, "generation-forged-terminal")
	identity := routeShardIdentity(t, registryLayout, "/g", "rk-forged-terminal")
	state := initializeDataShard(t, registryLayout, identity)
	starting := routeStarting(t, registryLayout, "/g", "rk-forged-terminal", "sandbox-forged", 1, true)
	applyDataOK(t, &state, 2, DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{Absent: true}, Route: &starting,
	})
	ready := readyRecord(starting, 1)
	applyDataOK(t, &state, 3, DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{LogIndex: 2}, Route: &ready,
	})
	deleting := deletingRecord(ready)
	applyDataOK(t, &state, 4, DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{LogIndex: 3}, Route: &deleting,
	})
	tombstone, _ := terminalRouteAndFence(deleting, 2)
	runtime := runtimeWithDataState(t, registryLayout, identity, &state)
	runtime.terminalVerifier = TerminalProofVerifierFunc(func(context.Context, TerminalProofRequest) error {
		return errors.New("durable node event is absent")
	})
	_, err := runtime.ApplyProvenExecutionMutation(context.Background(), DataCommand{
		Type: DataPutRoute, Identity: identity,
		Expect: RevisionExpectation{LogIndex: 4}, Route: &tombstone,
	})
	if err == nil || state.Routes[routeMapKey("/g", "rk-forged-terminal")].State != clusterstate.WorkflowRouteDeleting {
		t.Fatalf("forged terminal proof mutation error = %v", err)
	}
}

func TestRuntimeCommitsPlacementFailureFenceWithoutExecutionProof(t *testing.T) {
	registryLayout := testRegistryLayout(1, "generation-placement-fence")
	identity := routeShardIdentity(t, registryLayout, "/g", "rk-placement-fence")
	state := initializeDataShard(t, registryLayout, identity)
	starting := routeStarting(t, registryLayout, "/g", "rk-placement-fence", "sandbox-abandoned", 1, true)
	applyDataOK(t, &state, 2, DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{Absent: true}, Route: &starting,
	})
	rejectedFirst := cloneRouteRecord(state.Routes[routeMapKey("/g", "rk-placement-fence")])
	firstBinding := *rejectedFirst.Starting.Binding
	rejectedFirst.Starting.SelectedCandidate = nil
	rejectedFirst.Starting.Binding = nil
	rejectedFirst.Starting.DefinitivelyRejected = []uint32{0}
	rejectedFirst.Finalizations = []clusterstate.WorkflowFinalizationIntent{
		workflowFinalization(t, rejectedFirst.Starting.SandboxID, firstBinding, nil),
	}
	applyDataOK(t, &state, 3, DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{LogIndex: 2}, Route: &rejectedFirst,
	})
	selectedSecond := cloneRouteRecord(state.Routes[routeMapKey("/g", "rk-placement-fence")])
	second := uint32(1)
	secondBinding := testBinding(
		t, registryLayout, clusterstate.ExecutionKindSandbox, selectedSecond.Starting.SandboxID,
		selectedSecond.Group, selectedSecond.RouteKey, "node-2", selectedSecond.Starting.Intent,
	)
	selectedSecond.Starting.SelectedCandidate = &second
	selectedSecond.Starting.Binding = &secondBinding
	applyDataOK(t, &state, 4, DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{LogIndex: 3}, Route: &selectedSecond,
	})
	exhausted := cloneRouteRecord(state.Routes[routeMapKey("/g", "rk-placement-fence")])
	exhausted.Starting.SelectedCandidate = nil
	exhausted.Starting.Binding = nil
	exhausted.Starting.DefinitivelyRejected = []uint32{0, 1}
	exhausted.Finalizations = append(exhausted.Finalizations,
		workflowFinalization(t, exhausted.Starting.SandboxID, secondBinding, nil))
	applyDataOK(t, &state, 5, DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{LogIndex: 4}, Route: &exhausted,
	})
	failure := clusterstate.RoutePlacementFailureState{
		SandboxID: exhausted.Starting.SandboxID, PlacementRound: exhausted.Starting.PlacementRound,
		CandidatePool:        append([]clusterstate.PlacementCandidate(nil), exhausted.Starting.CandidatePool...),
		DefinitivelyRejected: append([]uint32(nil), exhausted.Starting.DefinitivelyRejected...),
		Intent:               exhausted.Starting.Intent, Reason: "placement candidate pool exhausted",
	}
	tombstone := clusterstate.RouteWorkflowRecord{
		Group: exhausted.Group, RouteKey: exhausted.RouteKey, State: clusterstate.WorkflowRouteTombstone,
		Tombstone: &clusterstate.RouteTombstoneState{PlacementFailure: &failure},
	}
	applyDataOK(t, &state, 6, DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{LogIndex: 5}, Route: &tombstone,
	})
	fence, err := clusterstate.NewPlacementFailureFence(
		exhausted.Group, exhausted.RouteKey, registryLayout.RegistryGeneration, failure,
	)
	if err != nil {
		t.Fatal(err)
	}
	runtime := runtimeWithDataState(t, registryLayout, identity, &state)
	result, err := runtime.ApplyData(context.Background(), DataCommand{
		Type: DataPutFence, Identity: identity, Expect: RevisionExpectation{Absent: true}, Fence: &fence,
	})
	if err != nil || !result.Applied || len(state.Fences) != 1 {
		t.Fatalf("placement-failure fence = %+v, %v", result, err)
	}
}

func runtimeWithDataState(
	t *testing.T,
	registryLayout RegistryLayout,
	identity ShardRequestIdentity,
	state *DataState,
) *Runtime {
	t.Helper()
	host := newFakeNodeHost()
	host.read = func(_ uint64, query any) (any, error) {
		switch value := query.(type) {
		case DataLookup:
			return LookupData(*state, value)
		case DataMutationLookup:
			return LookupDataMutation(*state, value)
		default:
			return nil, errors.New("unexpected data lookup")
		}
	}
	host.propose = func(raw []byte) (sm.Result, error) {
		command, err := DecodeDataCommand(raw)
		if err != nil {
			return sm.Result{}, err
		}
		result := ApplyDataCommand(state, state.LastApplied+1, command)
		encoded, _ := json.Marshal(result)
		return sm.Result{Data: encoded}, nil
	}
	digest, _ := registryLayout.Digest()
	runtime := &Runtime{
		registryLayout: registryLayout, registryLayoutDigest: digest, nodeHost: host,
		permitCache: NewPermitCache(time.Now), member: registryLayout.Members[0],
		enrollment: LocalEnrollment{Replicas: []LocalReplicaEnrollment{{
			ShardID: DataRaftShardID(identity.ShardID), ReplicaID: registryLayout.DataShards[identity.ShardID].Replicas[0].ReplicaID,
			StartPlan: ReplicaInitial, LocalState: ReplicaActive,
		}}},
	}
	if err := runtime.permitCache.Install(PermitGrant{
		PermitIdentity: identity.PermitIdentity, CommitIndex: 1, MaxLifetimeMillis: 1_000,
		ServeGate: true, WriteGate: true, CutoverGate: true, RecoveryClosed: true,
	}, time.Now()); err != nil {
		t.Fatal(err)
	}
	return runtime
}

func TestRuntimeLocalReadRequiresPermitAndReturnsLeaderHintNotFinalMiss(t *testing.T) {
	registryLayout := testRegistryLayout(4, "generation-1")
	identity := routeShardIdentity(t, registryLayout, "/g", "missing")
	state := initializeDataShard(t, registryLayout, identity)
	host := newFakeNodeHost()
	host.leaderID, host.leaderTerm = 2, 7
	host.read = func(shardID uint64, query any) (any, error) {
		return LookupData(state, query.(DataLookup))
	}
	runtime := &Runtime{
		registryLayout: registryLayout, registryLayoutDigest: identity.RegistryLayoutDigest,
		nodeHost: host, permitCache: NewPermitCache(time.Now),
		member: registryLayout.Members[0], enrollment: LocalEnrollment{Replicas: []LocalReplicaEnrollment{{
			ShardID: DataRaftShardID(identity.ShardID), ReplicaID: 1,
			StartPlan: ReplicaInitial, LocalState: ReplicaActive,
		}}},
	}
	request := routeapi.ReadRouteRequest{
		RequestIdentity: routeIdentity(identity), Group: "/g", RouteKey: "missing",
	}
	query := DataLookup{Route: &request}
	if _, err := runtime.ReadData(context.Background(), query); err != ErrPermitMissing {
		t.Fatalf("read without permit error = %v", err)
	}
	grant := PermitGrant{
		PermitIdentity: identity.PermitIdentity, CommitIndex: 2, MaxLifetimeMillis: 1000,
		ServeGate: true, WriteGate: true, CutoverGate: true, RecoveryClosed: true,
	}
	if err := runtime.permitCache.Install(grant, time.Now()); err != nil {
		t.Fatal(err)
	}
	result, err := runtime.ReadData(context.Background(), query)
	if err != nil {
		t.Fatal(err)
	}
	if result.Route == nil || result.Route.Outcome != routeapi.ReadNeedLeader ||
		result.Route.LeaderHint == nil || result.Route.LeaderHint.MemberID != "registry-b" ||
		result.Route.LeaderHint.Term != 7 {
		t.Fatalf("local miss = %+v", result.Route)
	}

	request.Strong = true
	result, err = runtime.ReadData(context.Background(), DataLookup{Route: &request})
	if err != nil {
		t.Fatal(err)
	}
	if result.Route == nil || result.Route.Outcome != routeapi.ReadNotFound || result.Route.LeaderHint != nil {
		t.Fatalf("strong miss = %+v", result.Route)
	}
}

func TestRuntimeRejectsRawDataLifecycleCommands(t *testing.T) {
	runtime := &Runtime{permitCache: NewPermitCache(time.Now)}
	for _, commandType := range []DataCommandType{
		DataInitializeShard, DataPrepareEpoch, DataRetireEpoch, DataCompactFence,
	} {
		if _, err := runtime.ApplyData(context.Background(), DataCommand{Type: commandType}); err == nil {
			t.Fatalf("raw %s command was accepted", commandType)
		}
	}
}

func TestRemovedReplicaCanOnlyServeTheDrainingRegistryLayoutEpoch(t *testing.T) {
	oldRegistryLayout := testRegistryLayout(2, "generation-1")
	oldDigest, err := oldRegistryLayout.Digest()
	if err != nil {
		t.Fatal(err)
	}
	nextRegistryLayout := transitionRegistryLayout(t)
	nextRegistryLayout.RegistryLayoutVersion = 2
	nextRegistryLayout.PreviousRegistryLayoutVersion = 1
	nextRegistryLayout.PreviousRegistryLayoutDigest = oldDigest
	nextDigest, err := nextRegistryLayout.Digest()
	if err != nil {
		t.Fatal(err)
	}
	member, found := registryLayoutMember(nextRegistryLayout, "registry-c")
	if !found {
		t.Fatal("old replica member is absent from the transition catalog")
	}
	runtime := &Runtime{
		registryLayout: nextRegistryLayout, registryLayoutDigest: nextDigest, member: member,
		enrollment: LocalEnrollment{Replicas: []LocalReplicaEnrollment{{
			ShardID: DataRaftShardID(0), ReplicaID: 3,
			StartPlan: ReplicaInitial, LocalState: ReplicaActive,
		}}},
	}
	identity := ShardRequestIdentity{PermitIdentity: PermitIdentity{
		ClusterID: oldRegistryLayout.ClusterID, RegistryGeneration: oldRegistryLayout.RegistryGeneration,
		SystemEpoch: 2, RegistryLayoutDigest: nextDigest,
	}, ShardID: 0}
	if err := runtime.authorizeLocalDataReplica(identity); !errors.Is(err, ErrNoLocalReplica) {
		t.Fatalf("removed replica next-epoch authorization = %v", err)
	}
	identity.SystemEpoch = 1
	identity.RegistryLayoutDigest = oldDigest
	runtime.enrollment.Replicas[0].LocalState = ReplicaRemoving
	if err := runtime.authorizeLocalDataReplica(identity); err != nil {
		t.Fatalf("draining old epoch was rejected before Permit/DataState checks: %v", err)
	}
}
