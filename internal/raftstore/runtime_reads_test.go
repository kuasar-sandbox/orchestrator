package raftstore

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/routeapi"
	sm "github.com/lni/dragonboat/v4/statemachine"
)

func TestRuntimeSetServingGatesUsesDedicatedWorkflow(t *testing.T) {
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
		if command.Type != SystemSetGates {
			t.Fatalf("command type = %s", command.Type)
		}
		var result SystemApplyResult
		state, result = ApplySystemCommand(state, state.LastApplied+1, command)
		encoded, _ := json.Marshal(result)
		return sm.Result{Data: encoded}, nil
	}
	runtime := &Runtime{
		config:         RuntimeConfig{Tuning: DefaultRuntimeTuning()},
		registryLayout: registryLayout, registryLayoutDigest: digest, nodeHost: host,
		permitCache: NewPermitCache(time.Now),
		enrollment: LocalEnrollment{Replicas: []LocalReplicaEnrollment{{
			ShardID: SystemRaftShardID, ReplicaID: 1, StartPlan: ReplicaInitial, LocalState: ReplicaActive,
		}}},
	}
	opened, err := runtime.SetServingGates(context.Background(), GateUpdate{Serve: true, Write: true, Cutover: true})
	if err != nil || !opened.ServeGate || !opened.WriteGate || !opened.CutoverGate {
		t.Fatalf("configured gates = %+v, %v", opened, err)
	}
	if _, err := runtime.ApplySystem(context.Background(), SystemCommand{
		Type: SystemSetGates, Gates: &GateUpdate{Serve: false},
	}); err == nil {
		t.Fatal("raw SystemSetGates command was accepted")
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
