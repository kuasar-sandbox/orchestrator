package raftstore

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/routeapi"
)

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
	if err := runtime.authorizeLocalDataReplica(identity); err != nil {
		t.Fatalf("draining old epoch was rejected before Permit/DataState checks: %v", err)
	}
}
