package raftstore

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/routeapi"
)

func TestRuntimeLocalReadRequiresPermitAndReturnsLeaderHintNotFinalMiss(t *testing.T) {
	manifest := testManifest(4, "generation-1")
	identity := routeShardIdentity(t, manifest, "/g", "missing")
	state := initializeDataShard(t, manifest, identity)
	host := newFakeNodeHost()
	host.leaderID, host.leaderTerm = 2, 7
	host.read = func(shardID uint64, query any) (any, error) {
		return LookupData(state, query.(DataLookup))
	}
	runtime := &Runtime{
		manifest: manifest, manifestDigest: identity.ManifestDigest,
		nodeHost: host, permitCache: NewPermitCache(time.Now),
		member: manifest.Members[0], enrollment: LocalEnrollment{Replicas: []LocalReplicaEnrollment{{
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

func TestRemovedReplicaCanOnlyServeTheDrainingManifestEpoch(t *testing.T) {
	oldManifest := testManifest(2, "generation-1")
	oldDigest, err := oldManifest.Digest()
	if err != nil {
		t.Fatal(err)
	}
	nextManifest := transitionManifest(t)
	nextManifest.ManifestVersion = 2
	nextManifest.PreviousManifestDigest = oldDigest
	nextDigest, err := nextManifest.Digest()
	if err != nil {
		t.Fatal(err)
	}
	member, found := manifestMember(nextManifest, "registry-c")
	if !found {
		t.Fatal("old replica member is absent from the transition catalog")
	}
	runtime := &Runtime{
		manifest: nextManifest, manifestDigest: nextDigest, member: member,
		enrollment: LocalEnrollment{Replicas: []LocalReplicaEnrollment{{
			ShardID: DataRaftShardID(0), ReplicaID: 3,
			StartPlan: ReplicaInitial, LocalState: ReplicaActive,
		}}},
	}
	identity := ShardRequestIdentity{PermitIdentity: PermitIdentity{
		ClusterID: oldManifest.ClusterID, StorageGeneration: oldManifest.StorageGeneration,
		SystemEpoch: 2, ManifestDigest: nextDigest,
	}, ShardID: 0}
	if err := runtime.authorizeLocalDataReplica(identity); !errors.Is(err, ErrNoLocalReplica) {
		t.Fatalf("removed replica next-epoch authorization = %v", err)
	}
	identity.SystemEpoch = 1
	identity.ManifestDigest = oldDigest
	if err := runtime.authorizeLocalDataReplica(identity); err != nil {
		t.Fatalf("draining old epoch was rejected before Permit/DataState checks: %v", err)
	}
}
