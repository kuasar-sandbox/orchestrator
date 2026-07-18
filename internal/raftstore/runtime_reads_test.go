package raftstore

import (
	"context"
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
