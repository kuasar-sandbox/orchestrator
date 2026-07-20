package raftstore

import (
	"fmt"
	"testing"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/routeapi"
)

func TestRouteLookupSeparatesLocalPositiveAndStrongNegativeReads(t *testing.T) {
	registryLayout := testRegistryLayout(4, "generation-1")
	state, identity := initializedRouteShard(t, registryLayout, "/g", "rk")
	starting := routeStarting(t, registryLayout, "/g", "rk", "sandbox-1", 1, true)
	applyDataOK(t, &state, 2, DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{Absent: true}, Route: &starting,
	})

	request := routeLookupRequest(identity, "/g", "rk", false)
	response := lookupRouteResult(t, state, request)
	if response.Outcome != routeapi.ReadNeedLeader {
		t.Fatalf("local STARTING outcome = %s", response.Outcome)
	}
	request.Strong = true
	response = lookupRouteResult(t, state, request)
	if response.Outcome != routeapi.ReadConflict {
		t.Fatalf("strong STARTING outcome = %s", response.Outcome)
	}

	ready := readyRecord(starting, 1)
	applyDataOK(t, &state, 3, DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{LogIndex: 2}, Route: &ready,
	})
	request.Strong = false
	response = lookupRouteResult(t, state, request)
	if response.Outcome != routeapi.ReadReady || response.RouteRevision != 3 || response.Route.SandboxID != "sandbox-1" {
		t.Fatalf("local READY response = %+v", response)
	}
	if err := response.ValidateFor(request); err != nil {
		t.Fatal(err)
	}

	request.MinRouteRevision = 4
	if got := lookupRouteResult(t, state, request).Outcome; got != routeapi.ReadReplicaBehind {
		t.Fatalf("minimum-revision outcome = %s", got)
	}
	missingKey := routeKeyForShard(t, registryLayout, "/g", identity.ShardID, "missing")
	missing := routeLookupRequest(identity, "/g", missingKey, false)
	if got := lookupRouteResult(t, state, missing).Outcome; got != routeapi.ReadNeedLeader {
		t.Fatalf("local miss outcome = %s", got)
	}
	missing.Strong = true
	missingResponse := lookupRouteResult(t, state, missing)
	if missingResponse.Outcome != routeapi.ReadNotFound || missingResponse.ValidateFor(missing) != nil {
		t.Fatalf("strong miss response = %+v", missingResponse)
	}

	fenced := routeLookupRequest(identity, "/g", "rk", false)
	fenced.RegistryLayoutDigest = digestFor("old-registryLayout")
	if got := lookupRouteResult(t, state, fenced).Outcome; got != routeapi.ReadUnavailable {
		t.Fatalf("fenced identity outcome = %s", got)
	}
}

func TestBuildLookupReturnsOnlyBoundPositiveProjection(t *testing.T) {
	registryLayout := testRegistryLayout(4, "generation-1")
	state, identity := initializedBuildShard(t, registryLayout, "/g", "build-1")
	starting := buildStarting(t, registryLayout, "/g", "build-1", true)
	applyDataOK(t, &state, 2, DataCommand{
		Type: DataPutBuild, Identity: identity, Expect: RevisionExpectation{Absent: true}, Build: &starting,
	})
	request := buildLookupRequest(identity, "/g", "build-1", false)
	if got := lookupBuildResult(t, state, request).Outcome; got != routeapi.ReadNeedLeader {
		t.Fatalf("unprojected Build outcome = %s", got)
	}

	registered := buildRegistrationRecord(starting)
	applyDataOK(t, &state, 3, DataCommand{
		Type: DataPutBuild, Identity: identity, Expect: RevisionExpectation{LogIndex: 2}, Build: &registered,
	})
	response := lookupBuildResult(t, state, request)
	if response.Outcome != routeapi.ReadReady || response.BuildState != clusterstate.BuildRegistered || response.BuildRevision != 3 {
		t.Fatalf("registered Build response = %+v", response)
	}
	if err := response.ValidateFor(request); err != nil {
		t.Fatal(err)
	}
	request.MinBuildRevision = 4
	if got := lookupBuildResult(t, state, request).Outcome; got != routeapi.ReadReplicaBehind {
		t.Fatalf("Build minimum-revision outcome = %s", got)
	}
}

func TestPendingLookupRecoversCommittedWorkflowIntentOnly(t *testing.T) {
	registryLayout := testRegistryLayout(4, "generation-1")
	routeKey := "rk"
	identity := routeShardIdentity(t, registryLayout, "/g", routeKey)
	buildID := buildIDForShard(t, registryLayout, "/g", identity.ShardID)
	state := initializeDataShard(t, registryLayout, identity)
	route := routeStarting(t, registryLayout, "/g", routeKey, "sandbox-1", 1, true)
	build := buildStarting(t, registryLayout, "/g", buildID, true)
	applyDataOK(t, &state, 2, DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{Absent: true}, Route: &route,
	})
	applyDataOK(t, &state, 3, DataCommand{
		Type: DataPutBuild, Identity: identity, Expect: RevisionExpectation{Absent: true}, Build: &build,
	})

	first := pendingResult(t, state, PendingLookup{Identity: identity, Limit: 1})
	if len(first.Workflows) != 1 || first.NextKey == "" {
		t.Fatalf("first recovery page = %+v", first)
	}
	second := pendingResult(t, state, PendingLookup{Identity: identity, AfterKey: first.NextKey, Limit: 1})
	if len(second.Workflows) != 1 || second.NextKey != "" || second.Workflows[0].Key == first.Workflows[0].Key {
		t.Fatalf("second recovery page = %+v", second)
	}

	for _, workflow := range append(first.Workflows, second.Workflows...) {
		switch {
		case workflow.Route != nil:
			workflow.Route.Starting.Intent.DispatchSpec[0] ^= 1
			stored := state.Routes[routeMapKey("/g", routeKey)]
			if stored.Starting.Intent.DispatchSpec[0] == workflow.Route.Starting.Intent.DispatchSpec[0] {
				t.Fatal("pending Route lookup aliases consensus intent")
			}
		case workflow.Build != nil:
			workflow.Build.Starting.Intent.DispatchSpec[0] ^= 1
			stored := state.Builds[buildMapKey("/g", buildID)]
			if stored.Starting.Intent.DispatchSpec[0] == workflow.Build.Starting.Intent.DispatchSpec[0] {
				t.Fatal("pending Build lookup aliases consensus intent")
			}
		default:
			t.Fatal("pending lookup returned an empty workflow")
		}
	}
}

func TestRouteBucketSnapshotAndChangefeedUseShardRevisions(t *testing.T) {
	registryLayout := testRegistryLayout(4, "generation-1")
	group := "/g"
	firstKey := "rk"
	bucket, _, err := clusterstate.RouteShardFor(
		group, firstKey, registryLayout.RouteBucketCount, registryLayout.VirtualShardCount,
	)
	if err != nil {
		t.Fatal(err)
	}
	identity := routeShardIdentity(t, registryLayout, group, firstKey)
	state := initializeDataShard(t, registryLayout, identity)
	first := routeStarting(t, registryLayout, group, firstKey, "sandbox-1", 1, true)
	applyDataOK(t, &state, 2, DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{Absent: true}, Route: &first,
	})
	ready := readyRecord(first, 1)
	applyDataOK(t, &state, 3, DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{LogIndex: 2}, Route: &ready,
	})

	secondKey := routeKeyForBucket(t, registryLayout, group, bucket, "second")
	second := routeStarting(t, registryLayout, group, secondKey, "sandbox-2", 1, true)
	applyDataOK(t, &state, 4, DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{Absent: true}, Route: &second,
	})
	otherGroup := "/other"
	otherKey := routeKeyForShard(t, registryLayout, otherGroup, identity.ShardID, "other")
	other := routeStarting(t, registryLayout, otherGroup, otherKey, "sandbox-3", 1, true)
	applyDataOK(t, &state, 5, DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{Absent: true}, Route: &other,
	})

	list := lookupRouteBucket(state, RouteBucketLookup{Identity: identity, Group: group, Bucket: bucket})
	if !list.Available || list.SnapshotRevision != 5 || len(list.Routes) != 2 ||
		list.Routes[0].RouteKey != firstKey || list.Routes[1].RouteKey != secondKey {
		t.Fatalf("Route bucket snapshot = %+v", list)
	}
	list.Routes[0].Ready.AccessToken = "mutated"
	if state.Routes[routeMapKey(group, firstKey)].Ready.AccessToken == "mutated" {
		t.Fatal("Route bucket snapshot aliases consensus state")
	}

	page := lookupRouteChangefeed(state, RouteChangefeedLookup{
		Identity: identity, Group: group, Bucket: bucket, AfterRevision: 1, Limit: 2,
	})
	if !page.Available || page.Reset || len(page.Changes) != 2 || page.Changes[0].Revision != 2 ||
		page.Changes[0].RouteKey != firstKey || page.Changes[0].State != clusterstate.WorkflowRouteStarting ||
		page.Changes[1].Revision != 3 || page.Changes[1].State != clusterstate.WorkflowRouteReady ||
		page.CursorRevision != 3 || page.HeadRevision != 5 {
		t.Fatalf("first Route changefeed page = %+v", page)
	}
	page = lookupRouteChangefeed(state, RouteChangefeedLookup{
		Identity: identity, Group: group, Bucket: bucket, AfterRevision: page.CursorRevision, Limit: 2,
	})
	if !page.Available || len(page.Changes) != 1 || page.Changes[0].Revision != 4 || page.CursorRevision != 5 {
		t.Fatalf("second Route changefeed page = %+v", page)
	}

	advanceDataApplied(&state, RouteChangefeedRetentionRevisions+10)
	reset := lookupRouteChangefeed(state, RouteChangefeedLookup{
		Identity: identity, Group: group, Bucket: bucket, AfterRevision: 1, Limit: 1,
	})
	if !reset.Available || !reset.Reset || reset.FloorRevision != 10 || len(reset.Changes) != 0 {
		t.Fatalf("compacted Route changefeed = %+v", reset)
	}
}

func lookupRouteResult(t *testing.T, state DataState, request routeapi.ReadRouteRequest) routeapi.ReadRouteResponse {
	t.Helper()
	result, err := LookupData(state, DataLookup{Route: &request})
	if err != nil || result.Route == nil {
		t.Fatalf("Route lookup = %+v, %v", result, err)
	}
	return *result.Route
}

func lookupBuildResult(t *testing.T, state DataState, request routeapi.ReadBuildRequest) routeapi.ReadBuildResponse {
	t.Helper()
	result, err := LookupData(state, DataLookup{Build: &request})
	if err != nil || result.Build == nil {
		t.Fatalf("Build lookup = %+v, %v", result, err)
	}
	return *result.Build
}

func pendingResult(t *testing.T, state DataState, query PendingLookup) PendingLookupResult {
	t.Helper()
	result, err := LookupData(state, DataLookup{Pending: &query})
	if err != nil || result.Pending == nil {
		t.Fatalf("pending lookup = %+v, %v", result, err)
	}
	return *result.Pending
}

func routeLookupRequest(
	identity ShardRequestIdentity,
	group, routeKey string,
	strong bool,
) routeapi.ReadRouteRequest {
	return routeapi.ReadRouteRequest{
		RequestIdentity: routeIdentity(identity), Group: group, RouteKey: routeKey,
		Strong: strong,
	}
}

func buildLookupRequest(identity ShardRequestIdentity, group, buildID string, strong bool) routeapi.ReadBuildRequest {
	return routeapi.ReadBuildRequest{
		RequestIdentity: routeIdentity(identity), Group: group, BuildID: buildID, Strong: strong,
	}
}

func routeIdentity(identity ShardRequestIdentity) routeapi.RequestIdentity {
	return routeapi.RequestIdentity{
		ClusterID: identity.ClusterID, RegistryGeneration: identity.RegistryGeneration,
		SystemEpoch: identity.SystemEpoch, RegistryLayoutDigest: identity.RegistryLayoutDigest, ShardID: identity.ShardID,
	}
}

func routeKeyForShard(t *testing.T, registryLayout RegistryLayout, group string, shardID uint32, prefix string) string {
	t.Helper()
	for index := 0; index < 10000; index++ {
		key := fmt.Sprintf("%s-%d", prefix, index)
		_, got, err := clusterstate.RouteShardFor(group, key, registryLayout.RouteBucketCount, registryLayout.VirtualShardCount)
		if err != nil {
			t.Fatal(err)
		}
		if got == shardID {
			return key
		}
	}
	t.Fatal("no Route key mapped to test shard")
	return ""
}

func routeKeyForBucket(t *testing.T, registryLayout RegistryLayout, group string, bucket uint32, prefix string) string {
	t.Helper()
	for index := 0; index < 10000; index++ {
		key := fmt.Sprintf("%s-%d", prefix, index)
		got, _, err := clusterstate.RouteShardFor(group, key, registryLayout.RouteBucketCount, registryLayout.VirtualShardCount)
		if err != nil {
			t.Fatal(err)
		}
		if got == bucket {
			return key
		}
	}
	t.Fatal("no Route key mapped to test bucket")
	return ""
}

func buildIDForShard(t *testing.T, registryLayout RegistryLayout, group string, shardID uint32) string {
	t.Helper()
	for index := 0; index < 10000; index++ {
		buildID := fmt.Sprintf("build-%d", index)
		_, got, err := clusterstate.BuildShardFor(group, buildID, registryLayout.BuildBucketCount, registryLayout.VirtualShardCount)
		if err != nil {
			t.Fatal(err)
		}
		if got == shardID {
			return buildID
		}
	}
	t.Fatal("no Build ID mapped to test shard")
	return ""
}
