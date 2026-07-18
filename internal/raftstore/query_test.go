package raftstore

import (
	"fmt"
	"testing"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/routeapi"
)

func TestRouteLookupSeparatesLocalPositiveAndStrongNegativeReads(t *testing.T) {
	manifest := testManifest(4, "generation-1")
	state, identity := initializedRouteShard(t, manifest, "/g", "rk")
	starting := routeStarting(t, manifest, "/g", "rk", "sandbox-1", 1, true)
	applyDataOK(t, &state, 2, DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{Absent: true}, Route: &starting,
	})

	request := routeLookupRequest(identity, "/g", "rk", "sandbox-1", false)
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
	request.MinRouteRevision = 0
	request.SandboxID = "sandbox-old"
	if got := lookupRouteResult(t, state, request).Outcome; got != routeapi.ReadNeedLeader {
		t.Fatalf("local SID mismatch outcome = %s", got)
	}

	missingKey := routeKeyForShard(t, manifest, "/g", identity.ShardID, "missing")
	missing := routeLookupRequest(identity, "/g", missingKey, "", false)
	if got := lookupRouteResult(t, state, missing).Outcome; got != routeapi.ReadNeedLeader {
		t.Fatalf("local miss outcome = %s", got)
	}
	missing.Strong = true
	missingResponse := lookupRouteResult(t, state, missing)
	if missingResponse.Outcome != routeapi.ReadNotFound || missingResponse.ValidateFor(missing) != nil {
		t.Fatalf("strong miss response = %+v", missingResponse)
	}

	fenced := routeLookupRequest(identity, "/g", "rk", "", false)
	fenced.ManifestDigest = digestFor("old-manifest")
	if got := lookupRouteResult(t, state, fenced).Outcome; got != routeapi.ReadUnavailable {
		t.Fatalf("fenced identity outcome = %s", got)
	}
}

func TestBuildLookupReturnsOnlyBoundPositiveProjection(t *testing.T) {
	manifest := testManifest(4, "generation-1")
	state, identity := initializedBuildShard(t, manifest, "/g", "build-1")
	starting := buildStarting(t, manifest, "/g", "build-1", true)
	applyDataOK(t, &state, 2, DataCommand{
		Type: DataPutBuild, Identity: identity, Expect: RevisionExpectation{Absent: true}, Build: &starting,
	})
	request := buildLookupRequest(identity, "/g", "build-1", false)
	if got := lookupBuildResult(t, state, request).Outcome; got != routeapi.ReadNeedLeader {
		t.Fatalf("unprojected Build outcome = %s", got)
	}

	queued := buildProjectionRecord(starting, clusterstate.BuildQueued, 1)
	applyDataOK(t, &state, 3, DataCommand{
		Type: DataPutBuild, Identity: identity, Expect: RevisionExpectation{LogIndex: 2}, Build: &queued,
	})
	response := lookupBuildResult(t, state, request)
	if response.Outcome != routeapi.ReadReady || response.BuildState != clusterstate.BuildQueued || response.BuildRevision != 3 {
		t.Fatalf("queued Build response = %+v", response)
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
	manifest := testManifest(4, "generation-1")
	routeKey := "rk"
	identity := routeShardIdentity(t, manifest, "/g", routeKey)
	buildID := buildIDForShard(t, manifest, "/g", identity.ShardID)
	state := initializeDataShard(t, manifest, identity)
	route := routeStarting(t, manifest, "/g", routeKey, "sandbox-1", 1, true)
	build := buildStarting(t, manifest, "/g", buildID, true)
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
	group, routeKey, sandboxID string,
	strong bool,
) routeapi.ReadRouteRequest {
	return routeapi.ReadRouteRequest{
		RequestIdentity: routeIdentity(identity), Group: group, RouteKey: routeKey,
		SandboxID: sandboxID, Strong: strong,
	}
}

func buildLookupRequest(identity ShardRequestIdentity, group, buildID string, strong bool) routeapi.ReadBuildRequest {
	return routeapi.ReadBuildRequest{
		RequestIdentity: routeIdentity(identity), Group: group, BuildID: buildID, Strong: strong,
	}
}

func routeIdentity(identity ShardRequestIdentity) routeapi.RequestIdentity {
	return routeapi.RequestIdentity{
		ClusterID: identity.ClusterID, StorageGeneration: identity.StorageGeneration,
		SystemEpoch: identity.SystemEpoch, ManifestDigest: identity.ManifestDigest, ShardID: identity.ShardID,
	}
}

func routeKeyForShard(t *testing.T, manifest Manifest, group string, shardID uint32, prefix string) string {
	t.Helper()
	for index := 0; index < 10000; index++ {
		key := fmt.Sprintf("%s-%d", prefix, index)
		_, got, err := clusterstate.RouteShardFor(group, key, manifest.RouteBucketCount, manifest.VirtualShardCount)
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

func buildIDForShard(t *testing.T, manifest Manifest, group string, shardID uint32) string {
	t.Helper()
	for index := 0; index < 10000; index++ {
		buildID := fmt.Sprintf("build-%d", index)
		_, got, err := clusterstate.BuildShardFor(group, buildID, manifest.BuildBucketCount, manifest.VirtualShardCount)
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
