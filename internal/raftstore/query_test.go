package raftstore

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
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

	paused := pausedRecord(ready, 2)
	applyDataOK(t, &state, 4, DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{LogIndex: 3}, Route: &paused,
	})
	request.MinRouteRevision = 0
	if got := lookupRouteResult(t, state, request).Outcome; got != routeapi.ReadNeedLeader {
		t.Fatalf("local PAUSED outcome = %s", got)
	}
	request.Strong = true
	if got := lookupRouteResult(t, state, request).Outcome; got != routeapi.ReadConflict {
		t.Fatalf("ordinary strong PAUSED outcome = %s", got)
	}
	request.Addressable = true
	response = lookupRouteResult(t, state, request)
	if response.Outcome != routeapi.ReadReady || response.State != clusterstate.WorkflowRoutePaused ||
		response.RouteRevision != 4 || response.Route.SandboxID != "sandbox-1" {
		t.Fatalf("addressable PAUSED response = %+v", response)
	}
	if err := response.ValidateFor(request); err != nil {
		t.Fatal(err)
	}
	request.Strong, request.Addressable = false, false

	request.MinRouteRevision = 5
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
	request.Strong = true
	pending := lookupBuildResult(t, state, request)
	if pending.Outcome != routeapi.ReadConflict || pending.BuildState != clusterstate.BuildStarting ||
		pending.BuildRevision != 2 || pending.Pending == nil || pending.Pending.BuildID != "build-1" ||
		pending.Pending.TemplateRef != "template-1" {
		t.Fatalf("strong BUILD_STARTING projection = %+v", pending)
	}
	if err := pending.ValidateFor(request); err != nil {
		t.Fatal(err)
	}

	registered := buildRegistrationRecord(starting)
	applyDataOK(t, &state, 3, DataCommand{
		Type: DataPutBuild, Identity: identity, Expect: RevisionExpectation{LogIndex: 2}, Build: &registered,
	})
	request.Strong = false
	response := lookupBuildResult(t, state, request)
	if response.Outcome != routeapi.ReadReady || response.BuildState != clusterstate.BuildRegistered || response.BuildRevision != 3 {
		t.Fatalf("registered Build response = %+v", response)
	}
	if err := response.ValidateFor(request); err != nil {
		t.Fatal(err)
	}
	leases, err := lookupLeaseBindings(state, LeaseBindingLookup{Identity: identity, Limit: 10})
	if err != nil || len(leases.Bindings) != 1 || leases.Bindings[0].Group != "/g" ||
		leases.Bindings[0].NodeID != registered.Projection.NodeID ||
		leases.Bindings[0].AuthKeyFingerprint == "" || leases.Bindings[0].ManifestKeyFingerprint == "" {
		t.Fatalf("registered Build lease Binding = %+v, %v", leases, err)
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

func TestPendingLookupBoundsEncodedResponseBytes(t *testing.T) {
	registryLayout := testRegistryLayout(4, "generation-1")
	state, identity := initializedRouteShard(t, registryLayout, "/g", "rk-page-bytes")
	record := routeStarting(t, registryLayout, "/g", "rk-page-bytes", "sandbox-1", 1, false)
	record.Starting.Intent.NormalizedDemand = bytes.Repeat([]byte{'d'}, clusterstate.MaxNormalizedDemandBytes)
	record.Starting.Intent.DispatchSpec = bytes.Repeat([]byte{'s'}, clusterstate.MaxDispatchSpecBytes)
	state.Routes = make(map[string]clusterstate.RouteWorkflowRecord)
	for index := 0; index < 100; index++ {
		key := routeMapKey("/g", fmt.Sprintf("route-%03d", index))
		state.Routes[key] = record
	}
	result, err := lookupPending(state, PendingLookup{Identity: identity, Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	if result.NextKey == "" || len(result.Workflows) >= 100 || len(encoded) > MaxPendingLookupResponseBytes {
		t.Fatalf("bounded pending page: workflows=%d next=%q bytes=%d", len(result.Workflows), result.NextKey, len(encoded))
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
	secondReady := readyRecord(second, 1)
	applyDataOK(t, &state, 5, DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{LogIndex: 4}, Route: &secondReady,
	})
	secondPaused := pausedRecord(secondReady, 2)
	applyDataOK(t, &state, 6, DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{LogIndex: 5}, Route: &secondPaused,
	})
	otherGroup := "/other"
	otherKey := routeKeyForShard(t, registryLayout, otherGroup, identity.ShardID, "other")
	other := routeStarting(t, registryLayout, otherGroup, otherKey, "sandbox-3", 1, true)
	applyDataOK(t, &state, 7, DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{Absent: true}, Route: &other,
	})

	list := lookupRouteBucket(state, RouteBucketLookup{
		Identity: identity, Group: group, Bucket: bucket, Limit: 10,
	})
	if !list.Available || list.SnapshotRevision != 7 || len(list.Routes) != 2 ||
		list.Routes[0].RouteKey != firstKey || list.Routes[1].RouteKey != secondKey ||
		list.Routes[1].State != clusterstate.WorkflowRoutePaused {
		t.Fatalf("Route bucket snapshot = %+v", list)
	}
	paused := lookupRouteBucket(state, RouteBucketLookup{
		Identity: identity, Group: group, Bucket: bucket, State: clusterstate.WorkflowRoutePaused, Limit: 10,
	})
	if len(paused.Routes) != 1 || paused.Routes[0].RouteKey != secondKey {
		t.Fatalf("paused Route bucket snapshot = %+v", paused)
	}
	page := lookupRouteBucket(state, RouteBucketLookup{
		Identity: identity, Group: group, Bucket: bucket, Limit: 1,
	})
	if len(page.Routes) != 1 || page.NextRouteKey != firstKey {
		t.Fatalf("first Route bucket page = %+v", page)
	}
	page = lookupRouteBucket(state, RouteBucketLookup{
		Identity: identity, Group: group, Bucket: bucket, AfterRouteKey: page.NextRouteKey, Limit: 1,
	})
	if len(page.Routes) != 1 || page.Routes[0].RouteKey != secondKey || page.NextRouteKey != "" {
		t.Fatalf("second Route bucket page = %+v", page)
	}
	leasePage, err := lookupLeaseBindings(state, LeaseBindingLookup{Identity: identity, Limit: 2})
	if err != nil || len(leasePage.Bindings) != 2 || leasePage.NextKey == "" {
		t.Fatalf("first lease Binding page = %+v, %v", leasePage, err)
	}
	leasePage, err = lookupLeaseBindings(state, LeaseBindingLookup{
		Identity: identity, AfterKey: leasePage.NextKey, Limit: 2,
	})
	if err != nil || len(leasePage.Bindings) != 1 || leasePage.NextKey != "" {
		t.Fatalf("second lease Binding page = %+v, %v", leasePage, err)
	}

	changePage, err := lookupRouteChangefeed(state, RouteChangefeedLookup{
		Identity: identity, Group: group, Bucket: bucket, AfterRevision: 1, Limit: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !changePage.Available || changePage.Reset || len(changePage.Changes) != 2 || changePage.Changes[0].Revision != 2 ||
		changePage.Changes[0].RouteKey != firstKey || changePage.Changes[0].State != clusterstate.WorkflowRouteStarting ||
		changePage.Changes[1].Revision != 3 || changePage.Changes[1].State != clusterstate.WorkflowRouteReady ||
		changePage.CursorRevision != 3 || changePage.HeadRevision != 7 {
		t.Fatalf("first Route changefeed page = %+v", changePage)
	}
	changePage, err = lookupRouteChangefeed(state, RouteChangefeedLookup{
		Identity: identity, Group: group, Bucket: bucket, AfterRevision: changePage.CursorRevision, Limit: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !changePage.Available || len(changePage.Changes) != 2 || changePage.Changes[0].Revision != 4 ||
		changePage.Changes[1].Revision != 5 || changePage.Changes[1].State != clusterstate.WorkflowRouteReady ||
		changePage.CursorRevision != 5 {
		t.Fatalf("second Route changefeed page = %+v", changePage)
	}
	changePage, err = lookupRouteChangefeed(state, RouteChangefeedLookup{
		Identity: identity, Group: group, Bucket: bucket, AfterRevision: changePage.CursorRevision, Limit: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !changePage.Available || len(changePage.Changes) != 1 || changePage.Changes[0].Revision != 6 ||
		changePage.Changes[0].State != clusterstate.WorkflowRoutePaused || changePage.CursorRevision != 7 {
		t.Fatalf("third Route changefeed page = %+v", changePage)
	}

	advanceDataApplied(&state, RouteChangefeedRetentionRevisions+10)
	reset, err := lookupRouteChangefeed(state, RouteChangefeedLookup{
		Identity: identity, Group: group, Bucket: bucket, AfterRevision: 1, Limit: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reset.Available || !reset.Reset || reset.FloorRevision != 10 || len(reset.Changes) != 0 {
		t.Fatalf("compacted Route changefeed = %+v", reset)
	}
}

func TestRouteChangefeedBoundsEncodedResponseWithoutSkippingCursor(t *testing.T) {
	identity := ShardRequestIdentity{PermitIdentity: PermitIdentity{
		ClusterID: "cluster-1", RegistryGeneration: "generation-1", SystemEpoch: 1,
		RegistryLayoutDigest: digestFor("registry-layout"),
	}}
	largeKey := strings.Repeat("k", 3<<20)
	state := DataState{
		Initialized: true, ClusterID: identity.ClusterID, RegistryGeneration: identity.RegistryGeneration,
		ShardID: 0, RouteBucketCount: 1, VirtualShardCount: 1,
		ServingEpochs: []PermitIdentity{identity.PermitIdentity}, LastApplied: 3,
		RouteChanges: []RouteChange{
			{Revision: 1, Group: "/g", RouteKey: largeKey + "1", State: clusterstate.WorkflowRouteStarting},
			{Revision: 2, Group: "/g", RouteKey: largeKey + "2", State: clusterstate.WorkflowRouteReady},
			{Revision: 3, Group: "/g", RouteKey: largeKey + "3", State: clusterstate.WorkflowRoutePaused},
		},
	}
	page, err := lookupRouteChangefeed(state, RouteChangefeedLookup{
		Identity: identity, Group: "/g", Bucket: 0, Limit: 4096,
	})
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(page)
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) > MaxRouteChangefeedResponseBytes || len(page.Changes) != 2 || page.CursorRevision != 2 {
		t.Fatalf("bounded Route changefeed bytes=%d changes=%d cursor=%d", len(encoded), len(page.Changes), page.CursorRevision)
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
