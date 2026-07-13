package registry

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/cluster/shardkv"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

func TestClusterStoresRouteAndNodeUseLocatedOwners(t *testing.T) {
	ctx := context.Background()
	view := clusterstate.MemberView{Version: 1, Members: []string{"a", "b", "c"}}
	cluster := newShardStoreCluster(t, []string{"a", "b", "c"}, 2, 2, 1, 1)
	stores := cluster["a"]

	group, routeKey := "/cluster/located/group", "rk"
	if _, err := stores.PutSandbox(ctx, &SandboxRecord{Group: group, RouteKey: routeKey, SID: "sb", State: StateReady}); err != nil {
		t.Fatalf("PutSandbox: %v", err)
	}
	routeOwners, err := view.Owners(group, 2)
	if err != nil {
		t.Fatal(err)
	}
	assertShardRecordOwners(t, ctx, cluster, shardkv.Namespace(clusterstate.NamespaceRouteLink), clusterstate.RouteLinkShard(group), clusterstate.RecordSetRouteSandbox, clusterstate.RouteSandboxRecordKey(routeKey), routeOwners)

	nodeID := "node-located"
	if err := stores.PutNode(ctx, &NodeRecord{NodeID: nodeID}); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	nodeOwners, err := view.Owners(nodeID, 2)
	if err != nil {
		t.Fatal(err)
	}
	assertShardRecordOwners(t, ctx, cluster, shardkv.Namespace(clusterstate.NamespaceNodeLink), clusterstate.NodeLinkShard(nodeID), clusterstate.RecordSetNodeProfile, clusterstate.NodeLinkProfileRecord, nodeOwners)
}

func TestStoresBuildShardKVNamespaces(t *testing.T) {
	view := clusterstate.MemberView{Version: 1, Members: []string{"r1", "r2", "r3"}}

	stores := NewClusterStores("r1", view, 2, 3, 1, 2)
	stores.SetNodeListTopology([]clusterstate.MemberView{view}, 1)
	stores.SetPlacerLinkTopology([]clusterstate.MemberView{view}, 2)

	shards := stores.ShardStore()
	if shards == nil {
		t.Fatal("ShardStore is nil")
	}
	cases := []struct {
		ns    shardkv.Namespace
		shard shardkv.ShardKey
		want  int
	}{
		{shardkv.Namespace(clusterstate.NamespaceRouteLink), clusterstate.RouteLinkShard("/g"), 2},
		{shardkv.Namespace(clusterstate.NamespaceNodeLink), clusterstate.NodeLinkShard("n1"), 3},
		{shardkv.Namespace(clusterstate.NamespaceNodeList), clusterstate.NodeListShard, 1},
		{shardkv.Namespace(clusterstate.NamespacePlacerLink), clusterstate.PlacerImportSourceShard("source-a"), 2},
	}
	for _, tc := range cases {
		sh, err := shards.Shard(tc.ns, tc.shard)
		if err != nil {
			t.Fatalf("Shard(%s,%s): %v", tc.ns, tc.shard, err)
		}
		got, err := sh.View(context.Background())
		if err != nil {
			t.Fatalf("View(%s,%s): %v", tc.ns, tc.shard, err)
		}
		if len(got.WriteSets) != 1 || len(got.WriteSets[0].Members) != tc.want {
			t.Fatalf("View(%s,%s)=%+v, want %d members", tc.ns, tc.shard, got, tc.want)
		}
	}
}

func TestNodeLinkShardRecordsAssembleNodeView(t *testing.T) {
	ctx := context.Background()
	stores := NewStores()
	node := &NodeRecord{
		NodeID: "n1", Labels: map[string]string{"pool": "p"}, Capacity: 10,
		DataEndpoint: "10.0.0.1:8443", LastHeartbeatUnix: time.Now().Unix(), LinkOwner: "r1",
	}
	if err := stores.putNodeProfileShard(ctx, node); err != nil {
		t.Fatalf("putNodeProfileShard: %v", err)
	}
	if err := stores.addNodeSandboxRefShard(ctx, "n1", clusterstate.NodeSandboxRef{Group: "/g", RouteKey: "rk", SandboxID: "sb"}); err != nil {
		t.Fatalf("addNodeSandboxRefShard: %v", err)
	}
	if err := stores.addNodeBuildRefShard(ctx, "n1", clusterstate.NodeBuildRef{Group: "/g", BuildID: "b1"}); err != nil {
		t.Fatalf("addNodeBuildRefShard: %v", err)
	}
	if err := stores.upsertNodeManifestKeyShard(ctx, "n1", clusterstate.NodeManifestKey{Fingerprint: "fp", Type: clusterstate.SecretInline, Value: "mk", ExpiresUnix: 123}); err != nil {
		t.Fatalf("upsertNodeManifestKeyShard: %v", err)
	}
	got, found, err := stores.getNodeShard(ctx, "n1")
	if err != nil || !found {
		t.Fatalf("getNodeShard found=%v err=%v", found, err)
	}
	if got.NodeID != "n1" || got.Labels["pool"] != "p" || len(got.Sandboxes) != 1 || len(got.Builds) != 1 || len(got.ManifestKeys) != 1 {
		t.Fatalf("node view=%+v", got)
	}
	if err := stores.dropNodeManifestKeyShard(ctx, "n1", "fp"); err != nil {
		t.Fatalf("dropNodeManifestKeyShard: %v", err)
	}
	got, found, err = stores.getNodeShard(ctx, "n1")
	if err != nil || !found {
		t.Fatalf("getNodeShard after drop found=%v err=%v", found, err)
	}
	if len(got.ManifestKeys) != 0 {
		t.Fatalf("manifest keys after tombstone=%+v", got.ManifestKeys)
	}
}

func TestNodeObjectRefsAreScopedByNodeID(t *testing.T) {
	ctx := context.Background()
	stores := NewStores()
	if err := stores.AddNodeSandboxRef(ctx, "n1", clusterstate.NodeSandboxRef{SandboxID: "same", Group: "/g1", RouteKey: "rk1"}); err != nil {
		t.Fatal(err)
	}
	if err := stores.AddNodeSandboxRef(ctx, "n2", clusterstate.NodeSandboxRef{SandboxID: "same", Group: "/g2", RouteKey: "rk2"}); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		node, group string
	}{
		{node: "n1", group: "/g1"},
		{node: "n2", group: "/g2"},
	} {
		ref, found, err := stores.GetNodeSandboxRef(ctx, tc.node, "same")
		if err != nil || !found || ref.Group != tc.group {
			t.Fatalf("node %s ref=%+v found=%v err=%v", tc.node, ref, found, err)
		}
	}
	if err := stores.AddNodeSandboxRef(ctx, "n1", clusterstate.NodeSandboxRef{Group: "/g"}); err == nil {
		t.Fatal("invalid sandbox ref was accepted")
	}
	if err := stores.AddNodeBuildRef(ctx, "n1", clusterstate.NodeBuildRef{BuildID: "b"}); err == nil {
		t.Fatal("invalid build ref was accepted")
	}
}

func TestPutNodeRuntimeUpdatesProfileOnly(t *testing.T) {
	ctx := context.Background()
	stores := NewStores()
	node := &NodeRecord{
		NodeID: "n-runtime", Labels: map[string]string{"pool": "p"}, Capacity: 10,
		DataEndpoint: "10.0.0.1:8443", LastHeartbeatUnix: time.Now().Unix(), LinkOwner: "r1",
		Sandboxes: []clusterstate.NodeSandboxRef{{Group: "/g", RouteKey: "rk", SandboxID: "sb"}},
		Builds:    []clusterstate.NodeBuildRef{{Group: "/g", BuildID: "b1"}},
		ManifestKeys: []clusterstate.NodeManifestKey{{
			Fingerprint: "fp", Type: clusterstate.SecretInline, Value: "mk", ExpiresUnix: 123,
		}},
	}
	if err := stores.PutNode(ctx, node); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	profileBefore := mustNodeRecordSetSnapshot(t, ctx, stores, "n-runtime", clusterstate.RecordSetNodeProfile).Rev
	sandboxBefore := mustNodeRecordSetSnapshot(t, ctx, stores, "n-runtime", clusterstate.RecordSetNodeSandbox).Rev
	buildBefore := mustNodeRecordSetSnapshot(t, ctx, stores, "n-runtime", clusterstate.RecordSetNodeBuild).Rev
	keyBefore := mustNodeRecordSetSnapshot(t, ctx, stores, "n-runtime", clusterstate.RecordSetNodeManifestKey).Rev

	node.Counts = 7
	node.LastHeartbeatUnix++
	if err := stores.PutNodeRuntime(ctx, node); err != nil {
		t.Fatalf("PutNodeRuntime: %v", err)
	}
	profileAfter := mustNodeRecordSetSnapshot(t, ctx, stores, "n-runtime", clusterstate.RecordSetNodeProfile).Rev
	sandboxAfter := mustNodeRecordSetSnapshot(t, ctx, stores, "n-runtime", clusterstate.RecordSetNodeSandbox).Rev
	buildAfter := mustNodeRecordSetSnapshot(t, ctx, stores, "n-runtime", clusterstate.RecordSetNodeBuild).Rev
	keyAfter := mustNodeRecordSetSnapshot(t, ctx, stores, "n-runtime", clusterstate.RecordSetNodeManifestKey).Rev

	if profileAfter <= profileBefore {
		t.Fatalf("profile rev did not advance: before=%d after=%d", profileBefore, profileAfter)
	}
	if sandboxAfter != sandboxBefore || buildAfter != buildBefore || keyAfter != keyBefore {
		t.Fatalf("runtime update rewrote side recordSets: sandbox %d->%d build %d->%d key %d->%d",
			sandboxBefore, sandboxAfter, buildBefore, buildAfter, keyBefore, keyAfter)
	}
}

func TestRouteLinkShardSeparatesSandboxesAndBuilds(t *testing.T) {
	ctx := context.Background()
	stores := NewStores()
	if _, err := stores.putRouteSandboxShard(ctx, &SandboxRecord{Group: "/g", RouteKey: "rk", SID: "sb", State: StateReady}); err != nil {
		t.Fatalf("putRouteSandboxShard: %v", err)
	}
	if _, err := stores.putRouteBuildShard(ctx, &BuildRecord{Group: "/g", BuildID: "b1", NodeID: "n1", State: BuildRegistered}); err != nil {
		t.Fatalf("putRouteBuildShard: %v", err)
	}
	route, _, found, err := stores.getRouteSandboxShard(ctx, "/g", "rk")
	if err != nil || !found || route.SID != "sb" {
		t.Fatalf("getRouteSandboxShard route=%+v found=%v err=%v", route, found, err)
	}
	build, _, found, err := stores.getRouteBuildShard(ctx, "/g", "b1")
	if err != nil || !found || build.BuildID != "b1" {
		t.Fatalf("getRouteBuildShard build=%+v found=%v err=%v", build, found, err)
	}
	var routes []string
	if err := stores.rangeRouteSandboxesShard(ctx, "/g", func(r *SandboxRecord) error {
		routes = append(routes, r.RouteKey)
		return nil
	}); err != nil {
		t.Fatalf("rangeRouteSandboxesShard: %v", err)
	}
	if len(routes) != 1 || routes[0] != "rk" {
		t.Fatalf("routes=%v, want [rk]", routes)
	}
	var builds []string
	if err := stores.rangeRouteBuildsShard(ctx, "/g", func(b *BuildRecord) error {
		builds = append(builds, b.BuildID)
		return nil
	}); err != nil {
		t.Fatalf("rangeRouteBuildsShard: %v", err)
	}
	if len(builds) != 1 || builds[0] != "b1" {
		t.Fatalf("builds=%v, want [b1]", builds)
	}
}

func TestNodeListShardFixedShard(t *testing.T) {
	ctx := context.Background()
	stores := NewStores()
	if err := stores.putNodeListEntryShard(ctx, clusterstate.NodeListEntry{NodeID: "n2", Labels: map[string]string{"pool": "p2"}}); err != nil {
		t.Fatalf("putNodeListEntryShard n2: %v", err)
	}
	if err := stores.putNodeListEntryShard(ctx, clusterstate.NodeListEntry{NodeID: "n1", Labels: map[string]string{"pool": "p1"}}); err != nil {
		t.Fatalf("putNodeListEntryShard n1: %v", err)
	}
	var ids []string
	if err := stores.rangeNodeListShard(ctx, func(entry clusterstate.NodeListEntry) error {
		ids = append(ids, entry.NodeID)
		return nil
	}); err != nil {
		t.Fatalf("rangeNodeListShard: %v", err)
	}
	if len(ids) != 2 || ids[0] != "n1" || ids[1] != "n2" {
		t.Fatalf("ids=%v, want sorted n1,n2", ids)
	}
	if err := stores.deleteNodeListEntryShard(ctx, "n1"); err != nil {
		t.Fatalf("deleteNodeListEntryShard: %v", err)
	}
	ids = nil
	if err := stores.rangeNodeListShard(ctx, func(entry clusterstate.NodeListEntry) error {
		ids = append(ids, entry.NodeID)
		return nil
	}); err != nil {
		t.Fatalf("rangeNodeListShard after delete: %v", err)
	}
	if len(ids) != 1 || ids[0] != "n2" {
		t.Fatalf("ids after delete=%v, want n2", ids)
	}
}

func TestPlacerLinkImportSourceShard(t *testing.T) {
	ctx := context.Background()
	stores := NewStores()
	state, acquired, err := stores.acquirePlacerImportSourceShard(ctx, "source-a", "s1", "run-1", time.Minute)
	if err != nil || !acquired {
		t.Fatalf("acquire state=%+v acquired=%v err=%v", state, acquired, err)
	}
	if state.SourceID != "source-a" || state.OwnerID != "s1" || state.Term == 0 {
		t.Fatalf("unexpected state=%+v", state)
	}
	if ok := stores.checkPlacerImportSourceShard(ctx, "source-a", "s1", "run-1", state.Term); !ok {
		t.Fatal("lease check failed")
	}
	held, acquired, err := stores.acquirePlacerImportSourceShard(ctx, "source-a", "s2", "run-2", time.Minute)
	if err != nil || acquired || held.OwnerID != "s1" {
		t.Fatalf("second acquire held=%+v acquired=%v err=%v", held, acquired, err)
	}
	next, err := stores.checkpointPlacerImportSourceShard(ctx, "source-a", "s1", "run-1", state.Term, "cursor-1", false, "")
	if err != nil || next.Cursor != "cursor-1" {
		t.Fatalf("checkpoint next=%+v err=%v", next, err)
	}
}

func TestClusterStoresPlacerLinkSourceLeaseUsesLocatedOwners(t *testing.T) {
	ctx := context.Background()
	view := clusterstate.MemberView{Version: 1, Members: []string{"a", "b", "c"}}
	cluster := newShardStoreCluster(t, []string{"a", "b", "c"}, 2, 2, 2, 1)
	stores := cluster["a"]

	sourceID := "source-a"
	rec, acquired, err := stores.AcquirePlacerLinkSourceLease(ctx, sourceID, "s1", "run-1", time.Second)
	if err != nil {
		t.Fatalf("AcquirePlacerLinkSourceLease: %v", err)
	}
	if !acquired || rec.SourceID != sourceID || rec.OwnerID != "s1" || rec.RunID != "run-1" || rec.Term == 0 {
		t.Fatalf("unexpected lease acquired=%v rec=%+v", acquired, rec)
	}

	shard := clusterstate.PlacerImportSourceShard(sourceID)
	owners, err := view.Owners(string(shard), 2)
	if err != nil {
		t.Fatal(err)
	}
	assertPlacerImportShardOwners(t, ctx, cluster, owners, sourceID, "s1")

	held, acquired, err := stores.AcquirePlacerLinkSourceLease(ctx, sourceID, "s2", "run-2", time.Second)
	if err != nil {
		t.Fatalf("second AcquirePlacerLinkSourceLease: %v", err)
	}
	if acquired || held.OwnerID != "s1" || held.RunID != "run-1" {
		t.Fatalf("live lease was not fenced: acquired=%v held=%+v", acquired, held)
	}
}

func TestClusterStoresPlacerLinkSourceCursorUsesLeaseFencing(t *testing.T) {
	ctx := context.Background()
	stores := NewStores()

	rec, acquired, err := stores.AcquirePlacerLinkSourceLease(ctx, "source-a", "s1", "run-1", time.Second)
	if err != nil || !acquired {
		t.Fatalf("AcquirePlacerLinkSourceLease acquired=%v err=%v", acquired, err)
	}
	if _, err := stores.CheckpointPlacerLinkSource(ctx, "source-a", "s2", "run-2", rec.Term, "next", false, ""); !errors.Is(err, errPlacerLinkStaleLease) {
		t.Fatalf("stale checkpoint err=%v, want errPlacerLinkStaleLease", err)
	}
	next, err := stores.CheckpointPlacerLinkSource(ctx, "source-a", "s1", "run-1", rec.Term, "next", false, "")
	if err != nil {
		t.Fatalf("CheckpointPlacerLinkSource: %v", err)
	}
	if next.Cursor != "next" || next.Round != 0 {
		t.Fatalf("checkpoint did not preserve cursor/round: %+v", next)
	}
	done, err := stores.CheckpointPlacerLinkSource(ctx, "source-a", "s1", "run-1", rec.Term, "", true, "")
	if err != nil {
		t.Fatalf("complete CheckpointPlacerLinkSource: %v", err)
	}
	if done.Cursor != "" || done.Round != 1 {
		t.Fatalf("complete checkpoint did not advance round: %+v", done)
	}
}

func TestClusterStoresJointMembershipWritesBothOwnerSets(t *testing.T) {
	ctx := context.Background()
	active := clusterstate.MemberView{Version: 1, Members: []string{"a", "b", "c"}}
	next := clusterstate.MemberView{Version: 2, Members: []string{"b", "c", "d"}}
	cluster := newShardStoreClusterWithViews(t, []clusterstate.MemberView{active, next}, 2, 2, 1, 1)
	stores := cluster["a"]

	group, routeKey := "/cluster/joint/group", "rk"
	if _, err := stores.PutSandbox(ctx, &SandboxRecord{Group: group, RouteKey: routeKey, SID: "sb", State: StateReady}); err != nil {
		t.Fatalf("PutSandbox: %v", err)
	}
	for _, view := range []clusterstate.MemberView{active, next} {
		owners, err := view.Owners(group, 2)
		if err != nil {
			t.Fatal(err)
		}
		assertShardRecordPresent(t, ctx, cluster, shardkv.Namespace(clusterstate.NamespaceRouteLink), clusterstate.RouteLinkShard(group), clusterstate.RecordSetRouteSandbox, clusterstate.RouteSandboxRecordKey(routeKey), owners)
	}

	nodeID := "node-joint"
	if err := stores.PutNode(ctx, &NodeRecord{NodeID: nodeID}); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	for _, view := range []clusterstate.MemberView{active, next} {
		owners, err := view.Owners(nodeID, 2)
		if err != nil {
			t.Fatal(err)
		}
		assertShardRecordPresent(t, ctx, cluster, shardkv.Namespace(clusterstate.NamespaceNodeLink), clusterstate.NodeLinkShard(nodeID), clusterstate.RecordSetNodeProfile, clusterstate.NodeLinkProfileRecord, owners)
	}
}

func TestClusterStoresCutoverReadsColdOldGraceRoute(t *testing.T) {
	ctx := context.Background()
	oldView := clusterstate.MemberView{Version: 1, Label: "v1", Members: []string{"a", "b", "c"}}
	newView := clusterstate.MemberView{Version: 2, Label: "v2", Members: []string{"c", "d", "e"}}
	cluster := newShardStoreClusterWithViews(t, []clusterstate.MemberView{oldView}, 3, 3, 3, 3)
	group, routeKey := "/cluster/cold-cutover/group", "rk"
	if _, err := cluster["a"].PutSandbox(ctx, &SandboxRecord{Group: group, RouteKey: routeKey, SID: "sb-old", State: StateReady}); err != nil {
		t.Fatalf("old PutSandbox: %v", err)
	}

	cutoverViews := []clusterstate.MemberView{
		{Version: oldView.Version, Label: oldView.Label, Members: oldView.Members, ReadOnly: true},
		newView,
	}
	for _, id := range []string{"d", "e"} {
		cluster[id] = NewClusterStoresWithViews(id, cutoverViews, 3, 3, 3, 3)
	}
	transport := shardkv.TransportFunc(func(ctx context.Context, member shardkv.MemberID, req shardkv.Request) (shardkv.Response, error) {
		store := cluster[string(member)]
		if store == nil || store.ShardStore() == nil {
			return shardkv.Response{}, shardkv.ErrReplicaUnavailable
		}
		return store.ShardStore().Handle(ctx, req)
	})
	for _, store := range cluster {
		store.SetMemberViews(cutoverViews)
		store.SetShardTransport(transport, nil)
	}

	got, _, found, err := cluster["d"].GetSandbox(ctx, group, routeKey)
	if err != nil || !found || got.SID != "sb-old" {
		t.Fatalf("cold old_grace GetSandbox got=%+v found=%v err=%v", got, found, err)
	}
	newOwners, err := newView.Owners(group, 3)
	if err != nil {
		t.Fatal(err)
	}
	assertShardRecordPresent(t, ctx, cluster, shardkv.Namespace(clusterstate.NamespaceRouteLink), clusterstate.RouteLinkShard(group), clusterstate.RecordSetRouteSandbox, clusterstate.RouteSandboxRecordKey(routeKey), newOwners)
}

func assertShardRecordPresent(t *testing.T, ctx context.Context, stores map[string]*Stores, ns shardkv.Namespace, shard shardkv.ShardKey, recordSet shardkv.RecordSetName, key shardkv.RecordKey, owners []string) {
	t.Helper()
	for _, id := range owners {
		store := stores[id]
		if store == nil {
			t.Fatalf("owner %s missing store", id)
		}
		has := localShardHasRecord(t, ctx, store, ns, shard, recordSet, key)
		if !has {
			t.Fatalf("owner %s missing %s/%s/%s key %q; owners=%v", id, ns, shard, recordSet, key, owners)
		}
	}
}

func assertShardRecordOwners(t *testing.T, ctx context.Context, stores map[string]*Stores, ns shardkv.Namespace, shard shardkv.ShardKey, recordSet shardkv.RecordSetName, key shardkv.RecordKey, owners []string) {
	t.Helper()
	ownerSet := map[string]bool{}
	for _, owner := range owners {
		ownerSet[owner] = true
	}
	for id, store := range stores {
		has := localShardHasRecord(t, ctx, store, ns, shard, recordSet, key)
		if ownerSet[id] && !has {
			t.Fatalf("owner %s missing %s/%s/%s key %q; owners=%v", id, ns, shard, recordSet, key, owners)
		}
		if !ownerSet[id] && has {
			t.Fatalf("non-owner %s unexpectedly has %s/%s/%s key %q; owners=%v", id, ns, shard, recordSet, key, owners)
		}
	}
}

func localShardHasRecord(t *testing.T, ctx context.Context, store *Stores, ns shardkv.Namespace, shard shardkv.ShardKey, recordSet shardkv.RecordSetName, key shardkv.RecordKey) bool {
	t.Helper()
	resp, err := store.ShardStore().Handle(ctx, shardkv.Request{Op: shardkv.OpSnapshot, Namespace: ns, Shard: shard, RecordSet: recordSet})
	if err != nil {
		if errors.Is(err, shardkv.ErrInvalidView) {
			return false
		}
		t.Fatalf("snapshot %s/%s/%s: %v", ns, shard, recordSet, err)
	}
	for _, rec := range resp.Records {
		if rec.Key == key && !rec.Deleted {
			return true
		}
	}
	return false
}

func mustNodeRecordSetSnapshot(t *testing.T, ctx context.Context, stores *Stores, nodeID string, recordSet shardkv.RecordSetName) shardkv.Snapshot {
	t.Helper()
	sh, err := stores.nodeLinkRecordSet(nodeID, recordSet)
	if err != nil {
		t.Fatalf("node recordSet %s: %v", recordSet, err)
	}
	snap, err := sh.Snapshot(ctx)
	if err != nil {
		t.Fatalf("snapshot node %s recordSet %s: %v", nodeID, recordSet, err)
	}
	return snap
}

func TestRoutingNodeOwnerUsesLinkOwner(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	if err := reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1", LinkOwner: "remote"}); err != nil {
		t.Fatal(err)
	}
	remote := &routingNodeOwnerRecorder{allow: true}
	reg.SetRemoteNodeOwners(map[string]NodeOwner{"remote": remote})

	if err := reg.nodeOwner.PutManifestKey(ctx, "n1", "fp", "inline", "key", 123); err != nil {
		t.Fatalf("PutManifestKey: %v", err)
	}
	if !reg.nodeOwner.AdmitBuild(ctx, "n1", "b1", &routesync.BuildResources{CPU: 1}) {
		t.Fatal("AdmitBuild returned false")
	}
	if err := reg.nodeOwner.DeleteSandbox(ctx, "n1", "sb1"); err != nil {
		t.Fatalf("DeleteSandbox: %v", err)
	}
	reg.nodeOwner.ReleaseBuild(ctx, "n1", "b1")

	if len(remote.keys) != 1 || remote.keys[0] != "n1/fp" {
		t.Fatalf("remote keys=%v", remote.keys)
	}
	if len(remote.admitted) != 1 || remote.admitted[0] != "n1/b1" {
		t.Fatalf("remote admitted=%v", remote.admitted)
	}
	if len(remote.deleted) != 1 || remote.deleted[0] != "n1/sb1" {
		t.Fatalf("remote deleted=%v", remote.deleted)
	}
	if len(remote.released) != 1 || remote.released[0] != "n1/b1" {
		t.Fatalf("remote released=%v", remote.released)
	}
}

func TestRoutingNodeOwnerReleasesBuildFromEveryPossibleAdmittingOwner(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	if err := reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1", LinkOwner: "new-owner"}); err != nil {
		t.Fatal(err)
	}
	oldOwner := &routingNodeOwnerRecorder{}
	newOwner := &routingNodeOwnerRecorder{}
	reg.SetRemoteNodeOwners(map[string]NodeOwner{
		"old-owner": oldOwner,
		"new-owner": newOwner,
	})

	reg.nodeOwner.ReleaseBuild(ctx, "n1", "b1")
	for name, owner := range map[string]*routingNodeOwnerRecorder{
		"old": oldOwner,
		"new": newOwner,
	} {
		if len(owner.released) != 1 || owner.released[0] != "n1/b1" {
			t.Fatalf("%s owner releases=%v, want n1/b1", name, owner.released)
		}
	}
}

func TestNodeOwnerUsesProfileReadWhenLocalIsNotNodeShardOwner(t *testing.T) {
	ctx := context.Background()
	view := clusterstate.MemberView{Version: 1, Members: []string{"a", "b", "c"}}
	cluster := newShardStoreCluster(t, view.Members, 1, 1, 1, 1)
	nodeID := nodeNotOwnedBy(t, view, "a")
	if err := cluster["b"].PutNode(ctx, &NodeRecord{
		NodeID: nodeID, LinkOwner: "remote", Capacity: 10,
		BuildCapacity: &routesync.BuildResources{CPU: 1000},
		DataEndpoint:  "127.0.0.1:12345",
	}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	if _, _, err := cluster["a"].GetNode(ctx, nodeID); !errors.Is(err, shardkv.ErrInvalidView) {
		t.Fatalf("non-owner full GetNode err=%v, want ErrInvalidView", err)
	}
	profile, found, err := cluster["a"].GetNodeProfile(ctx, nodeID)
	if err != nil || !found || profile.DataEndpoint != "127.0.0.1:12345" || profile.LinkOwner != "remote" {
		t.Fatalf("profile=%+v found=%v err=%v", profile, found, err)
	}

	reg := New(cluster["a"], nil, time.Second, nil)
	reg.addNode(&fakeConn{nodeID: nodeID})
	if node, found, err := reg.localNodeOwner.Runtime(ctx, nodeID); err != nil || !found || node.DataEndpoint != "127.0.0.1:12345" {
		t.Fatalf("local runtime profile=%+v found=%v err=%v", node, found, err)
	}
	remote := &routingNodeOwnerRecorder{allow: true}
	reg.SetRemoteNodeOwners(map[string]NodeOwner{"remote": remote})
	if !reg.nodeOwner.AdmitBuild(ctx, nodeID, "b1", &routesync.BuildResources{CPU: 1}) {
		t.Fatal("routed AdmitBuild returned false")
	}
	if len(remote.admitted) != 1 || remote.admitted[0] != nodeID+"/b1" {
		t.Fatalf("remote admitted=%v", remote.admitted)
	}
}

func TestRoutingNodeOwnerProbesShardOwnerWhenProfileReadFails(t *testing.T) {
	ctx := context.Background()
	view := clusterstate.MemberView{Version: 1, Members: []string{"a", "b", "c"}}
	cluster := newShardStoreCluster(t, view.Members, 1, 1, 1, 1)
	nodeID := nodeNotOwnedBy(t, view, "a")
	owners, err := view.Owners(nodeID, 1)
	if err != nil {
		t.Fatal(err)
	}
	remote := &routingNodeOwnerRecorder{allow: true}
	reg := New(cluster["a"], nil, time.Second, nil)
	reg.SetRemoteNodeOwners(map[string]NodeOwner{owners[0]: remote})
	delete(cluster, owners[0])
	if _, _, err := reg.stores.GetNodeProfile(ctx, nodeID); err == nil {
		t.Fatal("profile read unexpectedly succeeded without its shard owner")
	}
	if err := reg.nodeOwner.Connected(ctx, nodeID); err != nil {
		t.Fatalf("Connected: %v", err)
	}
	if len(remote.connected) != 1 || remote.connected[0] != nodeID {
		t.Fatalf("remote connected probes=%v, want %s", remote.connected, nodeID)
	}

	if err := reg.nodeOwner.SendCommand(ctx, nodeID, &routesync.Command{Kind: routesync.CmdDelete, SID: "sb1"}); err != nil {
		t.Fatalf("SendCommand: %v", err)
	}
	if len(remote.deleted) != 1 || remote.deleted[0] != nodeID+"/"+routesync.CmdDelete {
		t.Fatalf("remote deleted=%v", remote.deleted)
	}
}

func TestRoutingNodeOwnerUsesConnectedShardOwnerWhenLocalIsCandidate(t *testing.T) {
	ctx := context.Background()
	cluster := newShardStoreCluster(t, []string{"a", "b", "c"}, 1, 2, 1, 1)

	var nodeID, remoteID string
	for i := 0; i < 1000; i++ {
		candidate := fmt.Sprintf("node-local-candidate-%d", i)
		owners, err := cluster["a"].NodeOwnerCandidates(ctx, candidate)
		if err != nil {
			t.Fatal(err)
		}
		localCandidate := false
		for _, owner := range owners {
			if owner == "a" {
				localCandidate = true
			} else if owner != "" {
				remoteID = owner
			}
		}
		if localCandidate && remoteID != "" {
			nodeID = candidate
			break
		}
		remoteID = ""
	}
	if nodeID == "" {
		t.Fatal("could not find node with local and remote shard owners")
	}

	remote := &routingNodeOwnerRecorder{allow: true}
	reg := New(cluster["a"], nil, time.Second, nil)
	reg.SetRemoteNodeOwners(map[string]NodeOwner{remoteID: remote})
	if err := reg.nodeOwner.Connected(ctx, nodeID); err != nil {
		t.Fatalf("Connected: %v", err)
	}
	if err := reg.nodeOwner.SendCommand(ctx, nodeID, &routesync.Command{Kind: routesync.CmdDelete, SID: "sb1"}); err != nil {
		t.Fatalf("SendCommand: %v", err)
	}
	if len(remote.deleted) != 1 || remote.deleted[0] != nodeID+"/"+routesync.CmdDelete {
		t.Fatalf("remote deleted=%v", remote.deleted)
	}
}

func TestNodeRegisterUsesProfileReadWhenLocalIsNotNodeShardOwner(t *testing.T) {
	ctx := context.Background()
	view := clusterstate.MemberView{Version: 1, Members: []string{"a", "b", "c"}}
	cluster := newShardStoreCluster(t, view.Members, 1, 1, 1, 1)
	nodeID := nodeNotOwnedBy(t, view, "a")
	if err := cluster["b"].PutNode(ctx, &NodeRecord{NodeID: nodeID, LinkOwner: "b", DataEndpoint: "old"}); err != nil {
		t.Fatalf("seed node: %v", err)
	}
	reg := New(cluster["a"], nil, time.Second, nil)
	if _, err := reg.updateNodeRegister(ctx, &routesync.NodeRegister{
		NodeID: nodeID, Capacity: 10, DataEndpoint: "new", Labels: map[string]string{"pool": "p"},
	}); err != nil {
		t.Fatalf("non-owner node register: %v", err)
	}
	profile, found, err := cluster["b"].GetNodeProfile(ctx, nodeID)
	if err != nil || !found || profile.LinkOwner != "a" || profile.DataEndpoint != "new" {
		t.Fatalf("profile after register=%+v found=%v err=%v", profile, found, err)
	}
}

func TestNodeRegisterProjectsOnlyAfterConnectionIsLive(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	registered, err := reg.updateNodeRegister(ctx, &routesync.NodeRegister{
		NodeID: "n1", Capacity: 10, DataEndpoint: "127.0.0.1:19001",
	})
	if err != nil {
		t.Fatal(err)
	}
	reg.projectRegisteredNode(ctx, registered)
	found := false
	if err := reg.stores.RangeNodeList(ctx, func(entry clusterstate.NodeListEntry) error {
		found = found || entry.NodeID == "n1"
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("registering node was projected before its node-link became live")
	}

	reg.addNode(&fakeConn{nodeID: "n1"})
	reg.projectRegisteredNode(ctx, registered)
	if err := reg.stores.RangeNodeList(ctx, func(entry clusterstate.NodeListEntry) error {
		found = found || entry.NodeID == "n1"
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("live registered node was not projected")
	}
}

func TestRegisteredNodeProjectionDoesNotReReadProfile(t *testing.T) {
	ctx := context.Background()
	view := clusterstate.MemberView{Version: 1, Members: []string{"a", "b", "c"}}
	nodeListOwners, err := view.Owners(string(clusterstate.NodeListShard), 1)
	if err != nil {
		t.Fatal(err)
	}
	var nodeID, profileOwner string
	for i := 0; i < 1000; i++ {
		candidate := fmt.Sprintf("projection-node-%d", i)
		owners, err := view.Owners(candidate, 1)
		if err != nil {
			t.Fatal(err)
		}
		if owners[0] != "a" && owners[0] != nodeListOwners[0] {
			nodeID, profileOwner = candidate, owners[0]
			break
		}
	}
	if nodeID == "" {
		t.Fatal("failed to choose disjoint node profile and node_list owners")
	}
	cluster := newShardStoreCluster(t, view.Members, 1, 1, 1, 1)
	reg := New(cluster["a"], nil, time.Second, nil)
	registered, err := reg.updateNodeRegister(ctx, &routesync.NodeRegister{
		NodeID: nodeID, Capacity: 10, DataEndpoint: "127.0.0.1:19001",
	})
	if err != nil {
		t.Fatal(err)
	}
	reg.addNode(&fakeConn{nodeID: nodeID})
	delete(cluster, profileOwner)
	if _, _, err := reg.stores.GetNodeProfile(ctx, nodeID); err == nil {
		t.Fatal("profile read unexpectedly succeeded without its shard owner")
	}

	reg.projectRegisteredNode(ctx, registered)
	found := false
	if err := reg.stores.RangeNodeList(ctx, func(entry clusterstate.NodeListEntry) error {
		found = found || entry.NodeID == nodeID
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("committed registration was not projected after profile read became unavailable")
	}
}

func TestNodeRegisterDoesNotFailWhenNodeListProjectionUnavailable(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	view := clusterstate.MemberView{Version: 1, Members: []string{"a", "b"}}
	stores := NewClusterStores("a", view, 1, 1, 2, 1)
	nodeID := nodeOwnedBy(t, stores, "a")
	reg := New(stores, nil, time.Second, nil)

	registered, err := reg.updateNodeRegister(ctx, &routesync.NodeRegister{
		NodeID: nodeID, Capacity: 10, DataEndpoint: "127.0.0.1:19001", Labels: map[string]string{"pool": "p"},
	})
	if err != nil {
		t.Fatalf("updateNodeRegister should keep node_link alive when node_list projection fails: %v", err)
	}
	reg.addNode(&fakeConn{nodeID: nodeID})
	reg.projectRegisteredNode(ctx, registered)
	profile, found, err := stores.GetNodeProfile(ctx, nodeID)
	if err != nil || !found || profile.DataEndpoint != "127.0.0.1:19001" || profile.LinkOwner != "a" {
		t.Fatalf("profile after register=%+v found=%v err=%v", profile, found, err)
	}
}

func TestNodeRegisterRetriesNodeListProjectionAfterQuorumRecovers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	view := clusterstate.MemberView{Version: 1, Members: []string{"a", "b"}}
	cluster := map[string]*Stores{
		"a": NewClusterStores("a", view, 1, 1, 2, 1),
		"b": NewClusterStores("b", view, 1, 1, 2, 1),
	}
	nodeID := nodeOwnedBy(t, cluster["a"], "a")
	reg := New(cluster["a"], nil, time.Second, nil)

	registered, err := reg.updateNodeRegister(ctx, &routesync.NodeRegister{
		NodeID: nodeID, Capacity: 10, DataEndpoint: "127.0.0.1:19001", Labels: map[string]string{"pool": "p"},
	})
	if err != nil {
		t.Fatalf("updateNodeRegister: %v", err)
	}
	reg.addNode(&fakeConn{nodeID: nodeID})
	reg.projectRegisteredNode(ctx, registered)

	transport := shardkv.TransportFunc(func(ctx context.Context, member shardkv.MemberID, req shardkv.Request) (shardkv.Response, error) {
		store := cluster[string(member)]
		if store == nil || store.ShardStore() == nil {
			return shardkv.Response{}, shardkv.ErrReplicaUnavailable
		}
		return store.ShardStore().Handle(ctx, req)
	})
	for _, stores := range cluster {
		stores.SetShardTransport(transport, nil)
	}

	deadline := time.Now().Add(3 * time.Second)
	var lastErr error
	for {
		found := false
		lastErr = cluster["a"].RangeNodeList(ctx, func(entry clusterstate.NodeListEntry) error {
			if entry.NodeID == nodeID && entry.DataEndpoint == "127.0.0.1:19001" && entry.Labels["pool"] == "p" {
				found = true
			}
			return nil
		})
		if lastErr == nil && found {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("node_list projection did not recover; lastErr=%v", lastErr)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestConcurrentHeartbeatsDoNotRewriteNodeListAcrossDeadAfter(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	const nodes = 8
	for i := 0; i < nodes; i++ {
		nodeID := fmt.Sprintf("n-%d", i)
		registered, err := reg.updateNodeRegister(ctx, &routesync.NodeRegister{NodeID: nodeID, Capacity: 1})
		if err != nil {
			t.Fatalf("register %s: %v", nodeID, err)
		}
		reg.addNode(&fakeConn{nodeID: nodeID})
		reg.projectRegisteredNode(ctx, registered)
	}
	before, err := reg.stores.NodeListRev(ctx)
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	stop := make(chan struct{})
	timer := time.AfterFunc(3100*time.Millisecond, func() { close(stop) })
	defer timer.Stop()
	var wg sync.WaitGroup
	for i := 0; i < nodes; i++ {
		nodeID := fmt.Sprintf("n-%d", i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			ticker := time.NewTicker(20 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-stop:
					return
				case <-ticker.C:
					reg.updateHeartbeat(ctx, nodeID, &routesync.Heartbeat{Counts: 1})
				}
			}
		}()
	}
	wg.Wait()
	if elapsed := time.Since(started); elapsed < 3*time.Second {
		t.Fatalf("heartbeat run crossed only %s, want at least 3s", elapsed)
	}
	after, err := reg.stores.NodeListRev(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if after != before {
		t.Fatalf("heartbeat-only updates advanced node_list rev: %d -> %d", before, after)
	}
	for i := 0; i < nodes; i++ {
		nodeID := fmt.Sprintf("n-%d", i)
		node, found, err := reg.stores.GetNodeProfile(ctx, nodeID)
		if err != nil || !found || node.LastHeartbeatUnix < started.Unix() {
			t.Fatalf("node %s profile did not retain live heartbeat: node=%+v found=%v err=%v", nodeID, node, found, err)
		}
	}
}

func TestNodeListReplicatesToLocatedOwnerSet(t *testing.T) {
	ctx := context.Background()
	view := clusterstate.MemberView{Version: 1, Members: []string{"a", "b", "c"}}
	cluster := newShardStoreCluster(t, []string{"a", "b", "c"}, 1, 1, 1, 2)
	stores := cluster["a"]

	entry := clusterstate.NodeListEntry{NodeID: "n1", Labels: map[string]string{"pool": "p"}}
	if err := stores.PutNodeListEntry(ctx, entry); err != nil {
		t.Fatalf("PutNodeListEntry: %v", err)
	}
	owners, err := view.Owners(clusterstate.NamespaceNodeList, 2)
	if err != nil {
		t.Fatal(err)
	}
	assertShardRecordOwners(t, ctx, cluster, shardkv.Namespace(clusterstate.NamespaceNodeList), clusterstate.NodeListShard, clusterstate.RecordSetNodeListNodes, clusterstate.NodeListRecordKey("n1"), owners)
}

func TestNodeListTombstoneRejectsStaleProjection(t *testing.T) {
	ctx := context.Background()
	stores := NewClusterStores("a", clusterstate.MemberView{Version: 1, Members: []string{"a"}}, 1, 1, 1, 1)

	if err := stores.PutNodeListEntry(ctx, clusterstate.NodeListEntry{
		NodeID: "n1", SourceMeta: clusterstate.RecordMeta{Ballot: clusterstate.Ballot{Round: 2, Writer: "a"}, Rev: 2}, Labels: map[string]string{"gen": "new"},
	}); err != nil {
		t.Fatalf("PutNodeListEntry new: %v", err)
	}
	if err := stores.DeleteNodeList(ctx, "n1"); err != nil {
		t.Fatalf("DeleteNodeList: %v", err)
	}
	if err := stores.PutNodeListEntry(ctx, clusterstate.NodeListEntry{
		NodeID: "n1", SourceMeta: clusterstate.RecordMeta{Ballot: clusterstate.Ballot{Round: 1, Writer: "a"}, Rev: 1}, Labels: map[string]string{"gen": "stale"},
	}); err != nil {
		t.Fatalf("PutNodeListEntry stale: %v", err)
	}
	found := false
	if err := stores.RangeNodeList(ctx, func(got clusterstate.NodeListEntry) error {
		found = found || got.NodeID == "n1"
		return nil
	}); err != nil {
		t.Fatalf("RangeNodeList: %v", err)
	}
	if found {
		t.Fatal("stale node_list projection resurrected after delete")
	}
}

func TestNodeListDuplicateProjectionDoesNotAdvanceRev(t *testing.T) {
	ctx := context.Background()
	stores := NewClusterStores("a", clusterstate.MemberView{Version: 1, Members: []string{"a"}}, 1, 1, 1, 1)
	entry := clusterstate.NodeListEntry{
		NodeID: "n1", SourceMeta: clusterstate.RecordMeta{Ballot: clusterstate.Ballot{Round: 1, Writer: "a"}, Rev: 1}, Labels: map[string]string{"pool": "p"},
	}

	if err := stores.PutNodeListEntry(ctx, entry); err != nil {
		t.Fatalf("PutNodeListEntry first: %v", err)
	}
	first, err := stores.NodeListRev(ctx)
	if err != nil {
		t.Fatalf("first rev: %v", err)
	}
	if err := stores.PutNodeListEntry(ctx, entry); err != nil {
		t.Fatalf("PutNodeListEntry duplicate: %v", err)
	}
	second, err := stores.NodeListRev(ctx)
	if err != nil {
		t.Fatalf("second rev: %v", err)
	}
	if second != first {
		t.Fatalf("duplicate projection advanced node_list rev: %d -> %d", first, second)
	}
}

func TestNodeListRangeRepairsLocalFromOwnerSet(t *testing.T) {
	ctx := context.Background()
	cluster := newShardStoreCluster(t, []string{"a", "b", "c"}, 1, 1, 1, 3)
	stores := cluster["a"]
	seed := clusterstate.NodeListEntry{
		Meta:   clusterstate.RecordMeta{Ballot: clusterstate.Ballot{Round: 5, Writer: "b"}, Rev: 3, UpdatedAt: time.Now()},
		NodeID: "n-remote", Labels: map[string]string{"pool": "p"},
	}
	seedNodeListShardRecord(t, ctx, cluster["b"], seed, shardkv.Ballot{Round: 5, Writer: shardkv.MemberID("b")}, 3)
	seedNodeListShardRecord(t, ctx, cluster["c"], seed, shardkv.Ballot{Round: 5, Writer: shardkv.MemberID("b")}, 3)

	var got []clusterstate.NodeListEntry
	if err := stores.RangeNodeList(ctx, func(entry clusterstate.NodeListEntry) error {
		got = append(got, entry)
		return nil
	}); err != nil {
		t.Fatalf("RangeNodeList: %v", err)
	}
	if len(got) != 1 || got[0].NodeID != seed.NodeID {
		t.Fatalf("unexpected range result: %+v", got)
	}
	if !localShardHasRecord(t, ctx, stores, shardkv.Namespace(clusterstate.NamespaceNodeList), clusterstate.NodeListShard, clusterstate.RecordSetNodeListNodes, clusterstate.NodeListRecordKey(seed.NodeID)) {
		t.Fatalf("local repair did not materialize node_list record")
	}
}

func seedNodeListShardRecord(t *testing.T, ctx context.Context, store *Stores, entry clusterstate.NodeListEntry, ballot shardkv.Ballot, rev uint64) {
	t.Helper()
	value, err := clusterstate.EncodeShardValue(entry)
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.ShardStore().Handle(ctx, shardkv.Request{
		Op:        shardkv.OpRepair,
		Namespace: shardkv.Namespace(clusterstate.NamespaceNodeList),
		Shard:     clusterstate.NodeListShard,
		RecordSet: clusterstate.RecordSetNodeListNodes,
		Record: shardkv.Record{
			Namespace: shardkv.Namespace(clusterstate.NamespaceNodeList),
			Shard:     clusterstate.NodeListShard,
			RecordSet: clusterstate.RecordSetNodeListNodes,
			Key:       clusterstate.NodeListRecordKey(entry.NodeID),
			Value:     value,
			Meta:      shardkv.RecordMeta{Ballot: ballot, Rev: rev, UpdatedAt: time.Now()},
		},
	})
	if err != nil {
		t.Fatalf("seed node_list shard: %v", err)
	}
}

func nodeNotOwnedBy(t *testing.T, view clusterstate.MemberView, local string) string {
	t.Helper()
	for i := 0; i < 1000; i++ {
		nodeID := fmt.Sprintf("node-non-owner-%d", i)
		owners, err := view.Owners(nodeID, 1)
		if err != nil {
			t.Fatal(err)
		}
		if len(owners) == 1 && owners[0] != local {
			return nodeID
		}
	}
	t.Fatalf("could not find node not owned by %s", local)
	return ""
}

func newShardStoreCluster(t *testing.T, members []string, routeOwners, nodeOwners, scaleOwners, nodeListOwners int) map[string]*Stores {
	t.Helper()
	view := clusterstate.MemberView{Version: 1, Members: members}
	return newShardStoreClusterWithViews(t, []clusterstate.MemberView{view}, routeOwners, nodeOwners, scaleOwners, nodeListOwners)
}

func newShardStoreClusterWithViews(t *testing.T, views []clusterstate.MemberView, routeOwners, nodeOwners, scaleOwners, nodeListOwners int) map[string]*Stores {
	t.Helper()
	if len(views) == 0 {
		t.Fatal("views are required")
	}
	memberSet := map[string]bool{}
	var members []string
	for _, view := range views {
		for _, member := range view.Members {
			if member == "" || memberSet[member] {
				continue
			}
			memberSet[member] = true
			members = append(members, member)
		}
	}
	out := map[string]*Stores{}
	for _, id := range members {
		stores := NewClusterStoresWithViews(id, views, routeOwners, nodeOwners, nodeListOwners, scaleOwners)
		stores.SetPlacerLinkTopology(views, scaleOwners)
		stores.SetNodeListTopology(views, nodeListOwners)
		out[id] = stores
	}
	transport := shardkv.TransportFunc(func(ctx context.Context, member shardkv.MemberID, req shardkv.Request) (shardkv.Response, error) {
		store := out[string(member)]
		if store == nil || store.ShardStore() == nil {
			return shardkv.Response{}, shardkv.ErrReplicaUnavailable
		}
		return store.ShardStore().Handle(ctx, req)
	})
	for _, stores := range out {
		stores.SetShardTransport(transport, nil)
	}
	return out
}

func assertPlacerImportShardOwners(t *testing.T, ctx context.Context, stores map[string]*Stores, owners []string, sourceID, ownerID string) {
	t.Helper()
	ownerSet := map[string]bool{}
	for _, owner := range owners {
		ownerSet[owner] = true
	}
	for id, store := range stores {
		if !ownerSet[id] {
			continue
		}
		sh, err := store.ShardStore().Shard(shardkv.Namespace(clusterstate.NamespacePlacerLink), clusterstate.PlacerImportSourceShard(sourceID))
		if err != nil {
			t.Fatalf("shard %s: %v", id, err)
		}
		rs, err := sh.RecordSet(clusterstate.RecordSetPlacerImport)
		if err != nil {
			t.Fatalf("record set %s: %v", id, err)
		}
		rec, found, err := rs.Get(ctx, clusterstate.PlacerLinkStateRecord)
		if err != nil || !found {
			t.Fatalf("owner %s read found=%v err=%v", id, found, err)
		}
		state, err := clusterstate.DecodeShardValue[clusterstate.PlacerImportSourceState](rec.Value)
		if err != nil {
			t.Fatalf("owner %s decode: %v", id, err)
		}
		if state.SourceID != sourceID || state.OwnerID != ownerID {
			t.Fatalf("owner %s state=%+v", id, state)
		}
	}
}

type routingNodeOwnerRecorder struct {
	allow     bool
	connected []string
	keys      []string
	admitted  []string
	deleted   []string
	released  []string
}

func (r *routingNodeOwnerRecorder) Connected(ctx context.Context, nodeID string) error {
	r.connected = append(r.connected, nodeID)
	return nil
}

func (r *routingNodeOwnerRecorder) PutManifestKey(ctx context.Context, nodeID, fingerprint, keyType, keyValue string, expiresUnix int64) error {
	r.keys = append(r.keys, nodeID+"/"+fingerprint)
	return nil
}

func (r *routingNodeOwnerRecorder) DropManifestKey(ctx context.Context, nodeID, fingerprint string) error {
	return nil
}

func (r *routingNodeOwnerRecorder) AdmitBuild(ctx context.Context, nodeID, buildID string, want *routesync.BuildResources) bool {
	r.admitted = append(r.admitted, nodeID+"/"+buildID)
	return r.allow
}

func (r *routingNodeOwnerRecorder) ReleaseBuild(ctx context.Context, nodeID, buildID string) {
	r.released = append(r.released, nodeID+"/"+buildID)
}

func (r *routingNodeOwnerRecorder) Runtime(ctx context.Context, nodeID string) (*NodeRecord, bool, error) {
	return &NodeRecord{NodeID: nodeID}, true, nil
}

func (r *routingNodeOwnerRecorder) DeleteSandbox(ctx context.Context, nodeID, sid string) error {
	r.deleted = append(r.deleted, nodeID+"/"+sid)
	return nil
}

func (r *routingNodeOwnerRecorder) SendCommand(ctx context.Context, nodeID string, cmd *routesync.Command) error {
	r.deleted = append(r.deleted, nodeID+"/"+cmd.Kind)
	return nil
}

func (r *routingNodeOwnerRecorder) SendCommandAndWait(ctx context.Context, nodeID string, cmd *routesync.Command, timeout time.Duration) (*routesync.CmdAck, error) {
	r.deleted = append(r.deleted, nodeID+"/"+cmd.Kind)
	return &routesync.CmdAck{CmdID: cmd.CmdID, Status: routesync.AckAccepted}, nil
}
