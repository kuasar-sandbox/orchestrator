package registry

import (
	"context"
	"testing"
	"time"

	clusterstate "github.com/kuasar-sandbox/sandbox-orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/clusterstore"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/routesync"
)

func TestClusterStoresRouteAndNodeUseLocatedOwners(t *testing.T) {
	ctx := context.Background()
	view := clusterstate.MemberView{Version: 1, Members: []string{"a", "b", "c"}}
	rb, rc := clusterstate.NewMemoryRouteReplica(), clusterstate.NewMemoryRouteReplica()
	nb, nc := clusterstate.NewMemoryNodeReplica(), clusterstate.NewMemoryNodeReplica()
	kv := clusterstore.OpenMemory(100)
	defer kv.Close()

	stores := NewClusterStores(kv, "a", view, 2, 2,
		map[string]clusterstate.RouteReplica{"b": rb, "c": rc},
		map[string]clusterstate.NodeReplica{"b": nb, "c": nc})

	group, routeKey := "/cluster/located/group", "rk"
	if _, err := stores.PutSandbox(ctx, &SandboxRecord{Group: group, RouteKey: routeKey, SID: "sb", State: StateReady}); err != nil {
		t.Fatalf("PutSandbox: %v", err)
	}
	routeOwners, err := view.Owners(group, 2)
	if err != nil {
		t.Fatal(err)
	}
	assertRouteOwnerKeys(t, ctx, routeOwners, group+"\x00"+routeKey, map[string]clusterstate.RouteReplica{
		"a": stores.LocalRouteReplica(), "b": rb, "c": rc,
	})

	nodeID := "node-located"
	if err := stores.PutNode(ctx, &NodeRecord{NodeID: nodeID}); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	nodeOwners, err := view.Owners(nodeID, 2)
	if err != nil {
		t.Fatal(err)
	}
	assertNodeOwnerKeys(t, ctx, nodeOwners, nodeID, map[string]clusterstate.NodeReplica{
		"a": stores.LocalNodeReplica(), "b": nb, "c": nc,
	})
}

func TestClusterStoresJointMembershipWritesBothOwnerSets(t *testing.T) {
	ctx := context.Background()
	active := clusterstate.MemberView{Version: 1, Members: []string{"a", "b", "c"}}
	next := clusterstate.MemberView{Version: 2, Members: []string{"b", "c", "d"}}
	rb, rc, rd := clusterstate.NewMemoryRouteReplica(), clusterstate.NewMemoryRouteReplica(), clusterstate.NewMemoryRouteReplica()
	nb, nc, nd := clusterstate.NewMemoryNodeReplica(), clusterstate.NewMemoryNodeReplica(), clusterstate.NewMemoryNodeReplica()
	kv := clusterstore.OpenMemory(100)
	defer kv.Close()

	stores := NewClusterStoresWithViews(kv, "a", []clusterstate.MemberView{active, next}, 2, 2,
		map[string]clusterstate.RouteReplica{"b": rb, "c": rc, "d": rd},
		map[string]clusterstate.NodeReplica{"b": nb, "c": nc, "d": nd})

	group, routeKey := "/cluster/joint/group", "rk"
	if _, err := stores.PutSandbox(ctx, &SandboxRecord{Group: group, RouteKey: routeKey, SID: "sb", State: StateReady}); err != nil {
		t.Fatalf("PutSandbox: %v", err)
	}
	routeReps := map[string]clusterstate.RouteReplica{"a": stores.LocalRouteReplica(), "b": rb, "c": rc, "d": rd}
	for _, view := range []clusterstate.MemberView{active, next} {
		owners, err := view.Owners(group, 2)
		if err != nil {
			t.Fatal(err)
		}
		assertRouteOwnersHaveKey(t, ctx, owners, group+"\x00"+routeKey, routeReps)
	}

	nodeID := "node-joint"
	if err := stores.PutNode(ctx, &NodeRecord{NodeID: nodeID}); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	nodeReps := map[string]clusterstate.NodeReplica{"a": stores.LocalNodeReplica(), "b": nb, "c": nc, "d": nd}
	for _, view := range []clusterstate.MemberView{active, next} {
		owners, err := view.Owners(nodeID, 2)
		if err != nil {
			t.Fatal(err)
		}
		assertNodeOwnersHaveKey(t, ctx, owners, nodeID, nodeReps)
	}
}

func assertRouteOwnerKeys(t *testing.T, ctx context.Context, owners []string, key string, reps map[string]clusterstate.RouteReplica) {
	t.Helper()
	ownerSet := map[string]bool{}
	for _, owner := range owners {
		ownerSet[owner] = true
	}
	for id, rep := range reps {
		has := contains(rep.Keys(ctx), key)
		if ownerSet[id] && !has {
			t.Fatalf("route owner %s missing key %q; owners=%v", id, key, owners)
		}
		if !ownerSet[id] && has {
			t.Fatalf("route non-owner %s unexpectedly has key %q; owners=%v", id, key, owners)
		}
	}
}

func assertRouteOwnersHaveKey(t *testing.T, ctx context.Context, owners []string, key string, reps map[string]clusterstate.RouteReplica) {
	t.Helper()
	for _, owner := range owners {
		rep := reps[owner]
		if rep == nil || !contains(rep.Keys(ctx), key) {
			t.Fatalf("route owner %s missing key %q; owners=%v", owner, key, owners)
		}
	}
}

func assertNodeOwnerKeys(t *testing.T, ctx context.Context, owners []string, key string, reps map[string]clusterstate.NodeReplica) {
	t.Helper()
	ownerSet := map[string]bool{}
	for _, owner := range owners {
		ownerSet[owner] = true
	}
	for id, rep := range reps {
		has := contains(rep.Keys(ctx), key)
		if ownerSet[id] && !has {
			t.Fatalf("node owner %s missing key %q; owners=%v", id, key, owners)
		}
		if !ownerSet[id] && has {
			t.Fatalf("node non-owner %s unexpectedly has key %q; owners=%v", id, key, owners)
		}
	}
}

func assertNodeOwnersHaveKey(t *testing.T, ctx context.Context, owners []string, key string, reps map[string]clusterstate.NodeReplica) {
	t.Helper()
	for _, owner := range owners {
		rep := reps[owner]
		if rep == nil || !contains(rep.Keys(ctx), key) {
			t.Fatalf("node owner %s missing key %q; owners=%v", owner, key, owners)
		}
	}
}

func contains(xs []string, want string) bool {
	for _, x := range xs {
		if x == want {
			return true
		}
	}
	return false
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
	reg.nodeOwner.ReleaseBuild(ctx, "b1")

	if len(remote.keys) != 1 || remote.keys[0] != "n1/fp" {
		t.Fatalf("remote keys=%v", remote.keys)
	}
	if len(remote.admitted) != 1 || remote.admitted[0] != "n1/b1" {
		t.Fatalf("remote admitted=%v", remote.admitted)
	}
	if len(remote.deleted) != 1 || remote.deleted[0] != "n1/sb1" {
		t.Fatalf("remote deleted=%v", remote.deleted)
	}
	if len(remote.released) != 1 || remote.released[0] != "b1" {
		t.Fatalf("remote released=%v", remote.released)
	}
}

func TestNodeListHeartbeatRefreshIsConfigurable(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	reg.stores.SetNodeListHeartbeatRefresh(time.Second)
	if err := reg.updateNodeRegister(ctx, &routesync.NodeRegister{NodeID: "n1", Capacity: 1}); err != nil {
		t.Fatal(err)
	}
	rev1, err := reg.stores.NodeListRev(ctx)
	if err != nil {
		t.Fatal(err)
	}
	node, found, err := reg.stores.GetNode(ctx, "n1")
	if err != nil || !found {
		t.Fatalf("node found=%v err=%v", found, err)
	}
	node.LastHeartbeatUnix -= 2
	if err := reg.stores.PutNodeRuntime(ctx, node); err != nil {
		t.Fatal(err)
	}
	reg.updateHeartbeat(ctx, "n1", &routesync.Heartbeat{})
	rev2, err := reg.stores.NodeListRev(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if rev2 <= rev1 {
		t.Fatalf("node_list rev did not advance after heartbeat refresh: %d -> %d", rev1, rev2)
	}
}

type routingNodeOwnerRecorder struct {
	allow    bool
	keys     []string
	admitted []string
	deleted  []string
	released []string
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

func (r *routingNodeOwnerRecorder) ReleaseBuild(ctx context.Context, buildID string) {
	r.released = append(r.released, buildID)
}

func (r *routingNodeOwnerRecorder) Runtime(ctx context.Context, nodeID string) (*NodeRecord, bool, error) {
	return &NodeRecord{NodeID: nodeID}, true, nil
}

func (r *routingNodeOwnerRecorder) DeleteSandbox(ctx context.Context, nodeID, sid string) error {
	r.deleted = append(r.deleted, nodeID+"/"+sid)
	return nil
}
