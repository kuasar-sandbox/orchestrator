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

func TestClusterStoresScaleLinkLeaseUsesLocatedOwners(t *testing.T) {
	ctx := context.Background()
	view := clusterstate.MemberView{Version: 1, Members: []string{"a", "b", "c"}}
	sb, sc := clusterstate.NewMemoryScaleLinkReplica(), clusterstate.NewMemoryScaleLinkReplica()
	kv := clusterstore.OpenMemory(100)
	defer kv.Close()

	stores := NewClusterStores(kv, "a", view, 2, 2, nil, nil)
	stores.SetScaleLinkTopology([]clusterstate.MemberView{view}, 2, map[string]clusterstate.ScaleLinkReplica{
		"b": sb, "c": sc,
	})

	taskID := "source-a"
	rec, acquired, err := stores.AcquireScaleLinkLease(ctx, taskID, "s1", "run-1", "registry.1.test", time.Second)
	if err != nil {
		t.Fatalf("AcquireScaleLinkLease: %v", err)
	}
	if !acquired || rec.OwnerID != "s1" || rec.RunID != "run-1" || rec.Term == 0 {
		t.Fatalf("unexpected lease acquired=%v rec=%+v", acquired, rec)
	}

	recordKey := scaleLinkTaskKey(taskID)
	key := clusterstate.ScaleLinkShardKey(recordKey)
	owners, err := view.Owners(key, 2)
	if err != nil {
		t.Fatal(err)
	}
	assertScaleLinkOwnerKeys(t, ctx, owners, recordKey, "s1", map[string]clusterstate.ScaleLinkReplica{
		"a": stores.LocalScaleLinkReplica(), "b": sb, "c": sc,
	})

	held, acquired, err := stores.AcquireScaleLinkLease(ctx, taskID, "s2", "run-2", "registry.1.test", time.Second)
	if err != nil {
		t.Fatalf("second AcquireScaleLinkLease: %v", err)
	}
	if acquired || held.OwnerID != "s1" || held.RunID != "run-1" {
		t.Fatalf("live lease was not fenced: acquired=%v held=%+v", acquired, held)
	}
}

func TestClusterStoresScaleLinkAllocationUsesLocatedOwners(t *testing.T) {
	ctx := context.Background()
	view := clusterstate.MemberView{Version: 1, Members: []string{"a", "b", "c"}}
	sb, sc := clusterstate.NewMemoryScaleLinkReplica(), clusterstate.NewMemoryScaleLinkReplica()
	kv := clusterstore.OpenMemory(100)
	defer kv.Close()

	stores := NewClusterStores(kv, "a", view, 2, 2, nil, nil)
	stores.SetScaleLinkTopology([]clusterstate.MemberView{view}, 2, map[string]clusterstate.ScaleLinkReplica{
		"b": sb, "c": sc,
	})

	group := "/scale-link/allocation"
	rec, err := stores.PutScaleLinkAllocation(ctx, &routesync.SelectorPatch{
		Group: group, NodeIDs: []string{"n1", "n2"}, NodeAllocation: true,
		KeyFingerprint: "fp", ManifestKeyType: clusterstate.SecretInline, ManifestKey: "mk",
	}, time.Second)
	if err != nil {
		t.Fatalf("PutScaleLinkAllocation: %v", err)
	}
	if rec.Kind != clusterstate.ScaleLinkKindAllocation || rec.Group != group || rec.KeyFingerprint != "fp" {
		t.Fatalf("unexpected allocation record: %+v", rec)
	}

	recordKey := scaleLinkAllocationKey(group)
	key := clusterstate.ScaleLinkShardKey(recordKey)
	owners, err := view.Owners(key, 2)
	if err != nil {
		t.Fatal(err)
	}
	assertScaleLinkAllocationOwnerKeys(t, ctx, owners, recordKey, group, map[string]clusterstate.ScaleLinkReplica{
		"a": stores.LocalScaleLinkReplica(), "b": sb, "c": sc,
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
		_, has, err := rep.Read(ctx, key)
		if err != nil {
			t.Fatalf("route owner %s read key %q: %v", id, key, err)
		}
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
		if rep == nil {
			t.Fatalf("route owner %s missing replica", owner)
		}
		if _, found, err := rep.Read(ctx, key); err != nil || !found {
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
		_, has, err := rep.Read(ctx, key)
		if err != nil {
			t.Fatalf("node owner %s read key %q: %v", id, key, err)
		}
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
		if rep == nil {
			t.Fatalf("node owner %s missing replica", owner)
		}
		if _, found, err := rep.Read(ctx, key); err != nil || !found {
			t.Fatalf("node owner %s missing key %q; owners=%v", owner, key, owners)
		}
	}
}

func assertScaleLinkOwnerKeys(t *testing.T, ctx context.Context, owners []string, key, ownerID string, reps map[string]clusterstate.ScaleLinkReplica) {
	t.Helper()
	ownerSet := map[string]bool{}
	for _, owner := range owners {
		ownerSet[owner] = true
	}
	for id, rep := range reps {
		rec, has, err := rep.Read(ctx, key)
		if err != nil {
			t.Fatalf("scale_link owner %s read key %q: %v", id, key, err)
		}
		if ownerSet[id] && (!has || rec.OwnerID != ownerID) {
			t.Fatalf("scale_link owner %s missing key %q owner %q; owners=%v rec=%+v", id, key, ownerID, owners, rec)
		}
		if !ownerSet[id] && has {
			t.Fatalf("scale_link non-owner %s unexpectedly has key %q; owners=%v rec=%+v", id, key, owners, rec)
		}
	}
}

func assertScaleLinkAllocationOwnerKeys(t *testing.T, ctx context.Context, owners []string, key, group string, reps map[string]clusterstate.ScaleLinkReplica) {
	t.Helper()
	ownerSet := map[string]bool{}
	for _, owner := range owners {
		ownerSet[owner] = true
	}
	for id, rep := range reps {
		rec, has, err := rep.Read(ctx, key)
		if err != nil {
			t.Fatalf("scale_link owner %s read key %q: %v", id, key, err)
		}
		if ownerSet[id] && (!has || rec.Kind != clusterstate.ScaleLinkKindAllocation || rec.Group != group) {
			t.Fatalf("scale_link owner %s missing allocation key %q group %q; owners=%v rec=%+v", id, key, group, owners, rec)
		}
		if !ownerSet[id] && has {
			t.Fatalf("scale_link non-owner %s unexpectedly has allocation key %q; owners=%v rec=%+v", id, key, owners, rec)
		}
	}
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

func TestNodeListReplicatesToLocatedOwnerSet(t *testing.T) {
	ctx := context.Background()
	view := clusterstate.MemberView{Version: 1, Members: []string{"a", "b", "c"}}
	rb, rc := clusterstate.NewMemoryNodeListReplica(), clusterstate.NewMemoryNodeListReplica()
	kv := clusterstore.OpenMemory(100)
	defer kv.Close()
	stores := NewClusterStores(kv, "a", view, 1, 1, nil, nil)
	stores.SetNodeListTopology([]clusterstate.MemberView{view}, 2, map[string]clusterstate.NodeListReplica{"b": rb, "c": rc})

	entry := clusterstate.NodeListEntry{NodeID: "n1", Labels: map[string]string{"pool": "p"}, LastHeartbeatUnix: time.Now().Unix()}
	if err := stores.PutNodeListEntry(ctx, entry); err != nil {
		t.Fatalf("PutNodeListEntry: %v", err)
	}
	owners, err := view.Owners(clusterstate.NamespaceNodeList, 2)
	if err != nil {
		t.Fatal(err)
	}
	reps := map[string]func() bool{
		"a": func() bool {
			_, found, _ := stores.LocalNodeListReplica().Read(ctx, "n1")
			return found
		},
		"b": func() bool { _, found, _ := rb.Read(ctx, "n1"); return found },
		"c": func() bool { _, found, _ := rc.Read(ctx, "n1"); return found },
	}
	ownerSet := map[string]bool{}
	for _, owner := range owners {
		ownerSet[owner] = true
	}
	for id, has := range reps {
		if ownerSet[id] && !has() {
			t.Fatalf("node_list owner %s missing replicated entry; owners=%v", id, owners)
		}
		if !ownerSet[id] && has() {
			t.Fatalf("node_list non-owner %s unexpectedly has replicated entry; owners=%v", id, owners)
		}
	}
}

func TestNodeListTombstoneRejectsStaleProjection(t *testing.T) {
	ctx := context.Background()
	kv := clusterstore.OpenMemory(100)
	defer kv.Close()
	stores := NewClusterStores(kv, "a", clusterstate.MemberView{Version: 1, Members: []string{"a"}}, 1, 1, nil, nil)

	if err := stores.PutNodeListEntry(ctx, clusterstate.NodeListEntry{
		NodeID: "n1", LastHeartbeatUnix: 100, Labels: map[string]string{"gen": "new"},
	}); err != nil {
		t.Fatalf("PutNodeListEntry new: %v", err)
	}
	if err := stores.DeleteNodeList(ctx, "n1"); err != nil {
		t.Fatalf("DeleteNodeList: %v", err)
	}
	if err := stores.PutNodeListEntry(ctx, clusterstate.NodeListEntry{
		NodeID: "n1", LastHeartbeatUnix: 99, Labels: map[string]string{"gen": "stale"},
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
	kv := clusterstore.OpenMemory(100)
	defer kv.Close()
	stores := NewClusterStores(kv, "a", clusterstate.MemberView{Version: 1, Members: []string{"a"}}, 1, 1, nil, nil)
	entry := clusterstate.NodeListEntry{NodeID: "n1", LastHeartbeatUnix: 100, Labels: map[string]string{"pool": "p"}}

	if err := stores.PutNodeListEntry(ctx, entry); err != nil {
		t.Fatalf("PutNodeListEntry first: %v", err)
	}
	first, found, err := stores.LocalNodeListReplica().Read(ctx, "n1")
	if err != nil || !found {
		t.Fatalf("first read found=%v err=%v", found, err)
	}
	if err := stores.PutNodeListEntry(ctx, entry); err != nil {
		t.Fatalf("PutNodeListEntry duplicate: %v", err)
	}
	second, found, err := stores.LocalNodeListReplica().Read(ctx, "n1")
	if err != nil || !found {
		t.Fatalf("second read found=%v err=%v", found, err)
	}
	if second.Meta.Rev != first.Meta.Rev {
		t.Fatalf("duplicate projection advanced node_list rev: %d -> %d", first.Meta.Rev, second.Meta.Rev)
	}
}

func TestNodeListRangeRepairsLocalFromOwnerSet(t *testing.T) {
	ctx := context.Background()
	view := clusterstate.MemberView{Version: 1, Members: []string{"a", "b"}}
	rb := clusterstate.NewMemoryNodeListReplica()
	seed := clusterstate.NodeListEntry{
		Meta:   clusterstate.RecordMeta{Ballot: clusterstate.Ballot{Round: 5, Writer: "b"}, Rev: 3, UpdatedAt: time.Now()},
		NodeID: "n-remote", Labels: map[string]string{"pool": "p"}, LastHeartbeatUnix: time.Now().Unix(),
	}
	if ok, err := rb.Accept(ctx, seed.NodeID, seed, seed.Meta.Ballot); err != nil || !ok {
		t.Fatalf("seed remote node_list ok=%v err=%v", ok, err)
	}
	kv := clusterstore.OpenMemory(100)
	defer kv.Close()
	stores := NewClusterStores(kv, "a", view, 1, 1, nil, nil)
	stores.SetNodeListTopology([]clusterstate.MemberView{view}, 2, map[string]clusterstate.NodeListReplica{"b": rb})

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
	if _, found, err := stores.LocalNodeListReplica().Read(ctx, seed.NodeID); err != nil || !found {
		t.Fatalf("local repair found=%v err=%v", found, err)
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

func (r *routingNodeOwnerRecorder) SendCommand(ctx context.Context, nodeID string, cmd *routesync.Command) error {
	r.deleted = append(r.deleted, nodeID+"/"+cmd.Kind)
	return nil
}

func (r *routingNodeOwnerRecorder) SendCommandAndWait(ctx context.Context, nodeID string, cmd *routesync.Command, timeout time.Duration) (*routesync.CmdAck, error) {
	r.deleted = append(r.deleted, nodeID+"/"+cmd.Kind)
	return &routesync.CmdAck{CmdID: cmd.CmdID, Status: routesync.AckAccepted}, nil
}
