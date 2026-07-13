package registry

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/cluster/shardkv"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

type recordingNodeOwner struct {
	allow    bool
	admitted []string
	released []string
	ack      *routesync.CmdAck
	ackErr   error
}

func (a *recordingNodeOwner) PutManifestKey(ctx context.Context, nodeID, fingerprint, keyType, keyValue string, expiresUnix int64) error {
	return nil
}

func (a *recordingNodeOwner) DropManifestKey(ctx context.Context, nodeID, fingerprint string) error {
	return nil
}

func (a *recordingNodeOwner) AdmitBuild(ctx context.Context, nodeID, buildID string, want *routesync.BuildResources) bool {
	a.admitted = append(a.admitted, nodeID+"/"+buildID)
	return a.allow
}

func (a *recordingNodeOwner) ReleaseBuild(ctx context.Context, buildID string) {
	a.released = append(a.released, buildID)
}

func (a *recordingNodeOwner) Runtime(ctx context.Context, nodeID string) (*NodeRecord, bool, error) {
	return &NodeRecord{NodeID: nodeID}, true, nil
}

func (a *recordingNodeOwner) DeleteSandbox(ctx context.Context, nodeID, sid string) error {
	return nil
}

func (a *recordingNodeOwner) SendCommand(ctx context.Context, nodeID string, cmd *routesync.Command) error {
	return nil
}

func (a *recordingNodeOwner) SendCommandAndWait(ctx context.Context, nodeID string, cmd *routesync.Command, timeout time.Duration) (*routesync.CmdAck, error) {
	if a.ackErr != nil {
		return nil, a.ackErr
	}
	if a.ack != nil {
		return a.ack, nil
	}
	return &routesync.CmdAck{CmdID: cmd.CmdID, Status: routesync.AckAccepted}, nil
}

func buildAckConn(reg *Registry, nodeID string) *fakeConn {
	return &fakeConn{nodeID: nodeID, onCmd: func(cmd *routesync.Command) {
		if cmd.Kind == routesync.CmdBuildRegister {
			go reg.ackCommand(&routesync.CmdAck{CmdID: cmd.CmdID, Status: routesync.AckAccepted})
		}
	}}
}

func TestReserveBuildUsesNodeOwnerBoundary(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	reg.SetPlacer(placementWithToken("n1"))
	reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1"})
	reg.addNode(buildAckConn(reg, "n1"))
	admitter := &recordingNodeOwner{allow: true}
	reg.SetNodeOwner(admitter)

	res, err := reg.ReserveBuild(ctx, BuildReserveReq{Group: "/g", Resources: &routesync.BuildResources{CPU: 2000}})
	if err != nil {
		t.Fatalf("reserve build: %v", err)
	}
	if len(admitter.admitted) != 1 || admitter.admitted[0] != "n1/"+res.BuildID {
		t.Fatalf("admitted=%v, want n1/%s", admitter.admitted, res.BuildID)
	}
	reg.applyBuildEvent(ctx, "n1", &routesync.BuildEvent{Group: "/g", BuildID: res.BuildID, State: string(BuildReady)})
	if len(admitter.released) != 1 || admitter.released[0] != res.BuildID {
		t.Fatalf("released=%v, want %s", admitter.released, res.BuildID)
	}
}

func TestReserveBuildKeepsCommittedBuildOnAckTimeout(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	reg.SetPlacer(placementWithToken("n1"))
	owner := &recordingNodeOwner{allow: true, ackErr: context.DeadlineExceeded}
	reg.SetNodeOwner(owner)

	res, err := reg.ReserveBuild(ctx, BuildReserveReq{
		Group: "/g", BuildID: "bld-fixed", TemplateID: "transient-fixed",
		Resources: &routesync.BuildResources{CPU: 2000},
	})
	if err != nil {
		t.Fatalf("reserve build should return the committed build on ack timeout: %v", err)
	}
	if res.BuildID != "bld-fixed" || res.TemplateID != "transient-fixed" || res.NodeID != "n1" {
		t.Fatalf("reserve result=%+v", res)
	}
	rec, found, err := reg.stores.GetBuildInGroup(ctx, "/g", "bld-fixed")
	if err != nil || !found {
		t.Fatalf("build record found=%v err=%v", found, err)
	}
	if rec.State != BuildRegistered || rec.TemplateID != "transient-fixed" {
		t.Fatalf("build record after timeout=%+v", rec)
	}
	if len(owner.released) != 0 {
		t.Fatalf("ack timeout should not release an ambiguous build admission: %v", owner.released)
	}
}

func TestReserveBuildSkipsDisconnectedCatalogNode(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	var sawExclusion bool
	reg.SetPlacer(placementFunc(func(_ context.Context, req PlaceRequest) (*Placement, error) {
		for _, nodeID := range req.ExcludeNodeIDs {
			if nodeID == "stale" {
				sawExclusion = true
				return &Placement{NodeID: "live"}, nil
			}
		}
		return &Placement{NodeID: "stale"}, nil
	}))
	for _, nodeID := range []string{"stale", "live"} {
		if err := reg.stores.PutNode(ctx, &NodeRecord{
			NodeID: nodeID, BuildCapacity: &routesync.BuildResources{CPU: 2000},
		}); err != nil {
			t.Fatal(err)
		}
	}
	reg.addNode(buildAckConn(reg, "live"))

	res, err := reg.ReserveBuild(ctx, BuildReserveReq{
		Group: "/g", Resources: &routesync.BuildResources{CPU: 1000},
	})
	if err != nil {
		t.Fatalf("ReserveBuild: %v", err)
	}
	if res.NodeID != "live" || !sawExclusion {
		t.Fatalf("result=%+v saw stale exclusion=%v", res, sawExclusion)
	}
}

func TestLocalNodeOwnerRequiresLiveConnection(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	if err := reg.stores.PutNode(ctx, &NodeRecord{
		NodeID: "n1", BuildCapacity: &routesync.BuildResources{CPU: 2000},
	}); err != nil {
		t.Fatal(err)
	}
	if node, found, err := reg.localNodeOwner.Runtime(ctx, "n1"); !errors.Is(err, ErrNodeGone) || found || node != nil {
		t.Fatalf("disconnected Runtime node=%+v found=%v err=%v", node, found, err)
	}
	if reg.localNodeOwner.AdmitBuild(ctx, "n1", "b1", &routesync.BuildResources{CPU: 1000}) {
		t.Fatal("disconnected node was admitted for build")
	}
	reg.addNode(&fakeConn{nodeID: "n1"})
	if node, found, err := reg.localNodeOwner.Runtime(ctx, "n1"); err != nil || !found || node == nil {
		t.Fatalf("connected Runtime node=%+v found=%v err=%v", node, found, err)
	}
	if !reg.localNodeOwner.AdmitBuild(ctx, "n1", "b1", &routesync.BuildResources{CPU: 1000}) {
		t.Fatal("connected node was not admitted for build")
	}
}

// TestReserveBuildResourceAware drives the §7.5 core: a build reserves its node's
// build pool IMMEDIATELY (RESERVED occupies), a second build that would
// oversubscribe is refused, and a terminal build event releases the pool.
func TestReserveBuildResourceAware(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	reg.SetPlacer(placementWithToken("n1"))
	// n1 has a 2-core build pool.
	reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1", BuildCapacity: &routesync.BuildResources{CPU: 2000, Mem: 8 << 30, Storage: 100 << 30}})
	reg.addNode(buildAckConn(reg, "n1"))

	// First build (1.5 cores) fits + is committed RESERVED on n1.
	r1, err := reg.ReserveBuild(ctx, BuildReserveReq{Group: "/g", Resources: &routesync.BuildResources{CPU: 1500}})
	if err != nil || r1.NodeID != "n1" || r1.BuildID == "" {
		t.Fatalf("first build: %+v err=%v", r1, err)
	}
	if rec, found, _ := reg.stores.GetBuildInGroup(ctx, "/g", r1.BuildID); !found || rec.State != BuildRegistered || !rec.occupies() {
		t.Fatalf("first build not RESERVED in BuildStore: %+v found=%v", rec, found)
	}

	// Second build (1.5 cores) would oversubscribe (1.5+1.5 > 2.0) → refused now,
	// before any heartbeat — RESERVED occupies immediately (the user's invariant).
	if _, err := reg.ReserveBuild(ctx, BuildReserveReq{Group: "/g", Resources: &routesync.BuildResources{CPU: 1500}}); err != ErrNoNode {
		t.Fatalf("oversubscribing build should be refused (ErrNoNode), got %v", err)
	}

	// The first build finishes → releases the pool → a second build now fits.
	reg.applyBuildEvent(ctx, "n1", &routesync.BuildEvent{BuildID: r1.BuildID, Group: "/g", State: "ready", TemplateID: "e2b-img-x"})
	if rec, _, _ := reg.stores.GetBuildInGroup(ctx, "/g", r1.BuildID); rec.occupies() {
		t.Fatal("a ready build should not occupy the pool")
	}
	if r3, err := reg.ReserveBuild(ctx, BuildReserveReq{Group: "/g", Resources: &routesync.BuildResources{CPU: 1500}}); err != nil || r3.NodeID != "n1" {
		t.Fatalf("after release, build should fit: %+v err=%v", r3, err)
	}
}

// TestReserveBuildDefaultResources: an unspecified request uses the default pool
// footprint (cluster.md "不指定则使用默认").
func TestReserveBuildDefaultResources(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	reg.SetPlacer(placementWithToken("n1"))
	reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1"}) // no declared build pool → unconstrained
	reg.addNode(buildAckConn(reg, "n1"))
	r1, err := reg.ReserveBuild(ctx, BuildReserveReq{Group: "/g"})
	if err != nil || r1.BuildID == "" || r1.TemplateID == "" {
		t.Fatalf("default-resources build: %+v err=%v", r1, err)
	}
	rec, found, _ := reg.stores.GetBuildInGroup(ctx, "/g", r1.BuildID)
	if !found || rec.Resources == nil || rec.Resources.CPU != defaultBuildResources.CPU {
		t.Fatalf("default resources not applied: %+v", rec)
	}
}

func TestBuildStoreUsesRouteLinkBuildShardAndDoesNotLeakToSandboxList(t *testing.T) {
	ctx := context.Background()
	view := clusterstate.MemberView{Version: 1, Members: []string{"r1", "r2", "r3"}}
	cluster := newShardStoreCluster(t, []string{"r1", "r2", "r3"}, 3, 1, 1, 1)
	stores := cluster["r1"]

	if err := stores.PutBuild(ctx, &BuildRecord{
		Group: "/g", BuildID: "bld-1", NodeID: "n1", State: BuildRegistered,
		Resources: &routesync.BuildResources{CPU: 1000}, CreatedU: 123,
	}); err != nil {
		t.Fatalf("PutBuild: %v", err)
	}
	owners, err := view.Owners("/g", 3)
	if err != nil {
		t.Fatal(err)
	}
	assertShardRecordOwners(t, ctx, cluster, shardkv.Namespace(clusterstate.NamespaceRouteLink), clusterstate.RouteLinkShard("/g"), clusterstate.RecordSetRouteBuild, clusterstate.RouteBuildRecordKey("bld-1"), owners)

	var sandboxes []string
	if err := stores.RangeSandboxes(ctx, "/g", func(s *SandboxRecord) error {
		sandboxes = append(sandboxes, s.RouteKey)
		return nil
	}); err != nil {
		t.Fatalf("RangeSandboxes: %v", err)
	}
	if len(sandboxes) != 0 {
		t.Fatalf("build record leaked into sandbox list: %v", sandboxes)
	}

	got, found, err := stores.GetBuildInGroup(ctx, "/g", "bld-1")
	if err != nil || !found || got.Resources == nil || got.Resources.CPU != 1000 || got.CreatedU != 123 {
		t.Fatalf("GetBuildInGroup=%+v found=%v err=%v", got, found, err)
	}
}

func TestReserveBuildReleasesAdmissionOnSendFailure(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	reg.SetPlacer(placementWithToken("n1"))
	reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1", BuildCapacity: &routesync.BuildResources{CPU: 2000}})
	reg.addNode(&fakeConn{nodeID: "n1", err: errors.New("send failed")})

	if _, err := reg.ReserveBuild(ctx, BuildReserveReq{Group: "/g", Resources: &routesync.BuildResources{CPU: 2000}}); err == nil {
		t.Fatal("first build should fail when build_register send fails")
	}

	reg.addNode(buildAckConn(reg, "n1"))
	res, err := reg.ReserveBuild(ctx, BuildReserveReq{Group: "/g", Resources: &routesync.BuildResources{CPU: 2000}})
	if err != nil || res.NodeID != "n1" {
		t.Fatalf("admission lease was not released after send failure: res=%+v err=%v", res, err)
	}
}

func TestReserveBuildReleasesAdmissionOnRejectedAck(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	reg.SetPlacer(placementWithToken("n1"))
	reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1", BuildCapacity: &routesync.BuildResources{CPU: 2000}})
	reject := true
	reg.addNode(&fakeConn{nodeID: "n1", onCmd: func(cmd *routesync.Command) {
		if cmd.Kind != routesync.CmdBuildRegister {
			return
		}
		status, reason := routesync.AckAccepted, ""
		if reject {
			status, reason = routesync.AckRejected, "no budget"
		}
		go reg.ackCommand(&routesync.CmdAck{CmdID: cmd.CmdID, Status: status, Reason: reason})
	}})

	if _, err := reg.ReserveBuild(ctx, BuildReserveReq{Group: "/g", Resources: &routesync.BuildResources{CPU: 2000}}); err == nil {
		t.Fatal("rejected build_register should fail")
	} else if fmt.Sprint(err) == "" {
		t.Fatal("expected non-empty rejection error")
	}
	reject = false
	res, err := reg.ReserveBuild(ctx, BuildReserveReq{Group: "/g", Resources: &routesync.BuildResources{CPU: 2000}})
	if err != nil || res.NodeID != "n1" {
		t.Fatalf("admission lease was not released after rejected ack: res=%+v err=%v", res, err)
	}
}
