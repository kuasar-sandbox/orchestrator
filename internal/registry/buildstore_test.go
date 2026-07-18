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
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

type recordingNodeOwner struct {
	allow        bool
	admitted     []string
	released     []string
	connectedErr error
	ack          *routesync.CmdAck
	ackErr       error
	commands     []*routesync.Command
}

func (a *recordingNodeOwner) Connected(context.Context, string) error { return a.connectedErr }

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

func (a *recordingNodeOwner) ReleaseBuild(ctx context.Context, nodeID, buildID string) {
	a.released = append(a.released, nodeID+"/"+buildID)
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
	a.commands = append(a.commands, cmd)
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

	res, err := reg.ReserveBuild(ctx, BuildReserveReq{Group: "/g", Profile: types.ProfileBare, Resources: &routesync.BuildResources{CPU: 2000}})
	if err != nil {
		t.Fatalf("reserve build: %v", err)
	}
	wantLease := buildAdmissionID("/g", res.BuildID)
	if len(admitter.admitted) != 1 || admitter.admitted[0] != "n1/"+wantLease {
		t.Fatalf("admitted=%v, want n1/%q", admitter.admitted, wantLease)
	}
	if res.Profile != types.ProfileBare || len(admitter.commands) != 1 || admitter.commands[0].Profile != string(types.ProfileBare) {
		t.Fatalf("profile did not reach build_register: result=%q commands=%+v", res.Profile, admitter.commands)
	}
	reg.applyBuildEvent(ctx, "n1", &routesync.BuildEvent{BuildID: res.BuildID, State: string(BuildReady)})
	if len(admitter.released) != 1 || admitter.released[0] != "n1/"+wantLease {
		t.Fatalf("released=%v, want n1/%q", admitter.released, wantLease)
	}
}

func TestLegacyBuildConvergenceRejectsFencedFinalEvent(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	owner := &recordingNodeOwner{allow: true}
	reg.SetNodeOwner(owner)
	build := &BuildRecord{Group: "/g", BuildID: "b1", NodeID: "n1", State: BuildRegistered}
	if err := reg.stores.PutBuild(ctx, build); err != nil {
		t.Fatal(err)
	}
	if err := reg.stores.AddNodeBuildRef(ctx, "n1", clusterstate.NodeBuildRef{Group: "/g", BuildID: "b1"}); err != nil {
		t.Fatal(err)
	}
	reg.applyBuildEvent(ctx, "n1", &routesync.BuildEvent{
		BuildID: "b1", State: string(BuildReady), NodeEpoch: 7, EventSeq: 1,
		StorageGeneration: "g1", BindingDigest: "binding-digest",
	})
	got, found, err := reg.stores.GetBuildInGroup(ctx, "/g", "b1")
	if err != nil || !found || got.State != BuildRegistered {
		t.Fatalf("legacy path applied fenced event: build=%+v found=%v err=%v", got, found, err)
	}
	if len(owner.released) != 0 {
		t.Fatalf("legacy path released Admission for rejected event: %v", owner.released)
	}
}

func TestBuildAdmissionIsNodeScoped(t *testing.T) {
	m := newBuildAdmissionManager()
	cap := &routesync.BuildResources{CPU: 1000}
	want := &routesync.BuildResources{CPU: 1000}
	if !m.admit("n1", "same-build", cap, want) || !m.admit("n2", "same-build", cap, want) {
		t.Fatal("same build id on different nodes should have independent admission")
	}
	if m.admit("n1", "other-build", cap, want) || m.admit("n2", "other-build", cap, want) {
		t.Fatal("node-local capacity was not enforced")
	}
	m.release("n1", "same-build")
	if !m.admit("n1", "other-build", cap, want) {
		t.Fatal("releasing n1 did not free n1 capacity")
	}
	if m.admit("n2", "other-build", cap, want) {
		t.Fatal("releasing n1 incorrectly freed n2 capacity")
	}
}

func TestReserveBuildRejectsSameNodeIDCollisionAcrossGroups(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	reg.SetPlacer(placementWithToken("n1"))
	owner := &recordingNodeOwner{allow: true}
	reg.SetNodeOwner(owner)

	const buildID = "shared-build"
	first, err := reg.ReserveBuild(ctx, BuildReserveReq{Group: "/g1", BuildID: buildID, Profile: types.ProfileE2B})
	if err != nil {
		t.Fatalf("first reserve: %v", err)
	}
	if first.NodeID != "n1" {
		t.Fatalf("first placement=%+v", first)
	}
	reg.applyBuildEvent(ctx, "n1", &routesync.BuildEvent{BuildID: buildID, State: string(BuildReady)})
	if _, err := reg.ReserveBuild(ctx, BuildReserveReq{Group: "/g2", BuildID: buildID, Profile: types.ProfileE2B}); !errors.Is(err, errNodeBuildIDConflict) {
		t.Fatalf("second reserve err=%v, want node build-id conflict", err)
	}

	ref, found, err := reg.stores.GetNodeBuildRef(ctx, "n1", buildID)
	if err != nil || !found || ref.Group != "/g1" {
		t.Fatalf("original ownership ref=%+v found=%v err=%v", ref, found, err)
	}
	if _, found, err := reg.stores.GetBuildInGroup(ctx, "/g2", buildID); err != nil || found {
		t.Fatalf("conflicting build record remained: found=%v err=%v", found, err)
	}
	wantReleased := []string{
		"n1/" + buildAdmissionID("/g1", buildID),
		"n1/" + buildAdmissionID("/g2", buildID),
	}
	if len(owner.released) != len(wantReleased) || owner.released[0] != wantReleased[0] || owner.released[1] != wantReleased[1] {
		t.Fatalf("released=%q, want %q", owner.released, wantReleased)
	}
}

func TestBuildEventsResolveSameIDByNodeOwnerTable(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	for _, tc := range []struct {
		node  string
		group string
	}{
		{node: "n1", group: "/g1"},
		{node: "n2", group: "/g2"},
	} {
		if err := reg.stores.PutBuild(ctx, &BuildRecord{Group: tc.group, BuildID: "same-build", NodeID: tc.node, State: BuildRegistered}); err != nil {
			t.Fatal(err)
		}
		if err := reg.stores.AddNodeBuildRef(ctx, tc.node, clusterstate.NodeBuildRef{Group: tc.group, BuildID: "same-build"}); err != nil {
			t.Fatal(err)
		}
	}

	reg.applyBuildEvent(ctx, "n1", &routesync.BuildEvent{BuildID: "same-build", State: string(BuildBuilding)})
	g1, found, err := reg.stores.GetBuildInGroup(ctx, "/g1", "same-build")
	if err != nil || !found || g1.State != BuildBuilding {
		t.Fatalf("g1 build=%+v found=%v err=%v", g1, found, err)
	}
	g2, found, err := reg.stores.GetBuildInGroup(ctx, "/g2", "same-build")
	if err != nil || !found || g2.State != BuildRegistered {
		t.Fatalf("n1 event changed g2 build=%+v found=%v err=%v", g2, found, err)
	}
}

func TestDeadBuildCASDoesNotOverwriteReplacement(t *testing.T) {
	ctx := context.Background()
	stores := NewStores()
	original := &BuildRecord{Group: "/g", BuildID: "same-build", NodeID: "n1", State: BuildRegistered, TemplateID: "old"}
	if err := stores.PutBuild(ctx, original); err != nil {
		t.Fatal(err)
	}
	stale, rev, found, err := stores.getRouteBuildShard(ctx, original.Group, original.BuildID)
	if err != nil || !found {
		t.Fatalf("stale build found=%v err=%v", found, err)
	}
	replacement := &BuildRecord{Group: "/g", BuildID: "same-build", NodeID: "n1", State: BuildRegistered, TemplateID: "new"}
	if err := stores.PutBuild(ctx, replacement); err != nil {
		t.Fatal(err)
	}
	stale.State, stale.Reason = BuildError, "node disconnected"
	if _, ok, err := stores.casRouteBuildShard(ctx, stale, rev); err != nil || ok {
		t.Fatalf("stale dead-build CAS ok=%v err=%v", ok, err)
	}
	got, found, err := stores.GetBuildInGroup(ctx, replacement.Group, replacement.BuildID)
	if err != nil || !found || got.State != BuildRegistered || got.TemplateID != "new" {
		t.Fatalf("replacement build=%+v found=%v err=%v", got, found, err)
	}
}

func TestTerminalBuildStoreRetryOutlivesLinkContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	attempts := 0
	err := retryTerminalBuildStore(ctx, func(writeCtx context.Context) error {
		if err := writeCtx.Err(); err != nil {
			t.Fatalf("retry inherited canceled node-link context: %v", err)
		}
		attempts++
		if attempts < 3 {
			return shardkv.ErrQuorum
		}
		return nil
	})
	if err != nil {
		t.Fatalf("retry terminal build store: %v", err)
	}
	if attempts != 3 {
		t.Fatalf("attempts=%d, want 3", attempts)
	}
}

func TestReserveBuildKeepsCommittedBuildOnAckTimeout(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	reg.SetPlacer(placementWithToken("n1"))
	owner := &recordingNodeOwner{allow: true, ackErr: context.DeadlineExceeded}
	reg.SetNodeOwner(owner)

	res, err := reg.ReserveBuild(ctx, BuildReserveReq{
		Group: "/g", BuildID: "bld-fixed", TemplateID: "transient-fixed", Profile: types.ProfileE2B,
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

func TestReserveBuildKeepsCommittedBuildAfterAmbiguousCommandError(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	placements := 0
	reg.SetPlacer(placementFunc(func(context.Context, PlaceRequest) (*Placement, error) {
		placements++
		return &Placement{NodeID: fmt.Sprintf("n%d", placements)}, nil
	}))
	ackErr := errors.New("build acknowledgement response lost")
	owner := &recordingNodeOwner{allow: true, ackErr: ackErr}
	reg.SetNodeOwner(owner)

	res, err := reg.ReserveBuild(ctx, BuildReserveReq{Group: "/g", BuildID: "bld-fixed", Profile: types.ProfileE2B})
	if err != nil {
		t.Fatalf("ReserveBuild: %v", err)
	}
	if res == nil || res.BuildID != "bld-fixed" || res.NodeID != "n1" {
		t.Fatalf("ReserveBuild result=%+v", res)
	}
	if placements != 1 || len(owner.admitted) != 1 {
		t.Fatalf("ambiguous build was retried: placements=%d admitted=%v", placements, owner.admitted)
	}
	if rec, found, err := reg.stores.GetBuildInGroup(ctx, "/g", "bld-fixed"); err != nil || !found || rec.State != BuildRegistered {
		t.Fatalf("build record=%+v found=%v err=%v, want committed REGISTERED", rec, found, err)
	}
	if len(owner.released) != 0 {
		t.Fatalf("ambiguous build admission was released: %v", owner.released)
	}
	res, err = reg.ReserveBuild(ctx, BuildReserveReq{Group: "/g", BuildID: "bld-fixed", Profile: types.ProfileE2B})
	if err != nil || res == nil || res.NodeID != "n1" || placements != 1 || len(owner.admitted) != 1 {
		t.Fatalf("stable build replay result=%+v err=%v placements=%d admitted=%v", res, err, placements, owner.admitted)
	}
	if _, err := reg.ReserveBuild(ctx, BuildReserveReq{Group: "/g", BuildID: "bld-fixed", Profile: types.ProfileBare}); err == nil {
		t.Fatal("stable build replay accepted a different profile")
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
		Group: "/g", Profile: types.ProfileE2B, Resources: &routesync.BuildResources{CPU: 1000},
	})
	if err != nil {
		t.Fatalf("ReserveBuild: %v", err)
	}
	if res.NodeID != "live" || !sawExclusion {
		t.Fatalf("result=%+v saw stale exclusion=%v", res, sawExclusion)
	}
}

func TestReserveBuildDoesNotExcludeOnConnectionCheckError(t *testing.T) {
	ctx := context.Background()
	placements := 0
	reg := New(NewStores(), placementFunc(func(context.Context, PlaceRequest) (*Placement, error) {
		placements++
		return &Placement{NodeID: "n1"}, nil
	}), time.Second, nil)
	checkErr := errors.New("node owner temporarily unavailable")
	owner := &recordingNodeOwner{allow: true, connectedErr: checkErr}
	reg.SetNodeOwner(owner)

	if _, err := reg.ReserveBuild(ctx, BuildReserveReq{Group: "/g", Profile: types.ProfileE2B}); !errors.Is(err, checkErr) {
		t.Fatalf("ReserveBuild err=%v, want connection check error", err)
	}
	if placements != 1 || len(owner.admitted) != 0 {
		t.Fatalf("connection check error retried/admitted: placements=%d admitted=%v", placements, owner.admitted)
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

func TestReserveBuildRequiresProfile(t *testing.T) {
	reg := testReg(t)
	if _, err := reg.ReserveBuild(context.Background(), BuildReserveReq{Group: "/g"}); err == nil {
		t.Fatal("ReserveBuild accepted a missing profile")
	}
}

func TestLocalNodeOwnerConnectedDoesNotRequireProfile(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	reg.addNode(&fakeConn{nodeID: "n1"})

	if err := reg.localNodeOwner.Connected(ctx, "n1"); err != nil {
		t.Fatalf("Connected: %v", err)
	}
	if _, found, err := reg.localNodeOwner.Runtime(ctx, "n1"); err != nil || found {
		t.Fatalf("Runtime found=%v err=%v, want missing profile", found, err)
	}
	if err := reg.nodeRuntimeLive(ctx, "n1"); err != nil {
		t.Fatalf("liveness check depended on profile: %v", err)
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
	r1, err := reg.ReserveBuild(ctx, BuildReserveReq{Group: "/g", Profile: types.ProfileE2B, Resources: &routesync.BuildResources{CPU: 1500}})
	if err != nil || r1.NodeID != "n1" || r1.BuildID == "" {
		t.Fatalf("first build: %+v err=%v", r1, err)
	}
	if rec, found, _ := reg.stores.GetBuildInGroup(ctx, "/g", r1.BuildID); !found || rec.State != BuildRegistered || !rec.occupies() {
		t.Fatalf("first build not RESERVED in BuildStore: %+v found=%v", rec, found)
	}

	// Second build (1.5 cores) would oversubscribe (1.5+1.5 > 2.0) → refused now,
	// before any heartbeat — RESERVED occupies immediately (the user's invariant).
	if _, err := reg.ReserveBuild(ctx, BuildReserveReq{Group: "/g", Profile: types.ProfileE2B, Resources: &routesync.BuildResources{CPU: 1500}}); err != ErrNoNode {
		t.Fatalf("oversubscribing build should be refused (ErrNoNode), got %v", err)
	}

	// The first build finishes → releases the pool → a second build now fits.
	reg.applyBuildEvent(ctx, "n1", &routesync.BuildEvent{BuildID: r1.BuildID, State: "ready", TemplateID: "e2b-img-x"})
	if rec, _, _ := reg.stores.GetBuildInGroup(ctx, "/g", r1.BuildID); rec.occupies() {
		t.Fatal("a ready build should not occupy the pool")
	}
	if r3, err := reg.ReserveBuild(ctx, BuildReserveReq{Group: "/g", Profile: types.ProfileE2B, Resources: &routesync.BuildResources{CPU: 1500}}); err != nil || r3.NodeID != "n1" {
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
	r1, err := reg.ReserveBuild(ctx, BuildReserveReq{Group: "/g", Profile: types.ProfileE2B})
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
		Group: "/g", BuildID: "bld-1", NodeID: "n1", Profile: types.ProfileBare, State: BuildRegistered,
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
	if err != nil || !found || got.Profile != types.ProfileBare || got.Resources == nil || got.Resources.CPU != 1000 || got.CreatedU != 123 {
		t.Fatalf("GetBuildInGroup=%+v found=%v err=%v", got, found, err)
	}
}

func TestReserveBuildReleasesAdmissionOnDefinitiveSendFailure(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	reg.SetPlacer(placementWithToken("n1"))
	reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1", BuildCapacity: &routesync.BuildResources{CPU: 2000}})
	reg.addNode(&fakeConn{nodeID: "n1", err: ErrNodeGone})

	if _, err := reg.ReserveBuild(ctx, BuildReserveReq{Group: "/g", Profile: types.ProfileE2B, Resources: &routesync.BuildResources{CPU: 2000}}); err == nil {
		t.Fatal("first build should fail when the node is gone")
	}

	reg.addNode(buildAckConn(reg, "n1"))
	res, err := reg.ReserveBuild(ctx, BuildReserveReq{Group: "/g", Profile: types.ProfileE2B, Resources: &routesync.BuildResources{CPU: 2000}})
	if err != nil || res.NodeID != "n1" {
		t.Fatalf("admission lease was not released after definitive send failure: res=%+v err=%v", res, err)
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

	if _, err := reg.ReserveBuild(ctx, BuildReserveReq{Group: "/g", Profile: types.ProfileE2B, Resources: &routesync.BuildResources{CPU: 2000}}); err == nil {
		t.Fatal("rejected build_register should fail")
	} else if fmt.Sprint(err) == "" {
		t.Fatal("expected non-empty rejection error")
	}
	reject = false
	res, err := reg.ReserveBuild(ctx, BuildReserveReq{Group: "/g", Profile: types.ProfileE2B, Resources: &routesync.BuildResources{CPU: 2000}})
	if err != nil || res.NodeID != "n1" {
		t.Fatalf("admission lease was not released after rejected ack: res=%+v err=%v", res, err)
	}
}
