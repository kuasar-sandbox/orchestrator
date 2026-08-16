package registry

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/cluster/shardkv"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
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
	deleted      []string
	runtime      map[string]*NodeRecord
}

func testWireBuildResources() *routesync.BuildResources {
	return &routesync.BuildResources{CPU: 2000, Memory: 2 << 30}
}

func (a *recordingNodeOwner) Connected(context.Context, string) error { return a.connectedErr }

func (a *recordingNodeOwner) PutKeyPair(ctx context.Context, nodeID string, pair clusterstate.NodeKeyPair) error {
	return nil
}

func (a *recordingNodeOwner) DropKeyPair(ctx context.Context, nodeID, apiSecretFingerprint string) error {
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
	if a.runtime != nil {
		node, found := a.runtime[nodeID]
		return node, found, nil
	}
	return &NodeRecord{NodeID: nodeID}, true, nil
}

func (a *recordingNodeOwner) DeleteSandbox(ctx context.Context, nodeID, sid, apiSecretFingerprint string) error {
	a.deleted = append(a.deleted, nodeID+"/"+sid+"/"+apiSecretFingerprint)
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

func TestReserveBuildDelegatesAdmissionToNodeCommand(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	reg.SetPlacer(placementWithToken("n1"))
	reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1"})
	reg.addNode(buildAckConn(reg, "n1"))
	admitter := &recordingNodeOwner{allow: true}
	reg.SetNodeOwner(admitter)

	res, err := reg.ReserveBuild(ctx, BuildReserveReq{Group: "/g", Profile: types.ProfileBare, Resources: testWireBuildResources()})
	if err != nil {
		t.Fatalf("reserve build: %v", err)
	}
	if res.Profile != types.ProfileBare || len(admitter.commands) != 1 || admitter.commands[0].Profile != string(types.ProfileBare) {
		t.Fatalf("profile did not reach build_register: result=%q commands=%+v", res.Profile, admitter.commands)
	}
	if got := admitter.commands[0].BuildResources; got == nil || got.CPU != 2000 || got.Memory != 2<<30 {
		t.Fatalf("build resources did not reach node authority: %+v", got)
	}
}

func TestReserveBuildRejectsRuntimeOwnedResourceBeforePlacement(t *testing.T) {
	reg := testReg(t)
	_, err := reg.ReserveBuild(context.Background(), BuildReserveReq{
		Group: "/g", Profile: types.ProfileE2B,
		Metadata: map[string]string{sandboxcfg.NsResource: `{"allocatable":{"deflate_on_oom":false}}`},
	})
	if !errors.Is(err, errInvalidSandboxConfig) || !strings.Contains(err.Error(), "node-managed") {
		t.Fatalf("ReserveBuild error = %v", err)
	}
}

func TestReserveBuildRejectsSameNodeIDCollisionAcrossGroups(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	reg.SetPlacer(placementWithToken("n1"))
	owner := &recordingNodeOwner{allow: true}
	reg.SetNodeOwner(owner)

	const buildID = "shared-build"
	first, err := reg.ReserveBuild(ctx, BuildReserveReq{Group: "/g1", BuildID: buildID, Profile: types.ProfileE2B, Resources: testWireBuildResources()})
	if err != nil {
		t.Fatalf("first reserve: %v", err)
	}
	if first.NodeID != "n1" {
		t.Fatalf("first placement=%+v", first)
	}
	reg.applyBuildEvent(ctx, "n1", &routesync.BuildEvent{BuildID: buildID, State: string(BuildReady)})
	if _, err := reg.ReserveBuild(ctx, BuildReserveReq{Group: "/g2", BuildID: buildID, Profile: types.ProfileE2B, Resources: testWireBuildResources()}); !errors.Is(err, errNodeBuildIDConflict) {
		t.Fatalf("second reserve err=%v, want node build-id conflict", err)
	}

	ref, found, err := reg.stores.GetNodeBuildRef(ctx, "n1", buildID)
	if err != nil || !found || ref.Group != "/g1" {
		t.Fatalf("original ownership ref=%+v found=%v err=%v", ref, found, err)
	}
	if _, found, err := reg.stores.GetBuildInGroup(ctx, "/g2", buildID); err != nil || found {
		t.Fatalf("conflicting build record remained: found=%v err=%v", found, err)
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

func TestReserveBuildRetriesSameNodeAfterAckTimeout(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	reg.SetPlacer(placementWithToken("n1"))
	owner := &recordingNodeOwner{allow: true, ackErr: context.DeadlineExceeded}
	reg.SetNodeOwner(owner)

	req := BuildReserveReq{
		Group: "/g", BuildID: "bld-fixed", TemplateID: "transient-fixed", Profile: types.ProfileE2B,
		Resources: testWireBuildResources(), Metadata: map[string]string{"kuasar-sandbox.launch": `{}`},
	}
	if res, err := reg.ReserveBuild(ctx, req); err == nil || res != nil {
		t.Fatalf("ack timeout result=%+v err=%v, want ambiguous error", res, err)
	}
	rec, found, err := reg.stores.GetBuildInGroup(ctx, "/g", "bld-fixed")
	if err != nil || !found {
		t.Fatalf("build record found=%v err=%v", found, err)
	}
	if rec.State != BuildStarting || rec.TemplateID != "transient-fixed" {
		t.Fatalf("build record after timeout=%+v", rec)
	}
	if rec.RegistrationImageRepo != "repo" || rec.RegistrationRegistryAuth != "auth-json" {
		t.Fatalf("ambiguous registration lost its replay envelope: %+v", rec)
	}
	if _, found := reg.ResolveBuild(ctx, "/g", "bld-fixed"); found {
		t.Fatal("ambiguous build became routable before durable node acceptance")
	}
	owner.ackErr = nil
	res, err := reg.ReserveBuild(ctx, req)
	if err != nil || res == nil || res.BuildID != "bld-fixed" || res.TemplateID != "transient-fixed" || res.NodeID != "n1" {
		t.Fatalf("same-node retry result=%+v err=%v", res, err)
	}
	if len(owner.commands) != 2 || owner.commands[0].BuildID != owner.commands[1].BuildID ||
		owner.commands[0].TemplateRef != owner.commands[1].TemplateRef ||
		owner.commands[0].Config["kuasar-sandbox.launch"] != owner.commands[1].Config["kuasar-sandbox.launch"] ||
		owner.commands[0].ImageRepo != owner.commands[1].ImageRepo ||
		owner.commands[0].RegistryAuth != owner.commands[1].RegistryAuth {
		t.Fatalf("retry did not replay the exact registration: %+v", owner.commands)
	}
	accepted, found, err := reg.stores.GetBuildInGroup(ctx, "/g", "bld-fixed")
	if err != nil || !found || accepted.State != BuildRegistered ||
		accepted.RegistrationImageRepo != "" || accepted.RegistrationRegistryAuth != "" {
		t.Fatalf("accepted registration retained transient credentials: build=%+v found=%v err=%v", accepted, found, err)
	}
}

func TestReserveBuildKeepsCommittedBuildAfterAmbiguousCommandError(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	placements := 0
	reg.SetPlacer(placementFunc(func(context.Context, PlaceRequest) (*Placement, error) {
		placements++
		return &Placement{NodeID: fmt.Sprintf("n%d", placements), APISecretFingerprint: testAPIFingerprint}, nil
	}))
	ackErr := errors.New("build acknowledgement response lost")
	owner := &recordingNodeOwner{allow: true, ackErr: ackErr}
	reg.SetNodeOwner(owner)

	req := BuildReserveReq{Group: "/g", BuildID: "bld-fixed", Profile: types.ProfileE2B, Resources: testWireBuildResources()}
	if res, err := reg.ReserveBuild(ctx, req); err == nil || res != nil {
		t.Fatalf("ambiguous ReserveBuild result=%+v err=%v", res, err)
	}
	if placements != 1 || len(owner.commands) != 1 {
		t.Fatalf("ambiguous build changed placement: placements=%d commands=%d", placements, len(owner.commands))
	}
	if rec, found, err := reg.stores.GetBuildInGroup(ctx, "/g", "bld-fixed"); err != nil || !found || rec.State != BuildStarting {
		t.Fatalf("build record=%+v found=%v err=%v, want pinned STARTING", rec, found, err)
	}
	owner.ackErr = nil
	res, err := reg.ReserveBuild(ctx, req)
	if err != nil || res == nil || res.NodeID != "n1" || placements != 1 || len(owner.commands) != 2 {
		t.Fatalf("stable build replay result=%+v err=%v placements=%d commands=%d", res, err, placements, len(owner.commands))
	}
	if _, err := reg.ReserveBuild(ctx, BuildReserveReq{Group: "/g", BuildID: "bld-fixed", Profile: types.ProfileBare, Resources: testWireBuildResources()}); err == nil {
		t.Fatal("stable build replay accepted a different profile")
	}
	differentResources := testWireBuildResources()
	differentResources.Memory++
	if _, err := reg.ReserveBuild(ctx, BuildReserveReq{Group: "/g", BuildID: "bld-fixed", Profile: types.ProfileE2B, Resources: differentResources}); err == nil {
		t.Fatal("stable build replay accepted different resources")
	}
	if _, err := reg.ReserveBuild(ctx, BuildReserveReq{
		Group: "/g", BuildID: "bld-fixed", Profile: types.ProfileE2B,
		Resources: testWireBuildResources(), Metadata: map[string]string{"kuasar-sandbox.launch": `{"pid_namespace":"private"}`},
	}); err == nil {
		t.Fatal("stable build replay accepted different phase metadata")
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
				return &Placement{NodeID: "live", APISecretFingerprint: testAPIFingerprint}, nil
			}
		}
		return &Placement{NodeID: "stale", APISecretFingerprint: testAPIFingerprint}, nil
	}))
	for _, nodeID := range []string{"stale", "live"} {
		if err := reg.stores.PutNode(ctx, &NodeRecord{
			NodeID: nodeID, BuildRegistrationCapacity: &routesync.BuildAdmissionLimit{Resources: &routesync.BuildResources{CPU: 2000}},
		}); err != nil {
			t.Fatal(err)
		}
	}
	reg.addNode(buildAckConn(reg, "live"))

	res, err := reg.ReserveBuild(ctx, BuildReserveReq{
		Group: "/g", Profile: types.ProfileE2B, Resources: testWireBuildResources(),
	})
	if err != nil {
		t.Fatalf("ReserveBuild: %v", err)
	}
	if res.NodeID != "live" || !sawExclusion {
		t.Fatalf("result=%+v saw stale exclusion=%v", res, sawExclusion)
	}
}

func TestReserveBuildFiltersByHolderRegistrationHeadroom(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	var sawFullExclusion bool
	reg.SetPlacer(placementFunc(func(_ context.Context, req PlaceRequest) (*Placement, error) {
		for _, nodeID := range req.ExcludeNodeIDs {
			if nodeID == "full" {
				sawFullExclusion = true
				return &Placement{NodeID: "free", APISecretFingerprint: testAPIFingerprint}, nil
			}
		}
		return &Placement{NodeID: "full", APISecretFingerprint: testAPIFingerprint}, nil
	}))
	owner := &recordingNodeOwner{runtime: map[string]*NodeRecord{
		"full": {
			NodeID: "full",
			BuildRegistrationCapacity: &routesync.BuildAdmissionLimit{
				MaxBuilds: 1, Resources: &routesync.BuildResources{CPU: 2000, Memory: 2 << 30},
			},
			BuildRegistrationUsage: &routesync.BuildAdmissionUsage{
				Builds: 1, Resources: &routesync.BuildResources{CPU: 2000, Memory: 2 << 30},
			},
		},
		"free": {
			NodeID: "free",
			BuildRegistrationCapacity: &routesync.BuildAdmissionLimit{
				MaxBuilds: 1, Resources: &routesync.BuildResources{CPU: 2000, Memory: 2 << 30},
			},
			BuildRegistrationUsage: &routesync.BuildAdmissionUsage{},
		},
	}}
	reg.SetNodeOwner(owner)

	result, err := reg.ReserveBuild(ctx, BuildReserveReq{
		Group: "/g", Profile: types.ProfileE2B, Resources: testWireBuildResources(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.NodeID != "free" || !sawFullExclusion {
		t.Fatalf("result=%+v saw full exclusion=%v", result, sawFullExclusion)
	}
	if len(owner.commands) != 1 || owner.commands[0].BuildID != result.BuildID {
		t.Fatalf("commands after headroom filtering = %+v", owner.commands)
	}
}

func TestReserveBuildDoesNotExcludeOnConnectionCheckError(t *testing.T) {
	ctx := context.Background()
	placements := 0
	reg := New(NewStores(), placementFunc(func(context.Context, PlaceRequest) (*Placement, error) {
		placements++
		return &Placement{NodeID: "n1", APISecretFingerprint: testAPIFingerprint}, nil
	}), time.Second, nil)
	checkErr := errors.New("node owner temporarily unavailable")
	owner := &recordingNodeOwner{allow: true, connectedErr: checkErr}
	reg.SetNodeOwner(owner)

	if _, err := reg.ReserveBuild(ctx, BuildReserveReq{Group: "/g", Profile: types.ProfileE2B, Resources: testWireBuildResources()}); !errors.Is(err, checkErr) {
		t.Fatalf("ReserveBuild err=%v, want connection check error", err)
	}
	if placements != 1 || len(owner.commands) != 0 {
		t.Fatalf("connection check error retried/dispatched: placements=%d commands=%d", placements, len(owner.commands))
	}
}

func TestLocalNodeOwnerRequiresLiveConnection(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	if err := reg.stores.PutNode(ctx, &NodeRecord{
		NodeID: "n1", BuildRegistrationCapacity: &routesync.BuildAdmissionLimit{Resources: &routesync.BuildResources{CPU: 2000}},
	}); err != nil {
		t.Fatal(err)
	}
	if node, found, err := reg.localNodeOwner.Runtime(ctx, "n1"); !errors.Is(err, ErrNodeGone) || found || node != nil {
		t.Fatalf("disconnected Runtime node=%+v found=%v err=%v", node, found, err)
	}
	reg.addNode(&fakeConn{nodeID: "n1"})
	if node, found, err := reg.localNodeOwner.Runtime(ctx, "n1"); err != nil || !found || node == nil {
		t.Fatalf("connected Runtime node=%+v found=%v err=%v", node, found, err)
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

func TestReserveBuildRequiresCanonicalResources(t *testing.T) {
	reg := testReg(t)
	_, err := reg.ReserveBuild(context.Background(), BuildReserveReq{Group: "/g", Profile: types.ProfileE2B})
	if err == nil || !strings.Contains(err.Error(), "resources are required") {
		t.Fatalf("missing resources error = %v", err)
	}
}

func TestBuildStoreUsesRouteLinkBuildShardAndDoesNotLeakToSandboxList(t *testing.T) {
	ctx := context.Background()
	view := clusterstate.MemberView{Version: 1, Members: []string{"r1", "r2", "r3"}}
	cluster := newShardStoreCluster(t, []string{"r1", "r2", "r3"}, 3, 1, 1, 1)
	stores := cluster["r1"]

	if err := stores.PutBuild(ctx, &BuildRecord{
		Group: "/g", BuildID: "bld-1", NodeID: "n1", Profile: types.ProfileBare, State: BuildRegistered,
		Resources: testWireBuildResources(), CreatedU: 123,
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
	if err != nil || !found || got.Profile != types.ProfileBare || got.Resources == nil || got.Resources.CPU != 2000 || got.CreatedU != 123 {
		t.Fatalf("GetBuildInGroup=%+v found=%v err=%v", got, found, err)
	}
}

func TestReserveBuildPinsConnectionLossToSameNode(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	reg.SetPlacer(placementWithToken("n1"))
	reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1"})
	reg.addNode(&fakeConn{nodeID: "n1", err: ErrNodeGone})
	req := BuildReserveReq{Group: "/g", BuildID: "bld-fixed", TemplateID: "transient-fixed", Profile: types.ProfileE2B, Resources: testWireBuildResources()}

	if res, err := reg.ReserveBuild(ctx, req); err == nil || res != nil {
		t.Fatalf("connection loss result=%+v err=%v, want pinned ambiguity", res, err)
	}
	if rec, found, err := reg.stores.GetBuildInGroup(ctx, "/g", req.BuildID); err != nil || !found || rec.State != BuildStarting || rec.NodeID != "n1" {
		t.Fatalf("pinned record=%+v found=%v err=%v", rec, found, err)
	}

	reg.addNode(buildAckConn(reg, "n1"))
	res, err := reg.ReserveBuild(ctx, req)
	if err != nil || res.NodeID != "n1" {
		t.Fatalf("same-node retry after reconnect: res=%+v err=%v", res, err)
	}
}

func TestReserveBuildReleasesAdmissionOnRejectedAck(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	reg.SetPlacer(placementWithToken("n1"))
	reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1"})
	reject := true
	reg.addNode(&fakeConn{nodeID: "n1", onCmd: func(cmd *routesync.Command) {
		if cmd.Kind != routesync.CmdBuildRegister {
			return
		}
		status, reason, httpStatus := routesync.AckAccepted, "", 0
		if reject {
			status, reason, httpStatus = routesync.AckRejected, "no budget", http.StatusTooManyRequests
		}
		go reg.ackCommand(&routesync.CmdAck{CmdID: cmd.CmdID, Status: status, Reason: reason, HTTPStatus: httpStatus})
	}})

	if _, err := reg.ReserveBuild(ctx, BuildReserveReq{Group: "/g", Profile: types.ProfileE2B, Resources: testWireBuildResources()}); err == nil {
		t.Fatal("rejected build_register should fail")
	} else if fmt.Sprint(err) == "" {
		t.Fatal("expected non-empty rejection error")
	}
	reject = false
	res, err := reg.ReserveBuild(ctx, BuildReserveReq{Group: "/g", Profile: types.ProfileE2B, Resources: testWireBuildResources()})
	if err != nil || res.NodeID != "n1" {
		t.Fatalf("admission lease was not released after rejected ack: res=%+v err=%v", res, err)
	}
}

func TestBuildRegistrationConflictIsDefinitive(t *testing.T) {
	if !definitiveBuildRegistrationRejection(&routesync.CmdAck{
		Status: routesync.AckRejected, HTTPStatus: http.StatusConflict,
	}) {
		t.Fatal("immutable node conflict was treated as ambiguous")
	}
	if definitiveBuildRegistrationRejection(&routesync.CmdAck{
		Status: routesync.AckRejected, HTTPStatus: http.StatusInternalServerError,
	}) {
		t.Fatal("node internal error was treated as definitive")
	}
}

func TestBuildRegistrationDefinitiveRejectionPreservesHTTPStatus(t *testing.T) {
	for _, status := range []int{
		http.StatusBadRequest,
		http.StatusConflict,
		http.StatusTooManyRequests,
	} {
		err := buildRegistrationRejectionError(&routesync.CmdAck{
			Status: routesync.AckRejected, HTTPStatus: status, Reason: "node rejected build registration",
		})
		if got := routeLinkStatus(err); got != status {
			t.Fatalf("routeLinkStatus(build rejection %d) = %d", status, got)
		}
		var rejected *nodeCommandRejection
		if !errors.As(err, &rejected) || rejected.reason != "node rejected build registration" {
			t.Fatalf("typed rejection for %d = %#v (%v)", status, rejected, err)
		}
	}
}
