package registry

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/cluster/metadata"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
)

func TestSandboxRouteRequiresValidProfile(t *testing.T) {
	stores := newTestStores(t)
	for _, profile := range []string{"", "other"} {
		record := testSandboxIdentityRecord("/g", "rk", "sb", "n1", StateReady)
		record.Profile = profile
		if _, err := stores.PutSandbox(context.Background(), record); err == nil {
			t.Fatalf("PutSandbox accepted profile %q", profile)
		}
	}
}

func TestNodeSandboxRefsRequireValidProfile(t *testing.T) {
	ctx := context.Background()
	stores := newTestStores(t)
	for _, profile := range []string{"", "unknown"} {
		ref := testNodeSandboxRef("/g", "rk", "sb-"+profile, profile, testAPIFingerprint)
		if err := addTestSandboxRef(t, stores, ctx, "n1", ref); err == nil {
			t.Fatalf("AddNodeSandboxRef accepted profile %q", profile)
		}
	}

	e2b := testNodeSandboxRef("/g", "rk", "sb", "e2b", testAPIFingerprint)
	if err := addTestSandboxRef(t, stores, ctx, "n1", e2b); err != nil {
		t.Fatal(err)
	}
	bare := e2b
	bare.Profile = "bare"
	if err := addTestSandboxRef(t, stores, ctx, "n1", bare); !errors.Is(err, errNodeSandboxIDConflict) {
		t.Fatalf("profile rebind error=%v, want %v", err, errNodeSandboxIDConflict)
	}
}

type invalidSandboxPage struct {
	metadata.OwnershipRepository
	ref metadata.NodeSandboxRef
}

func (p invalidSandboxPage) ListSandboxOwners(context.Context, string, metadata.Cursor, int) (metadata.Page[metadata.Versioned[metadata.NodeSandboxRef]], error) {
	return metadata.Page[metadata.Versioned[metadata.NodeSandboxRef]]{Items: []metadata.Versioned[metadata.NodeSandboxRef]{{Exists: true, Version: testVersion(1), Value: p.ref}}}, nil
}

func TestNodeSandboxSnapshotsRejectInvalidCredentialFingerprint(t *testing.T) {
	for name, fingerprint := range map[string]string{"missing": "", "short": "abcd", "uppercase": strings.ToUpper(testAPIFingerprint)} {
		t.Run(name, func(t *testing.T) {
			stores := newTestStores(t)
			seedTestNode(t, stores, "n1")
			stores.ownership = invalidSandboxPage{OwnershipRepository: stores.ownership, ref: testNodeSandboxRef("/g", "rk", "sb", "e2b", fingerprint)}
			if _, _, err := stores.GetNode(context.Background(), "n1"); err == nil {
				t.Fatal("node aggregate accepted an invalid ownership fingerprint")
			}
		})
	}
}

func TestTypedChildrenAssembleNodeView(t *testing.T) {
	ctx := context.Background()
	stores := newTestStores(t)
	profile := &NodeRecord{NodeID: "n1", Labels: map[string]string{"pool": "p"}, Capacity: 10, APIEndpoint: "node-api", DataEndpoint: "node-data", LastHeartbeatUnix: time.Now().Unix(), LinkOwner: "r1"}
	if err := stores.PutNode(ctx, profile); err != nil {
		t.Fatal(err)
	}
	if err := addTestSandboxRef(t, stores, ctx, "n1", testNodeSandboxRef("/g", "rk", "sb", "e2b", testAPIFingerprint)); err != nil {
		t.Fatal(err)
	}
	if err := stores.AddNodeBuildRef(ctx, "n1", clusterstate.NodeBuildRef{Group: "/g", BuildID: "b1"}); err != nil {
		t.Fatal(err)
	}
	pair := testNodeKeyPair(strings.Repeat("a", 64), strings.Repeat("b", 64), 123)
	if err := stores.UpsertNodeKeyPair(ctx, "n1", pair); err != nil {
		t.Fatal(err)
	}
	got, found, err := stores.GetNode(ctx, "n1")
	if err != nil || !found || got.APIEndpoint != profile.APIEndpoint || got.DataEndpoint != profile.DataEndpoint || got.Labels["pool"] != "p" || len(got.Sandboxes) != 1 || len(got.Builds) != 1 || len(got.KeyPairs) != 1 || !got.Version.IsCommitted() {
		t.Fatalf("node aggregate=%+v found=%v err=%v", got, found, err)
	}
	if err := stores.DropNodeKeyPair(ctx, "n1", pair.APISecretFingerprint); err != nil {
		t.Fatal(err)
	}
	got, found, err = stores.GetNode(ctx, "n1")
	if err != nil || !found || len(got.KeyPairs) != 0 || len(got.Sandboxes) != 1 || len(got.Builds) != 1 {
		t.Fatalf("node aggregate after key removal=%+v found=%v err=%v", got, found, err)
	}
}

func TestNodeReapSnapshotDoesNotDeleteNewChildRevisions(t *testing.T) {
	ctx := context.Background()
	stores := newTestStores(t)
	seedTestNode(t, stores, "n1")
	sandboxRef := testNodeSandboxRef("/g", "rk", "sb", "e2b", testAPIFingerprint)
	buildRef := metadata.NodeBuildRef{Group: "/g", BuildID: "build"}
	pair := testNodeKeyPair(strings.Repeat("a", 64), strings.Repeat("b", 64), 100)
	if err := addTestSandboxRef(t, stores, ctx, "n1", sandboxRef); err != nil {
		t.Fatal(err)
	}
	if err := stores.AddNodeBuildRef(ctx, "n1", buildRef); err != nil {
		t.Fatal(err)
	}
	if err := stores.UpsertNodeKeyPair(ctx, "n1", pair); err != nil {
		t.Fatal(err)
	}
	oldSandbox, err := stores.ownership.ReadSandboxOwner(ctx, "n1", sandboxRef.NodeSandboxID)
	if err != nil {
		t.Fatal(err)
	}
	oldBuild, err := stores.ownership.ReadBuildOwner(ctx, "n1", buildRef.BuildID)
	if err != nil {
		t.Fatal(err)
	}
	keys, err := stores.keyPairs.ListKeyPairs(ctx, "n1", "", 10)
	if err != nil || len(keys.Items) != 1 {
		t.Fatalf("key snapshot: %d %v", len(keys.Items), err)
	}
	// A refresh before the reap claim invalidates every captured child stamp,
	// even where the ownership identity itself remains exactly the same.
	if err := addTestSandboxRef(t, stores, ctx, "n1", sandboxRef); err != nil {
		t.Fatal(err)
	}
	if err := stores.AddNodeBuildRef(ctx, "n1", buildRef); err != nil {
		t.Fatal(err)
	}
	pair.ExpiresUnix = 200
	if err := stores.UpsertNodeKeyPair(ctx, "n1", pair); err != nil {
		t.Fatal(err)
	}
	profile, err := stores.nodes.ReadNodeProfile(ctx, "n1")
	if err != nil {
		t.Fatal(err)
	}
	claim, err := stores.nodes.BeginReap(ctx, "n1", profile.Version)
	if err != nil {
		t.Fatal(err)
	}
	for name, err := range map[string]error{
		"sandbox": stores.nodes.CleanupSandbox(ctx, claim, oldSandbox, metadata.Version{}, true),
		"build":   stores.nodes.CleanupBuild(ctx, claim, oldBuild, metadata.Version{}),
		"key":     stores.nodes.CleanupKeyPair(ctx, claim, keys.Items[0]),
	} {
		if !errors.Is(err, metadata.ErrConflict) {
			t.Fatalf("stale %s cleanup: %v", name, err)
		}
	}
	got, found, err := stores.GetNode(ctx, "n1")
	if err != nil || !found || len(got.Sandboxes) != 1 || len(got.Builds) != 1 || len(got.KeyPairs) != 1 || got.KeyPairs[0].ExpiresUnix != 200 {
		t.Fatalf("stale cleanup removed refreshed child: found=%v err=%v", found, err)
	}
}

func TestProtectedRouteCredentialsStayInsideInternalStoreReads(t *testing.T) {
	ctx := context.Background()
	stores := newTestStores(t)
	credentials := &sandboxcfg.Credentials{ServiceSecret: strings.Repeat("1", 64), EnvdAccessToken: "envd-private", TrafficAccessToken: "traffic-private"}
	route := testE2BSandboxRecord("/g", "rk", "sb", "n1", StateReserved)
	route.CreateCredentials = credentials
	if _, err := stores.PutSandbox(ctx, route); err != nil {
		t.Fatal(err)
	}
	stored, _, found, err := stores.GetSandbox(ctx, route.Group, route.RouteKey)
	if err != nil || !found || stored.CreateCredentials == nil || *stored.CreateCredentials != *credentials || !sameRouteCredentials(stored, route) {
		t.Fatalf("internal route lost credential material: found=%v err=%v", found, err)
	}
	empty := testSandboxIdentityRecord("/g", "empty", "sb-empty", "n1", StateReserved)
	empty.CreateCredentials = &sandboxcfg.Credentials{}
	if _, err := stores.PutSandbox(ctx, empty); err != nil {
		t.Fatal(err)
	}
	stored, _, found, err = stores.GetSandbox(ctx, empty.Group, empty.RouteKey)
	if err != nil || !found || stored.CreateCredentials == nil || *stored.CreateCredentials != (sandboxcfg.Credentials{}) {
		t.Fatalf("explicit empty credentials lost: found=%v err=%v", found, err)
	}
	if err := stores.RangeSandboxes(ctx, "/g", func(record *SandboxRecord) error {
		encoded, err := json.Marshal(record)
		if err != nil {
			return err
		}
		var projected map[string]json.RawMessage
		if err := json.Unmarshal(encoded, &projected); err != nil {
			return err
		}
		for _, field := range []string{"api_secret_fingerprint", "manifest_key_fingerprint", "create_credentials", "stable_id", "api_secret", "service_secret", "envd_access_token", "traffic_access_token", "forward_access_token"} {
			if _, found := projected[field]; found {
				t.Fatalf("external route list exposed protected field %q", field)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestNodeObjectRefsAreScopedByNodeID(t *testing.T) {
	ctx := context.Background()
	stores := newTestStores(t)
	n1Ref := testNodeSandboxRef("/g1", "rk1", "same", "e2b", testAPIFingerprint)
	if err := addTestSandboxRef(t, stores, ctx, "n1", n1Ref); err != nil {
		t.Fatal(err)
	}
	if err := addTestSandboxRef(t, stores, ctx, "n2", testNodeSandboxRef("/g2", "rk2", "same", "e2b", testAPIFingerprint)); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		node, group string
	}{
		{node: "n1", group: "/g1"},
		{node: "n2", group: "/g2"},
	} {
		ref, found, err := stores.GetNodeSandboxRef(ctx, tc.node, EncodeNodeSandboxID("same", 0))
		if err != nil || !found || ref.Group != tc.group {
			t.Fatalf("node %s ref=%+v found=%v err=%v", tc.node, ref, found, err)
		}
	}
	if err := addTestSandboxRef(t, stores, ctx, "n1", n1Ref); err != nil {
		t.Fatalf("idempotent same-node sandbox ref: %v", err)
	}
	if err := addTestSandboxRef(t, stores, ctx, "n1", testNodeSandboxRef("/other", "other", "same", "e2b", testAPIFingerprint)); !errors.Is(err, errNodeSandboxIDConflict) {
		t.Fatalf("same-node ownership collision err=%v", err)
	}
	if ref, found, err := stores.GetNodeSandboxRef(ctx, "n1", n1Ref.NodeSandboxID); err != nil || !found || ref != n1Ref {
		t.Fatalf("original sandbox ref=%+v found=%v err=%v", ref, found, err)
	}
	if err := addTestSandboxRef(t, stores, ctx, "n1", clusterstate.NodeSandboxRef{Profile: "e2b", Group: "/g"}); err == nil {
		t.Fatal("invalid sandbox ref was accepted")
	}
	if err := stores.AddNodeBuildRef(ctx, "n1", clusterstate.NodeBuildRef{BuildID: "b"}); err == nil {
		t.Fatal("invalid build ref was accepted")
	}
}

func TestTypedRouteAndBuildNamespacesAreSeparate(t *testing.T) {
	ctx := context.Background()
	stores := newTestStores(t)
	if _, err := stores.PutSandbox(ctx, testSandboxIdentityRecord("/g", "rk", "sb", "n1", StateReady)); err != nil {
		t.Fatalf("PutSandbox: %v", err)
	}
	if _, err := stores.putBuild(ctx, &BuildRecord{Profile: "e2b", APISecretFingerprint: testAPIFingerprint, Group: "/g", BuildID: "b1", NodeID: "n1", State: BuildRegistered}); err != nil {
		t.Fatalf("putBuild: %v", err)
	}
	route, _, found, err := stores.GetSandbox(ctx, "/g", "rk")
	if err != nil || !found || route.SandboxID != "sb" || route.NodeSandboxID != EncodeNodeSandboxID("sb", 0) {
		t.Fatalf("GetSandbox route=%+v found=%v err=%v", route, found, err)
	}
	build, _, found, err := stores.getBuild(ctx, "/g", "b1")
	if err != nil || !found || build.BuildID != "b1" {
		t.Fatalf("getBuild build=%+v found=%v err=%v", build, found, err)
	}
	var routes []string
	if err := stores.RangeSandboxes(ctx, "/g", func(r *SandboxRecord) error {
		routes = append(routes, r.RouteKey)
		return nil
	}); err != nil {
		t.Fatalf("RangeSandboxes: %v", err)
	}
	if len(routes) != 1 || routes[0] != "rk" {
		t.Fatalf("routes=%v, want [rk]", routes)
	}
	var builds []string
	if err := stores.RangeBuildsInGroup(ctx, "/g", func(b *BuildRecord) error {
		builds = append(builds, b.BuildID)
		return nil
	}); err != nil {
		t.Fatalf("RangeBuildsInGroup: %v", err)
	}
	if len(builds) != 1 || builds[0] != "b1" {
		t.Fatalf("builds=%v, want [b1]", builds)
	}
}

func TestClusterStoresPlacerLinkSourceCursorUsesLeaseFencing(t *testing.T) {
	ctx := context.Background()
	stores := newTestStores(t)

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

func TestRoutingNodeOwnerUsesLinkOwner(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	if err := putTestNode(reg.stores, ctx, &NodeRecord{NodeID: "n1", LinkOwner: "remote"}); err != nil {
		t.Fatal(err)
	}
	remote := &routingNodeOwnerRecorder{allow: true}
	reg.SetRemoteNodeOwners(map[string]NodeOwner{"remote": remote})

	pair := testNodeKeyPair(strings.Repeat("a", 64), strings.Repeat("b", 64), 123)
	if err := reg.nodeOwner.PutKeyPair(ctx, "n1", pair); err != nil {
		t.Fatalf("PutKeyPair: %v", err)
	}
	if err := reg.nodeOwner.DeleteSandbox(ctx, "n1", "sb1", pair.APISecretFingerprint); err != nil {
		t.Fatalf("DeleteSandbox: %v", err)
	}

	if len(remote.keys) != 1 || remote.keys[0] != "n1/"+pair.APISecretFingerprint {
		t.Fatalf("remote keys=%v", remote.keys)
	}
	if len(remote.deleted) != 1 || remote.deleted[0] != "n1/sb1" {
		t.Fatalf("remote deleted=%v", remote.deleted)
	}
}

type profileReadUnavailable struct{ metadata.NodeRepository }

func (profileReadUnavailable) ReadNodeProfile(context.Context, string) (metadata.NodeSnapshot, error) {
	return metadata.NodeSnapshot{}, metadata.ErrUnavailable
}

type fixedConnectionOwners struct{ members []string }

func (fixedConnectionOwners) LocalID() string { return "registry" }
func (o fixedConnectionOwners) Candidates(context.Context, string) ([]string, error) {
	return append([]string(nil), o.members...), nil
}

func TestRoutingNodeOwnerUsesConnectedCandidateWhenProfileUnavailable(t *testing.T) {
	for _, unavailable := range []bool{false, true} {
		name := "missing_profile"
		if unavailable {
			name = "read_unavailable"
		}
		t.Run(name, func(t *testing.T) {
			reg := testReg(t)
			reg.stores.owners = fixedConnectionOwners{members: []string{"registry", "remote"}}
			if unavailable {
				reg.stores.nodes = profileReadUnavailable{reg.stores.nodes}
			}
			remote := &routingNodeOwnerRecorder{}
			reg.SetRemoteNodeOwners(map[string]NodeOwner{"remote": remote})
			if err := reg.nodeOwner.Connected(context.Background(), "n1"); err != nil {
				t.Fatal(err)
			}
			if err := reg.nodeOwner.SendCommand(context.Background(), "n1", &routesync.Command{Kind: routesync.CmdDelete, SID: "sb1"}); err != nil {
				t.Fatal(err)
			}
			if len(remote.connected) == 0 || len(remote.deleted) != 1 || remote.deleted[0] != "n1/"+routesync.CmdDelete {
				t.Fatalf("connection candidates did not reach remote owner: probes=%v commands=%v", remote.connected, remote.deleted)
			}
		})
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

func (r *routingNodeOwnerRecorder) PutKeyPair(ctx context.Context, nodeID string, pair clusterstate.NodeKeyPair) error {
	r.keys = append(r.keys, nodeID+"/"+pair.APISecretFingerprint)
	return nil
}

func (r *routingNodeOwnerRecorder) DropKeyPair(ctx context.Context, nodeID, apiSecretFingerprint string) error {
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

func (r *routingNodeOwnerRecorder) DeleteSandbox(ctx context.Context, nodeID, sid, apiSecretFingerprint string) error {
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
