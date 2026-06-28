package registry

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	clusterstate "github.com/kuasar-sandbox/sandbox-orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/clusterstore"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/routesync"
)

func testRegWithBox(t *testing.T) *Registry {
	t.Helper()
	kv := clusterstore.OpenMemory(0)
	t.Cleanup(func() { kv.Close() })
	return New(NewStores(kv), nil, 5*time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

const testMK = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"
const testAuthKey = "ffeeddccbbaa99887766554433221100ffeeddccbbaa99887766554433221100"

type placementFunc func(context.Context, PlaceRequest) (*Placement, error)

func (f placementFunc) Place(ctx context.Context, req PlaceRequest) (*Placement, error) {
	return f(ctx, req)
}

func placementWithToken(nodeID string) Placer {
	return placementFunc(func(ctx context.Context, req PlaceRequest) (*Placement, error) {
		tok := ""
		if req.SandboxID != "" {
			var err error
			tok, err = clusterstate.DeriveAccessToken(testAuthKey, req.SandboxID)
			if err != nil {
				return nil, err
			}
		}
		return &Placement{
			NodeID: nodeID, TemplateRef: "e2b-snp-tmpl", Config: mergeConfig(map[string]string{"a": "1"}, req.Config),
			KeyFingerprint: keyFingerprint(testMK), AccessToken: tok, ImageRepo: "repo", RegistryAuth: "auth-json",
		}, nil
	})
}

func TestKeyPredistribution(t *testing.T) {
	ctx := context.Background()
	reg := testRegWithBox(t)
	reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1", Labels: map[string]string{"zone": "east"}})
	var cmds []*routesync.Command
	reg.addNode(&fakeConn{nodeID: "n1", onCmd: func(c *routesync.Command) { cmds = append(cmds, c) }})

	pushKeyAllocation(reg, "/g", []string{"n1"}, testMK)
	reg.reconcileKeys(ctx)

	if len(cmds) != 1 || cmds[0].Kind != routesync.CmdKeyPut || cmds[0].ManifestKey != testMK {
		t.Fatalf("expected one key_put with the group key, got %+v", cmds)
	}
}

func TestKeyDropOnLeave(t *testing.T) {
	ctx := context.Background()
	reg := testRegWithBox(t)
	reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1", Labels: map[string]string{"zone": "east"}})
	var cmds []*routesync.Command
	reg.addNode(&fakeConn{nodeID: "n1", onCmd: func(c *routesync.Command) { cmds = append(cmds, c) }})

	pushKeyAllocation(reg, "/g", []string{"n1"}, testMK)
	reg.reconcileKeys(ctx)
	pushKeyAllocation(reg, "/g", nil, "")
	cmds = nil
	reg.reconcileKeys(ctx)

	if len(cmds) != 1 || cmds[0].Kind != routesync.CmdKeyDrop {
		t.Fatalf("expected one key_drop after the scaler removed allocation, got %+v", cmds)
	}
}

func TestKeyDistributionUsesScalerAllocation(t *testing.T) {
	ctx := context.Background()
	reg := testRegWithBox(t)
	reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1", Labels: map[string]string{"zone": "east"}})
	reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n2", Labels: map[string]string{"zone": "west"}})
	cmds := map[string][]*routesync.Command{}
	reg.addNode(&fakeConn{nodeID: "n1", onCmd: func(c *routesync.Command) { cmds["n1"] = append(cmds["n1"], c) }})
	reg.addNode(&fakeConn{nodeID: "n2", onCmd: func(c *routesync.Command) { cmds["n2"] = append(cmds["n2"], c) }})

	pushKeyAllocation(reg, "/g", []string{"n2"}, testMK)
	reg.reconcileKeys(ctx)

	if len(cmds["n1"]) != 0 {
		t.Fatalf("selector-matching n1 got commands despite scaler allocation to n2: %+v", cmds["n1"])
	}
	if len(cmds["n2"]) != 1 || cmds["n2"][0].Kind != routesync.CmdKeyPut {
		t.Fatalf("n2 commands=%+v, want one key_put", cmds["n2"])
	}

	cmds = map[string][]*routesync.Command{}
	pushKeyAllocation(reg, "/g", nil, "")
	reg.reconcileKeys(ctx)
	if len(cmds["n2"]) != 1 || cmds["n2"][0].Kind != routesync.CmdKeyDrop {
		t.Fatalf("empty scaler allocation should drop n2 key, commands=%+v", cmds["n2"])
	}
}

func TestKeyDistributionDropsDeletedGroupAndRotatedKey(t *testing.T) {
	ctx := context.Background()
	reg := testRegWithBox(t)
	reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1"})
	var cmds []*routesync.Command
	reg.addNode(&fakeConn{nodeID: "n1", onCmd: func(c *routesync.Command) { cmds = append(cmds, c) }})

	pushKeyAllocation(reg, "/g", []string{"n1"}, testMK)
	reg.reconcileKeys(ctx)
	if len(cmds) != 1 || cmds[0].Kind != routesync.CmdKeyPut {
		t.Fatalf("initial commands=%+v, want key_put", cmds)
	}

	cmds = nil
	pushKeyAllocation(reg, "/g", []string{"n1"}, testAuthKey)
	reg.reconcileKeys(ctx)
	if len(cmds) != 2 || cmds[0].Kind != routesync.CmdKeyDrop || cmds[1].Kind != routesync.CmdKeyPut {
		t.Fatalf("rotated key commands=%+v, want key_drop old + key_put new", cmds)
	}

	cmds = nil
	pushKeyAllocation(reg, "/g", nil, "")
	reg.reconcileKeys(ctx)
	if len(cmds) != 1 || cmds[0].Kind != routesync.CmdKeyDrop {
		t.Fatalf("deleted group commands=%+v, want key_drop", cmds)
	}
}

func testReg(t *testing.T) *Registry {
	t.Helper()
	kv := clusterstore.OpenMemory(0)
	t.Cleanup(func() { kv.Close() })
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	return New(NewStores(kv), nil, 5*time.Second, log)
}

func pushKeyAllocation(reg *Registry, group string, nodes []string, manifestKey string) {
	patch := &routesync.SelectorPatch{Group: group, NodeIDs: nodes, NodeAllocation: true}
	if manifestKey != "" {
		patch.KeyFingerprint = keyFingerprint(manifestKey)
		patch.ManifestKeyType = clusterstate.SecretInline
		patch.ManifestKey = manifestKey
	}
	reg.applySelectorPatch(patch)
}

// fakeConn implements nodeConn; its onCmd hook lets a test simulate the node
// reacting to a command (e.g. reporting the sandbox running).
type fakeConn struct {
	nodeID string
	onCmd  func(*routesync.Command)
	err    error
}

func (c *fakeConn) id() string { return c.nodeID }
func (c *fakeConn) send(cmd *routesync.Command) error {
	if c.err != nil {
		return c.err
	}
	if c.onCmd != nil {
		c.onCmd(cmd)
	}
	return nil
}

func TestReserveSandboxCreateFlow(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	reg.SetPlacer(placementWithToken("n1"))
	if err := reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1"}); err != nil {
		t.Fatal(err)
	}

	// The fake node, on create, reports the sandbox running (the node is the
	// route authority; the registry's Reserve waits on this).
	conn := &fakeConn{nodeID: "n1"}
	conn.onCmd = func(cmd *routesync.Command) {
		if cmd.Kind != routesync.CmdCreate {
			return
		}
		go reg.applyRoute(context.Background(), "n1", &routesync.RouteEntry{
			SandboxID: cmd.SID, Group: cmd.Group, RouteKey: cmd.RouteKey,
			State: routesync.StateRunning,
		})
	}
	reg.addNode(conn)

	res, err := reg.ReserveSandbox(ctx, "/c/p/a/g1", "u1:s1", nil)
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	want, err := clusterstate.DeriveAccessToken(testAuthKey, res.SID)
	if err != nil {
		t.Fatal(err)
	}
	if res.NodeID != "n1" || res.SID == "" || res.AccessToken != want {
		t.Fatalf("reserve result: %+v", res)
	}

	// The sandbox is now READY; a second reserve for the same key returns it
	// directly (session affinity) without re-placing.
	res2, err := reg.ReserveSandbox(ctx, "/c/p/a/g1", "u1:s1", nil)
	if err != nil {
		t.Fatalf("re-reserve: %v", err)
	}
	if res2.SID != res.SID || res2.NodeID != "n1" {
		t.Fatalf("re-reserve mismatch: %+v vs %+v", res2, res)
	}

	// SandboxStore reflects READY keyed by (group, route_key).
	rec, _, found, err := reg.stores.GetSandbox(ctx, "/c/p/a/g1", "u1:s1")
	if err != nil || !found || rec.State != StateReady {
		t.Fatalf("stored record: %+v found=%v err=%v", rec, found, err)
	}
}

func TestReserveSandboxCreateUsesDerivedAccessToken(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	reg.SetPlacer(placementWithToken("n1"))
	if err := reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1"}); err != nil {
		t.Fatal(err)
	}

	var commandToken string
	conn := &fakeConn{nodeID: "n1"}
	conn.onCmd = func(cmd *routesync.Command) {
		if cmd.Kind != routesync.CmdCreate {
			return
		}
		commandToken = cmd.AccessToken
		go reg.applyRoute(context.Background(), "n1", &routesync.RouteEntry{
			SandboxID: cmd.SID, Group: cmd.Group, RouteKey: cmd.RouteKey,
			State: routesync.StateRunning, AccessToken: cmd.AccessToken,
		})
	}
	reg.addNode(conn)

	res, err := reg.ReserveSandbox(ctx, "/g", "u1:s1", nil)
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	want, err := clusterstate.DeriveAccessToken(testAuthKey, res.SID)
	if err != nil {
		t.Fatal(err)
	}
	if commandToken != want || res.AccessToken != want {
		t.Fatalf("derived access token mismatch command=%q result=%q want=%q", commandToken, res.AccessToken, want)
	}
}

func TestReadyRouteUsesDerivedAccessToken(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1"})
	reg.addNode(&fakeConn{nodeID: "n1"})
	want, err := clusterstate.DeriveAccessToken(testAuthKey, "sb-ready")
	if err != nil {
		t.Fatal(err)
	}
	reg.stores.PutSandbox(ctx, &SandboxRecord{Group: "/g", RouteKey: "rk", SID: "sb-ready", State: StateReady, NodeID: "n1", AccessToken: want})
	reg.indexSID("sb-ready", "/g", "rk")

	res, err := reg.ReserveSandbox(ctx, "/g", "rk", nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.AccessToken != want {
		t.Fatalf("ready reserve token=%q, want derived %q", res.AccessToken, want)
	}
	rr, found, err := reg.ResolveSID(ctx, "/g", "rk", "sb-ready")
	if err != nil || !found {
		t.Fatalf("resolve found=%v err=%v", found, err)
	}
	if rr.AccessToken != want {
		t.Fatalf("resolve token=%q, want derived %q", rr.AccessToken, want)
	}
	if _, found, err := reg.ResolveSID(ctx, "/other", "rk", "sb-ready"); err != nil || found {
		t.Fatalf("wrong-group resolve found=%v err=%v", found, err)
	}
	if _, found, err := reg.ResolveSID(ctx, "/g", "other-rk", "sb-ready"); err != nil || found {
		t.Fatalf("wrong-route-key resolve found=%v err=%v", found, err)
	}
}

func TestReserveSandboxCreateUsesPlacementMaterial(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	reg.SetPlacer(placementWithToken("n1"))
	if err := reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1"}); err != nil {
		t.Fatal(err)
	}
	var got *routesync.Command
	conn := &fakeConn{nodeID: "n1"}
	conn.onCmd = func(cmd *routesync.Command) {
		if cmd.Kind != routesync.CmdCreate {
			return
		}
		cp := *cmd
		got = &cp
		go reg.applyRoute(context.Background(), "n1", &routesync.RouteEntry{
			SandboxID: cmd.SID, Group: cmd.Group, RouteKey: cmd.RouteKey,
			State: routesync.StateRunning, AccessToken: cmd.AccessToken,
		})
	}
	reg.addNode(conn)

	if _, err := reg.ReserveSandbox(ctx, "/g", "rk", map[string]string{"b": "2"}); err != nil {
		t.Fatal(err)
	}
	if got == nil {
		t.Fatal("create command not sent")
	}
	if got.TemplateRef != "e2b-snp-tmpl" || got.Config["a"] != "1" || got.Config["b"] != "2" {
		t.Fatalf("create command did not use placement config: %+v", got)
	}
	if got.KeyFingerprint != keyFingerprint(testMK) {
		t.Fatalf("key fingerprint=%q, want provider key fp", got.KeyFingerprint)
	}
}

func TestReserveSandboxNoNode(t *testing.T) {
	reg := testReg(t)
	if _, err := reg.ReserveSandbox(context.Background(), "/c/p/a/g1", "u1:s1", nil); err == nil {
		t.Fatal("expected error with no nodes")
	}
}

func TestSendAndWaitAck(t *testing.T) {
	reg := testReg(t)
	conn := &fakeConn{nodeID: "n1", onCmd: func(cmd *routesync.Command) {
		go reg.ackCommand(&routesync.CmdAck{CmdID: cmd.CmdID, Status: routesync.AckAccepted})
	}}
	ack, err := reg.sendAndWait(context.Background(), conn, &routesync.Command{CmdID: "c1", Kind: routesync.CmdKeyPut}, 2*time.Second)
	if err != nil {
		t.Fatalf("sendAndWait: %v", err)
	}
	if ack.Status != routesync.AckAccepted {
		t.Fatalf("ack: %+v", ack)
	}
}

func TestSendAndWaitTimeout(t *testing.T) {
	reg := testReg(t)
	conn := &fakeConn{nodeID: "n1"} // never acks
	if _, err := reg.sendAndWait(context.Background(), conn, &routesync.Command{CmdID: "c2", Kind: routesync.CmdKeyPut}, 100*time.Millisecond); err == nil {
		t.Fatal("expected timeout waiting for an ack")
	}
}

func TestCreateRejectFastFails(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1"})
	conn := &fakeConn{nodeID: "n1", onCmd: func(cmd *routesync.Command) {
		if cmd.Kind == routesync.CmdCreate {
			go reg.ackCommand(&routesync.CmdAck{CmdID: cmd.CmdID, Status: routesync.AckRejected, Reason: "bad template"})
		}
	}}
	reg.addNode(conn)

	start := time.Now()
	_, err := reg.ReserveSandbox(ctx, "/g", "rk", nil)
	if err == nil {
		t.Fatal("expected reserve to fail on a rejected create")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("reserve took %v; expected fast-fail on reject (park_timeout is 5s)", elapsed)
	}
}

func TestDeadReportDeletesRoute(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	reg.stores.PutSandbox(ctx, &SandboxRecord{Group: "/g", RouteKey: "rk", SID: "sb-dead", State: StateReady, NodeID: "n1"})
	reg.indexSID("sb-dead", "/g", "rk")

	reg.applyRoute(ctx, "n1", &routesync.RouteEntry{
		SandboxID: "sb-dead", Group: "/g", RouteKey: "rk", State: routesync.StateDead,
	})

	if _, _, found, _ := reg.stores.GetSandbox(ctx, "/g", "rk"); found {
		t.Fatal("dead report should delete route")
	}
	reg.mu.Lock()
	_, indexed := reg.sidKeys["sb-dead"]
	reg.mu.Unlock()
	if indexed {
		t.Fatal("dead report left sid index behind")
	}
}

func TestOrphanRouteReportDeletesSandboxOnNode(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	var cmds []*routesync.Command
	reg.addNode(&fakeConn{nodeID: "n1", onCmd: func(cmd *routesync.Command) {
		cmds = append(cmds, cmd)
	}})

	reg.applyRoute(ctx, "n1", &routesync.RouteEntry{
		SandboxID: "sb-orphan", Group: "/g", RouteKey: "rk", State: routesync.StateRunning,
	})

	if len(cmds) != 1 || cmds[0].Kind != routesync.CmdDelete || cmds[0].SID != "sb-orphan" {
		t.Fatalf("orphan delete cmds=%+v", cmds)
	}
	if _, _, found, _ := reg.stores.GetSandbox(ctx, "/g", "rk"); found {
		t.Fatal("orphan route report should not create route_link record")
	}
}

func TestParkTimeoutRollback(t *testing.T) {
	ctx := context.Background()
	kv := clusterstore.OpenMemory(0)
	defer kv.Close()
	reg := New(NewStores(kv), nil, 200*time.Millisecond, slog.New(slog.NewTextHandler(io.Discard, nil)))
	reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1"})
	reg.addNode(&fakeConn{nodeID: "n1"}) // accepts create but never reports running

	_, err := reg.ReserveSandbox(ctx, "/g", "rk", nil)
	if err == nil {
		t.Fatal("expected park timeout error")
	}
	// The RESERVED record (never reached READY) must be rolled back, not stranded.
	if _, _, found, _ := reg.stores.GetSandbox(ctx, "/g", "rk"); found {
		t.Fatal("RESERVED record stranded after park timeout (not rolled back)")
	}
}

func TestReplaceOnRejectSucceeds(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1"})
	creates := 0
	conn := &fakeConn{nodeID: "n1"}
	conn.onCmd = func(cmd *routesync.Command) {
		if cmd.Kind != routesync.CmdCreate {
			return
		}
		creates++
		if creates == 1 {
			go reg.ackCommand(&routesync.CmdAck{CmdID: cmd.CmdID, Status: routesync.AckRejected, Reason: "transient"})
			return
		}
		go reg.applyRoute(context.Background(), "n1", &routesync.RouteEntry{
			SandboxID: cmd.SID, Group: cmd.Group, RouteKey: cmd.RouteKey, State: routesync.StateRunning, AccessToken: "tok",
		})
	}
	reg.addNode(conn)

	res, err := reg.ReserveSandbox(ctx, "/g", "rk", nil)
	if err != nil {
		t.Fatalf("reserve should succeed after one re-place: %v", err)
	}
	if res.NodeID != "n1" || creates != 2 {
		t.Fatalf("expected 2 creates (reject then re-place success); got %d creates, res=%+v", creates, res)
	}
}

func TestReservePausedResume(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1"})
	want, err := clusterstate.DeriveAccessToken(testAuthKey, "sb-x")
	if err != nil {
		t.Fatal(err)
	}
	// Seed a PAUSED sandbox on n1.
	reg.stores.PutSandbox(ctx, &SandboxRecord{Group: "/g", RouteKey: "rk", SID: "sb-x", State: StatePaused, NodeID: "n1", AccessToken: want})

	conn := &fakeConn{nodeID: "n1"}
	conn.onCmd = func(cmd *routesync.Command) {
		if cmd.Kind != routesync.CmdConnect {
			return
		}
		go reg.applyRoute(context.Background(), "n1", &routesync.RouteEntry{
			SandboxID: "sb-x", Group: "/g", RouteKey: "rk", State: routesync.StateRunning,
		})
	}
	reg.addNode(conn)

	res, err := reg.ReserveSandbox(ctx, "/g", "rk", nil)
	if err != nil {
		t.Fatalf("resume reserve: %v", err)
	}
	if res.SID != "sb-x" || res.AccessToken != want {
		t.Fatalf("resume result: %+v", res)
	}
}

func TestNodeListWatchProjectsLowFrequencyFields(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	lastBeat := time.Now().Unix()
	reg.stores.PutNode(ctx, &NodeRecord{
		NodeID: "n1", Labels: map[string]string{"pool": "p"}, Capacity: 10,
		BuildCapacity: &routesync.BuildResources{CPU: 2000},
		DataEndpoint:  "10.0.0.1:8443", RuntimeDigest: "rt1", LastHeartbeatUnix: lastBeat,
		Allocated: 99, Pool: 100, Counts: 7, BuildAlloc: &routesync.BuildResources{CPU: 1000},
	})

	mux := http.NewServeMux()
	reg.ServeScaleLink(mux)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	resp, err := http.Get(srv.URL + ScaleLinkNodeListWatchPath)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if rs := readViewFrame(t, resp.Body); rs.Type != "reset" {
		t.Fatalf("expected reset, got %+v", rs)
	}
	put := readViewFrame(t, resp.Body)
	if put.Type != "put" || put.Key != "n1" {
		t.Fatalf("node_list put: %+v", put)
	}
	var raw map[string]any
	if err := json.Unmarshal(put.Value, &raw); err != nil {
		t.Fatal(err)
	}
	if raw["node_id"] != "n1" || raw["data_endpoint"] == "" || raw["runtime_digest"] != "rt1" || raw["last_heartbeat_unix"] == nil {
		t.Fatalf("node_list value = %v", raw)
	}
	if _, ok := raw["build_capacity"].(map[string]any); !ok {
		t.Fatalf("node_list missing build_capacity: %v", raw)
	}
	for _, field := range []string{"allocated", "pool", "counts", "build_alloc"} {
		if _, ok := raw[field]; ok {
			t.Fatalf("node_list exposed high-frequency field %q: %v", field, raw)
		}
	}
	if bm := readViewFrame(t, resp.Body); bm.Type != "bookmark" {
		t.Fatalf("expected bookmark, got %+v", bm)
	}
}

func TestNodeListWatchIgnoresHeartbeatWatermarks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reg := testReg(t)
	now := time.Now().Unix()
	if err := reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1", LastHeartbeatUnix: now}); err != nil {
		t.Fatal(err)
	}
	rev, err := reg.stores.NodeListRev(ctx)
	if err != nil {
		t.Fatal(err)
	}
	ch, err := reg.stores.WatchNodeList(ctx, rev)
	if err != nil {
		t.Fatal(err)
	}

	reg.updateHeartbeat(ctx, "n1", &routesync.Heartbeat{Allocated: 100, Pool: 200, Counts: 5})
	select {
	case ev := <-ch:
		t.Fatalf("watermark-only heartbeat emitted node_list event: %+v", ev)
	case <-time.After(100 * time.Millisecond):
	}

	reg.updateHeartbeat(ctx, "n1", &routesync.Heartbeat{Allocated: 100, Pool: 200, Counts: 5, Draining: true})
	select {
	case ev := <-ch:
		if ev.Type != clusterstore.EventPut || ev.Key != "n1" {
			t.Fatalf("draining update event=%+v", ev)
		}
		var raw map[string]any
		if err := json.Unmarshal(ev.Value, &raw); err != nil {
			t.Fatal(err)
		}
		if raw["draining"] != true {
			t.Fatalf("draining update value=%v", raw)
		}
	case <-time.After(time.Second):
		t.Fatal("draining heartbeat did not emit node_list event")
	}
}

func TestNodeLinkResumeTokenReturnedOnReconnect(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	mux := http.NewServeMux()
	mux.HandleFunc(routesync.NodeLinkPath, reg.ServeNodeLink)
	srv := httptest.NewServer(h2c.NewHandler(mux, &http2.Server{}))
	defer srv.Close()
	addr := srv.Listener.Addr().String()

	open := func(bookmark string) string {
		t.Helper()
		tr := &http2.Transport{AllowHTTP: true}
		tr.DialTLSContext = func(ctx context.Context, _, _ string, _ *tls.Config) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "tcp", addr)
		}
		defer tr.CloseIdleConnections()
		pr, pw := io.Pipe()
		defer pw.Close()
		req, err := http.NewRequestWithContext(ctx, http.MethodPut, "http://registry"+routesync.NodeLinkPath, pr)
		if err != nil {
			t.Fatal(err)
		}
		errc := make(chan error, 1)
		go func() {
			errc <- routesync.WriteMsg(pw, &routesync.Msg{Type: routesync.TypeNodeRegister, NodeReg: &routesync.NodeRegister{NodeID: "n1"}})
		}()
		resp, err := tr.RoundTrip(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if err := <-errc; err != nil {
			t.Fatal(err)
		}
		msg, err := routesync.ReadMsg(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		if bookmark != "" {
			if err := routesync.WriteMsg(pw, &routesync.Msg{Type: routesync.TypeBookmark, RevToken: bookmark}); err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(3 * time.Second)
			for {
				node, found, _ := reg.stores.GetNode(ctx, "n1")
				got := ""
				if found {
					got = node.ResumeToken
				}
				if got == bookmark {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("registry did not record resume token %q", bookmark)
				}
				time.Sleep(10 * time.Millisecond)
			}
		}
		if msg.Type != routesync.TypeHello || msg.Hello == nil {
			t.Fatalf("first frame=%+v, want hello", msg)
		}
		return msg.Hello.ResumeFrom
	}

	if got := open("node-fp:7"); got != "" {
		t.Fatalf("first connect resume_from=%q, want empty", got)
	}
	if got := open(""); got != "node-fp:7" {
		t.Fatalf("reconnect resume_from=%q, want node-fp:7", got)
	}
}

func readViewFrame(t *testing.T, r io.Reader) *ViewEvent {
	t.Helper()
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, binary.LittleEndian.Uint32(hdr[:]))
	if _, err := io.ReadFull(r, buf); err != nil {
		t.Fatal(err)
	}
	var ev ViewEvent
	if err := json.Unmarshal(buf, &ev); err != nil {
		t.Fatal(err)
	}
	return &ev
}

func TestSandboxKeyNoCollision(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	// Nested groups: a per-group range over /a must NOT bleed into /a/b.
	reg.stores.PutSandbox(ctx, &SandboxRecord{Group: "/a", RouteKey: "x", SID: "sb-a", State: StateReady})
	reg.stores.PutSandbox(ctx, &SandboxRecord{Group: "/a/b", RouteKey: "y", SID: "sb-ab", State: StateReady})
	var got []string
	reg.stores.RangeSandboxes(ctx, "/a", func(s *SandboxRecord) error { got = append(got, s.SID); return nil })
	if len(got) != 1 || got[0] != "sb-a" {
		t.Fatalf("RangeSandboxes(/a) leaked across groups: %v (want [sb-a])", got)
	}
	// Aliasing across the group/route_key boundary must not overwrite:
	// (/a, "b/y") and (/a/b, "y") must be distinct records.
	reg.stores.PutSandbox(ctx, &SandboxRecord{Group: "/a", RouteKey: "b/y", SID: "sb-1", State: StateReady})
	r1, _, _, _ := reg.stores.GetSandbox(ctx, "/a", "b/y")
	rab, _, _, _ := reg.stores.GetSandbox(ctx, "/a/b", "y")
	if r1 == nil || r1.SID != "sb-1" || rab == nil || rab.SID != "sb-ab" {
		t.Fatalf("key aliasing overwrote a different tenant: (/a,b/y)=%v (/a/b,y)=%v", r1, rab)
	}
}

func TestStoresRouteLinkUsesQuorumRepair(t *testing.T) {
	ctx := context.Background()
	kv := clusterstore.OpenMemory(0)
	defer kv.Close()
	r2, r3 := clusterstate.NewMemoryRouteReplica(), clusterstate.NewMemoryRouteReplica()
	view := clusterstate.MemberView{Version: 1, Members: []string{"r1", "r2", "r3"}}
	stores := NewClusterStores(kv, "r1", view, 3, 1,
		map[string]clusterstate.RouteReplica{"r2": r2, "r3": r3}, nil)
	reg := New(stores, nil, 5*time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))

	seed := clusterstate.RouteRecord{
		Meta:      clusterstate.RecordMeta{Ballot: clusterstate.Ballot{Round: 7, Writer: "seed"}, Rev: 3, UpdatedAt: time.Now()},
		Group:     "/g",
		RouteKey:  "rk",
		SandboxID: "sb-q",
		State:     clusterstate.RouteReady,
		NodeID:    "n1",
	}
	if ok, err := stores.LocalRouteReplica().Accept(ctx, clusterstate.RouteKey("/g", "rk"), seed, seed.Meta.Ballot); err != nil || !ok {
		t.Fatalf("seed route accept ok=%v err=%v", ok, err)
	}

	got, rev, found, err := reg.stores.GetSandbox(ctx, "/g", "rk")
	if err != nil || !found {
		t.Fatalf("GetSandbox found=%v err=%v", found, err)
	}
	if got.SID != "sb-q" || got.State != StateReady || rev == 0 {
		t.Fatalf("route from quorum = %+v rev=%d", got, rev)
	}
	if repaired, found, err := r2.Read(ctx, clusterstate.RouteKey("/g", "rk")); err != nil || !found || repaired.SandboxID != "sb-q" {
		t.Fatalf("lagging route replica not repaired: %+v found=%v err=%v", repaired, found, err)
	}
}

func TestStoresNodeLinkUsesQuorumRepair(t *testing.T) {
	ctx := context.Background()
	kv := clusterstore.OpenMemory(0)
	defer kv.Close()
	n2, n3 := clusterstate.NewMemoryNodeReplica(), clusterstate.NewMemoryNodeReplica()
	view := clusterstate.MemberView{Version: 1, Members: []string{"n1", "n2", "n3"}}
	stores := NewClusterStores(kv, "n1", view, 1, 3,
		nil, map[string]clusterstate.NodeReplica{"n2": n2, "n3": n3})
	reg := New(stores, nil, 5*time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))

	seed := clusterstate.NodeRecord{
		Meta:         clusterstate.RecordMeta{Ballot: clusterstate.Ballot{Round: 9, Writer: "seed"}, Rev: 4, UpdatedAt: time.Now()},
		NodeID:       "node-q",
		State:        clusterstate.NodeLive,
		Labels:       map[string]string{"pool": "p"},
		DataEndpoint: "10.0.0.1:8443",
	}
	if ok, err := stores.LocalNodeReplica().Accept(ctx, "node-q", seed, seed.Meta.Ballot); err != nil || !ok {
		t.Fatalf("seed node accept ok=%v err=%v", ok, err)
	}

	got, found, err := reg.stores.GetNode(ctx, "node-q")
	if err != nil || !found {
		t.Fatalf("GetNode found=%v err=%v", found, err)
	}
	if got.DataEndpoint != "10.0.0.1:8443" || got.Labels["pool"] != "p" {
		t.Fatalf("node from quorum = %+v", got)
	}
	if repaired, found, err := n2.Read(ctx, "node-q"); err != nil || !found || repaired.DataEndpoint != "10.0.0.1:8443" {
		t.Fatalf("lagging node replica not repaired: %+v found=%v err=%v", repaired, found, err)
	}
}

func TestRouteLinkHandoffGateBlocksLiveWrites(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	reg.stores.SetMembershipVersion(1)
	reg.stores.SetRouteHandoffGate("/g", "rk", clusterstate.HandoffGate{
		FromVersion: 1,
		ToVersion:   2,
		Phase:       clusterstate.HandoffSwitching,
	})
	_, err := reg.stores.PutSandbox(ctx, &SandboxRecord{Group: "/g", RouteKey: "rk", SID: "sb", State: StateReady})
	if !errors.Is(err, clusterstate.ErrHandoffRetry) {
		t.Fatalf("handoff write err=%v, want retry", err)
	}
}

func TestNodeLinkHandoffGateReturnsMoved(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	reg.stores.SetMembershipVersion(1)
	reg.stores.SetNodeHandoffGate("n1", clusterstate.HandoffGate{
		Move:        clusterstate.KeyMove{Key: "n1", To: []string{"m2", "m3"}},
		FromVersion: 1,
		ToVersion:   2,
		Phase:       clusterstate.HandoffOldGrace,
		GraceUntil:  time.Now().Add(time.Minute),
	})
	err := reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1"})
	if !errors.Is(err, clusterstate.ErrHandoffMoved) {
		t.Fatalf("handoff node err=%v, want moved", err)
	}
}

func TestMergeConfig(t *testing.T) {
	merged := mergeConfig(map[string]string{"a": "1", "b": "2"}, map[string]string{"b": "X", "c": "3"})
	if merged["a"] != "1" || merged["b"] != "X" || merged["c"] != "3" {
		t.Fatalf("mergeConfig: %v", merged)
	}
}

func TestSweepKeepsInflightReserved(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	reg.stores.PutNode(ctx, &NodeRecord{NodeID: "dead", LastHeartbeatUnix: time.Now().Add(-time.Hour).Unix()})
	// A RESERVED row whose single-flight is still in flight must survive the sweep.
	reg.stores.PutSandbox(ctx, &SandboxRecord{Group: "/g", RouteKey: "live", SID: "sb-r", State: StateReserved, NodeID: "dead"})
	reg.mu.Lock()
	reg.inflight[flightKey("/g", "live")] = &reserveCall{done: make(chan struct{})}
	reg.mu.Unlock()
	// A RESERVED row with no in-flight reserve is stale → swept.
	reg.stores.PutSandbox(ctx, &SandboxRecord{Group: "/g", RouteKey: "stale", SID: "sb-s", State: StateReserved, NodeID: "dead"})

	reg.sweepDeadNodes(ctx, 30*time.Second)

	if _, _, found, _ := reg.stores.GetSandbox(ctx, "/g", "live"); !found {
		t.Fatal("swept a RESERVED row owned by an in-flight reserve")
	}
	if _, _, found, _ := reg.stores.GetSandbox(ctx, "/g", "stale"); found {
		t.Fatal("did not sweep a stale RESERVED row on a dead node")
	}
}

func TestSweepDeadNodes(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	// Disconnected node with a stale heartbeat + a READY sandbox → both swept.
	reg.stores.PutNode(ctx, &NodeRecord{NodeID: "dead", LastHeartbeatUnix: time.Now().Add(-time.Hour).Unix()})
	reg.stores.PutSandbox(ctx, &SandboxRecord{Group: "/g", RouteKey: "rk", SID: "sb-1", State: StateReady, NodeID: "dead"})
	reg.indexSID("sb-1", "/g", "rk")
	// Connected node with a stale heartbeat → NOT swept (a live channel isn't dead).
	reg.stores.PutNode(ctx, &NodeRecord{NodeID: "live", LastHeartbeatUnix: time.Now().Add(-time.Hour).Unix()})
	reg.addNode(&fakeConn{nodeID: "live"})
	reg.stores.PutSandbox(ctx, &SandboxRecord{Group: "/g2", RouteKey: "rk", SID: "sb-2", State: StateReady, NodeID: "live"})

	reg.sweepDeadNodes(ctx, 30*time.Second)

	if _, found, _ := reg.stores.GetNode(ctx, "dead"); found {
		t.Fatal("dead node not swept")
	}
	if _, _, found, _ := reg.stores.GetSandbox(ctx, "/g", "rk"); found {
		t.Fatal("dead node's sandbox not reset")
	}
	if _, found, _ := reg.stores.GetNode(ctx, "live"); !found {
		t.Fatal("connected node wrongly swept")
	}
	if _, _, found, _ := reg.stores.GetSandbox(ctx, "/g2", "rk"); !found {
		t.Fatal("connected node's sandbox wrongly reset")
	}
}
