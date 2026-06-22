package registry

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/clusterstore"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/secretbox"
)

func testRegWithBox(t *testing.T) *Registry {
	t.Helper()
	kv, err := clusterstore.Open(filepath.Join(t.TempDir(), "reg.db"), 0)
	if err != nil {
		t.Fatalf("open kv: %v", err)
	}
	t.Cleanup(func() { kv.Close() })
	box, err := secretbox.NewFromColonHex("0000000000000000000000000000000000000000000000000000000000000001")
	if err != nil {
		t.Fatalf("box: %v", err)
	}
	return New(NewStores(kv, box), nil, 5*time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

const testMK = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"

func TestKeyPredistribution(t *testing.T) {
	ctx := context.Background()
	reg := testRegWithBox(t)
	reg.stores.PutGroup(ctx, &GroupConfig{Group: "/g", ManifestKey: testMK})
	reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1", Labels: map[string]string{"zone": "east"}})
	var cmds []*routesync.Command
	reg.addNode(&fakeConn{nodeID: "n1", onCmd: func(c *routesync.Command) { cmds = append(cmds, c) }})

	reg.reconcileKeys(ctx)

	if len(cmds) != 1 || cmds[0].Kind != routesync.CmdKeyPut || cmds[0].ManifestKey != testMK {
		t.Fatalf("expected one key_put with the group key, got %+v", cmds)
	}
}

func TestKeyDropOnLeave(t *testing.T) {
	ctx := context.Background()
	reg := testRegWithBox(t)
	reg.stores.PutGroup(ctx, &GroupConfig{Group: "/g", ManifestKey: testMK, NodeSelectors: []map[string]string{{"zone": "east"}}})
	reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1", Labels: map[string]string{"zone": "east"}})
	var cmds []*routesync.Command
	reg.addNode(&fakeConn{nodeID: "n1", onCmd: func(c *routesync.Command) { cmds = append(cmds, c) }})

	reg.reconcileKeys(ctx) // n1 matches the selector → key_put
	reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1", Labels: map[string]string{"zone": "west"}})
	cmds = nil
	reg.reconcileKeys(ctx) // n1 left the set → key_drop

	if len(cmds) != 1 || cmds[0].Kind != routesync.CmdKeyDrop {
		t.Fatalf("expected one key_drop after the node left the allocation set, got %+v", cmds)
	}
}

func testReg(t *testing.T) *Registry {
	t.Helper()
	kv, err := clusterstore.Open(filepath.Join(t.TempDir(), "reg.db"), 0)
	if err != nil {
		t.Fatalf("open kv: %v", err)
	}
	t.Cleanup(func() { kv.Close() })
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	return New(NewStores(kv, nil), nil, 5*time.Second, log)
}

// fakeConn implements nodeConn; its onCmd hook lets a test simulate the node
// reacting to a command (e.g. reporting the sandbox running).
type fakeConn struct {
	nodeID string
	onCmd  func(*routesync.Command)
}

func (c *fakeConn) id() string { return c.nodeID }
func (c *fakeConn) send(cmd *routesync.Command) error {
	if c.onCmd != nil {
		c.onCmd(cmd)
	}
	return nil
}

func TestReserveSandboxCreateFlow(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
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
			State: routesync.StateRunning, AccessToken: "tok-" + cmd.SID,
		})
	}
	reg.addNode(conn)

	res, err := reg.ReserveSandbox(ctx, "/c/p/a/g1", "u1:s1", nil)
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if res.NodeID != "n1" || res.SID == "" || res.AccessToken != "tok-"+res.SID {
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

func TestRecordGCIdleSaved(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	now := time.Now().Unix()
	// An old SAVED record (idle past TTL) is reclaimed; a fresh SAVED + a READY are kept.
	reg.stores.PutSandbox(ctx, &SandboxRecord{Group: "/g", RouteKey: "old", SID: "sb-old", State: StateSaved, LastActive: now - 7200})
	reg.stores.PutSandbox(ctx, &SandboxRecord{Group: "/g", RouteKey: "fresh", SID: "sb-fresh", State: StateSaved, LastActive: now})
	reg.stores.PutSandbox(ctx, &SandboxRecord{Group: "/g", RouteKey: "ready", SID: "sb-r", State: StateReady, LastActive: now - 7200})

	reg.gcIdleRecords(ctx, time.Hour)

	if _, _, found, _ := reg.stores.GetSandbox(ctx, "/g", "old"); found {
		t.Fatal("idle SAVED record should be GC'd")
	}
	if _, _, found, _ := reg.stores.GetSandbox(ctx, "/g", "fresh"); !found {
		t.Fatal("a fresh SAVED record must be kept")
	}
	if _, _, found, _ := reg.stores.GetSandbox(ctx, "/g", "ready"); !found {
		t.Fatal("a READY record must never be GC'd (only idle SAVED)")
	}
}

func TestParkTimeoutRollback(t *testing.T) {
	ctx := context.Background()
	kv, _ := clusterstore.Open(filepath.Join(t.TempDir(), "r.db"), 0)
	defer kv.Close()
	reg := New(NewStores(kv, nil), nil, 200*time.Millisecond, slog.New(slog.NewTextHandler(io.Discard, nil)))
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
	// Seed a PAUSED sandbox on n1.
	reg.stores.PutSandbox(ctx, &SandboxRecord{Group: "/g", RouteKey: "rk", SID: "sb-x", State: StatePaused, NodeID: "n1"})

	conn := &fakeConn{nodeID: "n1"}
	conn.onCmd = func(cmd *routesync.Command) {
		if cmd.Kind != routesync.CmdConnect {
			return
		}
		go reg.applyRoute(context.Background(), "n1", &routesync.RouteEntry{
			SandboxID: "sb-x", Group: "/g", RouteKey: "rk", State: routesync.StateRunning, AccessToken: "tok",
		})
	}
	reg.addNode(conn)

	res, err := reg.ReserveSandbox(ctx, "/g", "rk", nil)
	if err != nil {
		t.Fatalf("resume reserve: %v", err)
	}
	if res.SID != "sb-x" || res.AccessToken != "tok" {
		t.Fatalf("resume result: %+v", res)
	}
}

func TestControlWatch(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1", DataEndpoint: "10.0.0.1:8443"})
	reg.stores.PutSandbox(ctx, &SandboxRecord{Group: "/g", RouteKey: "rk", SID: "sb-1", State: StateReady, NodeID: "n1", AccessToken: "tok"})

	mux := http.NewServeMux()
	reg.ServeControl(mux)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/control/watch")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	// Snapshot: a reset, one put (sb-1, resolved to its node's data endpoint), a bookmark.
	if rs := readWatchFrame(t, resp.Body); rs.Type != "reset" {
		t.Fatalf("expected reset, got %+v", rs)
	}
	if put := readWatchFrame(t, resp.Body); put.Type != "put" || put.Route == nil || put.Route.SID != "sb-1" || put.Route.DataEndpoint != "10.0.0.1:8443" {
		t.Fatalf("snapshot put: %+v", put)
	}
	bm := readWatchFrame(t, resp.Body)
	if bm.Type != "bookmark" {
		t.Fatalf("expected bookmark, got %+v", bm)
	}
	// Live delta: a new sandbox shows up as a put.
	reg.stores.PutSandbox(ctx, &SandboxRecord{Group: "/g", RouteKey: "rk2", SID: "sb-2", State: StateReady, NodeID: "n1", AccessToken: "t2"})
	if d := readWatchFrame(t, resp.Body); d.Type != "put" || d.Route == nil || d.Route.SID != "sb-2" {
		t.Fatalf("delta put: %+v", d)
	}

	// Resume from the bookmark rev: deltas only, NO reset/snapshot (incremental sync).
	resp2, err := http.Get(fmt.Sprintf("%s/control/watch?from_rev=%d", srv.URL, bm.Rev))
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	if d := readWatchFrame(t, resp2.Body); d.Type == "reset" || d.Type == "bookmark" {
		t.Fatalf("resume should replay deltas, not re-snapshot; got %+v", d)
	}
}

func readWatchFrame(t *testing.T, r io.Reader) *WatchEvent {
	t.Helper()
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, binary.LittleEndian.Uint32(hdr[:]))
	if _, err := io.ReadFull(r, buf); err != nil {
		t.Fatal(err)
	}
	var ev WatchEvent
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

func TestGroupResolverStoreAndMerge(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	// No manifest_key here: sealing it needs an encryption box (PutGroup errors
	// otherwise); the key projection (g.ManifestKey → Key.ManifestKey) is trivial.
	if err := reg.stores.PutGroup(ctx, &GroupConfig{Group: "/g", ProjectID: "p", TemplateRef: "tmpl",
		SandboxConfig: map[string]string{"a": "1", "b": "2"}, NodeSelectors: []map[string]string{{"z": "e"}}}); err != nil {
		t.Fatal(err)
	}
	// The default store-backed resolver projects each fine-grained interface.
	if k, ok, _ := reg.resolver.Key.Key(ctx, "/g"); !ok || k.ProjectID != "p" {
		t.Fatalf("store key: %+v ok=%v", k, ok)
	}
	if sc, ok, _ := reg.resolver.Sandbox.SandboxConfig(ctx, "/g"); !ok || sc.TemplateRef != "tmpl" || sc.Config["a"] != "1" {
		t.Fatalf("store sandbox: %+v ok=%v", sc, ok)
	}
	if pl, ok, _ := reg.resolver.Placement.Placement(ctx, "/g"); !ok || len(pl.NodeSelectors) != 1 {
		t.Fatalf("store placement: %+v ok=%v", pl, ok)
	}
	// §7.2 merge: the group's sandbox_config defaults fold under create (create wins).
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

func TestSweepKeepsFreshAndSaved(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	// Fresh (recent) disconnected node → not stale → kept.
	reg.stores.PutNode(ctx, &NodeRecord{NodeID: "fresh", LastHeartbeatUnix: time.Now().Unix()})
	// Stale disconnected node carrying a SAVED (unbound) sandbox → node swept,
	// SAVED left for re-placement elsewhere.
	reg.stores.PutNode(ctx, &NodeRecord{NodeID: "dead", LastHeartbeatUnix: time.Now().Add(-time.Hour).Unix()})
	reg.stores.PutSandbox(ctx, &SandboxRecord{Group: "/g", RouteKey: "rk", SID: "sb-s", State: StateSaved, NodeID: "dead"})

	reg.sweepDeadNodes(ctx, 30*time.Second)

	if _, found, _ := reg.stores.GetNode(ctx, "fresh"); !found {
		t.Fatal("fresh disconnected node wrongly swept")
	}
	if _, _, found, _ := reg.stores.GetSandbox(ctx, "/g", "rk"); !found {
		t.Fatal("SAVED sandbox wrongly reset (it is unbound)")
	}
}
