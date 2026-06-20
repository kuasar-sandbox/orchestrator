package registry

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/clusterstore"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/routesync"
)

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
