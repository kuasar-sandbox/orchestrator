package registry

import (
	"context"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

// Legacy transport fakes reply on their pending command's original connection.
// Production has no unbound ACK entry point.
func (r *Registry) ackCommand(ack *routesync.CmdAck) {
	if ack == nil {
		return
	}
	r.mu.Lock()
	waiter := r.acks[ack.CmdID]
	r.mu.Unlock()
	if waiter != nil {
		r.ackNodeCommand(waiter.conn, ack)
	}
}

type fencedTestConn struct {
	node string
	sent chan *routesync.Command
}

func (c *fencedTestConn) id() string                        { return c.node }
func (c *fencedTestConn) send(cmd *routesync.Command) error { c.sent <- cmd; return nil }

func TestGroupCommandAckCannotCrossConnections(t *testing.T) {
	r := New(newTestStores(t), time.Second, nil)
	old := &fencedTestConn{node: "node", sent: make(chan *routesync.Command, 1)}
	fresh := &fencedTestConn{node: "node", sent: make(chan *routesync.Command, 1)}
	r.mu.Lock()
	r.nodes[old.node] = old
	r.mu.Unlock()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	result := make(chan *routesync.CmdAck, 1)
	go func() {
		ack, _ := r.sendAndWait(ctx, old, &routesync.Command{CmdID: "same", Kind: routesync.CmdGroupGet}, time.Second)
		result <- ack
	}()
	select {
	case <-old.sent:
	case <-ctx.Done():
		t.Fatal("command not dispatched")
	}
	r.mu.Lock()
	r.nodes[fresh.node] = fresh
	r.mu.Unlock()
	r.ackNodeCommand(old, &routesync.CmdAck{CmdID: "same", Status: routesync.AckAccepted})
	r.ackNodeCommand(fresh, &routesync.CmdAck{CmdID: "same", Status: routesync.AckAccepted})
	select {
	case ack := <-result:
		t.Fatalf("superseded or unrelated connection acknowledged: %+v", ack)
	case <-time.After(20 * time.Millisecond):
	}
	cancel()
	select {
	case ack := <-result:
		if ack != nil {
			t.Fatal("late reply escaped cancellation")
		}
	case <-time.After(time.Second):
		t.Fatal("command wait leaked")
	}
}
