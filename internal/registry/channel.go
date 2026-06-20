package registry

import (
	"io"
	"net/http"
	"sync"

	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/routesync"
)

// nodeChannel is the registry's per-node channel handle: it writes commands to
// the node on the h2c response body (serialized). It implements nodeConn.
type nodeChannel struct {
	nodeID string
	mu     sync.Mutex
	w      io.Writer
	flush  func()
}

func (c *nodeChannel) id() string { return c.nodeID }

func (c *nodeChannel) send(cmd *routesync.Command) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := routesync.WriteMsg(c.w, &routesync.Msg{Type: routesync.TypeCommand, Cmd: cmd}); err != nil {
		return err
	}
	c.flush()
	return nil
}

// ServeNodeLink handles one node's node-link connection (the node DIALS the
// registry and is the route authority). The request body carries NodeRegister
// (first frame), then the node's sandbox route stream (upsert/delete/bookmark) +
// heartbeats + cmd_acks; the response body carries registry commands. It returns
// when the node disconnects (request-body EOF) — which deregisters it.
func (r *Registry) ServeNodeLink(w http.ResponseWriter, req *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "node-link needs a flushable (h2c) writer", http.StatusInternalServerError)
		return
	}
	body := req.Body
	ctx := req.Context()

	first, err := routesync.ReadMsg(body)
	if err != nil || first.Type != routesync.TypeNodeRegister || first.NodeReg == nil {
		http.Error(w, "node-link: expected node_register first frame", http.StatusBadRequest)
		return
	}
	nr := first.NodeReg
	if err := r.updateNodeRegister(ctx, nr); err != nil {
		r.log.Error("node-link: register", "node", nr.NodeID, "err", err)
		http.Error(w, "register failed", http.StatusInternalServerError)
		return
	}

	conn := &nodeChannel{nodeID: nr.NodeID, w: w, flush: flusher.Flush}
	r.addNode(conn)
	defer r.removeNode(conn)
	r.onNodeConnected() // predistribute this node's groups' manifest keys (§7.6)

	// Ack with a Hello so the node's RoundTrip returns and it starts streaming.
	if err := routesync.WriteMsg(w, &routesync.Msg{Type: routesync.TypeHello, Hello: &routesync.Hello{Version: routesync.Version}}); err != nil {
		return
	}
	flusher.Flush()
	r.log.Info("node-link: node connected", "node", nr.NodeID, "labels", nr.Labels)

	for {
		m, err := routesync.ReadMsg(body)
		if err != nil {
			if ctx.Err() == nil {
				r.log.Debug("node-link: read end", "node", nr.NodeID, "err", err)
			}
			return
		}
		switch m.Type {
		case routesync.TypeUpsert:
			if m.Route != nil {
				r.applyRoute(ctx, nr.NodeID, m.Route)
			}
		case routesync.TypeDelete:
			r.applyDeleteBySID(ctx, m.SID)
		case routesync.TypeHeartbeat:
			if m.Beat != nil {
				r.updateHeartbeat(ctx, nr.NodeID, m.Beat)
			}
		case routesync.TypeCmdAck:
			// Command receipt: wakes a sendAndWait (key gating) or fast-fails a
			// rejected lifecycle command's Reserve (cluster.md §5.1).
			r.ackCommand(m.Ack)
		case routesync.TypeBookmark:
			// initial-sync marker; the route stream itself converges state.
		}
	}
}
