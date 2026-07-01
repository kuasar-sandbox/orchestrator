package registry

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"

	clusterstate "github.com/kuasar-sandbox/sandbox-orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/routesync"
)

const NodeLinkRelayPath = "/internal/node-link/relay"

type NodeLinkRelayPeer struct {
	Endpoint string
	Client   *http.Client
}

// nodeChannel is the registry's per-node node_link handle: it writes commands to
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

	if r.localOwnsNodeLink(ctx, nr.NodeID) {
		if err := r.serveNodeLinkLocal(ctx, w, flusher.Flush, body, nr); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}
	if err := r.relayNodeLink(w, flusher.Flush, req, first, body); err != nil {
		r.log.Warn("node-link: relay failed", "node", nr.NodeID, "err", err)
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
	}
}

func (r *Registry) localOwnsNodeLink(ctx context.Context, nodeID string) bool {
	owners, err := r.stores.NodeOwnerCandidates(ctx, nodeID)
	if err != nil || len(owners) == 0 {
		return true
	}
	local := r.stores.WriterID()
	for _, owner := range owners {
		if owner == local {
			return true
		}
	}
	return false
}

func (r *Registry) relayNodeLink(w http.ResponseWriter, flush func(), req *http.Request, first *routesync.Msg, body io.Reader) error {
	nodeID := ""
	if first != nil && first.NodeReg != nil {
		nodeID = first.NodeReg.NodeID
	}
	owners, err := r.stores.NodeOwnerCandidates(req.Context(), nodeID)
	if err != nil {
		return err
	}
	local := r.stores.WriterID()
	var errs []string
	for _, owner := range owners {
		if owner == "" || owner == local {
			continue
		}
		peer, ok := r.nodeLinkRelayPeer(owner)
		if !ok {
			errs = append(errs, owner+": missing relay peer")
			continue
		}
		if err := r.relayNodeLinkToPeer(w, flush, req, first, body, owner, peer); err != nil {
			errs = append(errs, owner+": "+err.Error())
			continue
		}
		return nil
	}
	if len(errs) == 0 {
		return fmt.Errorf("node-link: no remote owner candidates for node %q", nodeID)
	}
	return fmt.Errorf("node-link: relay failed for node %q: %s", nodeID, strings.Join(errs, "; "))
}

func (r *Registry) nodeLinkRelayPeer(member string) (NodeLinkRelayPeer, bool) {
	r.nodeLinkRelayMu.RLock()
	defer r.nodeLinkRelayMu.RUnlock()
	peer, ok := r.nodeLinkRelayPeers[member]
	return peer, ok
}

func (r *Registry) relayNodeLinkToPeer(w http.ResponseWriter, flush func(), req *http.Request, first *routesync.Msg, body io.Reader, owner string, peer NodeLinkRelayPeer) error {
	pr, pw := io.Pipe()
	startCopy := make(chan struct{})
	abortCopy := make(chan struct{})
	go func() {
		if err := routesync.WriteMsg(pw, first); err != nil {
			_ = pw.CloseWithError(err)
			return
		}
		select {
		case <-startCopy:
		case <-abortCopy:
			_ = pw.CloseWithError(context.Canceled)
			return
		case <-req.Context().Done():
			_ = pw.CloseWithError(req.Context().Err())
			return
		}
		_, err := io.Copy(pw, body)
		if err != nil {
			_ = pw.CloseWithError(err)
			return
		}
		_ = pw.Close()
	}()
	defer func() {
		_ = pr.Close()
		_ = pw.Close()
	}()

	client := peer.Client
	if client == nil {
		client = http.DefaultClient
	}
	relayReq, err := http.NewRequestWithContext(req.Context(), http.MethodPut, strings.TrimRight(peer.Endpoint, "/")+NodeLinkRelayPath, pr)
	if err != nil {
		return err
	}
	relayReq.Header.Set("Content-Type", "application/octet-stream")
	resp, err := client.Do(relayReq)
	if err != nil {
		close(abortCopy)
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		close(abortCopy)
		return fmt.Errorf("relay owner %s status %s", owner, resp.Status)
	}
	close(startCopy)
	for key, values := range resp.Header {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}
	w.WriteHeader(resp.StatusCode)
	if err := copyFlush(w, flush, resp.Body); err != nil {
		return err
	}
	return nil
}

func copyFlush(w io.Writer, flush func(), r io.Reader) error {
	buf := make([]byte, 32*1024)
	for {
		n, err := r.Read(buf)
		if n > 0 {
			if _, werr := w.Write(buf[:n]); werr != nil {
				return werr
			}
			flush()
		}
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
	}
}

func (r *Registry) ServeNodeLinkRelay(w http.ResponseWriter, req *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "node-link relay needs a flushable writer", http.StatusInternalServerError)
		return
	}
	body := req.Body
	first, err := routesync.ReadMsg(body)
	if err != nil || first.Type != routesync.TypeNodeRegister || first.NodeReg == nil {
		http.Error(w, "node-link relay: expected node_register first frame", http.StatusBadRequest)
		return
	}
	nr := first.NodeReg
	if !r.localOwnsNodeLink(req.Context(), nr.NodeID) {
		http.Error(w, "node-link relay: local member is not a node owner", http.StatusConflict)
		return
	}
	if err := r.serveNodeLinkLocal(req.Context(), w, flusher.Flush, body, nr); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (r *Registry) serveNodeLinkLocal(ctx context.Context, w io.Writer, flush func(), body io.Reader, nr *routesync.NodeRegister) error {
	if err := r.updateNodeRegister(ctx, nr); err != nil {
		r.log.Error("node-link: register", "node", nr.NodeID, "err", err)
		return fmt.Errorf("node-link: register failed: %w", err)
	}

	conn := &nodeChannel{nodeID: nr.NodeID, w: w, flush: flush}

	// Write Hello BEFORE exposing the node_link. After addNode, heartbeat
	// maintenance may send key refresh commands on this same h2 stream under
	// nodeChannel.mu; Hello goes out first while this is still the only writer.
	resumeFrom := ""
	if rec, found, err := r.stores.GetNode(ctx, nr.NodeID); err == nil && found {
		resumeFrom = rec.ResumeToken
	}
	if err := routesync.WriteMsg(w, &routesync.Msg{Type: routesync.TypeHello, Hello: &routesync.Hello{Version: routesync.Version, ResumeFrom: resumeFrom}}); err != nil {
		return nil
	}
	flush()

	r.addNode(conn)
	defer r.removeNode(conn)
	r.log.Info("node-link: node connected", "node", nr.NodeID, "labels", nr.Labels)

	collectingFull := true
	fullSeen := map[string]string{}
	for {
		m, err := routesync.ReadMsg(body)
		if err != nil {
			if ctx.Err() == nil {
				r.log.Debug("node-link: read end", "node", nr.NodeID, "err", err)
			}
			return nil
		}
		switch m.Type {
		case routesync.TypeUpsert:
			if m.Route != nil {
				if collectingFull && m.Route.Group != "" && m.Route.RouteKey != "" {
					fullSeen[clusterstate.RouteKey(m.Route.Group, m.Route.RouteKey)] = m.Route.SandboxID
				}
				r.applyRoute(ctx, nr.NodeID, m.Route)
			}
		case routesync.TypeDelete:
			r.applyDeleteBySID(ctx, nr.NodeID, m.SID)
		case routesync.TypeHeartbeat:
			if m.Beat != nil {
				r.updateHeartbeat(ctx, nr.NodeID, m.Beat)
			}
		case routesync.TypeCmdAck:
			// Command receipt wakes the matching node-owner SendCommandAndWait.
			r.ackCommand(m.Ack)
		case routesync.TypeBuildEvent:
			// Build state transition: converge the BuildStore (§7.5); a terminal
			// state releases the build's reserved node resources.
			if m.Build != nil {
				r.applyBuildEvent(ctx, nr.NodeID, m.Build)
			}
		case routesync.TypeBookmark:
			if collectingFull && m.FullSync {
				r.applyNodeFullSnapshot(ctx, nr.NodeID, fullSeen)
			}
			collectingFull = false
			fullSeen = nil
			if m.RevToken != "" {
				r.updateNodeResume(ctx, nr.NodeID, m.RevToken)
			}
		}
	}
}
