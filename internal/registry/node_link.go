package registry

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

const NodeLinkRelayPath = "/internal/node-link/relay"

type NodeLinkRelayPeer struct {
	Endpoint         string
	RedirectEndpoint string
	Client           *http.Client
}

// nodeChannel is the registry's per-node node_link handle: it writes commands to
// the node on the h2c response body (serialized). It implements nodeConn.
type nodeChannel struct {
	nodeID string
	mu     sync.Mutex
	w      io.Writer
	flush  func()
}

// nodeBuildFrameFence is the process-local active-session authority for Build
// projection frames from one NodeID. Build records intentionally have no
// session/generation field, so the fence must cover the store mutation rather
// than perform a racy preflight check.
type nodeBuildFrameFence struct {
	mu     sync.Mutex
	active *nodeChannel
}

var errSupersededNodeBuildSession = errors.New("node-link: superseded Build session")

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

func (r *Registry) withActiveNodeBuildFrame(conn *nodeChannel, apply func() error) error {
	if conn == nil || apply == nil {
		return errSupersededNodeBuildSession
	}
	fence := r.nodeBuildFrameFence(conn.nodeID)
	fence.mu.Lock()
	defer fence.mu.Unlock()
	if fence.active != conn {
		return errSupersededNodeBuildSession
	}
	return apply()
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

	owners, err := r.stores.NodeOwnerCandidates(ctx, nr.NodeID)
	if err != nil || len(owners) == 0 {
		owners = []string{r.stores.WriterID()}
	}
	if memberInList(r.stores.WriterID(), owners) {
		if err := r.serveNodeLinkLocal(ctx, w, flusher.Flush, body, nr); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}
	if nr.AcceptRedirect {
		if targets := r.nodeLinkRedirectTargets(owners); len(targets) > 0 {
			if err := routesync.WriteMsg(w, &routesync.Msg{Type: routesync.TypeHello, Hello: &routesync.Hello{
				Version:  routesync.Version,
				Redirect: &routesync.NodeLinkRedirect{Targets: targets},
			}}); err != nil {
				r.log.Warn("node-link: redirect response failed", "node", nr.NodeID, "err", err)
				return
			}
			flusher.Flush()
			return
		}
	}
	if err := r.relayNodeLink(w, flusher.Flush, req, first, body, owners); err != nil {
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

func memberInList(member string, members []string) bool {
	for _, cur := range members {
		if cur == member {
			return true
		}
	}
	return false
}

func (r *Registry) nodeLinkRedirectTargets(owners []string) []routesync.NodeLinkTarget {
	local := r.stores.WriterID()
	var targets []routesync.NodeLinkTarget
	for _, owner := range owners {
		if owner == "" || owner == local {
			continue
		}
		peer, ok := r.nodeLinkRelayPeer(owner)
		if !ok {
			continue
		}
		endpoint := peer.RedirectEndpoint
		if endpoint == "" {
			continue
		}
		targets = append(targets, routesync.NodeLinkTarget{MemberID: owner, Endpoint: endpoint})
	}
	return targets
}

func (r *Registry) relayNodeLink(w http.ResponseWriter, flush func(), req *http.Request, first *routesync.Msg, body io.Reader, owners []string) error {
	nodeID := ""
	if first != nil && first.NodeReg != nil {
		nodeID = first.NodeReg.NodeID
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
	registered, err := r.updateNodeRegister(ctx, nr)
	if err != nil {
		r.log.Error("node-link: register", "node", nr.NodeID, "err", err)
		return fmt.Errorf("node-link: register failed: %w", err)
	}

	conn := &nodeChannel{nodeID: nr.NodeID, w: w, flush: flush}

	// Write Hello BEFORE exposing the node_link. After addNode, heartbeat
	// maintenance may send key refresh commands on this same h2 stream under
	// nodeChannel.mu; Hello goes out first while this is still the only writer.
	resumeFrom := registered.ResumeToken
	fullExpected := append([]clusterstate.NodeSandboxRef(nil), registered.Sandboxes...)
	buildExpected := append([]clusterstate.NodeBuildRef(nil), registered.Builds...)
	if err := routesync.WriteMsg(w, &routesync.Msg{Type: routesync.TypeHello, Hello: &routesync.Hello{Version: routesync.Version, ResumeFrom: resumeFrom}}); err != nil {
		return nil
	}
	flush()

	r.addNode(conn)
	defer r.removeNode(conn)
	go r.projectRegisteredNode(ctx, registered)
	r.log.Info("node-link: node connected", "node", nr.NodeID, "labels", nr.Labels)

	heartbeatCh := make(chan *routesync.Heartbeat, 1)
	go r.runNodeHeartbeatUpdates(ctx, nr.NodeID, heartbeatCh)
	defer close(heartbeatCh)

	collectingFull := true
	fullSeen := map[string]struct{}{}
	buildCollecting := false
	buildSynced := false
	var buildSeen map[string]struct{}
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
				if collectingFull && m.Route.SandboxID != "" {
					fullSeen[m.Route.SandboxID] = struct{}{}
				}
				r.applyRoute(ctx, nr.NodeID, m.Route)
			}
		case routesync.TypeDelete:
			r.applyDeleteBySID(ctx, nr.NodeID, m.SID)
		case routesync.TypeHeartbeat:
			if m.Beat != nil {
				queueLatestHeartbeat(heartbeatCh, m.Beat)
			}
		case routesync.TypeCmdAck:
			// Command receipt wakes the matching node-owner SendCommandAndWait.
			r.ackCommand(m.Ack)
		case routesync.TypeBuildSyncBegin, routesync.TypeBuildUpsert, routesync.TypeBuildDelete, routesync.TypeBuildSyncEnd:
			err := r.withActiveNodeBuildFrame(conn, func() error {
				switch m.Type {
				case routesync.TypeBuildSyncBegin:
					if buildCollecting || buildSynced {
						return errors.New("node-link: invalid duplicate BuildSyncBegin")
					}
					buildCollecting = true
					buildSeen = make(map[string]struct{})
				case routesync.TypeBuildUpsert:
					if (!buildCollecting && !buildSynced) || m.Build == nil || m.Build.BuildID == "" {
						return errors.New("node-link: BuildUpsert outside a valid Build sync")
					}
					if buildCollecting {
						buildSeen[m.Build.BuildID] = struct{}{}
					}
					if err := r.applyBuildUpsert(ctx, nr.NodeID, m.Build); err != nil {
						return fmt.Errorf("node-link: apply BuildUpsert: %w", err)
					}
				case routesync.TypeBuildDelete:
					if (!buildCollecting && !buildSynced) || m.Build == nil || m.Build.BuildID == "" {
						return errors.New("node-link: BuildDelete outside a valid Build sync")
					}
					if err := r.applyBuildDelete(ctx, nr.NodeID, m.Build.BuildID); err != nil {
						return fmt.Errorf("node-link: apply BuildDelete: %w", err)
					}
				case routesync.TypeBuildSyncEnd:
					if !buildCollecting || buildSynced {
						return errors.New("node-link: invalid BuildSyncEnd")
					}
					if err := r.applyNodeBuildFullSnapshot(ctx, nr.NodeID, buildExpected, buildSeen); err != nil {
						return fmt.Errorf("node-link: finish Build full sync: %w", err)
					}
					buildCollecting = false
					buildSynced = true
					buildSeen = nil
				}
				return nil
			})
			if errors.Is(err, errSupersededNodeBuildSession) {
				return nil
			}
			if err != nil {
				return err
			}
		case routesync.TypeBookmark:
			if !buildSynced {
				return errors.New("node-link: Bookmark before Build full sync completed")
			}
			if collectingFull && m.FullSync {
				r.applyNodeFullSnapshot(ctx, nr.NodeID, fullExpected, fullSeen)
			}
			collectingFull = false
			fullSeen = nil
			if m.RevToken != "" {
				r.updateNodeResume(ctx, nr.NodeID, m.RevToken)
			}
		}
	}
}

func (r *Registry) runNodeHeartbeatUpdates(ctx context.Context, nodeID string, ch <-chan *routesync.Heartbeat) {
	keyRefreshCh := make(chan struct{}, 1)
	go r.runNodeKeyPairRefreshes(ctx, nodeID, keyRefreshCh)
	defer close(keyRefreshCh)
	for {
		select {
		case <-ctx.Done():
			return
		case hb, ok := <-ch:
			if !ok {
				return
			}
			if hb != nil {
				r.updateHeartbeat(ctx, nodeID, hb)
				queueNodeKeyPairRefresh(keyRefreshCh)
			}
		}
	}
}

func (r *Registry) runNodeKeyPairRefreshes(ctx context.Context, nodeID string, ch <-chan struct{}) {
	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-ch:
			if !ok {
				return
			}
			rec, found, err := r.getNodeForLinkUpdate(ctx, nodeID)
			if err == nil && found {
				r.refreshNodeKeyPairs(ctx, rec)
			}
		}
	}
}

func queueNodeKeyPairRefresh(ch chan<- struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

func queueLatestHeartbeat(ch chan *routesync.Heartbeat, hb *routesync.Heartbeat) {
	if hb == nil {
		return
	}
	cp := *hb
	select {
	case ch <- &cp:
		return
	default:
	}
	select {
	case <-ch:
	default:
	}
	select {
	case ch <- &cp:
	default:
	}
}
