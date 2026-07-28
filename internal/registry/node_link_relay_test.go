package registry

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	"github.com/kuasar-sandbox/orchestrator/internal/nodelink"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

func commandClusterContext(t *testing.T, cmd *routesync.Command) *routesync.ClusterSandboxContext {
	t.Helper()
	if cmd.Cluster == nil {
		t.Fatal("command cluster context is nil")
	}
	return cmd.Cluster
}

func TestNodeLinkIngressRelaysToNodeOwner(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster := newShardStoreCluster(t, []string{"ingress", "peer", "owner"}, 3, 1, 1, 1)
	nodeID := nodeOwnedBy(t, cluster["ingress"], "owner")
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	owner := New(cluster["owner"], placementWithToken(nodeID), 5*time.Second, log)
	ingress := New(cluster["ingress"], placementWithToken(nodeID), 5*time.Second, log)
	enableTestCreateAuth(t, ingress)
	ingress.SetRemoteNodeOwners(map[string]NodeOwner{"owner": owner.LocalNodeOwner()})

	ownerMux := http.NewServeMux()
	ownerMux.HandleFunc(NodeLinkRelayPath, owner.ServeNodeLinkRelay)
	ownerSrv := httptest.NewServer(h2c.NewHandler(ownerMux, &http2.Server{}))
	defer ownerSrv.Close()
	ingress.SetNodeLinkRelayPeers(map[string]NodeLinkRelayPeer{"owner": {Endpoint: ownerSrv.URL, Client: testH2CClient()}})

	ingressMux := http.NewServeMux()
	ingressMux.HandleFunc(routesync.NodeLinkPath, ingress.ServeNodeLink)
	ingressSrv := httptest.NewServer(h2c.NewHandler(ingressMux, &http2.Server{}))
	defer ingressSrv.Close()
	node := startRelayNodeStub(t, ctx, ingressSrv.Listener.Addr().String(), nodeID)
	defer node.close()

	waitNodeLinkRecord(t, ctx, owner.stores, nodeID, "owner")
	waitNodeLinkProfile(t, ctx, ingress.stores, nodeID, "owner")
	if _, live := ingress.node(nodeID); live {
		t.Fatal("ingress member unexpectedly holds the node_link stream")
	}
	if _, live := owner.node(nodeID); !live {
		t.Fatal("owner member does not hold the relayed node_link stream")
	}

	res, err := ingress.ReserveSandbox(ctx, testCreateReserve("/g", "rk", nil))
	if err != nil {
		t.Fatalf("reserve through ingress: %v", err)
	}
	if res.Route.NodeID != nodeID || res.Route.SandboxID == "" || res.Route.NodeSandboxID != EncodeNodeSandboxID(res.Route.SandboxID, 0) {
		t.Fatalf("reserve result=%+v, want node %s", res, nodeID)
	}
	cmd := node.waitCommand(t, routesync.CmdCreate)
	clusterContext := commandClusterContext(t, cmd)
	if clusterContext.Group != "/g" || clusterContext.RouteKey != "rk" || clusterContext.AuthSandboxID != res.Route.SandboxID ||
		cmd.SID != res.Route.NodeSandboxID || cmd.Profile != "e2b" {
		t.Fatalf("relayed command=%+v, reserve=%+v", cmd, res)
	}
}

func TestNodeLinkIngressRedirectsToNodeOwner(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster := newShardStoreCluster(t, []string{"ingress", "owner"}, 1, 1, 1, 1)
	nodeID := nodeOwnedBy(t, cluster["ingress"], "owner")
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	owner := New(cluster["owner"], placementWithToken(nodeID), 5*time.Second, log)
	ingress := New(cluster["ingress"], placementWithToken(nodeID), 5*time.Second, log)
	enableTestCreateAuth(t, ingress)

	ownerMux := http.NewServeMux()
	ownerMux.HandleFunc(NodeLinkRelayPath, owner.ServeNodeLinkRelay)
	ownerSrv := httptest.NewServer(h2c.NewHandler(ownerMux, &http2.Server{}))
	defer ownerSrv.Close()
	ingress.SetNodeLinkRelayPeers(map[string]NodeLinkRelayPeer{"owner": {
		Endpoint: ownerSrv.URL, RedirectEndpoint: "127.0.0.1:17702", Client: testH2CClient(),
	}})

	ingressMux := http.NewServeMux()
	ingressMux.HandleFunc(routesync.NodeLinkPath, ingress.ServeNodeLink)
	ingressSrv := httptest.NewServer(h2c.NewHandler(ingressMux, &http2.Server{}))
	defer ingressSrv.Close()

	pr, pw := io.Pipe()
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, "http://registry"+routesync.NodeLinkPath, pr)
	if err != nil {
		t.Fatal(err)
	}
	tr := &http2.Transport{
		AllowHTTP: true,
		DialTLSContext: func(ctx context.Context, _, _ string, _ *tls.Config) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "tcp", ingressSrv.Listener.Addr().String())
		},
	}
	defer tr.CloseIdleConnections()
	respCh := make(chan *http.Response, 1)
	errCh := make(chan error, 1)
	go func() {
		resp, err := tr.RoundTrip(req)
		if err != nil {
			errCh <- err
			return
		}
		respCh <- resp
	}()
	if err := routesync.WriteMsg(pw, &routesync.Msg{Type: routesync.TypeNodeRegister, NodeReg: &routesync.NodeRegister{
		NodeID: nodeID, DataEndpoint: "10.0.0.1:8443", AcceptRedirect: true,
	}}); err != nil {
		t.Fatal(err)
	}
	defer pw.Close()
	var resp *http.Response
	select {
	case resp = <-respCh:
	case err := <-errCh:
		t.Fatal(err)
	case <-time.After(3 * time.Second):
		t.Fatal("node-link redirect did not return response")
	}
	defer resp.Body.Close()
	hello, err := routesync.ReadMsg(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if hello.Type != routesync.TypeHello || hello.Hello == nil || hello.Hello.Redirect == nil ||
		len(hello.Hello.Redirect.Targets) != 1 || hello.Hello.Redirect.Targets[0].MemberID != "owner" ||
		hello.Hello.Redirect.Targets[0].Endpoint != "127.0.0.1:17702" {
		t.Fatalf("redirect hello=%+v", hello.Hello)
	}
	if _, live := ingress.node(nodeID); live {
		t.Fatal("ingress member unexpectedly holds redirected node_link stream")
	}
	if _, live := owner.node(nodeID); live {
		t.Fatal("owner should not hold stream until node reconnects")
	}
}

func TestNodeLinkRedirectReconnectsToOwnerAndReserveCompletes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster := newShardStoreCluster(t, []string{"ingress", "owner"}, 1, 1, 1, 1)
	nodeID := nodeOwnedBy(t, cluster["ingress"], "owner")
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	owner := New(cluster["owner"], placementWithToken(nodeID), 5*time.Second, log)
	ingress := New(cluster["ingress"], placementWithToken(nodeID), 5*time.Second, log)
	enableTestCreateAuth(t, ingress)

	ownerMux := http.NewServeMux()
	ownerMux.HandleFunc(routesync.NodeLinkPath, owner.ServeNodeLink)
	ownerMux.HandleFunc(NodeOwnerRPCPath, func(w http.ResponseWriter, req *http.Request) {
		ServeNodeOwner(w, req, owner.LocalNodeOwner())
	})
	ownerSrv := httptest.NewServer(h2c.NewHandler(ownerMux, &http2.Server{}))
	defer ownerSrv.Close()
	ingress.SetRemoteNodeOwners(map[string]NodeOwner{"owner": NewHTTPNodeOwner(ownerSrv.URL, ownerSrv.Client())})
	ingress.SetNodeLinkRelayPeers(map[string]NodeLinkRelayPeer{"owner": {
		Endpoint: ownerSrv.URL, RedirectEndpoint: ownerSrv.Listener.Addr().String(), Client: testH2CClient(),
	}})

	ingressMux := http.NewServeMux()
	ingressMux.HandleFunc(routesync.NodeLinkPath, ingress.ServeNodeLink)
	ingressSrv := httptest.NewServer(h2c.NewHandler(ingressMux, &http2.Server{}))
	defer ingressSrv.Close()

	node := newRedirectNodeStub(nodeID)
	client := nodelink.NewWithEndpoint(ingressSrv.Listener.Addr().String(), testNodeLinkDial, routesync.NodeRegister{
		NodeID: nodeID, Capacity: 10, DataEndpoint: "127.0.0.1:19191",
	}, node, 20*time.Millisecond, nil, log, true)
	go client.Run(ctx)

	waitNodeLinkRecord(t, ctx, owner.stores, nodeID, "owner")
	waitNodeLinkProfile(t, ctx, ingress.stores, nodeID, "owner")
	if _, live := ingress.node(nodeID); live {
		t.Fatal("ingress member unexpectedly holds redirected node_link stream")
	}
	waitRegistryNodeLive(t, owner, nodeID)

	res, err := ingress.ReserveSandbox(ctx, testCreateReserve("/g", "rk", nil))
	if err != nil {
		t.Fatalf("reserve through redirected node-link: %v", err)
	}
	if res.Route.NodeID != nodeID || res.Route.SandboxID == "" || res.Route.NodeSandboxID != EncodeNodeSandboxID(res.Route.SandboxID, 0) || res.Route.DataEndpoint != "127.0.0.1:19191" {
		t.Fatalf("reserve result=%+v, want node %s endpoint 127.0.0.1:19191", res, nodeID)
	}
	cmd := node.waitCommand(t, routesync.CmdCreate)
	clusterContext := commandClusterContext(t, cmd)
	if clusterContext.Group != "/g" || clusterContext.RouteKey != "rk" || clusterContext.AuthSandboxID != res.Route.SandboxID ||
		cmd.SID != res.Route.NodeSandboxID || cmd.Profile != "e2b" {
		t.Fatalf("redirected command=%+v, reserve=%+v", cmd, res)
	}
}

func TestNodeLinkRedirectDoesNotFallbackToRelayEndpoint(t *testing.T) {
	cluster := newShardStoreCluster(t, []string{"ingress", "owner"}, 1, 1, 1, 1)
	ingress := New(cluster["ingress"], nil, 5*time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ingress.SetNodeLinkRelayPeers(map[string]NodeLinkRelayPeer{"owner": {
		Endpoint: "http://owner-control.example.test",
	}})
	if targets := ingress.nodeLinkRedirectTargets([]string{"owner"}); len(targets) != 0 {
		t.Fatalf("redirect targets=%+v, want none without explicit RedirectEndpoint", targets)
	}
}

func testNodeLinkDial(ctx context.Context, endpoint string) (net.Conn, error) {
	if endpoint == "" {
		return nil, fmt.Errorf("empty node-link endpoint")
	}
	return (&net.Dialer{}).DialContext(ctx, "tcp", endpoint)
}

type redirectNodeStub struct {
	nodeID string

	mu     sync.Mutex
	routes map[string]routesync.RouteEntry
	subs   map[int]chan routesync.Event
	next   int
	cmdCh  chan *routesync.Command
}

func newRedirectNodeStub(nodeID string) *redirectNodeStub {
	return &redirectNodeStub{
		nodeID: nodeID,
		routes: map[string]routesync.RouteEntry{},
		subs:   map[int]chan routesync.Event{},
		cmdCh:  make(chan *routesync.Command, 16),
	}
}

func (n *redirectNodeStub) Range(ctx context.Context, fn func(routesync.RouteEntry) error) error {
	n.mu.Lock()
	routes := make([]routesync.RouteEntry, 0, len(n.routes))
	for _, route := range n.routes {
		routes = append(routes, route)
	}
	n.mu.Unlock()
	for _, route := range routes {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if err := fn(route); err != nil {
				return err
			}
		}
	}
	return nil
}

func (n *redirectNodeStub) Subscribe() (<-chan routesync.Event, func()) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.next++
	id := n.next
	ch := make(chan routesync.Event, 16)
	n.subs[id] = ch
	return ch, func() {
		n.mu.Lock()
		if cur, ok := n.subs[id]; ok {
			delete(n.subs, id)
			close(cur)
		}
		n.mu.Unlock()
	}
}

func (n *redirectNodeStub) HandleCommand(ctx context.Context, cmd *routesync.Command) *routesync.CmdAck {
	if cmd == nil {
		return nil
	}
	copied := *cmd
	select {
	case n.cmdCh <- &copied:
	default:
	}
	if cmd.Kind == routesync.CmdCreate || cmd.Kind == routesync.CmdConnect {
		go func() {
			time.Sleep(10 * time.Millisecond)
			route := testE2BRoute(cmd.Cluster.AuthSandboxID, routesync.StateRunning)
			route.SandboxID = cmd.SID
			n.publish(route)
		}()
	}
	if cmd.Kind == routesync.CmdDelete {
		n.mu.Lock()
		var sid string
		for key, route := range n.routes {
			if route.SandboxID == cmd.SID {
				delete(n.routes, key)
				sid = route.SandboxID
			}
		}
		n.mu.Unlock()
		if sid != "" {
			n.publishEvent(routesync.Event{Kind: routesync.TypeDelete, SID: sid})
		}
	}
	return &routesync.CmdAck{CmdID: cmd.CmdID, Status: routesync.AckAccepted}
}

func (n *redirectNodeStub) Heartbeat() *routesync.Heartbeat {
	n.mu.Lock()
	defer n.mu.Unlock()
	return &routesync.Heartbeat{Counts: len(n.routes)}
}

func (n *redirectNodeStub) BuildEvents() <-chan *routesync.BuildEvent { return nil }
func (n *redirectNodeStub) OnWake(ctx context.Context, sid string)    {}
func (n *redirectNodeStub) Policy() routesync.Policy                  { return routesync.Policy{} }

func (n *redirectNodeStub) publish(route routesync.RouteEntry) {
	n.mu.Lock()
	n.routes[route.SandboxID] = route
	n.mu.Unlock()
	n.publishEvent(routesync.Event{Kind: routesync.TypeUpsert, Route: route})
}

func (n *redirectNodeStub) publishEvent(ev routesync.Event) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for id, ch := range n.subs {
		select {
		case ch <- ev:
		default:
			close(ch)
			delete(n.subs, id)
		}
	}
}

func (n *redirectNodeStub) waitCommand(t *testing.T, kind string) *routesync.Command {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case cmd := <-n.cmdCh:
			if cmd.Kind == kind {
				return cmd
			}
		case <-deadline:
			t.Fatalf("timeout waiting for command %s", kind)
		}
	}
}

func testH2CClient() *http.Client {
	return &http.Client{Transport: &http2.Transport{
		AllowHTTP: true,
		DialTLSContext: func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, addr)
		},
	}}
}

type relayNodeStub struct {
	cancel context.CancelFunc
	pw     *io.PipeWriter
	resp   *http.Response
	cmdCh  chan *routesync.Command
}

func startRelayNodeStub(t *testing.T, ctx context.Context, addr, nodeID string) *relayNodeStub {
	t.Helper()
	nctx, cancel := context.WithCancel(ctx)
	pr, pw := io.Pipe()
	tr := &http2.Transport{
		AllowHTTP: true,
		DialTLSContext: func(ctx context.Context, _, _ string, _ *tls.Config) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "tcp", addr)
		},
	}
	req, err := http.NewRequestWithContext(nctx, http.MethodPut, "http://registry"+routesync.NodeLinkPath, pr)
	if err != nil {
		t.Fatal(err)
	}
	respCh := make(chan *http.Response, 1)
	errCh := make(chan error, 1)
	go func() {
		resp, err := tr.RoundTrip(req)
		if err != nil {
			errCh <- err
			return
		}
		respCh <- resp
	}()
	if err := routesync.WriteMsg(pw, &routesync.Msg{Type: routesync.TypeNodeRegister, NodeReg: &routesync.NodeRegister{
		NodeID: nodeID, Capacity: 10, DataEndpoint: "10.0.0.1:8443",
	}}); err != nil {
		t.Fatal(err)
	}
	var resp *http.Response
	select {
	case resp = <-respCh:
	case err := <-errCh:
		t.Fatal(err)
	case <-time.After(3 * time.Second):
		t.Fatal("node-link relay did not return response")
	}
	hello, err := routesync.ReadMsg(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if hello.Type != routesync.TypeHello {
		t.Fatalf("first response=%+v, want hello", hello)
	}
	stub := &relayNodeStub{cancel: cancel, pw: pw, resp: resp, cmdCh: make(chan *routesync.Command, 16)}
	go stub.readLoop(t)
	return stub
}

func (n *relayNodeStub) readLoop(t *testing.T) {
	for {
		m, err := routesync.ReadMsg(n.resp.Body)
		if err != nil {
			return
		}
		if m.Type != routesync.TypeCommand || m.Cmd == nil {
			continue
		}
		cmd := *m.Cmd
		select {
		case n.cmdCh <- &cmd:
		default:
		}
		_ = routesync.WriteMsg(n.pw, &routesync.Msg{Type: routesync.TypeCmdAck, Ack: &routesync.CmdAck{CmdID: cmd.CmdID, Status: routesync.AckAccepted}})
		if cmd.Kind == routesync.CmdCreate || cmd.Kind == routesync.CmdConnect {
			route := testE2BRoute(cmd.Cluster.AuthSandboxID, routesync.StateRunning)
			route.SandboxID = cmd.SID
			_ = routesync.WriteMsg(n.pw, &routesync.Msg{Type: routesync.TypeUpsert, Route: &route})
		}
	}
}

func (n *relayNodeStub) waitCommand(t *testing.T, kind string) *routesync.Command {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case cmd := <-n.cmdCh:
			if cmd.Kind == kind {
				return cmd
			}
		case <-deadline:
			t.Fatalf("timeout waiting for command %s", kind)
		}
	}
}

func (n *relayNodeStub) close() {
	n.cancel()
	_ = n.pw.Close()
	if n.resp != nil {
		_ = n.resp.Body.Close()
	}
}

func nodeOwnedBy(t *testing.T, stores *Stores, owner string) string {
	t.Helper()
	for i := 0; i < 1000; i++ {
		nodeID := fmt.Sprintf("node-relay-%d", i)
		owners, err := stores.NodeOwnerCandidates(context.Background(), nodeID)
		if err != nil {
			t.Fatal(err)
		}
		if len(owners) == 1 && owners[0] == owner {
			return nodeID
		}
	}
	t.Fatalf("could not find node owned by %s", owner)
	return ""
}

func waitNodeLinkRecord(t *testing.T, ctx context.Context, stores *Stores, nodeID, linkOwner string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	var last *NodeRecord
	var lastFound bool
	var lastErr error
	for {
		node, found, err := stores.GetNode(ctx, nodeID)
		last, lastFound, lastErr = node, found, err
		if err == nil && found && node.LinkOwner == linkOwner {
			return
		}
		if time.Now().After(deadline) {
			owners, ownerErr := stores.NodeOwnerCandidates(ctx, nodeID)
			t.Fatalf("node %s link_owner not replicated to %s on writer %s: candidates=%v candidate_err=%v found=%v node=%+v err=%v",
				nodeID, linkOwner, stores.WriterID(), owners, ownerErr, lastFound, last, lastErr)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func waitNodeLinkProfile(t *testing.T, ctx context.Context, stores *Stores, nodeID, linkOwner string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	var last *NodeRecord
	var lastFound bool
	var lastErr error
	for {
		node, found, err := stores.GetNodeProfile(ctx, nodeID)
		last, lastFound, lastErr = node, found, err
		if err == nil && found && node.LinkOwner == linkOwner {
			return
		}
		if time.Now().After(deadline) {
			owners, ownerErr := stores.NodeOwnerCandidates(ctx, nodeID)
			t.Fatalf("node %s profile link_owner not readable as %s from writer %s: candidates=%v candidate_err=%v found=%v node=%+v err=%v",
				nodeID, linkOwner, stores.WriterID(), owners, ownerErr, lastFound, last, lastErr)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func waitRegistryNodeLive(t *testing.T, reg *Registry, nodeID string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		if _, live := reg.node(nodeID); live {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("node %s is not live on registry writer %s", nodeID, reg.stores.WriterID())
		}
		time.Sleep(10 * time.Millisecond)
	}
}
