package nodelink

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/clusterstore"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/registry"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/routesync"
)

// fakeNode implements Node: it streams its routes and, on a create command,
// "boots" the sandbox by adding a running route + emitting an upsert event — the
// same path a real node takes (the registry's Reserve waits on that route).
type fakeNode struct {
	mu     sync.Mutex
	routes map[string]routesync.RouteEntry
	events chan routesync.Event
}

func newFakeNode() *fakeNode {
	return &fakeNode{routes: map[string]routesync.RouteEntry{}, events: make(chan routesync.Event, 16)}
}

func (n *fakeNode) Range(ctx context.Context, fn func(routesync.RouteEntry) error) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, e := range n.routes {
		if err := fn(e); err != nil {
			return err
		}
	}
	return nil
}
func (n *fakeNode) Subscribe() (<-chan routesync.Event, func()) { return n.events, func() {} }
func (n *fakeNode) OnWake(ctx context.Context, sid string)      {}
func (n *fakeNode) Policy() routesync.Policy                    { return routesync.Policy{} }
func (n *fakeNode) Heartbeat() *routesync.Heartbeat             { return &routesync.Heartbeat{} }

func (n *fakeNode) HandleCommand(ctx context.Context, cmd *routesync.Command) *routesync.CmdAck {
	ack := &routesync.CmdAck{CmdID: cmd.CmdID, Status: routesync.AckAccepted}
	if cmd.Kind != routesync.CmdCreate {
		return ack
	}
	e := routesync.RouteEntry{
		SandboxID: cmd.SID, Group: cmd.Group, RouteKey: cmd.RouteKey,
		State: routesync.StateRunning, AccessToken: "tok-" + cmd.SID,
	}
	n.mu.Lock()
	n.routes[cmd.SID] = e
	n.mu.Unlock()
	n.events <- routesync.Event{Kind: routesync.TypeUpsert, Route: e}
	return ack
}

func TestNodeLinkReserveRoundTrip(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	kv, err := clusterstore.Open(filepath.Join(t.TempDir(), "r.db"), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer kv.Close()
	reg := registry.New(registry.NewStores(kv, nil), nil, 5*time.Second, log)

	mux := http.NewServeMux()
	mux.HandleFunc(routesync.NodeLinkPath, reg.ServeNodeLink)
	srv := httptest.NewServer(h2c.NewHandler(mux, &http2.Server{}))
	defer srv.Close()
	addr := srv.Listener.Addr().String()

	node := newFakeNode()
	client := New(
		func(ctx context.Context) (net.Conn, error) { return net.Dial("tcp", addr) },
		routesync.NodeRegister{NodeID: "n1", DataEndpoint: "10.0.0.1:8443"},
		node, 50*time.Millisecond, log,
	)
	go client.Run(ctx)

	// Wait for the node to register over the channel.
	deadline := time.Now().Add(3 * time.Second)
	for {
		if _, found, _ := reg.Stores().GetNode(ctx, "n1"); found {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("node never registered over node-link")
		}
		time.Sleep(10 * time.Millisecond)
	}

	res, err := reg.ReserveSandbox(ctx, "/cell/proj/app/g1", "u1:sess1", nil)
	if err != nil {
		t.Fatalf("reserve over node-link: %v", err)
	}
	if res.NodeID != "n1" || res.SID == "" || res.AccessToken != "tok-"+res.SID {
		t.Fatalf("reserve result: %+v", res)
	}
}
