package nodelink

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/registry"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

const testAuthKey = "ffeeddccbbaa99887766554433221100ffeeddccbbaa99887766554433221100"

type testPlacer struct{}

func (testPlacer) Place(ctx context.Context, req registry.PlaceRequest) (*registry.Placement, error) {
	tok, err := clusterstate.DeriveAccessToken(testAuthKey, req.SandboxID)
	if err != nil {
		return nil, err
	}
	return &registry.Placement{NodeID: "n1", AccessToken: tok}, nil
}

// fakeNode implements Node: it streams its routes and, on a create command,
// "boots" the sandbox by adding a running route + emitting an upsert event — the
// same path a real node takes (the registry's Reserve waits on that route).
type fakeNode struct {
	mu     sync.Mutex
	routes map[string]routesync.RouteEntry
	events chan routesync.Event
}

type fixedSessionSequencer struct {
	called atomic.Bool
	tuple  routesync.SessionTuple
}

func (s *fixedSessionSequencer) NextSession(context.Context) (routesync.SessionTuple, error) {
	s.called.Store(true)
	return s.tuple, nil
}

func newFakeNode() *fakeNode {
	return &fakeNode{routes: map[string]routesync.RouteEntry{}, events: make(chan routesync.Event, 16)}
}

func TestCommandMatchesCurrentSession(t *testing.T) {
	identity := routesync.NodeRegister{NodeEpoch: 7, SessionSeq: 11}
	for _, tc := range []struct {
		name string
		cmd  *routesync.Command
		want bool
	}{
		{name: "legacy dormant path", cmd: &routesync.Command{}, want: true},
		{name: "exact", cmd: &routesync.Command{NodeEpoch: 7, SessionSeq: 11}, want: true},
		{name: "old epoch", cmd: &routesync.Command{NodeEpoch: 6, SessionSeq: 11}},
		{name: "old session", cmd: &routesync.Command{NodeEpoch: 7, SessionSeq: 10}},
		{name: "partial", cmd: &routesync.Command{NodeEpoch: 7}},
		{name: "nil", cmd: nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := commandMatchesSession(tc.cmd, identity); got != tc.want {
				t.Fatalf("match = %v, want %v", got, tc.want)
			}
		})
	}
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
func (n *fakeNode) Subscribe() (<-chan routesync.Event, func())          { return n.events, func() {} }
func (n *fakeNode) OnWake(ctx context.Context, wake routesync.RouteWake) {}
func (n *fakeNode) Policy() routesync.Policy                             { return routesync.Policy{} }
func (n *fakeNode) Heartbeat() *routesync.Heartbeat                      { return &routesync.Heartbeat{} }
func (n *fakeNode) BuildEvents() <-chan *routesync.BuildEvent            { return nil }

func (n *fakeNode) HandleCommand(ctx context.Context, cmd *routesync.Command) *routesync.CmdAck {
	ack := &routesync.CmdAck{CmdID: cmd.CmdID, Status: routesync.AckAccepted}
	if cmd.Kind != routesync.CmdCreate {
		return ack
	}
	e := routesync.RouteEntry{
		SandboxID: cmd.SID,
		State:     routesync.StateRunning, AccessToken: cmd.AccessToken,
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
	reg := registry.New(registry.NewStores(), testPlacer{}, 5*time.Second, log)

	mux := http.NewServeMux()
	mux.HandleFunc(routesync.NodeLinkPath, reg.ServeNodeLink)
	srv := httptest.NewServer(h2c.NewHandler(mux, &http2.Server{}))
	defer srv.Close()
	addr := srv.Listener.Addr().String()

	node := newFakeNode()
	client := New(
		func(ctx context.Context) (net.Conn, error) { return net.Dial("tcp", addr) },
		routesync.NodeRegister{NodeID: "n1", DataEndpoint: "10.0.0.1:8443"},
		node, 50*time.Millisecond, nil, log,
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

	var res *registry.ReserveResult
	var err error
	for {
		res, err = reg.ReserveSandbox(ctx, "/cell/proj/app/g1", "u1:sess1", nil)
		if err == nil {
			break
		}
		if !errors.Is(err, registry.ErrNodeGone) || time.Now().After(deadline) {
			t.Fatalf("reserve over node-link: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	want, err := clusterstate.DeriveAccessToken(testAuthKey, res.SID)
	if err != nil {
		t.Fatal(err)
	}
	if res.NodeID != "n1" || res.SID == "" || res.AccessToken != want {
		t.Fatalf("reserve result: %+v", res)
	}
}

func TestNodeLinkClientFollowsRedirect(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	reg := registry.New(registry.NewStores(), testPlacer{}, 5*time.Second, log)

	ownerMux := http.NewServeMux()
	ownerMux.HandleFunc(routesync.NodeLinkPath, reg.ServeNodeLink)
	ownerSrv := httptest.NewServer(h2c.NewHandler(ownerMux, &http2.Server{}))
	defer ownerSrv.Close()

	ingressMux := http.NewServeMux()
	ingressMux.HandleFunc(routesync.NodeLinkPath, func(w http.ResponseWriter, req *http.Request) {
		first, err := routesync.ReadMsg(req.Body)
		if err != nil || first.Type != routesync.TypeNodeRegister || first.NodeReg == nil {
			http.Error(w, "bad register", http.StatusBadRequest)
			return
		}
		if !first.NodeReg.AcceptRedirect {
			http.Error(w, "redirect not accepted", http.StatusConflict)
			return
		}
		if err := routesync.WriteMsg(w, &routesync.Msg{Type: routesync.TypeHello, Hello: &routesync.Hello{
			Version: routesync.Version,
			Redirect: &routesync.NodeLinkRedirect{Targets: []routesync.NodeLinkTarget{{
				MemberID: "owner", Endpoint: ownerSrv.URL,
			}}},
		}}); err != nil {
			return
		}
		w.(http.Flusher).Flush()
	})
	ingressSrv := httptest.NewServer(h2c.NewHandler(ingressMux, &http2.Server{}))
	defer ingressSrv.Close()

	node := newFakeNode()
	client := NewWithEndpoint(
		ingressSrv.URL,
		func(ctx context.Context, endpoint string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "tcp", endpoint)
		},
		routesync.NodeRegister{NodeID: "n1", DataEndpoint: "10.0.0.1:8443"},
		node, 50*time.Millisecond, nil, log, true,
	)
	go client.Run(ctx)

	deadline := time.Now().Add(3 * time.Second)
	for {
		if got, found, _ := reg.Stores().GetNode(ctx, "n1"); found && got.DataEndpoint == "10.0.0.1:8443" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("node never registered after redirect")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestNodeLinkPersistsAndStampsTupleBeforeDial(t *testing.T) {
	registered := make(chan routesync.NodeRegister, 1)
	mux := http.NewServeMux()
	mux.HandleFunc(routesync.NodeLinkPath, func(w http.ResponseWriter, req *http.Request) {
		first, err := routesync.ReadMsg(req.Body)
		if err != nil || first.NodeReg == nil {
			http.Error(w, "bad register", http.StatusBadRequest)
			return
		}
		registered <- *first.NodeReg
		_ = routesync.WriteMsg(w, &routesync.Msg{Type: routesync.TypeHello, Hello: &routesync.Hello{Version: routesync.Version}})
	})
	srv := httptest.NewServer(h2c.NewHandler(mux, &http2.Server{}))
	defer srv.Close()
	sequencer := &fixedSessionSequencer{tuple: routesync.SessionTuple{NodeEpoch: 7, SessionSeq: 12}}
	client := New(
		func(ctx context.Context) (net.Conn, error) {
			if !sequencer.called.Load() {
				return nil, errors.New("dial occurred before durable session sequence")
			}
			return (&net.Dialer{}).DialContext(ctx, "tcp", srv.Listener.Addr().String())
		},
		routesync.NodeRegister{NodeID: "node-1", DataEndpoint: "10.0.0.1:8443"},
		newFakeNode(), time.Second, nil, slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	client.SetSessionSequencer(sequencer)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = client.session(ctx, "")
	select {
	case got := <-registered:
		if got.Version != routesync.Version || got.NodeEpoch != 7 || got.SessionSeq != 12 {
			t.Fatalf("registered identity = %+v", got)
		}
	case <-ctx.Done():
		t.Fatal("node registration not received")
	}
}

func TestNodeLinkRejectsMismatchedHelloVersion(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc(routesync.NodeLinkPath, func(w http.ResponseWriter, req *http.Request) {
		first, err := routesync.ReadMsg(req.Body)
		if err != nil || first.NodeReg == nil || first.NodeReg.Version != routesync.Version {
			http.Error(w, "bad register", http.StatusBadRequest)
			return
		}
		_ = routesync.WriteMsg(w, &routesync.Msg{
			Type:  routesync.TypeHello,
			Hello: &routesync.Hello{Version: routesync.Version - 1},
		})
		w.(http.Flusher).Flush()
	})
	srv := httptest.NewServer(h2c.NewHandler(mux, &http2.Server{}))
	defer srv.Close()
	client := New(
		func(ctx context.Context) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "tcp", srv.Listener.Addr().String())
		},
		routesync.NodeRegister{NodeID: "node-1"}, newFakeNode(), time.Second, nil,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := client.session(ctx, ""); err == nil || err.Error() != "node-link: incompatible registry protocol version" {
		t.Fatalf("session error = %v", err)
	}
}

func TestFullJitterIsBounded(t *testing.T) {
	const max = 5 * time.Second
	for range 1000 {
		got := fullJitter(max)
		if got < 0 || got > max {
			t.Fatalf("fullJitter(%s) = %s", max, got)
		}
	}
	if got := fullJitter(0); got != 0 {
		t.Fatalf("fullJitter(0) = %s", got)
	}
}

func TestNodeLinkOutboxPrioritizesCommandThenDurableEventBeforeHeartbeat(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	outbox := make(chan *routesync.Msg, 2)
	highOut := make(chan *routesync.Msg, 1)
	eventOut := make(chan *routesync.Msg, 1)
	hbUpdate := make(chan struct{}, 1)
	var hbMu sync.Mutex
	heartbeat := &routesync.Msg{Type: routesync.TypeHeartbeat, Beat: &routesync.Heartbeat{Counts: 7}}
	latestHeartbeat := heartbeat

	highOut <- &routesync.Msg{Type: routesync.TypeCmdAck, Ack: &routesync.CmdAck{CmdID: "cmd-1", Status: routesync.AckAccepted}}
	eventOut <- &routesync.Msg{Type: routesync.TypeExecutionEvent, ExecutionEvent: &routesync.ExecutionEvent{ObjectID: "sandbox-1"}}
	hbUpdate <- struct{}{}
	go runNodeLinkOutbox(ctx, outbox, highOut, eventOut, hbUpdate, &hbMu, &latestHeartbeat)

	first := receiveOutboxMsg(t, outbox)
	if first.Type != routesync.TypeCmdAck || first.Ack == nil || first.Ack.CmdID != "cmd-1" {
		t.Fatalf("first outbox msg=%+v, want cmd_ack before heartbeat", first)
	}
	second := receiveOutboxMsg(t, outbox)
	if second.Type != routesync.TypeExecutionEvent || second.ExecutionEvent == nil || second.ExecutionEvent.ObjectID != "sandbox-1" {
		t.Fatalf("second outbox msg=%+v, want durable execution event", second)
	}
	third := receiveOutboxMsg(t, outbox)
	if third.Type != routesync.TypeHeartbeat || third.Beat == nil || third.Beat.Counts != 7 {
		t.Fatalf("third outbox msg=%+v, want latest heartbeat", third)
	}
}

func TestNodeLinkOutboxBoundsEventBurstBeforeHeartbeat(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	outbox := make(chan *routesync.Msg, 64)
	highOut := make(chan *routesync.Msg)
	eventOut := make(chan *routesync.Msg, 128)
	hbUpdate := make(chan struct{}, 1)
	var hbMu sync.Mutex
	latestHeartbeat := &routesync.Msg{Type: routesync.TypeHeartbeat, Beat: &routesync.Heartbeat{Counts: 9}}
	for index := 0; index < 100; index++ {
		eventOut <- &routesync.Msg{
			Type:           routesync.TypeExecutionEvent,
			ExecutionEvent: &routesync.ExecutionEvent{ObjectID: fmt.Sprintf("sandbox-%d", index)},
		}
	}
	hbUpdate <- struct{}{}
	go runNodeLinkOutbox(ctx, outbox, highOut, eventOut, hbUpdate, &hbMu, &latestHeartbeat)

	eventsBeforeHeartbeat := 0
	for {
		message := receiveOutboxMsg(t, outbox)
		if message.Type == routesync.TypeHeartbeat {
			break
		}
		eventsBeforeHeartbeat++
		if eventsBeforeHeartbeat > 32 {
			t.Fatal("sustained event backlog starved heartbeat")
		}
	}
	if eventsBeforeHeartbeat == 0 {
		t.Fatal("heartbeat bypassed the configured event-priority burst")
	}
}

func TestNodeLinkOutboxBoundsCommandACKBurstBeforeDurableEvent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	outbox := make(chan *routesync.Msg, 64)
	highOut := make(chan *routesync.Msg, 128)
	eventOut := make(chan *routesync.Msg, 1)
	hbUpdate := make(chan struct{}, 1)
	var hbMu sync.Mutex
	latestHeartbeat := &routesync.Msg{Type: routesync.TypeHeartbeat, Beat: &routesync.Heartbeat{Counts: 11}}
	for index := 0; index < 100; index++ {
		highOut <- &routesync.Msg{
			Type: routesync.TypeCmdAck,
			Ack:  &routesync.CmdAck{CmdID: fmt.Sprintf("cmd-%d", index), Status: routesync.AckAccepted},
		}
	}
	eventOut <- &routesync.Msg{
		Type:           routesync.TypeExecutionEvent,
		ExecutionEvent: &routesync.ExecutionEvent{ObjectID: "sandbox-durable"},
	}
	hbUpdate <- struct{}{}
	go runNodeLinkOutbox(ctx, outbox, highOut, eventOut, hbUpdate, &hbMu, &latestHeartbeat)

	acksBeforeEvent := 0
	for {
		message := receiveOutboxMsg(t, outbox)
		if message.Type == routesync.TypeExecutionEvent {
			break
		}
		acksBeforeEvent++
		if acksBeforeEvent > 32 {
			t.Fatal("sustained command ACK backlog starved durable event")
		}
	}
	if acksBeforeEvent == 0 {
		t.Fatal("durable event bypassed command ACK priority")
	}
}

type fakeDurableEventOutbox struct {
	mu      sync.Mutex
	events  []routesync.ExecutionEvent
	wake    chan struct{}
	queries int
}

func (f *fakeDurableEventOutbox) PendingExecutionEvents(
	_ context.Context,
	_ string,
	_ uint64,
	_ routesync.EventCursor,
	maxCount, _ int,
) ([]routesync.ExecutionEvent, routesync.EventCursor, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.queries++
	if maxCount > len(f.events) {
		maxCount = len(f.events)
	}
	events := append([]routesync.ExecutionEvent(nil), f.events[:maxCount]...)
	var next routesync.EventCursor
	if len(events) > 0 {
		last := events[len(events)-1]
		next = routesync.EventCursor{ObjectKind: last.ObjectKind, ObjectID: last.ObjectID}
	}
	return events, next, nil
}

func (f *fakeDurableEventOutbox) AckExecutionEvent(
	context.Context,
	string,
	uint64,
	routesync.EventAck,
) error {
	return nil
}

func (f *fakeDurableEventOutbox) EventWake() <-chan struct{} { return f.wake }

func TestDurableEventReplayStartsImmediatelyAndIsBatchBounded(t *testing.T) {
	durable := &fakeDurableEventOutbox{
		wake: make(chan struct{}, 1),
		events: []routesync.ExecutionEvent{
			{ObjectKind: "sandbox", ObjectID: "sandbox-1", NodeID: "node-1", NodeEpoch: 7,
				StorageGeneration: "generation-1", BindingDigest: strings.Repeat("a", 64), EventSeq: 2, State: "READY"},
			{ObjectKind: "build", ObjectID: "build-1", NodeID: "node-1", NodeEpoch: 7,
				StorageGeneration: "generation-1", BindingDigest: strings.Repeat("b", 64), EventSeq: 3, State: "BUILDING"},
		},
	}
	eventOut := make(chan *routesync.Msg, 2)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go runDurableEventReplay(ctx, durable, "node-1", 7, 1, 1<<20, time.Hour, eventOut,
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	message := receiveOutboxMsg(t, eventOut)
	if message.Type != routesync.TypeExecutionEvent || message.ExecutionEvent == nil ||
		message.ExecutionEvent.ObjectID != "sandbox-1" {
		t.Fatalf("replayed message = %+v", message)
	}
	select {
	case extra := <-eventOut:
		t.Fatalf("batch limit was ignored: %+v", extra)
	case <-time.After(20 * time.Millisecond):
	}
	durable.mu.Lock()
	queries := durable.queries
	durable.mu.Unlock()
	if queries != 1 {
		t.Fatalf("queries = %d, want one bounded startup batch", queries)
	}
}

func receiveOutboxMsg(t *testing.T, ch <-chan *routesync.Msg) *routesync.Msg {
	t.Helper()
	select {
	case msg := <-ch:
		return msg
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for outbox msg")
	}
	return nil
}
