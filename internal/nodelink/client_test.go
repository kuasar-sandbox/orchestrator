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

	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

type fakeNode struct {
	commands chan routesync.Command
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
	return &fakeNode{commands: make(chan routesync.Command, 16)}
}

func TestCommandMatchesCurrentSession(t *testing.T) {
	identity := routesync.NodeRegister{NodeEpoch: 7, SessionSeq: 11}
	for _, tc := range []struct {
		name string
		cmd  *routesync.Command
		want bool
	}{
		{name: "unfenced final command", cmd: &routesync.Command{}},
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
	if !commandMatchesSession(&routesync.Command{}, routesync.NodeRegister{}) {
		t.Fatal("legacy command did not match a legacy zero-tuple session")
	}
}

func (n *fakeNode) HandleCommand(ctx context.Context, cmd *routesync.Command) *routesync.CmdAck {
	select {
	case n.commands <- *cmd:
	case <-ctx.Done():
		return &routesync.CmdAck{CmdID: cmd.CmdID, Status: routesync.AckRejected, Outcome: routesync.DispatchUnknown}
	}
	return &routesync.CmdAck{CmdID: cmd.CmdID, Status: routesync.AckAccepted, Outcome: routesync.DispatchAcceptedAdmitted}
}

func (n *fakeNode) PlacementLoad(context.Context) (*routesync.PlacementLoadSnapshot, error) {
	return &routesync.PlacementLoadSnapshot{
		WaterZone: "green", SandboxSlotUsed: 2, SandboxRateTokenAvailable: true,
	}, nil
}

func TestNodeLinkPublishesPlacementAndHandlesCurrentSessionCommand(t *testing.T) {
	receivedLoad := make(chan routesync.PlacementLoadSnapshot, 1)
	receivedAck := make(chan routesync.CmdAck, 1)
	mux := http.NewServeMux()
	mux.HandleFunc(routesync.NodeLinkPath, func(w http.ResponseWriter, req *http.Request) {
		first, err := routesync.ReadMsg(req.Body)
		if err != nil || first.NodeReg == nil {
			http.Error(w, "bad register", http.StatusBadRequest)
			return
		}
		identity := *first.NodeReg
		_ = routesync.WriteMsg(w, &routesync.Msg{Type: routesync.TypeHello, Hello: &routesync.Hello{Version: routesync.Version}})
		w.(http.Flusher).Flush()
		load, err := routesync.ReadMsg(req.Body)
		if err != nil || load.Type != routesync.TypePlacementLoad || load.Load == nil {
			return
		}
		receivedLoad <- *load.Load
		_ = routesync.WriteMsg(w, &routesync.Msg{Type: routesync.TypeCommand, Cmd: &routesync.Command{
			CmdID: "cmd-1", Kind: routesync.CmdSandboxResume,
			NodeEpoch: identity.NodeEpoch, SessionSeq: identity.SessionSeq,
		}})
		w.(http.Flusher).Flush()
		ack, err := routesync.ReadMsg(req.Body)
		if err == nil && ack.Type == routesync.TypeCmdAck && ack.Ack != nil {
			receivedAck <- *ack.Ack
		}
	})
	srv := httptest.NewServer(h2c.NewHandler(mux, &http2.Server{}))
	defer srv.Close()

	node := newFakeNode()
	client := New(
		srv.URL,
		func(ctx context.Context, endpoint string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "tcp", endpoint)
		},
		routesync.NodeRegister{
			NodeID: "n1", NodeEpoch: 7, SessionSeq: 11, DataEndpoint: "10.0.0.1:8443",
			Capacity: 8, LoadModelVersion: 1, RuntimeDigest: "runtime-v1",
		},
		node, &fixedSessionSequencer{tuple: routesync.SessionTuple{NodeEpoch: 7, SessionSeq: 11}},
		newFakeDurableEventOutbox(), time.Hour, nil, slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go func() { _ = client.session(ctx, srv.URL) }()

	select {
	case load := <-receivedLoad:
		if load.NodeID != "n1" || load.NodeEpoch != 7 || load.SessionSeq != 11 ||
			load.SampleSeq != 1 || load.SandboxSlotCapacity != 8 || load.SandboxSlotUsed != 2 {
			t.Fatalf("placement snapshot = %+v", load)
		}
	case <-ctx.Done():
		t.Fatal("placement snapshot not received")
	}
	select {
	case ack := <-receivedAck:
		if ack.CmdID != "cmd-1" || ack.Status != routesync.AckAccepted || ack.Outcome != routesync.DispatchAcceptedAdmitted {
			t.Fatalf("command acknowledgement = %+v", ack)
		}
	case <-ctx.Done():
		t.Fatal("command acknowledgement not received")
	}
}

func TestNodeLinkClientFollowsRedirect(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	registered := make(chan routesync.NodeRegister, 1)
	ownerMux := http.NewServeMux()
	ownerMux.HandleFunc(routesync.NodeLinkPath, func(w http.ResponseWriter, req *http.Request) {
		first, err := routesync.ReadMsg(req.Body)
		if err != nil || first.NodeReg == nil {
			http.Error(w, "bad register", http.StatusBadRequest)
			return
		}
		registered <- *first.NodeReg
		_ = routesync.WriteMsg(w, &routesync.Msg{Type: routesync.TypeHello, Hello: &routesync.Hello{Version: routesync.Version}})
		w.(http.Flusher).Flush()
		<-req.Context().Done()
	})
	ownerSrv := httptest.NewServer(h2c.NewHandler(ownerMux, &http2.Server{}))
	defer ownerSrv.Close()

	ingressMux := http.NewServeMux()
	ingressMux.HandleFunc(routesync.NodeLinkPath, func(w http.ResponseWriter, req *http.Request) {
		first, err := routesync.ReadMsg(req.Body)
		if err != nil || first.Type != routesync.TypeNodeRegister || first.NodeReg == nil {
			http.Error(w, "bad register", http.StatusBadRequest)
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
	client := New(
		ingressSrv.URL,
		func(ctx context.Context, endpoint string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "tcp", endpoint)
		},
		routesync.NodeRegister{
			NodeID: "n1", NodeEpoch: 7, SessionSeq: 11, DataEndpoint: "10.0.0.1:8443",
			Capacity: 8, LoadModelVersion: 1,
		},
		node, &fixedSessionSequencer{tuple: routesync.SessionTuple{NodeEpoch: 7, SessionSeq: 11}},
		newFakeDurableEventOutbox(), 50*time.Millisecond, nil, log,
	)
	go client.Run(ctx)

	select {
	case got := <-registered:
		if got.NodeID != "n1" || got.DataEndpoint != "10.0.0.1:8443" {
			t.Fatalf("redirected registration = %+v", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("node never registered after redirect")
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
		srv.URL,
		func(ctx context.Context, endpoint string) (net.Conn, error) {
			if !sequencer.called.Load() {
				return nil, errors.New("dial occurred before durable session sequence")
			}
			return (&net.Dialer{}).DialContext(ctx, "tcp", endpoint)
		},
		routesync.NodeRegister{NodeID: "node-1", DataEndpoint: "10.0.0.1:8443"},
		newFakeNode(), sequencer, newFakeDurableEventOutbox(), time.Second, nil,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = client.session(ctx, srv.URL)
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
		srv.URL,
		func(ctx context.Context, endpoint string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "tcp", endpoint)
		},
		routesync.NodeRegister{NodeID: "node-1"}, newFakeNode(),
		&fixedSessionSequencer{tuple: routesync.SessionTuple{NodeEpoch: 7, SessionSeq: 11}},
		newFakeDurableEventOutbox(), time.Second, nil,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := client.session(ctx, srv.URL); err == nil || err.Error() != "node-link: incompatible registry protocol version" {
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

func TestNodeLinkOutboxPrioritizesCommandThenDurableEventBeforePlacementLoad(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	outbox := make(chan *routesync.Msg, 2)
	highOut := make(chan *routesync.Msg, 1)
	eventOut := make(chan *routesync.Msg, 1)
	loadUpdate := make(chan struct{}, 1)
	var loadMu sync.Mutex
	placementLoad := &routesync.Msg{Type: routesync.TypePlacementLoad, Load: &routesync.PlacementLoadSnapshot{SandboxSlotUsed: 7}}
	latestLoad := placementLoad

	highOut <- &routesync.Msg{Type: routesync.TypeCmdAck, Ack: &routesync.CmdAck{CmdID: "cmd-1", Status: routesync.AckAccepted}}
	eventOut <- &routesync.Msg{Type: routesync.TypeExecutionEvent, ExecutionEvent: &routesync.ExecutionEvent{ObjectID: "sandbox-1"}}
	loadUpdate <- struct{}{}
	go runNodeLinkOutbox(ctx, outbox, highOut, eventOut, loadUpdate, &loadMu, &latestLoad)

	first := receiveOutboxMsg(t, outbox)
	if first.Type != routesync.TypeCmdAck || first.Ack == nil || first.Ack.CmdID != "cmd-1" {
		t.Fatalf("first outbox msg=%+v, want cmd_ack before placement load", first)
	}
	second := receiveOutboxMsg(t, outbox)
	if second.Type != routesync.TypeExecutionEvent || second.ExecutionEvent == nil || second.ExecutionEvent.ObjectID != "sandbox-1" {
		t.Fatalf("second outbox msg=%+v, want durable execution event", second)
	}
	third := receiveOutboxMsg(t, outbox)
	if third.Type != routesync.TypePlacementLoad || third.Load == nil || third.Load.SandboxSlotUsed != 7 {
		t.Fatalf("third outbox msg=%+v, want latest placement load", third)
	}
}

func TestNodeLinkOutboxBoundsEventBurstBeforePlacementLoad(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	outbox := make(chan *routesync.Msg, 64)
	highOut := make(chan *routesync.Msg)
	eventOut := make(chan *routesync.Msg, 128)
	loadUpdate := make(chan struct{}, 1)
	var loadMu sync.Mutex
	latestLoad := &routesync.Msg{Type: routesync.TypePlacementLoad, Load: &routesync.PlacementLoadSnapshot{SandboxSlotUsed: 9}}
	for index := 0; index < 100; index++ {
		eventOut <- &routesync.Msg{
			Type:           routesync.TypeExecutionEvent,
			ExecutionEvent: &routesync.ExecutionEvent{ObjectID: fmt.Sprintf("sandbox-%d", index)},
		}
	}
	loadUpdate <- struct{}{}
	go runNodeLinkOutbox(ctx, outbox, highOut, eventOut, loadUpdate, &loadMu, &latestLoad)

	eventsBeforeLoad := 0
	for {
		message := receiveOutboxMsg(t, outbox)
		if message.Type == routesync.TypePlacementLoad {
			break
		}
		eventsBeforeLoad++
		if eventsBeforeLoad > 32 {
			t.Fatal("sustained event backlog starved placement load")
		}
	}
	if eventsBeforeLoad == 0 {
		t.Fatal("placement load bypassed the configured event-priority burst")
	}
}

func TestNodeLinkOutboxBoundsCommandACKBurstBeforeDurableEvent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	outbox := make(chan *routesync.Msg, 64)
	highOut := make(chan *routesync.Msg, 128)
	eventOut := make(chan *routesync.Msg, 1)
	loadUpdate := make(chan struct{}, 1)
	var loadMu sync.Mutex
	latestLoad := &routesync.Msg{Type: routesync.TypePlacementLoad, Load: &routesync.PlacementLoadSnapshot{SandboxSlotUsed: 11}}
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
	loadUpdate <- struct{}{}
	go runNodeLinkOutbox(ctx, outbox, highOut, eventOut, loadUpdate, &loadMu, &latestLoad)

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

func newFakeDurableEventOutbox() *fakeDurableEventOutbox {
	return &fakeDurableEventOutbox{wake: make(chan struct{}, 1)}
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
				RegistryGeneration: "generation-1", BindingDigest: strings.Repeat("a", 64), EventSeq: 2, State: "READY"},
			{ObjectKind: "build", ObjectID: "build-1", NodeID: "node-1", NodeEpoch: 7,
				RegistryGeneration: "generation-1", BindingDigest: strings.Repeat("b", 64), EventSeq: 3, State: "BUILDING"},
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
