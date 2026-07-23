package controlplane

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/session"
)

const (
	defaultDispatchTimeout = 5 * time.Second
	defaultEventWorkers    = 32
	defaultReconnectRate   = 200
	initialRegistrationTTL = 5 * time.Second
)

// ExecutionEventSink converges one node-authoritative fact through its owning
// Route/Build shard. Success means the same or a newer fact is durably committed
// and the exact event may be acknowledged.
type ExecutionEventSink interface {
	ConvergeExecutionEvent(context.Context, routesync.ExecutionEvent) error
}

// ReconnectRouter is consulted only before a new node-link session is
// installed. An empty target list means this member is the selected Holder.
type ReconnectRouter interface {
	RedirectTargets(string) ([]routesync.NodeLinkTarget, error)
}

type NodeLinkServer struct {
	holder           *session.Holder
	store            *RaftStore
	events           ExecutionEventSink
	reconnect        ReconnectRouter
	dispatchTimeout  time.Duration
	eventSlots       chan struct{}
	reconnectLimiter *reconnectLimiter
	log              *slog.Logger
}

func (s *NodeLinkServer) SetReconnectRouter(router ReconnectRouter) { s.reconnect = router }

func (s *NodeLinkServer) SetLimits(eventWorkers, reconnectPerSecond int) error {
	if eventWorkers <= 0 || eventWorkers > 4096 || reconnectPerSecond <= 0 || reconnectPerSecond > 100_000 {
		return errors.New("controlplane: invalid node-link worker or reconnect bound")
	}
	s.eventSlots = make(chan struct{}, eventWorkers)
	s.reconnectLimiter = newReconnectLimiter(reconnectPerSecond, time.Now())
	return nil
}

func NewNodeLinkServer(holder *session.Holder, store *RaftStore, events ExecutionEventSink, log *slog.Logger) (*NodeLinkServer, error) {
	if holder == nil || store == nil || events == nil {
		return nil, errors.New("controlplane: node-link requires Holder, Raft store, and event sink")
	}
	if log == nil {
		log = slog.Default()
	}
	return &NodeLinkServer{
		holder: holder, store: store, events: events, dispatchTimeout: defaultDispatchTimeout,
		eventSlots:       make(chan struct{}, defaultEventWorkers),
		reconnectLimiter: newReconnectLimiter(defaultReconnectRate, time.Now()), log: log,
	}, nil
}

func (s *NodeLinkServer) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPut || request.URL.Path != routesync.NodeLinkPath {
		http.NotFound(w, request)
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "node-link requires a flushable HTTP/2 response", http.StatusInternalServerError)
		return
	}
	first, err := readInitialNodeRegistration(request.Context(), request.Body)
	if err != nil || first == nil || first.Type != routesync.TypeNodeRegister || first.NodeReg == nil {
		http.Error(w, "node-link requires node_register as its first frame", http.StatusBadRequest)
		return
	}
	registration, err := registrationFromWire(*first.NodeReg)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if !s.reconnectLimiter.Allow(time.Now()) {
		w.Header().Set("Retry-After", "1")
		http.Error(w, "node-link reconnect rate exceeded", http.StatusTooManyRequests)
		return
	}
	if s.reconnect != nil {
		targets, routeErr := s.reconnect.RedirectTargets(registration.NodeID)
		if routeErr != nil {
			http.Error(w, "node-link Holder selection unavailable", http.StatusServiceUnavailable)
			return
		}
		if len(targets) > 0 {
			w.Header().Set("Content-Type", "application/octet-stream")
			w.WriteHeader(http.StatusOK)
			_ = routesync.WriteMsg(w, &routesync.Msg{Type: routesync.TypeHello, Hello: &routesync.Hello{
				Version: routesync.Version, Redirect: &routesync.NodeLinkRedirect{Targets: targets},
			}})
			flusher.Flush()
			return
		}
	}

	ctx, cancel := context.WithCancel(request.Context())
	endpoint := newWireEndpoint(ctx, cancel, registration, s.dispatchTimeout)
	lease, err := s.holder.Register(ctx, registration, endpoint)
	if err != nil {
		cancel()
		http.Error(w, "node-link registration rejected", http.StatusConflict)
		return
	}
	defer lease.Close()
	defer endpoint.FenceStaleSession()

	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	if err := routesync.WriteMsg(w, &routesync.Msg{Type: routesync.TypeHello, Hello: &routesync.Hello{Version: routesync.Version}}); err != nil {
		return
	}
	flusher.Flush()

	readerDone := make(chan error, 1)
	go func() {
		err := s.readLoop(ctx, request.Body, endpoint, registration)
		readerDone <- err
		cancel()
	}()

	writeErr := endpoint.writeLoop(w, flusher.Flush)
	cancel()
	if writeErr != nil && !errors.Is(writeErr, context.Canceled) && ctx.Err() == nil {
		s.log.Warn("node-link write ended", "node", registration.NodeID, "err", writeErr)
	}
	select {
	case readErr := <-readerDone:
		if readErr != nil && !errors.Is(readErr, io.EOF) && !errors.Is(readErr, context.Canceled) &&
			request.Context().Err() == nil {
			s.log.Warn("node-link read ended", "node", registration.NodeID, "err", readErr)
		}
	default:
	}
}

func readInitialNodeRegistration(ctx context.Context, body io.ReadCloser) (*routesync.Msg, error) {
	readCtx, cancel := context.WithTimeout(ctx, initialRegistrationTTL)
	defer cancel()
	stopCancellation := context.AfterFunc(readCtx, func() { _ = body.Close() })
	message, err := routesync.ReadMsg(body)
	if !stopCancellation() || readCtx.Err() != nil {
		if contextErr := readCtx.Err(); contextErr != nil {
			return nil, contextErr
		}
	}
	return message, err
}

type reconnectLimiter struct {
	mu     sync.Mutex
	rate   float64
	burst  float64
	tokens float64
	last   time.Time
}

func newReconnectLimiter(perSecond int, now time.Time) *reconnectLimiter {
	rate := float64(max(perSecond, 1))
	return &reconnectLimiter{rate: rate, burst: rate, tokens: rate, last: now}
}

func (l *reconnectLimiter) Allow(now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if now.Before(l.last) {
		now = l.last
	}
	elapsed := now.Sub(l.last).Seconds()
	l.tokens = min(l.burst, l.tokens+elapsed*l.rate)
	l.last = now
	if l.tokens < 1 {
		return false
	}
	l.tokens--
	return true
}

func registrationFromWire(node routesync.NodeRegister) (session.Registration, error) {
	if node.Version != routesync.Version || node.Capacity <= 0 || node.NodeEpoch == 0 || node.SessionSeq == 0 {
		return session.Registration{}, errors.New("node-link registration has an invalid protocol, tuple, or Sandbox capacity")
	}
	registration := session.Registration{
		NodeID: node.NodeID, EnrollmentID: node.EnrollmentID,
		Tuple:        session.Tuple{NodeEpoch: node.NodeEpoch, SessionSeq: node.SessionSeq},
		DataEndpoint: node.DataEndpoint, RuntimeDigest: node.RuntimeDigest,
		Labels: cloneStrings(node.Labels), Capabilities: cloneBools(node.Capabilities),
		FailureDomain: node.FailureDomain, LoadModelVersion: node.LoadModelVersion,
		SandboxSlots: uint64(node.Capacity), Draining: node.Draining,
	}
	if build := node.BuildCapacity; build != nil {
		if build.Slots < 0 || build.CPU < 0 || build.Mem < 0 || build.Storage < 0 {
			return session.Registration{}, errors.New("node-link registration contains a negative Build capacity")
		}
		registration.BuildSlots = uint64(build.Slots)
		registration.BuildCPU = uint64(build.CPU)
		registration.BuildMemory = uint64(build.Mem)
		registration.BuildStorage = uint64(build.Storage)
	}
	if err := registration.Validate(); err != nil {
		return session.Registration{}, err
	}
	return registration, nil
}

func (s *NodeLinkServer) readLoop(
	ctx context.Context,
	body io.Reader,
	endpoint *wireEndpoint,
	registration session.Registration,
) error {
	for {
		message, err := routesync.ReadMsg(body)
		if err != nil {
			return err
		}
		switch message.Type {
		case routesync.TypeCmdAck:
			if message.Ack == nil {
				return errors.New("node-link received an empty command acknowledgement")
			}
			endpoint.acceptAck(*message.Ack)
		case routesync.TypePlacementLoad:
			if message.Load == nil {
				return errors.New("node-link received an empty PlacementLoadSnapshot")
			}
			if err := s.holder.UpdateSnapshot(registration.Tuple, *message.Load); err != nil {
				return fmt.Errorf("node-link rejected PlacementLoadSnapshot: %w", err)
			}
		case routesync.TypeExecutionEvent:
			if message.ExecutionEvent == nil {
				return errors.New("node-link received an empty execution event")
			}
			event := *message.ExecutionEvent
			if err := event.Validate(); err != nil || event.NodeID != registration.NodeID || event.NodeEpoch != registration.NodeEpoch {
				return errors.New("node-link received an invalid or cross-session execution event")
			}
			select {
			case s.eventSlots <- struct{}{}:
			case <-ctx.Done():
				return ctx.Err()
			}
			go s.convergeEvent(ctx, endpoint, event)
		default:
			return fmt.Errorf("node-link received removed message type %q", message.Type)
		}
	}
}

func (s *NodeLinkServer) convergeEvent(ctx context.Context, endpoint *wireEndpoint, event routesync.ExecutionEvent) {
	defer func() { <-s.eventSlots }()
	identity, err := s.store.ServeIdentity()
	if err != nil || identity.RegistryGeneration != event.RegistryGeneration ||
		s.store.AuthorizeEvent(identity, false) != nil || s.store.AuthorizeNodeSession(identity, endpoint.reg) != nil {
		return
	}
	if err := s.events.ConvergeExecutionEvent(ctx, event); err != nil {
		if ctx.Err() == nil {
			s.log.Warn("node-link event convergence failed", "node", event.NodeID, "kind", event.ObjectKind,
				"object", event.ObjectID, "event_seq", event.EventSeq, "err", err)
		}
		return
	}
	if s.store.AuthorizeEvent(identity, true) != nil || s.store.AuthorizeNodeSession(identity, endpoint.reg) != nil {
		return
	}
	endpoint.enqueue(&routesync.Msg{Type: routesync.TypeEventAck, EventAck: &routesync.EventAck{
		ObjectKind: event.ObjectKind, ObjectID: event.ObjectID, RegistryGeneration: event.RegistryGeneration,
		BindingDigest: event.BindingDigest, EventSeq: event.EventSeq,
	}})
}

type outboundMessage struct {
	message *routesync.Msg
	written chan error
}

type wireEndpoint struct {
	ctx      context.Context
	cancel   context.CancelFunc
	reg      session.Registration
	timeout  time.Duration
	out      chan outboundMessage
	pendingM sync.Mutex
	pending  map[string]chan routesync.CmdAck
	once     sync.Once
}

func newWireEndpoint(ctx context.Context, cancel context.CancelFunc, registration session.Registration, timeout time.Duration) *wireEndpoint {
	return &wireEndpoint{
		ctx: ctx, cancel: cancel, reg: registration, timeout: timeout,
		out: make(chan outboundMessage, 128), pending: make(map[string]chan routesync.CmdAck),
	}
}

func (e *wireEndpoint) FenceStaleSession() {
	e.once.Do(e.cancel)
}

func (e *wireEndpoint) AdmitAndDispatch(ctx context.Context, command session.DispatchCommand) (session.DispatchReply, error) {
	wire, err := dispatchCommandToWire(command)
	if err != nil {
		return session.DispatchReply{}, err
	}
	ack, sent, err := e.sendCommand(ctx, wire)
	if !sent {
		return session.DispatchReply{Outcome: clusterstate.DispatchSessionMoved, Reason: "node-link session closed before command send"}, nil
	}
	if err != nil {
		return session.DispatchReply{Outcome: clusterstate.DispatchUnknown, Reason: err.Error()}, nil
	}
	outcome := clusterstate.DispatchOutcome(ack.Outcome)
	if err := outcome.Validate(); err != nil {
		return session.DispatchReply{Outcome: clusterstate.DispatchUnknown, Reason: "node returned an invalid dispatch outcome"}, nil
	}
	return session.DispatchReply{Outcome: outcome, Reason: ack.Reason}, nil
}

func (e *wireEndpoint) SendNodeCommand(ctx context.Context, command *routesync.Command) (routesync.CmdAck, bool, error) {
	return e.sendCommand(ctx, command)
}

func (e *wireEndpoint) sendCommand(ctx context.Context, command *routesync.Command) (routesync.CmdAck, bool, error) {
	if command == nil {
		return routesync.CmdAck{}, false, errors.New("node-link command is required")
	}
	if command.CmdID == "" {
		command.CmdID = newCommandID()
	}
	waiter := make(chan routesync.CmdAck, 1)
	e.pendingM.Lock()
	if _, exists := e.pending[command.CmdID]; exists {
		e.pendingM.Unlock()
		return routesync.CmdAck{}, false, errors.New("node-link command correlation ID collision")
	}
	e.pending[command.CmdID] = waiter
	e.pendingM.Unlock()
	defer func() {
		e.pendingM.Lock()
		delete(e.pending, command.CmdID)
		e.pendingM.Unlock()
	}()

	written := make(chan error, 1)
	message := outboundMessage{message: &routesync.Msg{Type: routesync.TypeCommand, Cmd: command}, written: written}
	select {
	case e.out <- message:
	case <-ctx.Done():
		return routesync.CmdAck{}, false, ctx.Err()
	case <-e.ctx.Done():
		return routesync.CmdAck{}, false, session.ErrSessionUnavailable
	}
	select {
	case err := <-written:
		if err != nil {
			return routesync.CmdAck{}, true, err
		}
	case <-ctx.Done():
		return routesync.CmdAck{}, true, ctx.Err()
	case <-e.ctx.Done():
		return routesync.CmdAck{}, true, session.ErrSessionUnavailable
	}

	timeout := e.timeout
	if timeout <= 0 {
		timeout = defaultDispatchTimeout
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case ack := <-waiter:
		return ack, true, nil
	case <-timer.C:
		return routesync.CmdAck{}, true, context.DeadlineExceeded
	case <-ctx.Done():
		return routesync.CmdAck{}, true, ctx.Err()
	case <-e.ctx.Done():
		return routesync.CmdAck{}, true, session.ErrSessionUnavailable
	}
}

func (e *wireEndpoint) acceptAck(ack routesync.CmdAck) {
	if ack.CmdID == "" || (ack.Status != routesync.AckAccepted && ack.Status != routesync.AckRejected) {
		return
	}
	e.pendingM.Lock()
	waiter := e.pending[ack.CmdID]
	e.pendingM.Unlock()
	if waiter == nil {
		return
	}
	select {
	case waiter <- ack:
	default:
	}
}

func (e *wireEndpoint) enqueue(message *routesync.Msg) bool {
	if message == nil {
		return true
	}
	select {
	case e.out <- outboundMessage{message: message}:
		return true
	case <-e.ctx.Done():
		return false
	}
}

func (e *wireEndpoint) writeLoop(writer io.Writer, flush func()) error {
	for {
		select {
		case <-e.ctx.Done():
			return e.ctx.Err()
		case outbound := <-e.out:
			err := routesync.WriteMsg(writer, outbound.message)
			if err == nil && flush != nil {
				flush()
			}
			if outbound.written != nil {
				outbound.written <- err
			}
			if err != nil {
				e.cancel()
				return err
			}
		}
	}
}

func dispatchCommandToWire(command session.DispatchCommand) (*routesync.Command, error) {
	if err := command.Intent.Validate(); err != nil {
		return nil, err
	}
	if err := command.Binding.ValidateWorkflow(command.Kind, command.ObjectID, command.Group, command.RouteKey, command.Intent); err != nil {
		return nil, err
	}
	wire := &routesync.Command{
		NodeEpoch: command.NodeEpoch, SessionSeq: command.SessionSeq,
		RegistryGeneration: command.Binding.RegistryGeneration,
		Binding:            command.Binding.OpaqueBinding, BindingDigest: command.Binding.BindingDigest,
		DemandDigest: command.Intent.DemandDigest, DispatchSpecDigest: command.Intent.DispatchSpecDigest,
		Group: command.Group, RouteKey: command.RouteKey,
		NormalizedDemand: append([]byte(nil), command.Intent.NormalizedDemand...),
		DispatchSpec:     append([]byte(nil), command.Intent.DispatchSpec...),
		ProviderPolicy:   command.Intent.ProviderPolicyVersion,
	}
	switch command.Kind {
	case clusterstate.ExecutionKindSandbox:
		wire.Kind, wire.SID = routesync.CmdSandboxAdmitDispatch, command.ObjectID
	case clusterstate.ExecutionKindBuild:
		wire.Kind, wire.BuildID = routesync.CmdBuildAdmitDispatch, command.ObjectID
	default:
		return nil, errors.New("node-link dispatch has an unsupported execution kind")
	}
	return wire, nil
}

func newCommandID() string {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return fmt.Sprintf("cmd-%d", time.Now().UnixNano())
	}
	return "cmd-" + hex.EncodeToString(value[:])
}
