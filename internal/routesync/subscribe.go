package routesync

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"time"

	"golang.org/x/net/http2"
)

// Sink applies inbound route messages on the subscriber side (the route table).
type Sink interface {
	// BeginSync marks the start of a fresh sync stream: entries from a prior stream
	// are tentatively stale until re-applied via ApplyUpsert before the Bookmark.
	BeginSync()
	ApplyUpsert(r RouteEntry) error
	ApplyDelete(sid string)
	// Bookmark marks the initial route stream complete: the table is synced, and
	// entries not seen since the matching BeginSync are dropped (deleted while
	// disconnected).
	Bookmark()
	SetPolicy(p Policy)
}

// InvalidatableSink optionally fails sensitive synchronized state closed as
// soon as a session ends, including during reconnect backoff.
type InvalidatableSink interface {
	InvalidateSync()
}

// WakeSource yields sandbox ids the subscriber wants the orchestrator to resume. It
// blocks until a wake is available or ctx is done (ok=false on ctx done). A nil
// WakeSource means the subscriber issues no wakes (a pure route observer).
type WakeSource interface {
	NextWake(ctx context.Context) (sid string, ok bool)
}

// Subscriber is the subscriber side of the route stream (the proxy master's shared
// route table, or a route observer). It dials the orchestrator's config-socket, registers
// (PUT /internal/plugin/{id}/register) with its caps, and keeps its Sink in sync over
// a persistent, auto-reconnecting bidi h2c stream — forwarding Wakes up for route_wake.
type Subscriber struct {
	dial  func(ctx context.Context) (net.Conn, error)
	id    string
	reg   Register
	sink  Sink
	wakes WakeSource // nil if this subscriber issues no wakes
	log   *slog.Logger
}

// NewSubscriber builds a Subscriber. dial returns a fresh connection to the
// orchestrator's config-socket (e.g. a unix dial). wakes may be nil.
func NewSubscriber(dial func(ctx context.Context) (net.Conn, error), id string, reg Register, sink Sink, wakes WakeSource, log *slog.Logger) *Subscriber {
	return &Subscriber{dial: dial, id: id, reg: reg, sink: sink, wakes: wakes, log: log}
}

// Run keeps a single registration/sync session alive (reconnecting with capped
// backoff) until ctx is cancelled.
func (s *Subscriber) Run(ctx context.Context) {
	tr := &http2.Transport{
		AllowHTTP: true,
		DialTLSContext: func(ctx context.Context, _, _ string, _ *tls.Config) (net.Conn, error) {
			return s.dial(ctx)
		},
	}
	defer tr.CloseIdleConnections()
	backoff := 200 * time.Millisecond
	for ctx.Err() == nil {
		err := s.session(ctx, tr)
		if ctx.Err() != nil {
			return
		}
		s.log.Warn("routesync: registration session ended; reconnecting", "id", s.id, "err", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, 5*time.Second)
	}
}

// session runs one registration: it PUTs the register stream (request body =
// Register then Wakes/RouteBarrierAcks) and applies the down stream (response body
// = Hello, Upserts, Bookmark, deltas/barriers), full-duplex.
func (s *Subscriber) session(ctx context.Context, tr *http2.Transport) error {
	sctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if invalidatable, ok := s.sink.(InvalidatableSink); ok {
		defer invalidatable.InvalidateSync()
	}

	pr, pw := io.Pipe()
	req, err := http.NewRequestWithContext(sctx, http.MethodPut, "http://orch"+PluginRegisterPath(s.id), pr)
	if err != nil {
		return err
	}

	// One writer owns the request body after Register. Wake production and
	// barrier application both enqueue frames here, so they can never race writes
	// to the length-prefixed stream.
	up := make(chan *Msg)
	if s.wakes != nil {
		go s.forwardWakes(sctx, up)
	}
	go func() { pw.CloseWithError(s.writeUp(sctx, pw, up)) }()

	resp, err := tr.RoundTrip(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// The first down frame is the versioned Hello. Fail closed before beginning a
	// sync generation or applying any route business frame.
	hello, err := ReadMsg(resp.Body)
	if err != nil {
		return err
	}
	if err := ValidateHello(hello); err != nil {
		return err
	}

	// Reader: preserve the established BeginSync-before-policy callback order,
	// then apply route frames until EOF/error.
	s.sink.BeginSync()
	s.sink.SetPolicy(hello.Hello.Policy)
	for {
		m, err := ReadMsg(resp.Body)
		if err != nil {
			return err
		}
		if err := s.apply(sctx, m, up); err != nil {
			return err
		}
	}
}

// writeUp is the session's sole up-stream writer. It sends Register first, then
// serializes Wake and RouteBarrierAck frames until the session ends.
func (s *Subscriber) writeUp(ctx context.Context, w io.Writer, up <-chan *Msg) error {
	r := s.reg
	if err := WriteMsg(w, &Msg{Type: TypeRegister, Register: &r}); err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case m := <-up:
			if m != nil {
				if err := WriteMsg(w, m); err != nil {
					return err
				}
			}
		}
	}
}

func (s *Subscriber) forwardWakes(ctx context.Context, up chan<- *Msg) {
	for {
		sid, ok := s.wakes.NextWake(ctx)
		if !ok {
			return
		}
		if !enqueueUp(ctx, up, &Msg{Type: TypeWake, SID: sid}) {
			return
		}
	}
}

func enqueueUp(ctx context.Context, up chan<- *Msg, m *Msg) bool {
	select {
	case up <- m:
		return true
	case <-ctx.Done():
		return false
	}
}

func (s *Subscriber) apply(ctx context.Context, m *Msg, up chan<- *Msg) error {
	switch m.Type {
	case TypeHello:
		return fmt.Errorf("routesync: unexpected hello frame after handshake")
	case TypeUpsert:
		if m.Route != nil {
			if err := s.sink.ApplyUpsert(*m.Route); err != nil {
				return fmt.Errorf("routesync: apply upsert %s: %w", m.Route.SandboxID, err)
			}
		}
	case TypeDelete:
		s.sink.ApplyDelete(m.SID)
	case TypeBookmark:
		s.sink.Bookmark()
	case TypeRouteBarrier:
		if m.BarrierID == "" {
			return fmt.Errorf("routesync: empty route barrier id")
		}
		if !s.reg.handlesWake() {
			return nil
		}
		if !enqueueUp(ctx, up, &Msg{Type: TypeRouteBarrierAck, BarrierID: m.BarrierID}) {
			return ctx.Err()
		}
	default:
		s.log.Warn("routesync: unknown message", "type", m.Type)
	}
	return nil
}
