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
	ApplyUpsert(r RouteEntry)
	ApplyDelete(RouteDelete)
	// Bookmark marks the initial route stream complete: the table is synced, and
	// entries not seen since the matching BeginSync are dropped (deleted while
	// disconnected).
	Bookmark()
	SetPolicy(p Policy)
}

// WakeSource yields exact executions the subscriber wants the orchestrator to
// resume. It blocks until a wake is available or ctx is done (ok=false on ctx
// done). A nil WakeSource means the subscriber issues no wakes.
type WakeSource interface {
	NextWake(ctx context.Context) (wake RouteWake, ok bool)
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
	reg.Version = Version
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
// Register then Wakes) and applies the down stream (response body = Hello, Upserts,
// Bookmark, deltas), full-duplex.
func (s *Subscriber) session(ctx context.Context, tr *http2.Transport) error {
	sctx, cancel := context.WithCancel(ctx)
	defer cancel()

	pr, pw := io.Pipe()
	req, err := http.NewRequestWithContext(sctx, http.MethodPut, "http://orch"+PluginRegisterPath(s.id), pr)
	if err != nil {
		return err
	}

	// Writer goroutine: Register frame, then (route_wake) Wake frames. Closing pw
	// ends the request.
	go func() { pw.CloseWithError(s.writeUp(sctx, pw)) }()

	resp, err := tr.RoundTrip(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Validate the authority version before touching the current route-table sync
	// epoch. Mixed versions fail closed and reconnect only after both sides
	// run the exact protocol.
	hello, err := ReadMsg(resp.Body)
	if err != nil {
		return err
	}
	if hello.Type != TypeHello || hello.Hello == nil || hello.Hello.Version != Version {
		got := 0
		if hello.Hello != nil {
			got = hello.Hello.Version
		}
		return fmt.Errorf("routesync: authority protocol version %d is incompatible with required version %d", got, Version)
	}
	s.sink.BeginSync()
	s.sink.SetPolicy(hello.Hello.Policy)
	for {
		m, err := ReadMsg(resp.Body)
		if err != nil {
			return err
		}
		s.apply(m)
	}
}

// writeUp sends the Register frame, then forwards Wakes (route_wake) until ctx ends.
// With no WakeSource it holds the request body open (the down stream is what matters).
func (s *Subscriber) writeUp(ctx context.Context, w io.Writer) error {
	r := s.reg
	if err := WriteMsg(w, &Msg{Type: TypeRegister, Register: &r}); err != nil {
		return err
	}
	if s.wakes == nil {
		<-ctx.Done()
		return ctx.Err()
	}
	for {
		wake, ok := s.wakes.NextWake(ctx)
		if !ok {
			return ctx.Err()
		}
		if err := WriteMsg(w, &Msg{Type: TypeWake, Wake: &wake}); err != nil {
			return err
		}
	}
}

func (s *Subscriber) apply(m *Msg) {
	switch m.Type {
	case TypeHello:
		if m.Hello != nil {
			s.sink.SetPolicy(m.Hello.Policy)
		}
	case TypeUpsert:
		if m.Route != nil {
			s.sink.ApplyUpsert(*m.Route)
		}
	case TypeDelete:
		if m.Delete == nil || m.Delete.Validate() != nil {
			s.log.Warn("routesync: invalid route delete")
			return
		}
		s.sink.ApplyDelete(*m.Delete)
	case TypeBookmark:
		s.sink.Bookmark()
	default:
		s.log.Warn("routesync: unknown message", "type", m.Type)
	}
}
