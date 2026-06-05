package routesync

import (
	"context"
	"log/slog"
	"net/http"
)

// Sink applies inbound route messages on the proxy side (the route table).
type Sink interface {
	ApplySnapshot(routes []RouteEntry)
	ApplyUpsert(r RouteEntry)
	ApplyDelete(sid string)
	SetPolicy(p Policy)
}

// WakeSource yields sandbox ids the proxy wants the orchestrator to resume. It
// blocks until a wake is available or ctx is done (ok=false on ctx done).
type WakeSource interface {
	NextWake(ctx context.Context) (sid string, ok bool)
}

// Server is the proxy-side endpoint of the route-sync stream. One Server instance
// serves the single orchestrator that dials this proxy's UDS.
type Server struct {
	sink  Sink
	wakes WakeSource
	log   *slog.Logger
}

func NewServer(sink Sink, wakes WakeSource, log *slog.Logger) *Server {
	return &Server{sink: sink, wakes: wakes, log: log}
}

// ServeSync handles one bidi route-sync stream (the orchestrator's POST to
// SyncPath with SyncHeader set). It reads route frames from the request body and
// writes wake frames to the response body, full-duplex over h2c, until either side
// closes. It is the handler the proxy's UDS mux dispatches the route-sync stream to.
func (s *Server) ServeSync(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "routesync needs a flushable (h2c) writer", http.StatusInternalServerError)
		return
	}
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	// Reader: apply inbound route frames until EOF/error, then cancel so the
	// writer loop exits and the handler returns (closing the response -> the
	// orchestrator reconnects).
	go func() {
		defer cancel()
		for {
			m, err := readMsg(r.Body)
			if err != nil {
				if ctx.Err() == nil {
					s.log.Debug("routesync: read end", "err", err)
				}
				return
			}
			s.apply(m)
		}
	}()

	// Writer: ack the handshake (flushes response headers so the orchestrator's
	// RoundTrip returns and starts reading), then forward wakes.
	if err := writeMsg(w, &Msg{Type: TypeHelloAck, Hello: &Hello{Version: Version, Role: "proxy"}}); err != nil {
		return
	}
	flusher.Flush()
	for {
		sid, ok := s.wakes.NextWake(ctx)
		if !ok {
			return
		}
		if err := writeMsg(w, &Msg{Type: TypeWake, SID: sid}); err != nil {
			return
		}
		flusher.Flush()
	}
}

func (s *Server) apply(m *Msg) {
	switch m.Type {
	case TypeHello:
		if m.Hello != nil {
			s.sink.SetPolicy(m.Hello.Policy)
		}
	case TypeSnapshot:
		s.sink.ApplySnapshot(m.Routes)
	case TypeUpsert:
		if m.Route != nil {
			s.sink.ApplyUpsert(*m.Route)
		}
	case TypeDelete:
		s.sink.ApplyDelete(m.SID)
	default:
		s.log.Warn("routesync: unknown message", "type", m.Type)
	}
}
