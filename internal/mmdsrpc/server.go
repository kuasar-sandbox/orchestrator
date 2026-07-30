package mmdsrpc

import (
	"io"
	"log/slog"
)

// Route is a resolved MMDS route, as returned by a Handler.
//
// For Type=="secret": Present/Retryable/ContentType/Body mirror
// EndpointResponse's fields of the same name -- Body is base64-encoded
// exactly like the underlying store.MMDSSecretValue.BodyBase64 (unlike a
// static route's Body, which is raw UTF-8 text); Server passes it straight
// through onto the wire unchanged, and WorkerView.MMDSRoute is the one place
// that decodes it back to raw bytes, since a secret value is not required to
// be valid UTF-8 and encoding/json would silently corrupt it otherwise.
//
// For Type=="service": Target/ServiceName (once synced) are meaningful -- the
// Handler resolves the specification (a fast, synchronous map lookup, same as
// static/secret) but never dials the registered local service itself; the
// worker does that independently after this RPC returns, using its own local
// copy of the service registry. See EndpointResponse's doc comment for the
// full reasoning.
type Route struct {
	Type        string // "secret" | "service" | "static"
	ContentType string // static, and secret once Present
	Body        string // static (raw) or secret (base64, once Present)
	Present     bool   // secret only
	Retryable   bool   // secret only, meaningful when !Present
	Target      string // service only, meaningful when !Unavailable
	ServiceName string // service only, meaningful when !Unavailable
	Unavailable bool   // secret and service only; see EndpointResponse.Unavailable
}

// Handler resolves sid's specified MMDS route at path. ok=false means sid or
// path is unspecified.
type Handler func(sandboxID, path string) (route Route, ok bool)

// Server answers EndpointRequest frames from one worker's inherited
// socketpair endpoint, strictly in request order. Handler (a canonical-JSON
// map lookup backed by the proxy master's in-heap proxyshm.MMDSRoutes/
// MMDSSecrets stores) is synchronous and non-blocking for every route type,
// including "service" -- the registered local service is dialed by the
// worker itself, never by Handler -- so this loop stays deliberately
// sequential, with no per-request goroutines or inflight bookkeeping.
type Server struct {
	rw      io.ReadWriteCloser
	handler Handler
	log     *slog.Logger
}

// NewServer wraps rw (one end of an inherited socketpair) bound to one
// worker. handler must be safe for concurrent use across the node's workers
// (one Server per worker, all calling into the same master-side store).
func NewServer(rw io.ReadWriteCloser, handler Handler, log *slog.Logger) *Server {
	return &Server{rw: rw, handler: handler, log: log}
}

// Serve reads and answers requests until the connection errors or closes.
func (s *Server) Serve() {
	for {
		var req EndpointRequest
		if err := readFrame(s.rw, &req); err != nil {
			return
		}
		resp := EndpointResponse{RequestID: req.RequestID}
		if route, ok := s.handler(req.SandboxID, req.Path); ok {
			resp.Found = true
			resp.Type = route.Type
			resp.ContentType = route.ContentType
			resp.Body = route.Body
			resp.Present = route.Present
			resp.Retryable = route.Retryable
			resp.Target = route.Target
			resp.ServiceName = route.ServiceName
			resp.Unavailable = route.Unavailable
		}
		if err := writeFrame(s.rw, resp); err != nil {
			if s.log != nil {
				s.log.Debug("mmdsrpc: write response", "err", err)
			}
			return
		}
	}
}
