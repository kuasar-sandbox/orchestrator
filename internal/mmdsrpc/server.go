package mmdsrpc

import (
	"io"
	"log/slog"
)

// Route is a resolved MMDS route, as returned by a Handler.
type Route struct {
	Type        string // "secret" | "service" | "static"
	ContentType string // static only
	Body        string // static only
}

// Handler resolves sid's specified MMDS route at path. ok=false means sid or
// path is unspecified.
type Handler func(sandboxID, path string) (route Route, ok bool)

// Server answers EndpointRequest frames from one worker's inherited
// socketpair endpoint, strictly in request order. The current Handler (a
// canonical-JSON map lookup backed by the proxy master's in-heap
// proxyshm.MMDSRoutes store) is synchronous and non-blocking, so this loop is
// deliberately sequential -- no per-request goroutines or inflight
// bookkeeping. Revisit if a future Handler implementation can block (e.g. a
// decrypt or a UDS call to a local service).
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
		}
		if err := writeFrame(s.rw, resp); err != nil {
			if s.log != nil {
				s.log.Debug("mmdsrpc: write response", "err", err)
			}
			return
		}
	}
}
