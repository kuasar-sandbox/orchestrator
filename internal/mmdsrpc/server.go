package mmdsrpc

import (
	"io"
	"log/slog"
)

// Handler performs a bounded in-memory lookup. Unavailable is represented in
// the response rather than an error string so no sensitive state enters logs.
type Handler func(sandboxID, exactPath string) EndpointResponse

type Server struct {
	rw      io.ReadWriteCloser
	handler Handler
	log     *slog.Logger
}

func NewServer(rw io.ReadWriteCloser, handler Handler, log *slog.Logger) *Server {
	return &Server{rw: rw, handler: handler, log: log}
}

func (s *Server) Serve() {
	defer s.rw.Close()
	for {
		var request EndpointRequest
		if err := readFrame(s.rw, &request); err != nil {
			return
		}
		response := s.handler(request.SandboxID, request.Path)
		response.RequestID = request.RequestID
		if err := writeFrame(s.rw, response); err != nil {
			if s.log != nil {
				s.log.Debug("mmdsrpc: write response", "err", err)
			}
			return
		}
	}
}
