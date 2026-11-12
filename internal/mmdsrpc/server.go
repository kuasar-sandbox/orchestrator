package mmdsrpc

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
)

// Handler is a pure, bounded lookup on the master's route view.
type Handler func(sandboxID, exactPath string) EndpointResponse

// HoldHandler reaches the authority for a previously matched hold declaration.
// Only success and a failed value comparison carry the actual value.
type HoldHandler func(ctx context.Context, sandboxID, runID, exactPath string, value, ifMatch *int64) (actual *int64, status int)

type Server struct {
	rw      io.ReadWriteCloser
	handler Handler
	log     *slog.Logger
	ctx     context.Context
	hold    HoldHandler
}

func NewServer(rw io.ReadWriteCloser, handler Handler, log *slog.Logger) *Server {
	return &Server{rw: rw, handler: handler, log: log, ctx: context.Background()}
}

func (s *Server) WithHold(ctx context.Context, handler HoldHandler) *Server {
	s.ctx, s.hold = ctx, handler
	return s
}

func (s *Server) Serve() {
	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()
	defer s.rw.Close()
	// A slow control RPC must not serialize ordinary metadata lookups, and
	// guests cannot create unbounded control requests through one worker.
	slots := make(chan struct{}, 8)
	var writer sync.Mutex
	reply := func(id uint64, r EndpointResponse) bool {
		writer.Lock()
		defer writer.Unlock()
		r.RequestID = id
		if err := writeFrame(s.rw, r); err != nil {
			if s.log != nil {
				s.log.Debug("mmdsrpc: write response", "err", err)
			}
			_ = s.rw.Close()
			return false
		}
		return true
	}
	for {
		var req EndpointRequest
		if err := readFrame(s.rw, &req); err != nil {
			return
		}
		if req.Hold == nil {
			if !reply(req.RequestID, s.handler(req.SandboxID, req.Path)) {
				return
			}
			continue
		}
		select {
		case slots <- struct{}{}:
			go func(req EndpointRequest) {
				defer func() { <-slots }()
				reply(req.RequestID, s.callHold(ctx, req))
			}(req)
		default:
			if !reply(req.RequestID, EndpointResponse{HoldStatus: http.StatusServiceUnavailable}) {
				return
			}
		}
	}
}

func (s *Server) callHold(ctx context.Context, req EndpointRequest) EndpointResponse {
	h := req.Hold
	if req.SandboxID == "" || req.Path == "" || h.RunID == "" || (h.Value == nil && h.IfMatch != nil) || (h.Value != nil && *h.Value < 0) || (h.IfMatch != nil && *h.IfMatch < 0) {
		return EndpointResponse{HoldStatus: http.StatusBadRequest}
	}
	target := s.handler(req.SandboxID, req.Path)
	if target.Unavailable || s.hold == nil {
		return EndpointResponse{HoldStatus: http.StatusServiceUnavailable}
	}
	if !target.Found || target.Type != sandboxcfg.MMDSRouteHoldTraffic {
		return EndpointResponse{HoldStatus: http.StatusNotFound}
	}
	// Deadlines travel across the local socket so a queued operation cannot
	// start at the authority after the requesting worker's budget has expired.
	callCtx, cancel := context.WithDeadline(ctx, time.Unix(0, h.Deadline))
	defer cancel()
	if callCtx.Err() != nil {
		return EndpointResponse{HoldStatus: http.StatusServiceUnavailable}
	}
	actual, status := s.hold(callCtx, req.SandboxID, h.RunID, req.Path, h.Value, h.IfMatch)
	return EndpointResponse{HoldStatus: status, HoldValue: actual}
}
