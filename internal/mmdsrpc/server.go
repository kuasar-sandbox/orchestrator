package mmdsrpc

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"time"
)

// EndpointTable is the master-side surface Server resolves requests
// against — satisfied by *internal/proxyendpoints.Table. A separate,
// minimal interface (rather than importing proxyendpoints directly) keeps
// this package free to be tested without that package's full dependency
// tree and avoids a needless import-graph edge.
type EndpointTable interface {
	// Lookup, ServeStore, ServeRelay: a non-nil err means the table itself
	// failed/is unavailable — distinct from a legitimate found/present/
	// ok=false negative result. resolve() maps a non-nil err to
	// ErrCodeUnavailable, never to Found=false.
	Lookup(ctx context.Context, sandboxID, path string) (name, backendType string, found bool, err error)
	ServeStore(ctx context.Context, sandboxID, name string) (value []byte, contentType string, revision int64, present bool, err error)
	ServeRelay(ctx context.Context, sandboxID, name string) (status int, contentType string, body []byte, ok bool, err error)
}

// Counter is the narrow metrics surface Server needs, mirroring
// internal/mmds.Counter so this package does not depend on a concrete
// metrics type.
type Counter interface {
	Inc(name string)
}

type noopCounter struct{}

func (noopCounter) Inc(string) {}

// Server is the master-side RPC handler for one connected worker — one
// instance per accepted socketpair connection. It resolves
// and serves each EndpointRequest against table, enforcing a per-connection
// inflight cap (max_worker_inflight) by rejecting over-limit requests
// rather than queuing them unboundedly.
type Server struct {
	table       EndpointTable
	maxInflight int
	log         *slog.Logger
	mx          Counter

	writeMu sync.Mutex

	mu       sync.Mutex
	inflight map[uint64]context.CancelFunc
}

// NewServer builds a Server. maxInflight<=0 defaults to 128. mx may be nil
// (metrics off).
func NewServer(table EndpointTable, maxInflight int, log *slog.Logger, mx Counter) *Server {
	if maxInflight <= 0 {
		maxInflight = 128
	}
	if mx == nil {
		mx = noopCounter{}
	}
	return &Server{table: table, maxInflight: maxInflight, log: log, mx: mx, inflight: map[uint64]context.CancelFunc{}}
}

// Serve runs the request loop on conn until it errors (worker socket
// close/EOF) or ctx is cancelled (master shutdown) — one call per connected
// worker. Every inflight request on this connection is cancelled when Serve
// returns: a worker socket close cancels its inflight calls.
func (s *Server) Serve(ctx context.Context, conn io.ReadWriteCloser) error {
	sctx, cancelAll := context.WithCancel(ctx)
	defer cancelAll()
	defer s.cancelAllInflight()

	for {
		m, err := readFrame(conn)
		if err != nil {
			if sctx.Err() == nil {
				s.mx.Inc(`mmds_worker_rpc_errors_total{reason="conn_error"}`)
			}
			return err
		}
		switch m.Type {
		case typeRequest:
			if m.Request != nil {
				s.handleRequest(sctx, conn, m.RequestID, m.Request)
			}
		case typeCancel:
			s.cancelOne(m.RequestID)
		}
	}
}

func (s *Server) handleRequest(ctx context.Context, conn io.ReadWriteCloser, id uint64, req *EndpointRequest) {
	s.mu.Lock()
	if _, dup := s.inflight[id]; dup {
		s.mu.Unlock()
		s.mx.Inc(`mmds_worker_rpc_errors_total{reason="duplicate_request_id"}`)
		if s.log != nil {
			s.log.Warn("mmdsrpc: duplicate request_id from worker; dropped", "request_id", id)
		}
		return
	}
	if len(s.inflight) >= s.maxInflight {
		s.mu.Unlock()
		s.mx.Inc(`mmds_worker_rpc_errors_total{reason="worker_inflight_limit"}`)
		s.reply(conn, id, &EndpointResponse{ErrorCode: ErrCodeWorkerInflightLimit})
		return
	}
	var rctx context.Context
	var cancel context.CancelFunc
	if req.DeadlineUnixMS > 0 {
		rctx, cancel = context.WithDeadline(ctx, time.UnixMilli(req.DeadlineUnixMS))
	} else {
		rctx, cancel = context.WithCancel(ctx)
	}
	s.inflight[id] = cancel
	s.mu.Unlock()
	s.mx.Inc("mmds_worker_rpc_inflight_started_total")

	go func() {
		defer func() {
			s.mu.Lock()
			delete(s.inflight, id)
			s.mu.Unlock()
			cancel()
			s.mx.Inc("mmds_worker_rpc_inflight_completed_total")
		}()
		resp := s.resolve(rctx, req)
		s.reply(conn, id, resp)
	}()
}

// resolve performs the combined lookup+serve the doc calls a single
// EndpointRequest — see Client's doc comment for why this is one round
// trip rather than three.
func (s *Server) resolve(ctx context.Context, req *EndpointRequest) *EndpointResponse {
	name, backendType, found, err := s.table.Lookup(ctx, req.SandboxID, req.Path)
	if err != nil {
		return &EndpointResponse{ErrorCode: ErrCodeUnavailable}
	}
	if !found {
		return &EndpointResponse{Found: false}
	}
	switch backendType {
	case "store":
		value, contentType, revision, present, err := s.table.ServeStore(ctx, req.SandboxID, name)
		if err != nil {
			return &EndpointResponse{ErrorCode: ErrCodeUnavailable}
		}
		return &EndpointResponse{Found: true, Name: name, BackendType: backendType, Present: present, Body: value, ContentType: contentType, Revision: revision}
	case "relay":
		status, contentType, body, ok, err := s.table.ServeRelay(ctx, req.SandboxID, name)
		if err != nil {
			return &EndpointResponse{ErrorCode: ErrCodeUnavailable}
		}
		if !ok {
			return &EndpointResponse{Found: true, Name: name, BackendType: backendType, Present: false}
		}
		return &EndpointResponse{Found: true, Name: name, BackendType: backendType, Present: true, Status: status, ContentType: contentType, Body: body}
	default:
		return &EndpointResponse{Found: false}
	}
}

func (s *Server) reply(conn io.ReadWriteCloser, id uint64, resp *EndpointResponse) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := writeFrame(conn, &wireMsg{Type: typeResponse, RequestID: id, Response: resp}); err != nil && s.log != nil {
		s.log.Debug("mmdsrpc: write response", "request_id", id, "err", err)
	}
}

func (s *Server) cancelOne(id uint64) {
	s.mu.Lock()
	cancel, ok := s.inflight[id]
	s.mu.Unlock()
	if ok {
		cancel()
	}
}

func (s *Server) cancelAllInflight() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, cancel := range s.inflight {
		cancel()
	}
}
