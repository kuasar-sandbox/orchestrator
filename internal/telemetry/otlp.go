package telemetry

import (
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/appnet"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/receiver"
	"golang.org/x/net/netutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/tap"
)

type otlpReceiverConfig struct {
	HTTPListen      string `mapstructure:"http_listen"`
	GRPCListen      string `mapstructure:"grpc_listen"`
	MaxConnections  int    `mapstructure:"max_connections"`
	MaxRequests     int    `mapstructure:"max_requests"`
	MaxRequestBytes int    `mapstructure:"max_request_bytes"`
}

func defaultOTLPConfig() otlpReceiverConfig {
	return otlpReceiverConfig{HTTPListen: ":4318", GRPCListen: ":4317", MaxConnections: 256, MaxRequests: 32, MaxRequestBytes: 4 << 20}
}

func (c *otlpReceiverConfig) Validate() error {
	if c.MaxConnections < 1 || c.MaxConnections > 65536 || c.MaxRequests < 1 || c.MaxRequests > 4096 || c.MaxRequestBytes < 1024 || c.MaxRequestBytes > 64<<20 {
		return errors.New("sandboxotlp limits out of range")
	}
	for _, address := range []string{c.HTTPListen, c.GRPCListen} {
		_, port, err := net.SplitHostPort(address)
		if err != nil {
			return fmt.Errorf("sandboxotlp listen: %w", err)
		}
		number, err := strconv.Atoi(port)
		if err != nil || number < 0 || number > 65535 {
			return errors.New("sandboxotlp port must be in [0, 65535]")
		}
	}
	if c.HTTPListen == c.GRPCListen {
		_, port, _ := net.SplitHostPort(c.HTTPListen)
		if port != "0" {
			return errors.New("sandboxotlp listeners conflict")
		}
	}
	return nil
}

func otlpFactory(view *View, proxyNetNS string, fatal func(error)) receiver.Factory {
	return receiver.NewFactory(component.MustNewType("sandboxotlp"), func() component.Config {
		cfg := defaultOTLPConfig()
		return &cfg
	}, receiver.WithMetrics(func(_ context.Context, _ receiver.Settings, raw component.Config, next consumer.Metrics) (receiver.Metrics, error) {
		parsed := raw.(*otlpReceiverConfig)
		return &otlpReceiver{view: view, cfg: *parsed, proxyNetNS: proxyNetNS, next: next, fatal: fatal, requests: make(chan struct{}, parsed.MaxRequests)}, nil
	}, component.StabilityLevelStable))
}

type otlpReceiver struct {
	pmetricotlp.UnimplementedGRPCServer
	view                       *View
	lifecycle                  context.Context
	cfg                        otlpReceiverConfig
	proxyNetNS                 string
	next                       consumer.Metrics
	fatal                      func(error)
	requests                   chan struct{}
	httpServer                 *http.Server
	grpcServer                 *grpc.Server
	httpListener, grpcListener net.Listener
	wg                         sync.WaitGroup
}

// Pin the identity when accepting the actual transport, not for each request.
// A persistent connection must never acquire the identity of a successor that
// reuses its IP. No forwarded headers, self-reported IDs, or OTLP tokens apply.
type identityAddr struct {
	net.Addr
	entry *target
}
type identifiedConn struct {
	net.Conn
	address identityAddr
	mu      sync.Mutex
	stop    func() bool
}

func (c *identifiedConn) RemoteAddr() net.Addr { return c.address }
func (c *identifiedConn) Close() error {
	c.mu.Lock()
	stop := c.stop
	c.mu.Unlock()
	if stop != nil {
		stop()
	}
	return c.Conn.Close()
}

type identityListener struct {
	net.Listener
	view *View
}

func (l *identityListener) Accept() (net.Conn, error) {
	for {
		conn, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}
		host, _, err := net.SplitHostPort(conn.RemoteAddr().String())
		var entry *target
		if err == nil {
			entry, _ = l.view.peerTarget(host)
		}
		if entry == nil {
			// Never retain an unidentified HTTP/2 or keep-alive connection. A
			// client connecting before full sync must reconnect with a fresh
			// peer binding, rather than remaining permanently unauthenticated.
			_ = conn.Close()
			continue
		}
		wrapped := &identifiedConn{Conn: conn, address: identityAddr{Addr: conn.RemoteAddr(), entry: entry}}
		wrapped.mu.Lock()
		wrapped.stop = context.AfterFunc(entry.ctx, func() { _ = wrapped.Close() })
		wrapped.mu.Unlock()
		return wrapped, nil
	}
}

func (r *otlpReceiver) Start(ctx context.Context, _ component.Host) (err error) {
	r.lifecycle = ctx
	ns, err := appnet.OpenProxyNetNS(r.proxyNetNS)
	if err != nil {
		return err
	}
	defer ns.Close()
	listen := func(address string) (net.Listener, error) {
		ln, err := appnet.ListenTCPInNetNS(ns, address)
		if err != nil {
			return nil, err
		}
		return &identityListener{Listener: netutil.LimitListener(ln, r.cfg.MaxConnections), view: r.view}, nil
	}
	r.httpListener, err = listen(r.cfg.HTTPListen)
	if err != nil {
		return fmt.Errorf("OTLP HTTP listen: %w", err)
	}
	r.grpcListener, err = listen(r.cfg.GRPCListen)
	if err != nil {
		_ = r.httpListener.Close()
		return fmt.Errorf("OTLP gRPC listen: %w", err)
	}
	r.httpServer = &http.Server{Handler: r.httpHandler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16 << 10,
		BaseContext: func(net.Listener) context.Context { return ctx },
		ConnContext: func(ctx context.Context, conn net.Conn) context.Context {
			addr, _ := conn.RemoteAddr().(identityAddr)
			return withIdentity(ctx, addr.entry, "otlp")
		}}
	r.grpcServer = grpc.NewServer(grpc.MaxRecvMsgSize(r.cfg.MaxRequestBytes), grpc.MaxConcurrentStreams(uint32(r.cfg.MaxRequests)), grpc.ConnectionTimeout(5*time.Second),
		grpc.InTapHandle(r.grpcTap), grpc.KeepaliveParams(keepalive.ServerParameters{MaxConnectionIdle: 30 * time.Second}))
	pmetricotlp.RegisterGRPCServer(r.grpcServer, r)
	r.wg.Add(2)
	go func() {
		defer r.wg.Done()
		if err := r.httpServer.Serve(r.httpListener); err != nil && !errors.Is(err, http.ErrServerClosed) && !errors.Is(err, net.ErrClosed) {
			r.fatal(err)
		}
	}()
	go func() {
		defer r.wg.Done()
		if err := r.grpcServer.Serve(r.grpcListener); err != nil && !errors.Is(err, grpc.ErrServerStopped) && !errors.Is(err, net.ErrClosed) {
			r.fatal(err)
		}
	}()
	return nil
}

func (r *otlpReceiver) Shutdown(ctx context.Context) error {
	if r.httpServer == nil {
		return nil
	}
	done := make(chan struct{})
	go func() { r.grpcServer.GracefulStop(); close(done) }()
	err := r.httpServer.Shutdown(ctx)
	if err != nil {
		_ = r.httpServer.Close()
	}
	select {
	case <-done:
	case <-ctx.Done():
		r.grpcServer.Stop()
		<-done
		err = errors.Join(err, ctx.Err())
	}
	r.wg.Wait()
	return err
}

func (r *otlpReceiver) acquire(ctx context.Context) (func(), error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case r.requests <- struct{}{}:
		return func() { <-r.requests }, nil
	default:
		return nil, status.Error(codes.ResourceExhausted, "OTLP request concurrency exhausted")
	}
}

func (r *otlpReceiver) Export(ctx context.Context, request pmetricotlp.ExportRequest) (pmetricotlp.ExportResponse, error) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return pmetricotlp.ExportResponse{}, status.Error(codes.Unauthenticated, ErrIdentity.Error())
	}
	addr, ok := p.Addr.(identityAddr)
	if !ok || r.view.withCurrent(addr.entry, func() error { return nil }) != nil {
		return pmetricotlp.ExportResponse{}, status.Error(codes.Unauthenticated, ErrIdentity.Error())
	}
	ctx, cancel := context.WithTimeout(withIdentity(ctx, addr.entry, "otlp"), 10*time.Second)
	defer cancel()
	stop := context.AfterFunc(addr.entry.ctx, cancel)
	defer stop()
	if err := r.acceptAndDeliver(ctx, addr.entry, request.Metrics()); err != nil {
		code := codes.Unavailable
		if errors.Is(err, ErrIdentity) {
			code = codes.Unauthenticated
		}
		if errors.Is(err, ErrInvalidMetrics) || consumererror.IsPermanent(err) {
			code = codes.InvalidArgument
		}
		if errors.Is(err, context.Canceled) {
			code = codes.Canceled
		}
		if errors.Is(err, context.DeadlineExceeded) {
			code = codes.DeadlineExceeded
		}
		return pmetricotlp.ExportResponse{}, status.Error(code, "telemetry metrics rejected")
	}
	return pmetricotlp.NewExportResponse(), nil
}

// Reserve before gRPC reads/decodes message bodies. A handler interceptor would
// be too late: MaxConcurrentStreams applies per connection, not per component.
func (r *otlpReceiver) grpcTap(ctx context.Context, info *tap.Info) (context.Context, error) {
	if info.FullMethodName != "/opentelemetry.proto.collector.metrics.v1.MetricsService/Export" {
		return nil, status.Error(codes.Unimplemented, "only metrics OTLP is supported")
	}
	p, ok := peer.FromContext(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, ErrIdentity.Error())
	}
	addr, ok := p.Addr.(identityAddr)
	if !ok || r.view.withCurrent(addr.entry, func() error { return nil }) != nil {
		return nil, status.Error(codes.Unauthenticated, ErrIdentity.Error())
	}
	// Include transport message reads in the budget. Starting this deadline in
	// Export would allow a peer to hold a global slot forever without a body.
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	release, err := r.acquire(ctx)
	if err != nil {
		cancel()
		return nil, err
	}
	context.AfterFunc(ctx, func() { release(); cancel() })
	return ctx, nil
}

func (r *otlpReceiver) acceptAndDeliver(ctx context.Context, entry *target, metrics pmetric.Metrics) error {
	if err := r.view.acceptMetrics(ctx, entry, "otlp", metrics); err != nil {
		return err
	}
	parent := r.lifecycle
	if parent == nil {
		parent = context.Background()
	} // Direct handler test harness.
	pipelineCtx, cancel := context.WithTimeout(parent, 10*time.Second)
	defer cancel()
	return r.next.ConsumeMetrics(pipelineCtx, metrics)
}

func (r *otlpReceiver) httpHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/metrics", func(w http.ResponseWriter, req *http.Request) {
		identity, _ := req.Context().Value(identityContextKey{}).(ingressIdentity)
		if r.view.withCurrent(identity.entry, func() error { return nil }) != nil {
			http.Error(w, "unknown sandbox peer", http.StatusUnauthorized)
			return
		}
		ctx, cancel := context.WithTimeout(req.Context(), 10*time.Second)
		defer cancel()
		stop := context.AfterFunc(identity.entry.ctx, cancel)
		defer stop()
		release, err := r.acquire(ctx)
		if err != nil {
			w.Header().Set("Retry-After", "1")
			http.Error(w, "OTLP busy", 429)
			return
		}
		defer release()
		contentType, _, err := mime.ParseMediaType(req.Header.Get("Content-Type"))
		if err != nil || (contentType != "application/x-protobuf" && contentType != "application/json") {
			http.Error(w, "unsupported OTLP content type", 415)
			return
		}
		var body io.Reader = http.MaxBytesReader(w, req.Body, int64(r.cfg.MaxRequestBytes))
		switch req.Header.Get("Content-Encoding") {
		case "", "identity":
		case "gzip":
			compressed, err := gzip.NewReader(body)
			if err != nil {
				http.Error(w, "invalid OTLP gzip body", 400)
				return
			}
			defer compressed.Close()
			body = compressed
		default:
			http.Error(w, "unsupported OTLP encoding", 415)
			return
		}
		raw, err := io.ReadAll(io.LimitReader(body, int64(r.cfg.MaxRequestBytes)+1))
		if err != nil || len(raw) > r.cfg.MaxRequestBytes {
			http.Error(w, "OTLP body too large or unreadable", 413)
			return
		}
		request := pmetricotlp.NewExportRequest()
		if contentType == "application/json" {
			err = request.UnmarshalJSON(raw)
		} else {
			err = request.UnmarshalProto(raw)
		}
		if err != nil {
			http.Error(w, "invalid OTLP metrics", 400)
			return
		}
		if err = r.acceptAndDeliver(ctx, identity.entry, request.Metrics()); err != nil {
			code := http.StatusServiceUnavailable
			if errors.Is(err, ErrIdentity) {
				code = http.StatusUnauthorized
			}
			if errors.Is(err, ErrInvalidMetrics) || consumererror.IsPermanent(err) {
				code = http.StatusBadRequest
			}
			http.Error(w, "telemetry metrics rejected", code)
			return
		}
		response := pmetricotlp.NewExportResponse()
		if contentType == "application/json" {
			raw, err = response.MarshalJSON()
		} else {
			raw, err = response.MarshalProto()
		}
		if err != nil {
			http.Error(w, "encode OTLP response", 500)
			return
		}
		w.Header().Set("Content-Type", contentType)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(raw)
	})
	return mux
}
