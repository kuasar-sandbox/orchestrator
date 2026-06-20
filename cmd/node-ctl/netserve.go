package main

import (
	"context"
	"crypto/tls"
	"log/slog"
	"net"
	"net/http"
	"syscall"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"golang.org/x/sys/unix"

	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/metrics"
)

// serveListener serves handler on ln until ctx is cancelled. With tlsCert/tlsKey
// it terminates TLS (HTTP/2 via ALPN); otherwise it serves h2c (which also accepts
// HTTP/1.1). Returns nil on graceful shutdown.
func serveListener(ctx context.Context, ln net.Listener, handler http.Handler, tlsCert, tlsKey string, log *slog.Logger) error {
	srv := &http.Server{ReadHeaderTimeout: 10 * time.Second}
	go func() {
		<-ctx.Done()
		sctx, c := context.WithTimeout(context.Background(), 10*time.Second)
		defer c()
		_ = srv.Shutdown(sctx)
	}()
	var err error
	if tlsCert != "" && tlsKey != "" {
		srv.Handler = handler
		srv.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12, NextProtos: []string{"h2", "http/1.1"}}
		err = srv.ServeTLS(ln, tlsCert, tlsKey)
	} else {
		srv.Handler = h2c.NewHandler(handler, &http2.Server{})
		err = srv.Serve(ln)
	}
	if err == http.ErrServerClosed {
		return nil
	}
	return err
}

// listenReusePort binds addr with SO_REUSEPORT so multiple proxy worker processes
// can share one data-plane port (the kernel load-balances accepted connections).
func listenReusePort(ctx context.Context, network, addr string) (net.Listener, error) {
	lc := net.ListenConfig{
		Control: func(_, _ string, c syscall.RawConn) error {
			var serr error
			if err := c.Control(func(fd uintptr) {
				serr = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_REUSEPORT, 1)
			}); err != nil {
				return err
			}
			return serr
		},
	}
	return lc.Listen(ctx, network, addr)
}

// serveMetrics runs a tiny /metrics endpoint until ctx is cancelled.
func serveMetrics(ctx context.Context, addr string, mx *metrics.M, log *slog.Logger) {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", mx.Handler())
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		sctx, c := context.WithTimeout(context.Background(), 3*time.Second)
		defer c()
		_ = srv.Shutdown(sctx)
	}()
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Warn("metrics server", "addr", addr, "err", err)
	}
}
