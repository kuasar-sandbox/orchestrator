// Package appnet contains process-assembly networking shared by the built-in
// and statically customized conductor/proxy Apps.
package appnet

import (
	"context"
	"crypto/tls"
	"log/slog"
	"net"
	"net/http"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	"github.com/kuasar-sandbox/orchestrator/internal/metrics"
)

// Serve serves handler until ctx is cancelled. A non-nil TLS config is already
// populated with core-owned policy and key material; nil serves h2c and HTTP/1.1.
func Serve(ctx context.Context, ln net.Listener, handler http.Handler, tlsConfig *tls.Config) error {
	srv := &http.Server{ReadHeaderTimeout: 10 * time.Second}
	shutdownDone := make(chan struct{})
	defer close(shutdownDone)
	go func() {
		select {
		case <-ctx.Done():
			sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = srv.Shutdown(sctx)
		case <-shutdownDone:
		}
	}()
	var err error
	if tlsConfig != nil {
		srv.Handler = handler
		srv.TLSConfig = tlsConfig.Clone()
		err = srv.ServeTLS(ln, "", "")
	} else {
		srv.Handler = h2c.NewHandler(handler, &http2.Server{})
		err = srv.Serve(ln)
	}
	if err == http.ErrServerClosed {
		return nil
	}
	return err
}

// ServeMetrics runs a small /metrics endpoint until ctx is cancelled.
func ServeMetrics(ctx context.Context, addr string, mx *metrics.M, log *slog.Logger) {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", mx.Handler())
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			sctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_ = srv.Shutdown(sctx)
		case <-done:
		}
	}()
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Warn("metrics server", "addr", addr, "err", err)
	}
}
