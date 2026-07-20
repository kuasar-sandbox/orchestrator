package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	"github.com/kuasar-sandbox/orchestrator/internal/metrics"
)

// serveListener serves handler on ln until ctx is cancelled. With tlsCert/tlsKey
// it terminates TLS (HTTP/2 via ALPN); otherwise it serves h2c (which also accepts
// HTTP/1.1). Returns nil on graceful shutdown.
func serveListener(ctx context.Context, ln net.Listener, handler http.Handler, tlsCert, tlsKey, clientCA string, log *slog.Logger) error {
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
		if clientCA != "" {
			pem, readErr := os.ReadFile(clientCA)
			if readErr != nil {
				return readErr
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(pem) {
				return errors.New("node-ctl: client_ca contains no certificates")
			}
			srv.TLSConfig.ClientCAs = pool
			srv.TLSConfig.ClientAuth = tls.RequireAndVerifyClientCert
		}
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
