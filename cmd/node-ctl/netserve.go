package main

import (
	"context"
	"crypto/tls"
	"log/slog"
	"net"
	"net/http"

	"github.com/kuasar-sandbox/orchestrator/internal/appnet"
	"github.com/kuasar-sandbox/orchestrator/internal/metrics"
)

func serveListener(ctx context.Context, listener net.Listener, handler http.Handler, certificatePath, keyPath string, _ *slog.Logger) error {
	var tlsConfig *tls.Config
	if certificatePath != "" && keyPath != "" {
		certificate, err := tls.LoadX509KeyPair(certificatePath, keyPath)
		if err != nil {
			return err
		}
		tlsConfig = &tls.Config{
			Certificates: []tls.Certificate{certificate}, MinVersion: tls.VersionTLS12,
			NextProtos: []string{"h2", "http/1.1"},
		}
	}
	return appnet.Serve(ctx, listener, handler, tlsConfig)
}

func serveMetrics(ctx context.Context, addr string, mx *metrics.M, logger *slog.Logger) {
	appnet.ServeMetrics(ctx, addr, mx, logger)
}
