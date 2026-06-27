package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os/signal"
	"strings"
	"syscall"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"golang.org/x/sys/unix"

	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/clustercfg"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/router"
)

// runRouter starts the router role: the e2b-compatible unified ingress
// (cluster-router.md). It dials the registry's control API for reserve + route
// resolution. Phase 3 serves plain h2c (dev); router.tls + auth cache are Phase 7.
func runRouter(args []string, log *slog.Logger) error {
	fs := flag.NewFlagSet("router", flag.ExitOnError)
	cfgPath := fs.String("config", "/etc/cluster-ctl/router.yaml", "config file")
	_ = fs.Parse(args)

	cfg, err := clustercfg.LoadRouter(*cfgPath)
	if err != nil {
		return err
	}
	controlAddr := cfg.Registry.Endpoint // registry control_api to dial (default: co-located socket)

	// registry mTLS when the endpoint is a remote TCP addr and registry.tls is set
	// (cluster.md §5.4); a co-located UDS endpoint stays plain.
	var controlTLS *tls.Config
	if !strings.HasPrefix(controlAddr, "/") && cfg.Registry.TLS.Enabled() {
		host := controlAddr
		if i := strings.LastIndexByte(host, ':'); i >= 0 {
			host = host[:i]
		}
		if controlTLS, err = cfg.Registry.TLS.ClientConfig(host); err != nil {
			return fmt.Errorf("router: registry tls: %w", err)
		}
	}
	rt := router.New(controlAddr, cfg.Domain, cfg.AuthCacheDur(), controlTLS, log)
	rt.SetDataPlaneAuth(cfg.Auth.DataPlane)
	rt.SetAuthMode(cfg.Auth.APIKey)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// The cluster router's hot path is driven by active route/connection cache:
	// misses Reserve through the registry and stale entries fail fast.
	go rt.RunCleanup(ctx) // evict expired auth-cache / stale build-map entries

	// Optional Prometheus metrics endpoint (router_requests_total{plane,result}, §10).
	if cfg.MetricsListen != "" {
		mln, merr := net.Listen("tcp", cfg.MetricsListen)
		if merr != nil {
			return fmt.Errorf("router: metrics listen %s: %w", cfg.MetricsListen, merr)
		}
		mmux := http.NewServeMux()
		mmux.HandleFunc("/metrics", rt.Metrics().Handler())
		msrv := &http.Server{Handler: mmux}
		go func() { <-ctx.Done(); msrv.Close() }()
		go func() {
			if e := msrv.Serve(mln); e != nil && e != http.ErrServerClosed {
				log.Error("router metrics", "err", e)
			}
		}()
	}

	// SO_REUSEPORT so multiple same-host router replicas can share :443 (router §10).
	lc := net.ListenConfig{Control: func(_, _ string, c syscall.RawConn) error {
		var serr error
		if err := c.Control(func(fd uintptr) {
			serr = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_REUSEPORT, 1)
		}); err != nil {
			return err
		}
		return serr
	}}
	ln, err := lc.Listen(ctx, "tcp", cfg.Ingress.Listen)
	if err != nil {
		return fmt.Errorf("router: listen %s: %w", cfg.Ingress.Listen, err)
	}
	var srv *http.Server
	if cfg.Ingress.TLS.Enabled() {
		stls, terr := cfg.Ingress.TLS.ServerConfig()
		if terr != nil {
			return fmt.Errorf("router: tls: %w", terr)
		}
		srv = &http.Server{Handler: rt.Handler(), TLSConfig: stls}
	} else {
		srv = &http.Server{Handler: h2c.NewHandler(rt.Handler(), &http2.Server{})}
	}
	go func() {
		<-ctx.Done()
		srv.Close()
	}()
	log.Info("cluster-ctl router", "listen", cfg.Ingress.Listen, "domain", cfg.Domain, "registry", controlAddr, "tls", cfg.Ingress.TLS.Enabled())
	var serveErr error
	if cfg.Ingress.TLS.Enabled() {
		serveErr = srv.ServeTLS(ln, "", "")
	} else {
		serveErr = srv.Serve(ln)
	}
	if serveErr != nil && serveErr != http.ErrServerClosed {
		return serveErr
	}
	return nil
}
