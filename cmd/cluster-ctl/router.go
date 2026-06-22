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
// (cluster-router.md). It dials the registry's op interface for reserve + route
// resolution. Phase 3 serves plain h2c (dev); router.tls + auth cache are Phase 7.
func runRouter(args []string, log *slog.Logger) error {
	fs := flag.NewFlagSet("router", flag.ExitOnError)
	cfgPath := fs.String("config", "/etc/cluster-ctl/config.yaml", "config file")
	registryAddr := fs.String("registry", "", "override registry op endpoint (UDS path or host:port)")
	listen := fs.String("listen", "", "override router.listen")
	_ = fs.Parse(args)

	cfg, err := clustercfg.Load(*cfgPath)
	if err != nil {
		return err
	}
	opAddr := cfg.Router.Registry
	if *registryAddr != "" {
		opAddr = *registryAddr
	}
	if opAddr == "" {
		opAddr = cfg.Op.Listen // co-located: dial the registry's op UDS
	}
	if *listen != "" {
		cfg.Router.Listen = *listen
	}

	// op-mTLS when the op endpoint is a remote TCP addr and op.tls is configured
	// (cluster.md §5.4); a co-located UDS op stays plain.
	var opTLS *tls.Config
	if !strings.HasPrefix(opAddr, "/") && cfg.Op.TLS.Enabled() {
		host := opAddr
		if i := strings.LastIndexByte(host, ':'); i >= 0 {
			host = host[:i]
		}
		if opTLS, err = cfg.Op.TLS.ClientConfig(host); err != nil {
			return fmt.Errorf("router: op tls: %w", err)
		}
	}
	rt := router.New(opAddr, cfg.Domain, cfg.Router.AuthCacheDur(), opTLS, log)
	rt.SetDataPlaneAuth(cfg.Router.DataPlaneAuth)
	rt.SetAuthMode(cfg.Router.Auth)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Sync the local route cache from the registry op watch (hot-path data plane,
	// cluster.md §5; a cache miss falls back to op /route).
	go rt.RunWatch(ctx)
	go rt.RunCleanup(ctx) // evict expired auth-cache / stale build-map entries

	// Optional Prometheus metrics endpoint (router_requests_total{plane,result}, §10).
	if cfg.Router.MetricsListen != "" {
		mln, merr := net.Listen("tcp", cfg.Router.MetricsListen)
		if merr != nil {
			return fmt.Errorf("router: metrics listen %s: %w", cfg.Router.MetricsListen, merr)
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
	ln, err := lc.Listen(ctx, "tcp", cfg.Router.Listen)
	if err != nil {
		return fmt.Errorf("router: listen %s: %w", cfg.Router.Listen, err)
	}
	var srv *http.Server
	if cfg.Router.TLS.Enabled() {
		stls, terr := cfg.Router.TLS.ServerConfig()
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
	log.Info("cluster-ctl router", "listen", cfg.Router.Listen, "domain", cfg.Domain, "op", opAddr, "tls", cfg.Router.TLS.Enabled())
	var serveErr error
	if cfg.Router.TLS.Enabled() {
		serveErr = srv.ServeTLS(ln, "", "")
	} else {
		serveErr = srv.Serve(ln)
	}
	if serveErr != nil && serveErr != http.ErrServerClosed {
		return serveErr
	}
	return nil
}
