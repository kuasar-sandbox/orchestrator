package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os/signal"
	"syscall"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

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

	rt := router.New(opAddr, cfg.Domain, cfg.Router.AuthCacheDur(), log)
	rt.SetDataPlaneAuth(cfg.Router.DataPlaneAuth)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Sync the local route cache from the registry op watch (hot-path data plane,
	// cluster.md §5; a cache miss falls back to op /route).
	go rt.RunWatch(ctx)
	go rt.RunCleanup(ctx) // evict expired auth-cache / stale build-map entries

	ln, err := net.Listen("tcp", cfg.Router.Listen)
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
