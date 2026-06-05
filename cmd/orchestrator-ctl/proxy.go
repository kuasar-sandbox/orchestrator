package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/config"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/metrics"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/proxy"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/routetable"
)

// runProxy is the external data-plane proxy worker (proxy_mode=external). It:
//
//   - serves an h2c UDS (--socket) the orchestrator dials: the route-sync stream
//     (route table push + Wake) and the fallback data-forward path;
//
//   - serves the data-plane ingress on --data-listen with SO_REUSEPORT (so several
//     workers share one port), forwarding to envd UDS / floatingip from its synced
//     route table, parking a request until the route is ready (Wake -> resume).
//
//     orchestrator-ctl proxy --socket=<uds> --data-listen=<addr> [--tls-cert --tls-key]
//     [--auth=off|log|enforce] [--park-timeout=30s] [--metrics-listen=<addr>]
func runProxy(args []string, log *slog.Logger) error {
	fs := flag.NewFlagSet("proxy", flag.ExitOnError)
	socket := fs.String("socket", "", "routesync + fallback-forward UDS the orchestrator dials (required)")
	dataListen := fs.String("data-listen", "", "data-plane ingress addr, SO_REUSEPORT (e.g. :443); empty = UDS-only")
	tlsCert := fs.String("tls-cert", "", "TLS cert for the data-plane listener (empty = h2c)")
	tlsKey := fs.String("tls-key", "", "TLS key for the data-plane listener")
	auth := fs.String("auth", "", "data-plane auth until policy arrives: off|log|enforce (default enforce)")
	park := fs.Duration("park-timeout", 30*time.Second, "max time to hold a request awaiting route/resume")
	metricsListen := fs.String("metrics-listen", "", "optional Prometheus text endpoint, e.g. 127.0.0.1:9095")
	_ = fs.Parse(args)
	if *socket == "" {
		return fmt.Errorf("proxy: --socket is required")
	}
	authFallback := *auth
	if authFallback == "" {
		authFallback = config.AuthEnforce
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	mx := metrics.New()
	tbl := routetable.New(*park)
	// Auth mode prefers the orchestrator-pushed policy, falling back to --auth.
	authMode := func() string {
		if m := tbl.Policy().AuthMode; m != "" {
			return m
		}
		return authFallback
	}
	px := proxy.New(tableRouter{tbl}, authMode, log, mx)
	rs := routesync.NewServer(tbl, tbl, log)

	// UDS: route-sync stream (SyncHeader) + fallback data-forward (everything else).
	udsMux := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get(routesync.SyncHeader) == "1" {
			rs.ServeSync(w, r)
			return
		}
		px.ServeHTTP(w, r)
	})
	_ = os.Remove(*socket)
	udsLn, err := net.Listen("unix", *socket)
	if err != nil {
		return fmt.Errorf("proxy: listen %s: %w", *socket, err)
	}
	if err := os.Chmod(*socket, 0o600); err != nil {
		log.Warn("proxy: chmod socket", "err", err)
	}
	go func() {
		if err := serveListener(ctx, udsLn, udsMux, "", "", log); err != nil {
			log.Error("proxy: uds server", "err", err)
		}
	}()

	if *metricsListen != "" {
		go serveMetrics(ctx, *metricsListen, mx, log)
	}

	if *dataListen == "" {
		log.Warn("proxy: no --data-listen; serving fallback-forward over UDS only", "socket", *socket)
		<-ctx.Done()
		return nil
	}
	dataLn, err := listenReusePort(ctx, "tcp", *dataListen)
	if err != nil {
		return fmt.Errorf("proxy: listen %s: %w", *dataListen, err)
	}
	log.Info("orchestrator-ctl proxy serving", "data_listen", *dataListen, "socket", *socket,
		"tls", *tlsCert != "", "auth_fallback", authFallback)
	return serveListener(ctx, dataLn, px, *tlsCert, *tlsKey, log)
}

// tableRouter adapts the synced route table to proxy.Router: resolve (parking +
// Wake) then classify the port with the shared RouteForTarget.
type tableRouter struct{ tbl *routetable.Table }

func (a tableRouter) Route(ctx context.Context, sid string, port int) (proxy.Route, error) {
	r, ok := a.tbl.Resolve(ctx, sid)
	if !ok {
		return proxy.Route{Kind: proxy.KindNotFound}, nil
	}
	return proxy.RouteForTarget(r.Profile, r.EnvdUDS, r.CiUDS, r.FloatingIP, r.AccessToken, port), nil
}
