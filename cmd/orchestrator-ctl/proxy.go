package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/config"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/metrics"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/mmds"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/proxy"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/routetable"
)

// runProxy is the external data-plane proxy worker (proxy_mode=external). It:
//
//   - registers on the orchestrator's config-socket plugin plane (--config-socket,
//     --id) and keeps its route table synced over that single held connection (route
//     push down + Wake up — internal/routesync);
//
//   - serves an h2c UDS (--socket) for the data-plane requests the orchestrator's
//     gateway forwards to it (it advertises this path to the orchestrator at
//     registration);
//
//   - serves the data-plane ingress on --data-listen with SO_REUSEPORT (so several
//     workers share one port), forwarding to envd UDS / floatingip from its synced
//     route table, parking a request until the route is ready (Wake -> resume).
//
//     orchestrator-ctl proxy --config-socket=<uds> --id=<name> --socket=<uds>
//     --data-listen=<addr> [--tls-cert --tls-key] [--auth=off|log|enforce]
//     [--park-timeout=30s] [--metrics-listen=<addr>] [--mmds-listen=<addr>]
func runProxy(args []string, log *slog.Logger) error {
	fs := flag.NewFlagSet("proxy", flag.ExitOnError)
	configSocket := fs.String("config-socket", "", "orchestrator config-socket UDS to register + sync on (required)")
	id := fs.String("id", "", "this proxy worker's plugin id, unique per worker (required)")
	socket := fs.String("socket", "", "UDS this worker serves for gateway-forwarded data-plane requests (required)")
	dataListen := fs.String("data-listen", "", "data-plane ingress addr, SO_REUSEPORT (e.g. :443); empty = UDS-only")
	tlsCert := fs.String("tls-cert", "", "TLS cert for the data-plane listener (empty = h2c)")
	tlsKey := fs.String("tls-key", "", "TLS key for the data-plane listener")
	auth := fs.String("auth", "", "data-plane auth until policy arrives: off|log|enforce (default enforce)")
	park := fs.Duration("park-timeout", 30*time.Second, "max time to hold a request awaiting route/resume")
	metricsListen := fs.String("metrics-listen", "", "optional Prometheus text endpoint, e.g. 127.0.0.1:9095")
	mmdsListen := fs.String("mmds-listen", "", "MMDS metadata-service listen addr (e.g. :19254); empty = off. Set when serve has mmds.enabled, plus a host redirect of 169.254.169.254:80 -> this addr")
	_ = fs.Parse(args)
	if *configSocket == "" || *id == "" || *socket == "" {
		return fmt.Errorf("proxy: --config-socket, --id and --socket are required")
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

	// Register on the orchestrator's config-socket plugin plane and keep the route
	// table synced over that held connection (the table is both the Sink and, since
	// this is a proxy that resumes sandboxes, the WakeSource).
	reg := routesync.Register{
		Subscribe: &routesync.Subscribe{Kind: routesync.KindRouteWake},
		Proxy:     &routesync.Proxy{Socket: routesync.Socket{Path: *socket}},
		Mmds:      *mmdsListen != "",
	}
	dial := func(ctx context.Context) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", *configSocket)
	}
	go routesync.NewSubscriber(dial, *id, reg, tbl, tbl, log).Run(ctx)

	// UDS: the data-plane requests the orchestrator's gateway forwards to this worker.
	_ = os.Remove(*socket)
	udsLn, err := net.Listen("unix", *socket)
	if err != nil {
		return fmt.Errorf("proxy: listen %s: %w", *socket, err)
	}
	if err := os.Chmod(*socket, 0o600); err != nil {
		log.Warn("proxy: chmod socket", "err", err)
	}
	go func() {
		if err := serveListener(ctx, udsLn, px, "", "", log); err != nil {
			log.Error("proxy: uds server", "err", err)
		}
	}()

	if *metricsListen != "" {
		go serveMetrics(ctx, *metricsListen, mx, log)
	}

	// MMDS metadata service: serve envd's access-token hash keyed by the guest's
	// (SNAT'd) source floating IP, from the synced route table. Required when serve
	// runs sandboxes in FC mode (mmds.enabled); the host redirects 169.254.169.254:80
	// to this addr.
	if *mmdsListen != "" {
		mln, err := net.Listen("tcp", *mmdsListen)
		if err != nil {
			return fmt.Errorf("proxy: mmds listen %s: %w", *mmdsListen, err)
		}
		log.Info("proxy mmds metadata service", "mmds_listen", *mmdsListen)
		go func() {
			if err := mmds.New(tbl, *park, log).Serve(ctx, mln); err != nil {
				log.Error("proxy: mmds service", "err", err)
			}
		}()
	}

	if *dataListen == "" {
		log.Warn("proxy: no --data-listen; serving gateway-forward over UDS only", "socket", *socket)
		<-ctx.Done()
		return nil
	}
	dataLn, err := listenReusePort(ctx, "tcp", *dataListen)
	if err != nil {
		return fmt.Errorf("proxy: listen %s: %w", *dataListen, err)
	}
	log.Info("orchestrator-ctl proxy serving", "data_listen", *dataListen, "socket", *socket, "id", *id,
		"config_socket", *configSocket, "tls", *tlsCert != "", "auth_fallback", authFallback)
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
