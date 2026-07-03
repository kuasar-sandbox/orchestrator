package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/metrics"
	"github.com/kuasar-sandbox/orchestrator/internal/mmds"
	"github.com/kuasar-sandbox/orchestrator/internal/proxy"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/routetable"
)

// runProxy is the external data-plane proxy worker (proxy_mode=external). It reads
// the worker config (--config, default /etc/node-ctl/proxy.yaml) for the shared
// policy + endpoints, and takes only per-instance identity on the command line. It:
//
//   - registers on serve's config-socket plugin plane (config_socket + --id) and
//     keeps its route table synced over that single held connection (route push
//     down + Wake up — internal/routesync);
//
//   - serves an h2c UDS (--socket, default <dir(config_socket)>/<id>.sock) for the
//     data-plane requests serve's gateway forwards to it (advertised at registration);
//
//   - serves the data-plane ingress on data_listen with SO_REUSEPORT (so several
//     workers share one port), forwarding to envd UDS / floatingip from its synced
//     route table, parking a request until the route is ready (Wake -> resume).
//
//     node-ctl proxy serve --config <proxy.yaml> --id <name> [--socket <uds>]
//     [--metrics-listen <addr>] [--mmds]
func runProxy(args []string, log *slog.Logger) error {
	fs := flag.NewFlagSet("proxy serve", flag.ExitOnError)
	cfgPath := fs.String("config", "/etc/node-ctl/proxy.yaml", "worker config file")
	id := fs.String("id", "", "this worker's plugin id, unique per worker (required)")
	socket := fs.String("socket", "", `UDS this worker serves for gateway-forwarded requests; "" = <dir(config_socket)>/<id>.sock`)
	metricsListen := fs.String("metrics-listen", "", "optional Prometheus text endpoint (per-instance), e.g. 127.0.0.1:9095")
	enableMMDS := fs.Bool("mmds", false, "host the FC MMDS metadata service on this worker (addr = mmds.listen); set on exactly one worker")
	_ = fs.Parse(args)
	if *id == "" {
		return fmt.Errorf("proxy: --id is required")
	}
	cfg, err := config.LoadProxy(*cfgPath)
	if err != nil {
		return err
	}
	socketPath := *socket
	if socketPath == "" {
		socketPath = filepath.Join(filepath.Dir(cfg.ConfigSocket), *id+".sock")
	}
	mmdsListen := ""
	if *enableMMDS {
		mmdsListen = cfg.MMDSListen
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	mx := metrics.New()
	tbl := routetable.New(cfg.ParkTimeoutDur())
	// Auth mode prefers serve's pushed policy, falling back to the worker's config.
	authMode := func() string {
		if m := tbl.Policy().AuthMode; m != "" {
			return m
		}
		return cfg.Auth
	}
	px := proxy.New(tableRouter{tbl}, authMode, log, mx)

	// Register on serve's config-socket plugin plane and keep the route table synced
	// over that held connection (the table is both the Sink and, since this is a proxy
	// that resumes sandboxes, the WakeSource).
	reg := routesync.Register{
		Subscribe: &routesync.Subscribe{Kind: routesync.KindRouteWake},
		Proxy:     &routesync.Proxy{Socket: routesync.Socket{Path: socketPath}},
		Mmds:      *enableMMDS,
	}
	dial := func(ctx context.Context) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", cfg.ConfigSocket)
	}
	go routesync.NewSubscriber(dial, *id, reg, tbl, tbl, log).Run(ctx)

	// UDS: the data-plane requests serve's gateway forwards to this worker.
	_ = os.Remove(socketPath)
	udsLn, err := net.Listen("unix", socketPath)
	if err != nil {
		return fmt.Errorf("proxy: listen %s: %w", socketPath, err)
	}
	if err := os.Chmod(socketPath, 0o600); err != nil {
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
	// runs sandboxes in FC mode (mmds.enabled); enable on exactly one worker (--mmds);
	// the host redirects 169.254.169.254:80 to mmds.listen.
	if mmdsListen != "" {
		mln, err := net.Listen("tcp", mmdsListen)
		if err != nil {
			return fmt.Errorf("proxy: mmds listen %s: %w", mmdsListen, err)
		}
		log.Info("proxy mmds metadata service", "mmds_listen", mmdsListen)
		go func() {
			if err := mmds.New(tbl, cfg.ParkTimeoutDur(), log).Serve(ctx, mln); err != nil {
				log.Error("proxy: mmds service", "err", err)
			}
		}()
	}

	if cfg.DataListen == "" {
		log.Warn("proxy: no data_listen; serving gateway-forward over UDS only", "socket", socketPath)
		<-ctx.Done()
		return nil
	}
	dataLn, err := listenReusePort(ctx, "tcp", cfg.DataListen)
	if err != nil {
		return fmt.Errorf("proxy: listen %s: %w", cfg.DataListen, err)
	}
	log.Info("node-ctl proxy serving", "data_listen", cfg.DataListen, "socket", socketPath, "id", *id,
		"config_socket", cfg.ConfigSocket, "tls", cfg.TLS.Cert != "", "auth_fallback", cfg.Auth)
	return serveListener(ctx, dataLn, px, cfg.TLS.Cert, cfg.TLS.Key, log)
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
