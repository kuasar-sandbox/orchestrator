// Command orchestrator-ctl is the single-node, e2b-compatible sandbox orchestrator.
//
//	orchestrator-ctl serve     --config <yaml>                          # run the daemon
//	orchestrator-ctl run-sandbox --pidfile=<f> --config-socket=<uds> --sandbox-id=<sid>
//	orchestrator-ctl run-builder --pidfile=<f> --config-socket=<uds> --build-id=<bid>
//	                                                                    # in-unit launchers (not for humans)
//	orchestrator-ctl version
//
// Templates are built through the e2b API (POST /v3/templates ...), not a CLI.
// sandbox-runtime-e2b.erofs is assembled by deps/build-runtime-e2b.sh (Makefile).
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
	"strings"
	"syscall"
	"time"

	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/api"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/config"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/launcher"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/metrics"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/mmds"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/orch"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/proxy"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/secretbox"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/store"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/vswitch"
)

var version = "0.2.0-dev"

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	var err error
	switch os.Args[1] {
	case "serve":
		err = serve(os.Args[2:], log)
	case "proxy":
		err = runProxy(os.Args[2:], log)
	case "run-sandbox":
		err = runSandbox(os.Args[2:], log)
	case "run-builder":
		err = runBuilder(os.Args[2:], log)
	case "config":
		err = configCmd(os.Args[2:], log)
	case "manifest-key":
		err = manifestKeyCmd(os.Args[2:], log)
	case "export-sandbox":
		err = exportSandboxCmd(os.Args[2:], log)
	case "import-sandbox":
		err = importSandboxCmd(os.Args[2:], log)
	case "version", "-v", "--version":
		fmt.Println("orchestrator-ctl", version)
	default:
		usage()
	}
	if err != nil {
		log.Error("orchestrator-ctl", "cmd", os.Args[1], "err", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: orchestrator-ctl {serve|proxy|run-sandbox|run-builder|config|manifest-key|export-sandbox|import-sandbox|version} [flags]")
	os.Exit(2)
}

func serve(args []string, log *slog.Logger) error {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	cfgPath := fs.String("config", "/etc/orchestrator-ctl/config.yaml", "config file")
	proxyMode := fs.String("proxy", "", "override proxy_mode: internal|external|off")
	proxySock := fs.String("proxy-socket", "", "override proxy_sockets (comma-separated UDS, external mode)")
	_ = fs.Parse(args)

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	if *proxyMode != "" {
		cfg.Proxy.Mode = *proxyMode
	}
	if *proxySock != "" {
		cfg.Proxy.Sockets = splitComma(*proxySock)
	}
	if err := cfg.ValidateProxy(); err != nil { // re-check after flag overrides
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	box, err := secretbox.NewFromColonHex(cfg.EncryptionKeySpec())
	if err != nil {
		return err
	}
	st, err := store.Open(cfg.Paths.DBPath, box)
	if err != nil {
		return err
	}
	defer st.Close()
	if err := os.Chmod(cfg.Paths.DBPath, 0o600); err != nil {
		log.Warn("chmod db", "err", err)
	}

	lc, err := launcher.NewSystemd(ctx)
	if err != nil {
		return err
	}
	defer lc.Close()

	core := orch.New(cfg, st, lc, vswitch.New(cfg.VswitchCtl(), cfg.Sandbox.Network.Switch), log)
	if err := core.InstallUnits(ctx); err != nil {
		return err
	}
	if err := core.Reconcile(ctx); err != nil {
		log.Warn("reconcile", "err", err)
	}
	go core.Reaper(ctx, 5*time.Second)
	go core.BuildPool(ctx, 2*time.Second)

	// North api handler (e2b control plane + export/import). Built once and shared by
	// the TLS listener below and the local control socket's api plane. The node-uniform
	// VM resources (vcpu/memory from config; disk = writable overlay seed) are surfaced
	// in list/get responses, which the e2b SDK's ListedSandbox model requires.
	diskMB := 0
	if fi, err := os.Stat(cfg.Sandbox.Boot.OverlayDiffTemplate); err == nil {
		diskMB = int(fi.Size() >> 20)
	}
	res := api.Resources{VCPU: cfg.Sandbox.Resources.VCPU, MemoryMB: cfg.Sandbox.Resources.MemoryMiB(), DiskMB: diskMB}
	apiH := api.New(core, cfg.API.Domain, res, log).Handler()

	// Local control socket: one UDS, three planes — task LaunchSpec (run-sandbox /
	// run-builder; SO_PEERCRED pid == id pidfile), manifest-key management (admin plane, pid ∈
	// admin_pidfile or, when unset, the socket's 0600 perms), and the api plane over
	// plain h2c (X-API-KEY). See docs §6.
	cs := configsock.New(cfg.Paths.ConfigSocket, configsock.Deps{
		Provider:     core,
		Admin:        core,
		API:          apiH,
		AdminPidfile: cfg.Paths.AdminPidfile,
	}, log)
	go func() {
		if err := cs.Serve(ctx); err != nil {
			log.Error("config-socket", "err", err)
		}
	}()

	// Data-plane handler depends on proxy_mode: in-process proxy (internal),
	// forward-to-worker gateway (external), or reject (off). External mode also
	// starts the route-sync client that pushes the route table to each worker.
	mx := metrics.New()
	dataH := buildDataPlane(ctx, cfg, core, mx, log)

	// North handler: api.<domain> -> control plane; <port>-<sid>.<domain> -> data plane.
	mux := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host := r.Host
		if i := strings.IndexByte(host, ':'); i >= 0 {
			host = host[:i]
		}
		if host == "api."+cfg.API.Domain || strings.HasPrefix(host, "api.") {
			apiH.ServeHTTP(w, r)
			return
		}
		dataH.ServeHTTP(w, r)
	})

	if cfg.Proxy.MetricsListen != "" {
		go serveMetrics(ctx, cfg.Proxy.MetricsListen, mx, log)
	}
	// Optional dedicated data-plane listener. In external mode the proxy workers
	// own data_listen (SO_REUSEPORT), so the orchestrator does not bind it.
	if cfg.Proxy.Mode != config.ProxyExternal && cfg.Proxy.DataListen != "" {
		ln, err := net.Listen("tcp", cfg.Proxy.DataListen)
		if err != nil {
			return fmt.Errorf("data_listen %s: %w", cfg.Proxy.DataListen, err)
		}
		log.Info("orchestrator-ctl data-plane listener", "data_listen", cfg.Proxy.DataListen)
		go func() {
			if err := serveListener(ctx, ln, dataH, cfg.API.TLS.Cert, cfg.API.TLS.Key, log); err != nil {
				log.Error("data-plane listener", "err", err)
			}
		}()
	}

	// MMDS metadata service (mmds.enabled): re-keys envd (launched in FC mode) to fresh
	// per-identity tokens at /init. Internal mode serves it here from the orchestrator's
	// live sandbox set; external mode's proxy worker serves it from its synced route
	// table (--mmds-listen). The host must redirect 169.254.169.254:80 -> mmds.listen.
	if cfg.MMDS.Enabled && cfg.Proxy.Mode == config.ProxyInternal {
		mln, err := net.Listen("tcp", cfg.MMDS.Listen)
		if err != nil {
			return fmt.Errorf("mmds listen %s: %w", cfg.MMDS.Listen, err)
		}
		log.Info("mmds metadata service", "listen", cfg.MMDS.Listen)
		go func() {
			if err := mmds.New(core, cfg.ParkTimeoutDur(), log).Serve(ctx, mln); err != nil {
				log.Error("mmds service", "err", err)
			}
		}()
	}

	if cfg.API.TLS.Cert == "" || cfg.API.TLS.Key == "" {
		log.Warn("serving plain HTTP (dev): point the SDK with E2B_API_URL/E2B_SANDBOX_URL")
	}
	ln, err := net.Listen("tcp", cfg.API.Listen)
	if err != nil {
		return fmt.Errorf("listen %s: %w", cfg.API.Listen, err)
	}
	log.Info("orchestrator-ctl serving", "listen", cfg.API.Listen, "domain", cfg.API.Domain, "proxy_mode", cfg.Proxy.Mode)
	return serveListener(ctx, ln, mux, cfg.API.TLS.Cert, cfg.API.TLS.Key, log)
}

// buildDataPlane wires the data-plane handler for the configured proxy_mode.
func buildDataPlane(ctx context.Context, cfg *config.Config, core *orch.Orchestrator, mx *metrics.M, log *slog.Logger) http.Handler {
	switch cfg.Proxy.Mode {
	case config.ProxyOff:
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			mx.Inc(`data_requests_total{result="off"}`)
			http.Error(w, "data plane disabled (proxy_mode=off)", http.StatusNotImplemented)
		})
	case config.ProxyExternal:
		rc := routesync.NewClient(cfg.Proxy.Sockets, core, log)
		go rc.Run(ctx)
		log.Info("route-sync client started", "proxies", cfg.Proxy.Sockets)
		return newGateway(cfg.Proxy.Sockets, mx, log)
	default: // internal
		return proxy.New(core, func() string { return cfg.Proxy.Auth }, log, mx)
	}
}

func splitComma(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
