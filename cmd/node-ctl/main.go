// Command node-ctl is the single-node, e2b-compatible sandbox orchestrator.
//
//	node-ctl conductor serve --config <conductor.yaml>          # run the node conductor
//	node-ctl proxy serve --config <proxy.yaml>                  # external data-plane proxy master
//	node-ctl run-sandbox --pidfile=<f> --config-socket=<uds> --run-id=<rid>
//	node-ctl run-builder --pidfile=<f> --config-socket=<uds> --run-id=<rid>
//	                                                                    # in-unit launchers (not for humans)
//	node-ctl config <conductor|proxy> [--template|--config <f>|--resolve]  # config diagnose / generate
//	node-ctl manifest-key <add|list|remove> ...                 # tenant root-key whitelist (admin socket)
//	node-ctl export-sandbox|import-sandbox ...                  # paused-snapshot egress / ingress
//	node-ctl resource <status|list|drain|grant|reclaim>        # node resource controller (hosted in serve via resource_listen)
//	node-ctl version
//
// Templates are built through the e2b API (POST /v3/templates ...), not a CLI.
// The guest runtime image is built by the guest-runtime repo.
package main

import (
	"context"
	"crypto/tls"
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

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/clustercfg"
	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/launcher"
	"github.com/kuasar-sandbox/orchestrator/internal/metrics"
	"github.com/kuasar-sandbox/orchestrator/internal/mmds"
	"github.com/kuasar-sandbox/orchestrator/internal/netns"
	"github.com/kuasar-sandbox/orchestrator/internal/nodelink"
	"github.com/kuasar-sandbox/orchestrator/internal/orch"
	"github.com/kuasar-sandbox/orchestrator/internal/proxy"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/secretbox"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/vswitch"
)

var version = "0.2.0-dev"

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	var err error
	switch os.Args[1] {
	case "conductor":
		err = conductorCmd(os.Args[2:], log)
	case "proxy":
		err = proxyCmd(os.Args[2:], log)
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
	case "resource":
		os.Exit(resourceCmd(os.Args[2:]))
	case "version", "-v", "--version":
		fmt.Println("node-ctl", version)
	default:
		usage()
	}
	if err != nil {
		log.Error("node-ctl", "cmd", os.Args[1], "err", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: node-ctl {conductor|proxy|run-sandbox|run-builder|config|manifest-key|export-sandbox|import-sandbox|resource|version} [args]")
	os.Exit(2)
}

func conductorCmd(args []string, log *slog.Logger) error {
	if len(args) < 1 || args[0] != "serve" {
		return fmt.Errorf("usage: node-ctl conductor serve [--config <conductor.yaml>]")
	}
	return runConductor(args[1:], log)
}

func proxyCmd(args []string, log *slog.Logger) error {
	if len(args) < 1 || args[0] != "serve" {
		return fmt.Errorf("usage: node-ctl proxy serve [--config <proxy.yaml>]")
	}
	return runProxy(args[1:], log)
}

func runConductor(args []string, log *slog.Logger) error {
	fs := flag.NewFlagSet("conductor serve", flag.ExitOnError)
	cfgPath := fs.String("config", "/etc/node-ctl/conductor.yaml", "config file")
	_ = fs.Parse(args)

	cfg, err := config.Load(*cfgPath)
	if err != nil {
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

	core := orch.New(cfg, st, lc, vswitch.New(
		cfg.ConnectorCtl(),
		cfg.Sandbox.Network.Switch,
		vswitch.WithTapFDSocket(cfg.Sandbox.Network.TapFDSocket),
	), log)
	if err := core.InstallUnits(ctx); err != nil {
		return err
	}
	if err := core.Reconcile(ctx); err != nil {
		log.Warn("reconcile", "err", err)
	}
	go core.Reaper(ctx, 5*time.Second)

	// Optionally host the node resource controller in-process (resource_listen,
	// node-resource.md). Disabled => sandboxes use static cgroup.
	if cfg.ResourceListen != nil && cfg.ResourceListen.Enabled {
		probe, err := startResourceController(ctx, cfg.ResourceListen, log)
		if err != nil {
			return fmt.Errorf("resource_listen: %w", err)
		}
		core.SetResourceProbe(probe) // cluster heartbeat reports this node's water level + drain
	}

	// Connect to the cluster registry over node-link (node.md §10) if configured:
	// the node streams its sandbox routes up + executes the registry's commands.
	if cfg.Cluster.NodeLink.Endpoint != "" {
		nodeID := cfg.Cluster.NodeID
		if nodeID == "" {
			nodeID, _ = os.Hostname()
		}
		dataEndpoint := cfg.Cluster.DataEndpoint
		if dataEndpoint == "" {
			dataEndpoint = cfg.API.Listen
		}
		regAddr := cfg.Cluster.NodeLink.Endpoint
		hbInterval := 10 * time.Second
		if cfg.Cluster.HeartbeatInterval != "" {
			if d, err := time.ParseDuration(cfg.Cluster.HeartbeatInterval); err == nil {
				hbInterval = d
			}
		}
		var clientTLS *tls.Config
		if cfg.Cluster.NodeLink.TLS.Cert != "" {
			ct, terr := clustercfg.TLS{Cert: cfg.Cluster.NodeLink.TLS.Cert, Key: cfg.Cluster.NodeLink.TLS.Key, CA: cfg.Cluster.NodeLink.TLS.CA}.ClientConfig("")
			if terr != nil {
				return fmt.Errorf("cluster node-link tls: %w", terr)
			}
			clientTLS = ct
		}
		core.SetClusterContext(ctx) // node-link async work (boots) cancels on serve shutdown
		capacity, buildCap, runtimeDigest := core.ClusterNodeInfo()
		nl := nodelink.NewWithEndpoint(
			regAddr,
			func(dctx context.Context, endpoint string) (net.Conn, error) {
				if strings.HasPrefix(endpoint, "http://") || strings.HasPrefix(endpoint, "https://") {
					if req, err := http.NewRequest(http.MethodGet, endpoint, nil); err == nil && req.URL.Host != "" {
						endpoint = req.URL.Host
					}
				}
				if strings.HasPrefix(endpoint, "/") {
					return (&net.Dialer{}).DialContext(dctx, "unix", endpoint)
				}
				return (&net.Dialer{}).DialContext(dctx, "tcp", endpoint)
			},
			routesync.NodeRegister{
				NodeID: nodeID, Labels: cfg.Cluster.Labels, DataEndpoint: dataEndpoint,
				Capacity: capacity, BuildCapacity: buildCap, RuntimeDigest: runtimeDigest,
			},
			core, hbInterval, clientTLS, log, true,
		)
		go nl.Run(ctx)
		log.Info("node-ctl conductor: node-link to cluster registry", "registry", regAddr, "node_id", nodeID)
	}

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

	// Local control socket: one UDS multiplexes run assignment/result, task specs,
	// manifest-key management (admin plane, pid ∈ admin_pidfile or, when unset,
	// the socket's 0600 perms), plugin route registration, and the api plane over
	// plain h2c (X-API-KEY). See docs §6.
	// The plugin registry is shared: the config-socket plugin plane Adds/Removes
	// registrations (proxy master, route observers); the external-mode proxyForwarder
	// reads it to forward fallback data-plane requests to the registered proxy socket.
	plugins := configsock.NewRegistry()
	cs := configsock.New(cfg.Paths.ConfigSocket, configsock.Deps{
		Provider:           core,
		Admin:              core,
		MMDSSecretsAdmin:   core,
		API:                apiH,
		AdminPidfile:       cfg.Paths.AdminPidfile,
		RouteSource:        core,
		Plugins:            plugins,
		PluginPidfile:      cfg.Paths.PluginPidfile,
		MMDSSecretsPidfile: cfg.Paths.MMDSSecretsPidfile,
	}, log)
	configReady := make(chan struct{})
	configDone := make(chan error, 1)
	go func() {
		err := cs.ServeReady(ctx, configReady)
		if err != nil {
			log.Error("config-socket", "err", err)
		}
		configDone <- err
	}()
	select {
	case <-configReady:
	case err := <-configDone:
		if err == nil {
			return fmt.Errorf("config-socket stopped before becoming ready")
		}
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := core.StartRunPools(ctx); err != nil {
		return err
	}
	go core.BuildPool(ctx, 2*time.Second)

	// Data-plane handler depends on proxy_mode: in-process proxy (internal),
	// proxyForwarder to worker (external), or reject (off). External mode also
	// starts the route-sync client that pushes the route table to each worker.
	mx := metrics.New()
	var proxyNS *netns.NetNS
	if cfg.Proxy.Mode == config.ProxyInternal && cfg.Proxy.ProxyNetNS != "" {
		proxyNS, err = openProxyNetNS(cfg.Proxy.ProxyNetNS)
		if err != nil {
			return err
		}
		defer proxyNS.Close()
		log.Info("node-ctl internal proxy forwarding netns", "proxy_netns", cfg.Proxy.ProxyNetNS)
	}
	dataH := buildDataPlane(cfg, core, plugins, mx, log, proxyNS)

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
	// Optional dedicated data-plane listener. In external mode the proxy master
	// owns data_listen, so the conductor does not bind it.
	if cfg.Proxy.Mode != config.ProxyExternal && cfg.Proxy.DataListen != "" {
		ln, err := net.Listen("tcp", cfg.Proxy.DataListen)
		if err != nil {
			return fmt.Errorf("data_listen %s: %w", cfg.Proxy.DataListen, err)
		}
		log.Info("node-ctl data-plane listener", "data_listen", cfg.Proxy.DataListen)
		go func() {
			if err := serveListener(ctx, ln, dataH, cfg.API.TLS.Cert, cfg.API.TLS.Key, log); err != nil {
				log.Error("data-plane listener", "err", err)
			}
		}()
	}

	// MMDS metadata service (mmds.enabled): re-keys envd (launched in FC mode) to fresh
	// per-identity tokens at /init. Internal mode serves it here from the orchestrator's
	// live sandbox set; external mode's proxy workers serve it from the shared
	// route table on the master's mmds_listen. The host must redirect
	// 169.254.169.254:80 -> mmds.listen.
	if cfg.MMDS.Enabled && cfg.Proxy.Mode == config.ProxyInternal {
		mln, err := listenTCPInNetNS(proxyNS, cfg.MMDS.Listen)
		if err != nil {
			return fmt.Errorf("mmds listen %s: %w", cfg.MMDS.Listen, err)
		}
		log.Info("mmds metadata service", "listen", cfg.MMDS.Listen, "proxy_netns", cfg.Proxy.ProxyNetNS)
		go func() {
			if err := mmds.New(core, cfg.ParkTimeoutDur(), log).Serve(ctx, mln); err != nil {
				log.Error("mmds service", "err", err)
			}
		}()
	}

	if cfg.API.TLS.Cert == "" || cfg.API.TLS.Key == "" {
		log.Info("serving plain HTTP (dev): point the SDK with E2B_API_URL/E2B_SANDBOX_URL")
	}
	ln, err := net.Listen("tcp", cfg.API.Listen)
	if err != nil {
		return fmt.Errorf("listen %s: %w", cfg.API.Listen, err)
	}
	log.Info("node-ctl serving", "listen", cfg.API.Listen, "domain", cfg.API.Domain, "proxy_mode", cfg.Proxy.Mode)
	return serveListener(ctx, ln, mux, cfg.API.TLS.Cert, cfg.API.TLS.Key, log)
}

// buildDataPlane wires the data-plane handler for the configured proxy_mode.
func buildDataPlane(cfg *config.Config, core *orch.Orchestrator, plugins *configsock.Registry, mx *metrics.M, log *slog.Logger, proxyNS *netns.NetNS) http.Handler {
	switch cfg.Proxy.Mode {
	case config.ProxyOff:
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			mx.Inc(`data_requests_total{result="off"}`)
			http.Error(w, "data plane disabled (proxy_mode=off)", http.StatusNotImplemented)
		})
	case config.ProxyExternal:
		// The proxy master registers on the config-socket plugin plane and exposes
		// one UDS for proxyForwarder fallback requests.
		log.Info("external proxy mode: proxy master registers on the config socket")
		return newProxyForwarder(plugins, mx, log)
	default: // internal
		return proxy.NewWithDialer(core, func() string { return cfg.Proxy.Auth }, log, mx, routeDialerInNetNS(proxyNS), cfg.Paths.RunRoot)
	}
}
