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
//	node-ctl resource <status|list|drain>                      # node reservation controller inspection (hosted in serve via resource_listen)
//	node-ctl builder status                                    # durable Builder admission status (admin socket)
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
	"github.com/kuasar-sandbox/orchestrator/internal/nodectl"
	"github.com/kuasar-sandbox/orchestrator/internal/nodelink"
	"github.com/kuasar-sandbox/orchestrator/internal/orch"
	"github.com/kuasar-sandbox/orchestrator/internal/proxy"
	"github.com/kuasar-sandbox/orchestrator/internal/proxystats"
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
	case "builder":
		os.Exit(builderCmd(os.Args[2:]))
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
	fmt.Fprintln(os.Stderr, "usage: node-ctl {conductor|proxy|run-sandbox|run-builder|config|manifest-key|export-sandbox|import-sandbox|resource|builder|version} [args]")
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
	// Resolve the dynamic controller endpoint and all resource policy before
	// opening the durable store or touching systemd. The exact canonical socket
	// identity is then shared by the controller's owner/inventory state and
	// sandbox YAML; Listen remains the bind path selected by nodectl.Resolve.
	var resolvedResources *nodectl.Resolved
	if cfg.ResourceListen != nil && cfg.ResourceListen.Enabled {
		resolvedResources, err = nodectl.Resolve(cfg.ResourceListen)
		if err != nil {
			return fmt.Errorf("resource_listen: %w", err)
		}
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

	plugins := configsock.NewRegistry()
	mx := metrics.New()
	core := orch.New(cfg, st, lc, vswitch.New(
		cfg.ConnectorCtl(),
		cfg.Sandbox.Network.Switch,
		vswitch.WithTapFDSocket(cfg.Sandbox.Network.TapFDSocket),
	), log)
	core.SetMetrics(mx)
	core.SetLifecycleContext(ctx)
	core.SetProxyRouteBarrierCoordinator(plugins)
	if resolvedResources != nil {
		core.SetResourceControllerSocketIdentity(resolvedResources.SocketIdentity)
	}
	// This defer is registered after the store and launcher closes, so it runs
	// first on every conductor exit path. Cancel lifecycle admission and
	// cancellable launch work, then keep dependencies open until every accepted
	// launch and accepted pause/export operation has stopped using the store and
	// launcher. The service manager remains the outer bound for a permanently
	// unavailable dependency.
	defer func() {
		stop()
		if err := core.DrainBuilds(context.Background()); err != nil {
			log.Error("drain builder executions", "err", err)
		}
		if err := core.DrainLaunches(context.Background()); err != nil {
			log.Error("drain sandbox launches", "err", err)
		}
		if err := core.DrainPauses(context.Background()); err != nil {
			log.Error("drain accepted sandbox operations", "err", err)
		}
	}()
	if err := core.InstallUnits(ctx); err != nil {
		return err
	}
	if err := core.ReconcileSandboxes(ctx); err != nil {
		return fmt.Errorf("reconcile sandboxes: %w", err)
	}

	// Optionally host the node resource controller in-process (resource_listen,
	// node-resource.md). Disabled => sandboxes use static cgroup.
	if cfg.ResourceListen != nil && cfg.ResourceListen.Enabled {
		probe, err := startResourceController(ctx, cfg.ResourceListen, resolvedResources, cfg.Paths.RunRoot, log)
		if err != nil {
			return fmt.Errorf("resource_listen: %w", err)
		}
		core.SetResourceProbe(probe) // cluster heartbeat reports this node's water level + drain
		if provider, ok := probe.(orch.SandboxResourceProvider); ok {
			core.SetSandboxResourceProvider(provider)
		}
	}

	// Connect to the cluster registry over node-link (node.md §10) if configured:
	// the node streams its sandbox routes up + executes the registry's commands.
	var startNodeLink func()
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
		capacity, registrationCap, executionCap, runtimeDigest := core.ClusterNodeInfo()
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
				Capacity: capacity, BuildRegistrationCapacity: registrationCap,
				BuildExecutionCapacity: executionCap, RuntimeDigest: runtimeDigest,
			},
			core, hbInterval, clientTLS, log, true,
		)
		startNodeLink = func() {
			go nl.Run(ctx)
			log.Info("node-ctl conductor: node-link to cluster registry", "registry", regAddr, "node_id", nodeID)
		}
	}

	// Traffic stats providers are wired before either API listener can accept a
	// request. Internal mode uses the same WorkerStats→MasterStats absolute update
	// path in-process; external mode queries the registered proxy master's cache.
	var internalWorkerStats *proxystats.WorkerStats
	switch cfg.Proxy.Mode {
	case config.ProxyInternal:
		internalWorkerStats = proxystats.NewWorkerStats()
		masterStats := proxystats.NewMasterStats(mx, []string{"internal"})
		if err := masterStats.BeginWorker("internal", 1); err != nil {
			return err
		}
		senderDone, err := internalWorkerStats.StartSender(ctx, "internal", 1, func(frame proxystats.Frame) error {
			return masterStats.Receive("internal", 1, frame)
		})
		if err != nil {
			return err
		}
		go func() {
			if err := <-senderDone; err != nil && ctx.Err() == nil {
				log.Error("internal proxy stats sender", "err", err)
			}
		}()
		routeExists := func(sandboxID string) bool {
			_, found, err := core.LookupExec(ctx, sandboxID)
			// A transient store failure must retain the entry; only a definitive
			// route miss permits removal of its idle timestamp.
			return err != nil || found
		}
		go internalWorkerStats.RunGC(ctx, routeExists, 5*time.Minute)
		go masterStats.RunGC(ctx, routeExists, time.Minute)
		core.SetSandboxTrafficProvider(masterStats)
	case config.ProxyExternal:
		core.SetSandboxTrafficProvider(&externalTrafficProvider{plugins: plugins})
	}

	// North api handler (e2b control plane + export/import). Built once and shared by
	// the TLS listener below and the local control socket's api plane. The node-uniform
	// VM resources (vcpu/memory from config; disk = writable overlay seed) are surfaced
	// in list/get responses, which the e2b SDK's ListedSandbox model requires.
	diskMB := 0
	if fi, err := os.Stat(cfg.Sandbox.Boot.OverlayDiffTemplate); err == nil {
		diskMB = int(fi.Size() >> 20)
	}
	res := api.Resources{VCPU: cfg.Sandbox.Resources.Policy().Capacity.CPU, MemoryMB: cfg.Sandbox.Resources.MemoryMiB(), DiskMB: diskMB}
	apiH := api.New(core, cfg.API.Domain, res, log).Handler()

	// Local control socket: one UDS multiplexes run assignment/result, task specs,
	// manifest-key management (admin plane, pid ∈ admin_pidfile or, when unset,
	// the socket's 0600 perms), plugin route registration, and the api plane over
	// plain h2c (X-API-KEY). See docs §6.
	// The plugin registry is shared: the config-socket plugin plane Adds/Removes
	// registrations (proxy master, route observers); the external-mode proxyForwarder
	// reads it to forward fallback data-plane requests to the registered proxy socket.
	cs := configsock.New(cfg.Paths.ConfigSocket, configsock.Deps{
		Provider:                     core,
		Admin:                        core,
		MMDSRouteSecretAdmin:         core,
		BuilderAdmissionAdmin:        core,
		MaxMMDSRouteSecretValueBytes: cfg.MMDS.Routes.MaxSecretValueBytes,
		API:                          apiH,
		AdminPidfile:                 cfg.Paths.AdminPidfile,
		RouteSource:                  core,
		Plugins:                      plugins,
		PluginPidfile:                cfg.Paths.PluginPidfile,
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
	// A live builder can finish immediately after it is adopted. Bring the
	// authenticated phase/result endpoint up first, then attach recovery
	// monitors, and only then expose this node to cluster dispatch or admit new
	// run-pool work.
	if err := core.ReconcileBuilds(ctx); err != nil {
		return fmt.Errorf("reconcile builds: %w", err)
	}
	go core.Reaper(ctx, 5*time.Second)
	if err := core.StartRunPools(ctx); err != nil {
		return err
	}
	go core.BuildPool(ctx, 2*time.Second)
	if startNodeLink != nil {
		startNodeLink()
	}

	// Data-plane handler depends on proxy_mode: in-process proxy (internal),
	// proxyForwarder to worker (external), or reject (off). External mode also
	// starts the route-sync client that pushes the route table to each worker.
	var proxyNS *netns.NetNS
	if cfg.Proxy.Mode == config.ProxyInternal && cfg.Proxy.ProxyNetNS != "" {
		proxyNS, err = openProxyNetNS(cfg.Proxy.ProxyNetNS)
		if err != nil {
			return err
		}
		defer proxyNS.Close()
		log.Info("node-ctl internal proxy forwarding netns", "proxy_netns", cfg.Proxy.ProxyNetNS)
	}
	dataH := buildDataPlane(cfg, core, plugins, mx, internalWorkerStats, log, proxyNS)

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
	// route table on the conductor-owned MMDS listen delivered to the master.
	// The host must redirect
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
func buildDataPlane(cfg *config.Config, core *orch.Orchestrator, plugins *configsock.Registry, mx *metrics.M, workerStats *proxystats.WorkerStats, log *slog.Logger, proxyNS *netns.NetNS) http.Handler {
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
		counter := proxy.Counter(mx)
		if workerStats != nil {
			counter = workerStats
		}
		return proxy.NewWithDialer(core, func() string { return cfg.Proxy.Auth }, log, counter, routeDialerInNetNS(proxyNS), cfg.Paths.RunRoot).
			WithTrafficTracker(workerStats)
	}
}
