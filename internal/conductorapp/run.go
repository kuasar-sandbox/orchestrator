package conductorapp

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	conductorextension "github.com/kuasar-sandbox/orchestrator/app/conductor/extension"
	publicconfig "github.com/kuasar-sandbox/orchestrator/config"
	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/appnet"
	"github.com/kuasar-sandbox/orchestrator/internal/conductorext"
	"github.com/kuasar-sandbox/orchestrator/internal/configresolve"
	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/launcher"
	"github.com/kuasar-sandbox/orchestrator/internal/metrics"
	"github.com/kuasar-sandbox/orchestrator/internal/mmds"
	"github.com/kuasar-sandbox/orchestrator/internal/netns"
	"github.com/kuasar-sandbox/orchestrator/internal/nodectl"
	"github.com/kuasar-sandbox/orchestrator/internal/nodelink"
	"github.com/kuasar-sandbox/orchestrator/internal/orch"
	"github.com/kuasar-sandbox/orchestrator/internal/proxystats"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/vswitch"
)

// Run assembles and serves the conductor using a frozen declarative config,
// exact node-ctl executable path, and already resolved process runtime. Built-in
// node-ctl and custom conductor Apps enter this same function.
func Run(parent context.Context, cfg *publicconfig.Conductor, nodeCtlExecutable string, runtime *Runtime) error {
	if cfg == nil || runtime == nil || runtime.Logger == nil || runtime.SecretBox == nil {
		return fmt.Errorf("conductor app: unresolved startup state")
	}
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	logger := runtime.Logger
	executables := configresolve.ExecutablesForNodeCtl(nodeCtlExecutable)

	var resolvedResources *nodectl.Resolved
	var err error
	if cfg.ResourceListen != nil && cfg.ResourceListen.Enabled {
		resolvedResources, err = nodectl.Resolve(cfg.ResourceListen)
		if err != nil {
			return fmt.Errorf("resource_listen: %w", err)
		}
	}

	storage, err := store.Open(cfg.Paths.DBPath, runtime.SecretBox)
	if err != nil {
		cancel()
		return err
	}
	defer storage.Close()
	if err := os.Chmod(cfg.Paths.DBPath, 0o600); err != nil {
		logger.Warn("chmod db", "err", err)
	}

	lifecycle, err := launcher.NewSystemd(ctx)
	if err != nil {
		cancel()
		return err
	}
	defer lifecycle.Close()

	plugins := configsock.NewRegistry()
	mx := metrics.New()
	core := orch.NewResolved(cfg, storage, lifecycle, vswitch.New(
		executables.ConnectorCtl(),
		cfg.Sandbox.Network.Switch,
		vswitch.WithTapFDSocket(cfg.Sandbox.Network.TapFDSocket),
	), runtime.Files, logger)
	core.SetExecutables(executables)
	core.SetMetrics(mx)
	core.SetLifecycleContext(ctx)
	core.SetProxyRouteBarrierCoordinator(plugins)
	if resolvedResources != nil {
		core.SetResourceControllerSocketIdentity(resolvedResources.SocketIdentity)
	}
	defer func() {
		cancel()
		if err := core.DrainBuilds(context.Background()); err != nil {
			logger.Error("drain builder executions", "err", err)
		}
		if err := core.DrainLaunches(context.Background()); err != nil {
			logger.Error("drain sandbox launches", "err", err)
		}
		if err := core.DrainPauses(context.Background()); err != nil {
			logger.Error("drain accepted sandbox operations", "err", err)
		}
	}()
	startedExtension, err := startExtension(ctx, runtime, storage, core)
	if err != nil {
		return err
	}
	var apiHandler http.Handler
	if startedExtension != nil {
		apiHandler = newAPIHandler(cfg, core, logger)
		if startedExtension.api != nil {
			apiHandler, err = wrapExtensionAPI(startedExtension.api, apiHandler)
			if err != nil {
				return err
			}
		}
	}
	if err := core.InstallUnits(ctx); err != nil {
		return err
	}
	if err := core.ReconcileSandboxes(ctx); err != nil {
		return fmt.Errorf("reconcile sandboxes: %w", err)
	}

	if cfg.ResourceListen != nil && cfg.ResourceListen.Enabled {
		probe, err := StartResourceController(ctx, cfg.ResourceListen, resolvedResources, cfg.Paths.RunRoot, logger)
		if err != nil {
			return fmt.Errorf("resource_listen: %w", err)
		}
		core.SetResourceProbe(probe)
		if provider, ok := probe.(orch.SandboxResourceProvider); ok {
			core.SetSandboxResourceProvider(provider)
		}
	}

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
		registryAddress := cfg.Cluster.NodeLink.Endpoint
		heartbeatInterval := 10 * time.Second
		if cfg.Cluster.HeartbeatInterval != "" {
			if duration, err := time.ParseDuration(cfg.Cluster.HeartbeatInterval); err == nil {
				heartbeatInterval = duration
			}
		}
		capacity, registrationCapacity, executionCapacity, runtimeDigest := core.ClusterNodeInfo()
		nodeClient := nodelink.NewWithEndpoint(
			registryAddress,
			func(dialCtx context.Context, endpoint string) (net.Conn, error) {
				if strings.HasPrefix(endpoint, "http://") || strings.HasPrefix(endpoint, "https://") {
					if request, err := http.NewRequest(http.MethodGet, endpoint, nil); err == nil && request.URL.Host != "" {
						endpoint = request.URL.Host
					}
				}
				if strings.HasPrefix(endpoint, "/") {
					return (&net.Dialer{}).DialContext(dialCtx, "unix", endpoint)
				}
				return (&net.Dialer{}).DialContext(dialCtx, "tcp", endpoint)
			},
			routesync.NodeRegister{
				NodeID: nodeID, Labels: cfg.Cluster.Labels, DataEndpoint: dataEndpoint,
				Capacity: capacity, BuildRegistrationCapacity: registrationCapacity,
				BuildExecutionCapacity: executionCapacity, RuntimeDigest: runtimeDigest,
			},
			core, heartbeatInterval, runtime.NodeLinkTLS, logger, true,
		)
		startNodeLink = func() {
			go nodeClient.Run(ctx)
			logger.Info("node-ctl conductor: node-link to cluster registry", "registry", registryAddress, "node_id", nodeID)
		}
	}

	var internalWorkerStats *proxystats.WorkerStats
	switch cfg.Proxy.Mode {
	case publicconfig.ProxyInternal:
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
				logger.Error("internal proxy stats sender", "err", err)
			}
		}()
		routeExists := func(sandboxID string) bool {
			_, found, err := core.LookupExec(ctx, sandboxID)
			return err != nil || found
		}
		go internalWorkerStats.RunGC(ctx, routeExists, 5*time.Minute)
		go masterStats.RunGC(ctx, routeExists, time.Minute)
		core.SetSandboxTrafficProvider(masterStats)
	case publicconfig.ProxyExternal:
		core.SetSandboxTrafficProvider(&ExternalTrafficProvider{Plugins: plugins})
	}

	if apiHandler == nil {
		apiHandler = newAPIHandler(cfg, core, logger)
	}
	configServer := configsock.New(cfg.Paths.ConfigSocket, configsock.Deps{
		Provider: core, Admin: core, MMDSRouteSecretAdmin: core, BuilderAdmissionAdmin: core,
		MaxMMDSRouteSecretValueBytes: cfg.MMDS.Routes.MaxSecretValueBytes,
		API:                          apiHandler, AdminPidfile: cfg.Paths.AdminPidfile,
		RouteSource: core, Plugins: plugins, PluginPidfile: cfg.Paths.PluginPidfile,
	}, logger)
	configReady := make(chan struct{})
	configDone := make(chan error, 1)
	go func() {
		err := configServer.ServeReady(ctx, configReady)
		if err != nil {
			logger.Error("config-socket", "err", err)
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

	var proxyNS *netns.NetNS
	if cfg.Proxy.Mode == publicconfig.ProxyInternal && cfg.Proxy.ProxyNetNS != "" {
		proxyNS, err = appnet.OpenProxyNetNS(cfg.Proxy.ProxyNetNS)
		if err != nil {
			return err
		}
		defer proxyNS.Close()
		logger.Info("node-ctl internal proxy forwarding netns", "proxy_netns", cfg.Proxy.ProxyNetNS)
	}
	dataHandler := buildDataPlane(cfg, core, plugins, mx, internalWorkerStats, logger, proxyNS)
	mux := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		host := request.Host
		if index := strings.IndexByte(host, ':'); index >= 0 {
			host = host[:index]
		}
		if host == "api."+cfg.API.Domain || strings.HasPrefix(host, "api.") {
			apiHandler.ServeHTTP(w, request)
			return
		}
		dataHandler.ServeHTTP(w, request)
	})

	if cfg.Proxy.MetricsListen != "" {
		go appnet.ServeMetrics(ctx, cfg.Proxy.MetricsListen, mx, logger)
	}
	if cfg.Proxy.Mode != publicconfig.ProxyExternal && cfg.Proxy.DataListen != "" {
		listener, err := net.Listen("tcp", cfg.Proxy.DataListen)
		if err != nil {
			return fmt.Errorf("data_listen %s: %w", cfg.Proxy.DataListen, err)
		}
		logger.Info("node-ctl data-plane listener", "data_listen", cfg.Proxy.DataListen)
		go func() {
			if err := appnet.Serve(ctx, listener, dataHandler, runtime.APITLS); err != nil {
				logger.Error("data-plane listener", "err", err)
			}
		}()
	}

	if cfg.MMDS.Enabled && cfg.Proxy.Mode == publicconfig.ProxyInternal {
		listener, err := appnet.ListenTCPInNetNS(proxyNS, cfg.MMDS.Listen)
		if err != nil {
			return fmt.Errorf("mmds listen %s: %w", cfg.MMDS.Listen, err)
		}
		logger.Info("mmds metadata service", "listen", cfg.MMDS.Listen, "proxy_netns", cfg.Proxy.ProxyNetNS)
		go func() {
			if err := mmds.New(core, cfg.ParkTimeoutDur(), logger).Serve(ctx, listener); err != nil {
				logger.Error("mmds service", "err", err)
			}
		}()
	}

	if runtime.APITLS == nil {
		logger.Info("serving plain HTTP (dev): point the SDK with E2B_API_URL/E2B_SANDBOX_URL")
	}
	listener, err := net.Listen("tcp", cfg.API.Listen)
	if err != nil {
		return fmt.Errorf("listen %s: %w", cfg.API.Listen, err)
	}
	logger.Info("node-ctl serving", "listen", cfg.API.Listen, "domain", cfg.API.Domain, "proxy_mode", cfg.Proxy.Mode)
	return appnet.Serve(ctx, listener, mux, runtime.APITLS)
}

type startedExtension struct {
	api conductorextension.APIWrapper
}

func startExtension(ctx context.Context, runtime *Runtime, storage *store.Store, core *orch.Orchestrator) (*startedExtension, error) {
	if runtime.Extension == nil {
		return nil, nil
	}
	host, observer := conductorext.New(storage)
	core.SetExtensionObserver(observer)
	if err := runtime.Extension.Start(ctx, host); err != nil {
		return nil, fmt.Errorf("conductor extension start: %w", err)
	}
	// Optional capabilities are discovered only after Start succeeds and then
	// frozen for the process lifetime. They are not dynamic registrations.
	sandboxHook, _ := runtime.Extension.(conductorextension.SandboxHook)
	buildHook, _ := runtime.Extension.(conductorextension.BuildHook)
	apiWrapper, _ := runtime.Extension.(conductorextension.APIWrapper)
	core.SetExtensionHooks(sandboxHook, buildHook)
	return &startedExtension{api: apiWrapper}, nil
}

func wrapExtensionAPI(wrapper conductorextension.APIWrapper, next http.Handler) (http.Handler, error) {
	wrapped := wrapper.WrapAPI(next)
	if wrapped == nil {
		return nil, fmt.Errorf("conductor extension API wrapper returned nil")
	}
	return wrapped, nil
}

func newAPIHandler(cfg *publicconfig.Conductor, core api.Core, logger *slog.Logger) http.Handler {
	diskMB := 0
	if info, err := os.Stat(cfg.Sandbox.Boot.OverlayDiffTemplate); err == nil {
		diskMB = int(info.Size() >> 20)
	}
	resources := api.Resources{
		VCPU:     configresolve.SandboxResources(cfg.Sandbox.Resources).Capacity.CPU,
		MemoryMB: cfg.Sandbox.Resources.MemoryMiB(), DiskMB: diskMB,
	}
	return api.New(core, cfg.API.Domain, resources, logger).Handler()
}
