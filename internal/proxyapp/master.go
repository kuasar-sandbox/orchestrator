package proxyapp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	proxyextension "github.com/kuasar-sandbox/orchestrator/app/proxy/extension"
	"github.com/kuasar-sandbox/orchestrator/internal/appnet"
	"github.com/kuasar-sandbox/orchestrator/internal/listenresource"
	"github.com/kuasar-sandbox/orchestrator/internal/metrics"
	"github.com/kuasar-sandbox/orchestrator/internal/netns"
	"github.com/kuasar-sandbox/orchestrator/internal/proxyadmission"
	"github.com/kuasar-sandbox/orchestrator/internal/proxyext"
	"github.com/kuasar-sandbox/orchestrator/internal/proxyshm"
	"github.com/kuasar-sandbox/orchestrator/internal/proxystats"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// RunMaster owns routesync, shared state, listeners, metrics, and worker
// supervision for one frozen EffectiveConfig.
func RunMaster(ctx context.Context, effective *EffectiveConfig, runtime *Runtime) (returnErr error) {
	if effective == nil || effective.config == nil || runtime == nil || runtime.Logger == nil {
		return fmt.Errorf("proxy master: unresolved startup state")
	}
	cfg := effective.config
	logger := runtime.Logger
	instance := runtime.Instance
	if instance == nil {
		instance = NewInstance(routesync.ProxyPluginID, "", "", cfg.Workers)
	}
	defer func() { instance.finish(returnErr) }()
	if ctx.Err() != nil || instance.stopped() {
		return nil
	}
	instance.mu.Lock()
	instance.status.StatsSocket = cfg.StatsSocket
	instance.mu.Unlock()
	listeners, err := newListenerSet(runtime.ListenerFiles)
	if err != nil {
		return fmt.Errorf("proxy listeners: %w", err)
	}
	createTable := proxyshm.Create
	if runtime.privatePaths {
		createTable = proxyshm.CreateExclusive
	}
	shmOwned := true
	var table *proxyshm.Table
	if runtime.inherited != nil && runtime.inherited.Shm != nil {
		if runtime.inherited.ShmPath != cfg.ShmPath {
			return fmt.Errorf("proxy continuation shared-memory path changed")
		}
		identity, err := identifyPath(cfg.ShmPath)
		if err != nil || identity != runtime.inherited.ShmIdentity {
			return fmt.Errorf("proxy continuation shared-memory ownership changed: %v", err)
		}
		table, err = proxyshm.CreateFromFile(cfg.ShmPath, runtime.inherited.Shm, cfg.RouteCapacity)
		shmOwned = false
	} else {
		table, err = createTable(cfg.ShmPath, cfg.RouteCapacity)
	}
	if err != nil {
		return fmt.Errorf("proxy: create shared route table: %w", err)
	}
	defer table.Close()
	defer func() {
		if shmOwned {
			_ = os.Remove(cfg.ShmPath)
		}
	}()
	admission, err := proxyadmission.NewMaster(cfg.RouteCapacity, cfg.Workers)
	if err != nil {
		return fmt.Errorf("proxy: create shared admission arena: %w", err)
	}
	defer admission.Close()
	// Parent cancellation requests a bounded drain. Business dependencies and
	// worker processes retain their own live context until that drain completes.
	masterCtx, cancelMaster := context.WithCancel(context.WithoutCancel(ctx))
	startupCtx, cancelStartup := context.WithCancel(masterCtx)
	defer cancelStartup()
	runDone := make(chan struct{})
	defer close(runDone)
	go func() {
		select {
		case <-ctx.Done():
			instance.requestStop(time.Now().Add(cfg.ShutdownTimeoutDur()))
		case <-runDone:
		}
	}()
	go func() {
		select {
		case <-instance.stop:
			cancelStartup()
			instance.mu.Lock()
			initialized := instance.initialized
			instance.mu.Unlock()
			if !initialized {
				cancelMaster()
			}
		case <-runDone:
		}
	}()
	var masterGroup sync.WaitGroup
	extensionStarted := false
	defer func() {
		cancelMaster()
		masterGroup.Wait()
		if extensionStarted {
			instance.mu.Lock()
			deadline, stopping := instance.deadline, instance.stopping
			instance.mu.Unlock()
			if !stopping {
				deadline = time.Now().Add(cfg.ShutdownTimeoutDur())
			}
			ending, cancel := drainContext(deadline)
			defer cancel()
			returnErr = errors.Join(returnErr, shutdownExtension(ending, runtime.MasterExtension))
		}
	}()
	startMasterTask := func(task func()) {
		masterGroup.Add(1)
		go func() {
			defer masterGroup.Done()
			task()
		}()
	}

	view := proxyshm.NewMasterViewWithAdmission(table, admission, cfg.Traffic.MaxInflight, cfg.ParkTimeoutDur(), logger)
	if err := table.SetPolicy(routesync.Policy{
		AuthMode: cfg.Auth, ParkTimeoutMS: int(cfg.ParkTimeoutDur() / time.Millisecond),
	}); err != nil {
		return fmt.Errorf("proxy: set bootstrap policy: %w", err)
	}
	workerIDs := make([]string, cfg.Workers)
	for index := range workerIDs {
		workerIDs[index] = fmt.Sprintf("proxy-%d", index)
	}
	metricSet := metrics.New()
	masterStats := proxystats.NewMasterStats(metricSet, workerIDs)
	masterStats.InplaceRestart = runtime.inplaceRestart

	var routeSink routesync.Sink = view
	var managementWrapper proxyextension.ManagementWrapper
	if extension := runtime.MasterExtension; extension != nil {
		host, observingSink := proxyext.New(view, table, masterStats)
		routeSink = observingSink
		extensionStarted = true
		if err := extension.Start(masterCtx, host); err != nil {
			if instance.stopped() && errors.Is(err, context.Canceled) {
				return nil
			}
			return fmt.Errorf("proxy master extension start: %w", err)
		}
		managementWrapper, _ = extension.(proxyextension.ManagementWrapper)
	}
	routeSink = &readinessSink{Sink: routeSink, instance: instance}

	proxyNamespace, err := appnet.OpenProxyNetNS(cfg.ProxyNetNS)
	if err != nil {
		return err
	}
	if proxyNamespace != nil {
		defer proxyNamespace.Close()
	}

	var dataListener net.Listener
	if runtime.inherited != nil {
		dataListener, err = listeners.inherit(cfg.DataListen, runtime.inherited.DataExpression, runtime.inherited.Data)
	} else {
		dataListener, err = listeners.open(cfg.DataListen, func(address string) (net.Listener, error) { return net.Listen("tcp", address) })
	}
	if err != nil {
		return fmt.Errorf("proxy: listen data_listen %s: %w", cfg.DataListen, err)
	}
	defer dataListener.Close()

	var statsListener net.Listener
	statsOwned := true
	if runtime.inherited != nil && runtime.inherited.Stats != nil {
		if cfg.StatsSocket != runtime.inherited.StatsPath {
			return fmt.Errorf("proxy continuation stats path changed")
		}
		statsListener, err = listenresource.Listener(runtime.inherited.Stats)
		statsOwned = false
	} else {
		statsListener, err = listenUnixMode(cfg.StatsSocket, runtime.privatePaths)
	}
	if err != nil {
		return fmt.Errorf("proxy: listen stats_socket %s: %w", cfg.StatsSocket, err)
	}
	defer statsListener.Close()
	if runtime.retain != nil || !statsOwned {
		statsListener.(*net.UnixListener).SetUnlinkOnClose(false)
	}
	defer func() {
		if statsOwned {
			_ = os.Remove(cfg.StatsSocket)
		}
	}()
	statsServer := proxystats.NewStatsServer(masterStats, table.Synced, func(sandboxID string) (proxystats.RouteIdentity, bool) {
		route, found := table.Lookup(sandboxID)
		if !found {
			return proxystats.RouteIdentity{}, false
		}
		return proxystats.RouteIdentity{
			RunID: route.RunID, Profile: types.Profile(route.Profile), State: types.State(route.State), MaxInflight: route.EffectiveMaxInflight,
			TrafficPolicyInvalid: route.TrafficPolicyInvalid,
		}, true
	}, logger)
	managementHandler := statsServer.Handler()
	if managementWrapper != nil {
		managementHandler = managementWrapper.WrapManagement(managementHandler)
		if managementHandler == nil {
			return fmt.Errorf("proxy master extension returned a nil management handler")
		}
	}
	managementHandler = instance.management(managementHandler)
	startMasterTask(func() {
		masterStats.RunGC(masterCtx, func(sandboxID string) bool {
			_, found := table.Lookup(sandboxID)
			return found
		}, time.Minute)
	})
	startMasterTask(func() {
		if err := statsServer.ServeHandler(masterCtx, statsListener, managementHandler); err != nil && masterCtx.Err() == nil {
			logger.Error("proxy stats socket", "err", err)
		}
	})

	registration := routesync.Register{
		Subscribe: &routesync.Subscribe{Kind: routesync.KindRouteWake},
		Proxy: &routesync.Proxy{
			StatsSocket: &routesync.Socket{Path: cfg.StatsSocket},
		},
		Mmds: true,
	}
	dial := func(ctx context.Context) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", cfg.ConfigSocket)
	}
	if runtime.candidateDial != nil {
		dial = runtime.candidateDial
	}
	startMasterTask(func() {
		routesync.NewSubscriber(dial, instance.status.InstanceID, registration, routeSink, view, logger).Run(masterCtx)
	})

	var mmdsListener net.Listener
	mmdsPolicy, ready := view.WaitPolicy(startupCtx)
	if !ready {
		return nil
	}
	mmdsListen := ""
	if mmdsPolicy != nil && mmdsPolicy.Enabled {
		mmdsListen = mmdsPolicy.Listen
		if runtime.inherited != nil {
			mmdsListener, err = listeners.inherit(mmdsListen, runtime.inherited.MMDSExpression, runtime.inherited.MMDS)
		} else {
			mmdsListener, err = listeners.open(mmdsListen, func(address string) (net.Listener, error) { return appnet.ListenTCPInNetNS(proxyNamespace, address) })
		}
		if err != nil {
			return fmt.Errorf("proxy: listen conductor MMDS address %s: %w", mmdsListen, err)
		}
		defer mmdsListener.Close()
	} else if runtime.inherited != nil && runtime.inherited.MMDS != nil {
		return fmt.Errorf("proxy continuation MMDS was disabled by authority")
	}
	if err := listeners.complete(); err != nil {
		return err
	}
	instance.mu.Lock()
	if mmdsListener == nil {
		instance.status.MMDS = "disabled"
	}
	instance.initialized = true
	instance.recalculateLocked()
	instance.notifyLocked()
	instance.mu.Unlock()

	var metricsListener net.Listener
	if cfg.MetricsListen != "" {
		if runtime.inherited != nil {
			if runtime.inherited.Metrics == nil || runtime.inherited.MetricsExpression != cfg.MetricsListen {
				return fmt.Errorf("proxy continuation metrics resource changed")
			}
			metricsListener, err = listenresource.Listener(runtime.inherited.Metrics)
		} else {
			metricsListener, err = net.Listen("tcp", cfg.MetricsListen)
		}
		if err != nil {
			if runtime.inherited != nil {
				return fmt.Errorf("proxy inherited metrics listener: %w", err)
			}
			logger.Warn("proxy metrics listener", "err", err)
		} else {
			defer metricsListener.Close()
			instance.mu.Lock()
			instance.status.MetricsAddress = metricsListener.Addr().String()
			instance.mu.Unlock()
		}
	}
	if runtime.retain != nil {
		retained, err := retainResources(dataListener, mmdsListener, statsListener, metricsListener, cfg.DataListen, mmdsListen, cfg.StatsSocket, cfg.MetricsListen, cfg.ShmPath)
		if err != nil {
			return err
		}
		if err := runtime.retain(retained); err != nil {
			retained.close()
			return err
		}
		statsOwned = false
		shmOwned = false
	}
	if metricsListener != nil {
		mux := http.NewServeMux()
		mux.HandleFunc("/metrics", metricSet.Handler())
		server := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
		startMasterTask(func() { <-masterCtx.Done(); _ = server.Close() })
		startMasterTask(func() {
			if err := server.Serve(metricsListener); err != nil && !errors.Is(err, http.ErrServerClosed) {
				logger.Warn("proxy metrics server", "err", err)
			}
		})
	}

	for index := 0; index < cfg.Workers; index++ {
		workerIndex := index
		startMasterTask(func() {
			superviseWorker(masterCtx, instance, workerIndex, effective, proxyNamespace, dataListener, mmdsListener, view, admission, masterStats, logger)
		})
	}

	logger.Info("proxy master serving",
		"workers", cfg.Workers,
		"data_listen", cfg.DataListen,
		"stats_socket", cfg.StatsSocket,
		"mmds_listen", mmdsListen,
		"proxy_netns", cfg.ProxyNetNS,
		"config_socket", cfg.ConfigSocket,
		"shm_path", cfg.ShmPath,
		"route_capacity", cfg.RouteCapacity,
		"admission_mmap_bytes", func() int {
			report, _ := proxyadmission.Report(cfg.RouteCapacity, cfg.Workers)
			return report.MappedBytes
		}(),
	)
	<-instance.stop
	return instance.drainWorkers()
}

func superviseWorker(ctx context.Context, instance *Instance, index int, effective *EffectiveConfig, proxyNamespace *netns.NetNS, dataListener, mmdsListener net.Listener, view *proxyshm.MasterView, admission *proxyadmission.Master, stats *proxystats.MasterStats, logger *slog.Logger) {
	workerID := fmt.Sprintf("proxy-%d", index)
	var epoch uint64
	for ctx.Err() == nil && !instance.stopped() {
		epoch++
		err := runWorkerProcess(ctx, instance, index, workerID, epoch, effective, proxyNamespace, dataListener, mmdsListener, view, admission, stats, logger)
		if ctx.Err() != nil || instance.stopped() {
			return
		}
		logger.Warn("proxy worker exited; restarting", "worker", workerID, "err", err)
		select {
		case <-instance.stop:
			return
		case <-ctx.Done():
			return
		case <-time.After(500 * time.Millisecond):
		}
	}
}
