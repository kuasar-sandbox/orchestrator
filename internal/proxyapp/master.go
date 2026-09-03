package proxyapp

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"sync"
	"time"

	proxyextension "github.com/kuasar-sandbox/orchestrator/app/proxy/extension"
	"github.com/kuasar-sandbox/orchestrator/internal/appnet"
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
func RunMaster(ctx context.Context, effective *EffectiveConfig, runtime *Runtime) error {
	if effective == nil || effective.config == nil || runtime == nil || runtime.Logger == nil {
		return fmt.Errorf("proxy master: unresolved startup state")
	}
	cfg := effective.config
	logger := runtime.Logger
	table, err := proxyshm.Create(cfg.ShmPath, cfg.RouteCapacity)
	if err != nil {
		return fmt.Errorf("proxy: create shared route table: %w", err)
	}
	defer table.Close()
	defer os.Remove(cfg.ShmPath)
	admission, err := proxyadmission.NewMaster(cfg.RouteCapacity, cfg.Workers)
	if err != nil {
		return fmt.Errorf("proxy: create shared admission arena: %w", err)
	}
	defer admission.Close()
	masterCtx, cancelMaster := context.WithCancel(ctx)
	var masterGroup sync.WaitGroup
	defer func() {
		cancelMaster()
		masterGroup.Wait()
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

	var routeSink routesync.Sink = view
	var managementWrapper proxyextension.ManagementWrapper
	if extension := runtime.MasterExtension; extension != nil {
		host, observingSink := proxyext.New(view, table, masterStats)
		routeSink = observingSink
		if err := extension.Start(masterCtx, host); err != nil {
			return fmt.Errorf("proxy master extension start: %w", err)
		}
		managementWrapper, _ = extension.(proxyextension.ManagementWrapper)
	}

	proxyNamespace, err := appnet.OpenProxyNetNS(cfg.ProxyNetNS)
	if err != nil {
		return err
	}
	if proxyNamespace != nil {
		defer proxyNamespace.Close()
	}

	dataListener, err := net.Listen("tcp", cfg.DataListen)
	if err != nil {
		return fmt.Errorf("proxy: listen data_listen %s: %w", cfg.DataListen, err)
	}
	defer dataListener.Close()

	statsListener, err := listenUnix(cfg.StatsSocket)
	if err != nil {
		return fmt.Errorf("proxy: listen stats_socket %s: %w", cfg.StatsSocket, err)
	}
	defer statsListener.Close()
	defer os.Remove(cfg.StatsSocket)
	statsServer := proxystats.NewStatsServer(masterStats, table.Synced, func(sandboxID string) (proxystats.RouteIdentity, bool) {
		route, found := table.Lookup(sandboxID)
		if !found {
			return proxystats.RouteIdentity{}, false
		}
		return proxystats.RouteIdentity{
			RunID: route.RunID, Profile: types.Profile(route.Profile), State: types.State(route.State), MaxInflight: route.EffectiveMaxInflight,
		}, true
	}, logger)
	managementHandler := statsServer.Handler()
	if managementWrapper != nil {
		managementHandler = managementWrapper.WrapManagement(managementHandler)
		if managementHandler == nil {
			return fmt.Errorf("proxy master extension returned a nil management handler")
		}
	}
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
	startMasterTask(func() {
		routesync.NewSubscriber(dial, routesync.ProxyPluginID, registration, routeSink, view, logger).Run(masterCtx)
	})

	var mmdsListener net.Listener
	mmdsPolicy, ready := view.WaitPolicy(masterCtx)
	if !ready {
		return nil
	}
	mmdsListen := ""
	if mmdsPolicy != nil && mmdsPolicy.Enabled {
		mmdsListen = mmdsPolicy.Listen
		mmdsListener, err = appnet.ListenTCPInNetNS(proxyNamespace, mmdsListen)
		if err != nil {
			return fmt.Errorf("proxy: listen conductor MMDS address %s: %w", mmdsListen, err)
		}
		defer mmdsListener.Close()
	}

	if cfg.MetricsListen != "" {
		startMasterTask(func() { appnet.ServeMetrics(masterCtx, cfg.MetricsListen, metricSet, logger) })
	}

	for index := 0; index < cfg.Workers; index++ {
		workerIndex := index
		startMasterTask(func() {
			superviseWorker(masterCtx, workerIndex, effective, proxyNamespace, dataListener, mmdsListener, view, admission, masterStats, logger)
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
	<-masterCtx.Done()
	return nil
}

func superviseWorker(ctx context.Context, index int, effective *EffectiveConfig, proxyNamespace *netns.NetNS, dataListener, mmdsListener net.Listener, view *proxyshm.MasterView, admission *proxyadmission.Master, stats *proxystats.MasterStats, logger *slog.Logger) {
	workerID := fmt.Sprintf("proxy-%d", index)
	var epoch uint64
	for ctx.Err() == nil {
		epoch++
		err := runWorkerProcess(ctx, index, workerID, epoch, effective, proxyNamespace, dataListener, mmdsListener, view, admission, stats, logger)
		if ctx.Err() != nil {
			return
		}
		logger.Warn("proxy worker exited; restarting", "worker", workerID, "err", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(500 * time.Millisecond):
		}
	}
}
