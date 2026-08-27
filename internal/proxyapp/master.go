package proxyapp

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"sync"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/appnet"
	"github.com/kuasar-sandbox/orchestrator/internal/metrics"
	"github.com/kuasar-sandbox/orchestrator/internal/netns"
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

	view := proxyshm.NewMasterView(table, cfg.ParkTimeoutDur(), logger)
	if err := table.SetPolicy(routesync.Policy{
		AuthMode: cfg.Auth, ParkTimeoutMS: int(cfg.ParkTimeoutDur() / time.Millisecond),
	}); err != nil {
		return fmt.Errorf("proxy: set bootstrap policy: %w", err)
	}
	proxyNamespace, err := appnet.OpenProxyNetNS(cfg.ProxyNetNS)
	if err != nil {
		return err
	}
	if proxyNamespace != nil {
		defer proxyNamespace.Close()
	}

	forwardListener, err := listenUnix(cfg.ProxySocket)
	if err != nil {
		return fmt.Errorf("proxy: listen proxy_socket %s: %w", cfg.ProxySocket, err)
	}
	defer forwardListener.Close()

	var dataListener net.Listener
	if cfg.DataListen != "" {
		dataListener, err = net.Listen("tcp", cfg.DataListen)
		if err != nil {
			return fmt.Errorf("proxy: listen data_listen %s: %w", cfg.DataListen, err)
		}
		defer dataListener.Close()
	}

	statsListener, err := listenUnix(cfg.StatsSocket)
	if err != nil {
		return fmt.Errorf("proxy: listen stats_socket %s: %w", cfg.StatsSocket, err)
	}
	defer statsListener.Close()
	defer os.Remove(cfg.StatsSocket)
	workerIDs := make([]string, cfg.Workers)
	for index := range workerIDs {
		workerIDs[index] = fmt.Sprintf("proxy-%d", index)
	}
	metricSet := metrics.New()
	masterStats := proxystats.NewMasterStats(metricSet, workerIDs)
	startMasterTask(func() {
		masterStats.RunGC(masterCtx, func(sandboxID string) bool {
			_, found := table.Lookup(sandboxID)
			return found
		}, time.Minute)
	})
	statsServer := proxystats.NewStatsServer(masterStats, table.Synced, func(sandboxID string) (proxystats.RouteIdentity, bool) {
		route, found := table.Lookup(sandboxID)
		if !found {
			return proxystats.RouteIdentity{}, false
		}
		return proxystats.RouteIdentity{
			RunID: route.RunID, Profile: types.Profile(route.Profile), State: types.State(route.State),
		}, true
	}, logger)
	startMasterTask(func() {
		if err := statsServer.Serve(masterCtx, statsListener); err != nil && masterCtx.Err() == nil {
			logger.Error("proxy stats socket", "err", err)
		}
	})

	registration := routesync.Register{
		Subscribe: &routesync.Subscribe{Kind: routesync.KindRouteWake},
		Proxy: &routesync.Proxy{
			Socket:      routesync.Socket{Path: cfg.ProxySocket},
			StatsSocket: &routesync.Socket{Path: cfg.StatsSocket},
		},
		Mmds: true,
	}
	dial := func(ctx context.Context) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", cfg.ConfigSocket)
	}
	startMasterTask(func() {
		routesync.NewSubscriber(dial, routesync.ProxyPluginID, registration, view, view, logger).Run(masterCtx)
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
			superviseWorker(masterCtx, workerIndex, effective, proxyNamespace, dataListener, forwardListener, mmdsListener, view, masterStats, logger)
		})
	}

	logger.Info("proxy master serving",
		"workers", cfg.Workers,
		"data_listen", cfg.DataListen,
		"proxy_socket", cfg.ProxySocket,
		"stats_socket", cfg.StatsSocket,
		"mmds_listen", mmdsListen,
		"proxy_netns", cfg.ProxyNetNS,
		"config_socket", cfg.ConfigSocket,
		"shm_path", cfg.ShmPath,
		"route_capacity", cfg.RouteCapacity,
	)
	<-masterCtx.Done()
	return nil
}

func superviseWorker(ctx context.Context, index int, effective *EffectiveConfig, proxyNamespace *netns.NetNS, dataListener, forwardListener, mmdsListener net.Listener, view *proxyshm.MasterView, stats *proxystats.MasterStats, logger *slog.Logger) {
	workerID := fmt.Sprintf("proxy-%d", index)
	var epoch uint64
	for ctx.Err() == nil {
		epoch++
		err := runWorkerProcess(ctx, workerID, epoch, effective, proxyNamespace, dataListener, forwardListener, mmdsListener, view, stats, logger)
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
