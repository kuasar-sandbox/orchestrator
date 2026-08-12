package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/metrics"
	"github.com/kuasar-sandbox/orchestrator/internal/mmds"
	"github.com/kuasar-sandbox/orchestrator/internal/mmdsrpc"
	"github.com/kuasar-sandbox/orchestrator/internal/netns"
	"github.com/kuasar-sandbox/orchestrator/internal/proxy"
	"github.com/kuasar-sandbox/orchestrator/internal/proxyshm"
	"github.com/kuasar-sandbox/orchestrator/internal/proxystats"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

const (
	envProxyDataFD      = "KUASAR_PROXY_DATA_FD"
	envProxyForwardFD   = "KUASAR_PROXY_FORWARD_FD"
	envProxyMMDSFD      = "KUASAR_PROXY_MMDS_FD"
	envProxyWakeFD      = "KUASAR_PROXY_WAKE_FD"
	envProxyNotifyFD    = "KUASAR_PROXY_NOTIFY_FD"
	envProxyStatsFD     = "KUASAR_PROXY_STATS_FD"
	envProxyMMDSRPCFD   = "KUASAR_PROXY_MMDSRPC_FD"
	envProxyWorkerID    = "KUASAR_PROXY_WORKER_ID"
	envProxyWorkerEpoch = "KUASAR_PROXY_WORKER_EPOCH"
)

// runProxy is the external data-plane proxy master. It is the only process that
// registers on conductor's config-socket plugin plane. The master keeps routesync
// connected, writes the shared route table, owns listener sockets, and supervises
// worker processes. Workers inherit listener fds and read the shared table locally;
// they never register as plugins.
func runProxy(args []string, log *slog.Logger) error {
	fs := flag.NewFlagSet("proxy serve", flag.ExitOnError)
	cfgPath := fs.String("config", "/etc/node-ctl/proxy.yaml", "proxy config file")
	worker := fs.Bool("worker", false, "internal: run a proxy worker process")
	_ = fs.Parse(args)

	cfg, err := config.LoadProxy(*cfgPath)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if *worker {
		return runProxyWorker(ctx, cfg, log)
	}
	return runProxyMaster(ctx, *cfgPath, cfg, log)
}

func runProxyMaster(ctx context.Context, cfgPath string, cfg *config.ProxyFileConfig, log *slog.Logger) error {
	table, err := proxyshm.Create(cfg.ShmPath, cfg.RouteCapacity)
	if err != nil {
		return fmt.Errorf("proxy: create shared route table: %w", err)
	}
	defer table.Close()
	defer os.Remove(cfg.ShmPath)

	view := proxyshm.NewMasterView(table, cfg.ParkTimeoutDur(), log)
	if err := table.SetPolicy(routesync.Policy{AuthMode: cfg.Auth, ParkTimeoutMS: int(cfg.ParkTimeoutDur() / time.Millisecond)}); err != nil {
		return fmt.Errorf("proxy: set bootstrap policy: %w", err)
	}

	proxyNS, err := openProxyNetNS(cfg.ProxyNetNS)
	if err != nil {
		return err
	}
	if proxyNS != nil {
		defer proxyNS.Close()
	}

	forwardLn, err := listenUnix(cfg.ProxySocket)
	if err != nil {
		return fmt.Errorf("proxy: listen proxy_socket %s: %w", cfg.ProxySocket, err)
	}
	defer forwardLn.Close()

	var dataLn net.Listener
	if cfg.DataListen != "" {
		dataLn, err = net.Listen("tcp", cfg.DataListen)
		if err != nil {
			return fmt.Errorf("proxy: listen data_listen %s: %w", cfg.DataListen, err)
		}
		defer dataLn.Close()
	}

	statsLn, err := listenUnix(cfg.StatsSocket)
	if err != nil {
		return fmt.Errorf("proxy: listen stats_socket %s: %w", cfg.StatsSocket, err)
	}
	defer statsLn.Close()
	defer os.Remove(cfg.StatsSocket)
	workerIDs := make([]string, cfg.Workers)
	for i := range workerIDs {
		workerIDs[i] = fmt.Sprintf("proxy-%d", i)
	}
	mx := metrics.New()
	masterStats := proxystats.NewMasterStats(mx, workerIDs)
	statsServer := proxystats.NewStatsServer(masterStats, table.Synced, func(sandboxID string) (proxystats.RouteIdentity, bool) {
		route, found := table.Lookup(sandboxID)
		if !found {
			return proxystats.RouteIdentity{}, false
		}
		return proxystats.RouteIdentity{RunID: route.RunID, Profile: types.Profile(route.Profile), State: types.State(route.State)}, true
	}, log)
	go func() {
		if err := statsServer.Serve(ctx, statsLn); err != nil && ctx.Err() == nil {
			log.Error("proxy stats socket", "err", err)
		}
	}()

	// The external proxy always registers its trusted MMDS and stats
	// capabilities. The conductor Hello is the sole source for MMDS policy.
	reg := routesync.Register{
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
	go routesync.NewSubscriber(dial, routesync.ProxyPluginID, reg, view, view, log).Run(ctx)

	var mmdsLn net.Listener
	mmdsPolicy, ok := view.WaitPolicy(ctx)
	if !ok {
		return nil
	}
	mmdsListen := ""
	if mmdsPolicy != nil && mmdsPolicy.Enabled {
		mmdsListen = mmdsPolicy.Listen
		mmdsLn, err = listenTCPInNetNS(proxyNS, mmdsListen)
		if err != nil {
			return fmt.Errorf("proxy: listen conductor MMDS address %s: %w", mmdsListen, err)
		}
		defer mmdsLn.Close()
	}

	if cfg.MetricsListen != "" {
		go serveMetrics(ctx, cfg.MetricsListen, mx, log)
	}

	for i := 0; i < cfg.Workers; i++ {
		go superviseProxyWorker(ctx, i, cfgPath, cfg, proxyNS, dataLn, forwardLn, mmdsLn, view, masterStats, log)
	}

	log.Info("node-ctl proxy master serving",
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
	<-ctx.Done()
	return nil
}

func runProxyWorker(ctx context.Context, cfg *config.ProxyFileConfig, log *slog.Logger) error {
	workerID := os.Getenv(envProxyWorkerID)
	if workerID == "" {
		workerID = "worker"
	}
	epoch, err := strconv.ParseUint(os.Getenv(envProxyWorkerEpoch), 10, 64)
	if err != nil || epoch == 0 {
		return fmt.Errorf("proxy worker: invalid epoch")
	}
	table, err := proxyshm.Open(cfg.ShmPath)
	if err != nil {
		return fmt.Errorf("proxy worker: open shared route table: %w", err)
	}
	defer table.Close()

	updates := proxyshm.NewUpdatesFromFD(fdEnv(envProxyNotifyFD))
	wakes := proxyshm.NewWakeWriterFromFD(fdEnv(envProxyWakeFD))
	if wakes != nil {
		defer wakes.Close()
	}
	var wakeFn func(string)
	if wakes != nil {
		wakeFn = wakes.Wake
	}
	var mmdsClient *mmdsrpc.Client
	if fd := fdEnv(envProxyMMDSRPCFD); fd >= 0 {
		mmdsClient = mmdsrpc.NewClient(os.NewFile(uintptr(fd), "proxy-mmdsrpc"))
		defer mmdsClient.Close()
	}
	view := proxyshm.NewMMDSWorkerView(table, updates, wakeFn, cfg.ParkTimeoutDur(), mmdsClient)
	authMode := func() string {
		if m := view.Policy().AuthMode; m != "" {
			return m
		}
		return cfg.Auth
	}
	statsConn, err := connFromFD(fdEnv(envProxyStatsFD), "proxy-stats")
	if err != nil {
		return err
	}
	if statsConn == nil {
		return fmt.Errorf("proxy worker: missing stats stream fd")
	}
	defer statsConn.Close()
	if err := waitProxyTableSync(ctx, table); err != nil {
		return err
	}

	// Reconstruct every inherited listener before advertising ready. No Serve
	// goroutine starts until the synchronous hello+ready handshake succeeds, but
	// a malformed inherited FD must also fail before the master can consider this
	// replacement available.
	forwardLn, err := listenerFromFD(fdEnv(envProxyForwardFD), "proxy-forward")
	if err != nil {
		return err
	}
	if forwardLn == nil {
		return fmt.Errorf("proxy worker: missing forward listener fd")
	}
	defer forwardLn.Close()
	dataLn, err := listenerFromFD(fdEnv(envProxyDataFD), "proxy-data")
	if err != nil {
		return err
	}
	if dataLn != nil {
		defer dataLn.Close()
	}
	mmdsLn, err := listenerFromFD(fdEnv(envProxyMMDSFD), "proxy-mmds")
	if err != nil {
		return err
	}
	if mmdsLn != nil {
		defer mmdsLn.Close()
	}

	workerCtx, cancelWorker := context.WithCancel(ctx)
	defer cancelWorker()
	workerStats := proxystats.NewWorkerStats()
	senderDone, err := workerStats.StartSender(workerCtx, workerID, epoch, proxystats.StreamSender(statsConn))
	if err != nil {
		return fmt.Errorf("proxy worker: start stats stream: %w", err)
	}
	go workerStats.RunGC(workerCtx, func(sandboxID string) bool {
		_, found := table.Lookup(sandboxID)
		return found
	}, 5*time.Minute)
	px := proxy.NewWithDialer(view, authMode, log.With("proxy_worker", workerID), workerStats, nil, cfg.Paths.RunRoot).
		WithTrafficTracker(workerStats)

	errCh := make(chan error, 4)
	go func() { errCh <- <-senderDone }()
	go func() { errCh <- serveListener(workerCtx, forwardLn, px, "", "", log) }()

	if dataLn != nil {
		go func() { errCh <- serveListener(workerCtx, dataLn, px, cfg.TLS.Cert, cfg.TLS.Key, log) }()
	}

	if mmdsLn != nil {
		go func() { errCh <- mmds.New(view, cfg.ParkTimeoutDur(), log).Serve(workerCtx, mmdsLn) }()
	}

	log.Info("node-ctl proxy worker serving", "worker", workerID)
	select {
	case <-ctx.Done():
		return nil
	case err := <-errCh:
		if ctx.Err() != nil {
			return nil
		}
		return err
	}
}

func waitProxyTableSync(ctx context.Context, table *proxyshm.Table) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for !table.Synced() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
	return nil
}

func superviseProxyWorker(ctx context.Context, idx int, cfgPath string, cfg *config.ProxyFileConfig, proxyNS *netns.NetNS, dataLn, forwardLn, mmdsLn net.Listener, view *proxyshm.MasterView, stats *proxystats.MasterStats, log *slog.Logger) {
	workerID := fmt.Sprintf("proxy-%d", idx)
	var epoch uint64
	for ctx.Err() == nil {
		epoch++
		err := runProxyWorkerProcess(ctx, workerID, epoch, cfgPath, proxyNS, dataLn, forwardLn, mmdsLn, view, stats, log)
		if ctx.Err() != nil {
			return
		}
		log.Warn("proxy worker exited; restarting", "worker", workerID, "err", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func runProxyWorkerProcess(ctx context.Context, workerID string, epoch uint64, cfgPath string, proxyNS *netns.NetNS, dataLn, forwardLn, mmdsLn net.Listener, view *proxyshm.MasterView, stats *proxystats.MasterStats, log *slog.Logger) error {
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	var files []*os.File
	env := os.Environ()
	nextFD := 3
	addFile := func(name string, f *os.File) {
		if f == nil {
			env = append(env, name+"=-1")
			return
		}
		files = append(files, f)
		env = append(env, name+"="+strconv.Itoa(nextFD))
		nextFD++
	}

	dataFile, err := listenerFile(dataLn)
	if err != nil {
		closeFiles(files)
		return err
	}
	forwardFile, err := listenerFile(forwardLn)
	if err != nil {
		closeFiles(append(files, dataFile))
		return err
	}
	mmdsFile, err := listenerFile(mmdsLn)
	if err != nil {
		closeFiles(append(files, dataFile, forwardFile))
		return err
	}

	wakeR, wakeW, err := os.Pipe()
	if err != nil {
		closeFiles(append(files, dataFile, forwardFile, mmdsFile))
		return err
	}
	notifyR, notifyW, err := os.Pipe()
	if err != nil {
		closeFiles(append(files, dataFile, forwardFile, mmdsFile, wakeR, wakeW))
		return err
	}
	statsMasterFile, statsWorkerFile, err := newSocketpair()
	if err != nil {
		closeFiles(append(files, dataFile, forwardFile, mmdsFile, wakeR, wakeW, notifyR, notifyW))
		return err
	}
	mmdsRPCMaster, mmdsRPCWorker, err := newSocketpair()
	if err != nil {
		closeFiles(append(files, dataFile, forwardFile, mmdsFile, wakeR, wakeW, notifyR, notifyW, statsMasterFile, statsWorkerFile))
		return err
	}
	removeNotify := view.RegisterNotifyWriter(notifyW)
	go proxyshm.ReadWakeLoop(ctx, wakeR, view.Wake)
	go mmdsrpc.NewServer(mmdsRPCMaster, view.ResolveMMDS, log).Serve()

	addFile(envProxyDataFD, dataFile)
	addFile(envProxyForwardFD, forwardFile)
	addFile(envProxyMMDSFD, mmdsFile)
	addFile(envProxyWakeFD, wakeW)
	addFile(envProxyNotifyFD, notifyR)
	addFile(envProxyStatsFD, statsWorkerFile)
	addFile(envProxyMMDSRPCFD, mmdsRPCWorker)
	env = append(env, envProxyWorkerID+"="+workerID, envProxyWorkerEpoch+"="+strconv.FormatUint(epoch, 10))

	if err := stats.BeginWorker(workerID, epoch); err != nil {
		removeNotify()
		_ = mmdsRPCMaster.Close()
		closeFiles(append(files, statsMasterFile))
		return err
	}
	workerCtx, cancelWorker := context.WithCancel(ctx)
	defer cancelWorker()
	cmd := exec.CommandContext(workerCtx, exe, "proxy", "serve", "--config", cfgPath, "--worker")
	cmd.Env = env
	cmd.ExtraFiles = files
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := startCommandInNetNS(proxyNS, cmd); err != nil {
		removeNotify()
		_ = mmdsRPCMaster.Close()
		_ = statsMasterFile.Close()
		closeFiles(files)
		stats.WorkerExited(workerID, epoch)
		return err
	}
	closeFiles(files)
	defer removeNotify()
	defer mmdsRPCMaster.Close()
	defer statsMasterFile.Close()

	streamErr := make(chan error, 1)
	go func() { streamErr <- stats.ReadWorkerStream(workerCtx, statsMasterFile, workerID, epoch) }()
	waitErr := make(chan error, 1)
	go func() { waitErr <- cmd.Wait() }()

	var result error
	select {
	case err := <-streamErr:
		if ctx.Err() == nil {
			stats.StreamFault(workerID, epoch)
			if err == nil {
				err = fmt.Errorf("stats stream ended before worker exit")
			}
			result = fmt.Errorf("proxy worker %s stats stream: %w", workerID, err)
		}
		cancelWorker()
		<-waitErr // contribution remains unavailable until process exit is confirmed.
	case err := <-waitErr:
		result = err
		// cmd.Wait has now proved that every backend FD owned by this worker is
		// closed. Stop serving its cache contribution immediately while the
		// stream reader is being unblocked and drained.
		stats.StreamFault(workerID, epoch)
		_ = statsMasterFile.Close()
		<-streamErr
	}
	stats.WorkerExited(workerID, epoch)
	if ctx.Err() != nil {
		return nil
	}
	return result
}

func newSocketpair() (master, worker *os.File, err error) {
	// Only the explicitly passed worker endpoint may survive exec. Setting
	// CLOEXEC atomically prevents a concurrently spawned worker from inheriting
	// the master's endpoint before os.File.Close can run.
	fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM|syscall.SOCK_CLOEXEC, 0)
	if err != nil {
		return nil, nil, err
	}
	return os.NewFile(uintptr(fds[0]), "mmdsrpc-master"), os.NewFile(uintptr(fds[1]), "mmdsrpc-worker"), nil
}

func listenUnix(path string) (net.Listener, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	_ = os.Remove(path)
	ln, err := net.Listen("unix", path)
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(path, 0o600); err != nil {
		_ = ln.Close()
		return nil, err
	}
	return ln, nil
}

func listenerFile(ln net.Listener) (*os.File, error) {
	if ln == nil {
		return nil, nil
	}
	switch l := ln.(type) {
	case *net.TCPListener:
		return l.File()
	case *net.UnixListener:
		return l.File()
	default:
		return nil, fmt.Errorf("proxy: unsupported listener type %T", ln)
	}
}

func listenerFromFD(fd int, name string) (net.Listener, error) {
	if fd < 0 {
		return nil, nil
	}
	f := os.NewFile(uintptr(fd), name)
	if f == nil {
		return nil, fmt.Errorf("proxy worker: bad fd %d for %s", fd, name)
	}
	defer f.Close()
	return net.FileListener(f)
}

func connFromFD(fd int, name string) (net.Conn, error) {
	if fd < 0 {
		return nil, nil
	}
	f := os.NewFile(uintptr(fd), name)
	if f == nil {
		return nil, fmt.Errorf("proxy worker: bad fd %d for %s", fd, name)
	}
	defer f.Close()
	return net.FileConn(f)
}

func fdEnv(name string) int {
	v := os.Getenv(name)
	if v == "" {
		return -1
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return -1
	}
	return n
}

func closeFiles(files []*os.File) {
	for _, f := range files {
		if f != nil {
			_ = f.Close()
		}
	}
}
