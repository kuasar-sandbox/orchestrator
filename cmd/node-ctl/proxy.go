package main

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sys/unix"

	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/metrics"
	"github.com/kuasar-sandbox/orchestrator/internal/mmds"
	"github.com/kuasar-sandbox/orchestrator/internal/mmdsrelay"
	"github.com/kuasar-sandbox/orchestrator/internal/mmdsrpc"
	"github.com/kuasar-sandbox/orchestrator/internal/netns"
	"github.com/kuasar-sandbox/orchestrator/internal/proxy"
	"github.com/kuasar-sandbox/orchestrator/internal/proxyendpoints"
	"github.com/kuasar-sandbox/orchestrator/internal/proxyshm"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

const (
	proxyPluginID = "proxy"

	envProxyDataFD       = "KUASAR_PROXY_DATA_FD"
	envProxyForwardFD    = "KUASAR_PROXY_FORWARD_FD"
	envProxyMMDSFD       = "KUASAR_PROXY_MMDS_FD"
	envProxyWakeFD       = "KUASAR_PROXY_WAKE_FD"
	envProxyNotifyFD     = "KUASAR_PROXY_NOTIFY_FD"
	envProxyMetricsFD    = "KUASAR_PROXY_METRICS_FD"
	envProxyMmdsRpcFD    = "KUASAR_PROXY_MMDS_RPC_FD" // worker<->master MMDS endpoint RPC
	envProxyMmdsMaxFrame = "KUASAR_PROXY_MMDS_MAX_FRAME_BYTES"
	envProxyWorkerID     = "KUASAR_PROXY_WORKER_ID"
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
	view.SetPolicy(routesync.Policy{AuthMode: cfg.Auth, ParkTimeoutMS: int(cfg.ParkTimeoutDur() / time.Millisecond)})

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

	var mmdsLn net.Listener
	if cfg.MMDS.Enabled {
		mmdsLn, err = listenTCPInNetNS(proxyNS, cfg.MMDS.Listen)
		if err != nil {
			return fmt.Errorf("proxy: listen mmds.listen %s: %w", cfg.MMDS.Listen, err)
		}
		defer mmdsLn.Close()
	}

	mx := metrics.New()
	if cfg.MetricsListen != "" {
		go serveMetrics(ctx, cfg.MetricsListen, mx, log)
	}

	// MMDS endpoint table: bounded process memory,
	// staged-then-atomically-swapped per full-sync generation, shared by
	// every worker over its own per-worker RPC socketpair. Endpoint policy
	// (relay timeouts/size bounds/rate limits) comes from this file's own
	// mmds.endpoints block — a bootstrap-fallback local value, the same
	// kind Auth/ParkTimeout above are, just without their push-and-override
	// wiring yet. Unlike the conductor's mmds.endpoints, this block owns no
	// declaration/persistence policy at all (name/path rules, endpoint
	// counts, size caps) — this process trusts and mirrors whatever the
	// conductor already validated and decided to sync; see
	// config.ProxyMMDSEndpointsConfig's doc comment. Both
	// cfg.MMDS.Endpoints.Enabled AND cfg.MMDS.Enabled must be set for
	// endpoints to activate — mmds.enabled alone is the base MMDS (envd
	// token/metadata) listener and must not be read as implying the
	// endpoints feature too.
	epLimits := cfg.MMDS.Endpoints
	mmdsEndpointsEnabled := cfg.MMDS.Enabled && cfg.MMDS.Endpoints.Enabled
	var endpoints *proxyendpoints.Table
	if mmdsEndpointsEnabled {
		relayClient := mmdsrelay.New(mmdsrelay.Config{
			RequestTimeout:       epLimits.RelayRequestTimeoutDur(),
			MaxResponseBytes:     int64(epLimits.MaxRelayResponseBytes),
			MaxDecompressedBytes: int64(epLimits.MaxRelayDecompressedBytes),
			MaxDNSAnswers:        epLimits.MaxRelayDNSAnswers,
			MaxInflightPerKey:    epLimits.MaxRelayInflightPerSandbox,
			MaxRequestsPerSecond: float64(epLimits.MaxRelayRequestsPerSecond),
		}, mx)
		endpoints = proxyendpoints.New(relayClient, 0, epLimits.MMDSRuntimeConfig, mx)
	}

	for i := 0; i < cfg.Workers; i++ {
		go superviseProxyWorker(ctx, i, cfgPath, cfg, proxyNS, dataLn, forwardLn, mmdsLn, view, endpoints, epLimits.MaxWorkerInflight, epLimits.MaxWorkerRPCFrameBytes, mx, log)
	}

	reg := routesync.Register{
		Subscribe:     &routesync.Subscribe{Kind: routesync.KindRouteWake},
		Proxy:         &routesync.Proxy{Socket: routesync.Socket{Path: cfg.ProxySocket}},
		Mmds:          mmdsEndpointsEnabled,
		MmdsEndpoints: mmdsEndpointsEnabled,
	}
	dial := func(ctx context.Context) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", cfg.ConfigSocket)
	}
	var mmdsSink routesync.MmdsSink
	if mmdsEndpointsEnabled {
		mmdsSink = endpoints
	}
	go routesync.NewSubscriber(dial, proxyPluginID, reg, view, mmdsSink, view, log).Run(ctx)

	log.Info("node-ctl proxy master serving",
		"workers", cfg.Workers,
		"data_listen", cfg.DataListen,
		"proxy_socket", cfg.ProxySocket,
		"mmds.enabled", cfg.MMDS.Enabled,
		"mmds.listen", cfg.MMDS.Listen,
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
	maxRPCFrameBytes, _ := strconv.Atoi(os.Getenv(envProxyMmdsMaxFrame)) // "" / malformed -> 0 -> mmdsrpc default
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
	view := proxyshm.NewWorkerView(table, updates, wakeFn, cfg.ParkTimeoutDur())
	authMode := func() string {
		if m := view.Policy().AuthMode; m != "" {
			return m
		}
		return cfg.Auth
	}
	mx := newMetricsPipeCounter(ctx, fdEnv(envProxyMetricsFD), log.With("proxy_worker", workerID))
	px := proxy.New(view, authMode, log.With("proxy_worker", workerID), mx)

	errCh := make(chan error, 3)
	forwardLn, err := listenerFromFD(fdEnv(envProxyForwardFD), "proxy-forward")
	if err != nil {
		return err
	}
	if forwardLn == nil {
		return fmt.Errorf("proxy worker: missing forward listener fd")
	}
	go func() { errCh <- serveListener(ctx, forwardLn, px, "", "", log) }()

	if dataLn, err := listenerFromFD(fdEnv(envProxyDataFD), "proxy-data"); err != nil {
		return err
	} else if dataLn != nil {
		go func() { errCh <- serveListener(ctx, dataLn, px, cfg.TLS.Cert, cfg.TLS.Key, log) }()
	}

	if mmdsLn, err := listenerFromFD(fdEnv(envProxyMMDSFD), "proxy-mmds"); err != nil {
		return err
	} else if mmdsLn != nil {
		// MMDS endpoint dispatch: the RPC channel to the master's
		// endpoint table, if the master created one (nil when the feature
		// is disabled — every request then falls through to the built-in
		// envd token/metadata flow unmodified, exactly as internal mode
		// does when mmds.endpoints.enabled=false).
		var mmdsAuth mmds.EndpointAuthority
		if rpcFD := fdEnv(envProxyMmdsRpcFD); rpcFD >= 0 {
			rpcFile := os.NewFile(uintptr(rpcFD), "proxy-mmds-rpc")
			rpcConn, cerr := net.FileConn(rpcFile)
			rpcFile.Close() // net.FileConn dups the fd; the original is no longer needed
			if cerr != nil {
				return fmt.Errorf("proxy worker: mmds rpc conn: %w", cerr)
			}
			mmdsAuth = mmdsrpc.NewClient(rpcConn, maxRPCFrameBytes)
		}
		go func() { errCh <- mmds.New(view, mmdsAuth, cfg.ParkTimeoutDur(), log, mx).Serve(ctx, mmdsLn) }()
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

func superviseProxyWorker(ctx context.Context, idx int, cfgPath string, cfg *config.ProxyFileConfig, proxyNS *netns.NetNS, dataLn, forwardLn, mmdsLn net.Listener, view *proxyshm.MasterView, endpoints *proxyendpoints.Table, maxWorkerInflight, maxRPCFrameBytes int, mx *metrics.M, log *slog.Logger) {
	workerID := fmt.Sprintf("proxy-%d", idx)
	for ctx.Err() == nil {
		err := runProxyWorkerProcess(ctx, workerID, cfgPath, proxyNS, dataLn, forwardLn, mmdsLn, view, endpoints, maxWorkerInflight, maxRPCFrameBytes, mx, log)
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

func runProxyWorkerProcess(ctx context.Context, workerID, cfgPath string, proxyNS *netns.NetNS, dataLn, forwardLn, mmdsLn net.Listener, view *proxyshm.MasterView, endpoints *proxyendpoints.Table, maxWorkerInflight, maxRPCFrameBytes int, mx *metrics.M, log *slog.Logger) error {
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
	metricsR, metricsW, err := os.Pipe()
	if err != nil {
		closeFiles(append(files, dataFile, forwardFile, mmdsFile, wakeR, wakeW, notifyR, notifyW))
		return err
	}

	// MMDS endpoint RPC channel: a genuine bidirectional socketpair (unlike
	// the wake/notify/metrics pipes above, which are each one-directional)
	// — the worker multiplexes concurrent guest requests over it to the
	// master's endpoint table (internal/proxyendpoints). nil endpoints
	// (the feature disabled) still creates the pair so the fd-numbering
	// stays positionally stable between master and worker, but the worker
	// end reads -1 and mmds.New gets a nil EndpointAuthority.
	var rpcMasterFile, rpcWorkerFile *os.File
	if endpoints != nil {
		rpcFDs, serr := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM, 0)
		if serr != nil {
			closeFiles(append(files, dataFile, forwardFile, mmdsFile, wakeR, wakeW, notifyR, notifyW, metricsR, metricsW))
			return serr
		}
		rpcMasterFile = os.NewFile(uintptr(rpcFDs[0]), "proxy-mmds-rpc-master")
		rpcWorkerFile = os.NewFile(uintptr(rpcFDs[1]), "proxy-mmds-rpc-worker")
	}

	removeNotify := view.RegisterNotifyWriter(notifyW)
	go proxyshm.ReadWakeLoop(ctx, wakeR, view.Wake)
	go readMetricsLoop(ctx, metricsR, mx)
	if rpcMasterFile != nil {
		rpcConn, cerr := net.FileConn(rpcMasterFile)
		rpcMasterFile.Close() // net.FileConn dups the fd; the original is no longer needed
		if cerr != nil {
			removeNotify()
			closeFiles(append(files, dataFile, forwardFile, mmdsFile, wakeR, wakeW, notifyR, notifyW, metricsR, metricsW, rpcWorkerFile))
			return cerr
		}
		go func() {
			if err := mmdsrpc.NewServer(endpoints, maxWorkerInflight, log.With("proxy_worker", workerID), mx, maxRPCFrameBytes).Serve(ctx, rpcConn); err != nil && ctx.Err() == nil {
				log.Debug("mmdsrpc: worker connection ended", "worker", workerID, "err", err)
			}
		}()
	}

	addFile(envProxyDataFD, dataFile)
	addFile(envProxyForwardFD, forwardFile)
	addFile(envProxyMMDSFD, mmdsFile)
	addFile(envProxyWakeFD, wakeW)
	addFile(envProxyNotifyFD, notifyR)
	addFile(envProxyMetricsFD, metricsW)
	addFile(envProxyMmdsRpcFD, rpcWorkerFile)
	env = append(env, envProxyWorkerID+"="+workerID)
	// The worker subprocess has no access to the master's in-memory
	// epLimits (it only re-reads ProxyFileConfig, which carries no MMDS
	// endpoint policy fields today) — passed as a plain env var, the same
	// mechanism already used for envProxyWorkerID, so mmdsrpc.NewClient's
	// maxFrame actually reflects the node policy instead of silently using
	// a fixed default independent of what the master enforces.
	env = append(env, envProxyMmdsMaxFrame+"="+strconv.Itoa(maxRPCFrameBytes))

	cmd := exec.CommandContext(ctx, exe, "proxy", "serve", "--config", cfgPath, "--worker")
	cmd.Env = env
	cmd.ExtraFiles = files
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := startCommandInNetNS(proxyNS, cmd); err != nil {
		removeNotify()
		closeFiles(files)
		return err
	}
	closeFiles(files)
	defer removeNotify()
	err = cmd.Wait()
	if ctx.Err() != nil {
		return nil
	}
	return err
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

type metricsPipeCounter struct {
	f  *os.File
	ch chan string
}

func newMetricsPipeCounter(ctx context.Context, fd int, log *slog.Logger) *metricsPipeCounter {
	if fd < 0 {
		return nil
	}
	c := &metricsPipeCounter{
		f:  os.NewFile(uintptr(fd), "proxy-metrics"),
		ch: make(chan string, 4096),
	}
	go c.run(ctx, log)
	return c
}

func (c *metricsPipeCounter) Inc(name string) {
	if c == nil || name == "" || strings.ContainsAny(name, "\r\n") || len(name) > 1024 {
		return
	}
	select {
	case c.ch <- name:
	default:
	}
}

func (c *metricsPipeCounter) run(ctx context.Context, log *slog.Logger) {
	defer c.f.Close()
	for {
		select {
		case <-ctx.Done():
			return
		case name := <-c.ch:
			if err := writeMetricFrame(c.f, name); err != nil {
				if log != nil {
					log.Warn("proxy metrics pipe", "err", err)
				}
				return
			}
		}
	}
}

func readMetricsLoop(ctx context.Context, f *os.File, mx *metrics.M) {
	defer f.Close()
	for ctx.Err() == nil {
		name, err := readMetricFrame(f)
		if err != nil {
			return
		}
		mx.Inc(name)
	}
}

func writeMetricFrame(w io.Writer, name string) error {
	var lenbuf [4]byte
	b := []byte(name)
	binary.LittleEndian.PutUint32(lenbuf[:], uint32(len(b)))
	if _, err := w.Write(lenbuf[:]); err != nil {
		return err
	}
	_, err := w.Write(b)
	return err
}

func readMetricFrame(r io.Reader) (string, error) {
	var lenbuf [4]byte
	if _, err := io.ReadFull(r, lenbuf[:]); err != nil {
		return "", err
	}
	n := binary.LittleEndian.Uint32(lenbuf[:])
	if n == 0 || n > 1024 {
		return "", fmt.Errorf("proxy metrics: bad frame length %d", n)
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}
	return string(buf), nil
}
