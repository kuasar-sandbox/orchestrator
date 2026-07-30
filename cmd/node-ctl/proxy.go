package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
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

	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/metrics"
	"github.com/kuasar-sandbox/orchestrator/internal/mmds"
	"github.com/kuasar-sandbox/orchestrator/internal/mmdsrpc"
	"github.com/kuasar-sandbox/orchestrator/internal/mmdssvc"
	"github.com/kuasar-sandbox/orchestrator/internal/netns"
	"github.com/kuasar-sandbox/orchestrator/internal/proxy"
	"github.com/kuasar-sandbox/orchestrator/internal/proxyshm"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
)

const (
	envProxyDataFD    = "KUASAR_PROXY_DATA_FD"
	envProxyForwardFD = "KUASAR_PROXY_FORWARD_FD"
	envProxyMMDSFD    = "KUASAR_PROXY_MMDS_FD"
	envProxyWakeFD    = "KUASAR_PROXY_WAKE_FD"
	envProxyNotifyFD  = "KUASAR_PROXY_NOTIFY_FD"
	envProxyMetricsFD = "KUASAR_PROXY_METRICS_FD"
	envProxyMmdsRPCFD = "KUASAR_PROXY_MMDSRPC_FD"
	envProxyWorkerID  = "KUASAR_PROXY_WORKER_ID"
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
	if cfg.MMDSListen != "" {
		mmdsLn, err = listenTCPInNetNS(proxyNS, cfg.MMDSListen)
		if err != nil {
			return fmt.Errorf("proxy: listen mmds_listen %s: %w", cfg.MMDSListen, err)
		}
		defer mmdsLn.Close()
	}

	mx := metrics.New()
	if cfg.MetricsListen != "" {
		go serveMetrics(ctx, cfg.MetricsListen, mx, log)
	}

	for i := 0; i < cfg.Workers; i++ {
		go superviseProxyWorker(ctx, i, cfgPath, cfg, proxyNS, dataLn, forwardLn, mmdsLn, view, mx, log)
	}

	reg := routesync.Register{
		Subscribe:   &routesync.Subscribe{Kind: routesync.KindRouteWake},
		Proxy:       &routesync.Proxy{Socket: routesync.Socket{Path: cfg.ProxySocket}},
		Mmds:        cfg.MMDSListen != "",
		MMDSSecrets: cfg.MMDSListen != "",
	}
	dial := func(ctx context.Context) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", cfg.ConfigSocket)
	}
	go routesync.NewSubscriber(dial, routesync.ProxyPluginID, reg, view, view, log).Run(ctx)

	log.Info("node-ctl proxy master serving",
		"workers", cfg.Workers,
		"data_listen", cfg.DataListen,
		"proxy_socket", cfg.ProxySocket,
		"mmds_listen", cfg.MMDSListen,
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
	if fd := fdEnv(envProxyMmdsRPCFD); fd >= 0 {
		mmdsClient = mmdsrpc.NewClient(os.NewFile(uintptr(fd), "proxy-mmdsrpc"))
		defer mmdsClient.Close()
	}
	services, err := mmdssvc.BuildRegistry(cfg.Services)
	if err != nil {
		log.Warn("proxy worker: invalid mmds service registry entry; type:\"service\" routes fail closed (503)", "err", err)
	}
	view := proxyshm.NewWorkerView(table, updates, wakeFn, cfg.ParkTimeoutDur(), mmdsClient, cfg.ProxyRPCTimeoutDur(), services)
	authMode := func() string {
		if m := view.Policy().AuthMode; m != "" {
			return m
		}
		return cfg.Auth
	}
	mx := newMetricsPipeCounter(ctx, fdEnv(envProxyMetricsFD), log.With("proxy_worker", workerID))
	px := proxy.NewWithDialer(view, authMode, log.With("proxy_worker", workerID), mx, nil, cfg.Paths.RunRoot)

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
		go func() { errCh <- mmds.New(view, cfg.ParkTimeoutDur(), log).Serve(ctx, mmdsLn) }()
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

func superviseProxyWorker(ctx context.Context, idx int, cfgPath string, cfg *config.ProxyFileConfig, proxyNS *netns.NetNS, dataLn, forwardLn, mmdsLn net.Listener, view *proxyshm.MasterView, mx *metrics.M, log *slog.Logger) {
	workerID := fmt.Sprintf("proxy-%d", idx)
	for ctx.Err() == nil {
		err := runProxyWorkerProcess(ctx, workerID, cfgPath, proxyNS, dataLn, forwardLn, mmdsLn, view, mx, log)
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

func runProxyWorkerProcess(ctx context.Context, workerID, cfgPath string, proxyNS *netns.NetNS, dataLn, forwardLn, mmdsLn net.Listener, view *proxyshm.MasterView, mx *metrics.M, log *slog.Logger) error {
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
	mmdsRPCMaster, mmdsRPCWorker, err := newSocketpair()
	if err != nil {
		closeFiles(append(files, dataFile, forwardFile, mmdsFile, wakeR, wakeW, notifyR, notifyW, metricsR, metricsW))
		return err
	}
	removeNotify := view.RegisterNotifyWriter(notifyW)
	go proxyshm.ReadWakeLoop(ctx, wakeR, view.Wake)
	go readMetricsLoop(ctx, metricsR, mx)
	go mmdsrpc.NewServer(mmdsRPCMaster, mmdsRPCHandler(view), log).Serve()

	addFile(envProxyDataFD, dataFile)
	addFile(envProxyForwardFD, forwardFile)
	addFile(envProxyMMDSFD, mmdsFile)
	addFile(envProxyWakeFD, wakeW)
	addFile(envProxyNotifyFD, notifyR)
	addFile(envProxyMetricsFD, metricsW)
	addFile(envProxyMmdsRPCFD, mmdsRPCWorker)
	env = append(env, envProxyWorkerID+"="+workerID)

	cmd := exec.CommandContext(ctx, exe, "proxy", "serve", "--config", cfgPath, "--worker")
	cmd.Env = env
	cmd.ExtraFiles = files
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := startCommandInNetNS(proxyNS, cmd); err != nil {
		removeNotify()
		_ = mmdsRPCMaster.Close()
		closeFiles(files)
		return err
	}
	closeFiles(files)
	defer removeNotify()
	defer mmdsRPCMaster.Close()
	err = cmd.Wait()
	if ctx.Err() != nil {
		return nil
	}
	return err
}

// newSocketpair returns a connected AF_UNIX SOCK_STREAM pair for the
// mmdsrpc master<->worker channel: unlike the existing wake/notify/metrics
// pipes (each one-directional), request/response needs a bidirectional
// connection.
func newSocketpair() (masterEnd, workerEnd *os.File, err error) {
	fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err != nil {
		return nil, nil, err
	}
	return os.NewFile(uintptr(fds[0]), "mmdsrpc-master"), os.NewFile(uintptr(fds[1]), "mmdsrpc-worker"), nil
}

// mmdsRPCHandler answers a worker's EndpointRequest from the proxy master's
// in-heap MMDSRoutes/MMDSSecrets stores (see their doc comments). For a
// "secret" route it resolves the named value out of the synced
// store.MMDSSecretBlob -- absent because the sandbox's secrets row has never
// been touched at all (blob not synced yet) is Retryable, matching
// internal-mode Orchestrator.MMDSRoute's revision==0 bootstrap wait; absent
// with a synced blob (this name specifically was never set, or was revoked)
// is not.
func mmdsRPCHandler(view *proxyshm.MasterView) mmdsrpc.Handler {
	return func(sid, path string) (mmdsrpc.Route, bool) {
		canonical, ok := view.MMDSRoutes().Get(sid)
		if !ok {
			return mmdsrpc.Route{}, false
		}
		route, ok := sandboxcfg.LookupMMDSRoute(map[string]string{sandboxcfg.NsMMDS: canonical}, path)
		if !ok {
			return mmdsrpc.Route{}, false
		}
		switch route.Type {
		case sandboxcfg.MMDSRouteStatic:
			return mmdsrpc.Route{Type: route.Type, ContentType: route.ContentType, Body: route.Data}, true
		case sandboxcfg.MMDSRouteService:
			if !view.MMDSRoutes().Synced() {
				// Mid-resync (or never yet synced) after a disconnect: a
				// service route drives a real dial to a local trusted service
				// under the sandbox's identity, so a stale declaration must
				// not be acted on -- fail closed (503) rather than risk
				// dialing a target the sandbox is no longer (or was never)
				// authorized to reach. Unlike secret, there is no plaintext
				// to protect here, so MMDSRoutes only tracks sync readiness,
				// never eagerly clears -- static routes below stay available
				// through the same window since their content is immutable
				// and non-sensitive.
				return mmdsrpc.Route{Type: route.Type, Unavailable: true}, true
			}
			target, _ := sandboxcfg.LookupMMDSService(map[string]string{sandboxcfg.NsMMDS: canonical}, route.ServiceName)
			return mmdsrpc.Route{Type: route.Type, Target: target, ServiceName: route.ServiceName}, true
		}
		if !view.MMDSSecrets().Synced() {
			// Mid-resync (or never yet synced) after a disconnect: fail
			// closed rather than risk serving stale plaintext or a false
			// "never configured" 404 for a secret that may well be
			// configured -- WorkerView.MMDSRoute maps this to a hard error
			// (503), distinct from both outcomes.
			return mmdsrpc.Route{Type: route.Type, Unavailable: true}, true
		}
		blobJSON, ok := view.MMDSSecrets().Get(sid)
		if !ok {
			return mmdsrpc.Route{Type: route.Type, Present: false, Retryable: true}, true
		}
		var blob store.MMDSSecretBlob
		if err := json.Unmarshal([]byte(blobJSON), &blob); err != nil {
			return mmdsrpc.Route{Type: route.Type, Present: false, Retryable: true}, true
		}
		value, present := blob.Values[route.SecretName]
		if present && store.MMDSSecretExpired(value) {
			present = false
		}
		if !present {
			return mmdsrpc.Route{Type: route.Type, Present: false, Retryable: blob.Revision == 0}, true
		}
		return mmdsrpc.Route{Type: route.Type, ContentType: value.ContentType, Body: value.BodyBase64, Present: true}, true
	}
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
