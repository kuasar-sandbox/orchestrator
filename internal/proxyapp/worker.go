package proxyapp

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sys/unix"

	proxyextension "github.com/kuasar-sandbox/orchestrator/app/proxy/extension"
	"github.com/kuasar-sandbox/orchestrator/internal/mmds"
	"github.com/kuasar-sandbox/orchestrator/internal/mmdsrpc"
	"github.com/kuasar-sandbox/orchestrator/internal/proxy"
	"github.com/kuasar-sandbox/orchestrator/internal/proxyadmission"
	"github.com/kuasar-sandbox/orchestrator/internal/proxydrain"
	"github.com/kuasar-sandbox/orchestrator/internal/proxyext"
	"github.com/kuasar-sandbox/orchestrator/internal/proxyshm"
	"github.com/kuasar-sandbox/orchestrator/internal/proxystats"
)

// PreparedWorker owns inherited descriptors after bootstrap verification. The
// worker is a dedicated one-shot subprocess: successful route and admission
// mappings live until process exit, including on preparation or runtime failure.
// Run sends stats readiness, waits for initial route sync, then serves traffic.
type PreparedWorker struct {
	control   *controlPeer
	effective *EffectiveConfig
	process   Process
	table     *proxyshm.Table
	admission *proxyadmission.Worker

	wakeFile    *os.File
	notifyFile  *os.File
	statsConn   net.Conn
	mmdsRPCConn net.Conn
	data        net.Listener
	mmds        net.Listener
	run         atomic.Bool
	closeOnce   sync.Once
	closeErr    error
}

// PrepareWorker reconstructs the shared table, inherited listeners, pipes, and
// process-local connections. It performs no hook invocation and advertises no
// readiness. The caller must close descriptors and exit the worker process on
// every return path; this is not an in-process restartable server.
func PrepareWorker(bootstrap *WorkerBootstrap) (_ *PreparedWorker, returnErr error) {
	if bootstrap == nil || bootstrap.effective == nil || bootstrap.effective.config == nil {
		return nil, fmt.Errorf("proxy worker: verified bootstrap is required")
	}
	prepared := &PreparedWorker{effective: bootstrap.effective, process: bootstrap.process}
	prepared.table, returnErr = proxyshm.Open(bootstrap.effective.config.ShmPath)
	if returnErr != nil {
		return nil, fmt.Errorf("proxy worker: open shared route table: %w", returnErr)
	}
	fds, returnErr := bootstrap.takeFDs()
	if returnErr != nil {
		_ = prepared.Close()
		return nil, returnErr
	}
	owned := workerDescriptors(fds)
	defer func() {
		if returnErr != nil {
			closeDescriptors(owned)
			_ = prepared.Close()
		}
	}()
	control, err := connectionFromFD(fds.Control, "proxy-control")
	delete(owned, fds.Control)
	if err != nil {
		return nil, err
	}
	if control == nil {
		return nil, fmt.Errorf("proxy worker: missing control descriptor")
	}
	prepared.control = newControlPeer(control, bootstrap.identity)
	admissionFile, returnErr := inheritedFile(fds.Admission, "proxy-admission")
	if returnErr != nil {
		return nil, returnErr
	}
	delete(owned, fds.Admission)
	prepared.admission, returnErr = proxyadmission.OpenWorker(
		admissionFile, bootstrap.effective.config.RouteCapacity, bootstrap.effective.config.Workers,
		bootstrap.workerIndex, bootstrap.process.WorkerEpoch,
	)
	if returnErr != nil {
		return nil, fmt.Errorf("proxy worker: open shared admission arena: %w", returnErr)
	}

	prepared.wakeFile, returnErr = inheritedFile(fds.Wake, "proxy-wake")
	if returnErr != nil {
		return nil, returnErr
	}
	delete(owned, fds.Wake)
	prepared.notifyFile, returnErr = inheritedFile(fds.Notify, "proxy-notify")
	if returnErr != nil {
		return nil, returnErr
	}
	delete(owned, fds.Notify)

	prepared.mmdsRPCConn, returnErr = connectionFromFD(fds.MMDSRPC, "proxy-mmdsrpc")
	delete(owned, fds.MMDSRPC) // connectionFromFD always consumes the inherited descriptor.
	if returnErr != nil {
		return nil, returnErr
	}
	if prepared.mmdsRPCConn == nil {
		return nil, fmt.Errorf("proxy worker: missing MMDS RPC descriptor")
	}
	prepared.statsConn, returnErr = connectionFromFD(fds.Stats, "proxy-stats")
	delete(owned, fds.Stats)
	if returnErr != nil {
		return nil, returnErr
	}
	if prepared.statsConn == nil {
		return nil, fmt.Errorf("proxy worker: missing stats stream descriptor")
	}

	prepared.data, returnErr = listenerFromFD(fds.Data, "proxy-data")
	delete(owned, fds.Data)
	if returnErr != nil {
		return nil, returnErr
	}
	if prepared.data == nil {
		return nil, fmt.Errorf("proxy worker: missing data listener descriptor")
	}
	prepared.mmds, returnErr = listenerFromFD(fds.MMDS, "proxy-mmds")
	if fds.MMDS >= 0 {
		delete(owned, fds.MMDS)
	}
	if returnErr != nil {
		return nil, returnErr
	}
	return prepared, nil
}

// Run starts one prepared worker. It is one-shot and requires a fully resolved
// process-local Runtime. Returning ends the worker subprocess, not every handler:
// no successful mapping is unmapped while a handler or extension can use it.
func (worker *PreparedWorker) Run(ctx context.Context, runtime *Runtime) (returnErr error) {
	if worker == nil || worker.effective == nil || worker.table == nil || runtime == nil || runtime.Logger == nil {
		return fmt.Errorf("proxy worker: unresolved startup state")
	}
	if !worker.run.CompareAndSwap(false, true) {
		return fmt.Errorf("proxy worker can only be run once")
	}
	cfg := worker.effective.config
	process := worker.process
	logger := runtime.Logger

	notifyFile := worker.notifyFile
	worker.notifyFile = nil // Updates now owns this inherited descriptor and wrapper.
	updates, err := proxyshm.NewUpdatesFromFile(notifyFile)
	if err != nil {
		return fmt.Errorf("proxy worker: open notify descriptor: %w", err)
	}
	defer updates.Close()
	wakeFile := worker.wakeFile
	worker.wakeFile = nil // WakeWriter now owns this inherited descriptor and wrapper.
	wakes := proxyshm.NewWakeWriterFromFile(wakeFile)
	if wakes == nil {
		return fmt.Errorf("proxy worker: missing wake descriptor")
	}
	defer wakes.Close()
	mmdsConnection := worker.mmdsRPCConn
	worker.mmdsRPCConn = nil // Client owns the reconstructed connection.
	mmdsClient := mmdsrpc.NewClient(mmdsConnection)
	if mmdsClient == nil {
		return fmt.Errorf("proxy worker: missing MMDS RPC descriptor")
	}
	defer mmdsClient.Close()

	view := proxyshm.NewMMDSWorkerView(worker.table, worker.admission, updates, wakes.Wake, cfg.ParkTimeoutDur(), mmdsClient)
	authMode := func() string {
		if mode := view.Policy().AuthMode; mode != "" {
			return mode
		}
		return cfg.Auth
	}

	workerCtx, cancelWorker := context.WithCancel(ctx)
	defer cancelWorker()
	extensionStarted := false
	var endOnce sync.Once
	var endErr error
	endExtension := func(deadline time.Time) error {
		endOnce.Do(func() {
			cancelWorker()
			if extensionStarted {
				work, cancel := drainContext(deadline)
				defer cancel()
				endErr = shutdownExtension(work, runtime.WorkerExtension)
			}
		})
		return endErr
	}
	defer func() { returnErr = errors.Join(returnErr, endExtension(time.Now().Add(cfg.ShutdownTimeoutDur()))) }()
	workerStats := proxystats.NewWorkerStatsWithAdmission(worker.admission)
	var proxyHandler *proxy.Proxy
	var extensionHost proxyextension.WorkerHost
	if runtime.WorkerExtension != nil {
		proxyHandler = proxy.NewWithDialer(
			view, authMode, logger.With("proxy_worker", process.WorkerID), workerStats, nil, cfg.Paths.RunRoot,
		).WithTrafficTracker(workerStats)
		extensionHost = proxyext.NewWorkerHost(process, worker.table, proxyHandler)
	}
	senderDone, err := workerStats.StartSender(
		workerCtx, process.WorkerID, process.WorkerEpoch, proxystats.StreamSender(worker.statsConn),
	)
	if err != nil {
		return fmt.Errorf("proxy worker: start stats stream: %w", err)
	}
	if err := waitTableSync(workerCtx, worker.table); err != nil {
		return err
	}
	if proxyHandler == nil {
		proxyHandler = proxy.NewWithDialer(
			view, authMode, logger.With("proxy_worker", process.WorkerID), workerStats, nil, cfg.Paths.RunRoot,
		).WithTrafficTracker(workerStats)
	}
	var ingressHandler http.Handler = proxyHandler
	if runtime.WorkerExtension != nil {
		extensionStarted = true
		ingressHandler, err = startWorkerIngress(
			workerCtx, runtime.WorkerExtension, extensionHost, proxyHandler,
		)
		if err != nil {
			return err
		}
	}
	go workerStats.RunGC(workerCtx, func(sandboxID string) bool {
		_, found := worker.table.Lookup(sandboxID)
		return found
	}, 5*time.Minute)
	err = worker.serveControlled(workerCtx, endExtension, runtime, ingressHandler,
		mmds.New(view, cfg.ParkTimeoutDur(), logger).HTTPServer(workerCtx), senderDone)
	if ctx.Err() != nil {
		return nil
	}
	return err
}

func startWorkerIngress(
	ctx context.Context,
	extension proxyextension.WorkerExtension,
	host proxyextension.WorkerHost,
	next http.Handler,
) (http.Handler, error) {
	if extension == nil {
		return next, nil
	}
	if err := extension.Start(ctx, host); err != nil {
		return nil, fmt.Errorf("proxy worker extension start: %w", err)
	}
	wrapper, ok := extension.(proxyextension.IngressWrapper)
	if !ok {
		return next, nil
	}
	handler := wrapper.WrapIngress(next)
	if handler == nil {
		return nil, fmt.Errorf("proxy worker extension returned a nil ingress handler")
	}
	return handler, nil
}

// Close releases descriptors and is safe after Run has already closed them,
// and to call more than once. Successful mappings are deliberately NOT unmapped
// here: asynchronous users need not have stopped when Run returns. Kernel
// process teardown reclaims both mappings; the master separately clears this
// worker's counters only after cmd.Wait.
func (worker *PreparedWorker) Close() error {
	if worker == nil {
		return nil
	}
	worker.closeOnce.Do(func() {
		worker.closeErr = errors.Join(
			closeControl(worker.control),
			closeFile(worker.wakeFile), closeFile(worker.notifyFile), closeConn(worker.statsConn),
			closeConn(worker.mmdsRPCConn), closeListener(worker.data),
			closeListener(worker.mmds),
		)
	})
	return worker.closeErr
}

func waitTableSync(ctx context.Context, table *proxyshm.Table) error {
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

func inheritedFile(fd int, name string) (*os.File, error) {
	if fd < 0 {
		return nil, nil
	}
	flags, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0)
	if err != nil {
		return nil, fmt.Errorf("proxy worker: bad descriptor %d for %s: %w", fd, name, err)
	}
	if _, err := unix.FcntlInt(uintptr(fd), unix.F_SETFD, flags|unix.FD_CLOEXEC); err != nil {
		return nil, fmt.Errorf("proxy worker: protect descriptor %d for %s: %w", fd, name, err)
	}
	file := os.NewFile(uintptr(fd), name)
	if file == nil {
		return nil, fmt.Errorf("proxy worker: bad descriptor %d for %s", fd, name)
	}
	return file, nil
}

func listenerFromFD(fd int, name string) (net.Listener, error) {
	if fd < 0 {
		return nil, nil
	}
	file := os.NewFile(uintptr(fd), name)
	if file == nil {
		return nil, fmt.Errorf("proxy worker: bad descriptor %d for %s", fd, name)
	}
	defer file.Close()
	return net.FileListener(file)
}

func connectionFromFD(fd int, name string) (net.Conn, error) {
	if fd < 0 {
		return nil, nil
	}
	file := os.NewFile(uintptr(fd), name)
	if file == nil {
		return nil, fmt.Errorf("proxy worker: bad descriptor %d for %s", fd, name)
	}
	defer file.Close()
	return net.FileConn(file)
}

func workerDescriptors(fds workerFDMapping) map[int]struct{} {
	result := make(map[int]struct{}, 7)
	for _, fd := range []int{fds.Control, fds.Data, fds.MMDS, fds.Wake, fds.Notify, fds.Stats, fds.MMDSRPC, fds.Admission} {
		if fd >= 0 {
			result[fd] = struct{}{}
		}
	}
	return result
}

func closeControl(peer *controlPeer) error {
	if peer == nil {
		return nil
	}
	return closeConn(peer.Conn)
}

func (worker *PreparedWorker) serveControlled(ctx context.Context, endExtension func(time.Time) error, runtime *Runtime, ingress http.Handler, mmdsHTTP *http.Server, senderDone <-chan error) error {
	if worker.control == nil {
		return fmt.Errorf("proxy worker: lifecycle control is required")
	}
	ready := make(chan string, 2)
	data, err := proxydrain.New(ctx, ingress, runtime.DataTLS, func() { ready <- "data" })
	if err != nil {
		return err
	}
	defer data.ForceClose()
	var metadata *proxydrain.Server
	if worker.mmds != nil {
		metadata = proxydrain.NewHTTP1(mmdsHTTP, func() { ready <- "mmds" })
		defer metadata.ForceClose()
	}
	type event struct {
		message controlMessage
		err     error
	}
	commands := make(chan event, 1)
	go func() {
		for {
			message, err := worker.control.receive()
			select {
			case commands <- event{message, err}:
			case <-ctx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()
	type serveResult struct {
		endpoint string
		err      error
	}
	errorsFromServe := make(chan serveResult, 2)
	drained := make(chan event, 2)
	_, canShutdown := runtime.WorkerExtension.(proxyextension.Shutdown)
	if err := worker.control.send(controlMessage{Kind: "prepared", CanShutdown: runtime.WorkerExtension == nil || canShutdown}); err != nil {
		return err
	}
	started, dataReady, mmdsReady := false, false, metadata == nil
	dataDraining, mmdsDraining, dataDone, mmdsDone := false, false, false, false
	var deadline time.Time
	var drainErr error
	startDrain := func(server *proxydrain.Server, kind string) {
		go func() {
			var err error
			if server != nil {
				work, stop := drainContext(deadline)
				_ = server.Drain(work)
				err = server.Drain(context.Background()) // Await the actual forced-close result.
				stop()
			}
			drained <- event{message: controlMessage{Kind: kind}, err: err}
		}()
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-senderDone:
			return fmt.Errorf("proxy worker stats stream ended: %v", err)
		case result := <-errorsFromServe:
			if result.err != nil {
				return result.err
			}
			if (result.endpoint == "data" && !dataDraining) || (result.endpoint == "mmds" && !mmdsDraining) {
				return fmt.Errorf("proxy ingress exited before drain")
			}
			if result.endpoint == "data" {
				// Serve has finished publishing its accepted connections. The
				// drain guard is installed and this worker cannot accept again,
				// while already admitted handlers and tunnels may still run.
				if err := worker.control.send(controlMessage{Kind: "data_draining"}); err != nil {
					return err
				}
			}
		case endpoint := <-ready:
			if endpoint == "data" {
				dataReady = true
			} else {
				mmdsReady = true
			}
			if dataReady && mmdsReady && !dataDraining {
				if err := worker.control.send(controlMessage{Kind: "serving"}); err != nil {
					return err
				}
			}
		case e := <-drained:
			drainErr = errors.Join(drainErr, e.err)
			if e.message.Kind == "data_drained" {
				dataDone = true
			} else {
				mmdsDone = true
			}
			e.message.Forced = errors.Is(e.err, ErrForcedClose)
			if e.err != nil {
				e.message.Error = e.err.Error()
			}
			if err := worker.control.send(e.message); err != nil {
				return err
			}
		case e := <-commands:
			if e.err != nil {
				return fmt.Errorf("proxy worker control: %w", e.err)
			}
			switch e.message.Kind {
			case "start":
				if started || dataDraining {
					continue
				}
				started = true
				go func() { errorsFromServe <- serveResult{"data", data.Serve(worker.data)} }()
				if metadata != nil {
					go func() { errorsFromServe <- serveResult{"mmds", metadata.Serve(worker.mmds)} }()
				}
			case "drain":
				if dataDraining {
					continue
				}
				deadline = e.message.Deadline
				dataDraining, mmdsDraining = true, true
				startDrain(data, "data_drained")
				startDrain(metadata, "mmds_drained")
			case "stop":
				if !dataDone || !mmdsDone {
					return fmt.Errorf("proxy worker stop before endpoints completed")
				}
				drainErr = errors.Join(drainErr, endExtension(deadline))
				message := controlMessage{Kind: "stopped", Forced: errors.Is(drainErr, ErrForcedClose)}
				if drainErr != nil {
					message.Error = drainErr.Error()
				}
				_ = worker.control.send(message)
				return drainErr
			default:
				return fmt.Errorf("proxy worker: unexpected control command %q", e.message.Kind)
			}
		}
	}
}

func shutdownExtension(ctx context.Context, extension any) error {
	ending, ok := extension.(proxyextension.Shutdown)
	if !ok {
		return nil
	}
	result := make(chan error, 1)
	go func() { result <- ending.Shutdown(ctx) }()
	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		// Ending this wait cannot forcibly terminate an extension goroutine.
		// Connection/process owners report actual forced closure separately.
		return ctx.Err()
	}
}

func closeDescriptors(descriptors map[int]struct{}) {
	for descriptor := range descriptors {
		_ = unix.Close(descriptor)
	}
}

func closeFile(file *os.File) error {
	if file == nil {
		return nil
	}
	err := file.Close()
	if errors.Is(err, os.ErrClosed) {
		return nil
	}
	return err
}

func closeConn(connection net.Conn) error {
	if connection == nil {
		return nil
	}
	err := connection.Close()
	if errors.Is(err, net.ErrClosed) {
		return nil
	}
	return err
}

func closeListener(listener net.Listener) error {
	if listener == nil {
		return nil
	}
	err := listener.Close()
	if errors.Is(err, net.ErrClosed) {
		return nil
	}
	return err
}
