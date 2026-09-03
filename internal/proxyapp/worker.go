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
	"github.com/kuasar-sandbox/orchestrator/internal/appnet"
	"github.com/kuasar-sandbox/orchestrator/internal/mmds"
	"github.com/kuasar-sandbox/orchestrator/internal/mmdsrpc"
	"github.com/kuasar-sandbox/orchestrator/internal/proxy"
	"github.com/kuasar-sandbox/orchestrator/internal/proxyadmission"
	"github.com/kuasar-sandbox/orchestrator/internal/proxyext"
	"github.com/kuasar-sandbox/orchestrator/internal/proxyshm"
	"github.com/kuasar-sandbox/orchestrator/internal/proxystats"
)

// PreparedWorker owns every inherited worker resource after the bootstrap has
// been verified. Preparation precedes the public BindRuntime hook; Run sends
// stats readiness, waits for initial route sync, and only then serves traffic.
type PreparedWorker struct {
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
	// admissionEscaped is set before the admission-backed handler is exposed to
	// an extension or listener. HTTP/1 hijacked handlers are not awaited by
	// http.Server.Shutdown, so their mapping must remain valid until process
	// exit even after Run returns and Close releases the other worker resources.
	admissionEscaped atomic.Bool
	closeOnce        sync.Once
	closeErr         error
}

// PrepareWorker reconstructs the shared table, inherited listeners, pipes, and
// process-local connections. It performs no hook invocation and advertises no
// readiness. The caller must close the result on every path.
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
// process-local Runtime.
func (worker *PreparedWorker) Run(ctx context.Context, runtime *Runtime) error {
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
	// A CONNECT handler may outlive appnet.Serve because HTTP/1 hijacked
	// connections are outside http.Server.Shutdown's wait set. Keep the mmap
	// process-owned from this point; the kernel tears it down after every
	// handler has necessarily stopped at process exit.
	worker.admissionEscaped.Store(true)
	var ingressHandler http.Handler = proxyHandler
	if runtime.WorkerExtension != nil {
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
	errorChannel := make(chan error, 3)
	go func() { errorChannel <- <-senderDone }()
	go func() { errorChannel <- appnet.Serve(workerCtx, worker.data, ingressHandler, runtime.DataTLS) }()
	if worker.mmds != nil {
		go func() { errorChannel <- mmds.New(view, cfg.ParkTimeoutDur(), logger).Serve(workerCtx, worker.mmds) }()
	}

	logger.Info("proxy worker serving", "worker", process.WorkerID, "epoch", process.WorkerEpoch)
	select {
	case <-ctx.Done():
		return nil
	case err := <-errorChannel:
		if ctx.Err() != nil {
			return nil
		}
		return err
	}
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

// Close releases prepared resources and is safe to call more than once.
func (worker *PreparedWorker) Close() error {
	if worker == nil {
		return nil
	}
	worker.closeOnce.Do(func() {
		admission := worker.admission
		if worker.admissionEscaped.Load() {
			// Do not Munmap while an HTTP/1 hijacked handler can still execute
			// Worker.Valid or Lease.Release. This is a worker subprocess; process
			// exit is the safe and deterministic reclamation boundary.
			admission = nil
		}
		worker.closeErr = errors.Join(
			closeFile(worker.wakeFile), closeFile(worker.notifyFile), closeConn(worker.statsConn),
			closeConn(worker.mmdsRPCConn), closeListener(worker.data),
			closeListener(worker.mmds), closeTable(worker.table),
			closeAdmission(admission),
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
	for _, fd := range []int{fds.Data, fds.MMDS, fds.Wake, fds.Notify, fds.Stats, fds.MMDSRPC, fds.Admission} {
		if fd >= 0 {
			result[fd] = struct{}{}
		}
	}
	return result
}

func closeAdmission(admission *proxyadmission.Worker) error {
	if admission == nil {
		return nil
	}
	return admission.Close()
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
	return file.Close()
}

func closeConn(connection net.Conn) error {
	if connection == nil {
		return nil
	}
	return connection.Close()
}

func closeListener(listener net.Listener) error {
	if listener == nil {
		return nil
	}
	return listener.Close()
}

func closeTable(table *proxyshm.Table) error {
	if table == nil {
		return nil
	}
	return table.Close()
}
