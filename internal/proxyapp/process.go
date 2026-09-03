package proxyapp

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"strconv"
	"syscall"

	"github.com/kuasar-sandbox/orchestrator/internal/appnet"
	"github.com/kuasar-sandbox/orchestrator/internal/mmdsrpc"
	"github.com/kuasar-sandbox/orchestrator/internal/netns"
	"github.com/kuasar-sandbox/orchestrator/internal/proxyadmission"
	"github.com/kuasar-sandbox/orchestrator/internal/proxyshm"
	"github.com/kuasar-sandbox/orchestrator/internal/proxystats"
)

func runWorkerProcess(ctx context.Context, workerIndex int, workerID string, epoch uint64, effective *EffectiveConfig, proxyNamespace *netns.NetNS, dataListener, mmdsListener net.Listener, view *proxyshm.MasterView, admission *proxyadmission.Master, stats *proxystats.MasterStats, logger *slog.Logger) error {
	if effective == nil || view == nil || admission == nil || stats == nil || logger == nil {
		return fmt.Errorf("proxy worker process: unresolved startup state")
	}
	executable, err := os.Executable()
	if err != nil {
		return fmt.Errorf("proxy worker process: current executable: %w", err)
	}

	dataFile, err := listenerFile(dataListener)
	if err != nil {
		return err
	}
	mmdsFile, err := listenerFile(mmdsListener)
	if err != nil {
		closeFiles([]*os.File{dataFile})
		return err
	}
	wakeRead, wakeWrite, err := os.Pipe()
	if err != nil {
		closeFiles([]*os.File{dataFile, mmdsFile})
		return err
	}
	notifyRead, notifyWrite, err := os.Pipe()
	if err != nil {
		closeFiles([]*os.File{dataFile, mmdsFile, wakeRead, wakeWrite})
		return err
	}
	statsMaster, statsWorker, err := newSocketPair("proxy-stats")
	if err != nil {
		closeFiles([]*os.File{dataFile, mmdsFile, wakeRead, wakeWrite, notifyRead, notifyWrite})
		return err
	}
	mmdsRPCMaster, mmdsRPCWorker, err := newSocketPair("proxy-mmdsrpc")
	if err != nil {
		closeFiles([]*os.File{
			dataFile, mmdsFile, wakeRead, wakeWrite, notifyRead, notifyWrite,
			statsMaster, statsWorker,
		})
		return err
	}
	admissionFile, err := admission.DupFile()
	if err != nil {
		closeFiles([]*os.File{
			dataFile, mmdsFile, wakeRead, wakeWrite, notifyRead, notifyWrite,
			statsMaster, statsWorker, mmdsRPCMaster, mmdsRPCWorker,
		})
		return err
	}
	parentFiles := []*os.File{wakeRead, notifyWrite, statsMaster, mmdsRPCMaster}
	defer closeFiles(parentFiles)
	workerFiles := make([]*os.File, 0, 8)
	nextDescriptor := 3
	addWorkerFile := func(file *os.File) int {
		if file == nil {
			return -1
		}
		descriptor := nextDescriptor
		nextDescriptor++
		workerFiles = append(workerFiles, file)
		return descriptor
	}
	fds := workerFDMapping{
		Data: addWorkerFile(dataFile), MMDS: addWorkerFile(mmdsFile),
		Wake: addWorkerFile(wakeWrite), Notify: addWorkerFile(notifyRead), Stats: addWorkerFile(statsWorker),
		MMDSRPC:   addWorkerFile(mmdsRPCWorker),
		Admission: addWorkerFile(admissionFile),
	}
	bootstrapFile, err := newWorkerBootstrapFile(workerIndex, workerID, epoch, effective, fds)
	if err != nil {
		closeFiles(workerFiles)
		return err
	}
	bootstrapDescriptor := nextDescriptor
	workerFiles = append(workerFiles, bootstrapFile)
	defer closeFiles(workerFiles)

	removeNotify := view.RegisterNotifyWriter(notifyWrite)
	defer removeNotify()
	go proxyshm.ReadWakeLoop(ctx, wakeRead, view.Wake)
	go mmdsrpc.NewServer(mmdsRPCMaster, view.ResolveMMDS, logger).Serve()
	if err := admission.BeginWorker(workerIndex, epoch); err != nil {
		return err
	}
	admissionBegun := true
	defer func() {
		if admissionBegun {
			_ = admission.ClearWorker(workerIndex, epoch)
		}
	}()

	if err := stats.BeginWorker(workerID, epoch); err != nil {
		return err
	}
	workerBegun := true
	defer func() {
		if workerBegun {
			stats.WorkerExited(workerID, epoch)
		}
	}()

	workerCtx, cancelWorker := context.WithCancel(ctx)
	defer cancelWorker()
	command := exec.CommandContext(workerCtx, "/proc/self/exe")
	// Preserve the established worker process shape for operators and readiness
	// probes. The sealed bootstrap, rather than these arguments, authorizes the
	// private worker entry point; custom binaries also ignore the CLI arguments
	// and enter through App's bootstrap detection.
	command.Args = []string{executable, "proxy", "serve", "--worker"}
	command.Env = append(
		withoutWorkerEnvironment(os.Environ()),
		workerBootstrapEnvironment+"="+strconv.Itoa(bootstrapDescriptor),
	)
	command.ExtraFiles = workerFiles
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr
	if err := appnet.StartCommandInNetNS(proxyNamespace, command); err != nil {
		return err
	}
	closeFiles(workerFiles)
	workerFiles = nil

	streamResult := make(chan error, 1)
	go func() { streamResult <- stats.ReadWorkerStream(workerCtx, statsMaster, workerID, epoch) }()
	waitResult := make(chan error, 1)
	go func() { waitResult <- command.Wait() }()

	var result error
	select {
	case err := <-streamResult:
		if ctx.Err() == nil {
			stats.StreamFault(workerID, epoch)
			if err == nil {
				err = fmt.Errorf("stats stream ended before worker exit")
			}
			result = fmt.Errorf("proxy worker %s stats stream: %w", workerID, err)
		}
		cancelWorker()
		<-waitResult
	case err := <-waitResult:
		result = err
		stats.StreamFault(workerID, epoch)
		_ = statsMaster.Close()
		<-streamResult
	}
	stats.WorkerExited(workerID, epoch)
	workerBegun = false
	if err := admission.ClearWorker(workerIndex, epoch); err != nil && result == nil {
		result = err
	}
	admissionBegun = false
	if ctx.Err() != nil {
		return nil
	}
	return result
}

func newSocketPair(name string) (master, worker *os.File, err error) {
	descriptors, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM|syscall.SOCK_CLOEXEC, 0)
	if err != nil {
		return nil, nil, err
	}
	return os.NewFile(uintptr(descriptors[0]), name+"-master"), os.NewFile(uintptr(descriptors[1]), name+"-worker"), nil
}

func listenerFile(listener net.Listener) (*os.File, error) {
	if listener == nil {
		return nil, nil
	}
	switch value := listener.(type) {
	case *net.TCPListener:
		return value.File()
	case *net.UnixListener:
		return value.File()
	default:
		return nil, fmt.Errorf("proxy: unsupported listener type %T", listener)
	}
}

func closeFiles(files []*os.File) {
	for _, file := range files {
		if file != nil {
			_ = file.Close()
		}
	}
}
