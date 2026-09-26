package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"golang.org/x/sys/unix"
)

// runSandbox is the ExecStart of sandbox-runner@<run-id>.service: it waits until
// the run-id is assigned a sandbox id, fetches that sandbox's LaunchSpec over the
// config-socket, starts sandbox-ctl as its direct child, waits once, and reports
// the bounded child result before exiting.
//
//	node-ctl run-sandbox --pidfile=<f> --config-socket=<uds> --run-id=<rid>
func runSandbox(args []string, log *slog.Logger) error {
	fs := flag.NewFlagSet("run-sandbox", flag.ExitOnError)
	pidfile := fs.String("pidfile", "", "pidfile to lock+write (TASK_PIDFILE)")
	socket := fs.String("config-socket", "", "config-socket UDS (TASK_CONFIG_SOCKET)")
	runID := fs.String("run-id", "", "run id (TASK_RUN_ID)")
	_ = fs.Parse(args)
	envDefault(pidfile, "TASK_PIDFILE")
	envDefault(socket, "TASK_CONFIG_SOCKET")
	envDefault(runID, "TASK_RUN_ID")
	if *pidfile == "" || *socket == "" || *runID == "" {
		return fmt.Errorf("run-sandbox: --pidfile, --config-socket, and --run-id required")
	}
	return runAssignedSandbox(*pidfile, *socket, *runID, runSandboxOps{
		lockPidfile:    lockPidfile,
		prepareCgroup:  func() (*os.File, error) { return prepareRunnerCgroup(*runID) },
		openSession:    openRunSessionKeeper,
		waitAssignment: configsock.WaitAssignment,
		connectReady:   connectReadinessSocket,
		launchTask: func(socket, sid, runID string, ready, cgroup *os.File) error {
			ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
			defer stop()
			return launchTask(ctx, stop, socket, sid, runID, ready, cgroup, log)
		},
	})
}

type runSessionCloser interface{ Close() error }

func openRunSessionKeeper(ctx context.Context, socket, kind, runID string) (runSessionCloser, error) {
	return configsock.OpenRunSessionKeeper(ctx, socket, kind, runID)
}

type runSandboxOps struct {
	lockPidfile    func(string) error
	prepareCgroup  func() (*os.File, error)
	openSession    func(context.Context, string, string, string) (runSessionCloser, error)
	waitAssignment func(context.Context, string, string, string) (string, error)
	connectReady   func(string) (*os.File, error)
	launchTask     func(string, string, string, *os.File, *os.File) error
}

func runAssignedSandbox(pidfile, socket, runID string, ops runSandboxOps) error {
	if err := ops.lockPidfile(pidfile); err != nil {
		return err
	}
	vmmCgroup, err := ops.prepareCgroup()
	if err != nil {
		return fmt.Errorf("prepare runner cgroup: %w", err)
	}
	defer vmmCgroup.Close()
	if ops.openSession != nil {
		session, err := ops.openSession(context.Background(), socket, "sandbox", runID)
		if err != nil {
			return fmt.Errorf("open run session: %w", err)
		}
		defer session.Close()
	}
	sid, err := ops.waitAssignment(context.Background(), socket, "sandbox", runID)
	if err != nil {
		return fmt.Errorf("wait assignment: %w", err)
	}
	runRoot := filepath.Dir(filepath.Dir(pidfile))
	ready, err := ops.connectReady(configsock.ReadinessSocketPath(runRoot, sid))
	if err != nil {
		return fmt.Errorf("connect readiness socket: %w", err)
	}
	// The connection owns the bridge until child Start consumes it. Any config,
	// chdir, argv, or Start failure returns through this defer and turns into EOF
	// for the orchestrator instead of making it wait for the launch timeout.
	defer ready.Close()
	return ops.launchTask(socket, sid, runID, ready, vmmCgroup)
}

func connectReadinessSocket(path string) (*os.File, error) {
	conn, err := net.DialUnix("unix", nil, &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		return nil, err
	}
	f, err := conn.File()
	_ = conn.Close()
	if err != nil {
		return nil, err
	}
	// File returns a duplicate. Keep it close-on-exec throughout every fallible
	// preparation step. sandboxproc.Start explicitly passes the child copy;
	// the parent descriptor never needs to become inheritable.
	flags, err := unix.FcntlInt(f.Fd(), unix.F_GETFD, 0)
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("get readiness fd flags: %w", err)
	}
	if _, err := unix.FcntlInt(f.Fd(), unix.F_SETFD, flags|unix.FD_CLOEXEC); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("set readiness fd close-on-exec: %w", err)
	}
	return f, nil
}
