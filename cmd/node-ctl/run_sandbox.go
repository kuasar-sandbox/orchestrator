package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"golang.org/x/sys/unix"
)

// runSandbox is the ExecStart of sandbox-runner@<run-id>.service: it waits until
// the run-id is assigned a sandbox id, fetches that sandbox's LaunchSpec over the
// config-socket, and exec-replaces into sandbox-ctl.
//
//	node-ctl run-sandbox --pidfile=<f> --config-socket=<uds> --run-id=<rid>
func runSandbox(args []string, _ *slog.Logger) error {
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
		prepareCgroup:  prepareRunnerCgroup,
		waitAssignment: configsock.WaitAssignment,
		connectReady:   connectReadinessSocket,
		launchTask:     launchTask,
	})
}

type runSandboxOps struct {
	lockPidfile    func(string) error
	prepareCgroup  func() (*os.File, error)
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
	sid, err := ops.waitAssignment(context.Background(), socket, "sandbox", runID)
	if err != nil {
		return fmt.Errorf("wait assignment: %w", err)
	}
	runRoot := filepath.Dir(filepath.Dir(pidfile))
	ready, err := ops.connectReady(configsock.ReadinessSocketPath(runRoot, sid))
	if err != nil {
		return fmt.Errorf("connect readiness socket: %w", err)
	}
	// The connection owns the bridge until exec succeeds. Any pidfile, config,
	// chdir, argv, or exec failure returns through this defer and turns into EOF
	// for the orchestrator instead of making it wait for the launch timeout.
	defer ready.Close()
	return ops.launchTask(socket, "sandbox:"+sid, filepath.Join(runRoot, sid, sid+".pid"), ready, vmmCgroup)
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
	// pre-exec step; launchTask clears the bit only for the final sandbox-ctl exec.
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
