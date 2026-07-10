package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"path/filepath"

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
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
	if *socket == "" || *runID == "" {
		return fmt.Errorf("run-sandbox: --config-socket and --run-id required")
	}
	if *pidfile != "" {
		if err := lockPidfile(*pidfile); err != nil {
			return err
		}
	}
	sid, err := configsock.WaitAssignment(context.Background(), *socket, "sandbox", *runID)
	if err != nil {
		return fmt.Errorf("wait assignment: %w", err)
	}
	if *pidfile == "" {
		return fmt.Errorf("run-sandbox: --pidfile required for sandbox assignment")
	}
	runRoot := filepath.Dir(filepath.Dir(*pidfile))
	return launchTask(*socket, "sandbox:"+sid, filepath.Join(runRoot, sid, sid+".pid"))
}
