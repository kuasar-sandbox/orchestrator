package main

import (
	"flag"
	"fmt"
	"log/slog"
)

// runSandbox is the ExecStart of sandbox-runner@<sid>.service: it fetches the
// sandbox's LaunchSpec (exec=sandbox-ctl, MANIFEST_KEY in env) over the config-socket
// and exec-replaces into sandbox-ctl. Flags fall back to the TASK_* env (systemd %i
// wiring); TASK_* are stripped from the child environment.
//
//	node-ctl run-sandbox --pidfile=<f> --config-socket=<uds> --sandbox-id=<sid>
func runSandbox(args []string, _ *slog.Logger) error {
	fs := flag.NewFlagSet("run-sandbox", flag.ExitOnError)
	pidfile := fs.String("pidfile", "", "pidfile to lock+write (TASK_PIDFILE)")
	socket := fs.String("config-socket", "", "config-socket UDS (TASK_CONFIG_SOCKET)")
	sid := fs.String("sandbox-id", "", "sandbox id (TASK_SANDBOX_ID)")
	_ = fs.Parse(args)
	envDefault(pidfile, "TASK_PIDFILE")
	envDefault(socket, "TASK_CONFIG_SOCKET")
	envDefault(sid, "TASK_SANDBOX_ID")
	if *socket == "" || *sid == "" {
		return fmt.Errorf("run-sandbox: --config-socket and --sandbox-id required")
	}
	return launchTask(*socket, "sandbox:"+*sid, *pidfile)
}
