package main

import (
	"flag"
	"fmt"
	"log/slog"
)

// runBuilder is the ExecStart of sandbox-builder@<bid>.service: it fetches the
// build's LaunchSpec (exec=flatten-ctl, MANIFEST_KEY + registry creds in env) over the
// config-socket and exec-replaces into flatten-ctl. Flags fall back to the TASK_* env;
// TASK_* are stripped from the child environment. Kept separate from run-sandbox so the
// build path can grow its own orchestration (multi-step builds) without touching it.
//
//	orchestrator-ctl run-builder --pidfile=<f> --config-socket=<uds> --build-id=<bid>
func runBuilder(args []string, _ *slog.Logger) error {
	fs := flag.NewFlagSet("run-builder", flag.ExitOnError)
	pidfile := fs.String("pidfile", "", "pidfile to lock+write (TASK_PIDFILE)")
	socket := fs.String("config-socket", "", "config-socket UDS (TASK_CONFIG_SOCKET)")
	bid := fs.String("build-id", "", "build id (TASK_BUILD_ID)")
	_ = fs.Parse(args)
	envDefault(pidfile, "TASK_PIDFILE")
	envDefault(socket, "TASK_CONFIG_SOCKET")
	envDefault(bid, "TASK_BUILD_ID")
	if *socket == "" || *bid == "" {
		return fmt.Errorf("run-builder: --config-socket and --build-id required")
	}
	return launchTask(*socket, "build:"+*bid, *pidfile)
}
