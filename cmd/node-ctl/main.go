// Command node-ctl is the single-node, e2b-compatible sandbox orchestrator.
//
//	node-ctl conductor serve --config <conductor.yaml>          # run the node conductor
//	node-ctl proxy serve --config <proxy.yaml>                  # independent data-plane Proxy master
//	node-ctl run-sandbox --pidfile=<f> --config-socket=<uds> --run-id=<rid>
//	node-ctl run-builder --pidfile=<f> --config-socket=<uds> --run-id=<rid>
//	                                                                    # in-unit launchers (not for humans)
//	node-ctl config <conductor|proxy> [--template|--config <f>|--resolve]  # config diagnose / generate
//	node-ctl manifest-key <add|list|remove> ...                 # tenant root-key whitelist (admin socket)
//	node-ctl export-sandbox|import-sandbox ...                  # paused-snapshot egress / ingress
//	node-ctl resource <status|list|drain>                      # node reservation controller inspection (hosted in serve via resource_listen)
//	node-ctl builder status                                    # durable Builder admission status (admin socket)
//	node-ctl version
//
// Templates are built through the e2b API (POST /v3/templates ...), not a CLI.
// The guest runtime image is built by the guest-runtime repo.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	publicproxy "github.com/kuasar-sandbox/orchestrator/app/proxy"
	publicconfig "github.com/kuasar-sandbox/orchestrator/config"
	"github.com/kuasar-sandbox/orchestrator/internal/componentexec"
	"github.com/kuasar-sandbox/orchestrator/internal/conductorapp"
	"github.com/kuasar-sandbox/orchestrator/internal/configresolve"
	"github.com/kuasar-sandbox/orchestrator/internal/proxyapp"
)

var version = "0.2.0-dev"

func main() {
	if proxyapp.WorkerBootstrapPresent() {
		componentexec.ClearEnvironment()
		logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
		if err := publicproxy.New(publicproxy.Hooks{}).Run(); err != nil {
			logger.Error("node-ctl", "cmd", "proxy-worker", "err", err)
			os.Exit(1)
		}
		return
	}
	proxyapp.ClearWorkerEnvironment()
	// Top-level component bootstrap belongs only to a custom App. node-ctl never
	// propagates stale bootstrap state into utility commands or helpers.
	componentexec.ClearEnvironment()
	if len(os.Args) < 2 {
		usage()
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	var err error
	switch os.Args[1] {
	case "conductor":
		err = conductorCmd(os.Args[2:], log)
	case "proxy":
		err = proxyCmd(os.Args[2:], log)
	case "run-sandbox":
		err = runSandbox(os.Args[2:], log)
	case "run-builder":
		err = runBuilder(os.Args[2:], log)
	case "config":
		err = configCmd(os.Args[2:], log)
	case "manifest-key":
		err = manifestKeyCmd(os.Args[2:], log)
	case "export-sandbox":
		err = exportSandboxCmd(os.Args[2:], log)
	case "import-sandbox":
		err = importSandboxCmd(os.Args[2:], log)
	case "resource":
		os.Exit(resourceCmd(os.Args[2:]))
	case "builder":
		os.Exit(builderCmd(os.Args[2:]))
	case "version", "-v", "--version":
		fmt.Println("node-ctl", version)
	default:
		usage()
	}
	if err != nil {
		log.Error("node-ctl", "cmd", os.Args[1], "err", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: node-ctl {conductor|proxy|run-sandbox|run-builder|config|manifest-key|export-sandbox|import-sandbox|resource|builder|version} [args]")
	os.Exit(2)
}

func conductorCmd(args []string, log *slog.Logger) error {
	if len(args) < 1 || args[0] != "serve" {
		return fmt.Errorf("usage: node-ctl conductor serve [--config <conductor.yaml>]")
	}
	return runConductor(args[1:], log)
}

func proxyCmd(args []string, log *slog.Logger) error {
	if len(args) < 1 || args[0] != "serve" {
		return fmt.Errorf("usage: node-ctl proxy serve [--config <proxy.yaml>]")
	}
	return runProxy(args[1:], log)
}

func runConductor(args []string, log *slog.Logger) error {
	flags := flag.NewFlagSet("conductor serve", flag.ExitOnError)
	configPath := flags.String("config", "/etc/node-ctl/conductor.yaml", "config file")
	_ = flags.Parse(args)
	cfg, err := publicconfig.LoadConductor(*configPath)
	if err != nil {
		return err
	}
	if cfg.Paths.ConductorExecutable == "" {
		if err := publicconfig.ValidateConductorFinal(cfg); err != nil {
			return err
		}
	}
	executables, err := configresolve.CurrentExecutables()
	if err != nil {
		return err
	}
	if cfg.Paths.ConductorExecutable != "" {
		return componentexec.Exec(
			componentexec.ComponentConductor,
			componentexec.RoleConductor,
			executables.OrchestratorCtl(),
			cfg.Paths.ConductorExecutable,
			cfg,
		)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	frozenConfig := cfg.Clone()
	runtime, err := conductorapp.ResolveRuntime(ctx, frozenConfig, conductorapp.Bindings{Logger: log})
	if err != nil {
		return err
	}
	return conductorapp.Run(ctx, frozenConfig, executables.OrchestratorCtl(), runtime)
}
