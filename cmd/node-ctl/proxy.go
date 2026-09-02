package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os/signal"
	"syscall"

	publicconfig "github.com/kuasar-sandbox/orchestrator/config"
	"github.com/kuasar-sandbox/orchestrator/internal/componentexec"
	"github.com/kuasar-sandbox/orchestrator/internal/configresolve"
	"github.com/kuasar-sandbox/orchestrator/internal/proxyapp"
)

// runProxy is the node-ctl entry for the independent Proxy master. Workers are
// private re-executions detected before CLI parsing in main.
func runProxy(args []string, logger *slog.Logger) error {
	flags := flag.NewFlagSet("proxy serve", flag.ContinueOnError)
	configPath := flags.String("config", "/etc/node-ctl/proxy.yaml", "proxy config file")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("proxy serve: unexpected arguments")
	}
	cfg, err := publicconfig.LoadProxy(*configPath)
	if err != nil {
		return err
	}
	if cfg.Paths.ProxyExecutable != "" {
		executables, err := configresolve.CurrentExecutables()
		if err != nil {
			return err
		}
		return componentexec.Exec(
			componentexec.ComponentProxy,
			componentexec.RoleMaster,
			executables.OrchestratorCtl(),
			cfg.Paths.ProxyExecutable,
			cfg,
		)
	}
	if err := publicconfig.ValidateProxyFinal(cfg); err != nil {
		return err
	}
	effective, err := proxyapp.FreezeConfig(cfg)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	runtime, err := proxyapp.ResolveRuntime(ctx, effective.Config(), proxyapp.Bindings{Logger: logger})
	if err != nil {
		return err
	}
	return proxyapp.RunMaster(ctx, effective, runtime)
}
