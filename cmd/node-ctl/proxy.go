package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/kuasar-sandbox/orchestrator/app/activation"
	"github.com/kuasar-sandbox/orchestrator/internal/listenresource"
	"log/slog"
	"os"
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
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	files, err := activation.Receive()
	if err != nil {
		return err
	}
	defer listenresource.Close(files)
	raw, err := json.Marshal(cfg)
	if err != nil {
		return err
	}
	executable, err := os.Executable()
	if err != nil {
		return err
	}
	prepare := func(ctx context.Context, input proxyapp.ServiceInput) (*proxyapp.Startup, error) {
		var config publicconfig.Proxy
		if err := json.Unmarshal(input.Config, &config); err != nil {
			return nil, err
		}
		effective, err := proxyapp.FreezeConfig(&config)
		if err != nil {
			return nil, err
		}
		runtime, err := proxyapp.ResolveRuntime(ctx, effective.Config(), proxyapp.Bindings{Logger: logger, ListenerFiles: files})
		if err != nil {
			return nil, err
		}
		return &proxyapp.Startup{Effective: effective, Runtime: runtime}, nil
	}
	return proxyapp.RunEntry(ctx, proxyapp.NewService(), proxyapp.ServiceInput{Config: raw, Executable: executable, Native: true, Signals: true}, nil, prepare, proxyapp.RunMaster)
}
