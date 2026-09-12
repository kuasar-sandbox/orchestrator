package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os/signal"
	"syscall"

	"github.com/kuasar-sandbox/orchestrator/config"
	"github.com/kuasar-sandbox/orchestrator/internal/componentexec"
	"github.com/kuasar-sandbox/orchestrator/internal/configresolve"
	"github.com/kuasar-sandbox/orchestrator/internal/telemetryapp"
)

func telemetryCmd(args []string, logger *slog.Logger) error {
	if len(args) < 1 || args[0] != "serve" {
		return fmt.Errorf("usage: node-ctl telemetry serve [--config <telemetry.yaml>]")
	}
	flags := flag.NewFlagSet("telemetry serve", flag.ContinueOnError)
	path := flags.String("config", "/etc/node-ctl/telemetry.yaml", "telemetry config file")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("telemetry serve: unexpected arguments")
	}
	cfg, err := config.LoadTelemetry(*path)
	if err != nil {
		return err
	}
	if cfg.Paths.TelemetryExecutable != "" {
		executables, err := configresolve.CurrentExecutables()
		if err != nil {
			return err
		}
		return componentexec.Exec(componentexec.ComponentTelemetry, componentexec.RoleTelemetry, executables.OrchestratorCtl(), cfg.Paths.TelemetryExecutable, cfg)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	frozen := cfg.Clone()
	runtime, err := telemetryapp.ResolveRuntime(ctx, frozen, telemetryapp.Bindings{Logger: logger})
	if err != nil {
		return err
	}
	return telemetryapp.Run(ctx, frozen, runtime)
}
