// Command custom-telemetry is a trusted static App, not a standalone CLI or a
// Go runtime plugin. Select it through paths.telemetry_executable in node-ctl.
package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/kuasar-sandbox/orchestrator/app/telemetry"
	"github.com/kuasar-sandbox/orchestrator/app/telemetry/extension"
)

func main() {
	app := telemetry.New(telemetry.Hooks{Configure: func(_ context.Context, _ *telemetry.Config, runtime *telemetry.Runtime) error {
		runtime.Logger = slog.Default()
		runtime.Extension = &lifecycle{logger: runtime.Logger}
		// Native Collector config providers supply exporter credentials; keep
		// them in the corresponding component config, e.g. ${env:OTLP_TOKEN}.
		bindCollector(runtime)
		return nil
	}})
	if err := app.Run(); err != nil {
		slog.Error("custom telemetry", "err", err)
		os.Exit(1)
	}
}

type lifecycle struct{ logger *slog.Logger }

func (e *lifecycle) Start(ctx context.Context, host extension.Host) error {
	e.logger.Info("private telemetry extension started", "readable_primary", host.Reader() != nil)
	return ctx.Err()
}
func (e *lifecycle) Shutdown(ctx context.Context) error {
	e.logger.Info("private telemetry extension stopped")
	return ctx.Err()
}
