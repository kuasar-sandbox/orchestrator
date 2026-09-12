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
		// Optional private startup material, never copied back into bootstrap or
		// logged. Production may replace this with a bounded secret-file/provider.
		if authorization, present := os.LookupEnv("TELEMETRY_EXPORTER_AUTHORIZATION"); present {
			runtime.ExporterHeaders = func(ctx context.Context, _ string) (map[string]string, error) {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				return map[string]string{"Authorization": authorization}, nil
			}
		}
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
