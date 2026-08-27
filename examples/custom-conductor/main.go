// Command custom-conductor is the minimal statically customized conductor.
// It must be selected by paths.conductor_executable and invoked through
// `node-ctl conductor serve`; direct execution is rejected by the App.
package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/kuasar-sandbox/orchestrator/app/conductor"
)

func main() {
	app := conductor.New(conductor.Hooks{
		Configure: func(_ context.Context, cfg *conductor.Config, runtime *conductor.Runtime) error {
			// Apply startup-only declarative overrides here. Process-local PKI,
			// encryption-key, and object-store providers bind through runtime.
			// This minimal example keeps the node-ctl supplied configuration.
			runtime.Logger = slog.Default()
			return nil
		},
	})
	if err := app.Run(); err != nil {
		slog.Error("custom conductor", "err", err)
		os.Exit(1)
	}
}
