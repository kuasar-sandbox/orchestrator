// Command custom-conductor is the minimal statically customized conductor.
// It must be selected by paths.conductor_executable and invoked through
// `node-ctl conductor serve`; direct execution is rejected by the App.
package main

import (
	"context"
	"errors"
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
			logger := slog.Default()
			runtime.Logger = logger
			runtime.Extension = &objectExtension{logger: logger}
			return nil
		},
	})
	if err := app.Run(); err != nil {
		slog.Error("custom conductor", "err", err)
		os.Exit(1)
	}
}

// objectExtension is the single trusted, statically linked extension object.
// It keeps the Host for other private modules and owns its watcher goroutine.
type objectExtension struct {
	logger *slog.Logger
	host   conductor.Host
}

func (e *objectExtension) Start(ctx context.Context, host conductor.Host) error {
	e.host = host
	go func() {
		err := host.Sandboxes().Watch(ctx, func(event conductor.SandboxEvent) error {
			switch event.Kind {
			case conductor.SandboxSyncBegin, conductor.SandboxSyncEnd:
				e.logger.Info("sandbox watch marker", "kind", event.Kind, "generation", event.Generation)
			case conductor.SandboxUpsert, conductor.SandboxDelete:
				e.logger.Info("sandbox changed", "kind", event.Kind, "sandbox_id", event.SandboxID)
			}
			return nil
		})
		if err != nil && !errors.Is(err, context.Canceled) {
			e.logger.Error("sandbox watch stopped", "err", err)
		}
	}()
	return nil
}
