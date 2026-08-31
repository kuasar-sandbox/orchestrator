// Command custom-conductor is the minimal statically customized conductor.
// It must be selected by paths.conductor_executable and invoked through
// `node-ctl conductor serve`; direct execution is rejected by the App.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"time"

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

// WrapAPI adds one private route and otherwise preserves the canonical API.
// An extension may intentionally override core routes too; this example does
// not. Production private authentication must be designed for the deployment.
func (e *objectExtension) WrapAPI(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/private/extension/health" {
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok\n"))
			return
		}
		next.ServeHTTP(w, r)
	})
}

// PrepareSandbox applies a small request policy to new sandboxes. The
// operation is a deep copy; the core normalizes it again after this returns.
func (e *objectExtension) PrepareSandbox(_ context.Context, operation *conductor.SandboxOperation) error {
	if operation.Kind != conductor.SandboxOperationCreate || operation.Create == nil {
		return nil
	}
	if operation.Create.Metadata == nil {
		operation.Create.Metadata = make(map[string]string)
	}
	operation.Create.Metadata["example.extension"] = "custom-conductor"
	if operation.Create.TimeoutSeconds < int((15 * time.Minute).Seconds()) {
		operation.Create.TimeoutSeconds = int((15 * time.Minute).Seconds())
	}
	return nil
}
