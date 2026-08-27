// Command custom-proxy is the minimal statically customized external proxy.
// It must be selected by paths.proxy_executable and invoked through
// `node-ctl proxy serve`; direct execution is rejected by the App.
package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/kuasar-sandbox/orchestrator/app/proxy"
)

func main() {
	app := proxy.New(proxy.Hooks{
		Configure: func(_ context.Context, cfg *proxy.Config) error {
			// Apply master-only declarative overrides. This minimal example keeps
			// the node-ctl supplied configuration.
			return nil
		},
		BindRuntime: func(_ context.Context, process proxy.Process, runtime *proxy.Runtime) error {
			// Rebind process-local logger and TLS/HSM material in the master and
			// every worker. Runtime is never serialized into the worker snapshot.
			runtime.Logger = slog.Default().With("proxy_role", process.Role)
			return nil
		},
	})
	if err := app.Run(); err != nil {
		slog.Error("custom proxy", "err", err)
		os.Exit(1)
	}
}
