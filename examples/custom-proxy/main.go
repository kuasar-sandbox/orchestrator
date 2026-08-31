// Command custom-proxy is the minimal statically customized external proxy.
// It must be selected by paths.proxy_executable and invoked through
// `node-ctl proxy serve`; direct execution is rejected by the App.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"

	"github.com/kuasar-sandbox/orchestrator/app/proxy"
)

// masterExtension is trusted, statically linked code. A private deployment can
// compose its own modules behind this one object.
type masterExtension struct {
	log  *slog.Logger
	host proxy.MasterHost
}

func (e *masterExtension) Start(ctx context.Context, host proxy.MasterHost) error {
	e.host = host
	go func() {
		err := host.Routes().Watch(ctx, func(event proxy.RouteEvent) error {
			e.log.Info("route observation", "kind", event.Kind, "generation", event.Generation, "sandbox_id", event.SandboxID)
			return nil
		})
		if err != nil && !errors.Is(err, context.Canceled) {
			e.log.Error("route watch stopped", "err", err)
		}
	}()
	return nil
}

func (e *masterExtension) WrapManagement(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/private/route" {
			next.ServeHTTP(w, r)
			return
		}
		view, found, err := e.host.Routes().Get(r.Context(), r.URL.Query().Get("sandbox_id"))
		if err != nil {
			http.Error(w, "route lookup unavailable", http.StatusServiceUnavailable)
			return
		}
		if !found {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(view)
	})
}

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
			if process.Role == proxy.RoleMaster {
				runtime.MasterExtension = &masterExtension{log: runtime.Logger}
			}
			return nil
		},
	})
	if err := app.Run(); err != nil {
		slog.Error("custom proxy", "err", err)
		os.Exit(1)
	}
}
