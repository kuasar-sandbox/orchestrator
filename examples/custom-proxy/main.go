// Command custom-proxy is the minimal statically customized external proxy.
// It must be selected by paths.proxy_executable and invoked through
// `node-ctl proxy serve`; direct execution is rejected by the App.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/kuasar-sandbox/orchestrator/app/proxy"
)

// masterExtension is trusted, statically linked code. A private deployment can
// compose its own modules behind this one object.
type masterExtension struct {
	log  *slog.Logger
	host proxy.MasterHost
}

// workerExtension is a fresh object in each worker epoch. Its authentication
// rule is deliberately an obvious placeholder, not production authorization.
type workerExtension struct {
	log           *slog.Logger
	host          proxy.WorkerHost
	registrations sync.Map
}

type privateRegistration struct {
	SandboxID string
	Port      int
	Revision  uint64
}

type privateRegistrationKey struct {
	SandboxID string
	Port      int
}

func (e *workerExtension) Start(_ context.Context, host proxy.WorkerHost) error {
	e.host = host
	e.log.Info("worker extension started", "worker", host.Process().WorkerID, "epoch", host.Process().WorkerEpoch)
	return nil
}

func (e *workerExtension) WrapIngress(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sandboxID, port, guestPath, matched, err := privateIngressTarget(r)
		if !matched {
			next.ServeHTTP(w, r)
			return
		}
		if err != nil {
			http.Error(w, "invalid private target", http.StatusBadRequest)
			return
		}
		// Demonstration only. A production deployment must verify a real private
		// credential, bind it to the target, and avoid static shared strings.
		if r.Header.Get("X-Private-Authorization") != "example-only" {
			http.Error(w, "private authentication failed", http.StatusUnauthorized)
			return
		}
		registration, found := e.getRegistration(sandboxID, port)
		if !found {
			http.NotFound(w, r)
			return
		}
		e.host.ForwardAuthorized(w, r, proxy.ForwardRequest{
			SandboxID: registration.SandboxID,
			Target: proxy.ConnectTarget{
				Service: proxy.ConnectServiceForward,
				Port:    registration.Port,
			},
			Revalidate: func(context.Context) error {
				current, found := e.getRegistration(registration.SandboxID, registration.Port)
				if !found || current.Revision != registration.Revision {
					return errors.New("private registration revision changed")
				}
				return nil
			},
			Rewrite: func(outbound *http.Request) error {
				outbound.Header.Del("X-Private-Authorization")
				outbound.Header.Del("X-Sandbox-Id")
				outbound.Header.Del("X-Sandbox-Port")
				if guestPath != "" {
					outbound.URL.Path = guestPath
				}
				return nil
			},
		})
	})
}

func (e *workerExtension) getRegistration(sandboxID string, port int) (privateRegistration, bool) {
	view, found := e.host.GetRoute(sandboxID)
	if !found {
		return privateRegistration{}, false
	}
	key := privateRegistrationKey{SandboxID: sandboxID, Port: port}
	if current, loaded := e.registrations.Load(key); loaded {
		return current.(privateRegistration), true
	}
	// A real deployment would populate and revise this worker-local registry
	// from its private authority. The example seeds a stable demo revision from
	// the first core route observation so lifecycle route revisions remain
	// independent from the private registration fence.
	registration := privateRegistration{SandboxID: sandboxID, Port: port, Revision: view.Revision}
	current, _ := e.registrations.LoadOrStore(key, registration)
	return current.(privateRegistration), true
}

func privateIngressTarget(r *http.Request) (sandboxID string, port int, guestPath string, matched bool, err error) {
	if sandboxID = r.Header.Get("X-Sandbox-Id"); sandboxID != "" {
		port, err = strconv.Atoi(r.Header.Get("X-Sandbox-Port"))
		if err != nil || port < 1 || port > 65535 {
			return "", 0, "", true, fmt.Errorf("invalid private port")
		}
		return sandboxID, port, "", true, nil
	}
	const prefix = "/private/sandboxes/"
	if !strings.HasPrefix(r.URL.Path, prefix) {
		return "", 0, "", false, nil
	}
	parts := strings.SplitN(strings.TrimPrefix(r.URL.Path, prefix), "/", 3)
	if len(parts) < 2 || parts[0] == "" {
		return "", 0, "", true, fmt.Errorf("invalid private path")
	}
	port, err = strconv.Atoi(parts[1])
	if err != nil || port < 1 || port > 65535 {
		return "", 0, "", true, fmt.Errorf("invalid private port")
	}
	guestPath = "/"
	if len(parts) == 3 && parts[2] != "" {
		guestPath += parts[2]
	}
	return parts[0], port, guestPath, true, nil
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
			} else if process.Role == proxy.RoleWorker {
				runtime.WorkerExtension = &workerExtension{log: runtime.Logger}
			}
			return nil
		},
	})
	if err := app.Run(); err != nil {
		slog.Error("custom proxy", "err", err)
		os.Exit(1)
	}
}
