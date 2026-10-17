// Command proxy-lifecycle is a prebuilt E2E helper for the public Proxy App.
// Its private management actions exist only in this test program.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"

	"github.com/kuasar-sandbox/orchestrator/app/proxy"
)

var buildIdentity = "initial"

type extension struct{ app *proxy.App }

func (*extension) Start(context.Context, proxy.MasterHost) error { return nil }
func (*extension) Shutdown(context.Context) error                { return nil }
func (e *extension) WrapManagement(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		switch {
		case r.Method == "GET" && r.URL.Path == "/test/build":
			_ = json.NewEncoder(w).Encode(map[string]any{"build": buildIdentity, "pid": os.Getpid()})
		case r.Method == "POST" && r.URL.Path == "/test/restart":
			id, err := e.app.InplaceRestart(r.Context())
			if err != nil {
				http.Error(w, err.Error(), http.StatusConflict)
				return
			}
			fmt.Fprint(w, id)
		case r.Method == "POST" && r.URL.Path == "/test/shutdown":
			w.WriteHeader(http.StatusAccepted)
			w.(http.Flusher).Flush()
			go e.app.Shutdown(context.Background())
		default:
			next.ServeHTTP(w, r)
		}
	})
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	var app *proxy.App
	var once sync.Once
	var restart chan os.Signal
	var directory string
	app = proxy.New(proxy.Hooks{Configure: func(_ context.Context, config *proxy.Config) error {
		directory = filepath.Dir(config.StatsSocket)
		return nil
	}, BindRuntime: func(_ context.Context, process proxy.Process, runtime *proxy.Runtime) error {
		runtime.Logger = slog.Default().With("build", buildIdentity, "role", process.Role)
		if process.Role == proxy.RoleMaster {
			runtime.MasterExtension = &extension{app: app}
			if app.Status().CoordinatorPID == os.Getpid() {
				if buildIdentity == "target" {
					if _, err := os.Stat(filepath.Join(directory, "fail-successor")); err == nil {
						return fmt.Errorf("test-only successor initialization failure")
					}
				}
				once.Do(func() {
					restart = make(chan os.Signal, 1)
					signal.Notify(restart, syscall.SIGUSR1)
					go func() {
						for {
							select {
							case <-restart:
								_, err := app.InplaceRestart(context.Background())
								if err != nil {
									slog.Info("test custom restart trigger", "err", err)
								}
							case <-ctx.Done():
								return
							}
						}
					}()
				})
			}
		}
		return nil
	}})
	err := app.RunContext(ctx)
	if restart != nil {
		signal.Stop(restart)
	}
	if err != nil {
		slog.Error("test Proxy App", "err", err)
		os.Exit(1)
	}
}
