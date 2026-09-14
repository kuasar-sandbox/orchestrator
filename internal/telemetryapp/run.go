package telemetryapp

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"reflect"
	"time"

	"github.com/kuasar-sandbox/orchestrator/app/telemetry/extension"
	"github.com/kuasar-sandbox/orchestrator/config"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/telemetry"
	"golang.org/x/net/netutil"
)

type extensionHost struct{ reader extension.Reader }

func (h extensionHost) Reader() extension.Reader { return h.reader }

func Run(ctx context.Context, cfg *config.Telemetry, runtime *Runtime) (resultErr error) {
	if ctx == nil || cfg == nil || runtime == nil || runtime.Logger == nil {
		return errors.New("telemetry: resolved config, context and runtime required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := config.ValidateTelemetryFinal(cfg); err != nil {
		return err
	}
	cfg = cfg.Clone()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	shutdown := func(operation func(context.Context) error) {
		cancel() // Startup failures also cancel retained extension/receiver work.
		shutdownCtx, stop := context.WithTimeout(context.Background(), 20*time.Second)
		defer stop()
		resultErr = errors.Join(resultErr, operation(shutdownCtx))
	}
	var backend extension.Storage
	var err error
	switch cfg.Telemetry.Storage.Type {
	case "local":
		var local *telemetry.Local
		local, err = telemetry.OpenLocal(cfg.Telemetry.Storage, runtime.Logger)
		if local != nil {
			backend = local
		}
	case "custom":
		if runtime.Storage == nil {
			return errors.New("telemetry: custom storage provider missing")
		}
		backend, err = runtime.Storage(ctx, cfg.Clone().Telemetry.Storage)
		if backend == nil || reflect.ValueOf(backend).Kind() == reflect.Pointer && reflect.ValueOf(backend).IsNil() {
			backend = nil
			err = errors.Join(err, errors.New("telemetry: custom storage returned nil"))
		}
	case "prometheus":
		var remote *telemetry.Prometheus
		remote, err = telemetry.NewPrometheus(cfg.Telemetry.Storage)
		if remote != nil {
			backend = remote
		}
	case "clickhouse":
		var remote *telemetry.ClickHouse
		remote, err = telemetry.OpenClickHouse(ctx, cfg.Telemetry.Storage)
		if remote != nil {
			backend = remote
		}
	case "none":
	default:
		return errors.New("telemetry: unknown primary storage")
	}
	if backend != nil {
		defer func() { shutdown(backend.Shutdown) }()
	}
	if err != nil {
		return err
	}
	var reader extension.Reader
	var storageErrors <-chan error
	if backend != nil {
		reader = backend
		if health, ok := backend.(extension.HealthReporter); ok {
			storageErrors = health.Errors()
		}
	}
	if runtime.Extension != nil {
		defer func() { shutdown(runtime.Extension.Shutdown) }()
		if err := runtime.Extension.Start(ctx, extensionHost{reader: reader}); err != nil {
			return fmt.Errorf("telemetry extension start: %w", err)
		}
	}
	view := telemetry.NewView(cfg.RouteCapacity)
	defer view.InvalidateSync()
	fatal := make(chan error, 1)
	collector, err := telemetry.NewCollector(ctx, *cfg, view, backend, runtime.Collector, runtime.Logger, fatal)
	if err != nil {
		return err
	}
	defer func() { shutdown(collector.Shutdown) }()
	if err := collector.Start(ctx); err != nil {
		return fmt.Errorf("telemetry Collector start: %w", err)
	}
	registration := routesync.Register{Subscribe: &routesync.Subscribe{Kind: routesync.KindRoute}, Telemetry: &routesync.Telemetry{}}
	if reader != nil {
		listener, release, err := listenQuery(cfg.APISocket)
		if err != nil {
			return fmt.Errorf("telemetry query listen: %w", err)
		}
		defer release()
		defer listener.Close()
		server := &http.Server{Handler: telemetry.QueryHandler(reader), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 20 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16 << 10,
			BaseContext: func(net.Listener) context.Context { return ctx }}
		done := make(chan struct{})
		go func() {
			defer close(done)
			if err := server.Serve(netutil.LimitListener(listener, 64)); err != nil && !errors.Is(err, http.ErrServerClosed) && !errors.Is(err, net.ErrClosed) {
				select {
				case fatal <- err:
				default:
				}
			}
		}()
		defer func() {
			shutdown(func(ctx context.Context) error {
				err := server.Shutdown(ctx)
				if err != nil {
					_ = server.Close()
				}
				<-done
				return err
			})
		}()
		registration.Telemetry.API = &routesync.Socket{Path: cfg.APISocket}
	}
	subscriber := routesync.NewSubscriber(func(ctx context.Context) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", cfg.ConfigSocket)
	},
		routesync.TelemetryPluginID, registration, view, nil, runtime.Logger)
	subCtx, stopSubscription := context.WithCancel(ctx)
	subDone := make(chan struct{})
	go func() { defer close(subDone); subscriber.Run(subCtx) }()
	// First revoke the query lease, then drain query/ingress, then Collector,
	// extension, and finally TSDB. Storage remains alive for every consumer.
	defer func() { stopSubscription(); <-subDone }()
	runtime.Logger.Info("telemetry started", "storage", cfg.Telemetry.Storage.Type, "query", reader != nil)
	select {
	case <-ctx.Done():
		return nil
	case err := <-fatal:
		cancel()
		return fmt.Errorf("telemetry component failed: %w", err)
	case err, ok := <-storageErrors:
		cancel()
		if !ok || err == nil {
			err = errors.New("primary storage health reporter stopped")
		}
		return fmt.Errorf("telemetry primary storage failed: %w", err)
	}
}
