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
	var local *telemetry.Local
	var localErrors <-chan error
	var err error
	if cfg.Local.Enabled {
		local, err = telemetry.OpenLocal(cfg.Local, runtime.Logger)
		if err != nil {
			return err
		}
		defer func() { shutdown(local.Shutdown) }()
		localErrors = local.Errors()
	}
	var backend extension.QueryBackend
	switch cfg.Query.Backend {
	case "local":
		if local == nil {
			return errors.New("telemetry local query backend is not enabled")
		}
		backend = local
	case "custom":
		if runtime.QueryBackend == nil {
			return errors.New("telemetry: custom query provider missing")
		}
		backend, err = runtime.QueryBackend(ctx, cfg.Clone().Query)
		if backend == nil || reflect.ValueOf(backend).Kind() == reflect.Pointer && reflect.ValueOf(backend).IsNil() {
			backend = nil
			err = errors.Join(err, errors.New("telemetry: custom query backend returned nil"))
		}
	case "prometheus":
		var remote *telemetry.Prometheus
		remote, err = telemetry.NewPrometheus(cfg.Query)
		if remote != nil {
			backend = remote
		}
	case "clickhouse":
		var remote *telemetry.ClickHouse
		remote, err = telemetry.NewClickHouse(cfg.Query)
		if remote != nil {
			backend = remote
		}
	case "none":
	default:
		return errors.New("telemetry: unknown query backend")
	}
	if backend != nil && cfg.Query.Backend != "local" {
		defer func() { shutdown(backend.Shutdown) }()
	}
	if err != nil {
		return err
	}
	var reader extension.Reader
	var queryErrors <-chan error
	if backend != nil {
		reader = backend
		if health, ok := backend.(extension.HealthReporter); ok && cfg.Query.Backend != "local" {
			queryErrors = health.Errors()
		}
	}
	if cfg.Collector == nil && reader == nil {
		return errors.New("telemetry requires Collector configuration or a query backend")
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
	if cfg.Collector != nil {
		// Do not pass a typed nil local pointer into the exporter interface.
		var writer interface {
			Write(context.Context, []extension.Sample) error
		}
		if local != nil {
			writer = local
		}
		collector, err := telemetry.NewCollector(ctx, *cfg, view, writer, runtime.Collector, runtime.Logger, fatal)
		if err != nil {
			return err
		}
		defer func() { shutdown(collector.Shutdown) }()
		if err := collector.Start(ctx); err != nil {
			return fmt.Errorf("telemetry Collector start: %w", err)
		}
	}
	var handler extension.MetricsHandler
	switch cfg.Query.Handler {
	case "e2b":
		handler = telemetry.E2BHandler(cfg.Query.E2B)
	case "custom":
		handler = runtime.MetricsHandler
	}
	registration := routesync.Register{Subscribe: &routesync.Subscribe{Kind: routesync.KindRoute}, Telemetry: &routesync.Telemetry{}}
	if reader != nil && handler != nil {
		listener, release, err := listenQuery(cfg.APISocket)
		if err != nil {
			return fmt.Errorf("telemetry query listen: %w", err)
		}
		defer release()
		defer listener.Close()
		server := &http.Server{Handler: telemetry.QueryHandler(reader, handler), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 20 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16 << 10,
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
	runtime.Logger.Info("telemetry started", "query_backend", cfg.Query.Backend, "query_handler", cfg.Query.Handler, "local", local != nil, "collector", cfg.Collector != nil)
	select {
	case <-ctx.Done():
		return nil
	case err := <-fatal:
		cancel()
		return fmt.Errorf("telemetry component failed: %w", err)
	case err, ok := <-localErrors:
		cancel()
		if !ok || err == nil {
			err = errors.New("local TSDB health reporter stopped")
		}
		return fmt.Errorf("telemetry local TSDB failed: %w", err)
	case err, ok := <-queryErrors:
		cancel()
		if !ok || err == nil {
			err = errors.New("query backend health reporter stopped")
		}
		return fmt.Errorf("telemetry query backend failed: %w", err)
	}
}
