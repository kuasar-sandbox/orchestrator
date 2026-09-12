// Package telemetry exposes the one-shot startup API for trusted, statically
// linked telemetry executables. All collection and identity policy stays core-owned.
package telemetry

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os/signal"
	"sync/atomic"
	"syscall"

	"github.com/kuasar-sandbox/orchestrator/app/telemetry/extension"
	customotel "github.com/kuasar-sandbox/orchestrator/app/telemetry/otel"
	"github.com/kuasar-sandbox/orchestrator/config"
	"github.com/kuasar-sandbox/orchestrator/internal/componentexec"
	"github.com/kuasar-sandbox/orchestrator/internal/strictjson"
	"github.com/kuasar-sandbox/orchestrator/internal/telemetryapp"
)

type Config = config.Telemetry

// Runtime bindings never enter the sealed declarative bootstrap. Providers are
// authoritative: errors do not fall back to YAML, local storage, or credentials.
type Runtime struct {
	Logger          *slog.Logger
	Extension       extension.Extension
	Storage         func(context.Context, config.TelemetryStorage) (extension.Storage, error)
	ExporterHeaders func(context.Context, string) (map[string]string, error)
	StorageHeaders  func(context.Context) (map[string]string, error)
	// Collector is optional advanced integration, isolated in app/telemetry/otel.
	Collector customotel.Components
}

func (Runtime) MarshalJSON() ([]byte, error) {
	return nil, errors.New("telemetry Runtime is process-local and cannot be serialized")
}
func (*Runtime) UnmarshalJSON([]byte) error {
	return errors.New("telemetry Runtime is process-local and cannot be deserialized")
}

type Hooks struct {
	Configure func(context.Context, *Config, *Runtime) error
}
type App struct {
	hooks       Hooks
	initialized bool
	ran         atomic.Bool
	receive     func() (*componentexec.Bootstrap, error)
	run         func(context.Context, *config.Telemetry, *telemetryapp.Runtime) error
}

func New(hooks Hooks) *App {
	return &App{hooks: hooks, initialized: true, run: telemetryapp.Run,
		receive: func() (*componentexec.Bootstrap, error) {
			return componentexec.Receive(componentexec.ComponentTelemetry, componentexec.RoleTelemetry)
		}}
}
func (a *App) Run() error {
	if err := a.requireInitialized(); err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	return a.RunContext(ctx)
}
func (a *App) RunContext(ctx context.Context) error {
	if err := a.requireInitialized(); err != nil {
		return err
	}
	if ctx == nil {
		return errors.New("telemetry app context is nil")
	}
	if !a.ran.CompareAndSwap(false, true) {
		return errors.New("telemetry app can only be run once")
	}
	bootstrap, err := a.receive()
	if errors.Is(err, componentexec.ErrNoBootstrap) {
		return errors.New("custom telemetry must be invoked through node-ctl telemetry serve")
	}
	if err != nil {
		return err
	}
	if err := componentexec.VerifyCurrentExecutable(bootstrap.ComponentIdentity); err != nil {
		return err
	}
	var cfg Config
	if err := decodeConfig(bootstrap.Config, &cfg); err != nil {
		return fmt.Errorf("custom telemetry bootstrap config: %w", err)
	}
	executable := cfg.Paths.TelemetryExecutable
	if executable == "" || executable != bootstrap.ComponentExecutable {
		return errors.New("custom telemetry bootstrap executable does not match config")
	}
	bindings := &Runtime{}
	if a.hooks.Configure != nil {
		if err := a.hooks.Configure(ctx, &cfg, bindings); err != nil {
			return fmt.Errorf("custom telemetry configure: %w", err)
		}
	}
	if cfg.Paths.TelemetryExecutable != executable {
		return errors.New("custom telemetry Configure must not change paths.telemetry_executable")
	}
	if err := config.ValidateTelemetryFinal(&cfg); err != nil {
		return err
	}
	frozen := cfg.Clone()
	resolved, err := telemetryapp.ResolveRuntime(ctx, frozen, telemetryapp.Bindings{Logger: bindings.Logger, Extension: bindings.Extension,
		Storage: bindings.Storage, ExporterHeaders: bindings.ExporterHeaders, StorageHeaders: bindings.StorageHeaders, Collector: bindings.Collector.Clone()})
	if err != nil {
		return err
	}
	return a.run(ctx, frozen, resolved)
}
func (a *App) requireInitialized() error {
	if a == nil || !a.initialized {
		return errors.New("telemetry App must be constructed with telemetry.New")
	}
	return nil
}
func decodeConfig(raw []byte, out *Config) error {
	if err := strictjson.RejectDuplicateKeys(raw); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(out); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("trailing JSON value")
		}
		return err
	}
	return nil
}
