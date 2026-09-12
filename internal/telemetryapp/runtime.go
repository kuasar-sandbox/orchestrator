package telemetryapp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"os"

	"github.com/kuasar-sandbox/orchestrator/app/telemetry/extension"
	customotel "github.com/kuasar-sandbox/orchestrator/app/telemetry/otel"
	"github.com/kuasar-sandbox/orchestrator/config"
)

type Bindings struct {
	Logger          *slog.Logger
	Extension       extension.Extension
	Storage         func(context.Context, config.TelemetryStorage) (extension.Storage, error)
	ExporterHeaders func(context.Context, string) (map[string]string, error)
	StorageHeaders  func(context.Context) (map[string]string, error)
	Collector       customotel.Components
}

type Runtime struct{ Bindings }

func ResolveRuntime(ctx context.Context, cfg *config.Telemetry, bindings Bindings) (*Runtime, error) {
	if cfg == nil {
		return nil, errors.New("telemetry runtime: config is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if bindings.Logger == nil {
		bindings.Logger = slog.New(slog.NewTextHandler(os.Stderr, nil))
	}
	if (cfg.Telemetry.Storage.Type == "custom") != (bindings.Storage != nil) {
		return nil, errors.New("telemetry custom primary storage requires exactly one Storage provider and storage.type=custom")
	}
	bindings.Collector = bindings.Collector.Clone()
	if bindings.StorageHeaders != nil {
		headers, err := bindings.StorageHeaders(ctx)
		if err != nil {
			return nil, fmt.Errorf("telemetry primary storage credentials: %w", err)
		}
		switch cfg.Telemetry.Storage.Type {
		case "prometheus":
			cfg.Telemetry.Storage.Prometheus.Headers = maps.Clone(headers)
		case "clickhouse":
			cfg.Telemetry.Storage.ClickHouse.Headers = maps.Clone(headers)
		default:
			return nil, errors.New("StorageHeaders requires a built-in external primary storage")
		}
	}
	if bindings.ExporterHeaders != nil {
		for i := range cfg.Telemetry.Exporters {
			headers, err := bindings.ExporterHeaders(ctx, cfg.Telemetry.Exporters[i].Name)
			if err != nil {
				return nil, fmt.Errorf("telemetry exporter credentials: %w", err)
			}
			// Provider is authoritative, including an intentionally empty map.
			cfg.Telemetry.Exporters[i].Headers = maps.Clone(headers)
		}
	}
	if cfg.Telemetry.Storage.Type == "none" && len(cfg.Telemetry.Exporters) == 0 && len(bindings.Collector.Exporters) == 0 {
		return nil, errors.New("telemetry requires a primary storage or an exporter")
	}
	if err := config.ValidateTelemetryFinal(cfg); err != nil {
		return nil, err
	}
	return &Runtime{Bindings: bindings}, nil
}
