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
	Logger         *slog.Logger
	Extension      extension.Extension
	QueryBackend   func(context.Context, config.TelemetryQuery) (extension.QueryBackend, error)
	MetricsHandler extension.MetricsHandler
	QueryHeaders   func(context.Context) (map[string]string, error)
	Collector      customotel.Components
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
	if (cfg.Query.Backend == "custom") != (bindings.QueryBackend != nil) {
		return nil, errors.New("telemetry query.backend=custom requires exactly one QueryBackend provider")
	}
	if (cfg.Query.Handler == "custom") != (bindings.MetricsHandler != nil) {
		return nil, errors.New("query.handler=custom requires exactly one MetricsHandler")
	}
	bindings.Collector = bindings.Collector.Clone()
	if bindings.QueryHeaders != nil {
		headers, err := bindings.QueryHeaders(ctx)
		if err != nil {
			return nil, fmt.Errorf("telemetry query credentials: %w", err)
		}
		switch cfg.Query.Backend {
		case "prometheus":
			cfg.Query.Prometheus.Headers = maps.Clone(headers)
		case "clickhouse":
			cfg.Query.ClickHouse.Headers = maps.Clone(headers)
		default:
			return nil, errors.New("QueryHeaders requires a built-in remote query backend")
		}
	}
	if err := config.ValidateTelemetryFinal(cfg); err != nil {
		return nil, err
	}
	return &Runtime{Bindings: bindings}, nil
}
