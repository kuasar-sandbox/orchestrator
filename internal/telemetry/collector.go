package telemetry

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/kuasar-sandbox/orchestrator/app/telemetry/extension"
	customotel "github.com/kuasar-sandbox/orchestrator/app/telemetry/otel"
	"github.com/kuasar-sandbox/orchestrator/config"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configopaque"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/confmap/xconfmap"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/exporter/otlphttpexporter"
	"go.opentelemetry.io/collector/pipeline"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/service"
	"go.opentelemetry.io/collector/service/pipelines"
	servicetelemetry "go.opentelemetry.io/collector/service/telemetry"
	"go.uber.org/zap"
)

// NewCollector assembles an actual Collector service, not a parallel pipeline.
// Its exported handle exposes only the standard service lifecycle. Core owns
// every edge; customization is inserted strictly between the identity guards.
func NewCollector(ctx context.Context, cfg config.Telemetry, view *View, backend extension.Storage, custom customotel.Components, logger *slog.Logger, fatal chan error) (*service.Service, error) {
	report := func(err error) {
		select {
		case fatal <- err:
		default:
		}
	}
	settings := service.Settings{
		BuildInfo:     component.BuildInfo{Command: "node-ctl telemetry", Description: "Sandbox telemetry Collector", Version: "0.2.0-dev"},
		CollectorConf: confmap.New(), AsyncErrorChannel: fatal,
		ReceiversConfigs: make(map[component.ID]component.Config), ReceiversFactories: make(map[component.Type]receiver.Factory),
		ProcessorsConfigs: make(map[component.ID]component.Config), ProcessorsFactories: make(map[component.Type]processor.Factory),
		ExportersConfigs: make(map[component.ID]component.Config), ExportersFactories: make(map[component.Type]exporter.Factory),
		TelemetryFactory: servicetelemetry.NewFactory(func() component.Config { return &emptyConfig{} },
			servicetelemetry.WithCreateLogger(func(context.Context, servicetelemetry.LoggerSettings, component.Config) (*zap.Logger, component.ShutdownFunc, error) {
				return zap.New(&slogCore{logger: logger}), func(context.Context) error { return nil }, nil
			})),
	}
	graph := &pipelines.PipelineConfig{}
	addReceiver := func(factory receiver.Factory) {
		id := component.NewID(factory.Type())
		settings.ReceiversFactories[factory.Type()] = factory
		settings.ReceiversConfigs[id] = factory.CreateDefaultConfig()
		graph.Receivers = append(graph.Receivers, id)
	}
	addProcessor := func(factory processor.Factory, configure func(component.Config) error) error {
		if factory == nil {
			return errors.New("nil custom processor factory")
		}
		if _, exists := settings.ProcessorsFactories[factory.Type()]; exists {
			return fmt.Errorf("duplicate processor type %s", factory.Type())
		}
		id := component.NewID(factory.Type())
		cfg := factory.CreateDefaultConfig()
		if configure != nil {
			if err := configure(cfg); err != nil {
				return err
			}
		}
		if err := xconfmap.Validate(cfg); err != nil {
			return err
		}
		settings.ProcessorsFactories[factory.Type()] = factory
		settings.ProcessorsConfigs[id] = cfg
		graph.Processors = append(graph.Processors, id)
		return nil
	}
	addExporter := func(factory exporter.Factory, name string, cfg component.Config, repeatedBuiltin bool) error {
		if factory == nil {
			return errors.New("nil custom exporter factory")
		}
		if _, exists := settings.ExportersFactories[factory.Type()]; exists && !repeatedBuiltin {
			return fmt.Errorf("duplicate exporter type %s", factory.Type())
		}
		id := component.NewIDWithName(factory.Type(), name)
		if _, exists := settings.ExportersConfigs[id]; exists {
			return fmt.Errorf("duplicate exporter %s", id)
		}
		if err := xconfmap.Validate(cfg); err != nil {
			return err
		}
		settings.ExportersFactories[factory.Type()] = factory
		settings.ExportersConfigs[id] = cfg
		graph.Exporters = append(graph.Exporters, id)
		return nil
	}
	addReceiver(envdFactory(view, cfg.Telemetry.Scrape, logger))
	if *cfg.Telemetry.OTLP.Enabled {
		addReceiver(otlpFactory(view, cfg, report))
	}
	if err := addProcessor(identityFactory(view, false), nil); err != nil {
		return nil, err
	}
	for _, entry := range custom.Processors {
		// Reserve the final guard's type before processing custom factories.
		if entry.Factory != nil && entry.Factory.Type() == component.MustNewType("sandboxidentityguard") {
			return nil, errors.New("cannot replace core identity guard")
		}
		if err := addProcessor(entry.Factory, entry.Configure); err != nil {
			return nil, err
		}
	}
	if err := addProcessor(identityFactory(view, true), nil); err != nil {
		return nil, err
	}
	if backend != nil {
		factory := primaryFactory(backend)
		if err := addExporter(factory, "", factory.CreateDefaultConfig(), false); err != nil {
			return nil, err
		}
	}
	for _, destination := range cfg.Telemetry.Exporters {
		factory := otlphttpexporter.NewFactory()
		exporterConfig := factory.CreateDefaultConfig().(*otlphttpexporter.Config)
		exporterConfig.ClientConfig.Endpoint = destination.Endpoint
		exporterConfig.ClientConfig.Timeout = 10 * time.Second
		exporterConfig.ClientConfig.Headers = nil
		for key, value := range destination.Headers {
			exporterConfig.ClientConfig.Headers.Set(key, configopaque.String(value))
		}
		queue := exporterhelper.NewDefaultQueueConfig()
		if err := queue.Sizer.UnmarshalText([]byte("bytes")); err != nil {
			return nil, err
		}
		queue.QueueSize = 16 << 20
		queue.NumConsumers = 2
		exporterConfig.QueueConfig = configoptional.Some(queue)
		exporterConfig.RetryConfig.MaxElapsedTime = 30 * time.Second
		if err := addExporter(factory, destination.Name, exporterConfig, true); err != nil {
			return nil, err
		}
	}
	for _, entry := range custom.Exporters {
		if entry.Factory == nil {
			return nil, errors.New("nil custom exporter factory")
		}
		if entry.Factory.Type() == component.MustNewType("sandboxstorage") {
			return nil, errors.New("cannot replace core storage exporter")
		}
		exporterConfig := entry.Factory.CreateDefaultConfig()
		if entry.Configure != nil {
			if err := entry.Configure(exporterConfig); err != nil {
				return nil, err
			}
		}
		if err := addExporter(entry.Factory, "", exporterConfig, false); err != nil {
			return nil, err
		}
	}
	if err := graph.Validate(); err != nil {
		return nil, fmt.Errorf("telemetry pipeline: %w", err)
	}
	return service.New(ctx, settings, service.Config{Telemetry: settings.TelemetryFactory.CreateDefaultConfig(),
		Pipelines: pipelines.Config{pipeline.NewID(pipeline.SignalMetrics): graph}})
}
