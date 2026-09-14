package telemetry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"reflect"

	customotel "github.com/kuasar-sandbox/orchestrator/app/telemetry/otel"
	"github.com/kuasar-sandbox/orchestrator/config"
	"github.com/open-telemetry/opentelemetry-collector-contrib/connector/routingconnector"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/clickhouseexporter"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/prometheusremotewriteexporter"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/healthcheckextension"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/filterprocessor"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/transformprocessor"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/confmap/provider/envprovider"
	"go.opentelemetry.io/collector/confmap/provider/fileprovider"
	"go.opentelemetry.io/collector/confmap/provider/yamlprovider"
	"go.opentelemetry.io/collector/confmap/xconfmap"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/debugexporter"
	"go.opentelemetry.io/collector/exporter/otlpexporter"
	"go.opentelemetry.io/collector/exporter/otlphttpexporter"
	collectorextension "go.opentelemetry.io/collector/extension"
	"go.opentelemetry.io/collector/otelcol"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/batchprocessor"
	"go.opentelemetry.io/collector/processor/memorylimiterprocessor"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/otlpreceiver"
	"go.opentelemetry.io/collector/service"
	"go.opentelemetry.io/collector/service/telemetry/otelconftelemetry"
)

// Collector owns the standard service and native configuration provider. The
// service's graph comes solely from the resolved Collector configuration.
type Collector struct {
	*service.Service
	provider *otelcol.ConfigProvider
}

func (c *Collector) Shutdown(ctx context.Context) error {
	return errors.Join(c.Service.Shutdown(ctx), c.provider.Shutdown(ctx))
}

func factoryMap[T component.Factory](factories []T) (map[component.Type]T, error) {
	for _, factory := range factories {
		value := reflect.ValueOf(factory)
		if !value.IsValid() || value.Kind() == reflect.Pointer && value.IsNil() {
			return nil, errors.New("nil Collector factory")
		}
	}
	return otelcol.MakeFactoryMap(factories...)
}

func collectorFactories(cfg config.Telemetry, view *View, backend sampleWriter, custom customotel.Components, logger *slog.Logger, fatal chan error) (otelcol.Factories, error) {
	report := func(err error) {
		select {
		case fatal <- err:
		default:
		}
	}
	factories := otelcol.Factories{Telemetry: otelconftelemetry.NewFactory()}
	var err error
	factories.Receivers, err = factoryMap(append([]receiver.Factory{
		envdFactory(view, logger), sandboxStatsFactory(view, cfg.ConfigSocket, logger), otlpFactory(view, cfg.ProxyNetNS, report), otlpreceiver.NewFactory(),
	}, custom.Receivers...))
	if err != nil {
		return factories, err
	}
	factories.Processors, err = factoryMap(append([]processor.Factory{
		batchprocessor.NewFactory(), memorylimiterprocessor.NewFactory(), filterprocessor.NewFactory(), transformprocessor.NewFactory(),
	}, custom.Processors...))
	if err != nil {
		return factories, err
	}
	exporters := []exporter.Factory{otlpexporter.NewFactory(), otlphttpexporter.NewFactory(), debugexporter.NewFactory(), prometheusremotewriteexporter.NewFactory(), clickhouseexporter.NewFactory()}
	if backend != nil {
		exporters = append(exporters, localFactory(backend))
	}
	factories.Exporters, err = factoryMap(append(exporters, custom.Exporters...))
	if err != nil {
		return factories, err
	}
	factories.Connectors, err = factoryMap(append([]connector.Factory{routingconnector.NewFactory()}, custom.Connectors...))
	if err != nil {
		return factories, err
	}
	factories.Extensions, err = factoryMap(append([]collectorextension.Factory{healthcheckextension.NewFactory()}, custom.Extensions...))
	return factories, err
}

// NewCollector resolves providers and uses the Collector's own component
// unmarshalling, validation and service assembly for all signals and pipelines.
func NewCollector(ctx context.Context, cfg config.Telemetry, view *View, backend sampleWriter, custom customotel.Components, logger *slog.Logger, fatal chan error) (_ *Collector, resultErr error) {
	factories, err := collectorFactories(cfg, view, backend, custom, logger, fatal)
	if err != nil {
		return nil, err
	}
	raw, err := json.Marshal(cfg.Collector)
	if err != nil {
		return nil, fmt.Errorf("Collector configuration: %w", err)
	}
	provider, err := otelcol.NewConfigProvider(otelcol.ConfigProviderSettings{ResolverSettings: confmap.ResolverSettings{
		URIs: []string{"yaml:" + string(raw)}, DefaultScheme: "env",
		ProviderFactories:  append([]confmap.ProviderFactory{envprovider.NewFactory(), fileprovider.NewFactory(), yamlprovider.NewFactory()}, custom.Providers...),
		ConverterFactories: custom.Converters,
	}})
	if err != nil {
		return nil, err
	}
	defer func() {
		if resultErr != nil {
			resultErr = errors.Join(resultErr, provider.Shutdown(ctx))
		}
	}()
	resolved, err := provider.Get(ctx, factories)
	if err != nil {
		return nil, err
	}
	if err := xconfmap.Validate(resolved); err != nil {
		return nil, fmt.Errorf("Collector validation: %w", err)
	}
	conf := confmap.New()
	if err := conf.Marshal(resolved); err != nil {
		return nil, err
	}
	graph, err := service.New(ctx, service.Settings{
		BuildInfo:     component.BuildInfo{Command: "node-ctl telemetry", Description: "Sandbox telemetry Collector", Version: "0.2.0-dev"},
		CollectorConf: conf, AsyncErrorChannel: fatal,
		ReceiversConfigs: resolved.Receivers, ReceiversFactories: factories.Receivers,
		ProcessorsConfigs: resolved.Processors, ProcessorsFactories: factories.Processors,
		ExportersConfigs: resolved.Exporters, ExportersFactories: factories.Exporters,
		ConnectorsConfigs: resolved.Connectors, ConnectorsFactories: factories.Connectors,
		ExtensionsConfigs: resolved.Extensions, ExtensionsFactories: factories.Extensions,
		TelemetryFactory: factories.Telemetry,
	}, resolved.Service)
	if err != nil {
		return nil, err
	}
	return &Collector{Service: graph, provider: provider}, nil
}
