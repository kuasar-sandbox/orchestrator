// Package otel exposes Collector factories for trusted static telemetry builds.
// Ordinary query extensions do not need to import Collector component APIs.
package otel

import (
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/exporter"
	collectorextension "go.opentelemetry.io/collector/extension"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/receiver"
)

// Components registers each type once. Instances and all pipeline edges are
// configured with native Collector type/name IDs in declarative configuration.
// Factories are in the trusted deployment domain. Sandbox identity is stamped
// at source acceptance and then carried by pdata through ordinary batch/queues.
// No request context or current-route guard is required by downstream factories.
type Components struct {
	Receivers  []receiver.Factory
	Processors []processor.Factory
	Exporters  []exporter.Factory
	Connectors []connector.Factory
	Extensions []collectorextension.Factory
	Providers  []confmap.ProviderFactory
	Converters []confmap.ConverterFactory
}

func (c Components) Clone() Components {
	return Components{
		Receivers:  append([]receiver.Factory(nil), c.Receivers...),
		Processors: append([]processor.Factory(nil), c.Processors...),
		Exporters:  append([]exporter.Factory(nil), c.Exporters...),
		Connectors: append([]connector.Factory(nil), c.Connectors...),
		Extensions: append([]collectorextension.Factory(nil), c.Extensions...),
		Providers:  append([]confmap.ProviderFactory(nil), c.Providers...),
		Converters: append([]confmap.ConverterFactory(nil), c.Converters...),
	}
}
