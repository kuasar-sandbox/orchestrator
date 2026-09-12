// Package otel is the opt-in, Collector-specific surface for trusted static
// telemetry builds. Ordinary extensions and primary readers do not import it.
package otel

import (
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/processor"
)

// Components adds metrics processors and fan-out exporters to the core-owned
// service graph. It cannot replace receivers, the two identity guards, or the
// primary storage path. Components must preserve request context: asynchronous
// processors that detach it fail closed at the final identity guard.
// Factories and callbacks are process-local, never bootstrap configuration.
type Components struct {
	Processors []Processor
	Exporters  []Exporter
}

// Configure receives a fresh factory-default configuration during assembly.
// It must not retain/mutate that configuration after returning. Core validates
// it before creating any Collector component. Factory types must be unique.
type Processor struct {
	Factory   processor.Factory
	Configure func(component.Config) error
}

// Exporter only fans out writes; it never supplies a metrics history reader.
// Trusted implementations must preserve the core-enriched sandbox identities.
type Exporter struct {
	Factory   exporter.Factory
	Configure func(component.Config) error
}

func (c Components) Clone() Components {
	return Components{Processors: append([]Processor(nil), c.Processors...), Exporters: append([]Exporter(nil), c.Exporters...)}
}
