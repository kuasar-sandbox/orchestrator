package main

// Advanced integration is deliberately isolated from ordinary lifecycle and
// material bindings. Remove this file/binding if private Collector components
// are unnecessary; the ordinary extension contract imports no Collector types.
import (
	"context"

	"github.com/kuasar-sandbox/orchestrator/app/telemetry"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor"
)

func bindCollector(runtime *telemetry.Runtime) {
	factory := processor.NewFactory(component.MustNewType("privatedeployment"), func() component.Config { return &struct{}{} },
		processor.WithMetrics(func(_ context.Context, _ processor.Settings, _ component.Config, next consumer.Metrics) (processor.Metrics, error) {
			metrics, err := consumer.NewMetrics(func(ctx context.Context, data pmetric.Metrics) error {
				for i := 0; i < data.ResourceMetrics().Len(); i++ {
					data.ResourceMetrics().At(i).Resource().Attributes().PutStr("deployment.environment.name", "example")
				}
				// Accepted identity is already stored on each pdata resource;
				// ordinary batch, queue and retry can detach the request context.
				return next.ConsumeMetrics(ctx, data)
			}, consumer.WithCapabilities(consumer.Capabilities{MutatesData: true}))
			return &deploymentProcessor{Metrics: metrics}, err
		}, component.StabilityLevelStable))
	runtime.Collector.Processors = []processor.Factory{factory}
}

type deploymentProcessor struct{ consumer.Metrics }

func (*deploymentProcessor) Start(context.Context, component.Host) error { return nil }
func (*deploymentProcessor) Shutdown(context.Context) error              { return nil }
