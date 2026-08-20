package main

import (
	"context"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
)

// setupTracing installs the process tracer provider when endpoint is configured.
// Tracing is intentionally fail-open: the conductor remains available if a
// collector is unavailable during setup, export, or shutdown.
func setupTracing(ctx context.Context, endpoint string, log *slog.Logger) func() {
	if endpoint == "" {
		return func() {}
	}

	exporter, err := otlptracehttp.New(ctx, otlptracehttp.WithEndpointURL(endpoint))
	if err != nil {
		log.Warn("OpenTelemetry tracing disabled: create OTLP/HTTP exporter", "err", err)
		return func() {}
	}
	provider := trace.NewTracerProvider(
		trace.WithBatcher(exporter),
		trace.WithResource(resource.NewWithAttributes(
			"",
			attribute.String("service.name", "kuasar_sandbox"),
		)),
	)
	otel.SetTracerProvider(provider)
	log.Info("OpenTelemetry tracing enabled", "otlp_endpoint", endpoint)
	return func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := provider.Shutdown(shutdownCtx); err != nil {
			log.Warn("OpenTelemetry tracing shutdown", "err", err)
		}
	}
}

// traceHTTPHandler wraps only the public API handler. The local config socket
// deliberately uses the raw API handler and the data plane is not instrumented.
func traceHTTPHandler(handler http.Handler) http.Handler {
	// http.ServeMux exposes the matched route pattern. Add it before handing
	// off to otelhttp so span and metric attributes use the route template,
	// rather than paths containing tenant sandbox or template IDs.
	if mux, ok := handler.(*http.ServeMux); ok {
		handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, route := mux.Handler(r)
			if _, route, ok = strings.Cut(route, " "); !ok {
				route = ""
			}
			if route != "" {
				otelhttp.WithRouteTag(route, mux).ServeHTTP(w, r)
				return
			}
			mux.ServeHTTP(w, r)
		})
	}
	return otelhttp.NewHandler(handler, "node-ctl.e2b.api")
}
