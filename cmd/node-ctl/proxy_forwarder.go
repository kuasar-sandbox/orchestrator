package main

import (
	"log/slog"

	"github.com/kuasar-sandbox/orchestrator/internal/conductorapp"
	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/metrics"
)

type proxyForwarder = conductorapp.ProxyForwarder

func newProxyForwarder(registry *configsock.Registry, mx *metrics.M, logger *slog.Logger) *proxyForwarder {
	return conductorapp.NewProxyForwarder(registry, mx, logger)
}
