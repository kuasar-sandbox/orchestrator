package main

import (
	"context"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/conductorapp"
	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/proxystats"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

type externalTrafficProvider struct {
	plugins *configsock.Registry
}

func (p *externalTrafficProvider) SandboxTrafficStatsBatch(ctx context.Context, queries []proxystats.TrafficQuery) ([]proxystats.BatchResult, error) {
	var plugins *configsock.Registry
	if p != nil {
		plugins = p.plugins
	}
	return (&conductorapp.ExternalTrafficProvider{Plugins: plugins}).SandboxTrafficStatsBatch(ctx, queries)
}

func (p *externalTrafficProvider) SandboxTrafficStats(ctx context.Context, sandboxID, runID string, profile types.Profile, state types.State) (*api.TrafficStats, error) {
	var plugins *configsock.Registry
	if p != nil {
		plugins = p.plugins
	}
	return (&conductorapp.ExternalTrafficProvider{Plugins: plugins}).SandboxTrafficStats(ctx, sandboxID, runID, profile, state)
}
