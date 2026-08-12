package main

import (
	"context"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/proxystats"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

type externalTrafficProvider struct {
	plugins *configsock.Registry
}

func (p *externalTrafficProvider) SandboxTrafficStats(ctx context.Context, sandboxID, runID string, profile types.Profile, _ types.State) (*api.TrafficStats, error) {
	if p == nil || p.plugins == nil {
		return nil, api.ErrStatsUnavailable
	}
	socket, found := p.plugins.ProxyStatsTarget()
	if !found {
		return nil, api.ErrStatsUnavailable
	}
	results, err := proxystats.QuerySocket(ctx, socket, []proxystats.TrafficQuery{{
		SandboxID: sandboxID, RunID: runID, Profile: profile,
	}})
	if err != nil {
		return nil, err
	}
	if len(results) != 1 || results[0].SandboxID != sandboxID || results[0].Stats == nil {
		return nil, api.ErrStatsUnavailable
	}
	return results[0].Stats, nil
}
