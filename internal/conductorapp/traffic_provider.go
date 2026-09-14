package conductorapp

import (
	"context"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/proxystats"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// ExternalTrafficProvider reads traffic snapshots from the currently
// registered independent proxy master.
type ExternalTrafficProvider struct {
	Plugins *configsock.Registry
}

func (p *ExternalTrafficProvider) SandboxTrafficStats(ctx context.Context, sandboxID, runID string, profile types.Profile, state types.State) (*api.TrafficStats, error) {
	results, err := p.SandboxTrafficStatsBatch(ctx, []proxystats.TrafficQuery{{
		SandboxID: sandboxID, RunID: runID, Profile: profile, State: state,
	}})
	if err != nil {
		return nil, err
	}
	return results[0].Stats, nil
}

func (p *ExternalTrafficProvider) SandboxTrafficStatsBatch(ctx context.Context, queries []proxystats.TrafficQuery) ([]proxystats.BatchResult, error) {
	if p == nil || p.Plugins == nil {
		return nil, api.ErrStatsUnavailable
	}
	socket, found := p.Plugins.ProxyStatsTarget()
	if !found {
		return nil, api.ErrStatsUnavailable
	}
	return proxystats.QuerySocket(ctx, socket, queries)
}
