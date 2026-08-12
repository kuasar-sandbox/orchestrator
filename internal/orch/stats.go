package orch

import (
	"context"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// SandboxResourceProvider is the in-process resource controller view. It must
// return a lock-independent snapshot and never contact envd or activate a
// sandbox.
type SandboxResourceProvider interface {
	SandboxResourceStats(sandboxID string) (api.ResourceStats, bool)
}

type SandboxTrafficProvider interface {
	SandboxTrafficStats(ctx context.Context, sandboxID, runID string, profile types.Profile, state types.State) (*api.TrafficStats, error)
}

// SetSandboxResourceProvider wires the controller hosted by node-ctl serve.
// A nil provider represents the static-cgroup/controller-disabled mode.
func (o *Orchestrator) SetSandboxResourceProvider(provider SandboxResourceProvider) {
	o.resourceStats = provider
}

func (o *Orchestrator) SetSandboxTrafficProvider(provider SandboxTrafficProvider) {
	o.trafficStats = provider
}

// ResourceStats authenticates ownership before consulting node-local controller
// state. It performs no lifecycle operation and therefore cannot Wake/Resume.
func (o *Orchestrator) ResourceStats(ctx context.Context, id, apiKey string) (*api.ResourceStats, error) {
	sb, err := o.st.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if !ownsSandbox(sb, apiKey) {
		return nil, api.ErrNotFound
	}
	if o.resourceStats == nil {
		return nil, api.ErrStatsUnsupported
	}
	if stats, found := o.resourceStats.SandboxResourceStats(sb.ID); found {
		return &stats, nil
	}
	switch sb.State {
	case types.StatePaused:
		return nil, api.ErrStatsConflict
	case types.StateStarting, types.StateRunning:
		return nil, api.ErrStatsUnavailable
	default:
		return nil, api.ErrStatsConflict
	}
}

func (o *Orchestrator) TrafficStats(ctx context.Context, id, apiKey string) (*api.TrafficStats, error) {
	sb, err := o.st.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if !ownsSandbox(sb, apiKey) {
		return nil, api.ErrNotFound
	}
	if o.trafficStats == nil {
		return nil, api.ErrStatsUnsupported
	}
	return o.trafficStats.SandboxTrafficStats(ctx, sb.ID, sb.RunID, sb.Profile, sb.State)
}
