package orch

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/sandboxer/pkg/ctl"
)

// SandboxResourceProvider projects the node reservation. Its presence is also
// the existing build-phase release fence; native VMM observations never take
// part in that fence or in node resource-control decisions.
type SandboxResourceProvider interface {
	SandboxResourceStats(sandboxID string) (api.ResourceStats, bool)
}

type SandboxTrafficProvider interface {
	SandboxTrafficStats(ctx context.Context, sandboxID, runID string, profile types.Profile, state types.State) (*api.TrafficStats, error)
}

// SetSandboxResourceProvider wires the controller hosted by node-ctl serve.
// A nil provider represents static mode: native VMM stats remain available,
// while no node reservation is fabricated.
func (o *Orchestrator) SetSandboxResourceProvider(provider SandboxResourceProvider) {
	o.resourceStats = provider
}

func (o *Orchestrator) SetSandboxTrafficProvider(provider SandboxTrafficProvider) {
	o.trafficStats = provider
}

// ResourceStats authenticates ownership before the shared native read. It
// performs no lifecycle operation and therefore cannot Wake/Resume.
func (o *Orchestrator) ResourceStats(ctx context.Context, id, apiKey string) (*api.ResourceStats, error) {
	sb, err := o.st.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if !ownsSandbox(sb, apiKey) {
		return nil, api.ErrNotFound
	}
	return o.readResourceStats(ctx, sb)
}

func (o *Orchestrator) readResourceStats(ctx context.Context, sb *types.Sandbox) (*api.ResourceStats, error) {
	if sb.State != types.StateStarting && sb.State != types.StateRunning {
		return nil, api.ErrStatsConflict
	}
	if sb.RunDir == "" {
		return nil, api.ErrStatsUnavailable
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	observed, err := ctl.ReadResourceStats(ctx, filepath.Join(sb.RunDir, "ctl.sock"), sb.ID)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", api.ErrStatsUnavailable, err)
	}
	capacity := float64(observed.CPUCapacity)
	stats := &api.ResourceStats{CPUCapacity: &capacity, CPUAllocatable: &observed.CPUAllocatable,
		MemoryCapacity: &observed.MemoryCapacity, MemoryHeadroom: &observed.MemoryHeadroom,
		MemoryUsed: observed.MemoryUsed, TimestampUnix: observed.TimestampUnix}
	if usec := observed.CPUUsageUsec; usec != nil {
		seconds := json.Number(fmt.Sprintf("%d.%06d", *usec/1000000, *usec%1000000))
		stats.CPUSeconds = &seconds
	}
	if o.resourceStats != nil {
		if reservation, found := o.resourceStats.SandboxResourceStats(sb.ID); found {
			stats.MemoryReserved = reservation.MemoryReserved
		}
	}
	if err := o.statsBindingCurrent(ctx, sb); err != nil {
		return nil, err
	}
	return stats, nil
}

// Keep the existing object/run fence internal. A read racing pause, deletion,
// restart or binding reuse cannot publish the former runtime as the current one.
func (o *Orchestrator) statsBindingCurrent(ctx context.Context, expected *types.Sandbox) error {
	current, err := o.st.Get(ctx, expected.ID)
	if err != nil {
		return err
	}
	if current.RunID != expected.RunID || current.RunDir != expected.RunDir || current.State != expected.State ||
		current.VswitchPort != expected.VswitchPort || current.FloatingIP != expected.FloatingIP || current.CreatedUnix != expected.CreatedUnix {
		return api.ErrStatsUnavailable
	}
	return nil
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
	if sb.State != types.StateStarting && sb.State != types.StateRunning && sb.State != types.StatePaused {
		return nil, api.ErrStatsConflict
	}
	return o.trafficStats.SandboxTrafficStats(ctx, sb.ID, sb.RunID, sb.Profile, sb.State)
}
