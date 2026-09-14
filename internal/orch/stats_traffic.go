package orch

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"

	connector "github.com/kuasar-sandbox/connector/pkg/vswitch"
	conductorextension "github.com/kuasar-sandbox/orchestrator/app/conductor/extension"
	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/proxystats"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

type networkStatsReader interface {
	Stats(context.Context, []int) (*connector.StatsOutput, error)
}

type ingressBatchReader interface {
	SandboxTrafficStatsBatch(context.Context, []proxystats.TrafficQuery) ([]proxystats.BatchResult, error)
}

type trafficNetwork struct {
	platform api.TrafficCounters
	transit  api.TrafficCounters
}

func (o *Orchestrator) acquireStatsSlot(ctx context.Context) (func(), error) {
	select {
	case o.nativeStatsSlots <- struct{}{}:
		return func() { <-o.nativeStatsSlots }, nil
	case <-ctx.Done():
		return nil, fmt.Errorf("%w: %v", api.ErrStatsUnavailable, ctx.Err())
	}
}

// Batch preparation shares the same global budget as section reads. Release
// the slot before connector/Proxy preparation acquires its own slot.
func (o *Orchestrator) lookupTrafficSandboxes(ctx context.Context, ids []string) ([]*types.Sandbox, error) {
	release, err := o.acquireStatsSlot(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	sandboxes := make([]*types.Sandbox, len(ids))
	for i, id := range ids {
		sb, err := o.st.Get(ctx, id)
		if err != nil {
			return nil, fmt.Errorf("%w: traffic object lookup: %v", api.ErrStatsUnavailable, err)
		}
		if sb == nil {
			return nil, api.ErrNotFound
		}
		sandboxes[i] = sb
	}
	return sandboxes, nil
}

// prepareTrafficReads is shared by public single-object and trusted batch
// reads. The node has one configured switch; its ports are read in one bounded
// connector call. Full Proxy batch support is reused when the provider has it.
func (o *Orchestrator) prepareTrafficReads(ctx context.Context, sandboxes []*types.Sandbox) ([]trafficNetwork, []*api.TrafficStats, error) {
	if o.trafficStats == nil {
		return nil, nil, api.ErrStatsUnsupported
	}
	if len(sandboxes) < 1 || len(sandboxes) > conductorextension.MaxStatsSandboxes {
		return nil, nil, api.ErrBadRequest
	}
	queries := make([]proxystats.TrafficQuery, len(sandboxes))
	for i, sb := range sandboxes {
		if sb.State != types.StateStarting && sb.State != types.StateRunning && sb.State != types.StatePaused {
			return nil, nil, api.ErrStatsConflict
		}
		queries[i] = proxystats.TrafficQuery{SandboxID: sb.ID, RunID: sb.RunID, Profile: sb.Profile, State: sb.State}
	}
	network, err := o.readNetworkTraffic(ctx, sandboxes)
	if err != nil {
		return nil, nil, err
	}
	batch, ok := o.trafficStats.(ingressBatchReader)
	if !ok {
		return network, nil, nil
	}
	release, err := o.acquireStatsSlot(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer release()
	results, err := batch.SandboxTrafficStatsBatch(ctx, queries)
	if err != nil {
		return nil, nil, ingressReadError(err)
	}
	if len(results) != len(sandboxes) {
		return nil, nil, api.ErrStatsUnavailable
	}
	ingress := make([]*api.TrafficStats, len(results))
	for i, row := range results {
		if row.SandboxID != sandboxes[i].ID || row.Stats == nil {
			return nil, nil, api.ErrStatsUnavailable
		}
		ingress[i] = row.Stats
	}
	return network, ingress, nil
}

func (o *Orchestrator) readNetworkTraffic(ctx context.Context, sandboxes []*types.Sandbox) ([]trafficNetwork, error) {
	result := make([]trafficNetwork, len(sandboxes))
	ports := make([]int, 0, len(sandboxes))
	indices := make(map[int]int, len(sandboxes))
	for i, sb := range sandboxes {
		if sb.VswitchPort == "" {
			if sb.FloatingIP != "" || sb.InnerIP != "" || sb.PortMAC != "" {
				return nil, api.ErrStatsUnavailable
			}
			continue // No attached port: empty observations, not observed zero.
		}
		port, err := strconv.ParseUint(sb.VswitchPort, 10, 32)
		if err != nil || port == 0 || port > 4096 || sb.FloatingIP == "" || sb.InnerIP == "" || sb.PortMAC == "" {
			return nil, api.ErrStatsUnavailable
		}
		if _, found := indices[int(port)]; found {
			return nil, api.ErrStatsUnavailable
		}
		indices[int(port)] = i
		ports = append(ports, int(port))
	}
	if len(ports) == 0 {
		return result, nil
	}
	reader, ok := o.vs.(networkStatsReader)
	if !ok || o.cfg.Sandbox.Network.Switch == "" {
		return nil, api.ErrStatsUnavailable
	}
	release, err := o.acquireStatsSlot(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	// Reuse the lifecycle's allocation/detach fence. Do not wait behind device
	// moves, and never hold this lock while querying the Proxy or an exporter.
	if !o.networkAllocationMu.TryLock() {
		return nil, api.ErrStatsUnavailable
	}
	defer o.networkAllocationMu.Unlock()
	for _, sb := range sandboxes {
		if _, pending := o.detachedPortsPending[sb.VswitchPort]; pending {
			return nil, api.ErrStatsUnavailable
		}
		if err := o.statsBindingCurrent(ctx, sb); err != nil {
			return nil, err
		}
	}
	observed, err := reader.Stats(ctx, ports)
	if err != nil {
		return nil, fmt.Errorf("%w: connector stats: %v", api.ErrStatsUnavailable, err)
	}
	if observed == nil || observed.Switch != o.cfg.Sandbox.Network.Switch || len(observed.Ports) != len(ports) {
		return nil, api.ErrStatsUnavailable
	}
	for _, row := range observed.Ports {
		i, found := indices[int(row.Port)]
		if !found || row.FloatingIP != sandboxes[i].FloatingIP {
			return nil, api.ErrStatsUnavailable
		}
		ip, _, err := net.ParseCIDR(sandboxes[i].InnerIP)
		if err != nil || !ip.Equal(net.ParseIP(row.InnerIP)) {
			return nil, api.ErrStatsUnavailable
		}
		delete(indices, int(row.Port))
		result[i] = trafficNetwork{
			platform: api.TrafficCounters{RXPackets: &row.MgmtRxPackets, RXBytes: &row.MgmtRxBytes, TXPackets: &row.MgmtTxPackets, TXBytes: &row.MgmtTxBytes},
			transit:  api.TrafficCounters{RXPackets: &row.TransitRxPackets, RXBytes: &row.TransitRxBytes, TXPackets: &row.TransitTxPackets, TXBytes: &row.TransitTxBytes},
		}
	}
	for _, sb := range sandboxes {
		if err := o.statsBindingCurrent(ctx, sb); err != nil {
			return nil, err
		}
	}
	return result, nil
}

func (o *Orchestrator) readIngressStats(ctx context.Context, sb *types.Sandbox) (*api.TrafficStats, error) {
	stats, err := o.trafficStats.SandboxTrafficStats(ctx, sb.ID, sb.RunID, sb.Profile, sb.State)
	if err != nil {
		return nil, ingressReadError(err)
	}
	if stats == nil {
		return nil, api.ErrStatsUnavailable
	}
	return stats, nil
}

func ingressReadError(err error) error {
	if errors.Is(err, api.ErrStatsConflict) {
		return err
	}
	return fmt.Errorf("%w: proxy stats: %v", api.ErrStatsUnavailable, err)
}

func combineTraffic(ingress *api.TrafficStats, network trafficNetwork) *api.TrafficStats {
	result := *ingress
	if result.Services == nil {
		result.Services = map[string]api.ServiceTrafficStats{}
	}
	result.Platform, result.Transit = network.platform, network.transit
	result.Egress = struct{}{}
	return &result
}
