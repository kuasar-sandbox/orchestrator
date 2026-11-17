package conductorapp

import (
	"context"
	"log/slog"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/hostmetrics"
	"github.com/kuasar-sandbox/orchestrator/internal/nodectl"
)

// Resource observations belong to the admission controller. Telemetry does not
// discover sandboxes or query conductor for these metrics.
func hostResourceGroups(state *nodectl.State, observed time.Time) []hostmetrics.Group {
	node := state.ResourceSnapshot()
	groups := make([]hostmetrics.Group, 0, 1+len(state.PoolStates())+node.ReservationCount)
	groups = append(groups, hostmetrics.Group{Samples: []hostmetrics.Sample{
		{Name: "node.resource.memory.allocatable", Unit: "By", Timestamp: observed, Value: float64(node.AllocatablePool.MemoryBytes)},
		{Name: "node.resource.memory.reserved", Unit: "By", Timestamp: observed, Value: float64(node.Reserved.MemoryBytes)},
		{Name: "node.resource.cpu.allocatable", Unit: "{core}", Timestamp: observed, Value: float64(node.AllocatablePool.CPUMilli) / 1000},
		{Name: "node.resource.cpu.reserved", Unit: "{core}", Timestamp: observed, Value: float64(node.Reserved.CPUMilli) / 1000},
		{Name: "node.resource.sandbox.count", Unit: "{sandbox}", Timestamp: observed, Value: float64(node.ReservationCount)},
	}})
	for _, pool := range state.PoolSnapshots() {
		groups = append(groups, hostmetrics.Group{Resource: map[string]string{"resource.pool": pool.Name}, Samples: []hostmetrics.Sample{
			{Name: "node.resource.pool.memory.allocatable", Unit: "By", Timestamp: observed, Value: float64(pool.AllocatablePool.MemoryBytes)},
			{Name: "node.resource.pool.memory.reserved", Unit: "By", Timestamp: observed, Value: float64(pool.Reserved.MemoryBytes)},
			{Name: "node.resource.pool.cpu.allocatable", Unit: "{core}", Timestamp: observed, Value: float64(pool.AllocatablePool.CPUMilli) / 1000},
			{Name: "node.resource.pool.cpu.reserved", Unit: "{core}", Timestamp: observed, Value: float64(pool.Reserved.CPUMilli) / 1000},
			{Name: "node.resource.pool.utilization", Unit: "1", Timestamp: observed, Value: pool.Utilization},
		}})
	}
	for _, reservation := range state.ReservationViews() {
		if reservation.SandboxID == "" {
			continue
		}
		// Mirror the resource controller's reservation-accounting fallback:
		// before the sandbox settles, configured allocatable CPU may be zero.
		cpuMilli := reservation.ConfiguredAllocatable.CPUMilli
		if cpuMilli == 0 {
			cpuMilli = reservation.Capacity.CPUMilli
		}
		groups = append(groups, hostmetrics.Group{Resource: map[string]string{"sandbox.id": reservation.SandboxID}, Samples: []hostmetrics.Sample{
			{Name: "sandbox.resource.memory.reserved", Unit: "By", Timestamp: observed, Value: float64(reservation.ReservationMemory)},
			{Name: "sandbox.resource.cpu.reserved", Unit: "{core}", Timestamp: observed, Value: float64(cpuMilli) / 1000},
		}})
	}
	return groups
}

func runHostResourceMetrics(ctx context.Context, state *nodectl.State, nodeID string, logger *slog.Logger) {
	if state == nil {
		return
	}
	publisher := hostmetrics.New("node-ctl", "conductor", map[string]string{"node.id": nodeID})
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !publisher.Available() {
				continue
			}
			window, cancel := context.WithTimeout(ctx, 5*time.Second)
			groups := hostResourceGroups(state, time.Now())
			for offset := 0; offset < len(groups); offset += 64 {
				end := min(len(groups), offset+64)
				if err := publisher.EmitBatches(window, groups[offset:end]); err != nil {
					if logger != nil {
						logger.Debug("resource OTLP unavailable", "err", err)
					}
					break // No queue, don't create a burst of failing requests.
				}
			}
			cancel()
		}
	}
}
