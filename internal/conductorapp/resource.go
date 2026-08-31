package conductorapp

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"time"

	publicconfig "github.com/kuasar-sandbox/orchestrator/config"
	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/nodectl"
	"github.com/kuasar-sandbox/orchestrator/internal/orch"
)

// ResourceProbe adapts the resource controller to conductor heartbeat and API
// resource-stat contracts.
type ResourceProbe struct {
	state     *nodectl.State
	admission *nodectl.AdmissionController
}

func NewResourceProbe(state *nodectl.State, admission *nodectl.AdmissionController) ResourceProbe {
	return ResourceProbe{state: state, admission: admission}
}

func (p ResourceProbe) Snapshot() orch.ResourceProbeSnapshot {
	snapshot := p.state.ResourceSnapshot()
	return orch.ResourceProbeSnapshot{
		Zone: string(snapshot.Zone), Allocated: resourceProbeInt64(snapshot.Reserved.MemoryBytes),
		Pool: resourceProbeInt64(snapshot.AllocatablePool.MemoryBytes), Draining: p.admission.IsDrained(),
	}
}

func resourceProbeInt64(value uint64) int64 {
	if value > uint64(math.MaxInt64) {
		return math.MaxInt64
	}
	return int64(value)
}

func (p ResourceProbe) SandboxResourceStats(sandboxID string) (api.ResourceStats, bool) {
	snapshot, found := p.state.SnapshotSandboxResource(sandboxID)
	if !found {
		return api.ResourceStats{}, false
	}
	cpuCount := float64(snapshot.Capacity.CPUMilli) / 1000
	cpuAllocatable := float64(snapshot.CPUAllocatable) / 1000
	memTotal := snapshot.Capacity.MemoryBytes
	reservationMemory := snapshot.ReservationMemory
	stats := api.ResourceStats{
		CPUCount: &cpuCount, CPUAllocatable: &cpuAllocatable,
		MemTotal: &memTotal, MemAllocatable: &reservationMemory,
	}
	if !snapshot.LastReportAt.IsZero() {
		timestamp := snapshot.LastReportAt.Unix()
		memUsed := snapshot.HostMemoryCurrent
		stats.TimestampUnix = &timestamp
		stats.MemUsed = &memUsed
	}
	return stats, true
}

var _ orch.ResourceProbe = ResourceProbe{}
var _ orch.SandboxResourceProvider = ResourceProbe{}

func StartResourceController(ctx context.Context, cfg *publicconfig.ResourceListenConfig, resolved *nodectl.Resolved, managedRunRoot string, logger *slog.Logger) (orch.ResourceProbe, error) {
	if resolved == nil {
		return nil, fmt.Errorf("resolved resource_listen configuration is required")
	}
	resourceLogger := logger.With("component", "resource-controller")
	if cfg.StatePath != "" {
		resourceLogger.Warn("resource_listen.state_path is deprecated and ignored", "state_path", cfg.StatePath)
	}
	state := nodectl.NewState(
		resolved.PhysicalMemory, resolved.PhysicalCPU,
		resolved.HostReserved.MemoryBytes, resolved.HostReserved.CPUMilli,
		resolved.Watermarks,
	)
	admission := nodectl.NewAdmissionController(resolved.Admission)
	allocator := nodectl.NewAllocator(resolved.Allocator)
	inventoryLogger := resourceLogger.With("subsystem", "inventory")
	server := &nodectl.Server{
		Path: resolved.Listen, Identity: resolved.SocketIdentity,
		State: state, Admission: admission, Allocator: allocator,
		Inventory: &nodectl.Inventory{
			ControllerSocket: resolved.SocketIdentity, CgroupScanPaths: resolved.CgroupScanPaths,
			ManagedRunRoot: managedRunRoot, Pool: state.AllocatablePool,
			Logf: func(format string, args ...any) { inventoryLogger.Info(fmt.Sprintf(format, args...)) },
		},
		Logf: func(format string, args ...any) { resourceLogger.Info(fmt.Sprintf(format, args...)) },
	}
	if err := server.Listen(); err != nil {
		return nil, err
	}
	admission.SetWiring(state, resourceLogger.With("subsystem", "admission"),
		func(pending *nodectl.PendingAdmit) (*nodectl.Message, error) {
			return server.BuildAdmitOKFromQueue(pending)
		},
	)
	admission.Run()
	sweeperLogger := resourceLogger.With("subsystem", "sweeper")
	sweeper := &nodectl.IdleSweeper{
		State: state, Admission: admission, Allocator: allocator, Inventory: server.Inventory,
		StartupTTL: resolved.Admission.StartupTTL, Heartbeat: 30 * time.Second, Interval: 10 * time.Second,
		Logf: func(format string, args ...any) { sweeperLogger.Info(fmt.Sprintf(format, args...)) },
	}
	go sweeper.Run(ctx)
	resourceLogger.Info("resource controller listening (resource_listen)", "socket", resolved.Listen,
		"pool_mib", state.AllocatablePool.MemoryBytes>>20, "host_reserved_mib", resolved.HostReserved.MemoryBytes>>20)
	go func() {
		if err := server.Serve(ctx); err != nil {
			resourceLogger.Error("resource controller", "err", err)
		}
		admission.Stop()
	}()
	return ResourceProbe{state: state, admission: admission}, nil
}
