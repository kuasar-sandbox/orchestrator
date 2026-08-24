package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"math"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/nodectl"
	"github.com/kuasar-sandbox/orchestrator/internal/orch"
)

// resourceProbe adapts the resource controller's State + AdmissionController to
// orch.ResourceProbe, so the cluster heartbeat reports this node's water level.
type resourceProbe struct {
	state     *nodectl.State
	admission *nodectl.AdmissionController
}

func (p resourceProbe) Snapshot() orch.ResourceProbeSnapshot {
	snapshot := p.state.ResourceSnapshot()
	return orch.ResourceProbeSnapshot{
		Zone: string(snapshot.Zone), Allocated: resourceProbeInt64(snapshot.Reserved.MemoryBytes),
		Pool: resourceProbeInt64(snapshot.AllocatablePool.MemoryBytes), Draining: p.admission.IsDrained(),
	}
}

// Cluster heartbeat fields predate the uint64 reservation model. Recovery may
// conservatively charge more than one full-pool unknown consumer, so saturate
// instead of wrapping a positive fail-closed charge into a negative int64.
func resourceProbeInt64(value uint64) int64 {
	if value > uint64(math.MaxInt64) {
		return math.MaxInt64
	}
	return int64(value)
}

func (p resourceProbe) SandboxResourceStats(sandboxID string) (api.ResourceStats, bool) {
	snapshot, found := p.state.SnapshotSandboxResource(sandboxID)
	if !found {
		return api.ResourceStats{}, false
	}
	cpuCount := float64(snapshot.Capacity.CPUMilli) / 1000
	cpuAllocatable := float64(snapshot.CPUAllocatable) / 1000
	memTotal := snapshot.Capacity.MemoryBytes
	reservationMemory := snapshot.ReservationMemory
	stats := api.ResourceStats{
		CPUCount:       &cpuCount,
		CPUAllocatable: &cpuAllocatable,
		MemTotal:       &memTotal,
		// The E2B-compatible field name is retained at the HTTP boundary; its
		// memory value is node reservation, not guest demand or host charge.
		MemAllocatable: &reservationMemory,
	}
	if !snapshot.LastReportAt.IsZero() {
		timestamp := snapshot.LastReportAt.Unix()
		memUsed := snapshot.HostMemoryCurrent
		stats.TimestampUnix = &timestamp
		stats.MemUsed = &memUsed
	}
	return stats, true
}

var _ orch.ResourceProbe = resourceProbe{}
var _ orch.SandboxResourceProvider = resourceProbe{}

// startResourceController starts the in-process node resource controller — the
// serve `resource_listen` sub-server (node-resource.md) — and runs it until ctx
// is cancelled. rcfg is the inlined controller config from serve's config
// (config.Config.ResourceListen); resolved is computed once during conductor
// preflight and shared with the launch renderer. It returns once the listener is
// bound (the controller serves in a background goroutine), or an error if setup
// fails. When resource_listen is absent/disabled serve never calls this and
// sandboxes fall back to static cgroup.
func startResourceController(ctx context.Context, rcfg *config.ResourceListenConfig, resolved *nodectl.Resolved, managedRunRoot string, slogger *slog.Logger) (orch.ResourceProbe, error) {
	if rcfg.StatePath != "" {
		slogger.Warn("resource_listen.state_path is deprecated and ignored", "state_path", rcfg.StatePath)
	}
	if resolved == nil {
		return nil, fmt.Errorf("resolved resource_listen configuration is required")
	}

	state := nodectl.NewState(
		resolved.PhysicalMemory, resolved.PhysicalCPU,
		resolved.HostReserved.MemoryBytes, resolved.HostReserved.CPUMilli,
		resolved.Watermarks)

	admission := nodectl.NewAdmissionController(resolved.Admission)
	allocator := nodectl.NewAllocator(resolved.Allocator)

	auditor, aerr := nodectl.NewAuditor(resolved.AuditPath)
	if aerr != nil {
		log.Printf("[node-ctl resource] audit log disabled (%v)", aerr)
	}

	srv := &nodectl.Server{
		Path:      resolved.Listen,
		Identity:  resolved.SocketIdentity,
		State:     state,
		Admission: admission,
		Allocator: allocator,
		Inventory: &nodectl.Inventory{
			ControllerSocket: resolved.SocketIdentity, CgroupScanPaths: resolved.CgroupScanPaths,
			ManagedRunRoot: managedRunRoot, Pool: state.AllocatablePool,
			Logf: func(f string, a ...any) { log.Printf("[node-ctl resource inventory] "+f, a...) },
		},
		Auditor: auditor,
		Logf:    func(f string, a ...any) { log.Printf("[node-ctl resource] "+f, a...) },
	}
	if err := srv.Listen(); err != nil {
		auditor.Close()
		return nil, err
	}

	// Wire the admission worker: it builds the reservation + AdmitResponse for a
	// queued head via the same BuildAdmitOKFromQueue path as the sync admit.
	admission.SetWiring(state, auditor,
		func(f string, a ...any) { log.Printf("[node-ctl resource admit] "+f, a...) },
		func(p *nodectl.PendingAdmit) (*nodectl.Message, error) {
			return srv.BuildAdmitOKFromQueue(p)
		},
	)
	admission.Run()

	sweeper := &nodectl.IdleSweeper{
		State: state, Admission: admission, Allocator: allocator,
		Inventory:  srv.Inventory,
		StartupTTL: resolved.Admission.StartupTTL, Heartbeat: 30 * time.Second, Interval: 10 * time.Second,
		Logf: func(f string, a ...any) { log.Printf("[node-ctl resource sweep] "+f, a...) },
	}
	go sweeper.Run(ctx)

	slogger.Info("resource controller listening (resource_listen)", "socket", resolved.Listen,
		"pool_mib", state.AllocatablePool.MemoryBytes>>20, "host_reserved_mib", resolved.HostReserved.MemoryBytes>>20)

	go func() {
		if err := srv.Serve(ctx); err != nil {
			slogger.Error("resource controller", "err", err)
		}
		admission.Stop()
		auditor.Close()
	}()
	return resourceProbe{state: state, admission: admission}, nil
}
