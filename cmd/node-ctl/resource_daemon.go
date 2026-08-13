package main

import (
	"context"
	"log"
	"log/slog"
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
		Zone: string(snapshot.Zone), Allocated: int64(snapshot.Allocated.MemoryBytes),
		Pool: int64(snapshot.AllocatablePool.MemoryBytes), Draining: p.admission.IsDrained(),
	}
}

func (p resourceProbe) SandboxResourceStats(sandboxID string) (api.ResourceStats, bool) {
	snapshot, found := p.state.SnapshotSandboxResource(sandboxID)
	if !found {
		return api.ResourceStats{}, false
	}
	cpuCount := float64(snapshot.Capacity.CPUMilli) / 1000
	cpuAllocatable := float64(snapshot.CPUAllocatable) / 1000
	memTotal := snapshot.Capacity.MemoryBytes
	memAllocatable := snapshot.MemAllocatable
	stats := api.ResourceStats{
		CPUCount:       &cpuCount,
		CPUAllocatable: &cpuAllocatable,
		MemTotal:       &memTotal,
		MemAllocatable: &memAllocatable,
	}
	if !snapshot.LastReportAt.IsZero() {
		timestamp := snapshot.LastReportAt.Unix()
		memUsed := snapshot.LastReportedRSS
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
// (config.Config.ResourceListen); its tuning is resolved here (auto-detect +
// defaults). It returns once the listener is bound (the controller serves in a
// background goroutine), or an error if setup fails. When resource_listen is
// absent/disabled serve never calls this and sandboxes fall back to static cgroup.
func startResourceController(ctx context.Context, rcfg *config.ResourceListenConfig, managedRunRoot string, slogger *slog.Logger) (orch.ResourceProbe, error) {
	resolved, err := nodectl.Resolve(rcfg)
	if err != nil {
		return nil, err
	}

	state := nodectl.NewState(
		resolved.PhysicalMemory, resolved.PhysicalCPU,
		resolved.HostReserved.MemoryBytes, resolved.HostReserved.CPUMilli,
		resolved.Watermarks)

	persister := &nodectl.Persister{Path: resolved.StatePath}
	var legacyReservations map[string]*nodectl.Reservation
	if previous, loadErr := persister.Load(); loadErr == nil && previous != nil {
		legacyReservations = previous.PersistenceReservations()
		log.Printf("[node-ctl resource] loaded %d legacy reservations; live inventory remains authoritative", len(legacyReservations))
	} else if loadErr != nil {
		log.Printf("[node-ctl resource] legacy state load failed (inventory recovery continues): %v", loadErr)
	}

	admission := nodectl.NewAdmissionController(resolved.Admission)
	allocator := nodectl.NewAllocator(resolved.Allocator)

	auditor, aerr := nodectl.NewAuditor(resolved.AuditPath)
	if aerr != nil {
		log.Printf("[node-ctl resource] audit log disabled (%v)", aerr)
	}

	srv := &nodectl.Server{
		Path:               resolved.Listen,
		Identity:           resolved.SocketIdentity,
		State:              state,
		Admission:          admission,
		Allocator:          allocator,
		Persister:          persister,
		LegacyReservations: legacyReservations,
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
		State: state, Admission: admission, Allocator: allocator, Persister: persister,
		Inventory:  srv.Inventory,
		StartupTTL: resolved.Admission.StartupTTL, Heartbeat: 30 * time.Second, Interval: 10 * time.Second,
		Logf: func(f string, a ...any) { log.Printf("[node-ctl resource sweep] "+f, a...) },
	}
	go sweeper.Run(ctx)

	reclaimer := &nodectl.ActiveReclaimer{
		State: state, Persister: persister, Interval: 10 * time.Second, SafetyMargin: 1.25, Auditor: auditor,
		Logf: func(f string, a ...any) { log.Printf("[node-ctl resource reclaim] "+f, a...) },
	}
	go reclaimer.Run(ctx)

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
