package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/nodectl"
	"github.com/kuasar-sandbox/orchestrator/internal/orch"
)

type resourceRuntime struct {
	state     *nodectl.State
	admission *nodectl.AdmissionController
	prepared  *nodectl.PreparedAdmissionController
	socket    string
}

func (r *resourceRuntime) clusterLoad() orch.ClusterResourceLoad {
	r.state.Lock()
	allocated := r.state.NodeAllocated().MemoryBytes
	startup := r.state.StartupInFlightLocked()
	pool := r.state.AllocatablePool.MemoryBytes
	startupPool := r.state.StartupPoolBytes()
	buildReserved := r.state.BuildReserved.MemoryBytes
	zone := string(r.state.MemoryZone())
	r.state.Unlock()
	draining := r.admission.IsDrained()
	reason := ""
	switch {
	case draining:
		reason = "node_draining"
	case zone == "red" || zone == "critical":
		reason = "node_resource_safety"
	}
	return orch.ClusterResourceLoad{
		Controller: true, WaterZone: zone, Draining: draining,
		NodeAllocatedMemory: allocated, AllocatablePoolMemory: pool,
		BuildReservedMemory: buildReserved, StartupAllocatedMemory: startup,
		StartupPoolMemory:       startupPool,
		AdmissionTokenAvailable: r.admission.TokenAvailable(), SafetyRejectReason: reason,
	}
}

// startResourceController starts the in-process node resource controller — the
// serve `resource_listen` sub-server (node-resource.md) — and runs it until ctx
// is cancelled. rcfg is the inlined controller config from serve's config
// (config.Config.ResourceListen); its tuning is resolved here (auto-detect +
// defaults). It returns once the listener is bound (the controller serves in a
// background goroutine), or an error if setup fails. When resource_listen is
// absent/disabled serve never calls this and sandboxes fall back to static cgroup.
func startResourceController(
	ctx context.Context,
	rcfg *config.ResourceListenConfig,
	builder config.BuilderConfig,
	sandboxSlots uint64,
	resetPreparedState bool,
	slogger *slog.Logger,
) (*resourceRuntime, error) {
	resolved, err := nodectl.Resolve(rcfg, builder)
	if err != nil {
		return nil, err
	}

	state := nodectl.NewState(
		resolved.PhysicalMemory, resolved.PhysicalCPU,
		resolved.HostReserved.MemoryBytes, resolved.HostReserved.CPUMilli,
		resolved.BuildReserved,
		resolved.Watermarks)

	persister := &nodectl.Persister{Path: resolved.StatePath}
	if resetPreparedState && resolved.StatePath != "" {
		if err := os.Remove(resolved.StatePath); err != nil && !os.IsNotExist(err) {
			return nil, fmt.Errorf("remove prior-epoch resource state: %w", err)
		}
		if parent, err := os.Open(filepath.Dir(resolved.StatePath)); err == nil {
			if syncErr := parent.Sync(); syncErr != nil {
				parent.Close()
				return nil, fmt.Errorf("fsync prior-epoch resource state removal: %w", syncErr)
			}
			if err := parent.Close(); err != nil {
				return nil, err
			}
		}
	}
	// Missing state is valid only for a node with no accepted workflow; the
	// Authority reconciliation below proves that condition. Corruption is never
	// treated as an empty durable Admission history.
	if prev, err := persister.Load(); err == nil && prev != nil {
		state.Reservations = prev.Reservations
		state.PreparedSandboxAdmissions = prev.PreparedSandboxAdmissions
		state.NextPreparedQueueSeq = prev.NextPreparedQueueSeq
		log.Printf("[node-ctl resource] loaded %d reservations from %s", len(prev.Reservations), resolved.StatePath)
	} else if err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("load durable resource-controller state: %w", err)
	}

	admission := nodectl.NewAdmissionController(resolved.Admission)
	allocator := nodectl.NewAllocator(resolved.Allocator)

	auditor, aerr := nodectl.NewAuditor(resolved.AuditPath)
	if aerr != nil {
		log.Printf("[node-ctl resource] audit log disabled (%v)", aerr)
	}

	srv := &nodectl.Server{
		Path:      resolved.Listen,
		State:     state,
		Admission: admission,
		Allocator: allocator,
		Persister: persister,
		Auditor:   auditor,
		Logf:      func(f string, a ...any) { log.Printf("[node-ctl resource] "+f, a...) },
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
	var prepared *nodectl.PreparedAdmissionController
	if sandboxSlots > 0 {
		prepared, err = nodectl.NewPreparedAdmissionController(state, admission, persister, resolved.Admission, sandboxSlots)
		if err != nil {
			auditor.Close()
			return nil, err
		}
	}
	admission.Run()

	sweeper := &nodectl.IdleSweeper{
		State: state, Admission: admission, Allocator: allocator, Persister: persister,
		PreparedAdmission: prepared,
		StartupTTL:        resolved.Admission.StartupTTL, Heartbeat: 30 * time.Second, Interval: 10 * time.Second,
		Logf: func(f string, a ...any) { log.Printf("[node-ctl resource sweep] "+f, a...) },
	}
	go sweeper.Run(ctx)

	reclaimer := &nodectl.ActiveReclaimer{
		State: state, Persister: persister, Interval: 10 * time.Second, SafetyMargin: 1.25, Auditor: auditor,
		Logf: func(f string, a ...any) { log.Printf("[node-ctl resource reclaim] "+f, a...) },
	}
	go reclaimer.Run(ctx)

	slogger.Info("resource controller listening (resource_listen)", "socket", resolved.Listen,
		"pool_mib", state.AllocatablePool.MemoryBytes>>20, "host_reserved_mib", resolved.HostReserved.MemoryBytes>>20,
		"build_reserved_mib", resolved.BuildReserved.MemoryBytes>>20)

	go func() {
		if err := srv.Serve(ctx); err != nil {
			slogger.Error("resource controller", "err", err)
		}
		admission.Stop()
		auditor.Close()
	}()
	return &resourceRuntime{
		state: state, admission: admission, prepared: prepared, socket: resolved.Listen,
	}, nil
}
