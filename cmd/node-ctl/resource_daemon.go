package main

import (
	"context"
	"log"
	"log/slog"
	"time"

	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/nodectl"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/orch"
)

// resourceProbe adapts the resource controller's State + AdmissionController to
// orch.ResourceProbe, so the cluster heartbeat reports this node's water level.
type resourceProbe struct {
	state     *nodectl.State
	admission *nodectl.AdmissionController
}

func (p resourceProbe) Zone() string          { return string(p.state.MemoryZone()) }
func (p resourceProbe) AllocatedBytes() int64 { return int64(p.state.NodeAllocated().MemoryBytes) }
func (p resourceProbe) PoolBytes() int64      { return int64(p.state.AllocatablePool.MemoryBytes) }
func (p resourceProbe) Draining() bool        { return p.admission.IsDrained() }

var _ orch.ResourceProbe = resourceProbe{}

// startResourceController starts the in-process node resource controller — the
// serve `resource_listen` sub-server (node-resource.md) — and runs it until ctx
// is cancelled. configPath is the resource controller yaml ("" = built-in
// defaults); listenOverride overrides its UDS. It returns once the listener is
// bound (the controller serves in a background goroutine), or an error if setup
// fails. When resource_listen is disabled serve never calls this and sandboxes
// fall back to static cgroup.
func startResourceController(ctx context.Context, configPath, listenOverride string, slogger *slog.Logger) (orch.ResourceProbe, error) {
	cfg, err := nodectl.LoadDaemonConfig(configPath)
	if err != nil {
		return nil, err
	}
	if listenOverride != "" {
		cfg.Listen = listenOverride
	}
	resolved, err := cfg.Resolve()
	if err != nil {
		return nil, err
	}

	state := nodectl.NewState(
		resolved.PhysicalMemory, resolved.PhysicalCPU,
		resolved.HostReserved.MemoryBytes, resolved.HostReserved.CPUMilli,
		resolved.Watermarks)

	persister := &nodectl.Persister{Path: resolved.StatePath}
	// Best-effort load; missing/corrupt state.json starts fresh — sandbox-ctl
	// reattaches by token over RPC.
	if prev, err := persister.Load(); err == nil && prev != nil {
		state.Reservations = prev.Reservations
		log.Printf("[node-ctl resource] loaded %d reservations from %s", len(prev.Reservations), resolved.StatePath)
	} else if err != nil {
		log.Printf("[node-ctl resource] state load failed (continuing fresh): %v", err)
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
	admission.Run()

	sweeper := &nodectl.IdleSweeper{
		State: state, Admission: admission, Allocator: allocator, Persister: persister,
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
