package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/fullof-work/mass-sandbox/pkg/nodectl"
)

func daemonCmd(args []string) int {
	fs := flag.NewFlagSet("daemon", flag.ContinueOnError)
	configPath := fs.String("config", "", "path to node-ctl.yaml; empty = built-in defaults")
	listenOverride := fs.String("listen", "", "override yaml listen path")
	if err := fs.Parse(args); err != nil {
		return 2
	}

	cfg, err := nodectl.LoadDaemonConfig(*configPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	if *listenOverride != "" {
		cfg.Listen = *listenOverride
	}

	resolved, err := cfg.Resolve()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}

	state := nodectl.NewState(
		resolved.PhysicalMemory, resolved.PhysicalCPU,
		resolved.HostReserved.MemoryBytes, resolved.HostReserved.CPUMilli,
		resolved.Watermarks)

	persister := &nodectl.Persister{Path: resolved.StatePath}
	// Best-effort load. If state.json is missing or corrupt, start fresh
	// — sandbox-ctl will reattach by token over RPC.
	if prev, err := persister.Load(); err == nil && prev != nil {
		// Preserve reservations only; pool / wm come from the YAML.
		state.Reservations = prev.Reservations
		log.Printf("loaded %d reservations from %s", len(prev.Reservations), resolved.StatePath)
	} else if err != nil {
		log.Printf("state load failed (continuing fresh): %v", err)
	}

	admission := nodectl.NewAdmissionController(resolved.Admission)
	allocator := nodectl.NewAllocator(resolved.Allocator)

	auditor, aerr := nodectl.NewAuditor(resolved.AuditPath)
	if aerr != nil {
		log.Printf("audit log disabled (%v)", aerr)
	}
	defer auditor.Close()

	srv := &nodectl.Server{
		Path:      resolved.Listen,
		State:     state,
		Admission: admission,
		Allocator: allocator,
		Persister: persister,
		Auditor:   auditor,
		Logf:      func(f string, a ...any) { log.Printf("[node-ctl] "+f, a...) },
	}
	if err := srv.Listen(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}

	ctx, cancel := signalContext()
	defer cancel()

	// Idle sweeper — startup TTL + heartbeat staleness + GC.
	sweeper := &nodectl.IdleSweeper{
		State:      state,
		Admission:  admission,
		Allocator:  allocator,
		Persister:  persister,
		StartupTTL: resolved.Admission.StartupTTL,
		Heartbeat:  30 * time.Second,
		Interval:   10 * time.Second,
		Logf:       func(f string, a ...any) { log.Printf("[node-ctl sweep] "+f, a...) },
	}
	go sweeper.Run(ctx)

	// Active reclaimer — settled-期 allocatable shrink toward working
	// set + safety margin. Picks up via Heartbeat ack on the sandbox
	// side.
	reclaimer := &nodectl.ActiveReclaimer{
		State:        state,
		Persister:    persister,
		Interval:     10 * time.Second,
		SafetyMargin: 1.25,
		Auditor:      auditor,
		Logf:         func(f string, a ...any) { log.Printf("[node-ctl reclaim] "+f, a...) },
	}
	go reclaimer.Run(ctx)

	log.Printf("node-ctl listening on %s; pool=%d MiB, host_reserved=%d MiB",
		resolved.Listen, state.AllocatablePool.MemoryBytes>>20,
		resolved.HostReserved.MemoryBytes>>20)

	if err := srv.Serve(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	return 0
}
