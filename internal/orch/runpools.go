package orch

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/launcher"
)

// runPools adds only list-position selection around the existing single-pool
// state machine. Templates and sizes never collapse scheduling positions.
type runPools struct {
	mu    sync.Mutex
	next  int
	pools []*runPool
}

func newRunPools(kind string, configs []config.RunPoolConfig, wait time.Duration, root string, lc launcher.Launcher, runs *runIndex, log *slog.Logger) *runPools {
	out := &runPools{}
	for i, cfg := range configs {
		p := newRunPool(kind, cfg.Size, wait, root, lc, func(id string) string { return instanceUnit(cfg.Unit, id) }, log.With("pool", i, "unit", cfg.Unit))
		p.runs = runs
		out.pools = append(out.pools, p)
	}
	return out
}

func (p *runPools) Start(ctx context.Context) error {
	for _, pool := range p.pools {
		if err := pool.Start(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (p *runPools) Assign(ctx context.Context, taskID string, commit func(string) error) (string, error) {
	p.mu.Lock()
	pool := p.pools[p.next]
	p.next = (p.next + 1) % len(p.pools)
	p.mu.Unlock()
	return pool.Assign(ctx, taskID, commit)
}

// runIndex has two different lifetimes: waiting ends at handoff/retirement;
// units survive handoff and are released by the existing ownership lifecycle.
// Restart restores units only. Pool identity is process-local.
type runIndex struct {
	mu      sync.Mutex
	waiting map[string]*runPool
	units   map[string]string
}

func (r *runIndex) register(runID string, p *runPool) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.waiting == nil {
		r.waiting = make(map[string]*runPool)
	}
	if r.units == nil {
		r.units = make(map[string]string)
	}
	r.waiting[runID], r.units[runID] = p, p.unitName(runID)
}

func (r *runIndex) assigned(runID string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.waiting, runID)
}

func (r *runIndex) forget(runID string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.waiting, runID)
	delete(r.units, runID)
}

func (r *runIndex) forgetWaiting(p *runPool) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for id, pool := range r.waiting {
		if pool == p {
			delete(r.waiting, id)
			delete(r.units, id)
		}
	}
}

func (r *runIndex) unit(runID string) string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.units[runID]
}

func (r *runIndex) restore(runID, unit string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.units == nil {
		r.units = make(map[string]string)
	}
	r.units[runID] = unit
}

func (r *runIndex) wait(ctx context.Context, runID string) (string, bool, error) {
	r.mu.Lock()
	p := r.waiting[runID]
	r.mu.Unlock()
	if p == nil {
		return "", false, fmt.Errorf("run %s is not waiting for assignment", runID)
	}
	return p.WaitAssignment(ctx, runID)
}
