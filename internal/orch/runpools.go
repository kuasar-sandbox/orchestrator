package orch

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sort"
	"sync"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/launcher"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
)

// runPools chooses among the existing per-pool state machines. Duplicate
// templates remain independent logical pools with separate idle targets.
type runPools struct {
	pools []*runPool
}

func newRunPools(kind string, configs []config.RunPoolConfig, wait time.Duration, root string, lc launcher.Launcher, runs *runIndex, log *slog.Logger) *runPools {
	out := &runPools{}
	for i, cfg := range configs {
		p := newRunPool(kind, cfg.Size, wait, root, lc, func(id string) string { return instanceUnit(cfg.Unit, id) }, log.With("pool", i, "unit", cfg.Unit))
		p.runs = runs
		p.name, p.resourcePool, p.capacity, p.priority = cfg.PoolName(logicalPoolKind(kind), i), cfg.ResourcePoolName(), cfg.Capacity, cfg.Priority
		out.pools = append(out.pools, p)
	}
	return out
}

func logicalPoolKind(kind string) string {
	if kind == runKindSandbox {
		return "runner"
	}
	if kind == runKindBuild {
		return "builder"
	}
	return kind
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
	return p.AssignWithFence(ctx, taskID, nil, commit)
}

func (p *runPools) AssignWithFence(ctx context.Context, taskID string, sessionFence func(string) (bool, func()), commit func(string) error) (string, error) {
	return p.assignInResourcePool(ctx, "", taskID, sessionFence, commit)
}

func (p *runPools) assignInResourcePool(ctx context.Context, resourcePool, taskID string, sessionFence func(string) (bool, func()), commit func(string) error) (string, error) {
	candidates := append([]*runPool(nil), p.pools...)
	rand.Shuffle(len(candidates), func(i, j int) { candidates[i], candidates[j] = candidates[j], candidates[i] })
	sort.SliceStable(candidates, func(i, j int) bool { return candidates[i].priority > candidates[j].priority })
	for _, pool := range candidates {
		if resourcePool != "" && pool.resourcePool != resourcePool {
			continue
		}
		reservation := pool.runs.reserve(pool)
		if reservation == nil {
			continue
		}
		defer reservation.release()
		return pool.assignWithReservation(ctx, taskID, sessionFence, commit, reservation)
	}
	return "", fmt.Errorf("no runner supply in resource pool %q", resourcePool)
}

func (p *runPools) hasSupply(resourcePool string) bool {
	for _, pool := range p.pools {
		if pool.resourcePool == resourcePool && pool.runs.hasSupply(pool) {
			return true
		}
	}
	return false
}

// runIndex has two different lifetimes: waiting ends at handoff/retirement;
// units survive handoff and are released by the existing ownership lifecycle.
// Durable identities include idle, starting and assigned instances on restart.
type runIndex struct {
	mu       sync.Mutex
	retireMu sync.Mutex // never hold mu while calling the resource controller
	waiting  map[string]*runPool
	units    map[string]string
	owners   map[string]*runPool
	metadata map[string]store.RunPoolOwnership
	states   map[string]string
	retiring map[string]time.Time
	persist  func(store.RunPoolOwnership) error
	erase    func(store.RunPoolOwnership) error
}

func (r *runIndex) register(runID string, p *runPool) error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.waiting == nil {
		r.waiting = make(map[string]*runPool)
	}
	if r.units == nil {
		r.units = make(map[string]string)
	}
	if p.capacity != nil && r.countLocked(p) >= *p.capacity {
		return fmt.Errorf("runner pool %s capacity exhausted", p.name)
	}
	m := store.RunPoolOwnership{RunID: runID, Kind: p.kind, Pool: p.name, ResourcePool: p.resourcePool, Unit: p.unitName(runID)}
	if r.persist != nil {
		if err := r.persist(m); err != nil {
			return err
		}
	}
	if r.owners == nil {
		r.owners = map[string]*runPool{}
	}
	if r.metadata == nil {
		r.metadata = map[string]store.RunPoolOwnership{}
	}
	if r.states == nil {
		r.states = map[string]string{}
	}
	r.states[runID] = "starting"
	r.owners[runID], r.metadata[runID] = p, m
	r.waiting[runID], r.units[runID] = p, p.unitName(runID)
	return nil
}

func (r *runIndex) assigned(runID string, reservation *runReservation) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.waiting, runID)
	if r.states != nil {
		r.states[runID] = "bound"
	}
	// The bound instance now accounts for this request. Consume its temporary
	// reservation in the same critical section, before publishing the handoff.
	if reservation != nil && reservation.runs == r && reservation.pool == r.owners[runID] {
		reservation.releaseLocked()
	}
}

func (r *runIndex) markState(runID, state string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.states == nil {
		r.states = map[string]string{}
	}
	r.states[runID] = state
	if state == "stopping" {
		delete(r.waiting, runID)
	}
}

func (r *runIndex) owner(runID string) (unit string, waiting bool) {
	unit, _, waiting = r.ownerPool(runID)
	return unit, waiting
}

func (r *runIndex) ownerPool(runID string) (unit string, pool *runPool, waiting bool) {
	if r == nil {
		return "", nil, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	pool, waiting = r.waiting[runID]
	return r.units[runID], pool, waiting
}

func (r *runIndex) forget(runID string) error {
	if r == nil {
		return nil
	}
	r.retireMu.Lock()
	defer r.retireMu.Unlock()
	r.mu.Lock()
	m, recorded := r.metadata[runID]
	r.mu.Unlock()
	if recorded && r.erase != nil {
		if err := r.erase(m); err != nil {
			r.mu.Lock()
			if r.retiring == nil {
				r.retiring = map[string]time.Time{}
			}
			r.retiring[runID] = time.Now().Add(time.Second)
			r.mu.Unlock()
			return err
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if current, ok := r.metadata[runID]; ok && current != m {
		return fmt.Errorf("run %s ownership changed during retirement", runID)
	}
	delete(r.waiting, runID)
	delete(r.units, runID)
	delete(r.owners, runID)
	delete(r.metadata, runID)
	delete(r.states, runID)
	delete(r.retiring, runID)
	return nil
}

// Lifecycle owners may have already committed physical cleanup when deleting
// its small identity journal fails. Retry only those confirmed exits.
func (r *runIndex) retryRetired(pool *runPool, now time.Time) {
	if r == nil {
		return
	}
	r.mu.Lock()
	var ids []string
	for id, when := range r.retiring {
		if r.owners[id] == pool && !now.Before(when) {
			ids = append(ids, id)
		}
	}
	r.mu.Unlock()
	for _, id := range ids {
		_ = r.forget(id)
	}
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
		}
	}
}

func (r *runIndex) countLocked(p *runPool) int {
	n := 0
	for _, owner := range r.owners {
		if owner == p {
			n++
		}
	}
	return n
}
func (r *runIndex) supplyLocked(p *runPool) bool {
	if p.capacity == nil {
		return true
	}
	waiting := 0
	for _, owner := range r.waiting {
		if owner == p {
			waiting++
		}
	}
	return r.countLocked(p)+max(0, p.reserved+1-waiting) <= *p.capacity
}
func (r *runIndex) hasSupply(p *runPool) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.supplyLocked(p)
}

// A reservation belongs to one assignment, including retries across starting
// units. active is guarded by runs.mu so handoff and caller cleanup can each
// release it without decrementing another request's reservation.
type runReservation struct {
	runs   *runIndex
	pool   *runPool
	active bool
}

func (r *runIndex) reserve(p *runPool) *runReservation {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.supplyLocked(p) {
		return nil
	}
	p.reserved++
	return &runReservation{runs: r, pool: p, active: true}
}

func (reservation *runReservation) release() {
	if reservation == nil {
		return
	}
	reservation.runs.mu.Lock()
	defer reservation.runs.mu.Unlock()
	reservation.releaseLocked()
}

func (reservation *runReservation) releaseLocked() {
	if reservation.active {
		reservation.pool.reserved--
		reservation.active = false
	}
}

func (r *runIndex) ownership(runID string) (store.RunPoolOwnership, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	m, ok := r.metadata[runID]
	return m, ok
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
