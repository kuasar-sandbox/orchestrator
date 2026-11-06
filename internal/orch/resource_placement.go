package orch

import (
	"context"
	"fmt"
	"math"
	"os"
	"strings"

	"github.com/kuasar-sandbox/orchestrator/internal/nodectl"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/orchestrator/internal/util"
	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
)

func schedulingCapacity(resources rtconfig.ResourcesConfig) (nodectl.Resources, error) {
	if resources.Capacity.CPU <= 0 || uint64(resources.Capacity.CPU) > math.MaxUint64/1000 {
		return nodectl.Resources{}, fmt.Errorf("invalid scheduling CPU capacity")
	}
	memory, err := util.ParseSize(resources.Capacity.Memory)
	if err != nil {
		return nodectl.Resources{}, err
	}
	return nodectl.Resources{MemoryBytes: memory, CPUMilli: uint64(resources.Capacity.CPU) * 1000}, nil
}

func (o *Orchestrator) bindPreparedPlacement(ctx context.Context, sb *types.Sandbox, claim nodectl.Placement) error {
	m, ok := o.runs.ownership(sb.RunID)
	if !ok {
		return fmt.Errorf("missing runner identity")
	}
	if m.ResourcePool != claim.Pool {
		return fmt.Errorf("runner resource pool does not match placement")
	}
	m.TaskID, m.CapacityMemory, m.CapacityCPU = sb.ID, claim.Capacity.MemoryBytes, claim.Capacity.CPUMilli
	if err := o.saveRunOwnership(ctx, m); err != nil {
		return err
	}
	if err := o.memoryPressure.Load().state.BindPlacement(sb.ID, claim.Identity, sb.RunID); err != nil {
		return err
	}
	return nil
}

func (o *Orchestrator) assignSandboxRunner(ctx context.Context, sb *types.Sandbox, resources rtconfig.ResourcesConfig, fence func(string) (bool, func()), commit func(string) error) (string, error) {
	p := o.memoryPressure.Load()
	if p == nil {
		return o.runnerPool.AssignWithFence(ctx, sb.ID, fence, commit)
	}
	capacity, err := schedulingCapacity(resources)
	if err != nil {
		return "", err
	}
	exclude, err := o.recoveryExclusion(ctx, sb)
	if err != nil {
		return "", err
	}
	identity := nodectl.NewToken()
	supply, err := o.placementSupply(ctx, sb, capacity, "")
	if err != nil {
		return "", err
	}
	claim, err := p.state.ReservePlacement(sb.ID, identity, capacity, launchAdmission(sb).Operation, exclude, supply)
	if err != nil {
		return "", err
	}
	bound := false
	defer func() {
		if !bound {
			p.state.ReleasePlacement(sb.ID, identity)
		}
	}()
	return o.runnerPool.assignInResourcePool(ctx, claim.Pool, sb.ID, fence, func(runID string) error {
		m, ok := o.runs.ownership(runID)
		if !ok {
			return fmt.Errorf("missing runner pool identity")
		}
		m.TaskID, m.CapacityMemory, m.CapacityCPU = sb.ID, capacity.MemoryBytes, capacity.CPUMilli
		if err := o.saveRunOwnership(ctx, m); err != nil {
			return err
		}
		if err := commit(runID); err != nil {
			return err
		}
		if err := p.state.BindPlacement(sb.ID, identity, runID); err != nil {
			return err
		}
		bound = true
		return nil
	})
}

// Background recovery filters concrete saved budgets before acquiring a
// runner. Final Admit still checks the exact snapshot budget under the ledger
// lock. Ordinary launches retain their existing runtime admission semantics.
func (o *Orchestrator) placementSupply(ctx context.Context, sb *types.Sandbox, capacity nodectl.Resources, heldPool string) (func(string) bool, error) {
	state := o.memoryPressure.Load().state
	var budget uint64
	if state.IsBackgroundRecovery(sb.ID) {
		saved, err := o.st.RecoveryPlacement(ctx, sb.ID)
		if err != nil {
			return nil, err
		}
		budget = saved.Budget
	}
	return func(name string) bool {
		if name != heldPool && !o.runnerPool.hasSupply(name) {
			return false
		}
		return budget == 0 || state.CanPlace(name, sb.ID, capacity, budget, nodectl.LaunchAdmission{Operation: nodectl.OperationRecovery, SavedSource: true, Background: true})
	}, nil
}

// RestoreRunPoolIdentities validates live durable identities before installing
// templates. A configuration that reassigns a live template to another pool
// must fail before any new placement drop-ins can be applied to that unit.
func (o *Orchestrator) RestoreRunPoolIdentities(ctx context.Context) error {
	return o.restoreResourcePlacements(ctx, nil)
}

// Inventory installs conservative Capacity reservations after these identities
// are loaded. Admission and runner prewarming are still closed at this point.
func (o *Orchestrator) restoreResourcePlacements(ctx context.Context, state *nodectl.State) error {
	if state != nil {
		state.ResolvePool = o.resourcePoolForConsumer
	}
	rows, err := o.st.RunPoolOwnerships(ctx)
	if err != nil {
		return err
	}
	live := map[string]string{}
	for _, kind := range []string{runKindSandbox, runKindBuild} {
		units, err := o.listRunUnits(ctx, kind)
		if err != nil {
			return err
		}
		for _, unit := range units {
			if unit.ActiveState != "inactive" {
				live[o.runIDFromUnit(kind, unit.Name)] = unit.Name
			}
		}
	}
	recorded := map[string]bool{}
	byName := map[string]*runPool{}
	for _, set := range []*runPools{o.runnerPool, o.builderRunPool} {
		for _, p := range set.pools {
			byName[p.kind+":"+p.name] = p
		}
	}
	for _, m := range rows {
		if _, found := live[m.RunID]; !found {
			// Configuration may have removed/renamed a template. Query its
			// persisted actual unit before deciding the execution has exited.
			units, err := o.lc.List(ctx, m.Unit)
			if err != nil {
				return err
			}
			for _, unit := range units {
				if unit.Name == m.Unit && unit.ActiveState != "inactive" {
					live[m.RunID] = unit.Name
				}
			}
		}
		if actual, ok := live[m.RunID]; !ok {
			if err := o.st.DeleteRunPoolOwnership(ctx, m.RunID); err != nil {
				return err
			}
			continue
		} else if actual != m.Unit {
			return fmt.Errorf("run %s unit differs from persisted owner", m.RunID)
		}
		recorded[m.RunID] = true
		pool := byName[m.Kind+":"+m.Pool]
		if pool == nil || pool.resourcePool != m.ResourcePool || pool.unitName(m.RunID) != m.Unit {
			return fmt.Errorf("persisted run %s no longer matches its pool/template", m.RunID)
		}
		if state != nil {
			recovered, err := o.reconcileBuildPoolOwnership(ctx, m)
			if err != nil {
				return err
			}
			if recovered != m {
				if err := o.st.SaveRunPoolOwnership(ctx, recovered); err != nil {
					return err
				}
				m = recovered
			}
		}
		o.runs.mu.Lock()
		if o.runs.owners == nil {
			o.runs.owners = map[string]*runPool{}
		}
		if o.runs.metadata == nil {
			o.runs.metadata = map[string]store.RunPoolOwnership{}
		}
		o.runs.owners[m.RunID], o.runs.metadata[m.RunID] = pool, m
		o.runs.mu.Unlock()
		o.runs.restore(m.RunID, m.Unit)
		if state != nil && m.TaskID != "" && m.CapacityMemory > 0 {
			if err := state.RestorePlacement(nodectl.Placement{SandboxID: m.TaskID, Identity: m.RunID, Pool: m.ResourcePool, Capacity: nodectl.Resources{MemoryBytes: m.CapacityMemory, CPUMilli: m.CapacityCPU}}); err != nil {
				return err
			}
		}
	}
	for runID, unit := range live {
		if recorded[runID] {
			continue
		}
		var candidates []*runPool
		for _, pool := range byName {
			if pool.unitName(runID) == unit {
				candidates = append(candidates, pool)
			}
		}
		if len(candidates) == 0 {
			return fmt.Errorf("unknown live unit %s", unit)
		}
		if len(candidates) > 1 {
			for _, pool := range candidates {
				if pool.capacity != nil {
					return fmt.Errorf("legacy run %s has ambiguous runner pool; cannot enable strict capacity", runID)
				}
			}
			// Resource ownership is unambiguous by the template constraint;
			// historical logical runner-pool ownership remains unknown.
			o.runs.restore(runID, unit)
			continue
		}
		pool := candidates[0]
		m := store.RunPoolOwnership{RunID: runID, Kind: pool.kind, Pool: pool.name, ResourcePool: pool.resourcePool, Unit: unit}
		if pool.kind == runKindSandbox {
			sid, ok, err := o.st.GetSandboxIDByCurrentRunID(ctx, runID)
			if err != nil {
				return err
			}
			if ok {
				m.TaskID = sid
				if state != nil {
					if err := state.BindResourcePool(sid, pool.resourcePool); err != nil {
						return err
					}
				}
			}
		}
		if state != nil && pool.kind == runKindBuild {
			m, err = o.reconcileBuildPoolOwnership(ctx, m)
			if err != nil {
				return err
			}
			if m.TaskID != "" && m.CapacityMemory > 0 {
				if err := state.RestorePlacement(nodectl.Placement{SandboxID: m.TaskID, Identity: m.RunID, Pool: m.ResourcePool, Capacity: nodectl.Resources{MemoryBytes: m.CapacityMemory, CPUMilli: m.CapacityCPU}}); err != nil {
					return err
				}
			}
		}
		if err := o.st.SaveRunPoolOwnership(ctx, m); err != nil {
			return err
		}
		o.runs.mu.Lock()
		if o.runs.owners == nil {
			o.runs.owners = map[string]*runPool{}
		}
		if o.runs.metadata == nil {
			o.runs.metadata = map[string]store.RunPoolOwnership{}
		}
		o.runs.owners[runID], o.runs.metadata[runID] = pool, m
		o.runs.mu.Unlock()
		o.runs.restore(runID, unit)
	}

	return nil
}

func (o *Orchestrator) resourcePoolForConsumer(sid string, pid int, cgroup string) (string, error) {
	if cgroup == "" && pid > 0 {
		body, err := os.ReadFile(fmt.Sprintf("/proc/%d/cgroup", pid))
		if err != nil {
			return "", err
		}
		cgroup = string(body)
	}
	for _, kind := range []string{runKindSandbox, runKindBuild} {
		for _, cfg := range o.poolConfigs(kind) {
			prefix := strings.TrimSuffix(cfg.Unit, ".service")
			for _, part := range strings.Split(strings.TrimSpace(cgroup), "/") {
				if strings.HasPrefix(part, prefix) && strings.HasSuffix(part, ".service") {
					return cfg.ResourcePoolName(), nil
				}
			}
		}
	}
	if pools := o.cfg.ResourceListen.ResourcePoolConfigs(); len(pools) == 1 {
		return pools[0].Name, nil
	}
	return "", fmt.Errorf("cannot determine resource pool for consumer %s", sid)
}
