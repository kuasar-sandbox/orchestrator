package orch

import (
	"context"
	"fmt"

	"github.com/kuasar-sandbox/orchestrator/internal/nodectl"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// Before the first VM starts reserve the largest prospective phase. A/B and C
// execute serially; PostBuildPhase replaces this with the actual phase Capacity
// and waits for Release before retiring it. These are not Builder API quotas or
// outer systemd resource limits.
func buildPlacementCapacity(p *pendingBuild) (nodectl.Resources, error) {
	capacity, err := schedulingCapacity(p.resources)
	if err != nil {
		return capacity, err
	}
	if buildProducesMemorySandbox(p.build, p.sourceHasBuildCommands) {
		c, err := schedulingCapacity(p.sandboxResources)
		if err != nil {
			return capacity, err
		}
		capacity.MemoryBytes = max(capacity.MemoryBytes, c.MemoryBytes)
		capacity.CPUMilli = max(capacity.CPUMilli, c.CPUMilli)
	}
	return capacity, nil
}

func (o *Orchestrator) saveRunOwnership(ctx context.Context, m store.RunPoolOwnership) error {
	o.runs.retireMu.Lock()
	defer o.runs.retireMu.Unlock()
	o.runs.mu.Lock()
	defer o.runs.mu.Unlock()
	if _, exists := o.runs.metadata[m.RunID]; !exists {
		return fmt.Errorf("run %s ownership retired", m.RunID)
	}
	if err := o.st.SaveRunPoolOwnership(ctx, m); err != nil {
		return err
	}
	o.runs.metadata[m.RunID] = m
	return nil
}

func (o *Orchestrator) bindBuilderPlacement(ctx context.Context, b *types.Build, c nodectl.Placement) error {
	m, ok := o.runs.ownership(b.RunID)
	if !ok || m.ResourcePool != c.Pool {
		return fmt.Errorf("builder pool identity changed")
	}
	m.TaskID, m.CapacityMemory, m.CapacityCPU = b.BuildID, c.Capacity.MemoryBytes, c.Capacity.CPUMilli
	if err := o.saveRunOwnership(ctx, m); err != nil {
		return err
	}
	return o.memoryPressure.Load().state.BindPlacement(b.BuildID, c.Identity, b.RunID)
}

func (o *Orchestrator) beginBuildResourcePhase(ctx context.Context, p *pendingBuild, runID, phase, sid string) error {
	controller := o.memoryPressure.Load()
	if controller == nil {
		return nil
	}
	m, ok := o.runs.ownership(runID)
	if !ok {
		return fmt.Errorf("missing builder run pool identity")
	}
	resources := p.resources
	if phase == "c" {
		resources = p.sandboxResources
	}
	capacity, err := schedulingCapacity(resources)
	if err != nil {
		return err
	}
	if err := controller.state.BeginPhase(m.ResourcePool, m.TaskID, sid, runID, capacity); err != nil {
		return err
	}
	m.TaskID, m.CapacityMemory, m.CapacityCPU = sid, capacity.MemoryBytes, capacity.CPUMilli
	if err := o.saveRunOwnership(ctx, m); err != nil {
		controller.state.ReleasePlacement(sid, runID)
		return err
	}
	return nil
}

// Phase completion is durable before this call and has already fenced the VM
// and its runtime reservation. Keep no stale scheduling charge between phases.
func (o *Orchestrator) finishBuildResourcePhase(ctx context.Context, runID, sid string) error {
	controller := o.memoryPressure.Load()
	if controller == nil {
		return nil
	}
	if m, ok := o.runs.ownership(runID); ok && m.TaskID == sid {
		m.TaskID, m.CapacityMemory, m.CapacityCPU = "", 0, 0
		if err := o.saveRunOwnership(ctx, m); err != nil {
			return err
		}
	}
	controller.state.ReleasePlacement(sid, runID)
	return nil
}

// The Build row's phase CAS is authoritative across a crash between phase
// completion and the identity journal update. Never resurrect a finished VM's
// charge; keep prelaunch reservations and reconstruct current phase sizing.
func (o *Orchestrator) reconcileBuildPoolOwnership(ctx context.Context, m store.RunPoolOwnership) (store.RunPoolOwnership, error) {
	if m.Kind != runKindBuild {
		return m, nil
	}
	bid, found, err := o.st.GetClaimedBuildIDByRunID(ctx, m.RunID)
	if err != nil || !found {
		return m, err
	}
	b, err := o.st.GetBuild(ctx, bid)
	if err != nil {
		return m, err
	}
	if b.PhaseSandboxID == "" {
		if m.TaskID != bid {
			m.TaskID, m.CapacityMemory, m.CapacityCPU = "", 0, 0
		}
		return m, nil
	}
	prep, err := decodeBuildRuntimePreparation(b.RuntimePrepareJSON)
	if err != nil {
		return m, err
	}
	resources := prep.Resources
	if b.Phase == "c" {
		resources = prep.SandboxResources
	}
	c, err := schedulingCapacity(resources)
	if err != nil {
		return m, err
	}
	m.TaskID, m.CapacityMemory, m.CapacityCPU = b.PhaseSandboxID, c.MemoryBytes, c.CPUMilli
	return m, nil
}
