package orch

import (
	"context"
	"errors"
	"sort"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/nodectl"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

var errPressureCaptureStarted = errors.New("pressure capture attempt started")

func (o *Orchestrator) balanceDestination(sb *types.Sandbox, r nodectl.Reservation) string {
	p := o.memoryPressure.Load()
	if p == nil || r.SandboxID == "" {
		return ""
	}
	source := p.state.SandboxPool(sb.ID)
	views := p.state.PoolSnapshots()
	sourceCritical := false
	for _, v := range views {
		if v.Name == source && v.RawZone == nodectl.ZoneCritical {
			sourceCritical = true
		}
	}
	if !sourceCritical {
		return ""
	}
	sort.SliceStable(views, func(i, j int) bool { return views[i].Utilization < views[j].Utilization })
	for _, v := range views {
		if v.Name == source || v.Zone == nodectl.ZoneCritical || !o.runnerPool.hasSupply(v.Name) {
			continue
		}
		if p.state.CanPlace(v.Name, sb.ID, r.Capacity, r.ReservationMemory, nodectl.LaunchAdmission{Operation: nodectl.OperationRecovery, SavedSource: true}) {
			return v.Name
		}
	}
	return ""
}

// The complete scan is one pause opportunity. Only explicit busy samples can
// become fallback candidates; errors and lost ownership are never busy evidence.
func tryIdleFirst(candidates []*types.Sandbox, pause func(*types.Sandbox, bool) error) (string, error) {
	var busy *types.Sandbox
	var last error
	for _, sb := range candidates {
		err := pause(sb, true)
		if err == nil {
			return sb.ID, nil
		}
		last = err
		if errors.Is(err, errPressureCaptureStarted) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return sb.ID, err
		}
		if errors.Is(err, api.ErrSandboxBusy) && busy == nil {
			busy = sb
		}
	}
	if busy != nil {
		return busy.ID, pause(busy, false)
	}
	if last == nil {
		last = api.ErrResourceUnavailable
	}
	return "", last
}

type pressurePausePlan struct {
	sandbox     *types.Sandbox
	destination string
}

func (o *Orchestrator) pressurePausePlans(rows []*types.Sandbox, now time.Time, backoff map[string]time.Time) []pressurePausePlan {
	p := o.memoryPressure.Load()
	states := p.state.PoolStates()
	snapshots := map[string]nodectl.PressureSnapshot{}
	for name, s := range states {
		snapshots[name] = s.PressureSnapshot()
	}
	reservations := map[string]nodectl.Reservation{}
	for _, r := range p.state.ReservationViews() {
		reservations[r.SandboxID] = r
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].RunningSinceUnixNano < rows[j].RunningSinceUnixNano })
	var out []pressurePausePlan
	for _, sb := range rows {
		name := p.state.SandboxPool(sb.ID)
		snapshot := snapshots[name]
		if name == "" && len(states) == 1 {
			for _, v := range snapshots {
				snapshot = v
			}
		}
		if pressurePauseCandidate([]*types.Sandbox{sb}, snapshot, now, backoff, p.policy.MinimumRunTime, p.state.CanPressurePause) == nil {
			continue
		}
		if snapshot.PauseEligible {
			out = append(out, pressurePausePlan{sandbox: sb})
			continue
		}
		if len(states) > 1 && snapshot.RawZone == nodectl.ZoneCritical {
			if destination := o.balanceDestination(sb, reservations[sb.ID]); destination != "" {
				out = append(out, pressurePausePlan{sb, destination})
			}
		}
	}
	return out
}

func (o *Orchestrator) executePressurePausePlans(ctx context.Context, plans []pressurePausePlan) (string, []string, error) {
	var candidates []*types.Sandbox
	destinations := map[string]string{}
	for _, plan := range plans {
		candidates = append(candidates, plan.sandbox)
		destinations[plan.sandbox.ID] = plan.destination
	}
	var failed []string
	sid, err := tryIdleFirst(candidates, func(sb *types.Sandbox, idle bool) error {
		err := o.pauseForPressureMode(ctx, sb, idle, destinations[sb.ID])
		if err != nil {
			failed = append(failed, sb.ID)
		}
		return err
	})
	return sid, failed, err
}

func (o *Orchestrator) recoveryExclusion(ctx context.Context, sb *types.Sandbox) (string, error) {
	if !sb.ResourceObligation {
		return "", nil
	}
	p, err := o.st.RecoveryPlacement(ctx, sb.ID)
	if err != nil {
		return "", err
	}
	if p.DestinationPool != "" {
		return p.SourcePool, nil
	}
	return "", nil
}

func (o *Orchestrator) canBackgroundRecover(ctx context.Context, sb *types.Sandbox) bool {
	p := o.memoryPressure.Load()
	placement, err := o.st.RecoveryPlacement(ctx, sb.ID)
	if err != nil {
		return false
	}
	if placement.Budget == 0 {
		return true
	} // legacy snapshots still get full runtime Admit
	for name := range p.state.PoolStates() {
		if placement.DestinationPool != "" && name == placement.SourcePool {
			continue
		}
		if !o.runnerPool.hasSupply(name) {
			continue
		}
		if p.state.CanPlace(name, sb.ID, nodectl.Resources{MemoryBytes: placement.CapacityMemory, CPUMilli: placement.CapacityCPU}, placement.Budget, nodectl.LaunchAdmission{Operation: nodectl.OperationRecovery, SavedSource: true, Background: true}) {
			return true
		}
	}
	return false
}
