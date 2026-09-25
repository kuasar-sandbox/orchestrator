package orch

import (
	"context"
	"errors"
	"fmt"

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

type runSessionKey struct {
	kind  string
	runID string
}

type runSessionGeneration struct {
	owner *Orchestrator
	key   runSessionKey
	gen   uint64
}

func (o *Orchestrator) RegisterRunSession(ctx context.Context, kind, runID string) (configsock.RunSessionRegistration, bool, error) {
	if !validRunID(kind, runID) {
		return nil, false, nil
	}
	known, err := o.runSessionKnownOwner(ctx, kind, runID)
	if err != nil || !known {
		return nil, known, err
	}
	key := runSessionKey{kind: kind, runID: runID}
	o.runSessionsMu.Lock()
	defer o.runSessionsMu.Unlock()
	o.runSessionsNext++
	session := &runSessionGeneration{owner: o, key: key, gen: o.runSessionsNext}
	if o.runSessions == nil {
		o.runSessions = make(map[runSessionKey]*runSessionGeneration)
	}
	o.runSessions[key] = session
	return session, true, nil
}

func (o *Orchestrator) runSessionKnownOwner(ctx context.Context, kind, runID string) (bool, error) {
	if unit, _ := o.runs.owner(runID); unit != "" {
		return true, nil
	}
	switch kind {
	case runKindSandbox:
		_, found, err := o.st.GetSandboxIDByCurrentRunID(ctx, runID)
		return found, err
	case runKindBuild:
		buildID, found, err := o.st.GetClaimedBuildIDByRunID(ctx, runID)
		if err != nil || !found {
			return found, err
		}
		allowed, err := o.st.BuildingTaskIdentity(ctx, buildID, runID)
		return allowed, err
	default:
		return false, nil
	}
}

func (s *runSessionGeneration) Close(shutdown bool) {
	if s == nil || s.owner == nil {
		return
	}
	s.owner.closeRunSession(s.key, s.gen, shutdown)
}

func (o *Orchestrator) closeRunSession(key runSessionKey, gen uint64, shutdown bool) {
	o.runSessionsMu.Lock()
	current := o.runSessions[key]
	if current == nil || current.gen != gen {
		o.runSessionsMu.Unlock()
		return
	}
	delete(o.runSessions, key)
	o.runSessionsMu.Unlock()
	if shutdown {
		return
	}
	o.handleRunSessionDisconnect(key.kind, key.runID)
}

func (o *Orchestrator) handleRunSessionDisconnect(kind, runID string) {
	if o.retireUnassignedRun(kind, runID) {
		return
	}
	switch kind {
	case runKindSandbox:
		o.startSandboxRunEndCheck(runID)
	case runKindBuild:
		// Build result recovery remains owned by the existing build monitor/reconcile
		// path. The shared session is only an exact-run wakeup and must not create a
		// competing cleanup owner.
		return
	}
}

func (o *Orchestrator) startRunDisconnectCheck(kind, runID string) {
	if kind == runKindSandbox {
		o.startSandboxRunEndCheck(runID)
	}
}

func (o *Orchestrator) startSandboxRunEndCheck(runID string) {
	lifecycleCtx := o.launchContext()
	finish, err := o.acceptedOps.Begin(lifecycleCtx)
	if err != nil {
		return
	}
	o.runEndMu.Lock()
	if o.runEndActive == nil {
		o.runEndActive = make(map[string]struct{})
	}
	if _, exists := o.runEndActive[runID]; exists {
		o.runEndMu.Unlock()
		finish()
		return
	}
	o.runEndActive[runID] = struct{}{}
	o.runEndMu.Unlock()

	go func() {
		defer finish()
		defer func() {
			o.runEndMu.Lock()
			delete(o.runEndActive, runID)
			o.runEndMu.Unlock()
		}()
		select {
		case o.runEndSlots <- struct{}{}:
			defer func() { <-o.runEndSlots }()
		case <-lifecycleCtx.Done():
			return
		}
		delay := launchCleanupRetryMin
		for {
			if lifecycleCtx.Err() != nil {
				return
			}
			if err := o.checkDisconnectedRunOnce(lifecycleCtx, runKindSandbox, runID); err != nil {
				o.log.Warn("sandbox run end check incomplete; retrying",
					"run_id", runID, "retry_in", delay, "err", err)
				if !waitSandboxCleanupRetry(lifecycleCtx, delay) {
					return
				}
				delay = nextLaunchCleanupRetry(delay)
				continue
			}
			return
		}
	}()
}

func (o *Orchestrator) checkDisconnectedRunOnce(ctx context.Context, kind, runID string) error {
	if kind != runKindSandbox {
		return nil
	}
	checkCtx, cancel := cleanupContext()
	defer cancel()
	sid, found, err := o.st.GetSandboxIDByCurrentRunID(checkCtx, runID)
	if err != nil || !found {
		return err
	}

	unlock := o.lifecycle.Lock(sid)
	sb, err := o.st.Get(checkCtx, sid)
	unlock()
	if err != nil {
		return err
	}
	if sb == nil || sb.RunID != runID {
		return nil
	}
	if sb.ExecutionResult != nil {
		return o.finalizeSandboxResultOnce(ctx, sid, runID)
	}
	if o.runSessionActive(kind, runID) {
		return nil
	}
	switch sb.State {
	case types.StateStarting:
		return o.checkDisconnectedStartingSandbox(checkCtx, sb)
	case types.StateRunning:
		return o.checkDisconnectedRunningSandbox(checkCtx, sb)
	case types.StatePaused:
		if pausedCleanupPending(sb) {
			o.startPausedCleanupRetry(sid)
		}
		return nil
	case types.StateDeleting:
		o.startSandboxDeleteFinalizer(sid)
		return nil
	case types.StateDead:
		return nil
	default:
		return nil
	}
}

func (o *Orchestrator) checkDisconnectedStartingSandbox(ctx context.Context, sb *types.Sandbox) error {
	unit, err := o.resolveRunUnit(ctx, runKindSandbox, sb.RunID)
	if err != nil {
		return err
	}
	if unit != "" {
		active, err := o.sandboxUnitActive(ctx, unit)
		if err != nil {
			return err
		}
		if active {
			return nil
		}
	}
	o.launches.Cancel(sb.ID)
	return nil
}

func (o *Orchestrator) checkDisconnectedRunningSandbox(ctx context.Context, sb *types.Sandbox) error {
	unit, err := o.resolveRunUnit(ctx, runKindSandbox, sb.RunID)
	if err != nil {
		return err
	}
	if unit != "" {
		active, err := o.sandboxUnitActive(ctx, unit)
		if err != nil {
			return err
		}
		if active {
			return nil
		}
	}
	result := types.SandboxExecutionResult{
		SID:   sb.ID,
		RunID: sb.RunID,
		Stage: types.SandboxResultRun,
		Error: "runner session disconnected and exact unit is not active",
	}
	inserted, err := o.st.AcceptSandboxExecutionResult(ctx, sb.ID, sb.RunID, result)
	if err != nil {
		if errors.Is(err, store.ErrSandboxExecutionOwnership) || errors.Is(err, store.ErrSandboxResultConflict) {
			return nil
		}
		return err
	}
	if inserted {
		o.log.Info("sandbox execution result accepted from disconnected run", "sid", sb.ID, "run_id", sb.RunID)
	}
	return o.finalizeSandboxResultOnce(ctx, sb.ID, sb.RunID)
}

func (o *Orchestrator) retireUnassignedRun(kind, runID string) bool {
	unit, pool, waiting := o.runs.ownerPool(runID)
	if !waiting || unit == "" || pool == nil {
		return false
	}
	ctx, cancel := cleanupContext()
	defer cancel()
	unit, retired, err := pool.RetireUnassigned(ctx, runID, func(checkCtx context.Context) (bool, error) {
		assigned, err := o.runSessionDurableAssigned(checkCtx, kind, runID)
		if err != nil || assigned {
			return false, err
		}
		return true, nil
	})
	if err != nil {
		o.log.Warn("retire disconnected unassigned run", "kind", kind, "run_id", runID, "unit", unit, "err", err)
		return false
	}
	if !retired {
		return false
	}
	o.log.Info("retired disconnected unassigned run", "kind", kind, "run_id", runID, "unit", unit)
	return true
}

func (o *Orchestrator) runSessionDurableAssigned(ctx context.Context, kind, runID string) (bool, error) {
	switch kind {
	case runKindSandbox:
		_, found, err := o.st.GetSandboxIDByCurrentRunID(ctx, runID)
		return found, err
	case runKindBuild:
		buildID, found, err := o.st.GetClaimedBuildIDByRunID(ctx, runID)
		if err != nil || !found {
			return found, err
		}
		allowed, err := o.st.BuildingTaskIdentity(ctx, buildID, runID)
		return allowed, err
	default:
		return false, nil
	}
}

func (o *Orchestrator) runSessionAssignmentActive(kind, runID string) bool {
	if o.runSessionActive(kind, runID) {
		return true
	}
	return o.allowLegacyAssignmentWithoutRunSession
}

func (o *Orchestrator) runSessionActive(kind, runID string) bool {
	o.runSessionsMu.Lock()
	defer o.runSessionsMu.Unlock()
	return o.runSessions[runSessionKey{kind: kind, runID: runID}] != nil
}

func (o *Orchestrator) runDisconnectCheckActive(kind, runID string) bool {
	if kind != runKindSandbox {
		return false
	}
	o.runEndMu.Lock()
	defer o.runEndMu.Unlock()
	_, active := o.runEndActive[runID]
	return active
}

func (k runSessionKey) String() string { return fmt.Sprintf("%s:%s", k.kind, k.runID) }
