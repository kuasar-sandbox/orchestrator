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
		o.startRunDisconnectCheck(kind, runID)
	case runKindBuild:
		// Build result recovery remains owned by the existing build monitor/reconcile
		// path. The shared session is only an exact-run wakeup and must not create a
		// competing cleanup owner.
		return
	}
}

func (o *Orchestrator) startRunDisconnectCheck(kind, runID string) {
	lifecycleCtx := o.launchContext()
	finish, err := o.acceptedOps.Begin(lifecycleCtx)
	if err != nil {
		return
	}
	key := runSessionKey{kind: kind, runID: runID}
	o.runDisconnectMu.Lock()
	if o.runDisconnectActive == nil {
		o.runDisconnectActive = make(map[runSessionKey]struct{})
	}
	if _, exists := o.runDisconnectActive[key]; exists {
		o.runDisconnectMu.Unlock()
		finish()
		return
	}
	o.runDisconnectActive[key] = struct{}{}
	o.runDisconnectMu.Unlock()

	go func() {
		defer finish()
		delay := launchCleanupRetryMin
		for {
			if lifecycleCtx.Err() != nil {
				o.abandonRunDisconnectCheck(key)
				return
			}
			if err := o.checkDisconnectedRunOnce(lifecycleCtx, kind, runID); err != nil {
				o.log.Warn("run session disconnect check incomplete; retrying",
					"kind", kind, "run_id", runID, "retry_in", delay, "err", err)
				if !waitSandboxCleanupRetry(lifecycleCtx, delay) {
					o.abandonRunDisconnectCheck(key)
					return
				}
				delay = nextLaunchCleanupRetry(delay)
				continue
			}
			o.runDisconnectMu.Lock()
			delete(o.runDisconnectActive, key)
			o.runDisconnectMu.Unlock()
			return
		}
	}()
}

func (o *Orchestrator) abandonRunDisconnectCheck(key runSessionKey) {
	o.runDisconnectMu.Lock()
	delete(o.runDisconnectActive, key)
	o.runDisconnectMu.Unlock()
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
	defer unlock()

	sb, err := o.st.Get(checkCtx, sid)
	if err != nil {
		return err
	}
	if sb == nil || sb.RunID != runID {
		return nil
	}
	if sb.ExecutionResult != nil {
		o.startSandboxResultCleanup(sid, runID)
		return nil
	}
	switch sb.State {
	case types.StateStarting:
		o.launches.Cancel(sid)
		return nil
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
	o.startSandboxResultCleanup(sb.ID, sb.RunID)
	return nil
}

func (o *Orchestrator) retireUnassignedRun(kind, runID string) bool {
	unit, waiting := o.runs.owner(runID)
	if !waiting || unit == "" {
		return false
	}
	o.runs.forget(runID)
	go func() {
		ctx, cancel := cleanupContext()
		defer cancel()
		if err := o.lc.Stop(ctx, unit); err != nil {
			o.log.Warn("retire disconnected unassigned run", "kind", kind, "run_id", runID, "unit", unit, "err", err)
			return
		}
		if err := o.lc.ResetFailed(ctx, unit); err != nil {
			o.log.Warn("reset disconnected unassigned run", "kind", kind, "run_id", runID, "unit", unit, "err", err)
		}
	}()
	return true
}

func (o *Orchestrator) runSessionActive(kind, runID string) bool {
	o.runSessionsMu.Lock()
	defer o.runSessionsMu.Unlock()
	return o.runSessions[runSessionKey{kind: kind, runID: runID}] != nil
}

func (o *Orchestrator) runDisconnectCheckActive(kind, runID string) bool {
	o.runDisconnectMu.Lock()
	defer o.runDisconnectMu.Unlock()
	_, active := o.runDisconnectActive[runSessionKey{kind: kind, runID: runID}]
	return active
}

func (k runSessionKey) String() string { return fmt.Sprintf("%s:%s", k.kind, k.runID) }
