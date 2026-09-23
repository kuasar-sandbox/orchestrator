package orch

import (
	"context"

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
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
		_, found, err := o.st.GetClaimedSandboxIDByRunID(ctx, runID)
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
	ctx, cancel := cleanupContext()
	defer cancel()
	switch kind {
	case runKindSandbox:
		sid, found, err := o.st.GetClaimedSandboxIDByRunID(ctx, runID)
		if err != nil || !found {
			if err != nil {
				o.log.Warn("run session disconnect owner lookup failed", "kind", kind, "run_id", runID, "err", err)
			}
			return
		}
		sb, err := o.st.Get(ctx, sid)
		if err != nil || sb == nil || sb.RunID != runID {
			if err != nil {
				o.log.Warn("run session disconnect sandbox lookup failed", "sid", sid, "run_id", runID, "err", err)
			}
			return
		}
		if sb.ExecutionResult != nil {
			o.startSandboxResultCleanup(sid, runID)
			return
		}
		unit, err := o.resolveRunUnit(ctx, runKindSandbox, runID)
		if err != nil {
			o.log.Warn("run session disconnect exact unit lookup failed", "sid", sid, "run_id", runID, "err", err)
			return
		}
		if unit == "" {
			o.log.Warn("run session disconnected and exact sandbox unit is absent; existing recovery will retry", "sid", sid, "run_id", runID)
			return
		}
		active, err := o.sandboxUnitActive(ctx, unit)
		if err != nil {
			o.log.Warn("run session disconnect exact unit status failed", "sid", sid, "run_id", runID, "unit", unit, "err", err)
			return
		}
		if active {
			return
		}
		o.log.Warn("run session disconnected and exact sandbox unit is inactive; existing recovery will retry", "sid", sid, "run_id", runID, "unit", unit)
	case runKindBuild:
		// Build result recovery remains owned by the existing build monitor/reconcile
		// path. The shared session is only an exact-run wakeup and must not create a
		// competing cleanup owner.
		return
	}
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
