package orch

import (
	"context"
	"errors"
	"fmt"
	"time"

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

const sandboxRunEndConcurrency = 4

// Pending work is only an in-memory wakeup for a durable exact-run owner.
// Workers and timers, unlike entries, never scale with the number of RunIDs.
// Failed attempts rejoin the FIFO after their backoff rather than occupying a
// worker while sleeping, so a failed owner cannot starve unrelated cleanup.
type sandboxRunEndWork struct {
	running bool
	again   bool
	due     time.Time
	delay   time.Duration
}

func (o *Orchestrator) startSandboxRunEndCheck(runID string) {
	if runID == "" {
		return
	}
	ctx := o.launchContext()
	finishAdmission, err := o.acceptedOps.Begin(ctx)
	if err != nil {
		return
	}
	defer finishAdmission()

	o.runEndMu.Lock()
	defer o.runEndMu.Unlock()
	if o.runEndActive == nil {
		o.runEndActive = make(map[string]*sandboxRunEndWork)
	}
	if work := o.runEndActive[runID]; work != nil {
		// A notification arriving after a worker's last durable read must not
		// disappear when that worker retires. Queued duplicates already have
		// a future read and do not need another queue entry.
		if work.running {
			work.again = true
		}
		return
	}
	o.runEndActive[runID] = &sandboxRunEndWork{}
	o.runEndQueue = append(o.runEndQueue, runID)
	o.wakeSandboxRunEndLocked()
	limit := o.runEndWorkerLimit
	if limit <= 0 {
		limit = sandboxRunEndConcurrency
	}
	wanted := min(limit, o.runEndWorkers+len(o.runEndQueue))
	for o.runEndWorkers < wanted {
		finish, err := o.acceptedOps.Begin(ctx)
		if err != nil {
			// Shutdown closed admission. The durable row and existing Reaper
			// or startup recovery retain responsibility for pending work.
			if o.runEndWorkers == 0 {
				clear(o.runEndActive)
				o.runEndQueue = nil
			}
			return
		}
		o.runEndWorkers++
		go o.sandboxRunEndWorker(ctx, finish)
	}
}

// Caller holds runEndMu. Closing the generation wakes at most the fixed number
// of workers; there is no per-RunID goroutine, timer or channel.
func (o *Orchestrator) wakeSandboxRunEndLocked() {
	if o.runEndWake != nil {
		close(o.runEndWake)
	}
	o.runEndWake = make(chan struct{})
}

func (o *Orchestrator) sandboxRunEndWorker(ctx context.Context, finish func()) {
	defer finish()
	for {
		o.runEndMu.Lock()
		if ctx.Err() != nil || len(o.runEndQueue) == 0 {
			o.runEndWorkers--
			if o.runEndWorkers == 0 {
				clear(o.runEndActive)
				o.runEndQueue = nil
			}
			o.runEndMu.Unlock()
			return
		}

		now := time.Now()
		var runID string
		var work *sandboxRunEndWork
		var next time.Time
		// Rotate delayed retries behind new work. The usual ready FIFO path
		// is O(1); only a queue consisting of delayed retries needs a scan.
		for remaining := len(o.runEndQueue); remaining > 0; remaining-- {
			id := o.runEndQueue[0]
			o.runEndQueue[0] = ""
			o.runEndQueue = o.runEndQueue[1:]
			candidate := o.runEndActive[id]
			if !candidate.due.After(now) {
				runID, work = id, candidate
				work.running, work.again = true, false
				break
			}
			o.runEndQueue = append(o.runEndQueue, id)
			if next.IsZero() || candidate.due.Before(next) {
				next = candidate.due
			}
		}
		if work == nil {
			wake := o.runEndWake
			o.runEndMu.Unlock()
			timer := time.NewTimer(time.Until(next))
			select {
			case <-ctx.Done():
			case <-wake:
			case <-timer.C:
			}
			timer.Stop()
			continue
		}
		o.runEndMu.Unlock()

		err := o.checkDisconnectedRunOnce(ctx, runKindSandbox, runID)
		o.runEndMu.Lock()
		work.running = false
		switch {
		case ctx.Err() != nil:
			delete(o.runEndActive, runID)
		case err != nil:
			if work.delay == 0 {
				work.delay = launchCleanupRetryMin
			} else {
				work.delay = nextLaunchCleanupRetry(work.delay)
			}
			work.due = time.Now().Add(work.delay)
			o.runEndQueue = append(o.runEndQueue, runID)
		case work.again:
			work.due, work.delay = time.Time{}, 0
			o.runEndQueue = append(o.runEndQueue, runID)
		default:
			delete(o.runEndActive, runID)
		}
		o.wakeSandboxRunEndLocked()
		o.runEndMu.Unlock()
		if err != nil {
			o.log.Warn("sandbox run end check incomplete; retaining retry ownership", "run_id", runID, "err", err)
		}
	}
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
	expected, ok := o.launches.Lookup(sb.ID)
	if !ok || expected.RunID() != sb.RunID {
		return nil
	}
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

	// Unit I/O runs without the SID lock. A finished old attempt, a new run,
	// a running commit or a session reconnect may have won during that I/O.
	unlock := o.lifecycle.Lock(sb.ID)
	defer unlock()
	current, err := o.st.Get(ctx, sb.ID)
	if err != nil {
		return err
	}
	if current == nil || current.State != types.StateStarting || current.RunID != sb.RunID {
		return nil
	}
	o.runSessionsMu.Lock()
	defer o.runSessionsMu.Unlock()
	if current.ExecutionResult == nil && o.runSessions[runSessionKey{kind: runKindSandbox, runID: sb.RunID}] != nil {
		return nil
	}
	o.launches.CancelExact(expected)
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
