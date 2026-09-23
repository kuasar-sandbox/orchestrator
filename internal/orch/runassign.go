package orch

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/nodepath"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
)

func (o *Orchestrator) RunPidFile(kind, runID string) (string, bool) {
	if !validRunID(kind, runID) {
		return "", false
	}
	return nodepath.RunnerPID(o.cfg.Paths.RunRoot, runID), true
}

func (o *Orchestrator) WaitAssignment(ctx context.Context, kind, runID string) (string, bool, error) {
	if !validRunID(kind, runID) {
		return "", false, nil
	}
	switch kind {
	case runKindSandbox:
		// BindStartingRunner commits before runPool publishes its response. A retry
		// after response loss resolves the same exact starting assignment without
		// consuming another pool worker.
		sandboxID, found, err := o.st.GetClaimedSandboxIDByRunID(ctx, runID)
		if err != nil {
			return "", false, err
		}
		if found {
			return sandboxID, true, nil
		}
		return o.runs.wait(ctx, runID)
	case runKindBuild:
		// BindBuildRun commits before runPool publishes its response. A retry after
		// response loss (including across a controller restart) therefore resolves
		// the same immutable assignment without consulting process-local pool state.
		buildID, found, err := o.st.GetClaimedBuildIDByRunID(ctx, runID)
		if err != nil {
			return "", false, err
		}
		if found {
			allowed, err := o.st.BuildingTaskIdentity(ctx, buildID, runID)
			return buildID, allowed, err
		}
		buildID, found, err = o.runs.wait(ctx, runID)
		if err != nil || !found {
			return buildID, found, err
		}
		allowed, err := o.st.BuildingTaskIdentity(ctx, buildID, runID)
		return buildID, allowed, err
	default:
		return "", false, nil
	}
}

func (o *Orchestrator) PostSandboxResult(ctx context.Context, runID, sandboxID string, result configsock.SandboxExecutionResult) error {
	inserted, err := o.st.AcceptSandboxExecutionResult(ctx, sandboxID, runID, result)
	if err != nil {
		if errors.Is(err, store.ErrSandboxExecutionOwnership) || errors.Is(err, store.ErrSandboxResultConflict) {
			return configsock.RejectSandboxReport(err)
		}
		return err
	}
	if inserted {
		o.log.Info("sandbox execution result accepted", "sid", sandboxID, "run_id", runID, "stage", result.Stage)
	}
	o.startSandboxResultCleanup(sandboxID, runID)
	return nil
}

func (o *Orchestrator) PostBuildResult(ctx context.Context, runID, buildID string, result configsock.BuildResult) error {
	if err := o.waitBuildRecoveryReady(ctx); err != nil {
		return err
	}
	o.pendMu.Lock()
	pend := o.pend[buildID]
	o.pendMu.Unlock()
	if pend == nil {
		return configsock.RejectBuildReport(fmt.Errorf("unknown build %s", buildID))
	}
	pend.resultMu.Lock()
	defer pend.resultMu.Unlock()
	unlockEvent := o.lockBuildEvent(buildID)
	defer unlockEventFence(unlockEvent)
	build, err := o.st.GetBuild(ctx, buildID)
	if err != nil {
		return err
	}
	if build == nil || build.RunID != runID || (pend.templateID != "" && build.TemplateID != pend.templateID) {
		return configsock.RejectBuildReport(store.ErrBuildExecutionOwnership)
	}
	if err := validateBuildResult(build, result); err != nil {
		return configsock.RejectBuildReport(err)
	}
	if pend.resultClosed && build.ExecutionResult == nil {
		return configsock.RejectBuildReport(fmt.Errorf("build %s no longer accepts results for run %s", buildID, runID))
	}
	if _, err := o.st.AcceptBuildResult(ctx, buildID, runID, result); err != nil {
		if errors.Is(err, store.ErrBuildExecutionOwnership) || errors.Is(err, store.ErrBuildResultConflict) {
			return configsock.RejectBuildReport(err)
		}
		return err
	}
	select {
	case pend.result <- result:
		// The in-memory notification is only a wakeup. SQLite above is the
		// durable authority and is committed before this report is acknowledged.
	default:
		// Identical retries are idempotent and may find the first notification
		// still buffered (or a recovered result preloaded during reconciliation).
	}
	return nil
}

func (o *Orchestrator) PostBuildPhase(ctx context.Context, runID, buildID, phase, sandboxID, state string) error {
	if err := o.waitBuildRecoveryReady(ctx); err != nil {
		return err
	}
	o.pendMu.Lock()
	pend := o.pend[buildID]
	o.pendMu.Unlock()
	if pend == nil {
		return configsock.RejectBuildReport(fmt.Errorf("unknown build %s", buildID))
	}
	if state == "finished" {
		// run-builder reports finished only after sandbox-ctl exited and the
		// trusted phase VMM cgroup read back populated=0. In dynamic mode the
		// controller reservation is a second, independent ownership boundary:
		// do not clear the durable phase (and thereby allow the next phase to
		// reuse the VMM cgroup) until ordinary Release, or the controller's
		// conservative dead-consumer recovery, has removed that reservation.
		if err := o.waitBuildPhaseResourceReleased(ctx, sandboxID); err != nil {
			return fmt.Errorf("wait for build phase %s sandbox %s resource release: %w", phase, sandboxID, err)
		}
	}
	unlockEvent := o.lockBuildEvent(buildID)
	defer unlockEventFence(unlockEvent)
	current, err := o.st.GetBuild(ctx, buildID)
	if err != nil {
		return err
	}
	if current == nil || current.RunID != runID || current.TemplateID != pend.templateID && pend.templateID != "" {
		return configsock.RejectBuildReport(store.ErrBuildExecutionOwnership)
	}
	if err := o.st.SetBuildPhase(ctx, buildID, phase, sandboxID, state); err != nil {
		if errors.Is(err, store.ErrBuildExecutionOwnership) {
			return configsock.RejectBuildReport(err)
		}
		return err
	}
	o.log.Info("build phase", "bid", buildID, "run_id", runID, "phase", phase,
		"sandbox_id", sandboxID, "state", state)
	observed := cloneBuildForObservation(current)
	if state == "finished" {
		observed.Phase, observed.PhaseSandboxID = "", ""
	} else {
		observed.Phase, observed.PhaseSandboxID = phase, sandboxID
	}
	o.observeBuildUpsert(observed)
	return nil
}

const buildPhaseResourceReleasePoll = 25 * time.Millisecond

func (o *Orchestrator) waitBuildPhaseResourceReleased(ctx context.Context, sandboxID string) error {
	// A nil provider is the controller-disabled/static resource mode. There is
	// no nodectl reservation to fence in that mode; sandbox-ctl's process/cgroup
	// teardown remains the phase boundary.
	if o.resourceStats == nil {
		return nil
	}
	ticker := time.NewTicker(buildPhaseResourceReleasePoll)
	defer ticker.Stop()
	for {
		if _, found := o.resourceStats.SandboxResourceStats(sandboxID); !found {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (o *Orchestrator) waitBuildRecoveryReady(ctx context.Context) error {
	// A nil channel belongs only to small unit-test Orchestrator literals and
	// preserves their historical direct-call behavior. New always installs the
	// startup gate.
	if o.buildRecoveryReady == nil {
		return nil
	}
	select {
	case <-o.buildRecoveryReady:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (o *Orchestrator) buildRecoveryReadyNow() bool {
	if o.buildRecoveryReady == nil {
		return true
	}
	select {
	case <-o.buildRecoveryReady:
		return true
	default:
		return false
	}
}
