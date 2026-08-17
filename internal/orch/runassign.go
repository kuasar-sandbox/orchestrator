package orch

import (
	"context"
	"errors"
	"fmt"

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
)

func (o *Orchestrator) RunPidFile(kind, runID string) (string, bool) {
	if !validRunID(kind, runID) {
		return "", false
	}
	switch kind {
	case runKindSandbox:
		return o.runnerPool.runPidFile(runID), true
	case runKindBuild:
		return o.builderRunPool.runPidFile(runID), true
	default:
		return "", false
	}
}

func (o *Orchestrator) WaitAssignment(ctx context.Context, kind, runID string) (string, bool, error) {
	if !validRunID(kind, runID) {
		return "", false, nil
	}
	switch kind {
	case runKindSandbox:
		return o.runnerPool.WaitAssignment(ctx, runID)
	case runKindBuild:
		// BindBuildRun commits before runPool publishes its response. A retry after
		// response loss (including across a controller restart) therefore resolves
		// the same immutable assignment without consulting process-local pool state.
		buildID, found, err := o.st.GetClaimedBuildIDByRunID(ctx, runID)
		if err != nil {
			return "", false, err
		}
		if found {
			return buildID, true, nil
		}
		return o.builderRunPool.WaitAssignment(ctx, runID)
	default:
		return "", false, nil
	}
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
	if pend.build.RunID != runID {
		return configsock.RejectBuildReport(fmt.Errorf("build %s assigned to run %s, got %s", buildID, pend.build.RunID, runID))
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
	if pend.build.RunID != runID {
		return configsock.RejectBuildReport(fmt.Errorf("build %s assigned to run %s, got %s", buildID, pend.build.RunID, runID))
	}
	if err := o.st.SetBuildPhase(ctx, buildID, phase, sandboxID, state); err != nil {
		if errors.Is(err, store.ErrBuildExecutionOwnership) {
			return configsock.RejectBuildReport(err)
		}
		return err
	}
	o.log.Info("build phase", "bid", buildID, "run_id", runID, "phase", phase,
		"sandbox_id", sandboxID, "state", state)
	return nil
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
