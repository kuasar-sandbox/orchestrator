package orch

import (
	"context"
	"fmt"

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
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
		return o.builderRunPool.WaitAssignment(ctx, runID)
	default:
		return "", false, nil
	}
}

func (o *Orchestrator) PostBuildResult(ctx context.Context, runID, buildID string, result configsock.BuildResult) error {
	o.pendMu.Lock()
	pend := o.pend[buildID]
	o.pendMu.Unlock()
	if pend == nil {
		return fmt.Errorf("unknown build %s", buildID)
	}
	if pend.build.RunID != runID {
		return fmt.Errorf("build %s assigned to run %s, got %s", buildID, pend.build.RunID, runID)
	}
	select {
	case pend.result <- result:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	default:
		return fmt.Errorf("build %s result already posted", buildID)
	}
}

func (o *Orchestrator) PostBuildPhase(ctx context.Context, runID, buildID, phase, sandboxID, state string) error {
	o.pendMu.Lock()
	pend := o.pend[buildID]
	o.pendMu.Unlock()
	if pend == nil {
		return fmt.Errorf("unknown build %s", buildID)
	}
	if pend.build.RunID != runID {
		return fmt.Errorf("build %s assigned to run %s, got %s", buildID, pend.build.RunID, runID)
	}
	if err := o.st.SetBuildPhase(ctx, buildID, phase, sandboxID, state); err != nil {
		return err
	}
	o.log.Info("build phase", "bid", buildID, "run_id", runID, "phase", phase,
		"sandbox_id", sandboxID, "state", state)
	return nil
}
