package orch

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// reconcileBuilds accounts every durable execution claim against the current
// systemd unit set. A live unit is adopted without re-executing the pipeline;
// an absent unit is cleaned and terminally failed. In both cases the claim is
// released only after exact host ownership cleanup.
func (o *Orchestrator) reconcileBuilds(ctx context.Context) error {
	units, err := o.lc.List(ctx, o.builderPattern())
	if err != nil {
		return fmt.Errorf("reconcile builder units: %w", err)
	}
	live := make(map[string]string)
	all := make(map[string]string)
	for _, unit := range units {
		runID := o.builderUnitToRunID(unit.Name)
		if runID == "" {
			continue
		}
		all[runID] = unit.Name
		if builderUnitMayHaveProcesses(unit.ActiveState) {
			live[runID] = unit.Name
		}
	}

	building, err := o.st.BuildsByStatus(ctx, types.BuildBuilding)
	if err != nil {
		return err
	}
	knownRuns := make(map[string]bool, len(building))
	for _, build := range building {
		if !build.ExecutionClaimed {
			return fmt.Errorf("reconcile build %s: building row has no execution claim", build.BuildID)
		}
		if build.RunID != "" {
			knownRuns[build.RunID] = true
		}
		unit, isLive := live[build.RunID]
		if isLive {
			if err := o.adoptLiveBuild(ctx, build, unit); err != nil {
				return err
			}
			continue
		}
		if unit = all[build.RunID]; unit != "" {
			// A non-live failed unit has no running process, but stop
			// still settles any outstanding systemd job before host ownership is
			// reclaimed.
			if err := o.stopBuilderUnit(unit); err != nil {
				return err
			}
			_ = o.lc.ResetFailed(ctx, unit)
		}
		if err := o.failInterruptedBuild(ctx, build, "build unit was not live after controller restart"); err != nil {
			return err
		}
	}

	// Prestarted units from the previous config-socket cannot join the new pool;
	// stop them after all durable run-id owners have been identified.
	for runID, unit := range all {
		if knownRuns[runID] {
			continue
		}
		o.log.Info("reconcile: orphan builder", "run_id", runID, "unit", unit)
		if err := o.stopBuilderUnit(unit); err != nil {
			return err
		}
		_ = o.lc.ResetFailed(ctx, unit)
	}
	return nil
}

func (o *Orchestrator) adoptLiveBuild(ctx context.Context, build *types.Build, unit string) error {
	finish, err := o.buildOps.Begin(ctx)
	if err != nil {
		return err
	}
	started := false
	defer func() {
		if !started {
			finish()
		}
	}()
	if build.RunID == "" || build.RuntimeVswitchPort == "" {
		if err := o.stopBuilderUnit(unit); err != nil {
			return err
		}
		return o.failInterruptedBuild(ctx, build, "live build has incomplete durable runtime ownership")
	}
	wantProperties, err := builderResourceProperties(build.Resources)
	if err != nil {
		if stopErr := o.stopBuilderUnit(unit); stopErr != nil {
			return stopErr
		}
		return o.failInterruptedBuild(ctx, build, "live build has invalid resource properties: "+err.Error())
	}
	gotProperties, err := o.lc.Resources(ctx, unit, "Service")
	if err != nil || gotProperties != wantProperties {
		if stopErr := o.stopBuilderUnit(unit); stopErr != nil {
			return stopErr
		}
		return o.failInterruptedBuild(ctx, build,
			fmt.Sprintf("live build resource enforcement cannot be verified: effective=%+v want=%+v err=%v", gotProperties, wantProperties, err))
	}
	spec, network, templateNetwork, resources, err := o.resolveBuildPhaseInputs(ctx, build)
	if err != nil {
		if stopErr := o.stopBuilderUnit(unit); stopErr != nil {
			return stopErr
		}
		return o.failInterruptedBuild(ctx, build, "cannot reconstruct live build: "+err.Error())
	}
	pend := &pendingBuild{
		build: build, workdir: filepath.Join(o.cfg.Paths.RunRoot, build.BuildID), spec: spec,
		network: network, templateNetwork: templateNetwork, resources: resources,
		tapFD: o.vs.TapFD(build.RuntimeVswitchPort), mac: build.RuntimePortMAC,
		floating: build.RuntimeFloatingIP, envdToken: build.RuntimeEnvdAccessToken,
		result: make(chan configsock.BuildResult, 1),
	}
	o.pendMu.Lock()
	if o.pend[build.BuildID] != nil {
		o.pendMu.Unlock()
		return fmt.Errorf("reconcile build %s: duplicate process-local owner", build.BuildID)
	}
	o.pend[build.BuildID] = pend
	o.pendMu.Unlock()

	mmdsRow := o.publishRecoveredBuildMMDS(build)
	o.log.Info("reconcile: adopted live build", "bid", build.BuildID, "run_id", build.RunID)
	started = true
	go func() {
		defer finish()
		o.monitorRecoveredBuild(ctx, build, pend, unit, mmdsRow)
	}()
	return nil
}

func (o *Orchestrator) publishRecoveredBuildMMDS(build *types.Build) *types.Sandbox {
	if build.Profile != types.ProfileE2B || !o.cfg.MMDS.Enabled {
		return nil
	}
	row := &types.Sandbox{
		ID: "build-" + build.BuildID, Profile: build.Profile, TemplateID: build.TemplateID,
		State: types.StateRunning, RunID: build.RunID, FloatingIP: build.RuntimeFloatingIP,
		EnvdAccessToken: build.RuntimeEnvdAccessToken, APISecret: build.APISecret,
		ManifestKey: build.ManifestKey, Metadata: build.Metadata, CreatedUnix: time.Now().Unix(),
	}
	o.setMMDSBuildOwner(row.ID, build.BuildID)
	o.cache(row)
	o.publishUpsert(row)
	return row
}

func (o *Orchestrator) monitorRecoveredBuild(ctx context.Context, build *types.Build, pend *pendingBuild, unit string, mmdsRow *types.Sandbox) {
	defer func() {
		o.pendMu.Lock()
		delete(o.pend, build.BuildID)
		o.pendMu.Unlock()
		if mmdsRow != nil {
			o.uncache(mmdsRow.ID)
			o.publishDelete(mmdsRow.ID)
			o.setMMDSBuildOwner(mmdsRow.ID, "")
		}
	}()

	result, runErr := o.waitRecoveredBuild(ctx, build, pend, unit)
	if !errors.Is(runErr, errBuildCleanupPending) {
		cleanupErr := o.cleanupBuildRuntime(build, build.RuntimeVswitchPort, pend.workdir, true)
		if cleanupErr != nil {
			runErr = &buildCleanupPendingError{cause: runErr, cleanup: cleanupErr}
		}
	}
	o.completeBuild(ctx, build, result, runErr)
}

func (o *Orchestrator) waitRecoveredBuild(ctx context.Context, build *types.Build, pend *pendingBuild, unit string) (*buildResult, error) {
	deadline := time.Now().Add(time.Duration(o.cfg.Builder.TotalTimeoutSec+60) * time.Second)
	if build.ExecutionClaimedUnix > 0 {
		deadline = time.Unix(build.ExecutionClaimedUnix, 0).Add(time.Duration(o.cfg.Builder.TotalTimeoutSec+60) * time.Second)
	}
	remaining := time.Until(deadline)
	if remaining <= 0 {
		timeoutErr := fmt.Errorf("build: recovered execution exceeded total timeout")
		if err := o.stopBuilderUnit(unit); err != nil {
			return nil, &buildCleanupPendingError{cause: timeoutErr, cleanup: err}
		}
		return nil, timeoutErr
	}
	timer := time.NewTimer(remaining)
	defer timer.Stop()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case result := <-pend.result:
			if err := o.waitBuilderUnitExit(ctx, unit, 20*time.Second); err != nil {
				if stopErr := o.stopBuilderUnit(unit); stopErr != nil {
					return nil, &buildCleanupPendingError{cause: err, cleanup: stopErr}
				}
				return nil, err
			}
			return &result, nil
		case <-ctx.Done():
			if err := o.stopBuilderUnit(unit); err != nil {
				return nil, &buildCleanupPendingError{cause: ctx.Err(), cleanup: err}
			}
			return nil, ctx.Err()
		case <-timer.C:
			if err := o.stopBuilderUnit(unit); err != nil {
				return nil, &buildCleanupPendingError{cause: fmt.Errorf("build: recovered execution timed out"), cleanup: err}
			}
			return nil, fmt.Errorf("build: recovered execution timed out")
		case <-ticker.C:
			if !o.unitActive(ctx, unit) {
				return nil, fmt.Errorf("build: recovered unit %s exited without result", unit)
			}
		}
	}
}

func (o *Orchestrator) failInterruptedBuild(ctx context.Context, build *types.Build, reason string) error {
	if err := o.cleanupBuildRuntime(build, build.RuntimeVswitchPort, filepath.Join(o.cfg.Paths.RunRoot, build.BuildID), build.RuntimeVswitchPort != ""); err != nil {
		return fmt.Errorf("reconcile build %s cleanup: %w", build.BuildID, err)
	}
	build.Status, build.Reason = types.BuildError, reason
	if !o.persistTerminalBuild(ctx, build) {
		return fmt.Errorf("reconcile build %s: terminal persistence failed", build.BuildID)
	}
	o.publishBuildState(build.BuildID, "error", "", reason)
	return nil
}
