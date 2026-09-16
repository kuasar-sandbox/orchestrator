package orch

import (
	"context"
	"errors"
	"fmt"
	"time"

	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/keys"
	"github.com/kuasar-sandbox/orchestrator/internal/nodepath"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// reconcileBuilds accounts every durable execution claim against the current
// systemd unit set. A live unit is adopted without re-executing the pipeline;
// an absent unit is cleaned and terminally failed. In both cases the claim is
// released only after exact host ownership cleanup.
func (o *Orchestrator) reconcileBuilds(ctx context.Context) error {
	// A committed terminal deletion is independent of unit enumeration and
	// ordinary execution recovery. Never defer it to terminal TTL.
	deletionErr := o.reapRequestedBuilds(ctx)
	units, err := o.listRunUnits(ctx, runKindBuild)
	if err != nil {
		return errors.Join(deletionErr, fmt.Errorf("reconcile builder units: %w", err))
	}
	live := make(map[string]string)
	all := make(map[string]string)
	for _, unit := range units {
		runID := o.builderUnitToRunID(unit.Name)
		if runID == "" {
			continue
		}
		o.runs.restore(runID, unit.Name)
		all[runID] = unit.Name
		if builderUnitMayHaveProcesses(unit.ActiveState) {
			live[runID] = unit.Name
		}
	}

	building, err := o.st.BuildsRequiringRecovery(ctx)
	if err != nil {
		return err
	}
	knownRuns := make(map[string]bool, len(building))
	for _, build := range building {
		if build.RunID != "" {
			knownRuns[build.RunID] = true
		}
	}
	needsCleanup := func(build *types.Build) bool {
		return build.CancelRequestedUnix != 0 || build.DeleteRequestedUnix != 0 ||
			build.Status != types.BuildBuilding
	}
	finishOwnership := func(build *types.Build) error {
		if err := types.ValidateBuildID(build.BuildID); err != nil {
			return err
		}
		if build.RunID != "" {
			if err := o.stopBuilderRun(build.RunID); err != nil {
				return err
			}
		}
		if err := o.cleanupBuildRuntime(build, build.RuntimeVswitchPort, build.RuntimeVswitchPort != ""); err != nil {
			return err
		}
		if !o.commitBuildCompletion(ctx, build, nil, errors.New(store.BuildCancelledReason), func(_, _, _, _ string) { o.publishCommittedBuild(build) }) {
			return fmt.Errorf("reconcile build %s did not commit terminal ownership release", build.BuildID)
		}
		return nil
	}
	// Stop every bound cancelled/terminal owner before any unrelated adoption
	// preflight. One unavailable ordinary worker must not keep these units live.
	cleanupErrs := []error{deletionErr}
	cleaned := make(map[string]bool)
	for _, build := range building {
		if needsCleanup(build) && build.RunID != "" {
			if err := finishOwnership(build); err != nil {
				cleanupErrs = append(cleanupErrs, err)
			} else {
				cleaned[build.BuildID] = true
			}
		}
	}
	// A claim may have preceded durable RunID binding. Fence old unbound pool
	// units before releasing those claims; they cannot join this config socket.
	for runID, unit := range all {
		if knownRuns[runID] {
			continue
		}
		o.log.Info("reconcile: orphan builder", "run_id", runID, "unit", unit)
		if err := o.stopBuilderUnit(unit); err != nil {
			return errors.Join(append(cleanupErrs, err)...)
		}
		_ = o.lc.ResetFailed(ctx, unit)
		o.runs.forget(runID)
	}
	for _, build := range building {
		if needsCleanup(build) && build.RunID == "" && !cleaned[build.BuildID] {
			if err := finishOwnership(build); err != nil {
				cleanupErrs = append(cleanupErrs, err)
			}
		}
	}
	if err := errors.Join(cleanupErrs...); err != nil {
		return err
	}
	prepared := make(map[string]*liveBuildPreparation, len(building))
	// Validate all ordinary live workers before starting any adoption monitor.
	for _, build := range building {
		if needsCleanup(build) {
			continue
		}
		if err := types.ValidateBuildID(build.BuildID); err != nil {
			return fmt.Errorf("reconcile build: %w", err)
		}
		if !build.ExecutionClaimed {
			return fmt.Errorf("reconcile build %s: building row has no execution claim", build.BuildID)
		}
		_, isLive := live[build.RunID]
		if !isLive || build.ExecutionResult != nil {
			continue
		}
		prep, err := o.prepareLiveBuild(ctx, build)
		if err != nil {
			return err
		}
		prepared[build.BuildID] = prep
	}

	for _, build := range building {
		if needsCleanup(build) {
			continue
		}
		unit, isLive := live[build.RunID]
		if isLive {
			if err := o.adoptLiveBuild(ctx, build, unit, prepared[build.BuildID]); err != nil {
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
		if build.ExecutionResult != nil {
			if err := o.finishRecoveredBuildResult(ctx, build); err != nil {
				return err
			}
			continue
		}
		if err := o.failInterruptedBuild(ctx, build, "build unit was not live after controller restart"); err != nil {
			return err
		}
	}

	return o.reapRequestedBuilds(ctx)
}

type liveBuildPreparation struct {
	spec             sandboxcfg.SandboxSpec
	resources        rtconfig.ResourcesConfig
	sandboxResources rtconfig.ResourcesConfig
	sourceTemplate   bool
	durable          *buildRuntimePreparation
	final            *configsock.BuildSpec
	failureReason    string
}

func (o *Orchestrator) prepareLiveBuild(ctx context.Context, build *types.Build) (*liveBuildPreparation, error) {
	prep := &liveBuildPreparation{}
	if build.RunID == "" {
		prep.failureReason = "live build has no durable run ownership"
		return prep, nil
	}
	var err error
	prep.sourceTemplate, err = buildUsesSandboxTemplate(build)
	if err != nil {
		prep.failureReason = "cannot classify live build: " + err.Error()
		return prep, nil
	}
	// The durable runtime preparation freezes node-resolved resources and
	// network, but the immutable registration still owns portable Create
	// options. Reparse those options on both sides of the runtime-commit
	// boundary so an adopted worker receives the same final Sandbox config.
	prep.spec, err = sandboxcfg.ParseSpec(build.Metadata)
	if err != nil {
		prep.failureReason = "cannot reconstruct live build Sandbox config: " + err.Error()
		return prep, nil
	}
	if build.RuntimeVswitchPort == "" {
		if build.RuntimePrepareJSON != "" {
			prep.failureReason = "live preparing build has preparation without a runtime port"
			return prep, nil
		}
		prep.spec, prep.resources, prep.sandboxResources, err = o.resolveBuildRequestInputs(build, prep.sourceTemplate)
		if err != nil {
			prep.failureReason = "cannot reconstruct live build request: " + err.Error()
		}
		return prep, nil
	}
	durable, err := decodeBuildRuntimePreparation(build.RuntimePrepareJSON)
	if err != nil {
		prep.failureReason = err.Error()
		return prep, nil
	}
	prep.durable = &durable
	preflightPending := &pendingBuild{
		build:                  build,
		runDir:                 nodepath.BuildRunDir(o.cfg.Paths.RunRoot, build.BuildID),
		baseDir:                nodepath.BuildBaseDir(o.cfg.Paths.BaseRoot, build.BuildID),
		sourceTemplate:         prep.sourceTemplate,
		sourceHasBuildCommands: durable.SourceHasBuildCommands,
		spec:                   prep.spec, resources: durable.Resources, sandboxResources: durable.SandboxResources,
		checkpointPolicy: sandboxcfg.CloneSnapshotPolicy(durable.CheckpointPolicy),
		network:          durable.Network, templateNetwork: durable.TemplateNetwork,
		tapFD: o.vs.TapFD(build.RuntimeVswitchPort), mac: build.RuntimePortMAC,
		floating: build.RuntimeFloatingIP, envdToken: build.RuntimeEnvdAccessToken,
	}
	prep.final, err = o.buildSpecForPending(ctx, preflightPending)
	if err != nil {
		return nil, fmt.Errorf("rebuild live build final spec for %s: %w", build.BuildID, err)
	}
	return prep, nil
}

func (o *Orchestrator) adoptLiveBuild(ctx context.Context, build *types.Build, unit string, prep *liveBuildPreparation) error {
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
	if build.ExecutionResult != nil {
		// PostBuildResult acknowledged only after persisting the complete result.
		// Fence the still-live worker and finalize that authoritative result
		// directly: rebuilding already-completed phase inputs would let transient
		// snapshot reads or node-policy drift overwrite an accepted success.
		if err := o.stopBuilderUnit(unit); err != nil {
			return err
		}
		_ = o.lc.ResetFailed(ctx, unit)
		return o.finishRecoveredBuildResult(ctx, build)
	}
	if prep == nil {
		return fmt.Errorf("reconcile build %s: missing live-build preparation", build.BuildID)
	}
	if prep.failureReason != "" {
		if stopErr := o.stopBuilderUnit(unit); stopErr != nil {
			return stopErr
		}
		return o.failInterruptedBuild(ctx, build, prep.failureReason)
	}
	expectedDigest := ""
	if prep.durable != nil {
		expectedDigest = prep.durable.PrepareDigest
	}
	executionCtx, cancelExecution := context.WithCancel(ctx)
	pend := &pendingBuild{
		templateID: build.TemplateID, executionCtx: executionCtx, cancelExecution: cancelExecution, done: make(chan struct{}),
		build:          build,
		runDir:         nodepath.BuildRunDir(o.cfg.Paths.RunRoot, build.BuildID),
		baseDir:        nodepath.BuildBaseDir(o.cfg.Paths.BaseRoot, build.BuildID),
		sourceTemplate: prep.sourceTemplate,
		handoff:        newBuildTaskHandoff(prep.sourceTemplate, expectedDigest),
		spec:           prep.spec, resources: prep.resources, sandboxResources: prep.sandboxResources,
		result: make(chan configsock.BuildResult, 1),
	}
	if prep.durable != nil {
		pend.network, pend.templateNetwork, pend.resources = prep.durable.Network, prep.durable.TemplateNetwork, prep.durable.Resources
		pend.sourceHasBuildCommands = prep.durable.SourceHasBuildCommands
		pend.sandboxResources = prep.durable.SandboxResources
		pend.checkpointPolicy = sandboxcfg.CloneSnapshotPolicy(prep.durable.CheckpointPolicy)
		pend.tapFD, pend.mac = o.vs.TapFD(build.RuntimeVswitchPort), build.RuntimePortMAC
		pend.floating, pend.envdToken = build.RuntimeFloatingIP, build.RuntimeEnvdAccessToken
	}
	if build.ExecutionResult != nil {
		pend.result <- *build.ExecutionResult
	}
	unlockEvent := o.lockBuildEvent(build.BuildID)
	current, readErr := o.st.GetBuild(ctx, build.BuildID)
	if readErr != nil || current == nil || current.TemplateID != build.TemplateID || current.RunID != build.RunID {
		unlockEventFence(unlockEvent)
		cancelExecution()
		return fmt.Errorf("reconcile build %s identity changed: %w", build.BuildID, errors.Join(readErr, store.ErrBuildExecutionOwnership))
	}
	if current.CancelRequestedUnix != 0 || current.DeleteRequestedUnix != 0 {
		unlockEventFence(unlockEvent)
		cancelExecution()
		if err := o.stopBuilderUnit(unit); err != nil {
			return err
		}
		return o.failInterruptedBuild(ctx, current, store.BuildCancelledReason)
	}
	o.pendMu.Lock()
	if o.pend[build.BuildID] != nil {
		o.pendMu.Unlock()
		unlockEventFence(unlockEvent)
		cancelExecution()
		return fmt.Errorf("reconcile build %s: duplicate process-local owner", build.BuildID)
	}
	o.pend[build.BuildID] = pend
	o.pendMu.Unlock()

	var mmdsRow *types.Sandbox
	if prep.durable != nil {
		mmdsRow = o.publishBuildFinal(pend, prep.final)
	}
	stage := "preparing"
	if prep.durable != nil {
		stage = "prepared"
	}
	o.log.Info("reconcile: adopted live build", "bid", build.BuildID, "run_id", build.RunID, "stage", stage)
	o.observeBuildUpsert(build)
	unlockEventFence(unlockEvent)
	started = true
	go func() {
		defer finish()
		o.monitorRecoveredBuild(ctx, build, pend, unit, mmdsRow, prep.durable != nil)
	}()
	return nil
}

func (o *Orchestrator) finishRecoveredBuildResult(ctx context.Context, build *types.Build) error {
	if build.ExecutionResult == nil {
		return fmt.Errorf("reconcile build %s: missing accepted result", build.BuildID)
	}
	if err := o.cleanupBuildRuntime(build, build.RuntimeVswitchPort, build.RuntimeVswitchPort != ""); err != nil {
		return fmt.Errorf("reconcile build %s accepted-result cleanup: %w", build.BuildID, err)
	}
	result := *build.ExecutionResult
	// node-link starts only after reconciliation. The next reconnect full sync
	// reads every durable retained cluster Build directly from SQLite, so startup
	// does not depend on buffering these terminal notifications.
	o.completeBuild(ctx, build, &result, nil)
	if build.ExecutionClaimed {
		return fmt.Errorf("reconcile build %s: accepted result remained nonterminal", build.BuildID)
	}
	return nil
}

func (o *Orchestrator) publishRecoveredBuildMMDS(build *types.Build) *types.Sandbox {
	if build.Profile != types.ProfileE2B || !o.cfg.MMDS.Enabled {
		return nil
	}
	row := &types.Sandbox{
		ID: buildMMDSID(build.BuildID), Profile: build.Profile, TemplateID: build.TemplateID,
		State: types.StateRunning, RunID: build.RunID, FloatingIP: build.RuntimeFloatingIP,
		EnvdAccessToken: build.RuntimeEnvdAccessToken, APISecret: build.APISecret,
		ServiceSecret: build.ServiceSecret, TrafficAccessToken: build.TrafficAccessToken,
		ManifestKey: build.ManifestKey, Metadata: build.Metadata, CreatedUnix: time.Now().Unix(),
	}
	o.setMMDSBuildOwner(row.ID, build.BuildID)
	o.cache(row)
	o.publishUpsert(row)
	return row
}

func (o *Orchestrator) monitorRecoveredBuild(ctx context.Context, build *types.Build, pend *pendingBuild, unit string, mmdsRow *types.Sandbox, prepared bool) {
	defer o.releaseBuildOwner(build, pend)

	port := build.RuntimeVswitchPort
	runtimePersisted := prepared
	var result *buildResult
	var runErr error
	executionCtx := ctx
	if pend.executionCtx != nil {
		executionCtx = pend.executionCtx
	}
	joinStop := o.stopBuildOnCancellation(executionCtx, build.BuildID, unit)
	if prepared {
		result, runErr = o.waitRecoveredBuild(executionCtx, build, pend, unit)
	} else {
		result, port, runtimePersisted, mmdsRow, runErr = o.continueRecoveredBuildPreparation(executionCtx, build, pend, unit)
	}
	joinStop()
	if !errors.Is(runErr, errBuildCleanupPending) {
		runErr = o.cleanupRecoveredBuildRuntime(build, runErr, unit, port, runtimePersisted)
	}
	if mmdsRow != nil {
		o.uncache(mmdsRow.ID)
		o.publishDelete(mmdsRow.ID)
		o.setMMDSBuildOwner(mmdsRow.ID, "")
	}
	o.completeBuild(ctx, build, result, runErr)
}

func (o *Orchestrator) cleanupRecoveredBuildRuntime(
	build *types.Build,
	cause error,
	unit string,
	port string,
	persisted bool,
) error {
	// An adopted builder remains an execution owner even when preparation fails
	// before its connector is durable. Re-establish the exact unit fence before
	// detaching or deleting either derived Build directory on every exit path.
	if err := o.stopBuilderUnit(unit); err != nil {
		return retainBuildCleanup(cause, err, port, persisted)
	}
	progress, cleanupErr := o.cleanupBuildRuntimeProgress(build, port, persisted)
	if cleanupErr == nil {
		return cause
	}
	return retainBuildCleanup(cause, cleanupErr, progress.port, progress.persisted)
}

func (o *Orchestrator) continueRecoveredBuildPreparation(
	ctx context.Context,
	build *types.Build,
	pend *pendingBuild,
	unit string,
) (result *buildResult, portID string, persisted bool, mmdsRow *types.Sandbox, retErr error) {
	deadline := o.buildExecutionDeadline(build)
	buildCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	finalPublished := false
	defer func() {
		if retErr != nil && !finalPublished {
			pend.handoff.PublishFinal(nil, retErr)
		}
	}()

	prepareDigest := fastBuildPrepareDigest(build.BuildID)
	var inherited sandboxcfg.NetworkSpec
	if pend.sourceTemplate {
		summary, early, err := o.waitBuildPrepare(buildCtx, pend, unit)
		if err != nil {
			return nil, "", false, nil, buildFailed("artifact_prepare", err)
		}
		if early != nil {
			accepted, err := o.fenceAcceptedBuildResult(unit, *early)
			return accepted, "", false, nil, buildFailed("runtime", err)
		}
		artifactCapacity, inheritedNetwork, prepareErr := validateBuildPrepareSummary(summary)
		err = prepareErr
		if err != nil {
			return nil, "", false, nil, buildFailed("artifact_prepare", err)
		}
		inherited = inheritedNetwork
		pend.sourceHasBuildCommands = summary.HasBuildCommands
		if buildProducesSandbox(build, pend.sourceHasBuildCommands) {
			pend.sandboxResources, err = o.resolveBuildTargetResources(pend.spec, &artifactCapacity)
			if err != nil {
				return nil, "", false, nil, buildFailed("resource_resolve", err)
			}
		}
		prepareDigest = summary.ResolutionDigest
	}
	var err error
	pend.network, pend.templateNetwork, err = o.resolveBuildNetworks(
		build.Profile, inherited, pend.spec.Network, "build-"+shortID(build.BuildID),
	)
	if err != nil {
		return nil, "", false, nil, buildFailed("resource_resolve", err)
	}
	if buildProducesMemorySandbox(build, pend.sourceHasBuildCommands) {
		pend.checkpointPolicy, err = o.resolveSnapshotPolicy(build.Metadata, sandboxcfg.SnapshotPolicy{})
		if err != nil {
			return nil, "", false, nil, buildFailed("resource_resolve", err)
		}
	}
	port, err := o.attachNetwork(buildCtx, pend.network)
	if err != nil {
		return nil, "", false, nil, buildFailed("network_attach", err)
	}
	portID = port.Port
	envdToken := ""
	if build.Profile == types.ProfileE2B && buildProducesMemorySandbox(build, pend.sourceHasBuildCommands) {
		envdToken = build.EnvdAccessToken
		if envdToken == "" {
			envdToken, err = keys.MintToken()
			if err != nil {
				return nil, portID, false, nil, buildFailed("network_commit", fmt.Errorf("build: mint phase envd token: %w", err))
			}
		}
	}
	durable := buildRuntimePreparation{
		SchemaVersion: buildRuntimePrepareSchemaVersion, PrepareDigest: prepareDigest,
		SourceHasBuildCommands: pend.sourceHasBuildCommands,
		Network:                pend.network, TemplateNetwork: pend.templateNetwork, Resources: pend.resources,
		SandboxResources: pend.sandboxResources,
		CheckpointPolicy: sandboxcfg.CloneSnapshotPolicy(pend.checkpointPolicy),
	}
	prepareJSON, err := encodeBuildRuntimePreparation(durable)
	if err != nil {
		return nil, portID, false, nil, buildFailed("network_commit", err)
	}
	unlockEvent := o.lockBuildEvent(build.BuildID)
	owned, err := o.st.SetBuildRuntimePreparation(buildCtx, build.BuildID, build.RunID,
		port.Port, port.FloatingIP, port.MAC, envdToken, prepareJSON)
	if err != nil || !owned {
		unlockEventFence(unlockEvent)
		if err == nil {
			err = fmt.Errorf("build: exact-run ownership lost during recovered runtime preparation")
		}
		return nil, portID, false, nil, buildFailed("network_commit", err)
	}
	persisted = true
	build.RuntimeVswitchPort, build.RuntimeFloatingIP, build.RuntimePortMAC = port.Port, port.FloatingIP, port.MAC
	build.RuntimeEnvdAccessToken, build.RuntimePrepareJSON = envdToken, prepareJSON
	pend.tapFD, pend.mac, pend.floating = o.vs.TapFD(port.Port), port.MAC, port.FloatingIP
	pend.envdToken = envdToken
	unlockEventFence(unlockEvent)
	final, err := o.buildSpecForPending(buildCtx, pend)
	if err != nil {
		return nil, portID, true, nil, buildFailed("config_write", err)
	}
	unlockEvent = o.lockBuildEvent(build.BuildID)
	allowed, err := o.st.BuildingTaskIdentity(buildCtx, build.BuildID, build.RunID)
	if err != nil || !allowed {
		unlockEventFence(unlockEvent)
		if err == nil {
			err = store.ErrBuildExecutionOwnership
		}
		return nil, portID, true, nil, buildFailed("config_write", err)
	}
	mmdsRow = o.publishBuildFinal(pend, final)
	o.observeBuildUpsert(build)
	unlockEventFence(unlockEvent)
	finalPublished = true
	result, err = o.waitRecoveredBuild(buildCtx, build, pend, unit)
	return result, portID, true, mmdsRow, buildFailed("runtime", err)
}

func (o *Orchestrator) waitRecoveredBuild(ctx context.Context, build *types.Build, pend *pendingBuild, unit string) (*buildResult, error) {
	deadline := o.buildExecutionDeadline(build)
	remaining := time.Until(deadline)
	if remaining <= 0 {
		timeoutErr := fmt.Errorf("build: recovered execution exceeded total timeout")
		if err := o.stopBuilderUnit(unit); err != nil {
			return nil, &buildCleanupPendingError{cause: timeoutErr, cleanup: err}
		}
		return nil, timeoutErr
	}
	waitCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	return o.waitBuildResult(waitCtx, pend, unit)
}

func (o *Orchestrator) failInterruptedBuild(ctx context.Context, build *types.Build, reason string) error {
	if err := o.cleanupBuildRuntime(build, build.RuntimeVswitchPort, build.RuntimeVswitchPort != ""); err != nil {
		return fmt.Errorf("reconcile build %s cleanup: %w", build.BuildID, err)
	}
	o.completeBuild(ctx, build, nil, errors.New(reason))
	if build.ExecutionClaimed {
		return fmt.Errorf("reconcile build %s: terminal persistence failed", build.BuildID)
	}
	return nil
}
