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
	prepared := make(map[string]*liveBuildPreparation, len(building))
	// Validate every live worker before adopting any of them. If one external
	// read is transiently unavailable, startup can retry without canceling
	// monitors that this same reconciliation pass already started.
	for _, build := range building {
		if err := types.ValidateBuildID(build.BuildID); err != nil {
			return fmt.Errorf("reconcile build: %w", err)
		}
		if !build.ExecutionClaimed {
			return fmt.Errorf("reconcile build %s: building row has no execution claim", build.BuildID)
		}
		if build.RunID != "" {
			knownRuns[build.RunID] = true
		}
		unit, isLive := live[build.RunID]
		if !isLive || build.ExecutionResult != nil {
			continue
		}
		prep, err := o.prepareLiveBuild(ctx, build, unit)
		if err != nil {
			return err
		}
		prepared[build.BuildID] = prep
	}

	for _, build := range building {
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

type liveBuildPreparation struct {
	spec             sandboxcfg.SandboxSpec
	resources        rtconfig.ResourcesConfig
	sandboxResources rtconfig.ResourcesConfig
	sourceTemplate   bool
	durable          *buildRuntimePreparation
	final            *configsock.BuildSpec
	failureReason    string
}

func (o *Orchestrator) prepareLiveBuild(ctx context.Context, build *types.Build, unit string) (*liveBuildPreparation, error) {
	prep := &liveBuildPreparation{}
	if build.RunID == "" {
		prep.failureReason = "live build has no durable run ownership"
		return prep, nil
	}
	wantProperties, err := builderResourceProperties(build.Resources)
	if err != nil {
		prep.failureReason = "live build has invalid resource properties: " + err.Error()
		return prep, nil
	}
	gotProperties, err := o.lc.Resources(ctx, unit, "Service")
	if err != nil {
		return nil, fmt.Errorf("read live build resource enforcement for %s: %w", unit, err)
	}
	if gotProperties != wantProperties {
		prep.failureReason = fmt.Sprintf("live build resource enforcement does not match: effective=%+v want=%+v", gotProperties, wantProperties)
		return prep, nil
	}
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
	pend := &pendingBuild{
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
	o.pendMu.Lock()
	if o.pend[build.BuildID] != nil {
		o.pendMu.Unlock()
		unlockEventFence(unlockEvent)
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
	o.completeBuildWithPublisher(ctx, build, &result, nil, o.publishBuildStateBestEffort)
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

	port := build.RuntimeVswitchPort
	runtimePersisted := prepared
	var result *buildResult
	var runErr error
	if prepared {
		result, runErr = o.waitRecoveredBuild(ctx, build, pend, unit)
	} else {
		result, port, runtimePersisted, mmdsRow, runErr = o.continueRecoveredBuildPreparation(ctx, build, pend, unit)
	}
	if !errors.Is(runErr, errBuildCleanupPending) {
		runErr = o.cleanupRecoveredBuildRuntime(build, runErr, unit, port, runtimePersisted)
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
	final, err := o.buildSpecForPending(buildCtx, pend)
	if err != nil {
		unlockEventFence(unlockEvent)
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
	unlockEvent := o.lockBuildEvent(build.BuildID)
	defer unlockEventFence(unlockEvent)
	build.Status, build.Reason = types.BuildError, reason
	if !o.persistTerminalBuild(ctx, build) {
		return fmt.Errorf("reconcile build %s: terminal persistence failed", build.BuildID)
	}
	// node-link starts only after reconciliation. Do not let its bounded channel
	// block startup; node-ctl immediately follows with a complete durable replay.
	o.publishBuildStateBestEffort(build.BuildID, "error", "", reason)
	o.observeBuildRemove(build)
	return nil
}
