package orch

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/configresolve"
	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
)

const (
	maxRequiredSnapshotRefs       = 1024
	maxSnapshotNetworkMetadataLen = 1 << 20
)

type prepareWaitResult struct {
	summary configsock.SnapshotPrepareSummary
	err     error
}

// launchRestoreSandbox owns the host half of the exact-run two-stage protocol.
// The conductor consumes only the task's non-secret summary; it never opens or
// parses a snapshot artifact.
func (o *Orchestrator) launchRestoreSandbox(ctx context.Context, attempt *launchAttempt, sb *types.Sandbox, tmpl types.TemplateID, preparation *launchPreparation) (retErr error) {
	prepareStarted := time.Now()
	for _, dir := range []string{sb.RunDir, sb.BaseDir} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return launchFailed("snapshot_prepare", fmt.Errorf("orch: mkdir %s: %w", dir, err))
		}
	}
	if err := os.Chmod(sb.RunDir, 0o700); err != nil {
		return launchFailed("snapshot_prepare", fmt.Errorf("orch: chmod %s: %w", sb.RunDir, err))
	}

	// The listener exists before assignment becomes visible. node-ctl connects
	// immediately after WaitAssignment, making a task exit during root reading an
	// observable EOF instead of a launch-wide timeout.
	readyListener, err := listenRuntimeReadiness(o.cfg.Paths.RunRoot, sb.ID)
	if err != nil {
		return launchFailed("snapshot_prepare", err)
	}
	defer readyListener.Close()

	assignStarted := time.Now()
	assignmentCtx, cancelAssignment := context.WithTimeout(ctx, o.cfg.Units.PoolWaitDuration())
	var commitStarted, commitFinished time.Time
	runID, err := o.runnerPool.Assign(assignmentCtx, sb.ID, func(runID string) error {
		commitStarted = time.Now()
		unlock := o.lifecycle.Lock(sb.ID)
		defer unlock()
		changed, err := o.st.BindStartingRunner(ctx, sb.ID, runID)
		if err != nil {
			return err
		}
		if !changed {
			return errLaunchOwnershipLost
		}
		launchTimeout := o.sandboxReadyTimeout
		if launchTimeout <= 0 {
			launchTimeout = 60 * time.Second
		}
		attempt.SetDeadline(time.Now().Add(launchTimeout))
		sb.RunID = runID
		attempt.SetRunID(runID)
		bound := o.mutateCached(sb.ID, func(cached *types.Sandbox) { cached.RunID = runID })
		if bound == nil {
			bound = cloneSandbox(sb)
			o.cache(bound)
		}
		o.publishUpsert(bound)
		o.observeSandboxUpsert(bound)
		commitFinished = time.Now()
		return nil
	})
	cancelAssignment()
	waitFinished := time.Now()
	if !commitStarted.IsZero() {
		waitFinished = commitStarted
	}
	o.logLaunchPhase(attempt, sb, "runner_wait_duration", waitFinished.Sub(assignStarted))
	if !commitStarted.IsZero() {
		if commitFinished.IsZero() {
			commitFinished = time.Now()
		}
		o.logLaunchPhase(attempt, sb, "runner_commit_duration", commitFinished.Sub(commitStarted))
	}
	if err != nil {
		return launchFailed("snapshot_prepare", err)
	}
	if sb.RunID == "" {
		sb.RunID = runID
		attempt.SetRunID(runID)
	}
	o.logLaunchPhase(attempt, sb, "runner_handoff_duration", time.Since(commitFinished))

	deadline := attempt.Deadline()
	if deadline.IsZero() {
		return launchFailed("snapshot_prepare", errors.New("orch: restore launch deadline was not initialized"))
	}
	launchCtx, cancelLaunch := context.WithDeadline(ctx, deadline)
	defer cancelLaunch()
	finalPublished := false
	defer func() {
		if retErr != nil && !finalPublished {
			attempt.PublishFinalError(retErr)
		}
	}()

	readinessResult := make(chan error, 1)
	go func() {
		readyErr := waitRuntimeReadiness(launchCtx, readyListener)
		readinessResult <- readyErr
		if readyErr != nil {
			cancelLaunch()
		}
	}()
	prepareResult := make(chan prepareWaitResult, 1)
	go func() {
		summary, waitErr := attempt.WaitPrepare(launchCtx)
		prepareResult <- prepareWaitResult{summary: summary, err: waitErr}
	}()

	var summary configsock.SnapshotPrepareSummary
	select {
	case result := <-prepareResult:
		if result.err != nil {
			return launchFailed("snapshot_prepare", result.err)
		}
		summary = result.summary
	case readyErr := <-readinessResult:
		if readyErr == nil {
			readyErr = errors.New("orch: runtime readiness completed before snapshot preparation")
		}
		return launchFailed("snapshot_prepare", readyErr)
	case <-launchCtx.Done():
		return launchFailed("snapshot_prepare", launchCtx.Err())
	}

	snapshotCapacity, inheritedNetwork, err := validateSandboxPrepareSummary(summary)
	if err != nil {
		return launchFailed("snapshot_prepare", err)
	}
	hostPrepareStarted := time.Now()
	o.log.Info("sandbox task snapshot prepared",
		"sid", sb.ID,
		"run_id", sb.RunID,
		"task_snapshot_ref_count", summary.RequiredRefCount,
		"snapshot_prepare_handoff_duration", time.Since(prepareStarted))

	resources, err := o.resolveRestoredResources(preparation.Spec, snapshotCapacity)
	if err != nil {
		return launchFailed("resource_resolve", fmt.Errorf("orch: resolve restored sandbox resources: %w", err))
	}
	mergedNetwork := sandboxcfg.MergeNetwork(inheritedNetwork, preparation.Spec.Network)
	network, err := o.resolveNetwork(tmpl.Profile, mergedNetwork, o.cfg.Sandbox.Network.Hostname)
	if err != nil {
		return launchFailed("resource_resolve", err)
	}

	port, err := o.attachNetwork(launchCtx, network)
	if err != nil {
		return launchFailed("network_attach", err)
	}
	sb.VswitchPort, sb.FloatingIP, sb.PortMAC, sb.InnerIP = port.Port, port.FloatingIP, port.MAC, network.InnerIP
	resourcesOwned := store.StartingResources{
		FloatingIP: sb.FloatingIP, VswitchPort: sb.VswitchPort, InnerIP: sb.InnerIP, PortMAC: sb.PortMAC,
	}

	// Serialize exact ownership commit, YAML publication, and route publication
	// with Delete/Kill. If the lifecycle fence was lost, detach the uncommitted
	// port immediately; rollback retains it only when that detach itself fails.
	unlock := o.lifecycle.Lock(sb.ID)
	if err := launchCtx.Err(); err != nil {
		unlock()
		return launchFailed("network_commit", o.detachUncommittedLaunchPort(sb, port.Port, err))
	}
	changed, err := o.st.SetStartingResourcesForRun(launchCtx, sb.ID, sb.RunID, resourcesOwned)
	if err != nil || !changed {
		if err == nil {
			err = errLaunchOwnershipLost
		}
		unlock()
		return launchFailed("network_commit", o.detachUncommittedLaunchPort(sb, port.Port, err))
	}
	p := o.sandboxParams(sb, tmpl, preparation.Spec, network, resources)
	if err := p.WriteYAML(o.sandboxConfigPath(sb)); err != nil {
		unlock()
		return launchFailed("config_write", err)
	}
	updated := o.mutateCached(sb.ID, func(cached *types.Sandbox) {
		cached.VswitchPort = sb.VswitchPort
		cached.FloatingIP = sb.FloatingIP
		cached.PortMAC = sb.PortMAC
		cached.InnerIP = sb.InnerIP
	})
	if updated == nil {
		updated = cloneSandbox(sb)
		o.cache(updated)
	}
	o.publishUpsert(updated)
	o.observeSandboxUpsert(updated)
	unlock()

	if err := launchCtx.Err(); err != nil {
		return launchFailed("config_write", err)
	}
	final := o.sandboxFinalLaunchSpec(sb, tmpl, preparation.Spec, nil)
	attempt.PublishFinalSpec(final)
	finalPublished = true
	o.logLaunchPhase(attempt, sb, "host_resource_network_prepare_duration", time.Since(hostPrepareStarted))
	o.logLaunchPhase(attempt, sb, "prepare_duration", time.Since(prepareStarted))

	runtimeStarted := time.Now()
	select {
	case err = <-readinessResult:
	case <-launchCtx.Done():
		err = launchCtx.Err()
	}
	o.logLaunchPhase(attempt, sb, "runtime_ready_duration", time.Since(runtimeStarted))
	if err != nil {
		return launchFailed("runtime", fmt.Errorf("orch: sandbox %s: %w", sb.ID, err))
	}
	if tmpl.Profile == types.ProfileE2B {
		envdStarted := time.Now()
		err = o.envdInit(launchCtx, sb)
		o.logLaunchPhase(attempt, sb, "envd_init_duration", time.Since(envdStarted))
		if err != nil {
			return launchFailed("init", err)
		}
	}

	unlock = o.lifecycle.Lock(sb.ID)
	defer unlock()
	changed, err = o.st.CommitStartingRunning(launchCtx, sb.ID, sb.RunID)
	if err != nil {
		return launchFailed("runtime", fmt.Errorf("orch: commit launch %s: %w", sb.ID, err))
	}
	if !changed {
		return launchFailed("runtime", fmt.Errorf("orch: commit launch %s: exact runner ownership lost: %w", sb.ID, errLaunchOwnershipLost))
	}
	if attempt.Kind() == launchResume {
		o.clearDeadlineIntent(sb.ID)
	}
	running := o.mutateCached(sb.ID, func(cached *types.Sandbox) {
		cached.State = types.StateRunning
		cached.RunID = sb.RunID
	})
	if running == nil {
		running = cloneSandbox(sb)
		running.State = types.StateRunning
		o.cache(running)
	}
	sb.State = types.StateRunning
	sb.DeadlineUnix = running.DeadlineUnix
	o.publishUpsert(running)
	o.observeSandboxUpsert(running)
	return nil
}

func (o *Orchestrator) resolveRestoredResources(spec sandboxcfg.SandboxSpec, snapshotCapacity rtconfig.CapacityConfig) (rtconfig.ResourcesConfig, error) {
	dynamic := o.cfg.ResourceListen != nil && o.cfg.ResourceListen.Enabled
	return sandboxcfg.ResolveResources(sandboxcfg.ResourceResolveInput{
		Node:                     configresolve.SandboxResources(o.cfg.Sandbox.Resources),
		Patch:                    spec.Resource,
		Restore:                  true,
		SnapshotCapacity:         &snapshotCapacity,
		Dynamic:                  dynamic,
		ControllerSocketIdentity: o.resourceControllerSocketIdentity,
	})
}

func validateSandboxPrepareSummary(summary configsock.SnapshotPrepareSummary) (rtconfig.CapacityConfig, sandboxcfg.NetworkSpec, error) {
	if summary.SchemaVersion != configsock.SnapshotPrepareSchemaVersion {
		return rtconfig.CapacityConfig{}, sandboxcfg.NetworkSpec{}, fmt.Errorf("orch: unsupported snapshot prepare schema %d", summary.SchemaVersion)
	}
	if summary.RequiredRefCount < 1 || summary.RequiredRefCount > maxRequiredSnapshotRefs {
		return rtconfig.CapacityConfig{}, sandboxcfg.NetworkSpec{}, fmt.Errorf("orch: snapshot required ref count %d outside 1..%d", summary.RequiredRefCount, maxRequiredSnapshotRefs)
	}
	digest, err := hex.DecodeString(summary.ResolutionDigest)
	if err != nil || len(digest) != 32 {
		return rtconfig.CapacityConfig{}, sandboxcfg.NetworkSpec{}, errors.New("orch: snapshot resolution digest is not SHA-256")
	}
	if len(summary.RawNetworkMetadata) > maxSnapshotNetworkMetadataLen {
		return rtconfig.CapacityConfig{}, sandboxcfg.NetworkSpec{}, fmt.Errorf("orch: snapshot network metadata exceeds %d bytes", maxSnapshotNetworkMetadataLen)
	}
	capacity := rtconfig.CapacityConfig{CPU: summary.Capacity.CPU, Memory: summary.Capacity.Memory}
	if capacity.CPU <= 0 || strings.TrimSpace(capacity.Memory) == "" {
		return rtconfig.CapacityConfig{}, sandboxcfg.NetworkSpec{}, errors.New("orch: snapshot config has no usable resources.capacity")
	}
	var inherited sandboxcfg.NetworkSpec
	if raw := strings.TrimSpace(summary.RawNetworkMetadata); raw != "" {
		// Sandbox inheritance intentionally remains best-effort: malformed legacy
		// metadata behaves as an empty inherited network.
		var parsed sandboxcfg.NetworkSpec
		if err := json.Unmarshal([]byte(raw), &parsed); err == nil {
			inherited = parsed
		}
	}
	return capacity, inherited, nil
}

func (o *Orchestrator) detachUncommittedLaunchPort(sb *types.Sandbox, port string, cause error) error {
	detachCtx, cancel := cleanupContext()
	detachErr := o.vs.Detach(detachCtx, port)
	cancel()
	if detachErr == nil {
		sb.VswitchPort, sb.FloatingIP, sb.PortMAC, sb.InnerIP = "", "", "", ""
		return cause
	}
	return errors.Join(cause, fmt.Errorf("orch: detach uncommitted port %s: %w", port, detachErr))
}
