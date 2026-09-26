package orch

import (
	"context"
	"encoding/hex"
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
	maxRequiredArtifactRefs = 1024
)

type prepareWaitResult struct {
	summary configsock.ArtifactPrepareSummary
	err     error
}

// awaitArtifactReadinessResult preserves a completed readiness failure over
// the launch cancellation that failure triggers. The readiness worker publishes
// its result before canceling launchCtx, so a context wake-up can safely check
// the buffered result before falling back to the context error.
func awaitArtifactReadinessResult(ctx context.Context, result <-chan error) error {
	select {
	case err := <-result:
		if err != nil {
			return err
		}
		return ctx.Err()
	case <-ctx.Done():
		select {
		case err := <-result:
			if err != nil {
				return err
			}
		default:
		}
		return ctx.Err()
	}
}

// launchArtifactSandbox owns the host half of the exact-run two-stage protocol.
// The conductor consumes only the task's non-secret summary; it never opens or
// parses a tenant Artifact.
func (o *Orchestrator) launchArtifactSandbox(ctx context.Context, attempt *launchAttempt, sb *types.Sandbox, tmpl types.TemplateID, preparation *launchPreparation) (retErr error) {
	prepareStarted := time.Now()
	for _, dir := range []string{sb.RunDir, sb.BaseDir} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return launchFailed("artifact_prepare", fmt.Errorf("orch: mkdir %s: %w", dir, err))
		}
	}
	if err := os.Chmod(sb.RunDir, 0o700); err != nil {
		return launchFailed("artifact_prepare", fmt.Errorf("orch: chmod %s: %w", sb.RunDir, err))
	}

	// The listener exists before assignment becomes visible. node-ctl connects
	// immediately after WaitAssignment, making a task exit during root reading an
	// observable EOF instead of a launch-wide timeout.
	readyListener, err := listenRuntimeReadiness(o.cfg.Paths.RunRoot, sb.ID)
	if err != nil {
		return launchFailed("artifact_prepare", err)
	}
	defer readyListener.Close()

	assignStarted := time.Now()
	assignmentCtx, cancelAssignment := context.WithTimeout(ctx, o.cfg.Units.PoolWaitDuration())
	var commitStarted, commitFinished time.Time
	runID, err := o.runnerPool.AssignWithFence(assignmentCtx, sb.ID, func(runID string) bool { return o.runSessionAssignmentActive(runKindSandbox, runID) }, func(runID string) error {
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
		return launchFailed("artifact_prepare", err)
	}
	if sb.RunID == "" {
		sb.RunID = runID
		attempt.SetRunID(runID)
	}
	o.logLaunchPhase(attempt, sb, "runner_handoff_duration", time.Since(commitFinished))

	deadline := attempt.Deadline()
	if deadline.IsZero() {
		return launchFailed("artifact_prepare", errors.New("orch: artifact launch deadline was not initialized"))
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
		// Publication must precede cancellation; awaitArtifactReadinessResult
		// relies on this boundary to preserve the concrete readiness failure.
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

	var summary configsock.ArtifactPrepareSummary
	select {
	case result := <-prepareResult:
		if result.err != nil {
			if ctxErr := launchCtx.Err(); ctxErr != nil && errors.Is(result.err, ctxErr) {
				result.err = awaitArtifactReadinessResult(launchCtx, readinessResult)
			}
			return launchFailed("artifact_prepare", result.err)
		}
		summary = result.summary
	case readyErr := <-readinessResult:
		if readyErr == nil {
			readyErr = errors.New("orch: runtime readiness completed before artifact preparation")
		}
		return launchFailed("artifact_prepare", readyErr)
	case <-launchCtx.Done():
		return launchFailed("artifact_prepare", awaitArtifactReadinessResult(launchCtx, readinessResult))
	}

	if err := validatePreparedPair(summary, preparation.Source); err != nil {
		return launchFailed("artifact_prepare", err)
	}
	artifactCapacity, inheritedNetwork, err := validateArtifactPrepareSummary(summary, sb.LaunchMode)
	if err != nil {
		return launchFailed("artifact_prepare", err)
	}
	if artifactTopologyNeedsTemplate(summary.DiskTopology) && strings.TrimSpace(o.cfg.Sandbox.Boot.OverlayDiffTemplate) == "" {
		return launchFailed("artifact_prepare", errors.New("orch: artifact disk topology requires sandbox.boot.overlay_diff_template"))
	}
	hostPrepareStarted := time.Now()
	o.log.Info("sandbox task artifact prepared",
		"sid", sb.ID,
		"run_id", sb.RunID,
		"task_artifact_ref_count", summary.RequiredRefCount,
		"artifact_prepare_handoff_duration", time.Since(prepareStarted))

	resources, err := o.resolveArtifactResources(preparation.Spec, artifactCapacity)
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
	current, err := o.st.Get(launchCtx, sb.ID)
	if err != nil || current == nil || current.State != types.StateStarting || current.RunID != sb.RunID ||
		current.LaunchMode != sb.LaunchMode || current.ResumeSource != sb.ResumeSource || current.TemplateID != sb.TemplateID || current.CreatedUnix != sb.CreatedUnix {
		if err == nil {
			err = errLaunchOwnershipLost
		}
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
	p.ArtifactDisks = summary.DiskTopology
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
	err = awaitArtifactReadinessResult(launchCtx, readinessResult)
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
	changed, err = o.st.CommitPreparedRunning(launchCtx, sb, summary.RootSource)
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
		cached.ResumeSource = summary.RootSource
		cached.RunID = sb.RunID
		cached.LaunchMode = ""
	})
	if running == nil {
		running = cloneSandbox(sb)
		running.State = types.StateRunning
		running.ResumeSource = summary.RootSource
		running.LaunchMode = ""
		o.cache(running)
	}
	sb.State = types.StateRunning
	sb.ResumeSource = summary.RootSource
	sb.LaunchMode = ""
	sb.DeadlineUnix = running.DeadlineUnix
	o.publishUpsert(running)
	o.observeSandboxUpsert(running)
	return nil
}

func (o *Orchestrator) resolveArtifactResources(spec sandboxcfg.SandboxSpec, artifactCapacity rtconfig.CapacityConfig) (rtconfig.ResourcesConfig, error) {
	dynamic := o.cfg.ResourceListen != nil && o.cfg.ResourceListen.Enabled
	return sandboxcfg.ResolveResources(sandboxcfg.ResourceResolveInput{
		Node:                     configresolve.SandboxResources(o.cfg.Sandbox.Resources),
		Patch:                    spec.Resource,
		Restore:                  true,
		ArtifactCapacity:         &artifactCapacity,
		Dynamic:                  dynamic,
		ControllerSocketIdentity: o.resourceControllerSocketIdentity,
	})
}

// validatePreparedPair admits S-only input only from the launch's explicit
// template preparation input. Durable sources must match E as well as S.
func validatePreparedPair(summary configsock.ArtifactPrepareSummary, expected types.ResumeSource) error {
	if !summary.RootSource.Valid() || summary.RootSource.Kind != expected.Kind || summary.RootSource.Ref != expected.Ref ||
		(expected.SandboxRef != "" && summary.RootSource.SandboxRef != expected.SandboxRef) {
		return errors.New("orch: prepared root pair conflicts with the accepted source")
	}
	return nil
}

func validateArtifactPrepareSummary(summary configsock.ArtifactPrepareSummary, mode types.LaunchMode) (rtconfig.CapacityConfig, sandboxcfg.NetworkSpec, error) {
	if summary.SchemaVersion != configsock.ArtifactPrepareSchemaVersion {
		return rtconfig.CapacityConfig{}, sandboxcfg.NetworkSpec{}, fmt.Errorf("orch: unsupported artifact prepare schema %d", summary.SchemaVersion)
	}
	if summary.RequiredRefCount < 1 || summary.RequiredRefCount > maxRequiredArtifactRefs {
		return rtconfig.CapacityConfig{}, sandboxcfg.NetworkSpec{}, fmt.Errorf("orch: artifact required ref count %d outside 1..%d", summary.RequiredRefCount, maxRequiredArtifactRefs)
	}
	wantKind := types.ResumeSourceSandbox
	if mode == types.LaunchMemory {
		wantKind = types.ResumeSourceSnapshot
	} else if mode != types.LaunchCold {
		return rtconfig.CapacityConfig{}, sandboxcfg.NetworkSpec{}, fmt.Errorf("orch: invalid durable artifact launch mode %q", mode)
	}
	if types.ResumeSourceKind(summary.PreparedSourceKind) != wantKind {
		return rtconfig.CapacityConfig{}, sandboxcfg.NetworkSpec{}, fmt.Errorf(
			"orch: prepared source kind %q conflicts with launch mode %q", summary.PreparedSourceKind, mode)
	}
	if !summary.RootSource.Valid() || len(summary.RootSource.Ref) > 4096 || len(summary.RootSource.SandboxRef) > 4096 {
		return rtconfig.CapacityConfig{}, sandboxcfg.NetworkSpec{}, errors.New("orch: artifact root pair is incomplete or oversized")
	}
	digest, err := hex.DecodeString(summary.ResolutionDigest)
	if err != nil || len(digest) != 32 || hex.EncodeToString(digest) != summary.ResolutionDigest {
		return rtconfig.CapacityConfig{}, sandboxcfg.NetworkSpec{}, errors.New("orch: artifact resolution digest is not SHA-256")
	}
	capacity := rtconfig.CapacityConfig{CPU: summary.Capacity.CPU, Memory: summary.Capacity.Memory}
	if capacity.CPU <= 0 || strings.TrimSpace(capacity.Memory) == "" {
		return rtconfig.CapacityConfig{}, sandboxcfg.NetworkSpec{}, errors.New("orch: artifact config has no usable resources.capacity")
	}
	inherited := artifactNetworkSpec(summary.Network)
	if err := sandboxcfg.ValidateNetworkSpec(inherited); err != nil {
		return rtconfig.CapacityConfig{}, sandboxcfg.NetworkSpec{}, fmt.Errorf("orch: artifact network summary: %w", err)
	}
	if err := validateArtifactDiskTopology(summary.DiskTopology); err != nil {
		return rtconfig.CapacityConfig{}, sandboxcfg.NetworkSpec{}, err
	}
	return capacity, inherited, nil
}

func validateArtifactDiskTopology(topology types.ArtifactDiskTopology) error {
	if topology.Root.Name != "" || !topology.Root.Mode.Valid() {
		return errors.New("orch: artifact root disk topology is invalid")
	}
	if len(topology.Disks) > rtconfig.MaxDataDisks {
		return fmt.Errorf("orch: artifact data disk count %d exceeds %d", len(topology.Disks), rtconfig.MaxDataDisks)
	}
	seen := make(map[string]struct{}, len(topology.Disks))
	for i, disk := range topology.Disks {
		if disk.Name == "" || !disk.Mode.Valid() {
			return fmt.Errorf("orch: artifact data disk %d topology is invalid", i)
		}
		if _, duplicate := seen[disk.Name]; duplicate {
			return fmt.Errorf("orch: artifact data disk %d duplicates name %q", i, disk.Name)
		}
		seen[disk.Name] = struct{}{}
	}
	return nil
}

func artifactTopologyNeedsTemplate(topology types.ArtifactDiskTopology) bool {
	if !topology.Root.HasActiveBase {
		return true
	}
	for _, disk := range topology.Disks {
		if !disk.HasActiveBase {
			return true
		}
	}
	return false
}

func artifactNetworkSpec(network configsock.ArtifactNetwork) sandboxcfg.NetworkSpec {
	return sandboxcfg.NetworkSpec{
		Hostname: network.Hostname, DNS: append([]string(nil), network.DNS...),
		InnerIP: network.InnerIP, Nexthop: network.Nexthop,
		TransitGatewayIP: network.TransitGatewayIP, TransitGeneveVNI: network.TransitGeneveVNI,
		TransitMAC: network.TransitMAC,
	}
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
