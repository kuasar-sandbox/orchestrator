package orch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/buildcfg"
	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/configresolve"
	"github.com/kuasar-sandbox/orchestrator/internal/execadmission"
	"github.com/kuasar-sandbox/orchestrator/internal/migrationtoken"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// This file makes the orchestrator the node side of the cluster node-link
// (nodelink.Node): it executes the registry's lifecycle commands and reports the
// terminal state by sandbox/build ID. Registry-owned sandbox context is persisted
// separately from user metadata; the nodelink owner resolves cluster identity
// from its per-node ownership table.

// HandleCommand executes a registry node-link command and returns a receipt ack
// (cluster.md). Create/Connect are accepted only after their durable starting
// transition and launch ownership are established; the remaining boot/restore
// work runs asynchronously so the node-link reader is not blocked. Terminal
// sandbox state is reported on the route stream, which a Reserve waits on.
func (o *Orchestrator) HandleCommand(ctx context.Context, cmd *routesync.Command) *routesync.CmdAck {
	// The node-link wire enforces the same complete-token bound, but keep the
	// command entry defensive for direct callers and tests that bypass framing.
	if cmd != nil && len(cmd.MigrationToken) > migrationtoken.MaxWireSize {
		return reject(cmd, migrationtoken.ErrTokenTooLarge)
	}
	if cmd != nil && cmd.Kind != routesync.CmdExecSession && cmd.ExecConditionsSpecified() {
		return reject(cmd, fmt.Errorf("cluster command contains exec conditions: %w", api.ErrBadRequest))
	}
	if cmd != nil && cmd.Kind == routesync.CmdExecSession &&
		cmd.ExecConditionsSpecified() && len(cmd.ExecConditions) == 0 {
		return reject(cmd, fmt.Errorf("cluster command contains non-canonical exec conditions: %w", api.ErrBadRequest))
	}
	switch cmd.Kind {
	case routesync.CmdCreate:
		pair, tmpl, credentials, err := o.precheckCluster(ctx, cmd)
		if err != nil {
			o.log.Warn("cluster create rejected", "sid", cmd.SID, "err", err)
			return reject(cmd, err)
		}
		if _, _, err := o.acceptClusterCreate(ctx, cmd, pair, tmpl, credentials); err != nil {
			o.log.Warn("cluster create rejected", "sid", cmd.SID, "err", err)
			return reject(cmd, err)
		}
		return accept(cmd)
	case routesync.CmdConnect:
		if cmd.TimeoutSeconds < 0 || int64(cmd.TimeoutSeconds) > routesync.MaxConnectTimeoutSeconds {
			return &routesync.CmdAck{
				CmdID: cmd.CmdID, Status: routesync.AckRejected,
				Reason: "connect timeout is out of range", HTTPStatus: http.StatusBadRequest,
			}
		}
		requestedDeadline := clusterConnectDeadline(cmd.TimeoutSeconds)
		sb, err := o.prepareClusterConnect(ctx, cmd, requestedDeadline)
		if err != nil {
			return reject(cmd, err)
		}
		var deadline *int64
		if requestedDeadline > 0 {
			deadline = &requestedDeadline
		}
		sb, _, err = o.ensureResumeAccepted(ctx, cmd.SID, deadline, func(current *types.Sandbox) error {
			if err := validateClusterSandboxCredentialBinding(current, cmd.APISecretFingerprint); err != nil {
				return err
			}
			return validateClusterSandboxContext(current, cmd)
		})
		if err != nil {
			return reject(cmd, err)
		}
		result, err := clusterConnectResult(sb)
		if err != nil {
			return reject(cmd, err)
		}
		return acceptConnect(cmd, result)
	case routesync.CmdExecSession:
		_, result, err := o.prepareClusterExecSession(ctx, cmd, wallUnix)
		if err != nil {
			return reject(cmd, err)
		}
		return acceptExecSession(cmd, result)
	case routesync.CmdDelete:
		sb, err := o.clusterSandbox(ctx, cmd.SID, cmd.APISecretFingerprint)
		if err != nil {
			return reject(cmd, err)
		}
		go func() {
			if err := o.deleteCluster(o.asyncCtx(), sb); err != nil {
				o.log.Error("cluster delete", "sid", cmd.SID, "err", err)
			}
		}()
		return accept(cmd)
	case routesync.CmdKeyPut:
		if err := o.putClusterKeyPair(ctx, cmd); err != nil {
			return reject(cmd, err)
		}
		return accept(cmd)
	case routesync.CmdKeyDrop:
		if err := o.dropClusterKey(ctx, cmd.APISecretFingerprint); err != nil {
			return reject(cmd, err)
		}
		return accept(cmd)
	case routesync.CmdBuildRegister:
		// Pre-provision a registry-assigned build (cluster.md): create the build
		// record with the registry's ids + resolved key, durably retain encrypted
		// registration image-pull credentials for exact replay, and report
		// `registered` up. The e2b trigger (router-forwarded) then runs it; state
		// flows back as build events.
		if err := o.registerClusterBuild(ctx, cmd); err != nil {
			return reject(cmd, err)
		}
		return accept(cmd)
	default:
		return reject(cmd, fmt.Errorf("unhandled command kind %q", cmd.Kind))
	}
}

// ResourceProbe surfaces the node's water level for the cluster heartbeat. serve
// sets it (an adapter over the resource controller) when resource_listen is on;
// nil = no controller (static cgroup), then zone/water are reported as their
// zero-load defaults and only sandbox count + durable build admission usage
// carry load signals.
type ResourceProbeSnapshot struct {
	Zone      string
	Allocated int64 // sum of node reservations
	Pool      int64
	Draining  bool
}

type ResourceProbe interface {
	Snapshot() ResourceProbeSnapshot
}

// SetResourceProbe wires the node water-level source for the cluster heartbeat.
func (o *Orchestrator) SetResourceProbe(p ResourceProbe) { o.probe = p }

// registerClusterBuild pre-provisions a build the registry assigned + placed here
// (cluster.md): resolve the tenant key by the predistributed fingerprint,
// create the build record under the registry's ids, protect the immutable
// registration image-pull credentials for exact retries and restart recovery,
// and report `registered` up the node-link.
func (o *Orchestrator) registerClusterBuild(ctx context.Context, cmd *routesync.Command) error {
	if cmd.BuildID == "" || cmd.TemplateRef == "" {
		return fmt.Errorf("build_register: missing build_id / template_id")
	}
	profile, err := types.ParseProfile(cmd.Profile)
	if err != nil {
		return fmt.Errorf("build_register: %w", err)
	}
	// An exact retry after an ambiguous/lost ACK is identified from the durable
	// Build before consulting the mutable allowlist. Key withdrawal must prevent
	// new ownership, but it cannot convert already accepted ownership into a
	// definitive no-side-effect rejection that lets the Registry place the same
	// BuildID on another node.
	existing, err := o.st.GetBuild(ctx, cmd.BuildID)
	if err != nil {
		return err
	}
	var pair store.KeyPair
	if existing != nil {
		fingerprint, err := store.APISecretHash(existing.APISecret)
		if err != nil {
			return fmt.Errorf("build_register: verify durable credential: %w", err)
		}
		if fingerprint != cmd.APISecretFingerprint {
			return fmt.Errorf("build_register: %w: credential fingerprint differs", store.ErrBuildRegistrationConflict)
		}
		pair = store.KeyPair{APISecret: existing.APISecret, ManifestKey: existing.ManifestKey}
	} else {
		pair, err = o.resolveByFingerprint(ctx, cmd.APISecretFingerprint)
		if err != nil {
			if errors.Is(err, errClusterCredentialPairNotInstalled) {
				return fmt.Errorf("%w: build_register credential: %v", api.ErrBadRequest, err)
			}
			return err
		}
	}
	resources := cmd.BuildResources.Types()
	if err := resources.ValidateRequired(); err != nil {
		return fmt.Errorf("%w: build_register resources: %v", api.ErrBadRequest, err)
	}
	if _, err := builderResourceProperties(resources); err != nil {
		o.recordRegistrationRejection("systemd_encoding")
		return fmt.Errorf("%w: build_register resources cannot be enforced by systemd: %v", api.ErrBadRequest, err)
	}
	config, err := sandboxcfg.NormalizeResourceMetadata(cmd.Config)
	if err != nil {
		return fmt.Errorf("%w: build_register sandbox config: %v", api.ErrBadRequest, err)
	}
	meta, builderOpts, err := buildcfg.Extract(config)
	if err != nil {
		return fmt.Errorf("%w: build_register builder config: %v", api.ErrBadRequest, err)
	}
	if builderOpts.Resources != nil {
		return fmt.Errorf("%w: build_register %s.resources must be normalized into build_resources", api.ErrBadRequest, buildcfg.NsBuilder)
	}
	location, err := clusterstate.ObjectLocationFromMetadata(meta)
	if err != nil {
		return fmt.Errorf("%w: build_register cluster ownership: %v", api.ErrBadRequest, err)
	}
	meta = cloneStringMapWithout(meta, clusterstate.ObjectMetadataKey)
	var mmdsHeader *string
	if cmd.BuildMMDSSecrets != nil {
		raw, marshalErr := json.Marshal(struct {
			Secrets map[string]string `json:"secrets"`
		}{Secrets: cmd.BuildMMDSSecrets})
		if marshalErr != nil {
			return fmt.Errorf("%w: build_register MMDS initial values: %v", api.ErrBadRequest, marshalErr)
		}
		value := string(raw)
		mmdsHeader = &value
	}
	var mmdsDoc sandboxcfg.MMDSDocument
	if existing != nil {
		// A same-BuildID retry after an ambiguous/lost ACK is compared with
		// durable immutable identity below. Do not let mutable MMDS enablement,
		// route limits, reserved prefixes, or service configuration turn that
		// already accepted ownership into a definitive no-side-effect rejection.
		mmdsDoc, meta, err = sandboxcfg.ExtractMMDSReplay(meta, mmdsHeader)
	} else {
		mmdsDoc, meta, err = sandboxcfg.ExtractMMDS(meta, mmdsHeader, o.mmdsPolicy())
	}
	if err != nil {
		return fmt.Errorf("%w: build_register MMDS config: %v", api.ErrBadRequest, err)
	}
	if _, err := sandboxcfg.ParseSpec(meta); err != nil {
		return fmt.Errorf("%w: build_register sandbox config: %v", api.ErrBadRequest, err)
	}
	phaseResourcePatch := meta[sandboxcfg.NsResource]
	if phaseResourcePatch != "" {
		meta = cloneStringMapWithout(meta, sandboxcfg.NsResource)
	}
	if existing == nil {
		// Mutable node policy admits new ownership only. Exact replay is still
		// strictly decoded above and the store compares every immutable field;
		// reapplying changed Referer or Sandbox policy here could incorrectly
		// turn an ACK-lost acceptance into permission to place on another node.
		if err := o.validateBuildOptions(builderOpts, false); err != nil {
			return err
		}
		if err := o.validateBuildPhaseResources(phaseResourcePatch); err != nil {
			return err
		}
	}
	b := &types.Build{
		BuildID:                  cmd.BuildID,
		TemplateID:               cmd.TemplateRef,
		APISecret:                pair.APISecret,
		ManifestKey:              pair.ManifestKey,
		Profile:                  profile,
		Kind:                     types.KindImg,
		Status:                   types.BuildRegistered,
		FromImage:                o.imageURIFromMask(cmd.TemplateRef, cmd.BuildID),
		Resources:                resources,
		RegistrationImageRepo:    cmd.ImageRepo,
		RegistrationRegistryAuth: cmd.RegistryAuth,
		ClusterGroup:             location.Group,
		PhaseResourcePatch:       phaseResourcePatch,
		Metadata:                 meta,
		Builder:                  builderOpts,
		CreatedUnix:              time.Now().Unix(),
	}
	initialMMDS := initialMMDSRouteSecretValues(mmdsDoc)
	if initialMMDS != nil {
		transportRow := &types.Sandbox{
			ID: "build-" + b.BuildID, Profile: b.Profile, TemplateID: b.TemplateID,
			State: types.StateRunning, RunID: "build-registration-check",
			APISecret: b.APISecret, ManifestKey: b.ManifestKey, Metadata: b.Metadata,
		}
		if err := validateInitialMMDSRouteEntry(transportRow, initialMMDS); err != nil {
			return fmt.Errorf("%w: build_register MMDS config: %v", api.ErrBadRequest, err)
		}
		b.RegistrationMMDSRoutesDigest = initialMMDS.routesDigest
	}
	// A tightened execution policy rejects only new registration ownership. An
	// exact retry after an ambiguous/lost ACK must still reach the store's
	// transactional immutable comparison and return the persisted result.
	if existing == nil {
		executionLimit, err := configresolve.BuilderExecutionLimit(o.cfg.Builder)
		if err != nil {
			return err
		}
		if !executionLimit.AllowsOne(resources) {
			o.recordRegistrationRejection("execution_fit")
			return fmt.Errorf("%w: build resources cannot fit builder.admission.execution", api.ErrBadRequest)
		}
	}
	registrationLimit, err := configresolve.BuilderRegistrationLimit(o.cfg.Builder)
	if err != nil {
		return err
	}
	var routesDigest string
	var secretValues store.MMDSRouteSecretValues
	if initialMMDS != nil {
		routesDigest, secretValues = initialMMDS.routesDigest, initialMMDS.values
	}
	unlockEvent := o.lockExtensionBuildEvent(b.BuildID)
	defer unlockExtensionEvent(unlockEvent)
	registered, inserted, err := o.st.RegisterBuildWithMMDSRouteSecretValues(ctx, b, registrationLimit, routesDigest, secretValues)
	if errors.Is(err, store.ErrBuildRegistrationCapacity) {
		o.recordRegistrationRejection("capacity")
		return fmt.Errorf("%w: %v", api.ErrBuildAdmission, err)
	}
	if errors.Is(err, store.ErrBuildRegistrationConflict) {
		return fmt.Errorf("build_register: %w", err)
	}
	if err != nil {
		return err
	}
	o.refreshBuildAdmissionGauges(ctx)
	if registered.Status == types.BuildReady || registered.Status == types.BuildError {
		templateID := ""
		if registered.Status == types.BuildReady {
			templateID = registered.PersistID
		}
		if err := o.publishBuildStateRequired(ctx, registered.BuildID, string(registered.Status), templateID, registered.Reason); err != nil {
			return fmt.Errorf("build_register: republish durable terminal state: %w", err)
		}
		return nil
	}
	o.clusterBuildMu.Lock()
	o.clusterBuilds[cmd.BuildID] = &clusterBuild{imageRepo: cmd.ImageRepo, registryAuth: cmd.RegistryAuth}
	o.clusterBuildMu.Unlock()
	if inserted {
		o.publishBuildState(cmd.BuildID, "registered", "", "")
		o.observeBuildUpsert(registered)
	}
	return nil
}

// BuildEvents is the node-link client's source of build state transitions
// (nodelink.Node); the client streams them to the registry (§5.1).
func (o *Orchestrator) BuildEvents() <-chan *routesync.BuildEvent { return o.buildEvents }

// buildStateEvent constructs an event only for a cluster-owned Build. The
// process-local map is a fast path; durable ClusterGroup is authoritative after
// restart and after terminal cleanup removed transient credentials.
func (o *Orchestrator) buildStateEvent(ctx context.Context, buildID, state, templateID, reason string) (*routesync.BuildEvent, bool, error) {
	o.clusterBuildMu.Lock()
	cb := o.clusterBuilds[buildID]
	o.clusterBuildMu.Unlock()
	if cb == nil {
		// The process-local entry is only a fast path. Cluster ownership is a
		// dedicated durable Build field, so controller restart cannot suppress
		// building/terminal events or lose registration accounting convergence.
		build, err := o.st.GetBuild(ctx, buildID)
		if err != nil {
			return nil, false, fmt.Errorf("resolve durable cluster build ownership: %w", err)
		}
		if build == nil {
			return nil, false, nil
		}
		if build.ClusterGroup == "" {
			return nil, false, nil // direct-node build
		}
	}
	return &routesync.BuildEvent{BuildID: buildID, State: state, TemplateID: templateID, Reason: reason}, true, nil
}

func (o *Orchestrator) forgetTerminalClusterBuild(buildID, state string) {
	if state != string(types.BuildReady) && state != string(types.BuildError) {
		return
	}
	o.clusterBuildMu.Lock()
	delete(o.clusterBuilds, buildID) // terminal: drop the transient creds
	o.clusterBuildMu.Unlock()
}

// publishBuildState emits a build event for a cluster build (no-op for a non-
// cluster, e.g. single-node, build). Intermediate lifecycle transitions are a
// lossy notification, while a durable terminal result waits for node-link or
// controller cancellation. Startup reconciliation uses the explicitly
// best-effort helper below and is followed by a complete SQLite-backed replay.
func (o *Orchestrator) publishBuildState(buildID, state, templateID, reason string) {
	if state == string(types.BuildReady) || state == string(types.BuildError) {
		if err := o.publishBuildStateRequired(o.launchContext(), buildID, state, templateID, reason); err != nil {
			o.log.Warn("publish terminal cluster build state", "bid", buildID, "state", state, "err", err)
		}
		return
	}
	o.publishBuildStateBestEffort(buildID, state, templateID, reason)
}

func (o *Orchestrator) publishBuildStateBestEffort(buildID, state, templateID, reason string) {
	ev, ok, err := o.buildStateEvent(context.Background(), buildID, state, templateID, reason)
	if err != nil {
		o.log.Warn("resolve cluster build state event", "bid", buildID, "state", state, "err", err)
		return
	}
	if !ok {
		return
	}
	select {
	case o.buildEvents <- ev:
	default:
	}
	o.forgetTerminalClusterBuild(buildID, state)
}

// publishBuildStateRequired queues the durable terminal result before an exact
// BuildRegister replay is acknowledged. If the node-link cannot drain events,
// returning without an ACK keeps the Registry pinned to this node; a later
// identical retry can safely repeat the event.
func (o *Orchestrator) publishBuildStateRequired(ctx context.Context, buildID, state, templateID, reason string) error {
	delay := 20 * time.Millisecond
	for {
		ev, ok, err := o.buildStateEvent(ctx, buildID, state, templateID, reason)
		if err == nil {
			if !ok {
				return nil
			}
			select {
			case o.buildEvents <- ev:
				o.forgetTerminalClusterBuild(buildID, state)
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		// A transient SQLite read failure after restart is not evidence that this
		// is a direct-node Build. Retain the terminal result and retry ownership
		// resolution until the lifecycle context is canceled.
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		if delay < time.Second {
			delay *= 2
			if delay > time.Second {
				delay = time.Second
			}
		}
	}
}

// ReplayClusterBuildTerminalStates republishes every durable cluster terminal
// result after each node-link session is established. Startup reconciliation
// can terminally fail an arbitrary number of interrupted builders before
// node-link begins draining its bounded event channel, and a connection reset
// can lose a session-local outbox item. Rebuilding this stream from SQLite
// prevents either bounded optimization from becoming a correctness limit.
// Replays are idempotent at the Registry.
func (o *Orchestrator) ReplayClusterBuildTerminalStates(ctx context.Context) error {
	for _, state := range []types.BuildState{types.BuildReady, types.BuildError} {
		builds, err := o.st.BuildsByStatus(ctx, state)
		if err != nil {
			return fmt.Errorf("replay cluster builds in state %s: %w", state, err)
		}
		for _, build := range builds {
			if build.ClusterGroup == "" {
				continue
			}
			templateID := ""
			if state == types.BuildReady {
				templateID = build.PersistID
			}
			if err := o.publishBuildStateRequired(ctx, build.BuildID, string(state), templateID, build.Reason); err != nil {
				return fmt.Errorf("replay cluster build %s: %w", build.BuildID, err)
			}
		}
	}
	return nil
}

// clusterBuildCreds returns a cluster build's image-pull credentials. The
// process-local copy is a fast path; the encrypted immutable registration field
// in the Build row remains authoritative across controller restart and delayed
// registration retries. Trigger writes its separate work-order credential.
func (o *Orchestrator) clusterBuildCreds(build *types.Build) (string, bool) {
	if build == nil {
		return "", false
	}
	o.clusterBuildMu.Lock()
	cb := o.clusterBuilds[build.BuildID]
	o.clusterBuildMu.Unlock()
	if cb != nil {
		return cb.registryAuth, true
	}
	if build.ClusterGroup == "" {
		return "", false
	}
	// Registration auth is encrypted with the rest of the Build row and never
	// enters portable template metadata. Trigger copies the resolved value into
	// its separately encrypted work-order field without mutating this definition.
	return build.RegistrationRegistryAuth, true
}

// Heartbeat reports durable registration and execution usage. The SQLite Build
// rows are authoritative; a read failure is advertised as fully occupied so a
// registry can never infer unsafe headroom.
func (o *Orchestrator) Heartbeat() *routesync.Heartbeat {
	o.mu.Lock()
	count := len(o.reg)
	o.mu.Unlock()
	hb := &routesync.Heartbeat{Counts: count, Zone: string(nodectlZoneGreen)}
	if usage, err := o.st.BuildUsage(context.Background()); err == nil {
		hb.BuildRegistrationUsage = &routesync.BuildAdmissionUsage{
			Builds: usage.RegistrationBuilds, Resources: routesync.BuildResourcesFromTypes(usage.Registration),
			Waiting: usage.WaitingBuilds, OldestWaitingUnix: usage.OldestWaitingUnix,
		}
		hb.BuildExecutionUsage = &routesync.BuildAdmissionUsage{
			Builds: usage.ExecutionBuilds, Resources: routesync.BuildResourcesFromTypes(usage.Execution),
			Waiting: usage.WaitingBuilds, OldestWaitingUnix: usage.OldestWaitingUnix,
		}
	} else {
		o.log.Error("build admission usage unavailable; advertising no headroom", "err", err)
		hb.BuildRegistrationUsage = saturatedBuildUsage()
		hb.BuildExecutionUsage = saturatedBuildUsage()
	}
	if p := o.probe; p != nil {
		snapshot := p.Snapshot()
		hb.Zone, hb.Allocated, hb.Pool, hb.Draining = snapshot.Zone, snapshot.Allocated, snapshot.Pool, snapshot.Draining
	}
	return hb
}

func saturatedBuildUsage() *routesync.BuildAdmissionUsage {
	// Saturate every dimension, including configured-unlimited dimensions. The
	// checked admission arithmetic treats MaxInt64 + any positive request as an
	// overflow, so a failed durable usage read can never be mistaken for empty
	// headroom merely because the corresponding configured limit is zero.
	return &routesync.BuildAdmissionUsage{
		Builds: math.MaxInt64,
		Resources: routesync.BuildResourcesFromTypes(types.BuildResources{
			CPU: math.MaxInt64, Memory: math.MaxInt64, Storage: math.MaxInt64,
		}),
	}
}

// nodectlZoneGreen is the default zone reported when no resource controller is
// present (a static-cgroup node is never "hot" from the cluster's view).
const nodectlZoneGreen = "green"

// ClusterNodeInfo builds the static node-register fields for node-link (cluster.md
// §5.1): sandbox capacity, both build admission limits, and the guest runtime
// identity used by placement.
func (o *Orchestrator) ClusterNodeInfo() (capacity int, registration, execution *routesync.BuildAdmissionLimit, runtimeDigest string) {
	registrationLimit, _ := configresolve.BuilderRegistrationLimit(o.cfg.Builder)
	executionLimit, _ := configresolve.BuilderExecutionLimit(o.cfg.Builder)
	registration = routesync.BuildAdmissionLimitFromTypes(registrationLimit)
	execution = routesync.BuildAdmissionLimitFromTypes(executionLimit)
	if dig, err := sha256File(o.runtimeFileFor(types.ProfileE2B)); err == nil {
		runtimeDigest = dig
	}
	return o.cfg.Sandbox.Capacity, registration, execution, runtimeDigest
}

// SetLifecycleContext sets the common admission lifetime for standalone,
// data-plane, exec, cluster launch, and pause work. node-ctl calls it
// unconditionally before exposing any API or route surface.
func (o *Orchestrator) SetLifecycleContext(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}
	o.lifecycleCtxMu.Lock()
	o.lifecycleCtx = ctx
	o.lifecycleCtxMu.Unlock()
}

func (o *Orchestrator) launchContext() context.Context {
	o.lifecycleCtxMu.RLock()
	ctx := o.lifecycleCtx
	o.lifecycleCtxMu.RUnlock()
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

// SetClusterContext and asyncCtx remain thin aliases for existing embedders and
// tests; there is only one lifecycle context source.
func (o *Orchestrator) SetClusterContext(ctx context.Context) { o.SetLifecycleContext(ctx) }
func (o *Orchestrator) asyncCtx() context.Context             { return o.launchContext() }

// DrainLaunches waits for all accepted create/resume attempts to finish their
// terminal state, route publication, and resource cleanup. The lifecycle root
// must be canceled before calling it so admission cannot add a successor.
func (o *Orchestrator) DrainLaunches(ctx context.Context) error {
	return o.launches.Drain(ctx)
}

// DrainPauses closes admission and waits for accepted pause and export work to
// stop using shared dependencies. Accepted snapshots finish durable publication;
// exports cancel publication unless their source finalizer already won.
func (o *Orchestrator) DrainPauses(ctx context.Context) error {
	return o.acceptedOps.Drain(ctx)
}

func accept(cmd *routesync.Command) *routesync.CmdAck {
	return &routesync.CmdAck{CmdID: cmd.CmdID, Status: routesync.AckAccepted}
}

func acceptConnect(cmd *routesync.Command, result *routesync.ConnectResult) *routesync.CmdAck {
	return &routesync.CmdAck{CmdID: cmd.CmdID, Status: routesync.AckAccepted, Connect: result}
}

func acceptExecSession(cmd *routesync.Command, result *routesync.ExecSessionResult) *routesync.CmdAck {
	return &routesync.CmdAck{CmdID: cmd.CmdID, Status: routesync.AckAccepted, ExecSession: result}
}

func reject(cmd *routesync.Command, err error) *routesync.CmdAck {
	status, reason := clusterCommandRejection(err)
	return &routesync.CmdAck{
		CmdID: cmd.CmdID, Status: routesync.AckRejected,
		Reason: reason, HTTPStatus: status,
	}
}

func clusterCommandRejection(err error) (int, string) {
	switch {
	case errors.Is(err, api.ErrBadRequest):
		return http.StatusBadRequest, err.Error()
	case errors.Is(err, api.ErrBuildAdmission):
		return http.StatusTooManyRequests, api.ErrBuildAdmission.Error()
	case errors.Is(err, store.ErrBuildRegistrationConflict):
		return http.StatusConflict, store.ErrBuildRegistrationConflict.Error()
	case errors.Is(err, migrationtoken.ErrMalformedToken),
		errors.Is(err, migrationtoken.ErrInvalidPayload):
		return http.StatusBadRequest, "invalid migration token"
	case errors.Is(err, migrationtoken.ErrAuthentication),
		errors.Is(err, migrationtoken.ErrCredentialMismatch):
		return http.StatusForbidden, "migration credential not allowed"
	case errors.Is(err, migrationtoken.ErrIncompatible):
		return http.StatusConflict, "target environment incompatible"
	case errors.Is(err, migrationtoken.ErrTokenTooLarge):
		return http.StatusRequestEntityTooLarge, migrationtoken.ErrTokenTooLarge.Error()
	case errors.Is(err, api.ErrProxyUnavailable):
		return http.StatusServiceUnavailable, api.ErrProxyUnavailable.Error()
	default:
		return 0, err.Error()
	}
}

// CreateCluster is a synchronous wrapper over the same durable admission used by
// HandleCommand. It waits for that exact attempt; it does not maintain a second
// launch implementation.
func (o *Orchestrator) CreateCluster(ctx context.Context, cmd *routesync.Command) (*types.Sandbox, error) {
	pair, tmpl, credentials, err := o.precheckCluster(ctx, cmd)
	if err != nil {
		return nil, err
	}
	_, attempt, err := o.acceptClusterCreate(ctx, cmd, pair, tmpl, credentials)
	if err != nil {
		return nil, err
	}
	if err := attempt.wait(ctx); err != nil {
		return nil, err
	}
	current, err := o.st.Get(ctx, cmd.SID)
	if err != nil {
		return nil, err
	}
	if current == nil || current.State != types.StateRunning {
		return nil, fmt.Errorf("cluster create %s did not reach running", cmd.SID)
	}
	return cloneSandbox(current), nil
}

// precheckCluster resolves the manifest key (by the fingerprint the registry
// predistributed) and the snapshot template — the fast, synchronous preconditions
// whose failure is a rejected ack (rather than a slow create that fails only by
// Reserve timeout).
func (o *Orchestrator) precheckCluster(ctx context.Context, cmd *routesync.Command) (store.KeyPair, types.TemplateID, sandboxcfg.Credentials, error) {
	if cmd == nil {
		return store.KeyPair{}, types.TemplateID{}, sandboxcfg.Credentials{}, fmt.Errorf("cluster create: command is required")
	}
	if !types.ValidLocalSandboxID(cmd.SID) {
		return store.KeyPair{}, types.TemplateID{}, sandboxcfg.Credentials{}, fmt.Errorf("cluster create: invalid sandbox id")
	}
	profile, err := types.ParseProfile(cmd.Profile)
	if err != nil {
		return store.KeyPair{}, types.TemplateID{}, sandboxcfg.Credentials{}, fmt.Errorf("cluster create: %w", err)
	}
	if cmd.Cluster == nil || cmd.Cluster.Group == "" || cmd.Cluster.RouteKey == "" {
		return store.KeyPair{}, types.TemplateID{}, sandboxcfg.Credentials{}, fmt.Errorf("cluster create: group and route key are required")
	}
	pair, err := o.resolveByFingerprint(ctx, cmd.APISecretFingerprint)
	if err != nil {
		return store.KeyPair{}, types.TemplateID{}, sandboxcfg.Credentials{}, err
	}
	tmpl, err := types.ParseTemplateID(cmd.TemplateRef)
	if err != nil {
		return store.KeyPair{}, types.TemplateID{}, sandboxcfg.Credentials{}, fmt.Errorf("cluster create: template %q: %w", cmd.TemplateRef, err)
	}
	if tmpl.Profile != profile {
		return store.KeyPair{}, types.TemplateID{}, sandboxcfg.Credentials{}, fmt.Errorf("cluster create: profile %q does not match template profile %q", profile, tmpl.Profile)
	}
	config, err := sandboxcfg.NormalizeRestoreMetadata(cmd.Config)
	if err != nil {
		return store.KeyPair{}, types.TemplateID{}, sandboxcfg.Credentials{}, fmt.Errorf("cluster create: %w", err)
	}
	config, err = sandboxcfg.NormalizeResourceMetadata(config)
	if err != nil {
		return store.KeyPair{}, types.TemplateID{}, sandboxcfg.Credentials{}, fmt.Errorf("%w: cluster create: %v", api.ErrBadRequest, err)
	}
	config, err = sandboxcfg.NormalizeCheckpointMetadata(config)
	if err != nil {
		return store.KeyPair{}, types.TemplateID{}, sandboxcfg.Credentials{}, fmt.Errorf("cluster create: %w", err)
	}
	credentials, config, err := sandboxcfg.ExtractCredentials(config)
	if err != nil {
		return store.KeyPair{}, types.TemplateID{}, sandboxcfg.Credentials{}, fmt.Errorf("cluster create: %w", err)
	}
	if err := o.validateCreateCheckpointMode(config); err != nil {
		return store.KeyPair{}, types.TemplateID{}, sandboxcfg.Credentials{}, fmt.Errorf("cluster create: %w", err)
	}
	if err := validateSandboxCredentialOverrides(profile, credentials); err != nil {
		return store.KeyPair{}, types.TemplateID{}, sandboxcfg.Credentials{}, fmt.Errorf("cluster create: %w", err)
	}
	if _, err := sandboxcfg.ParseSpec(config); err != nil {
		return store.KeyPair{}, types.TemplateID{}, sandboxcfg.Credentials{}, fmt.Errorf("cluster create: %w", err)
	}
	cmd.Config = config
	return pair, tmpl, credentials, nil
}

// acceptClusterCreate persists and publishes starting before returning, so an
// Accepted ACK always names both a durable row and an active launch owner.
func (o *Orchestrator) acceptClusterCreate(ctx context.Context, cmd *routesync.Command, pair store.KeyPair, tmpl types.TemplateID, credentials sandboxcfg.Credentials) (*types.Sandbox, *launchAttempt, error) {
	admissionStarted := time.Now()
	meta := clusterSandboxMetadata(cmd.Config)

	sb := &types.Sandbox{
		ID:                 cmd.SID,
		Profile:            tmpl.Profile,
		Cluster:            &types.ClusterSandboxContext{Group: cmd.Cluster.Group, RouteKey: cmd.Cluster.RouteKey},
		AuthSandboxIDValue: cmd.Cluster.AuthSandboxID,
		TemplateID:         tmpl.String(),
		State:              types.StateStarting,
		RunDir:             o.cfg.Paths.RunRoot + "/" + cmd.SID,
		BaseDir:            o.cfg.Paths.BaseRoot + "/" + cmd.SID,
		APISecret:          pair.APISecret,
		ManifestKey:        pair.ManifestKey,
		Metadata:           meta,
		CreatedUnix:        time.Now().Unix(),
		DeadlineUnix:       time.Now().Add(time.Duration(o.cfg.Sandbox.TimeoutSec) * time.Second).Unix(),
	}
	if err := materializeSandboxCredentials(sb, credentials); err != nil {
		return nil, nil, fmt.Errorf("cluster create: %w", err)
	}
	if tmpl.Profile == types.ProfileE2B {
		sb.EnvdUDS = sb.RunDir + "/envd.sock"
		sb.CiUDS = sb.RunDir + "/ci.sock"
	}
	accepted, attempt, err := o.acceptFreshLaunch(ctx, sb, tmpl, nil)
	if err != nil {
		return nil, nil, err
	}
	o.logLaunchPhase(attempt, accepted, "admission_duration", time.Since(admissionStarted))
	return accepted, attempt, nil
}

func clusterSandboxMetadata(config map[string]string) map[string]string {
	metadata := make(map[string]string, len(config))
	for k, v := range config {
		if k != clusterstate.ObjectMetadataKey && k != sandboxcfg.NsCredentials {
			metadata[k] = v
		}
	}
	return metadata
}

var errClusterCredentialPairNotInstalled = errors.New("cluster credential pair is not installed")

func (o *Orchestrator) resolveByFingerprint(ctx context.Context, fp string) (store.KeyPair, error) {
	if fp == "" {
		return store.KeyPair{}, fmt.Errorf("%w: empty API secret fingerprint", errClusterCredentialPairNotInstalled)
	}
	pair, found, err := o.st.AllowedKeyPairByAPISecretFingerprint(ctx, fp)
	if err != nil {
		return store.KeyPair{}, err
	}
	if !found {
		return store.KeyPair{}, errClusterCredentialPairNotInstalled
	}
	return pair, nil
}

// clusterSandbox verifies that a trusted lifecycle command is bound to the
// existing sandbox's APISecret without carrying the original API key to node.
func (o *Orchestrator) clusterSandbox(ctx context.Context, sid, apiSecretFingerprint string) (*types.Sandbox, error) {
	if !types.ValidLocalSandboxID(sid) || apiSecretFingerprint == "" {
		return nil, fmt.Errorf("cluster: sandbox id and API secret fingerprint are required")
	}
	sb, err := o.st.Get(ctx, sid)
	if err != nil {
		return nil, err
	}
	if sb == nil {
		return nil, fmt.Errorf("cluster: sandbox not found")
	}
	if err := validateClusterSandboxCredentialBinding(sb, apiSecretFingerprint); err != nil {
		return nil, err
	}
	return sb, nil
}

func validateClusterSandboxCredentialBinding(sb *types.Sandbox, apiSecretFingerprint string) error {
	if sb == nil || apiSecretFingerprint == "" {
		return fmt.Errorf("cluster: sandbox and API secret fingerprint are required")
	}
	fingerprint, err := store.APISecretHash(sb.APISecret)
	if err != nil {
		return err
	}
	if fingerprint != apiSecretFingerprint {
		return fmt.Errorf("cluster: sandbox credential binding mismatch")
	}
	return nil
}

func validateClusterSandboxContext(sb *types.Sandbox, cmd *routesync.Command) error {
	if sb == nil || cmd == nil {
		return fmt.Errorf("cluster connect: sandbox and command are required")
	}
	profile, err := types.ParseProfile(cmd.Profile)
	if err != nil {
		return fmt.Errorf("cluster connect: %w", err)
	}
	if cmd.Cluster == nil || cmd.Cluster.Group == "" || cmd.Cluster.RouteKey == "" {
		return fmt.Errorf("cluster connect: group and route key are required")
	}
	authSandboxID := cmd.Cluster.AuthSandboxID
	if authSandboxID == "" {
		authSandboxID = cmd.SID
	}
	if sb.Profile != profile || sb.Cluster == nil ||
		sb.Cluster.Group != cmd.Cluster.Group || sb.Cluster.RouteKey != cmd.Cluster.RouteKey ||
		sb.AuthSandboxID() != authSandboxID {
		return fmt.Errorf("cluster connect: sandbox context mismatch")
	}
	return nil
}

// prepareClusterConnect validates an existing exact target without touching the
// optional token. If the target is absent, it synchronously authenticates and
// imports KMT1 under the Registry-selected NodeSandboxID before the Ack is sent.
func (o *Orchestrator) prepareClusterConnect(ctx context.Context, cmd *routesync.Command, requestedDeadline int64) (*types.Sandbox, error) {
	if cmd == nil || !types.ValidLocalSandboxID(cmd.SID) || cmd.APISecretFingerprint == "" {
		return nil, fmt.Errorf("cluster connect: valid sandbox id and API secret fingerprint are required")
	}
	profile, err := types.ParseProfile(cmd.Profile)
	if err != nil {
		return nil, fmt.Errorf("cluster connect: %w", err)
	}
	if cmd.Cluster == nil || cmd.Cluster.Group == "" || cmd.Cluster.RouteKey == "" {
		return nil, fmt.Errorf("cluster connect: group and route key are required")
	}

	sb, err := o.st.Get(ctx, cmd.SID)
	if err != nil {
		return nil, err
	}
	if sb != nil {
		if err := validateClusterSandboxCredentialBinding(sb, cmd.APISecretFingerprint); err != nil {
			return nil, err
		}
		if err := validateClusterSandboxContext(sb, cmd); err != nil {
			return nil, err
		}
		return sb, nil // an existing exact target ignores MigrationToken completely
	}
	if cmd.MigrationToken == "" {
		return nil, fmt.Errorf("cluster: sandbox not found")
	}

	pair, err := o.resolveByFingerprint(ctx, cmd.APISecretFingerprint)
	if err != nil {
		return nil, err
	}
	authSandboxID := cmd.Cluster.AuthSandboxID
	if authSandboxID == "" {
		authSandboxID = cmd.SID
	}
	sb, err = o.importSandboxWithKey(
		ctx,
		pair,
		cmd.MigrationToken,
		cmd.SID,
		migrationtoken.Expectations{AuthSandboxID: authSandboxID, Profile: profile},
		&types.ClusterSandboxContext{Group: cmd.Cluster.Group, RouteKey: cmd.Cluster.RouteKey},
		requestedDeadline,
	)
	if err != nil {
		if !errors.Is(err, api.ErrAlreadyExists) {
			return nil, err
		}
		// A concurrent command inserted the exact target. Re-read and validate it
		// under the normal existing-target contract; never overwrite or merge it.
		sb, err = o.clusterSandbox(ctx, cmd.SID, cmd.APISecretFingerprint)
		if err != nil {
			return nil, err
		}
	}
	if err := validateClusterSandboxContext(sb, cmd); err != nil {
		return nil, err
	}
	return sb, nil
}

// prepareClusterExecSession uses the same exact-target validation, optional
// synchronous KMT import, and durable resume admission as CmdConnect. Token
// signing runs under the lifecycle fence immediately before any new transition.
func (o *Orchestrator) prepareClusterExecSession(
	ctx context.Context,
	cmd *routesync.Command,
	now unixClock,
) (*types.Sandbox, *routesync.ExecSessionResult, error) {
	if err := validateClusterExecSessionEnvelope(cmd); err != nil {
		return nil, nil, err
	}
	compiler, err := execadmission.Default()
	if err != nil {
		return nil, nil, fmt.Errorf("cluster exec session: initialize admission: %w", err)
	}
	if _, err := compiler.Compile(cmd.ExecConditions); err != nil {
		return nil, nil, fmt.Errorf("cluster exec session: invalid conditions: %w", api.ErrBadRequest)
	}
	if _, err := execSessionExpiry(now(), cmd.TTLSeconds); err != nil {
		return nil, nil, err
	}
	if _, err := o.prepareClusterConnect(ctx, cmd, 0); err != nil {
		return nil, nil, err
	}
	var token string
	sb, _, err := o.ensureResumeAcceptedPrepared(ctx, cmd.SID, nil, func(current *types.Sandbox) error {
		if err := validateClusterSandboxCredentialBinding(current, cmd.APISecretFingerprint); err != nil {
			return err
		}
		return validateClusterSandboxContext(current, cmd)
	}, func(current *types.Sandbox) error {
		var err error
		token, err = mintExecSessionToken(current, cmd.TTLSeconds, cmd.ExecConditions, now())
		return err
	})
	if err != nil {
		return nil, nil, err
	}
	return sb, &routesync.ExecSessionResult{ExecAccessToken: token}, nil
}

func validateClusterExecSessionEnvelope(cmd *routesync.Command) error {
	if cmd == nil || cmd.Kind != routesync.CmdExecSession || cmd.CmdID == "" {
		return fmt.Errorf("cluster exec session: command id and kind are required")
	}
	if cmd.TTLSeconds < 0 || cmd.TimeoutSeconds != 0 || cmd.TemplateRef != "" || len(cmd.Config) != 0 ||
		cmd.APISecretType != "" || cmd.APISecret != "" || cmd.APISecretRef != "" ||
		cmd.ManifestKeyFingerprint != "" || cmd.ManifestKeyType != "" || cmd.ManifestKey != "" || cmd.ManifestKeyRef != "" ||
		cmd.ExpiresUnix != 0 || cmd.BuildID != "" || cmd.BuildResources != nil || cmd.ImageRepo != "" || cmd.RegistryAuth != "" ||
		len(cmd.BuildMMDSSecrets) != 0 {
		return fmt.Errorf("cluster exec session: command contains fields for another operation")
	}
	return nil
}

func clusterConnectDeadline(timeoutSeconds int) int64 {
	if timeoutSeconds <= 0 {
		return 0
	}
	return time.Now().Add(time.Duration(timeoutSeconds) * time.Second).Unix()
}

func clusterConnectResult(sb *types.Sandbox) (*routesync.ConnectResult, error) {
	if sb == nil || !types.ValidLocalSandboxID(sb.ID) {
		return nil, fmt.Errorf("cluster connect: valid node sandbox ID is required")
	}
	template, err := types.ParseTemplateID(sb.TemplateID)
	if err != nil || template.Profile != sb.Profile {
		return nil, fmt.Errorf("cluster connect: sandbox template and profile are inconsistent")
	}
	if sb.ForwardAccessToken == "" {
		return nil, fmt.Errorf("cluster connect: forward access token is required")
	}
	switch sb.Profile {
	case types.ProfileBare:
		if sb.EnvdAccessToken != "" || sb.TrafficAccessToken != "" {
			return nil, fmt.Errorf("cluster connect: bare sandbox contains e2b access tokens")
		}
	case types.ProfileE2B:
		if sb.EnvdAccessToken == "" || sb.TrafficAccessToken == "" ||
			!sandboxcfg.ValidE2BAccessToken(sb.EnvdAccessToken) ||
			!sandboxcfg.ValidE2BAccessToken(sb.TrafficAccessToken) {
			return nil, fmt.Errorf("cluster connect: e2b access tokens are required and must be valid")
		}
	default:
		return nil, fmt.Errorf("cluster connect: invalid sandbox profile %q", sb.Profile)
	}
	return &routesync.ConnectResult{
		NodeSandboxID:      sb.ID,
		TemplateID:         sb.TemplateID,
		Profile:            string(sb.Profile),
		EnvdAccessToken:    sb.EnvdAccessToken,
		TrafficAccessToken: sb.TrafficAccessToken,
		ForwardAccessToken: sb.ForwardAccessToken,
	}, nil
}

func (o *Orchestrator) deleteCluster(ctx context.Context, sb *types.Sandbox) error {
	unlock, err := o.lockLifecycleMutation(ctx, sb.ID)
	if err != nil {
		return err
	}
	defer unlock()
	o.launches.Cancel(sb.ID)

	current, err := o.st.Get(ctx, sb.ID)
	if err != nil {
		return err
	}
	if current == nil {
		return nil
	}
	attempt, launchActive := o.launches.Lookup(current.ID)
	if err := o.st.Delete(ctx, current.ID); err != nil {
		return err
	}
	if current.State == types.StateStarting && launchActive {
		cleanupCtx, cancel := cleanupContext()
		if err := o.stepLaunchCleanup(cleanupCtx, attempt, current, false); err != nil {
			o.log.Error("cluster sandbox delete cleanup incomplete; launch will retry",
				"sid", current.ID, "run_id", current.RunID, "err", err)
		}
		cancel()
	} else {
		cleanupCtx, cancel := cleanupContext()
		if err := o.teardown(cleanupCtx, current); err != nil {
			o.log.Error("cluster sandbox delete cleanup incomplete",
				"sid", current.ID, "run_id", current.RunID, "err", err)
		}
		cancel()
	}
	o.clearDeadlineIntent(current.ID)
	o.uncache(current.ID)
	o.publishDelete(current.ID)
	o.observeSandboxDelete(current)
	return nil
}

// putClusterKeyPair validates and atomically installs one inline tenant pair.
func (o *Orchestrator) putClusterKeyPair(ctx context.Context, cmd *routesync.Command) error {
	if cmd.APISecretType == clusterstate.SecretRef || cmd.APISecretRef != "" ||
		cmd.ManifestKeyType == clusterstate.SecretRef || cmd.ManifestKeyRef != "" {
		return fmt.Errorf("cluster: secret ref delivery is not configured")
	}
	if cmd.APISecret == "" || cmd.ManifestKey == "" ||
		cmd.APISecretFingerprint == "" || cmd.ManifestKeyFingerprint == "" {
		return fmt.Errorf("cluster: complete credential pair is required")
	}
	apiFP, err := store.APISecretHash(cmd.APISecret)
	if err != nil {
		return err
	}
	manifestFP, err := store.ManifestKeyHash(cmd.ManifestKey)
	if err != nil {
		return err
	}
	if apiFP != cmd.APISecretFingerprint || manifestFP != cmd.ManifestKeyFingerprint {
		return fmt.Errorf("cluster: credential pair fingerprint mismatch")
	}
	var ttl int64
	if cmd.ExpiresUnix > 0 {
		if ttl = cmd.ExpiresUnix - time.Now().Unix(); ttl <= 0 {
			ttl = 1
		}
	}
	_, err = o.st.AddKeyPair(ctx, store.KeyPair{
		APISecret: cmd.APISecret, ManifestKey: cmd.ManifestKey,
	}, "cluster", ttl, "")
	return err
}

// dropClusterKey removes a tenant pair from the node allowlist by its complete
// APISecret fingerprint. Existing sandbox/build credential copies are untouched.
// It is best-effort; normal withdrawal relies on TTL expiry when heartbeat
// refresh stops (cluster.md).
func (o *Orchestrator) dropClusterKey(ctx context.Context, fingerprint string) error {
	_, err := o.st.RemoveKeyPairByAPISecretFingerprint(ctx, fingerprint)
	return err
}
