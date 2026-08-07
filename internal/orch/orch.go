// Package orch is the orchestrator core. It ties together the store, systemd
// launcher, vswitch and config generation, and implements api.Core (control
// plane), proxy.Router (data plane) and configsock.Provider (dynamic config).
package orch

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/filestore"
	"github.com/kuasar-sandbox/orchestrator/internal/keys"
	"github.com/kuasar-sandbox/orchestrator/internal/launcher"
	"github.com/kuasar-sandbox/orchestrator/internal/migrationtoken"
	"github.com/kuasar-sandbox/orchestrator/internal/mmdssvc"
	"github.com/kuasar-sandbox/orchestrator/internal/proxy"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/orchestrator/internal/vswitch"
)

// vsClient is the vswitch surface the orchestrator uses; the production impl is
// *vswitch.CLI. Declared as an interface so the port attach/detach/tap-fd path
// can be substituted in tests (and launch exercised without a real connector-ctl vswitch).
type vsClient interface {
	Attach(ctx context.Context, req vswitch.AttachReq) (*vswitch.Port, error)
	Detach(ctx context.Context, port string) error
	TapFD(port string) vswitch.TapFD
}

type Orchestrator struct {
	cfg *config.Config
	st  *store.Store
	lc  launcher.Launcher
	vs  vsClient
	log *slog.Logger

	sandboxReadyTimeout time.Duration

	mu  sync.Mutex
	reg map[string]*types.Sandbox // in-memory immutable snapshots (hot path: Route/LaunchSpecFor)

	launches  launchGroup    // sole process-local owner of create and resume attempts
	lifecycle keyedLockGroup // serialize lifecycle mutations for one sid

	lifecycleCtxMu sync.RWMutex
	lifecycleCtx   context.Context // all accepted launch work; canceled on node shutdown

	deadlineIntentMu sync.Mutex
	deadlineIntents  map[string]struct{} // paused sandboxes whose next resume must preserve an explicit deadline

	subsMu sync.Mutex
	subs   map[int]chan routesync.Event // route-change subscribers (routesync clients)
	subSeq int

	routeLogMu sync.Mutex
	routeFP    string
	routeSeq   int64
	routeLog   []routeLogEntry

	pendMu sync.Mutex
	pend   map[string]*pendingBuild // builds whose unit is running (BuildSpecFor source)

	runnerPool     *runPool
	builderRunPool *runPool

	files *filestore.Store // COPY build-context object store; nil = unconfigured (COPY → 501)

	probe ResourceProbe // node water level for cluster heartbeat (set by serve when resource_listen on); nil = none

	clusterBuildMu sync.Mutex
	clusterBuilds  map[string]*clusterBuild   // build_id -> transient cluster image-pull creds (§7.5)
	buildEvents    chan *routesync.BuildEvent // node -> registry build state, drained by the node-link client

	// synthetic build sandbox id -> durable build owner id. Protected by mu
	// alongside reg; never persisted or exported.
	mmdsBuildOwners map[string]string
	// Parsed once from conductor-owned mmds.services. Values are absolute Unix
	// socket paths and never come from proxy.yaml or a tenant document.
	mmdsServices mmdssvc.Registry
}

// clusterBuild is a registry-driven build's transient image-pull context. Cluster
// identity remains opaque in Build.Metadata and is never interpreted here.
type clusterBuild struct {
	imageRepo    string
	registryAuth string
}

func New(cfg *config.Config, st *store.Store, lc launcher.Launcher, vs vsClient, log *slog.Logger) *Orchestrator {
	mmdsServices, _ := mmdssvc.BuildRegistry(cfg.MMDS.ServiceEndpoints())
	o := &Orchestrator{
		cfg: cfg, st: st, lc: lc, vs: vs, log: log,
		sandboxReadyTimeout: 60 * time.Second,
		reg:                 map[string]*types.Sandbox{},
		lifecycleCtx:        context.Background(),
		deadlineIntents:     map[string]struct{}{},
		subs:                map[int]chan routesync.Event{},
		routeFP:             uuid.NewString(),
		pend:                map[string]*pendingBuild{},
		clusterBuilds:       map[string]*clusterBuild{},
		buildEvents:         make(chan *routesync.BuildEvent, 64),
		mmdsBuildOwners:     map[string]string{},
		mmdsServices:        mmdsServices,
	}
	wait := cfg.Units.PoolWaitDuration()
	o.runnerPool = newRunPool(runKindSandbox, cfg.Units.RunnerPoolSize, wait, cfg.Paths.RunRoot, lc, o.runnerUnit, log.With("pool", "runner"))
	o.builderRunPool = newRunPool(runKindBuild, cfg.Units.BuilderPoolSize, wait, cfg.Paths.RunRoot, lc, o.builderUnit, log.With("pool", "builder"))
	if fc := cfg.Builder.FilesStorage; fc != nil {
		fs, err := filestore.New(fc)
		if err != nil {
			log.Warn("builder.files_storage init failed; COPY steps will be rejected", "err", err)
		} else {
			o.files = fs
		}
	}
	return o
}

func (o *Orchestrator) StartRunPools(ctx context.Context) error {
	if err := o.runnerPool.Start(ctx); err != nil {
		return fmt.Errorf("start runner pool: %w", err)
	}
	if err := o.builderRunPool.Start(ctx); err != nil {
		return fmt.Errorf("start builder pool: %w", err)
	}
	return nil
}

// --- api.Core ---

func (o *Orchestrator) Create(ctx context.Context, req api.CreateReq) (*types.Sandbox, error) {
	admissionStarted := time.Now()
	if req.TimeoutSec <= 0 {
		req.TimeoutSec = o.cfg.Sandbox.TimeoutSec // default TTL (sandbox.timeout_sec)
	}
	pair, err := o.resolveAllowed(ctx, req.APIKey)
	if err != nil {
		return nil, err
	}
	if pair.APISecret == "" {
		return nil, api.ErrNotAllowed
	}
	tmpl, err := types.ParseTemplateID(req.TemplateID)
	if err != nil {
		// Not a canonical persistent id — resolve the transient register id the SDK
		// reports (BuildInfo.template_id), or a build name/alias, to its persist id.
		persist := o.resolveTemplateAlias(ctx, req.APIKey, req.TemplateID)
		if persist == "" {
			return nil, err
		}
		if tmpl, err = types.ParseTemplateID(persist); err != nil {
			return nil, err
		}
	}
	// Layer the template's declared config under this create request. Restore,
	// credentials, and checkpoint policy are deliberately request-scoped, so
	// template metadata values in those namespaces are not inherited.
	var templateMetadata map[string]string
	if tb := o.templateBuild(ctx, req.APIKey, req.TemplateID); tb != nil && len(tb.Metadata) > 0 {
		templateMetadata = tb.Metadata
	}
	meta := sandboxcfg.MergeCreateMetadata(templateMetadata, req.Metadata)
	mmdsDoc, meta, err := sandboxcfg.ExtractMMDS(meta, req.MMDSHeader, o.mmdsPolicy())
	if err != nil {
		return nil, fmt.Errorf("%w: %v", api.ErrBadRequest, err)
	}
	// Validate request-scoped policy before allocating an identity, minting
	// credentials, creating directories, attaching networking, or starting a
	// process. Normalization also gives every later trust boundary one canonical
	// value to parse.
	meta, err = sandboxcfg.NormalizeRestoreMetadata(meta)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", api.ErrBadRequest, err)
	}
	meta, err = sandboxcfg.NormalizeCheckpointMetadata(meta)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", api.ErrBadRequest, err)
	}
	credentials, meta, err := sandboxcfg.ExtractCredentials(meta)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", api.ErrBadRequest, err)
	}
	if err := o.validateCreateCheckpointMode(meta); err != nil {
		return nil, err
	}
	if err := validateSandboxCredentialOverrides(tmpl.Profile, credentials); err != nil {
		return nil, fmt.Errorf("%w: %v", api.ErrBadRequest, err)
	}
	if _, err := sandboxcfg.ParseSpec(meta); err != nil {
		return nil, fmt.Errorf("%w: %v", api.ErrBadRequest, err)
	}

	id, err := uuid.NewV7()
	if err != nil {
		return nil, fmt.Errorf("orch: new id: %w", err)
	}
	sid := id.String()

	sb := &types.Sandbox{
		ID:           sid,
		Profile:      tmpl.Profile,
		TemplateID:   tmpl.String(), // canonical persist id (resolved from a transient/alias ref)
		State:        types.StateStarting,
		RunDir:       o.cfg.Paths.RunRoot + "/" + sid,
		BaseDir:      o.cfg.Paths.BaseRoot + "/" + sid,
		APISecret:    pair.APISecret,
		ManifestKey:  pair.ManifestKey,
		Metadata:     meta,
		Env:          req.EnvVars,
		CreatedUnix:  time.Now().Unix(),
		DeadlineUnix: time.Now().Add(time.Duration(req.TimeoutSec) * time.Second).Unix(),
	}
	if err := materializeSandboxCredentials(sb, credentials); err != nil {
		return nil, fmt.Errorf("orch: create credentials: %w", err)
	}
	if tmpl.Profile == types.ProfileE2B {
		sb.EnvdUDS = sb.RunDir + "/envd.sock"
		sb.CiUDS = sb.RunDir + "/ci.sock"
	}
	initialMMDS := initialMMDSRouteSecretValues(mmdsDoc)
	accepted, _, err := o.acceptFreshLaunch(ctx, sb, tmpl, initialMMDS)
	if err != nil {
		return nil, err
	}
	o.logLaunchPhase(nil, accepted, "admission_duration", time.Since(admissionStarted))
	return cloneSandbox(accepted), nil
}

// acceptFreshLaunch is the common standalone/cluster create admission. The
// lifecycle lock orders initial publication against a concurrent Delete; launch
// ownership is claimed before the durable starting row becomes visible.
func (o *Orchestrator) acceptFreshLaunch(ctx context.Context, sb *types.Sandbox, tmpl types.TemplateID, initialMMDS *mmdsInitialRouteSecretValues) (*types.Sandbox, *launchAttempt, error) {
	if sb == nil || sb.State != types.StateStarting {
		return nil, nil, fmt.Errorf("orch: fresh launch requires a starting sandbox")
	}
	if sb.RunID != "" || sb.FloatingIP != "" || sb.VswitchPort != "" || sb.InnerIP != "" || sb.PortMAC != "" {
		return nil, nil, fmt.Errorf("orch: fresh launch admission requires empty runner and network ownership")
	}
	if _, err := sandboxcfg.ParseSpec(sb.Metadata); err != nil {
		return nil, nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	lifecycleCtx := o.launchContext()
	if err := lifecycleCtx.Err(); err != nil {
		return nil, nil, fmt.Errorf("orch: lifecycle is stopping: %w", err)
	}
	if err := validateInitialMMDSRouteEntry(sb, initialMMDS); err != nil {
		return nil, nil, fmt.Errorf("%w: %v", api.ErrBadRequest, err)
	}

	unlock := o.lifecycle.Lock(sb.ID)
	defer unlock()
	attempt, err := o.launches.Claim(lifecycleCtx, sb.ID, launchCreate)
	if err != nil {
		return nil, nil, err
	}
	if err := lifecycleCtx.Err(); err != nil {
		o.launches.Finish(attempt, err)
		return nil, nil, fmt.Errorf("orch: lifecycle is stopping: %w", err)
	}
	var routesDigest string
	var secretValues store.MMDSRouteSecretValues
	if initialMMDS != nil {
		routesDigest = initialMMDS.routesDigest
		secretValues = initialMMDS.values
	}
	if err := o.st.InsertSandboxWithMMDSRouteSecretValues(ctx, cloneSandbox(sb), routesDigest, secretValues); err != nil {
		o.launches.Finish(attempt, err)
		return nil, nil, err
	}
	attempt.SetAcceptedAt(time.Now())
	initial := cloneSandbox(sb)
	o.cache(initial)
	o.publishUpsert(initial)
	work := cloneSandbox(sb)
	o.launches.Start(attempt, func(launchCtx context.Context, current *launchAttempt) error {
		return o.runLaunch(launchCtx, current, work, tmpl)
	})
	return cloneSandbox(initial), attempt, nil
}

type launchStageError struct {
	stage string
	err   error
}

func (e *launchStageError) Error() string { return e.err.Error() }
func (e *launchStageError) Unwrap() error { return e.err }

func launchFailed(stage string, err error) error {
	if err == nil {
		return nil
	}
	return &launchStageError{stage: stage, err: err}
}

func launchFailureStage(err error) string {
	var staged *launchStageError
	if errors.As(err, &staged) {
		return staged.stage
	}
	return "unknown"
}

func (o *Orchestrator) logLaunchPhase(attempt *launchAttempt, sb *types.Sandbox, phase string, duration time.Duration) {
	kind := launchCreate
	if attempt != nil {
		kind = attempt.Kind()
	}
	o.log.Info("sandbox launch phase",
		"sid", sb.ID,
		"run_id", sb.RunID,
		"kind", kind,
		"profile", sb.Profile,
		"phase", phase,
		"duration", duration)
}

func (o *Orchestrator) runLaunch(ctx context.Context, attempt *launchAttempt, sb *types.Sandbox, tmpl types.TemplateID) error {
	err := o.launchSandbox(ctx, attempt, sb, tmpl)
	result := "success"
	failureStage := ""
	if err != nil {
		result = "failure"
		failureStage = launchFailureStage(err)
		if rollbackErr := o.rollbackLaunch(attempt, sb); rollbackErr != nil {
			err = errors.Join(err, rollbackErr)
		}
	}
	acceptedAt := attempt.AcceptedAt()
	if acceptedAt.IsZero() {
		acceptedAt = time.Now()
	}
	o.log.Info("sandbox launch terminal",
		"sid", sb.ID,
		"run_id", attempt.RunID(),
		"kind", attempt.Kind(),
		"profile", sb.Profile,
		"result", result,
		"failure_stage", failureStage,
		"starting_total_duration", time.Since(acceptedAt),
		"err", err)
	return err
}

// launchSandbox prepares all runner handoff inputs, then spends separate runner
// assignment and runtime readiness budgets. The durable starting row already
// exists and is never replaced by a stale whole-row Put.
func (o *Orchestrator) launchSandbox(ctx context.Context, attempt *launchAttempt, sb *types.Sandbox, tmpl types.TemplateID) error {
	if sb == nil || attempt == nil || sb.State != types.StateStarting || sb.ID != attempt.SID() {
		return launchFailed("prepare", fmt.Errorf("orch: launch requires its claimed starting sandbox"))
	}
	prepareStarted := time.Now()
	prepareLogged := false
	defer func() {
		// A failure before runner handoff still needs a complete prepare timing.
		// Successful preparation logs at its exact boundary below.
		if !prepareLogged {
			o.logLaunchPhase(attempt, sb, "prepare_duration", time.Since(prepareStarted))
		}
	}()
	for _, d := range []string{sb.RunDir, sb.BaseDir} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			return launchFailed("prepare", fmt.Errorf("orch: mkdir %s: %w", d, err))
		}
	}
	// MkdirAll preserves an existing directory's mode. The readiness socket is
	// private launch coordination, so restore the run directory invariant before
	// binding it even when this is a resume into a pre-existing directory.
	if err := os.Chmod(sb.RunDir, 0o700); err != nil {
		return launchFailed("prepare", fmt.Errorf("orch: chmod %s: %w", sb.RunDir, err))
	}
	spec, err := sandboxcfg.ParseSpec(sb.Metadata)
	if err != nil {
		return launchFailed("prepare", err)
	}
	// Restore (resume / snp-template create / migration import): inherit the
	// snapshot's logical network for fields the create config left unset (point 7 —
	// explicit create config wins, the snapshot fills the rest), and pin capacity to
	// the snapshot (the runtime refuses a mismatch). Read before attach so an
	// inherited inner_ip / transit_* reaches resolveNetwork + attachNetwork.
	var snap snapInfo
	if ref := sandboxcfg.RestoreRefFor(sb, tmpl); ref != "" {
		snap = o.snapshotConfig(ctx, sb, ref)
		spec.Network = sandboxcfg.MergeNetwork(snap.Network, spec.Network)
	}
	network, err := o.resolveNetwork(tmpl.Profile, spec.Network, o.cfg.Sandbox.Network.Hostname)
	if err != nil {
		return launchFailed("prepare", err)
	}
	port, err := o.attachNetwork(ctx, network)
	if err != nil {
		return launchFailed("network", err)
	}
	sb.VswitchPort, sb.FloatingIP, sb.PortMAC, sb.InnerIP = port.Port, port.FloatingIP, port.MAC, network.InnerIP
	resources := store.StartingResources{
		FloatingIP: sb.FloatingIP, VswitchPort: sb.VswitchPort, InnerIP: sb.InnerIP, PortMAC: sb.PortMAC,
	}
	changed, err := o.st.SetStartingResources(ctx, sb.ID, resources)
	if err != nil || !changed {
		ownershipErr := err
		if ownershipErr == nil {
			ownershipErr = errLaunchOwnershipLost
		}
		detachCtx, cancelDetach := cleanupContext()
		detachErr := o.vs.Detach(detachCtx, port.Port)
		cancelDetach()
		if detachErr == nil {
			sb.VswitchPort, sb.FloatingIP, sb.PortMAC, sb.InnerIP = "", "", "", ""
		} else {
			// Retain the exact local ownership until rollback cleanup has had a
			// second chance to detach it. Clearing it here would permanently leak
			// an unpersisted port when the immediate detach failed.
			ownershipErr = errors.Join(ownershipErr,
				fmt.Errorf("orch: detach uncommitted port %s: %w", port.Port, detachErr))
		}
		return launchFailed("resources", ownershipErr)
	}

	p := o.sandboxParams(sb, tmpl, spec, network)
	// Pin capacity to the snapshot the runtime froze (read above). Template
	// snapshots are self-describing and may have been taken at a different budget
	// than this node's create defaults (e.g. the build pipeline's builder.vcpu/memory).
	if snap.HasCapacity {
		p.VCPU, p.Memory = snap.CapCPU, snap.CapMem
	}
	if err := p.WriteYAML(o.sandboxConfigPath(sb)); err != nil {
		return launchFailed("config", err)
	}
	readyListener, err := listenRuntimeReadiness(o.cfg.Paths.RunRoot, sb.ID)
	if err != nil {
		return launchFailed("readiness_socket", err)
	}
	defer readyListener.Close()
	// Persisted ownership comes first; publication is ordered against Delete and
	// occurs only once every runner handoff input is ready.
	unlock := o.lifecycle.Lock(sb.ID)
	current, getErr := o.st.Get(ctx, sb.ID)
	if getErr == nil && current != nil && current.State == types.StateStarting && current.RunID == "" && current.VswitchPort == sb.VswitchPort {
		o.cache(current)
		o.publishUpsert(current)
	}
	unlock()
	if getErr != nil {
		return launchFailed("resources", getErr)
	}
	if current == nil || current.State != types.StateStarting || current.RunID != "" || current.VswitchPort != sb.VswitchPort {
		return launchFailed("resources", errLaunchOwnershipLost)
	}
	o.logLaunchPhase(attempt, sb, "prepare_duration", time.Since(prepareStarted))
	prepareLogged = true

	// Binding precedes assignment so node-ctl can connect immediately after its
	// long-poll returns; no retry window is needed between the two processes.
	assignStarted := time.Now()
	assignmentCtx, cancelAssignment := context.WithTimeout(ctx, o.cfg.Units.PoolWaitDuration())
	var commitStarted, commitFinished time.Time
	runID, err := o.runnerPool.Assign(assignmentCtx, sb.ID, func(runID string) error {
		commitStarted = time.Now()
		// Serialize the runner-binding linearization point with Kill/Delete and
		// other lifecycle mutations. If deletion wins, the exact CAS below misses
		// and the pool never hands the task to this runner; if binding wins, the
		// destructive operation re-reads and stops the exact persisted run ID.
		unlockCommit := o.lifecycle.Lock(sb.ID)
		defer unlockCommit()
		changed, err := o.st.BindStartingRunner(ctx, sb.ID, runID)
		if err != nil {
			return err
		}
		if !changed {
			return errLaunchOwnershipLost
		}
		sb.RunID = runID
		attempt.SetRunID(runID)
		o.mutateCached(sb.ID, func(cached *types.Sandbox) { cached.RunID = runID })
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
		return launchFailed("runner", err)
	}
	if sb.RunID == "" {
		sb.RunID = runID
		attempt.SetRunID(runID)
	}
	if commitFinished.IsZero() {
		commitFinished = assignStarted
	}
	o.logLaunchPhase(attempt, sb, "runner_handoff_duration", time.Since(commitFinished))
	launchTimeout := o.sandboxReadyTimeout
	if launchTimeout <= 0 {
		launchTimeout = 60 * time.Second
	}
	// Runtime readiness and, for E2B, mandatory envd initialization consume one
	// launch budget beginning after assignment. /init is the first envd request:
	// health is meaningful only after initialization and is not a launch gate.
	launchCtx, cancelLaunch := context.WithTimeout(ctx, launchTimeout)
	defer cancelLaunch()
	runtimeStarted := time.Now()
	err = waitRuntimeReadiness(launchCtx, readyListener)
	o.logLaunchPhase(attempt, sb, "runtime_ready_duration", time.Since(runtimeStarted))
	if err != nil {
		return launchFailed("runtime", fmt.Errorf("orch: sandbox %s: %w", sb.ID, err))
	}
	if tmpl.Profile == types.ProfileE2B {
		envdStarted := time.Now()
		err = o.envdInit(launchCtx, sb)
		o.logLaunchPhase(attempt, sb, "envd_init_duration", time.Since(envdStarted))
		if err != nil {
			return launchFailed("envd", err)
		}
	}
	unlock = o.lifecycle.Lock(sb.ID)
	defer unlock()
	changed, err = o.st.CommitStartingRunning(ctx, sb.ID, sb.RunID)
	if err != nil {
		return launchFailed("commit", fmt.Errorf("orch: commit launch %s: %w", sb.ID, err))
	}
	if !changed {
		return launchFailed("commit", fmt.Errorf("orch: commit launch %s: starting runner %s no longer owns the sandbox: %w", sb.ID, sb.RunID, errLaunchOwnershipLost))
	}
	if attempt.Kind() == launchResume {
		// Explicit paused/starting deadline intent survives failed resumes and is
		// consumed only by a successful exact-run terminal commit.
		o.clearDeadlineIntent(sb.ID)
	}
	// Preserve narrow concurrent updates (notably SetTimeout) made after the
	// launch started. The worker's sb is deliberately a private snapshot and
	// must never replace the authoritative cached record after acceptance.
	running := o.mutateCached(sb.ID, func(cached *types.Sandbox) {
		cached.State = types.StateRunning
		cached.RunID = sb.RunID
	})
	if running == nil {
		running, err = o.st.Get(ctx, sb.ID)
		if err != nil {
			// The exact CAS above is the terminal linearization point. A read
			// failure must not turn a committed running sandbox into launch
			// cleanup (which would stop its runner and detach its network).
			o.log.Error("reload committed sandbox failed", "sid", sb.ID, "run_id", sb.RunID, "err", err)
			running = cloneSandbox(sb)
			running.State = types.StateRunning
		}
		if running == nil || running.State != types.StateRunning || running.RunID != sb.RunID {
			o.log.Error("committed sandbox cache invariant failed", "sid", sb.ID, "run_id", sb.RunID)
			running = cloneSandbox(sb)
			running.State = types.StateRunning
		}
		o.cache(running)
	}
	sb.State = types.StateRunning
	sb.DeadlineUnix = running.DeadlineUnix
	o.publishUpsert(running)
	return nil
}

func cleanupContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 30*time.Second)
}

const (
	launchCleanupRetryMin = 100 * time.Millisecond
	launchCleanupRetryMax = 5 * time.Second
)

// launchCleanupProgress remembers individual cleanup successes across retries.
// Stop/detach are expected to be idempotent, but retaining progress avoids
// turning an already released resource into a permanent retry error when a
// different cleanup operation failed in the same pass.
type launchCleanupProgress struct {
	unit           string
	runnerStopped  bool
	runnerReset    bool
	port           string
	portDetached   bool
	runDir         string
	runDirRemoved  bool
	baseDir        string
	baseDirRemoved bool
}

func (p *launchCleanupProgress) merge(o *Orchestrator, attempt *launchAttempt, sb *types.Sandbox) {
	runID := attempt.RunID()
	if runID == "" {
		runID = sb.RunID
	}
	if p.unit == "" && runID != "" {
		p.unit = o.runnerUnit(runID)
	}
	if p.port == "" && sb.VswitchPort != "" {
		p.port = sb.VswitchPort
	}
	if p.runDir == "" {
		p.runDir = sb.RunDir
	}
	if p.baseDir == "" && attempt.Kind() == launchCreate {
		p.baseDir = sb.BaseDir
	}
}

func (p *launchCleanupProgress) step(ctx context.Context, o *Orchestrator, includeLocal bool) error {
	if p.unit != "" && !p.runnerStopped {
		if err := o.lc.Stop(ctx, p.unit); err != nil {
			return fmt.Errorf("stop %s: %w", p.unit, err)
		}
		p.runnerStopped = true
	}
	if p.unit != "" && p.runnerStopped && !p.runnerReset {
		if err := o.lc.ResetFailed(ctx, p.unit); err != nil {
			return fmt.Errorf("reset %s: %w", p.unit, err)
		}
		p.runnerReset = true
	}
	if p.port != "" && !p.portDetached {
		if err := o.vs.Detach(ctx, p.port); err != nil {
			return fmt.Errorf("detach port %s: %w", p.port, err)
		}
		p.portDetached = true
	}
	if includeLocal && p.runDir != "" && !p.runDirRemoved {
		if err := os.RemoveAll(p.runDir); err != nil {
			return fmt.Errorf("remove run dir %s: %w", p.runDir, err)
		}
		p.runDirRemoved = true
	}
	if includeLocal && p.baseDir != "" && !p.baseDirRemoved {
		if err := os.RemoveAll(p.baseDir); err != nil {
			return fmt.Errorf("remove base dir %s: %w", p.baseDir, err)
		}
		p.baseDirRemoved = true
	}
	return nil
}

func (o *Orchestrator) stepLaunchCleanup(ctx context.Context, attempt *launchAttempt, sb *types.Sandbox, includeLocal bool) error {
	attempt.cleanupMu.Lock()
	defer attempt.cleanupMu.Unlock()
	if attempt.cleanup == nil {
		attempt.cleanup = &launchCleanupProgress{}
	}
	attempt.cleanup.merge(o, attempt, sb)
	return attempt.cleanup.step(ctx, o, includeLocal)
}

func nextLaunchCleanupRetry(delay time.Duration) time.Duration {
	delay *= 2
	if delay > launchCleanupRetryMax {
		return launchCleanupRetryMax
	}
	return delay
}

func waitLaunchCleanupRetry(delay time.Duration) {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	<-timer.C
}

func (o *Orchestrator) rollbackLaunch(attempt *launchAttempt, sb *types.Sandbox) error {
	retryDelay := launchCleanupRetryMin
	var firstCleanupErr error
	for {
		ctx, cancel := cleanupContext()
		cleanupErr := o.stepLaunchCleanup(ctx, attempt, sb, true)
		cancel()
		if cleanupErr == nil {
			break
		}
		if firstCleanupErr == nil {
			firstCleanupErr = cleanupErr
		}
		// Durable runner/network ownership and the process-local claim remain
		// starting until every local cleanup operation succeeds. This retry is
		// deliberately independent of the canceled attempt context; conductor
		// shutdown drains launchGroup before closing the store/launcher.
		o.log.Error("sandbox launch cleanup incomplete; retrying",
			"sid", sb.ID, "run_id", attempt.RunID(), "kind", attempt.Kind(),
			"retry_in", retryDelay, "err", cleanupErr)
		waitLaunchCleanupRetry(retryDelay)
		retryDelay = nextLaunchCleanupRetry(retryDelay)
	}

	expectedRunID := attempt.RunID()
	retryDelay = launchCleanupRetryMin
	var firstStoreErr error
	for {
		ctx, cancel := cleanupContext()
		unlock := o.lifecycle.Lock(sb.ID)
		var (
			changed bool
			err     error
		)
		if attempt.Kind() == launchResume {
			changed, err = o.st.RollbackStartingPaused(ctx, sb.ID, expectedRunID)
		} else {
			changed, err = o.st.RollbackStartingDead(ctx, sb.ID, expectedRunID)
		}
		if err != nil {
			unlock()
			cancel()
			if firstStoreErr == nil {
				firstStoreErr = err
			}
			o.log.Error("sandbox launch rollback commit failed; retrying",
				"sid", sb.ID, "run_id", expectedRunID, "kind", attempt.Kind(),
				"retry_in", retryDelay, "err", err)
			waitLaunchCleanupRetry(retryDelay)
			retryDelay = nextLaunchCleanupRetry(retryDelay)
			continue
		}
		if !changed {
			unlock()
			cancel()
			return errors.Join(firstCleanupErr, firstStoreErr)
		}
		if attempt.Kind() == launchCreate {
			o.uncache(sb.ID)
			o.publishDelete(sb.ID)
			unlock()
			cancel()
			return errors.Join(firstCleanupErr, firstStoreErr)
		}

		// Keep the lifecycle lock from the paused CAS through authoritative
		// reload and publication. A transient local-store read failure retries
		// without exposing paused to a new admission before its route is visible.
		for {
			paused, getErr := o.st.Get(ctx, sb.ID)
			if getErr == nil {
				if paused == nil || paused.State != types.StatePaused {
					unlock()
					cancel()
					return errors.Join(firstCleanupErr, firstStoreErr,
						fmt.Errorf("orch: resume rollback %s did not produce paused state", sb.ID))
				}
				o.cache(paused)
				o.publishUpsert(paused)
				unlock()
				cancel()
				return errors.Join(firstCleanupErr, firstStoreErr)
			}
			cancel()
			if firstStoreErr == nil {
				firstStoreErr = getErr
			}
			o.log.Error("sandbox resume rollback reload failed; retrying",
				"sid", sb.ID, "run_id", expectedRunID,
				"retry_in", retryDelay, "err", getErr)
			waitLaunchCleanupRetry(retryDelay)
			retryDelay = nextLaunchCleanupRetry(retryDelay)
			ctx, cancel = cleanupContext()
		}
	}
}

func (o *Orchestrator) Get(ctx context.Context, id, apiKey string) (*types.Sandbox, error) {
	sb, err := o.st.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if !ownsSandbox(sb, apiKey) {
		return nil, api.ErrNotFound
	}
	return sb, nil
}

func (o *Orchestrator) List(ctx context.Context, apiKey, state string, limit int, cursor string) ([]*types.Sandbox, string, error) {
	rows, next, err := o.st.List(ctx, state, ownerHash(apiKey), limit, cursor)
	if err != nil {
		return nil, "", err
	}
	// The hash pre-filter is non-unique; verify the MAC per row (and drop any
	// hash-collision rows belonging to another tenant).
	out := rows[:0]
	for _, sb := range rows {
		if verifyKey(apiKey, sb.APISecret) {
			out = append(out, sb)
		}
	}
	return out, next, nil
}

func (o *Orchestrator) Kill(ctx context.Context, id, apiKey string) (bool, error) {
	unlock := o.lifecycle.Lock(id)
	defer unlock()

	sb, err := o.st.Get(ctx, id)
	if err != nil {
		return false, err
	}
	if !ownsSandbox(sb, apiKey) {
		return false, nil
	}
	attempt, launchActive := o.launches.Lookup(id)
	o.launches.Cancel(id)
	if err := o.st.Delete(ctx, id); err != nil {
		return false, err
	}
	if sb.State == types.StateStarting && launchActive {
		// Share exact runner/network cleanup progress with rollback. Local dirs
		// remain the worker's responsibility because preparation may still be
		// returning from a late external call after this synchronous Kill pass.
		cleanupCtx, cancel := cleanupContext()
		if err := o.stepLaunchCleanup(cleanupCtx, attempt, sb, false); err != nil {
			o.log.Error("sandbox kill cleanup incomplete; launch will retry",
				"sid", sb.ID, "run_id", sb.RunID, "err", err)
		}
		cancel()
	} else {
		cleanupCtx, cancel := cleanupContext()
		if err := o.teardown(cleanupCtx, sb); err != nil {
			o.log.Error("sandbox kill cleanup incomplete", "sid", sb.ID, "run_id", sb.RunID, "err", err)
		}
		cancel()
	}
	o.clearDeadlineIntent(id)
	o.uncache(id)
	o.publishDelete(id) // tell external proxies the route is gone
	return true, nil
}

func (o *Orchestrator) Pause(ctx context.Context, id, apiKey string, actionOverride sandboxcfg.CheckpointPolicy) error {
	unlock := o.lifecycle.Lock(id)
	defer unlock()

	sb, err := o.st.Get(ctx, id)
	if err != nil {
		return err
	}
	if !ownsSandbox(sb, apiKey) {
		return api.ErrNotFound
	}
	if sb.State == types.StateStarting {
		return api.ErrSandboxStarting
	}
	policy, err := o.resolveCheckpointPolicy(sb.Metadata, actionOverride)
	if err != nil {
		return err
	}
	return o.pauseSandboxLocked(ctx, sb, policy)
}

// pauseSandbox snapshots a running sandbox and stops it — the work behind Pause
// and the reaper's auto-suspend (no api key: the caller has already authorized).
func (o *Orchestrator) pauseSandbox(ctx context.Context, sb *types.Sandbox) error {
	unlock := o.lifecycle.Lock(sb.ID)
	defer unlock()

	current, err := o.st.Get(ctx, sb.ID)
	if err != nil {
		return err
	}
	if current == nil {
		return api.ErrNotFound
	}
	if current.State == types.StatePaused {
		return api.ErrAlreadyPaused
	}
	if current.State == types.StateStarting {
		return api.ErrSandboxStarting
	}
	policy, err := o.resolveCheckpointPolicy(current.Metadata, sandboxcfg.CheckpointPolicy{})
	if err != nil {
		return err
	}
	return o.pauseSandboxLocked(ctx, current, policy)
}

func (o *Orchestrator) pauseSandboxLocked(ctx context.Context, sb *types.Sandbox, policy sandboxcfg.CheckpointPolicy) error {
	if sb.State == types.StatePaused {
		return api.ErrAlreadyPaused
	}
	if sb.State == types.StateStarting {
		return api.ErrSandboxStarting
	}
	if o.cfg.Checkpoint.Mode == config.CheckpointLocal {
		o.log.Info("checkpoint policy resolved",
			"sid", sb.ID,
			"merge_ref", checkpointPolicyValue(policy.MergeRef),
			"drop_caches", checkpointPolicyValue(policy.DropCaches))
	}
	ref, err := o.snapshot(ctx, sb, policy)
	if err != nil {
		return err
	}
	sb.SnapshotRef = ref
	sb.State = types.StatePaused
	_ = o.st.SetSnapshotRef(ctx, sb.ID, ref)
	_ = o.st.SetState(ctx, sb.ID, types.StatePaused)
	o.cache(sb)
	if sb.RunID != "" {
		_ = o.lc.Stop(ctx, o.runnerUnit(sb.RunID))
		_ = o.lc.ResetFailed(ctx, o.runnerUnit(sb.RunID))
	}
	_ = o.vs.Detach(ctx, sb.VswitchPort)
	o.publishUpsert(sb) // proxies keep the (now paused) route so traffic triggers a Wake
	return nil
}

func checkpointPolicyValue(value *bool) string {
	if value == nil {
		return "default"
	}
	if *value {
		return "true"
	}
	return "false"
}

func (o *Orchestrator) nodeCheckpointPolicy() sandboxcfg.CheckpointPolicy {
	return sandboxcfg.CloneCheckpointPolicy(sandboxcfg.CheckpointPolicy{
		MergeRef:   o.cfg.Checkpoint.MergeRef,
		DropCaches: o.cfg.Checkpoint.DropCaches,
	})
}

func checkpointMetadataPolicy(metadata map[string]string) (sandboxcfg.CheckpointPolicy, error) {
	raw, ok := metadata[sandboxcfg.NsCheckpoint]
	if !ok {
		return sandboxcfg.CheckpointPolicy{}, nil
	}
	policy, err := sandboxcfg.ParseCheckpointPolicyJSON(raw)
	if err != nil {
		return sandboxcfg.CheckpointPolicy{}, fmt.Errorf("metadata[%q]: %w", sandboxcfg.NsCheckpoint, err)
	}
	return policy, nil
}

func (o *Orchestrator) validateCreateCheckpointMode(metadata map[string]string) error {
	policy, err := checkpointMetadataPolicy(metadata)
	if err != nil {
		return fmt.Errorf("%w: %v", api.ErrBadRequest, err)
	}
	if o.cfg.Checkpoint.Mode == config.CheckpointRemote && !policy.Empty() {
		return fmt.Errorf("%w: checkpoint policy requires checkpoint.mode=local", api.ErrBadRequest)
	}
	return nil
}

// resolveCheckpointPolicy validates historical metadata under the lifecycle
// lock and resolves local policy field by field: node < metadata < action. In
// deprecated remote mode every new-policy source is rejected and legacy capture
// routing remains unchanged.
func (o *Orchestrator) resolveCheckpointPolicy(metadata map[string]string, actionOverride sandboxcfg.CheckpointPolicy) (sandboxcfg.CheckpointPolicy, error) {
	metadataPolicy, err := checkpointMetadataPolicy(metadata)
	if err != nil {
		return sandboxcfg.CheckpointPolicy{}, fmt.Errorf("%w: %v", api.ErrBadRequest, err)
	}
	switch o.cfg.Checkpoint.Mode {
	case config.CheckpointLocal:
		policy := sandboxcfg.OverlayCheckpointPolicy(o.nodeCheckpointPolicy(), metadataPolicy)
		return sandboxcfg.OverlayCheckpointPolicy(policy, actionOverride), nil
	case config.CheckpointRemote:
		if !metadataPolicy.Empty() {
			return sandboxcfg.CheckpointPolicy{}, fmt.Errorf("%w: sandbox checkpoint policy requires checkpoint.mode=local", api.ErrBadRequest)
		}
		if !actionOverride.Empty() {
			return sandboxcfg.CheckpointPolicy{}, fmt.Errorf("%w: Pause checkpoint policy requires checkpoint.mode=local", api.ErrBadRequest)
		}
		return sandboxcfg.CheckpointPolicy{}, nil
	default:
		return sandboxcfg.CheckpointPolicy{}, fmt.Errorf("%w: unsupported checkpoint.mode %q", api.ErrBadRequest, o.cfg.Checkpoint.Mode)
	}
}

func (o *Orchestrator) Connect(ctx context.Context, id, apiKey, migrationToken string, timeoutSec int) (*types.Sandbox, error) {
	_, err := o.prepareStandaloneTarget(ctx, id, apiKey, migrationToken)
	if err != nil {
		return nil, err
	}
	var requestedDeadline *int64
	if timeoutSec > 0 {
		deadline := time.Now().Add(time.Duration(timeoutSec) * time.Second).Unix()
		requestedDeadline = &deadline
	}
	sb, _, err := o.ensureResumeAccepted(ctx, id, requestedDeadline, func(current *types.Sandbox) error {
		if !ownsSandbox(current, apiKey) {
			return api.ErrNotFound
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return cloneSandbox(sb), nil
}

// ConnectWithMMDS is the standalone-only CONNECT extension. Existing targets
// are checked before the migration token or MMDS request is parsed and ignore
// both completely. A real import admits routes only from the token and accepts
// request-side initial secret values only.
func (o *Orchestrator) ConnectWithMMDS(ctx context.Context, id, apiKey, migrationToken string, timeoutSec int, metadata map[string]string, header *string) (*types.Sandbox, error) {
	existing, err := o.st.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if existing != nil || migrationToken == "" {
		return o.Connect(ctx, id, apiKey, "", timeoutSec)
	}
	if len(migrationToken) > migrationtoken.MaxWireSize {
		return nil, migrationtoken.ErrTokenTooLarge
	}
	if _, err := o.prepareStandaloneTargetWithMMDS(ctx, id, apiKey, migrationToken, metadata, header); err != nil {
		return nil, err
	}
	var requestedDeadline *int64
	if timeoutSec > 0 {
		deadline := time.Now().Add(time.Duration(timeoutSec) * time.Second).Unix()
		requestedDeadline = &deadline
	}
	sb, _, err := o.ensureResumeAccepted(ctx, id, requestedDeadline, func(current *types.Sandbox) error {
		if !ownsSandbox(current, apiKey) {
			return api.ErrNotFound
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return cloneSandbox(sb), nil
}

// prepareStandaloneTarget synchronously resolves, optionally imports, and
// authenticates a standalone API operation target. Callers perform their own
// operation-specific result preparation before scheduling an asynchronous
// resume.
func (o *Orchestrator) prepareStandaloneTarget(ctx context.Context, id, apiKey, migrationToken string) (*types.Sandbox, error) {
	sb, err := o.st.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	// Auto-migrate: connecting to a sandbox absent on this node with a migration
	// token (api_headers X-Kuasar-Migration-Token) imports it (paused row) then
	// schedules an asynchronous resume. ImportSandbox checks the tenant key,
	// token fingerprints, runtime digest, and exact caller-selected target ID.
	// An existing target ignores the token completely, including malformed input.
	if sb == nil && migrationToken != "" {
		imported, ierr := o.ImportSandbox(ctx, apiKey, migrationToken, id)
		if ierr != nil {
			if !errors.Is(ierr, api.ErrAlreadyExists) {
				return nil, ierr
			}
			// Another Connect may have won the insert-only import race. Treat the
			// winner as an existing target, then apply the normal ownership check
			// below. Never overwrite or merge the row that won.
		} else if imported != id {
			return nil, fmt.Errorf("connect: imported sandbox ID mismatch")
		}
		if sb, err = o.st.Get(ctx, id); err != nil {
			return nil, err
		}
	}
	if !ownsSandbox(sb, apiKey) {
		return nil, api.ErrNotFound
	}
	return sb, nil
}

func (o *Orchestrator) resumeDeadline(sb *types.Sandbox, requested *int64) int64 {
	if requested != nil {
		return *requested
	}
	if o.hasDeadlineIntent(sb.ID) {
		return sb.DeadlineUnix
	}
	if o.cfg.Sandbox.TimeoutSec > 0 {
		return time.Now().Add(time.Duration(o.cfg.Sandbox.TimeoutSec) * time.Second).Unix()
	}
	return sb.DeadlineUnix
}

// ensureResumeAccepted is the single durable resume admission used by Connect,
// Wake, native exec, KMT restore, and cluster commands. The caller supplies an
// authorization/identity validator that is re-run under the lifecycle lock.
func (o *Orchestrator) ensureResumeAccepted(
	ctx context.Context,
	sid string,
	requestedDeadline *int64,
	validate func(*types.Sandbox) error,
) (*types.Sandbox, *launchAttempt, error) {
	return o.ensureResumeAcceptedPrepared(ctx, sid, requestedDeadline, validate, nil)
}

// ensureResumeAcceptedPrepared adds a lightweight operation-specific prepare
// hook under the lifecycle fence. It runs after any previous terminal cleanup
// owner has finished, but before a new resume claim or durable mutation. Exec
// session issuance uses it to sample/mint token TTL at the end of synchronous
// admission without allowing a signing failure to start a sandbox.
func (o *Orchestrator) ensureResumeAcceptedPrepared(
	ctx context.Context,
	sid string,
	requestedDeadline *int64,
	validate func(*types.Sandbox) error,
	prepare func(*types.Sandbox) error,
) (*types.Sandbox, *launchAttempt, error) {
	admissionStarted := time.Now()
	unlock := o.lifecycle.Lock(sid)
	locked := true
	defer func() {
		if locked {
			unlock()
		}
	}()

	sb, err := o.st.Get(ctx, sid)
	if err != nil {
		return nil, nil, err
	}
	if sb == nil {
		return nil, nil, api.ErrNotFound
	}
	if validate != nil {
		if err := validate(sb); err != nil {
			return nil, nil, err
		}
	}

	switch sb.State {
	case types.StateRunning:
		if prepare != nil {
			if err := prepare(sb); err != nil {
				return nil, nil, err
			}
		}
		if requestedDeadline != nil {
			if err := o.st.SetDeadline(ctx, sid, *requestedDeadline); err != nil {
				return nil, nil, err
			}
			sb.DeadlineUnix = *requestedDeadline
			o.mutateCached(sid, func(cached *types.Sandbox) { cached.DeadlineUnix = *requestedDeadline })
		}
		return cloneSandbox(sb), nil, nil
	case types.StateStarting:
		if prepare != nil {
			if err := prepare(sb); err != nil {
				return nil, nil, err
			}
		}
		if requestedDeadline != nil {
			if err := o.st.SetDeadline(ctx, sid, *requestedDeadline); err != nil {
				return nil, nil, err
			}
			sb.DeadlineUnix = *requestedDeadline
			o.mutateCached(sid, func(cached *types.Sandbox) { cached.DeadlineUnix = *requestedDeadline })
		}
		attempt, found := o.launches.Lookup(sid)
		if !found {
			if requestedDeadline != nil && sb.SnapshotRef != "" {
				o.markDeadlineIntent(sid)
			}
			o.log.Error("sandbox starting without active launch attempt", "sid", sid, "invariant", "starting_without_launch_owner")
			return cloneSandbox(sb), nil, nil
		}
		if requestedDeadline != nil && attempt.Kind() == launchResume {
			o.markDeadlineIntent(sid)
		}
		return cloneSandbox(sb), attempt, nil
	case types.StatePaused:
		// A failed resume publishes paused before Finish closes done. During that
		// intentional terminal-publication window the old attempt still owns the
		// cleanup fence, so join it and retry admission after the claim is released.
		// Never surface errLaunchClaimed or start a second runner from this state.
		if previous, found := o.launches.Lookup(sid); found {
			unlock()
			locked = false
			if waitErr := previous.wait(ctx); waitErr != nil && ctx.Err() != nil {
				return nil, nil, ctx.Err()
			}
			return o.ensureResumeAcceptedPrepared(ctx, sid, requestedDeadline, validate, prepare)
		}
		// Stored metadata is trusted only after its pure parsers succeed. Do not
		// make starting durable if the worker could never consume its inputs.
		tmpl, err := types.ParseTemplateID(sb.TemplateID)
		if err != nil {
			return nil, nil, err
		}
		if _, err := sandboxcfg.ParseSpec(sb.Metadata); err != nil {
			return nil, nil, err
		}
		lifecycleCtx := o.launchContext()
		if err := lifecycleCtx.Err(); err != nil {
			return nil, nil, fmt.Errorf("orch: lifecycle is stopping: %w", err)
		}
		if prepare != nil {
			if err := prepare(sb); err != nil {
				return nil, nil, err
			}
		}
		attempt, err := o.launches.Claim(lifecycleCtx, sid, launchResume)
		if err != nil {
			return nil, nil, err
		}
		if err := lifecycleCtx.Err(); err != nil {
			o.launches.Finish(attempt, err)
			return nil, nil, fmt.Errorf("orch: lifecycle is stopping: %w", err)
		}
		deadline := o.resumeDeadline(sb, requestedDeadline)
		changed, err := o.st.BeginResume(ctx, sid, deadline)
		if err != nil {
			o.launches.Finish(attempt, err)
			return nil, nil, err
		}
		if !changed {
			o.launches.Finish(attempt, errLaunchOwnershipLost)
			return nil, nil, errLaunchOwnershipLost
		}
		if requestedDeadline != nil {
			o.markDeadlineIntent(sid)
		}
		attempt.SetAcceptedAt(time.Now())
		starting := cloneSandbox(sb)
		starting.State = types.StateStarting
		starting.DeadlineUnix = deadline
		starting.RunID = ""
		starting.FloatingIP = ""
		starting.VswitchPort = ""
		starting.InnerIP = ""
		starting.PortMAC = ""
		o.cache(starting)
		o.publishUpsert(starting)
		work := cloneSandbox(starting)
		o.launches.Start(attempt, func(launchCtx context.Context, current *launchAttempt) error {
			return o.runLaunch(launchCtx, current, work, tmpl)
		})
		o.logLaunchPhase(attempt, starting, "admission_duration", time.Since(admissionStarted))
		return cloneSandbox(starting), attempt, nil
	default:
		return nil, nil, api.ErrNotFound
	}
}

func (o *Orchestrator) SetTimeout(ctx context.Context, id, apiKey string, timeoutSec int) (bool, error) {
	unlock := o.lifecycle.Lock(id)
	defer unlock()

	sb, err := o.st.Get(ctx, id)
	if err != nil {
		return false, err
	}
	if !ownsSandbox(sb, apiKey) {
		return false, nil
	}
	deadline := time.Now().Add(time.Duration(timeoutSec) * time.Second).Unix()
	if err := o.st.SetDeadline(ctx, id, deadline); err != nil {
		return false, err
	}
	sb.DeadlineUnix = deadline
	o.mutateCached(id, func(nb *types.Sandbox) { nb.DeadlineUnix = deadline })
	if sb.State == types.StatePaused {
		o.markDeadlineIntent(id)
	} else if sb.State == types.StateStarting {
		if attempt, found := o.launches.Lookup(id); (found && attempt.Kind() == launchResume) || (!found && sb.SnapshotRef != "") {
			o.markDeadlineIntent(id)
		}
	}
	return true, nil
}

// --- proxy.Router (internal mode) ---

// Route resolves a canonical target for the in-process proxy. A paused sandbox
// first joins the common launch owner; concurrent control/data/exec activations
// therefore collapse to one resume. The forwarding decision is shared with the
// external route table via proxy.RouteForTarget.
func (o *Orchestrator) Route(ctx context.Context, sandboxID string, target proxy.ConnectTarget) (proxy.Route, error) {
	sb := o.lookup(sandboxID)
	if sb == nil {
		s, _ := o.st.Get(ctx, sandboxID)
		if s == nil {
			return proxy.Route{Kind: proxy.KindNotFound}, nil
		}
		sb = s
		o.cache(sb)
	}
	selected := proxy.RouteForTarget(
		string(sb.Profile), sb.EnvdUDS, sb.CiUDS, sb.FloatingIP,
		sb.EnvdAccessToken, sb.ForwardAccessToken, target,
	)
	// A recognized but unsupported logical service has no generic backend to
	// activate. Exec CONNECT is handled earlier by the authenticated
	// LookupExec/ActivateExec path; direct Route callers remain fail-closed.
	if selected.Kind == proxy.KindDeny {
		return selected, nil
	}
	switch sb.State {
	case types.StateRunning:
		// Ready to route below.
	case types.StatePaused:
		if _, _, err := o.ensureResumeAccepted(ctx, sandboxID, nil, nil); err != nil {
			if errors.Is(err, api.ErrNotFound) {
				return proxy.Route{Kind: proxy.KindNotFound}, nil
			}
			return proxy.Route{}, err
		}
		var err error
		sb, err = o.waitLaunchState(ctx, sandboxID)
		if err != nil {
			return proxy.Route{}, err
		}
		if sb == nil || sb.State != types.StateRunning {
			return proxy.Route{Kind: proxy.KindNotFound}, nil
		}
	case types.StateStarting:
		var err error
		sb, err = o.waitLaunchState(ctx, sandboxID)
		if err != nil {
			return proxy.Route{}, err
		}
		if sb == nil || sb.State != types.StateRunning {
			return proxy.Route{Kind: proxy.KindNotFound}, nil
		}
	default:
		return proxy.Route{Kind: proxy.KindNotFound}, nil
	}
	return proxy.RouteForTarget(
		string(sb.Profile), sb.EnvdUDS, sb.CiUDS, sb.FloatingIP,
		sb.EnvdAccessToken, sb.ForwardAccessToken, target,
	), nil
}

// waitLaunchState waits only for the already-accepted owner and then reloads the
// durable record. Raw worker errors never become data-plane responses; the
// authoritative terminal state decides route availability.
func (o *Orchestrator) waitLaunchState(ctx context.Context, sid string) (*types.Sandbox, error) {
	found, waitErr := o.launches.Wait(ctx, sid)
	if waitErr != nil && ctx.Err() != nil {
		return nil, ctx.Err()
	}
	current, err := o.st.Get(ctx, sid)
	if err != nil {
		return nil, err
	}
	if !found && current != nil && current.State == types.StateStarting {
		o.log.Error("sandbox starting without active launch attempt", "sid", sid, "invariant", "starting_without_launch_owner")
	}
	if current == nil || current.State != types.StateRunning {
		return nil, nil
	}
	o.cache(current)
	return cloneSandbox(current), nil
}

// snapInfo is the config the orchestrator inherits from a restore snapshot: the
// frozen capacity (the runtime pins it) and the logical network (the
// kuasar-sandbox.network key the orchestrator injected into the snapshot's metadata),
// used to fill create-config network fields left unset (point 7).
type snapInfo struct {
	Network     sandboxcfg.NetworkSpec
	CapCPU      int
	CapMem      string
	HasCapacity bool
}

// snapshotDescription is the subset of `sandbox-ctl info --json` consumed by
// the orchestrator. info marshals restore.SnapshotCfg by Go field name.
type snapshotDescription struct {
	Resources struct {
		Capacity struct {
			CPU    int    `json:"CPU"`
			Memory string `json:"Memory"`
		} `json:"Capacity"`
	} `json:"Resources"`
	Metadata map[string]string `json:"Metadata"`
}

func (o *Orchestrator) inspectSnapshotConfig(ctx context.Context, manifestKey, ref string) (snapshotDescription, error) {
	var cfg snapshotDescription
	locations := map[string]string{}
	if err := o.addRefLocation(locations, ref); err != nil {
		return cfg, err
	}
	args := []string{"info", "--json"}
	if strings.HasPrefix(ref, "manifest://") {
		args = append(args, "--manifest-config", o.cfg.ManifestConfig)
	}
	args = appendRefLocationArgs(args, locations)
	cmd := exec.CommandContext(ctx, o.cfg.SandboxCtl(), append(args, ref)...)
	cmd.Env = append(os.Environ(), "MANIFEST_KEY="+manifestKey)
	out, err := cmd.Output()
	if err != nil {
		return cfg, fmt.Errorf("snapshot config probe %q: %w", ref, err)
	}
	if err := json.Unmarshal(out, &cfg); err != nil {
		return cfg, fmt.Errorf("snapshot config parse %q: %w", ref, err)
	}
	return cfg, nil
}

// snapshotConfig reads resources.capacity + the kuasar-sandbox.network metadata from a
// snapshot ref's embedded snapshot.cfg (`sandbox-ctl info --json`; reads only the
// trailing ZIP, a few KB even via manifest://). Best-effort: any probe/parse failure
// yields a zero snapInfo and the launch proceeds with node defaults (the runtime
// stays the capacity enforcer).
func (o *Orchestrator) snapshotConfig(ctx context.Context, sb *types.Sandbox, ref string) snapInfo {
	var info snapInfo
	cfg, err := o.inspectSnapshotConfig(ctx, sb.ManifestKey, ref)
	if err != nil {
		o.log.Warn("snapshot config probe failed; using node defaults", "sid", sb.ID, "ref", ref, "err", err)
		return info
	}
	if cfg.Resources.Capacity.CPU > 0 && cfg.Resources.Capacity.Memory != "" {
		info.CapCPU, info.CapMem, info.HasCapacity = cfg.Resources.Capacity.CPU, cfg.Resources.Capacity.Memory, true
	}
	if nraw := strings.TrimSpace(cfg.Metadata[sandboxcfg.NsNetwork]); nraw != "" {
		_ = json.Unmarshal([]byte(nraw), &info.Network) // best-effort; malformed -> zero network
	}
	return info
}

// --- configsock.Provider ---

// sandboxConfigPath is where the per-sandbox SANDBOX_CONFIG yaml is written.
func (o *Orchestrator) sandboxConfigPath(sb *types.Sandbox) string {
	return sb.RunDir + "/" + sb.ID + ".yaml"
}

func (o *Orchestrator) sandboxParams(sb *types.Sandbox, tmpl types.TemplateID, spec sandboxcfg.SandboxSpec, network sandboxcfg.NetworkSpec) sandboxcfg.Params {
	return sandboxcfg.Params{
		Sandbox: sb, Template: tmpl,
		Runtime:        o.cfg.Sandbox.Boot.Runtime,
		Kernel:         o.cfg.Sandbox.Boot.Kernel,
		OverlayDiffTpl: o.cfg.Sandbox.Boot.OverlayDiffTemplate,
		TapFD:          sandboxTapFD(o.vs.TapFD(sb.VswitchPort)), EnvVars: sb.Env,
		VCPU: o.cfg.Sandbox.Resources.VCPU, Memory: o.cfg.Sandbox.Resources.Memory, ControllerSocket: o.cfg.Sandbox.Resources.ControlSocket,
		Network:     network,
		MMDSEnabled: o.cfg.MMDS.Enabled,
		Spec:        spec,
	}
}

func sandboxTapFD(t vswitch.TapFD) sandboxcfg.TapFD {
	return sandboxcfg.TapFD{
		Exec:    append([]string(nil), t.Exec...),
		Socket:  t.Socket,
		Request: t.Request,
		Timeout: t.Timeout,
	}
}

// resolveNetwork fills the existing NetworkSpec with the profile/node defaults
// needed for one VM start. Callers choose the path-specific default hostname and
// merge snapshot inheritance before calling it.
func (o *Orchestrator) resolveNetwork(profile types.Profile, specified sandboxcfg.NetworkSpec, defaultHostname string) (sandboxcfg.NetworkSpec, error) {
	innerCIDR := firstNonEmpty(specified.InnerIP, o.profileNet(profile).InnerIP)
	if _, _, err := net.ParseCIDR(innerCIDR); err != nil {
		return sandboxcfg.NetworkSpec{}, fmt.Errorf("orch: inner_ip %q: %w", innerCIDR, err)
	}
	dns := o.cfg.Sandbox.Network.DNS
	if len(specified.DNS) > 0 {
		dns = specified.DNS
	}
	return sandboxcfg.NetworkSpec{
		Hostname:         firstNonEmpty(specified.Hostname, defaultHostname),
		DNS:              dns,
		InnerIP:          innerCIDR,
		Nexthop:          firstNonEmpty(specified.Nexthop, o.profileNet(profile).Nexthop),
		TransitGatewayIP: specified.TransitGatewayIP,
		TransitGeneveVNI: specified.TransitGeneveVNI,
		TransitMAC:       specified.TransitMAC,
	}, nil
}

// attachNetwork derives the host-side AttachReq from one resolved NetworkSpec.
// Port lifecycle remains with the caller.
func (o *Orchestrator) attachNetwork(ctx context.Context, network sandboxcfg.NetworkSpec) (*vswitch.Port, error) {
	ip, _, err := net.ParseCIDR(network.InnerIP)
	if err != nil {
		return nil, fmt.Errorf("orch: inner_ip %q: %w", network.InnerIP, err)
	}
	return o.vs.Attach(ctx, vswitch.AttachReq{
		InnerIP:          ip.String(),
		TransitGatewayIP: network.TransitGatewayIP,
		TransitGeneveVNI: network.TransitGeneveVNI,
		TransitMAC:       network.TransitMAC,
	})
}

// LaunchSpecFor resolves "sandbox:<sid>" to the LaunchSpec the launcher
// exec-replaces into. The secret manifest key rides in LaunchSpec.Env; the bulky
// non-secret config is the file referenced by the args.
func (o *Orchestrator) LaunchSpecFor(ctx context.Context, configID string) (*configsock.LaunchSpec, string, bool, error) {
	kind, id, found := strings.Cut(configID, ":")
	if !found {
		return nil, "", false, nil
	}
	switch kind {
	case "sandbox":
		return o.sandboxLaunchSpec(ctx, id)
	default: // builds use the buildspec plane (BuildSpecFor), not exec-replace
		return nil, "", false, nil
	}
}

func (o *Orchestrator) sandboxLaunchSpec(ctx context.Context, sid string) (*configsock.LaunchSpec, string, bool, error) {
	sb := o.lookup(sid)
	if sb == nil {
		s, err := o.st.Get(ctx, sid)
		if err != nil || s == nil {
			return nil, "", false, err
		}
		sb = s
	}
	tmpl, err := types.ParseTemplateID(sb.TemplateID)
	if err != nil {
		return nil, "", false, err
	}
	cfgSpec, err := sandboxcfg.ParseSpec(sb.Metadata)
	if err != nil {
		return nil, "", false, err
	}
	// The fully resolved network was already rendered by launch before the unit
	// requests this spec. LaunchSpec only needs Params for restore/connect args.
	p := o.sandboxParams(sb, tmpl, cfgSpec, sandboxcfg.NetworkSpec{})
	// --run-root pins sandbox-ctl's socket/staging dir (ch.sock, ctl.sock, …) to the
	// orchestrator's run root, so RunDir == cfg.Paths.RunRoot/<sid> and the snapshot client
	// (also --run-root cfg.Paths.RunRoot) finds ctl.sock. Without it sandbox-ctl defaults to
	// /run/sandbox, splitting the dirs (snapshot/pause then can't reach ctl.sock).
	args := []string{
		"run", "--sandbox-id", sb.ID,
		"--config", o.sandboxConfigPath(sb),
		"--manifest-config", o.cfg.ManifestConfig,
		"--run-root", o.cfg.Paths.RunRoot,
		"--cgroup-adopt",
		// Route the sandbox's stdio + kernel dmesg to journald from this run-id
		// unit. App stdout/stderr is tagged "sandbox" with KUASAR_SANDBOX_ID; guest
		// dmesg is tagged "console" for host-only diagnostics.
		"--stdout-to", "journald=" + configsock.RunnerLogTag,
		"--stderr-to", "journald=" + configsock.RunnerLogTag,
		"--console", "journald=" + configsock.ConsoleTag,
	}
	locations, err := o.sandboxRefLocations(ctx, sb, tmpl)
	if err != nil {
		return nil, "", false, fmt.Errorf("sandbox %s ref locations: %w", sid, err)
	}
	args = appendRefLocationArgs(args, locations)
	if r := p.RestoreRef(); r != "" {
		args = append(args, "--restore", r)
	}
	for _, c := range p.ConnectSpecs() {
		args = append(args, "--connect", c)
	}
	spec := &configsock.LaunchSpec{
		Exec:    o.cfg.SandboxCtl(),
		Args:    args,
		Workdir: sb.RunDir,
		Env: map[string]string{
			"MANIFEST_KEY":      sb.ManifestKey,
			"KUASAR_RUN_ID":     sb.RunID,
			"KUASAR_SANDBOX_ID": sb.ID,
		},
	}
	return spec, sb.PidFile(), true, nil
}

// --- reaper / reconcile ---

// Reaper enforces TTLs: idle past deadline -> auto-suspend (pause).
func (o *Orchestrator) Reaper(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			// Collect past-deadline sandboxes (read-only scan), then pause them
			// after the scan — pauseSandbox writes the store, which must not run
			// while RangeByState's read cursor is open.
			now := time.Now().Unix()
			var due []*types.Sandbox
			_ = o.st.RangeByState(ctx, types.StateRunning, func(sb *types.Sandbox) error {
				if sb.DeadlineUnix > 0 && now >= sb.DeadlineUnix {
					due = append(due, sb)
				}
				return nil
			})
			for _, sb := range due {
				if err := o.pauseSandbox(ctx, sb); err != nil {
					o.log.Warn("reaper pause", "sid", sb.ID, "err", err)
				}
			}
			if n, err := o.st.PruneExpiredKeyPairs(ctx); err != nil {
				o.log.Warn("reaper prune key pairs", "err", err)
			} else if n > 0 {
				o.log.Info("reaper pruned expired key pairs", "n", n)
			}
		}
	}
}

// Reconcile adopts/cleans sandboxes after an orchestrator restart, using the
// systemd unit set as the liveness authority. A starting row is never adopted:
// launch completion was not committed, so an interrupted resume returns to its
// durable paused snapshot and an interrupted fresh create becomes dead.
func (o *Orchestrator) Reconcile(ctx context.Context) error {
	units, err := o.lc.List(ctx, o.runnerPattern())
	if err != nil {
		return err
	}
	alive := map[string]bool{}
	for _, u := range units {
		if u.ActiveState == "active" || u.ActiveState == "activating" {
			alive[o.unitToRunID(u.Name)] = true
		}
	}
	// Adopt live sandboxes in-memory inline (o.cache is not a store write);
	// collect the dead ones and tear them down after the scan, since teardown +
	// SetState write the store and must not run while the read cursor is open.
	var interrupted, dead []*types.Sandbox
	knownRuns := make(map[string]bool)
	if err := o.st.RangeByState(ctx, types.StateStarting, func(sb *types.Sandbox) error {
		if sb.RunID != "" {
			knownRuns[sb.RunID] = true
		}
		interrupted = append(interrupted, sb)
		return nil
	}); err != nil {
		return err
	}
	if err := o.st.RangeByState(ctx, types.StateRunning, func(sb *types.Sandbox) error {
		if sb.RunID != "" {
			knownRuns[sb.RunID] = true
		}
		if sb.RunID != "" && alive[sb.RunID] {
			o.cache(sb) // re-adopt: route + TTL already in store
		} else {
			dead = append(dead, sb)
		}
		return nil
	}); err != nil {
		return err
	}
	for _, sb := range interrupted {
		target := types.StateDead
		if sb.SnapshotRef != "" {
			target = types.StatePaused
		}
		o.log.Info("reconcile: interrupted sandbox launch", "sid", sb.ID, "target", target)
		if err := o.teardownReconcile(ctx, sb); err != nil {
			return fmt.Errorf("reconcile: cleanup interrupted sandbox %s: %w", sb.ID, err)
		}
		if target == types.StateDead {
			if err := os.RemoveAll(sb.BaseDir); err != nil {
				return fmt.Errorf("reconcile: remove interrupted sandbox base dir %s: %w", sb.ID, err)
			}
		}
		var changed bool
		if target == types.StatePaused {
			changed, err = o.st.RollbackStartingPaused(ctx, sb.ID, sb.RunID)
		} else {
			changed, err = o.st.RollbackStartingDead(ctx, sb.ID, sb.RunID)
		}
		if err != nil {
			return err
		}
		if !changed {
			o.log.Warn("reconcile: interrupted launch state changed", "sid", sb.ID, "run_id", sb.RunID)
		} else if target == types.StatePaused {
			// V1 has no persistent explicit-deadline marker. Conservatively retain
			// the durable deadline of every interrupted resume so a caller-selected
			// Connect/SetTimeout value cannot be replaced by the node default after
			// restart. The intent is consumed by the next exact-run success.
			o.markDeadlineIntent(sb.ID)
		}
	}
	for _, sb := range dead {
		o.log.Info("reconcile: dead sandbox", "sid", sb.ID)
		if err := o.teardownReconcile(ctx, sb); err != nil {
			return fmt.Errorf("reconcile: cleanup dead sandbox %s: %w", sb.ID, err)
		}
		_ = o.st.SetState(ctx, sb.ID, types.StateDead)
	}
	// Idle prestarted units have no sandbox row. They cannot be adopted by a new
	// pool instance because their old WaitAssignment request belonged to the
	// previous config-socket, so stop/reset any such orphan before refilling.
	for _, u := range units {
		runID := o.unitToRunID(u.Name)
		if runID == "" || knownRuns[runID] {
			continue
		}
		o.log.Info("reconcile: orphan runner", "run_id", runID, "unit", u.Name)
		_ = o.lc.Stop(ctx, u.Name)
		_ = o.lc.ResetFailed(ctx, u.Name)
	}
	return nil
}

func (o *Orchestrator) unitActive(ctx context.Context, unit string) bool {
	if unit == "" {
		return false
	}
	units, err := o.lc.List(ctx, unit)
	if err != nil {
		o.log.Debug("unit liveness check failed", "unit", unit, "err", err)
		return true
	}
	for _, u := range units {
		if u.Name == unit && (u.ActiveState == "active" || u.ActiveState == "activating") {
			return true
		}
	}
	return false
}

// --- helpers ---

func cloneSandbox(sb *types.Sandbox) *types.Sandbox {
	if sb == nil {
		return nil
	}
	nb := *sb
	if sb.Cluster != nil {
		cluster := *sb.Cluster
		nb.Cluster = &cluster
	}
	if sb.Metadata != nil {
		nb.Metadata = make(map[string]string, len(sb.Metadata))
		for key, value := range sb.Metadata {
			nb.Metadata[key] = value
		}
	}
	if sb.Env != nil {
		nb.Env = make(map[string]string, len(sb.Env))
		for key, value := range sb.Env {
			nb.Env[key] = value
		}
	}
	return &nb
}

func (o *Orchestrator) cache(sb *types.Sandbox) {
	if sb == nil {
		return
	}
	o.mu.Lock()
	o.reg[sb.ID] = cloneSandbox(sb)
	o.mu.Unlock()
}
func (o *Orchestrator) uncache(id string) {
	o.mu.Lock()
	delete(o.reg, id)
	o.mu.Unlock()
}
func (o *Orchestrator) lookup(id string) *types.Sandbox {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.reg[id]
}

// mutateCached atomically replaces the cached entry for id with a copy that fn
// has mutated (under o.mu) and returns the new snapshot, or nil if id is not
// cached. Published cache entries are immutable: every in-place field change to
// a cached *types.Sandbox goes through here (or through cache() of a freshly
// built, not-yet-published object), so a reader holding a cached pointer never
// observes a torn write.
func (o *Orchestrator) mutateCached(id string, fn func(*types.Sandbox)) *types.Sandbox {
	o.mu.Lock()
	defer o.mu.Unlock()
	cur := o.reg[id]
	if cur == nil {
		return nil
	}
	nb := cloneSandbox(cur)
	fn(nb)
	o.reg[id] = nb
	return nb
}

// ByFloatingIP maps a guest's (SNAT'd) source floating IP to its starting/running
// sandbox id for the in-process MMDS service (proxy_mode=internal). Starting must
// be visible because envd consults MMDS during mandatory /init. A paused sandbox's
// slot/floating IP is freed and may be reused, so paused remains excluded.
func (o *Orchestrator) ByFloatingIP(ip string) (sandboxID string, ok bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, sb := range o.reg {
		if sb.FloatingIP == ip && (sb.State == types.StateStarting || sb.State == types.StateRunning) {
			return sb.ID, true
		}
	}
	return "", false
}

// SandboxInfo returns a starting/running sid's template id + access token
// (mmds.Source, GET stage).
func (o *Orchestrator) SandboxInfo(sid string) (templateID, accessToken string, ok bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if sb, ok := o.reg[sid]; ok && (sb.State == types.StateStarting || sb.State == types.StateRunning) {
		return sb.TemplateID, sb.EnvdAccessToken, true
	}
	return "", "", false
}

// MmdsSecret derives sid's per-sandbox MMDS signing key for the in-process MMDS
// service (proxy_mode=internal); implements mmds.Source. Deterministic from the
// sandbox's manifest key + id (keys.MmdsSecret) — the same key any proxy worker
// would derive. Not gated on running state (a GET verifies a token minted moments
// earlier), but an unknown sandbox / missing manifest key yields ok=false.
func (o *Orchestrator) MmdsSecret(sid string) (secret []byte, ok bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	sb, found := o.reg[sid]
	if !found {
		return nil, false
	}
	s := keys.MmdsSecret(sb.ManifestKey, sid)
	return s, s != nil
}

func (o *Orchestrator) teardown(ctx context.Context, sb *types.Sandbox) error {
	// The sandbox runs in its systemd unit's own cgroup (sandbox-ctl --cgroup-adopt),
	// and the unit is KillMode=control-group, so StopUnit SIGKILLs every straggler
	// (cloud-hypervisor included). No separate cgroup drain/rmdir is needed.
	var cleanupErr error
	if sb.RunID != "" {
		unit := o.runnerUnit(sb.RunID)
		if err := o.lc.Stop(ctx, unit); err != nil {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("stop %s: %w", unit, err))
		}
		if err := o.lc.ResetFailed(ctx, unit); err != nil {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("reset %s: %w", unit, err))
		}
	}
	if sb.VswitchPort != "" {
		if err := o.vs.Detach(ctx, sb.VswitchPort); err != nil {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("detach port %s: %w", sb.VswitchPort, err))
		}
	}
	if err := os.RemoveAll(sb.RunDir); err != nil {
		cleanupErr = errors.Join(cleanupErr, fmt.Errorf("remove run dir %s: %w", sb.RunDir, err))
	}
	o.uncache(sb.ID)
	return cleanupErr
}

// teardownReconcile releases persisted ownership in dependency order. Unlike
// the best-effort API teardown above, Reconcile has no live attempt to retain
// per-step retry progress: on any failure it leaves later resources untouched,
// preserves the durable row, and fails node startup so the next invocation can
// retry safely before an API or data-plane surface opens.
func (o *Orchestrator) teardownReconcile(ctx context.Context, sb *types.Sandbox) error {
	progress := launchCleanupProgress{
		port:   sb.VswitchPort,
		runDir: sb.RunDir,
	}
	if sb.RunID != "" {
		progress.unit = o.runnerUnit(sb.RunID)
	}
	if err := progress.step(ctx, o, true); err != nil {
		return err
	}
	o.uncache(sb.ID)
	return nil
}

// snapshot pauses+captures the running sandbox via sandbox-ctl and returns its
// restore ref. The client dials <run-root>/<sid>/ctl.sock; the running
// snapshot captures sb per the configured checkpoint mode and returns the restore
// ref to persist: a canonical portable ref or a local bundle path
// (local — node-bound; the default). sandbox-ctl performs the work with its own
// boot-time manifest config; we pass the resolved binary + the run root.
func (o *Orchestrator) snapshot(ctx context.Context, sb *types.Sandbox, policy sandboxcfg.CheckpointPolicy) (string, error) {
	// Named-location publishing is deliberately outside the VM pause/capture
	// operation. Pause writes a local bundle; export or template finalization
	// later upgrades it to the configured portable location.
	if o.cfg.Checkpoint.Mode == config.CheckpointLocal {
		return o.snapshotLocal(ctx, sb, policy)
	}
	if o.cfg.Checkpoint.Remote.RefLocationParent != "" {
		return o.snapshotLocal(ctx, sb, sandboxcfg.CheckpointPolicy{})
	}
	key, err := o.snapshotRemote(ctx, sb)
	if err != nil {
		return "", err
	}
	return "manifest://" + key, nil
}

// snapshotRemote uploads the snapshot to the manifest store; stdout is the bare
// 64-hex manifest key. Used for remote checkpoints without a named location.
func (o *Orchestrator) snapshotRemote(ctx context.Context, sb *types.Sandbox) (string, error) {
	cmd := exec.CommandContext(ctx, o.cfg.SandboxCtl(), "snapshot",
		"--sandbox-id", sb.ID, "--upload", "--run-root", o.cfg.Paths.RunRoot)
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("orch: snapshot %s: %w: %s", sb.ID, err, errb.String())
	}
	key := strings.TrimSpace(out.String())
	if _, err := types.ParsePortableRef("manifest://" + key); err != nil {
		return "", fmt.Errorf("orch: snapshot %s: invalid manifest key %q", sb.ID, key)
	}
	return key, nil
}

// snapshotLocal writes the snapshot bundle to checkpoint.local_dir/<sid>/ and
// returns the bundle path (node-bound; restorable only on this node). The lower
// chain (the base template) stays remote, carried by reference.
func (o *Orchestrator) snapshotLocal(ctx context.Context, sb *types.Sandbox, policy sandboxcfg.CheckpointPolicy) (string, error) {
	dir := filepath.Join(o.cfg.Checkpoint.LocalDir, sb.ID)
	args := []string{"snapshot", "--sandbox-id", sb.ID, "--output", dir, "--run-root", o.cfg.Paths.RunRoot}
	args = appendCheckpointPolicyArgs(args, policy)
	cmd := exec.CommandContext(ctx, o.cfg.SandboxCtl(), args...)
	var errb bytes.Buffer
	cmd.Stderr = &errb
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("orch: snapshot %s (local): %w: %s", sb.ID, err, errb.String())
	}
	return filepath.Join(dir, sb.ID+".snapshot"), nil
}

func appendCheckpointPolicyArgs(args []string, policy sandboxcfg.CheckpointPolicy) []string {
	if policy.MergeRef != nil {
		args = append(args, fmt.Sprintf("--merge-ref=%t", *policy.MergeRef))
	}
	if policy.DropCaches != nil {
		args = append(args, fmt.Sprintf("--drop-caches=%t", *policy.DropCaches))
	}
	return args
}

// promote publishes a local checkpoint graph without booting it. The configured
// publisher is either manifest storage or a named ref location.
func (o *Orchestrator) promote(ctx context.Context, sb *types.Sandbox, localPath string) (string, error) {
	args := []string{"upload-snapshot", "--quiet"}
	if o.cfg.Checkpoint.Remote.RefLocationParent != "" {
		uri, err := o.cfg.Checkpoint.RefLocationURI(sb.ID)
		if err != nil {
			return "", fmt.Errorf("orch: promote %s: %w", sb.ID, err)
		}
		args = append(args, "--to-ref-location", sb.ID+"="+uri)
	} else {
		args = append(args, "--manifest-config", o.cfg.ManifestConfig)
	}
	args = append(args, localPath)
	cmd := exec.CommandContext(ctx, o.cfg.SandboxCtl(), args...)
	cmd.Env = append(os.Environ(), "MANIFEST_KEY="+sb.ManifestKey)
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("orch: promote %s: %w: %s", sb.ID, err, errb.String())
	}
	ref := strings.TrimSpace(out.String())
	parsed, err := types.ParsePortableRef(ref)
	if err != nil || (parsed.Scheme == "file" && !strings.HasSuffix(parsed.Path, ".snapshot")) {
		return "", fmt.Errorf("orch: promote %s: invalid snapshot ref %q", sb.ID, ref)
	}
	return ref, nil
}

const envdInitAttemptTimeout = 50 * time.Millisecond

// udsClient builds a bounded HTTP client that dials the envd --connect UDS.
func udsClient(sock string) *http.Client {
	return &http.Client{
		Timeout: envdInitAttemptTimeout,
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, "unix", sock)
			},
		},
	}
}

// envdInitRetryDelay returns the delay after the given consecutive transport
// failure. The first request has no delay; retries ramp quickly and cap at 5ms.
func envdInitRetryDelay(failures int) time.Duration {
	switch {
	case failures <= 0:
		return 0
	case failures == 1:
		return time.Millisecond
	case failures == 2:
		return 2 * time.Millisecond
	case failures == 3:
		return 4 * time.Millisecond
	default:
		return 5 * time.Millisecond
	}
}

// envdInit provisions envd after boot/restore: env vars, default user/workdir, time,
// and — only when MMDS is enabled — the access token. Body keys per envd spec
// (camelCase); success = 204. With MMDS disabled (-isnotfc) we deliberately omit the
// token so envd stays non-secure: pushing one would make envd reject snapshot forks
// (whose restored envd holds the source token and can't be re-keyed under -isnotfc),
// and the proxy already enforces X-Access-Token as the sole gate. With MMDS enabled
// (FC mode) the metadata service authorizes this token's hash, so /init re-keys envd
// to it — giving forks fresh per-identity tokens with envd-side enforcement too.
func (o *Orchestrator) envdInit(ctx context.Context, sb *types.Sandbox) error {
	cl := udsClient(sb.EnvdUDS)
	defer cl.CloseIdleConnections()

	for failures := 0; ; failures++ {
		payload := map[string]any{
			"envVars":        sb.Env,
			"defaultUser":    "user",
			"defaultWorkdir": "/home/user",
			"timestamp":      time.Now().UTC().Format(time.RFC3339),
		}
		if o.cfg.MMDS.Enabled {
			payload["accessToken"] = sb.EnvdAccessToken
		}
		body, err := json.Marshal(payload)
		if err != nil {
			return fmt.Errorf("orch: encode envd /init for %s: %w", sb.ID, err)
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://envd/init", bytes.NewReader(body))
		if err != nil {
			return fmt.Errorf("orch: create envd /init for %s: %w", sb.ID, err)
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := cl.Do(req)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusNoContent {
				return nil
			}
			// Do not include envd's response body: validation failures may echo the
			// request's access token or user-supplied environment values, and this
			// error is logged by create/cluster callers.
			return fmt.Errorf("orch: envd /init for %s status %d", sb.ID, resp.StatusCode)
		}
		if ctx.Err() != nil {
			return fmt.Errorf("orch: envd /init for %s: %w", sb.ID, ctx.Err())
		}

		timer := time.NewTimer(envdInitRetryDelay(failures + 1))
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("orch: envd /init for %s: %w", sb.ID, ctx.Err())
		case <-timer.C:
		}
	}
}

// firstNonEmpty returns a if non-empty, else b (override-over-default helper).
func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

// profileNet returns the configured inner IP / gateway for a profile (e2b vs bare).
func (o *Orchestrator) profileNet(p types.Profile) config.ProfileNet {
	if p == types.ProfileBare {
		return o.cfg.Sandbox.Network.Bare
	}
	return o.cfg.Sandbox.Network.E2B
}
