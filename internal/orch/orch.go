// Package orch is the conductor core. It ties together the store, systemd
// launcher, vswitch and config generation, and implements the control,
// lifecycle, route-authority, and config-socket services.
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
	"github.com/kuasar-sandbox/accelerator/pkg/manifest"

	conductorextension "github.com/kuasar-sandbox/orchestrator/app/conductor/extension"
	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/configresolve"
	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/filestore"
	"github.com/kuasar-sandbox/orchestrator/internal/launcher"
	"github.com/kuasar-sandbox/orchestrator/internal/metrics"
	"github.com/kuasar-sandbox/orchestrator/internal/migrationtoken"
	"github.com/kuasar-sandbox/orchestrator/internal/nodepath"
	"github.com/kuasar-sandbox/orchestrator/internal/reflocation"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/orchestrator/internal/vswitch"
	"github.com/kuasar-sandbox/sandboxer/pkg/artifact"
	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
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
	// executables is frozen process bootstrap state. It never enters the public
	// declarative configuration or any serialized snapshot.
	executables configresolve.Executables

	sandboxReadyTimeout time.Duration

	mu  sync.Mutex
	reg map[string]*types.Sandbox // in-memory immutable snapshots (hot path: Route/LaunchSpecFor)

	launches       launchGroup            // sole process-local owner of create and resume attempts
	acceptedOps    acceptedOperationGroup // accepted pauses/exports drain before shared dependencies close
	buildWake      chan struct{}
	buildUsageWake chan struct{}
	buildOps       acceptedOperationGroup // claimed/recovered Builds drain before store/launcher close
	deleteOps      acceptedOperationGroup // durable Sandbox finalizers drain before shared dependencies close
	exports        exportAttemptGroup     // publish/finalize owners that Resume may preempt or detach
	lifecycle      keyedLockGroup         // serialize lifecycle mutations for one sid

	deleteMu            sync.Mutex
	deleteActive        map[string]struct{} // deleting sandbox id -> live retrying finalizer
	pausedCleanupMu     sync.Mutex
	pausedCleanupActive map[string]struct{} // paused sandbox id -> live retrying ownership cleanup
	runSessionsMu       sync.Mutex
	runSessionsNext     uint64
	runSessions         map[runSessionKey]*runSessionGeneration
	runEndMu            sync.Mutex
	runEndActive        map[string]struct{} // sandbox RunID -> live retrying exact-run end/cleanup worker
	runEndSlots         chan struct{}

	allowLegacyAssignmentWithoutRunSession bool // test-only compatibility for in-process runner fixtures

	lifecycleCtxMu sync.RWMutex
	lifecycleCtx   context.Context // lifecycle admission root; canceled on node shutdown

	deadlineIntentMu sync.Mutex
	deadlineIntents  map[string]struct{} // paused sandboxes whose next resume must preserve an explicit deadline
	recoveryMu       sync.Mutex
	recoveredResumes []*types.Sandbox // cleaned durable starting resumes awaiting run-pool startup

	subsMu sync.Mutex
	subs   map[int]chan routesync.Event // route-change subscribers (routesync clients)
	subSeq int
	// routeBarriers is wired before any API or node-link listener starts. It is
	// consulted for every fresh Create admission.
	routeBarriers routesync.RouteBarrierCoordinator

	routeLogMu sync.Mutex
	routeFP    string
	routeSeq   int64
	routeLog   []routeLogEntry

	pendMu sync.Mutex
	pend   map[string]*pendingBuild // builds whose unit is running (BuildSpecFor source)
	// buildRecoveryReady closes only after every live builder has an adopted
	// pendingBuild owner. The config socket may bind first, but build-spec,
	// phase, and result requests wait here instead of failing in the
	// bind-to-adoption window.
	buildRecoveryReady     chan struct{}
	buildRecoveryReadyOnce sync.Once
	// networkAllocationMu serializes connector allocation with durable Sandbox
	// and Build detach -> ownership-clear/final-delete sequences. A detached but
	// still-persisted port blocks allocation so a retry cannot detach a reused
	// connector slot.
	networkAllocationMu  sync.Mutex
	detachedPortsPending map[string]struct{}

	runnerPool     *runPools
	builderRunPool *runPools
	runs           runIndex

	files *filestore.Store // COPY build-context object store; nil = unconfigured (COPY → 501)
	// commitBuildTrigger is the registered -> waiting linearization point. Keeping
	// it explicit also lets concurrency tests stop both callers immediately
	// before the database CAS without weakening the production store contract.
	commitBuildTrigger func(context.Context, *types.Build) (bool, error)

	probe            ResourceProbe // node water level for cluster heartbeat (set by serve when resource_listen on); nil = none
	resourceStats    SandboxResourceProvider
	trafficStats     SandboxTrafficProvider
	nativeStatsSlots chan struct{}
	// Set once from nodectl.Resolved.SocketIdentity before any API or node-link
	// listener starts. The raw resource_listen socket is never a sandbox policy
	// source.
	resourceControllerSocketIdentity string
	artifactPublisher                func(context.Context, *types.Sandbox, types.ResumeSource) (artifact.PublishReport, error)
	removeSandboxRunDir              func(string) error
	removeSandboxBaseDir             func(string) error
	removeBuildRunDir                func(string) error
	removeBuildBaseDir               func(string) error
	// extensionObserver is nil in the built-in path. The one nil branch at each
	// committed transition avoids hubs, queues, and background work otherwise.
	extensionObserver ExtensionObserver
	// buildEventFences orders each durable Build mutation with both Registry
	// projection and optional Extension observation.
	buildEventFences keyedLockGroup
	// buildRetention orders terminal-row deletion with registration/replay of
	// the same BuildID; buildEventFences separately orders every committed
	// transition with its process-local publications.
	buildRetention       keyedLockGroup
	extensionSandboxHook conductorextension.SandboxHook
	extensionBuildHook   conductorextension.BuildHook

	clusterBuildMu sync.Mutex
	clusterBuilds  map[string]*clusterBuild // build_id -> transient cluster image-pull creds (§7.5)
	buildSubsMu    sync.Mutex
	buildSubs      map[int]chan routesync.BuildEvent // node-link Build projection subscribers
	buildSubSeq    int

	// synthetic build sandbox id -> durable build owner id. Protected by mu
	// alongside reg; never persisted or exported.
	mmdsBuildOwners map[string]string
	buildAdmission  buildAdmissionObservability
	mx              *metrics.M
}

// DrainBuilds prevents new execution work and waits for every claimed or
// recovered Build goroutine to finish cleanup/persistence while dependencies
// remain open.
func (o *Orchestrator) DrainBuilds(ctx context.Context) error {
	return o.buildOps.Drain(ctx)
}

// clusterBuild is a registry-driven build's transient image-pull context. Cluster
// identity remains opaque in Build.Metadata and is never interpreted here.
type clusterBuild struct {
	imageRepo    string
	registryAuth string
}

func New(cfg *config.Config, st *store.Store, lc launcher.Launcher, vs vsClient, log *slog.Logger) *Orchestrator {
	var files *filestore.Store
	if fc := cfg.Builder.FilesStorage; fc != nil {
		fs, err := filestore.New(fc)
		if err != nil {
			log.Warn("builder.files_storage init failed; COPY steps will be rejected", "err", err)
		} else {
			files = fs
		}
	}
	return NewResolved(cfg, st, lc, vs, files, log)
}

// NewResolved constructs the core with startup materials already resolved by
// the conductor App. In particular, an authoritative object-store credential
// provider is validated before this constructor and is never replaced by the
// YAML or ambient AWS fallback inside the core.
func NewResolved(cfg *config.Config, st *store.Store, lc launcher.Launcher, vs vsClient, files *filestore.Store, log *slog.Logger) *Orchestrator {
	o := &Orchestrator{
		cfg: cfg, st: st, lc: lc, vs: vs, log: log,
		sandboxReadyTimeout:  60 * time.Second,
		reg:                  map[string]*types.Sandbox{},
		lifecycleCtx:         context.Background(),
		deadlineIntents:      map[string]struct{}{},
		subs:                 map[int]chan routesync.Event{},
		routeFP:              uuid.NewString(),
		pend:                 map[string]*pendingBuild{},
		buildWake:            make(chan struct{}, 1),
		buildUsageWake:       make(chan struct{}, 1),
		buildRecoveryReady:   make(chan struct{}),
		detachedPortsPending: map[string]struct{}{},
		deleteActive:         map[string]struct{}{},
		pausedCleanupActive:  map[string]struct{}{},
		runEndActive:         map[string]struct{}{},
		runEndSlots:          make(chan struct{}, 4),
		clusterBuilds:        map[string]*clusterBuild{},
		buildSubs:            map[int]chan routesync.BuildEvent{},
		mmdsBuildOwners:      map[string]string{},
		commitBuildTrigger:   st.CommitBuildTrigger,
		removeSandboxRunDir:  os.RemoveAll,
		removeSandboxBaseDir: os.RemoveAll,
		removeBuildRunDir:    os.RemoveAll,
		removeBuildBaseDir:   os.RemoveAll,
		files:                files,
		nativeStatsSlots:     make(chan struct{}, conductorextension.MaxStatsConcurrency),
	}
	wait := cfg.Units.PoolWaitDuration()
	o.runnerPool = newRunPools(runKindSandbox, cfg.Units.RunnerPoolConfigs(), wait, cfg.Paths.RunRoot, lc, &o.runs, log.With("kind", "runner"))
	o.builderRunPool = newRunPools(runKindBuild, cfg.Units.BuilderPoolConfigs(), wait, cfg.Paths.RunRoot, lc, &o.runs, log.With("kind", "builder"))
	return o
}

// SetExecutables freezes the exact node-ctl path and adjacent helper lookup.
// It must be called before unit installation or launch admission.
func (o *Orchestrator) SetExecutables(executables configresolve.Executables) {
	o.executables = executables
}

func (o *Orchestrator) StartRunPools(ctx context.Context) error {
	if err := o.runnerPool.Start(ctx); err != nil {
		return fmt.Errorf("start runner pool: %w", err)
	}
	if err := o.builderRunPool.Start(ctx); err != nil {
		return fmt.Errorf("start builder pool: %w", err)
	}
	return o.startRecoveredResumes(ctx)
}

// SetProxyRouteBarrierCoordinator wires the independent Proxy registration lease
// registry into Create admission. Wiring is immutable after serve starts.
func (o *Orchestrator) SetProxyRouteBarrierCoordinator(c routesync.RouteBarrierCoordinator) {
	o.routeBarriers = c
}

// SetResourceControllerSocketIdentity wires the canonical controller/lease
// identity resolved by nodectl. It must be called before launch admission.
func (o *Orchestrator) SetResourceControllerSocketIdentity(identity string) {
	o.resourceControllerSocketIdentity = identity
}

// --- api.Core ---

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
	preparation, err := o.prepareSandboxLaunch(ctx, sb, tmpl)
	if err != nil {
		return nil, nil, err
	}

	barrierStarted := time.Now()
	if o.routeBarriers == nil {
		err := configsock.ErrProxyRouteUnavailable
		o.logProxyRouteBarrier(sb.ID, err, time.Since(barrierStarted))
		return nil, nil, errors.Join(api.ErrProxyUnavailable, err)
	}
	barrier, err := o.routeBarriers.BeginProxyRouteBarrier()
	if err != nil {
		o.logProxyRouteBarrier(sb.ID, err, time.Since(barrierStarted))
		return nil, nil, errors.Join(api.ErrProxyUnavailable, err)
	}
	defer barrier.Cancel()

	unlock := o.lifecycle.Lock(sb.ID)
	defer unlock()
	attempt, err := o.launches.Claim(lifecycleCtx, sb.ID, launchCreate)
	if err != nil {
		return nil, nil, err
	}
	attempt.SetLaunchMode(sb.LaunchMode)
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
	o.observeSandboxUpsert(initial)
	barrierStarted = time.Now()
	o.publishRouteBarrier(barrier.ID())
	// The barrier is bounded by proxy policy, caller cancellation, and node
	// lifecycle shutdown. Its cleanup below deliberately uses an independent
	// background context after any of these cancellation sources fires.
	barrierCtx, cancelBarrier := context.WithTimeout(attempt.Context(), o.cfg.ParkTimeoutDur())
	stopRequestCancel := context.AfterFunc(ctx, cancelBarrier)
	barrierErr := barrier.Wait(barrierCtx)
	stopRequestCancel()
	cancelBarrier()
	if barrierErr == nil {
		barrierErr = barrier.Commit()
	}
	if barrierErr == nil {
		if err := ctx.Err(); err != nil {
			barrierErr = err
		} else if err := attempt.Context().Err(); err != nil {
			barrierErr = err
		}
	}
	o.logProxyRouteBarrier(sb.ID, barrierErr, time.Since(barrierStarted))
	if barrierErr != nil {
		admissionErr := errors.Join(api.ErrProxyUnavailable, barrierErr)
		rollbackErr := o.rollbackPreLaunchAdmission(sb.ID)
		terminalErr := errors.Join(admissionErr, rollbackErr)
		o.launches.Finish(attempt, terminalErr)
		return nil, nil, terminalErr
	}
	work := cloneSandbox(sb)
	o.launches.Start(attempt, func(launchCtx context.Context, current *launchAttempt) error {
		return o.runLaunch(launchCtx, current, work, tmpl, preparation)
	})
	return cloneSandbox(initial), attempt, nil
}

func (o *Orchestrator) rollbackPreLaunchAdmission(sid string) error {
	return o.rollbackPreLaunchAdmissionWith(sid, cleanupContext)
}

func (o *Orchestrator) rollbackPreLaunchAdmissionWith(sid string, newCleanupContext func() (context.Context, context.CancelFunc)) error {
	delay := launchCleanupRetryMin
	var firstErr error
	for {
		ctx, cancel := newCleanupContext()
		sb, err := o.st.Get(ctx, sid)
		if err == nil && (sb == nil || sb.State != types.StateStarting || !sb.ResumeSource.Empty()) {
			cancel()
			return errors.Join(firstErr, fmt.Errorf("orch: pre-launch rollback lost exact fresh starting ownership for %s", sid))
		}
		if err == nil {
			err = o.teardownPersistedOwnership(ctx, sb, true)
		}
		if err == nil {
			var changed bool
			changed, err = o.st.RollbackStartingDead(ctx, sb)
			if err == nil && !changed {
				cancel()
				return errors.Join(firstErr, fmt.Errorf("orch: pre-launch rollback lost exact starting ownership for %s", sid))
			}
			if err == nil {
				o.runs.forget(sb.RunID)
				o.releaseDetachedPortFence(sb.VswitchPort)
				o.uncache(sid)
				o.publishDelete(sid)
				dead := cloneSandbox(sb)
				dead.State, dead.LaunchMode = types.StateDead, ""
				dead.RunID, dead.FloatingIP, dead.VswitchPort, dead.InnerIP, dead.PortMAC = "", "", "", "", ""
				dead.RunDir, dead.BaseDir, dead.EnvdUDS, dead.CiUDS = "", "", "", ""
				dead.ResumeSource = types.ResumeSource{}
				o.observeSandboxUpsert(dead)
				cancel()
				return firstErr
			}
		}
		cancel()
		if firstErr == nil {
			firstErr = err
		}
		o.log.Error("sandbox pre-launch cleanup/dead commit failed; retrying", "sid", sid, "retry_in", delay, "err", err)
		waitLaunchCleanupRetry(delay)
		delay = nextLaunchCleanupRetry(delay)
	}
}

func (o *Orchestrator) logProxyRouteBarrier(sid string, err error, duration time.Duration) {
	result := "ok"
	switch {
	case errors.Is(err, context.Canceled):
		result = "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		result = "timeout"
	case errors.Is(err, configsock.ErrProxyRouteDisconnected):
		result = "disconnect"
	case errors.Is(err, configsock.ErrProxyRouteLeaseChanged):
		result = "lease_changed"
	case errors.Is(err, configsock.ErrProxyRouteUnavailable):
		result = "no_proxy"
	case err != nil:
		result = "error"
	}
	o.log.Info("sandbox proxy route barrier",
		"sid", sid,
		"result", result,
		"proxy_route_ack_duration", duration,
		"err", err)
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

func (o *Orchestrator) runLaunch(ctx context.Context, attempt *launchAttempt, sb *types.Sandbox, tmpl types.TemplateID, preparation *launchPreparation) error {
	err := o.launchSandbox(ctx, attempt, sb, tmpl, preparation)
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
func (o *Orchestrator) launchSandbox(ctx context.Context, attempt *launchAttempt, sb *types.Sandbox, tmpl types.TemplateID, preparation *launchPreparation) error {
	if sb == nil || attempt == nil || sb.State != types.StateStarting || sb.ID != attempt.SID() {
		return launchFailed("prepare", fmt.Errorf("orch: launch requires its claimed starting sandbox"))
	}
	if preparation == nil {
		return launchFailed("prepare", errors.New("orch: launch resources were not preflighted"))
	}
	if !preparation.Source.Empty() {
		return o.launchArtifactSandbox(ctx, attempt, sb, tmpl, preparation)
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
	spec := preparation.Spec
	network := preparation.Network
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
	p := o.sandboxParams(sb, tmpl, spec, network, preparation.Resources)
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
		o.observeSandboxUpsert(current)
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
	runID, err := o.runnerPool.AssignWithFence(assignmentCtx, sb.ID, func(runID string) bool { return o.runSessionAssignmentActive(runKindSandbox, runID) }, func(runID string) error {
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
		bound := o.mutateCached(sb.ID, func(cached *types.Sandbox) { cached.RunID = runID })
		if bound == nil {
			// BindStartingRunner succeeded under the lifecycle fence, so no
			// concurrent delete can own the row. Rebuild an unexpectedly missing
			// cache entry rather than withholding the assigned incarnation from
			// MMDS and route-sync until after envd initialization.
			bound = cloneSandbox(sb)
			o.cache(bound)
		}
		// Assignment is not visible to the runner until this callback returns.
		// Publish the incarnation-bound starting route first so a Proxy MMDS
		// worker can mint tokens during mandatory envd initialization.
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
		cached.LaunchMode = ""
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
			running.LaunchMode = ""
		}
		if running == nil || running.State != types.StateRunning || running.RunID != sb.RunID {
			o.log.Error("committed sandbox cache invariant failed", "sid", sb.ID, "run_id", sb.RunID)
			running = cloneSandbox(sb)
			running.State = types.StateRunning
			running.LaunchMode = ""
		}
		o.cache(running)
	}
	sb.State = types.StateRunning
	sb.LaunchMode = ""
	sb.DeadlineUnix = running.DeadlineUnix
	o.publishUpsert(running)
	o.observeSandboxUpsert(running)
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
	runID          string
	unit           string
	runnerStopped  bool
	runnerReset    bool
	runnerFenced   bool
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
		p.runID = runID
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
	if p.unit == "" && p.runID != "" && !p.runnerFenced {
		unit, err := o.resolveRunUnit(ctx, runKindSandbox, p.runID)
		if err != nil {
			return err
		}
		p.unit = unit
		p.runnerFenced = unit == ""
	}
	if p.unit != "" && !p.runnerFenced {
		if !p.runnerStopped {
			if err := o.lc.Stop(ctx, p.unit); err != nil {
				active, listErr := o.sandboxUnitActive(ctx, p.unit)
				if listErr != nil {
					return errors.Join(fmt.Errorf("stop %s: %w", p.unit, err), listErr)
				}
				if active {
					return fmt.Errorf("stop %s: %w", p.unit, err)
				}
			}
			p.runnerStopped = true
		}
		if !p.runnerReset {
			if err := o.lc.ResetFailed(ctx, p.unit); err != nil {
				return fmt.Errorf("reset %s: %w", p.unit, err)
			}
			p.runnerReset = true
		}
		active, err := o.sandboxUnitActive(ctx, p.unit)
		if err != nil {
			return err
		}
		if active {
			p.runnerStopped, p.runnerReset = false, false
			return fmt.Errorf("sandbox runner unit %s remained active after stop/reset", p.unit)
		}
		p.runnerFenced = true
	}
	if p.port != "" && !p.portDetached {
		if err := o.detachSandboxPort(ctx, p.port); err != nil {
			return err
		}
		p.portDetached = true
	}
	if includeLocal && p.runDir != "" && !p.runDirRemoved {
		removeRunDir := o.removeSandboxRunDir
		if removeRunDir == nil {
			removeRunDir = os.RemoveAll
		}
		if err := removeRunDir(p.runDir); err != nil {
			return fmt.Errorf("remove run dir %s: %w", p.runDir, err)
		}
		p.runDirRemoved = true
	}
	if includeLocal && p.baseDir != "" && !p.baseDirRemoved {
		removeBaseDir := o.removeSandboxBaseDir
		if removeBaseDir == nil {
			removeBaseDir = os.RemoveAll
		}
		if err := removeBaseDir(p.baseDir); err != nil {
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

// sameStartingLaunchIncarnation identifies the one durable starting row whose
// local resources may be adopted by a failed process-local launch. Network
// ownership is deliberately excluded: an exact resource CAS can miss because
// that same incarnation already persisted a different port, and that durable
// port must be fenced before the terminal transition. Deadline is also omitted
// because SetTimeout may update it concurrently without changing ownership.
func sameStartingLaunchIncarnation(attempt *launchAttempt, expected, current *types.Sandbox, expectedRunID string) bool {
	if attempt == nil || expected == nil || current == nil ||
		current.ID != expected.ID || current.State != types.StateStarting ||
		current.RunID != expectedRunID || current.LaunchMode != expected.LaunchMode ||
		current.CreatedUnix != expected.CreatedUnix || current.Profile != expected.Profile ||
		current.StableIDValue != expected.StableIDValue || current.TemplateID != expected.TemplateID ||
		current.RunDir != expected.RunDir || current.BaseDir != expected.BaseDir ||
		current.EnvdUDS != expected.EnvdUDS || current.CiUDS != expected.CiUDS ||
		current.APISecret != expected.APISecret || current.ManifestKey != expected.ManifestKey ||
		current.ServiceSecret != expected.ServiceSecret || current.EnvdAccessToken != expected.EnvdAccessToken ||
		current.TrafficAccessToken != expected.TrafficAccessToken || current.ForwardAccessToken != expected.ForwardAccessToken ||
		current.AutoPauseMemory != expected.AutoPauseMemory || !sameClusterSandboxOwner(current.Cluster, expected.Cluster) {
		return false
	}
	switch attempt.Kind() {
	case launchCreate:
		return current.ResumeSource.Empty() && expected.ResumeSource.Empty()
	case launchResume:
		return current.ResumeSource.Valid() && current.ResumeSource == expected.ResumeSource
	default:
		return false
	}
}

func sameClusterSandboxOwner(a, b *types.ClusterSandboxContext) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.Group == b.Group && a.RouteKey == b.RouteKey
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
			changed, err = o.st.RollbackStartingPaused(ctx, sb)
		} else {
			changed, err = o.st.RollbackStartingDead(ctx, sb)
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
			current, getErr := o.st.Get(ctx, sb.ID)
			if getErr != nil {
				unlock()
				cancel()
				if firstStoreErr == nil {
					firstStoreErr = getErr
				}
				o.log.Error("reload sandbox after launch rollback CAS miss; retrying",
					"sid", sb.ID, "run_id", expectedRunID, "kind", attempt.Kind(),
					"retry_in", retryDelay, "err", getErr)
				waitLaunchCleanupRetry(retryDelay)
				retryDelay = nextLaunchCleanupRetry(retryDelay)
				continue
			}
			if !sameStartingLaunchIncarnation(attempt, sb, current, expectedRunID) {
				// Delete may win after Attach but before SetStartingResources. The
				// attempt then owns and detaches a process-local port absent from the
				// deleting row. Release only that unpersisted fence here; a port still
				// named by the durable row remains excluded until its finalizer commits.
				if current == nil || current.VswitchPort != sb.VswitchPort {
					o.releaseDetachedPortFence(sb.VswitchPort)
				}
				if current == nil || current.RunID != expectedRunID {
					o.runs.forget(expectedRunID)
				}
				unlock()
				cancel()
				return errors.Join(firstCleanupErr, firstStoreErr)
			}

			// The exact terminal CAS can miss when this same accepted launch has a
			// durable owner that was not present in the worker snapshot (notably a
			// competing exact-run network commit). Never clear that tuple blindly:
			// adopt it, perform the complete ordered teardown, and retry the exact
			// terminal transition with the authoritative snapshot.
			if current.VswitchPort != sb.VswitchPort {
				o.releaseDetachedPortFence(sb.VswitchPort)
			}
			adopted := cloneSandbox(current)
			unlock()
			cancel()
			for {
				cleanupCtx, cleanupCancel := cleanupContext()
				cleanupErr := o.teardownPersistedOwnership(cleanupCtx, adopted, attempt.Kind() == launchCreate)
				cleanupCancel()
				if cleanupErr == nil {
					break
				}
				if firstCleanupErr == nil {
					firstCleanupErr = cleanupErr
				}
				o.log.Error("adopted sandbox launch cleanup incomplete; retrying",
					"sid", adopted.ID, "run_id", expectedRunID, "kind", attempt.Kind(),
					"retry_in", retryDelay, "err", cleanupErr)
				waitLaunchCleanupRetry(retryDelay)
				retryDelay = nextLaunchCleanupRetry(retryDelay)
			}
			sb = adopted
			retryDelay = launchCleanupRetryMin
			continue
		}
		o.runs.forget(expectedRunID)
		o.releaseDetachedPortFence(sb.VswitchPort)
		if attempt.Kind() == launchCreate {
			dead, getErr := o.st.Get(ctx, sb.ID)
			if getErr != nil {
				o.log.Error("reload rolled-back sandbox failed", "sid", sb.ID, "err", getErr)
			}
			o.uncache(sb.ID)
			o.publishDelete(sb.ID)
			if dead == nil {
				dead = cloneSandbox(sb)
				dead.State = types.StateDead
				dead.RunID, dead.FloatingIP, dead.VswitchPort, dead.InnerIP, dead.PortMAC = "", "", "", "", ""
				dead.RunDir, dead.BaseDir, dead.EnvdUDS, dead.CiUDS = "", "", "", ""
				dead.ResumeSource = types.ResumeSource{}
			}
			o.observeSandboxUpsert(dead)
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
				o.observeSandboxUpsert(paused)
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
	if o.extensionSandboxHook != nil {
		return o.killWithExtension(ctx, id, apiKey)
	}
	unlock, err := o.lockLifecycleMutation(ctx, id)
	if err != nil {
		return false, err
	}
	defer unlock()

	sb, err := o.st.Get(ctx, id)
	if err != nil {
		return false, err
	}
	if !ownsSandbox(sb, apiKey) {
		return false, nil
	}
	return o.killSandboxLocked(ctx, sb)
}

func (o *Orchestrator) killWithExtension(ctx context.Context, id, apiKey string) (bool, error) {
	unlock, err := o.lockLifecycleMutation(ctx, id)
	if err != nil {
		return false, err
	}
	sandbox, err := o.st.Get(ctx, id)
	if err != nil {
		unlock()
		return false, err
	}
	if !ownsSandbox(sandbox, apiKey) {
		unlock()
		return false, nil
	}
	if sandbox.State == types.StateDeleting {
		deleting, deleteErr := o.killSandboxLocked(ctx, sandbox)
		unlock()
		return deleting, deleteErr
	}
	precondition := sandboxPrecondition(sandbox)
	operation := newSandboxOperation(conductorextension.SandboxOperationDelete, conductorextension.SandboxOriginDirect, id, sandbox)
	operation.Delete = &conductorextension.SandboxDeleteRequest{Reason: "explicit API delete"}
	operationID := operation.ID
	unlock()

	if err := o.callSandboxHook(ctx, operation); err != nil {
		return false, err
	}
	if err := validateSandboxOperationEnvelope(operation, operationID, conductorextension.SandboxOperationDelete, conductorextension.SandboxOriginDirect, id); err != nil {
		return false, err
	}
	if cloneSandboxDeleteRequest(operation.Delete) == nil {
		return false, fmt.Errorf("%w: extension removed delete candidate", api.ErrBadRequest)
	}

	unlock, err = o.lockLifecycleMutation(ctx, id)
	if err != nil {
		return false, err
	}
	defer unlock()
	current, err := o.st.Get(ctx, id)
	if err != nil {
		return false, err
	}
	if !ownsSandbox(current, apiKey) {
		return false, nil
	}
	if current.State == types.StateDeleting {
		return o.killSandboxLocked(ctx, current)
	}
	if !sandboxPreconditionMatches(precondition, current) {
		return false, api.ErrSandboxChanged
	}
	return o.killSandboxLocked(ctx, current)
}

// killSandboxLocked performs an already-authorized ordinary delete while the
// target's lifecycle lock is held. Mandatory cleanup paths do not call it and
// therefore never depend on the optional Extension Hook.
func (o *Orchestrator) killSandboxLocked(ctx context.Context, sb *types.Sandbox) (bool, error) {
	return o.acceptSandboxDeleteLocked(ctx, sb)
}

func (o *Orchestrator) Pause(ctx context.Context, id, apiKey string, request sandboxcfg.CaptureRequest) error {
	if err := validateCaptureRequest(request); err != nil {
		return err
	}
	if o.extensionSandboxHook != nil {
		return o.pauseWithExtension(ctx, id, apiKey, request)
	}
	unlock, err := o.lockLifecycleMutation(ctx, id)
	if err != nil {
		return err
	}
	defer unlock()

	sb, err := o.st.Get(ctx, id)
	if err != nil {
		return err
	}
	if !ownsSandbox(sb, apiKey) {
		return api.ErrNotFound
	}
	switch sb.State {
	case types.StateStarting:
		return api.ErrSandboxStarting
	case types.StatePaused:
		return api.ErrAlreadyPaused
	case types.StateRunning:
	default:
		return api.ErrNotFound
	}
	if request.Kind == types.CaptureSnapshot {
		request.SnapshotPolicy, err = o.resolveSnapshotPolicy(sb.Metadata, request.SnapshotPolicy)
		if err != nil {
			return err
		}
	}
	return o.pauseSandboxLocked(ctx, sb, request)
}

func (o *Orchestrator) pauseWithExtension(ctx context.Context, id, apiKey string, request sandboxcfg.CaptureRequest) error {
	unlock, err := o.lockLifecycleMutation(ctx, id)
	if err != nil {
		return err
	}
	sandbox, err := o.st.Get(ctx, id)
	if err != nil {
		unlock()
		return err
	}
	if !ownsSandbox(sandbox, apiKey) {
		unlock()
		return api.ErrNotFound
	}
	switch sandbox.State {
	case types.StateStarting:
		unlock()
		return api.ErrSandboxStarting
	case types.StatePaused:
		unlock()
		return api.ErrAlreadyPaused
	case types.StateRunning:
	default:
		unlock()
		return api.ErrNotFound
	}
	if request.Kind == types.CaptureSnapshot {
		if _, err := o.resolveSnapshotPolicy(sandbox.Metadata, request.SnapshotPolicy); err != nil {
			unlock()
			return err
		}
	}
	precondition := sandboxPrecondition(sandbox)
	operation := newSandboxOperation(conductorextension.SandboxOperationPause, conductorextension.SandboxOriginDirect, id, sandbox)
	operation.Pause = &conductorextension.SandboxPauseRequest{
		CaptureKind:        conductorextension.CaptureKind(request.Kind),
		CheckpointMergeRef: cloneBool(request.SnapshotPolicy.MergeRef), CheckpointDropCaches: cloneBool(request.SnapshotPolicy.DropCaches),
	}
	operationID := operation.ID
	unlock()

	if err := o.callSandboxHook(ctx, operation); err != nil {
		return err
	}
	if err := validateSandboxOperationEnvelope(operation, operationID, conductorextension.SandboxOperationPause, conductorextension.SandboxOriginDirect, id); err != nil {
		return err
	}
	candidate := cloneSandboxPauseRequest(operation.Pause)
	if candidate == nil {
		return fmt.Errorf("%w: extension removed pause candidate", api.ErrBadRequest)
	}
	if types.CaptureKind(candidate.CaptureKind) != request.Kind {
		return fmt.Errorf("%w: extension changed core-owned pause capture kind", api.ErrBadRequest)
	}

	unlock, err = o.lockLifecycleMutation(ctx, id)
	if err != nil {
		return err
	}
	defer unlock()
	current, err := o.st.Get(ctx, id)
	if err != nil {
		return err
	}
	if !ownsSandbox(current, apiKey) {
		return api.ErrNotFound
	}
	if !sandboxPreconditionMatches(precondition, current) {
		return api.ErrSandboxChanged
	}
	finalRequest := sandboxcfg.CaptureRequest{
		Kind: types.CaptureKind(candidate.CaptureKind),
		SnapshotPolicy: sandboxcfg.SnapshotPolicy{
			MergeRef: cloneBool(candidate.CheckpointMergeRef), DropCaches: cloneBool(candidate.CheckpointDropCaches),
		},
	}
	if err := validateCaptureRequest(finalRequest); err != nil {
		return err
	}
	if finalRequest.Kind == types.CaptureSnapshot {
		finalRequest.SnapshotPolicy, err = o.resolveSnapshotPolicy(current.Metadata, finalRequest.SnapshotPolicy)
		if err != nil {
			return err
		}
	}
	return o.pauseSandboxLocked(ctx, current, finalRequest)
}

// pauseSandbox captures a running sandbox and stops it — the work behind the
// reaper's auto-suspend (no API key: the caller has already authorized).
func (o *Orchestrator) pauseSandbox(ctx context.Context, sb *types.Sandbox) error {
	unlock, err := o.lockLifecycleMutation(ctx, sb.ID)
	if err != nil {
		return err
	}
	defer unlock()

	current, err := o.st.Get(ctx, sb.ID)
	if err != nil {
		return err
	}
	if current == nil {
		return api.ErrNotFound
	}
	switch current.State {
	case types.StatePaused:
		return api.ErrAlreadyPaused
	case types.StateStarting:
		return api.ErrSandboxStarting
	case types.StateRunning:
	default:
		return api.ErrNotFound
	}
	request := sandboxcfg.CaptureRequest{Kind: types.CaptureSandbox}
	if current.AutoPauseMemory {
		request.Kind = types.CaptureSnapshot
		request.SnapshotPolicy, err = o.resolveSnapshotPolicy(current.Metadata, sandboxcfg.SnapshotPolicy{})
		if err != nil {
			return err
		}
	}
	return o.pauseSandboxLocked(ctx, current, request)
}

func validateCaptureRequest(request sandboxcfg.CaptureRequest) error {
	if !request.Kind.Valid() {
		return fmt.Errorf("%w: invalid capture kind %q", api.ErrBadRequest, request.Kind)
	}
	if request.Kind == types.CaptureSandbox && !request.SnapshotPolicy.Empty() {
		return fmt.Errorf("%w: snapshot-only checkpoint options require snapshot capture", api.ErrBadRequest)
	}
	return nil
}

func (o *Orchestrator) pauseSandboxLocked(ctx context.Context, sb *types.Sandbox, request sandboxcfg.CaptureRequest) error {
	if sb.State == types.StatePaused {
		return api.ErrAlreadyPaused
	}
	if sb.State == types.StateStarting {
		return api.ErrSandboxStarting
	}
	if sb.State != types.StateRunning {
		return api.ErrNotFound
	}
	opCtx, finish, err := o.beginPauseOperation(ctx)
	if err != nil {
		return err
	}
	defer finish()
	o.log.Info("pause capture resolved",
		"sid", sb.ID,
		"capture_kind", request.Kind,
		"mode", o.cfg.Checkpoint.Mode,
		"merge_ref", checkpointPolicyValue(request.SnapshotPolicy.MergeRef),
		"drop_caches", checkpointPolicyValue(request.SnapshotPolicy.DropCaches))
	result, err := o.capture(opCtx, sb, request)
	if err != nil {
		return err
	}
	changed, err := o.st.CommitRunningPaused(opCtx, sb.ID, sb.RunID, result.Source)
	if err != nil {
		return fmt.Errorf("orch: commit pause %s: %w", sb.ID, err)
	}
	if !changed {
		return fmt.Errorf("orch: commit pause %s: running sandbox is no longer owned by runner %s", sb.ID, sb.RunID)
	}
	paused := cloneSandbox(sb)
	paused.ResumeSource = result.Source
	paused.LaunchMode = ""
	paused.State = types.StatePaused

	cleanupCtx, cancel := cleanupContext()
	defer cancel()
	if err := o.cleanupPausedOwnership(cleanupCtx, paused); err != nil {
		// Capture and the paused transition are already committed. Retain every
		// uncleared exact ownership field for admission/restart reconciliation;
		// returning a cleanup error here would misleadingly invite a second Pause.
		o.log.Warn("pause: ownership cleanup incomplete", "sid", paused.ID, "err", err)
		o.startPausedCleanupRetry(paused.ID)
	}
	o.cache(paused)
	o.publishUpsert(paused) // proxies keep the (now paused) route so traffic triggers a Wake
	o.observeSandboxUpsert(paused)
	return nil
}

// cleanupPausedOwnership releases ownership in the canonical dependency order.
// Each exact RunID, port, and RunDir remains durable until its cleanup succeeds;
// clearing RunDir also clears the UDS paths it owns. BaseDir remains the paused
// artifact owner; only obsolete checkpoint entries are selectively retired here.
func (o *Orchestrator) cleanupPausedOwnership(ctx context.Context, sb *types.Sandbox) error {
	if sb == nil || sb.State != types.StatePaused {
		return errors.New("orch: paused ownership cleanup requires a paused sandbox")
	}
	if err := o.validateSandboxCleanupPaths(sb); err != nil {
		return err
	}
	if runID := sb.RunID; runID != "" {
		if err := o.fenceSandboxRunner(ctx, runID); err != nil {
			return err
		}
		changed, err := o.st.ClearPausedRunner(ctx, sb.ID, runID)
		if err != nil {
			return err
		}
		if !changed {
			return fmt.Errorf("orch: paused runner ownership changed for %s", sb.ID)
		}
		o.runs.forget(runID)
		sb.RunID = ""
	}
	if port := sb.VswitchPort; port != "" {
		o.networkAllocationMu.Lock()
		detachErr := o.vs.Detach(ctx, port)
		if detachErr != nil && !errors.Is(detachErr, vswitch.ErrPortNotAttached) {
			o.networkAllocationMu.Unlock()
			return fmt.Errorf("orch: detach paused port %s: %w", port, detachErr)
		}
		if o.detachedPortsPending == nil {
			o.detachedPortsPending = make(map[string]struct{})
		}
		o.detachedPortsPending[port] = struct{}{}
		changed, err := o.st.ClearPausedNetwork(ctx, sb.ID, port)
		if err == nil && changed {
			delete(o.detachedPortsPending, port)
		}
		o.networkAllocationMu.Unlock()
		if err != nil {
			return err
		}
		if !changed {
			return fmt.Errorf("orch: paused network ownership changed for %s", sb.ID)
		}
		sb.VswitchPort, sb.FloatingIP, sb.PortMAC, sb.InnerIP = "", "", "", ""
	}
	removeRunDir := o.removeSandboxRunDir
	if removeRunDir == nil {
		removeRunDir = os.RemoveAll
	}
	if runDir := sb.RunDir; runDir != "" {
		if err := o.cleanupCheckpoint(ctx, sb); err != nil {
			return err
		}
		if err := removeRunDir(runDir); err != nil {
			return fmt.Errorf("remove paused sandbox RunDir %s: %w", runDir, err)
		}
		changed, err := o.st.ClearPausedRunDir(ctx, sb.ID, runDir, sb.EnvdUDS, sb.CiUDS)
		if err != nil {
			return err
		}
		if !changed {
			return fmt.Errorf("orch: paused RunDir ownership changed for %s", sb.ID)
		}
		sb.RunDir, sb.EnvdUDS, sb.CiUDS = "", "", ""
	}
	return nil
}

func (o *Orchestrator) beginPauseOperation(requestCtx context.Context) (context.Context, func(), error) {
	if requestCtx == nil {
		requestCtx = context.Background()
	}
	if err := requestCtx.Err(); err != nil {
		return nil, nil, err
	}
	finish, err := o.acceptedOps.Begin(o.launchContext())
	if err != nil {
		return nil, nil, fmt.Errorf("orch: lifecycle is stopping: %w", err)
	}
	// A capture request is not canceled by closing its ctl.sock client. Once
	// accepted, killing that client only loses the completion response while the
	// runtime continues to commit the E/S artifact and destroy the VM.
	return context.WithoutCancel(requestCtx), finish, nil
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

func (o *Orchestrator) nodeSnapshotPolicy() sandboxcfg.SnapshotPolicy {
	return sandboxcfg.CloneSnapshotPolicy(sandboxcfg.SnapshotPolicy{
		MergeRef:   o.cfg.Checkpoint.MergeRef,
		DropCaches: o.cfg.Checkpoint.DropCaches,
	})
}

func checkpointMetadataPolicy(metadata map[string]string) (sandboxcfg.SnapshotPolicy, error) {
	raw, ok := metadata[sandboxcfg.NsCheckpoint]
	if !ok {
		return sandboxcfg.SnapshotPolicy{}, nil
	}
	policy, err := sandboxcfg.ParseSnapshotPolicyJSON(raw)
	if err != nil {
		return sandboxcfg.SnapshotPolicy{}, fmt.Errorf("metadata[%q]: %w", sandboxcfg.NsCheckpoint, err)
	}
	return policy, nil
}

func (o *Orchestrator) validateCreateCheckpointMode(metadata map[string]string) error {
	_, err := checkpointMetadataPolicy(metadata)
	if err != nil {
		return fmt.Errorf("%w: %v", api.ErrBadRequest, err)
	}
	return nil
}

// resolveSnapshotPolicy validates historical metadata under the lifecycle
// lock and resolves capture policy field by field: node < metadata < action.
func (o *Orchestrator) resolveSnapshotPolicy(metadata map[string]string, actionOverride sandboxcfg.SnapshotPolicy) (sandboxcfg.SnapshotPolicy, error) {
	metadataPolicy, err := checkpointMetadataPolicy(metadata)
	if err != nil {
		return sandboxcfg.SnapshotPolicy{}, fmt.Errorf("%w: %v", api.ErrBadRequest, err)
	}
	switch o.cfg.Checkpoint.Mode {
	case config.CheckpointLocal, config.CheckpointBundle:
		policy := sandboxcfg.OverlaySnapshotPolicy(o.nodeSnapshotPolicy(), metadataPolicy)
		return sandboxcfg.OverlaySnapshotPolicy(policy, actionOverride), nil
	default:
		return sandboxcfg.SnapshotPolicy{}, fmt.Errorf("%w: unsupported checkpoint.mode %q", api.ErrBadRequest, o.cfg.Checkpoint.Mode)
	}
}

func (o *Orchestrator) Connect(ctx context.Context, id, apiKey, migrationToken string, options api.ConnectOptions) (*types.Sandbox, error) {
	_, err := o.prepareStandaloneTarget(ctx, id, apiKey, migrationToken)
	if err != nil {
		return nil, err
	}
	var requestedDeadline *int64
	if options.TimeoutSec > 0 {
		deadline := time.Now().Add(time.Duration(options.TimeoutSec) * time.Second).Unix()
		requestedDeadline = &deadline
	}
	request := types.ResumeRequest{Trigger: types.ResumeTriggerConnect, Mode: types.ResumeModeForMemory(options.Memory)}
	sb, _, err := o.ensureResumeAccepted(ctx, id, requestedDeadline, request, func(current *types.Sandbox) error {
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
func (o *Orchestrator) ConnectWithMMDS(ctx context.Context, id, apiKey, migrationToken string, options api.ConnectOptions, metadata map[string]string, header *string) (*types.Sandbox, error) {
	existing, err := o.st.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if existing != nil || migrationToken == "" {
		return o.Connect(ctx, id, apiKey, "", options)
	}
	if len(migrationToken) > migrationtoken.MaxWireSize {
		return nil, migrationtoken.ErrTokenTooLarge
	}
	if _, err := o.prepareStandaloneTargetWithMMDS(ctx, id, apiKey, migrationToken, metadata, header); err != nil {
		return nil, err
	}
	var requestedDeadline *int64
	if options.TimeoutSec > 0 {
		deadline := time.Now().Add(time.Duration(options.TimeoutSec) * time.Second).Unix()
		requestedDeadline = &deadline
	}
	request := types.ResumeRequest{Trigger: types.ResumeTriggerConnect, Mode: types.ResumeModeForMemory(options.Memory)}
	sb, _, err := o.ensureResumeAccepted(ctx, id, requestedDeadline, request, func(current *types.Sandbox) error {
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
	request types.ResumeRequest,
	validate func(*types.Sandbox) error,
) (*types.Sandbox, *launchAttempt, error) {
	return o.ensureResumeAcceptedFrom(ctx, sid, requestedDeadline, request, conductorextension.SandboxOriginDirect, validate)
}

func (o *Orchestrator) ensureResumeAcceptedFrom(
	ctx context.Context,
	sid string,
	requestedDeadline *int64,
	request types.ResumeRequest,
	origin conductorextension.SandboxOperationOrigin,
	validate func(*types.Sandbox) error,
) (*types.Sandbox, *launchAttempt, error) {
	return o.ensureResumeAcceptedPreparedFrom(ctx, sid, requestedDeadline, request, origin, validate, nil)
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
	request types.ResumeRequest,
	validate func(*types.Sandbox) error,
	prepare func(*types.Sandbox) error,
) (*types.Sandbox, *launchAttempt, error) {
	return o.ensureResumeAcceptedPreparedFrom(ctx, sid, requestedDeadline, request, conductorextension.SandboxOriginDirect, validate, prepare)
}

func (o *Orchestrator) ensureResumeAcceptedPreparedFrom(
	ctx context.Context,
	sid string,
	requestedDeadline *int64,
	request types.ResumeRequest,
	origin conductorextension.SandboxOperationOrigin,
	validate func(*types.Sandbox) error,
	prepare func(*types.Sandbox) error,
) (*types.Sandbox, *launchAttempt, error) {
	if !request.Trigger.Valid() || !request.Mode.Valid() {
		return nil, nil, fmt.Errorf("%w: invalid resume request trigger=%q mode=%q", api.ErrBadRequest, request.Trigger, request.Mode)
	}
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
			updated := o.mutateCached(sid, func(cached *types.Sandbox) { cached.DeadlineUnix = *requestedDeadline })
			if updated == nil {
				updated = sb
			}
			o.observeSandboxUpsert(updated)
		}
		return cloneSandbox(sb), nil, nil
	case types.StateStarting:
		if sb.ResumeSource.Valid() && request.Mode != types.ResumeAuto {
			requestedLaunch := types.LaunchCold
			if request.Mode == types.ResumeMemory {
				requestedLaunch = types.LaunchMemory
			}
			if sb.LaunchMode != requestedLaunch {
				return nil, nil, fmt.Errorf("%w: starting launch mode is %s", types.ErrLaunchModeConflict, sb.LaunchMode)
			}
		}
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
			updated := o.mutateCached(sid, func(cached *types.Sandbox) { cached.DeadlineUnix = *requestedDeadline })
			if updated == nil {
				updated = sb
			}
			o.observeSandboxUpsert(updated)
		}
		attempt, found := o.launches.Lookup(sid)
		if !found {
			if requestedDeadline != nil && sb.ResumeSource.Valid() {
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
			return o.ensureResumeAcceptedPreparedFrom(ctx, sid, requestedDeadline, request, origin, validate, prepare)
		}
		// A crash or partial cleanup after CommitRunningPaused may leave exact
		// runner/network/RunDir ownership on the durable paused row. Reconcile it
		// before BeginResume restores paths and acquires a new runtime owner.
		beforeRunID, beforePort, beforeRunDir := sb.RunID, sb.VswitchPort, sb.RunDir
		cleanupCtx, cancel := cleanupContext()
		cleanupErr := o.cleanupPausedOwnership(cleanupCtx, sb)
		cancel()
		o.recordPausedCleanupProgress(sb, beforeRunID, beforePort, beforeRunDir)
		if cleanupErr != nil {
			o.startPausedCleanupRetry(sb.ID)
			return nil, nil, cleanupErr
		}
		if o.extensionSandboxHook != nil {
			precondition := sandboxPrecondition(sb)
			operation := newSandboxOperation(conductorextension.SandboxOperationResume, origin, sid, sb)
			operation.Resume = &conductorextension.SandboxResumeRequest{
				RequestedDeadlineUnix: cloneInt64(requestedDeadline),
				Mode:                  conductorextension.ResumeMode(request.Mode), Trigger: conductorextension.ResumeTrigger(request.Trigger),
			}
			operationID := operation.ID
			unlock()
			locked = false
			if err := o.callSandboxHook(ctx, operation); err != nil {
				return nil, nil, err
			}
			if err := validateSandboxOperationEnvelope(operation, operationID, conductorextension.SandboxOperationResume, origin, sid); err != nil {
				return nil, nil, err
			}
			candidate := cloneSandboxResumeRequest(operation.Resume)
			if candidate == nil {
				return nil, nil, fmt.Errorf("%w: extension removed resume candidate", api.ErrBadRequest)
			}
			if candidate.RequestedDeadlineUnix != nil && *candidate.RequestedDeadlineUnix < 0 {
				return nil, nil, fmt.Errorf("%w: resume deadline must be non-negative", api.ErrBadRequest)
			}
			if types.ResumeTrigger(candidate.Trigger) != request.Trigger || types.ResumeMode(candidate.Mode) != request.Mode {
				return nil, nil, fmt.Errorf("%w: extension changed core-owned resume trigger or mode", api.ErrBadRequest)
			}
			if err := ctx.Err(); err != nil {
				return nil, nil, err
			}
			unlock = o.lifecycle.Lock(sid)
			locked = true
			current, err := o.st.Get(ctx, sid)
			if err != nil {
				return nil, nil, err
			}
			if current == nil {
				return nil, nil, api.ErrNotFound
			}
			if validate != nil {
				if err := validate(current); err != nil {
					return nil, nil, err
				}
			}
			if current.State == types.StateRunning || current.State == types.StateStarting {
				// A competing admission won while the Hook ran. Never apply its
				// stale result to that incarnation; re-enter with the original
				// request so the established idempotent path can join it.
				unlock()
				locked = false
				return o.ensureResumeAcceptedPreparedFrom(ctx, sid, requestedDeadline, request, origin, validate, prepare)
			}
			if !sandboxPreconditionMatches(precondition, current) {
				return nil, nil, api.ErrSandboxChanged
			}
			sb = current
			requestedDeadline = cloneInt64(candidate.RequestedDeadlineUnix)
		}
		launchMode, err := types.ResolveLaunchMode(sb.ResumeSource, request.Mode)
		if err != nil {
			return nil, nil, err
		}
		sb.LaunchMode = launchMode
		// Stored metadata is trusted only after its pure parsers succeed. Do not
		// make starting durable if the worker could never consume its inputs.
		tmpl, err := types.ParseTemplateID(sb.TemplateID)
		if err != nil {
			return nil, nil, err
		}
		launchPreparation, err := o.prepareSandboxLaunch(ctx, sb, tmpl)
		if err != nil {
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
		deadline := o.resumeDeadline(sb, requestedDeadline)
		runDir := nodepath.SandboxRunDir(o.cfg.Paths.RunRoot, sid)
		envdUDS, ciUDS := "", ""
		if sb.Profile == types.ProfileE2B {
			envdUDS = filepath.Join(runDir, "envd.sock")
			ciUDS = filepath.Join(runDir, "ci.sock")
		}
		changed, err := o.st.BeginResume(ctx, sid, deadline, launchMode, runDir, envdUDS, ciUDS)
		if err != nil {
			return nil, nil, err
		}
		if !changed {
			return nil, nil, errLaunchOwnershipLost
		}
		starting := cloneSandbox(sb)
		starting.State = types.StateStarting
		starting.LaunchMode = launchMode
		starting.DeadlineUnix = deadline
		starting.RunDir = runDir
		starting.EnvdUDS = envdUDS
		starting.CiUDS = ciUDS
		starting.RunID = ""
		starting.FloatingIP = ""
		starting.VswitchPort = ""
		starting.InnerIP = ""
		starting.PortMAC = ""
		// The durable row is authoritative: create the process-local attempt only
		// after BeginResume has atomically persisted starting + launch_mode. Claim
		// cannot normally fail while the per-SID lifecycle lock is held; if the
		// lifecycle root closes in this narrow window, return the exact empty-owner
		// starting row to its original paused source.
		attempt, err := o.launches.Claim(lifecycleCtx, sid, launchResume)
		if err != nil {
			cleanupCtx, cancel := cleanupContext()
			rolledBack, rollbackErr := o.st.RollbackStartingPaused(cleanupCtx, starting)
			cancel()
			if rollbackErr != nil {
				return nil, nil, errors.Join(err, fmt.Errorf("orch: rollback unowned accepted resume %s: %w", sid, rollbackErr))
			}
			if !rolledBack {
				return nil, nil, errors.Join(err, fmt.Errorf("orch: rollback unowned accepted resume %s: %w", sid, errLaunchOwnershipLost))
			}
			return nil, nil, err
		}
		if exportState, found := o.exports.ResumeAccepted(sid, api.ErrExportPreempted); found {
			outcome := "preempted"
			kind := "kmt"
			if exportState == exportDetached {
				outcome = "detached"
				kind = "template"
			}
			o.log.Info("resume won export finalization", "sid", sid, "export_kind", kind, "outcome", outcome)
		}
		if requestedDeadline != nil {
			o.markDeadlineIntent(sid)
		}
		attempt.SetAcceptedAt(time.Now())
		attempt.SetLaunchMode(launchMode)
		o.cache(starting)
		o.publishUpsert(starting)
		o.observeSandboxUpsert(starting)
		work := cloneSandbox(starting)
		o.launches.Start(attempt, func(launchCtx context.Context, current *launchAttempt) error {
			return o.runLaunch(launchCtx, current, work, tmpl, launchPreparation)
		})
		o.logLaunchPhase(attempt, starting, "admission_duration", time.Since(admissionStarted))
		return cloneSandbox(starting), attempt, nil
	default:
		return nil, nil, api.ErrNotFound
	}
}

func (o *Orchestrator) SetTimeout(ctx context.Context, id, apiKey string, timeoutSec int) (bool, error) {
	unlock, err := o.lockLifecycleMutation(ctx, id)
	if err != nil {
		return false, err
	}
	defer unlock()

	sb, err := o.st.Get(ctx, id)
	if err != nil {
		return false, err
	}
	if !ownsSandbox(sb, apiKey) {
		return false, nil
	}
	switch sb.State {
	case types.StateStarting, types.StateRunning, types.StatePaused:
	default:
		return false, nil
	}
	deadline := time.Now().Add(time.Duration(timeoutSec) * time.Second).Unix()
	changed, err := o.st.SetDeadlineIfState(ctx, id, sb.State, deadline)
	if err != nil {
		return false, err
	}
	if !changed {
		return false, nil
	}
	sb.DeadlineUnix = deadline
	updated := o.mutateCached(id, func(nb *types.Sandbox) { nb.DeadlineUnix = deadline })
	if updated == nil {
		updated = sb
	}
	o.observeSandboxUpsert(updated)
	if sb.State == types.StatePaused {
		o.markDeadlineIntent(id)
	} else if sb.State == types.StateStarting {
		if attempt, found := o.launches.Lookup(id); (found && attempt.Kind() == launchResume) || (!found && sb.ResumeSource.Valid()) {
			o.markDeadlineIntent(id)
		}
	}
	return true, nil
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

// launchPreparation contains every pure, fallible input needed after launch
// acceptance. It is built before route publication, network attach, resource
// controller admission, cgroup creation, or runner assignment.
type launchPreparation struct {
	Spec      sandboxcfg.SandboxSpec
	Source    types.ResumeSource
	Network   sandboxcfg.NetworkSpec
	Resources rtconfig.ResourcesConfig
}

func (o *Orchestrator) prepareSandboxLaunch(ctx context.Context, sb *types.Sandbox, tmpl types.TemplateID) (*launchPreparation, error) {
	if sb == nil {
		return nil, errors.New("orch: sandbox launch preflight requires a sandbox")
	}
	spec, err := sandboxcfg.ParseSpec(sb.Metadata)
	if err != nil {
		return nil, err
	}
	source := sandboxcfg.SourceForLaunch(sb, tmpl)
	if !source.Empty() {
		// Artifact content is intentionally unavailable during synchronous
		// admission. Request-owned fields and immutable node defaults are still
		// checked here; inherited fields and capacity arrive asynchronously from
		// the exact runner's task-local E/S reader.
		dynamic := o.cfg.ResourceListen != nil && o.cfg.ResourceListen.Enabled
		if spec.Resource.Startup != nil && !dynamic {
			return nil, fmt.Errorf("%w: %s.startup requires dynamic resource control", api.ErrBadRequest, sandboxcfg.NsResource)
		}
		if _, err := o.resolveNetwork(tmpl.Profile, spec.Network, o.cfg.Sandbox.Network.Hostname); err != nil {
			return nil, err
		}
		return &launchPreparation{Spec: spec, Source: source}, nil
	}
	dynamic := o.cfg.ResourceListen != nil && o.cfg.ResourceListen.Enabled
	resources, err := sandboxcfg.ResolveResources(sandboxcfg.ResourceResolveInput{
		Node:                     configresolve.SandboxResources(o.cfg.Sandbox.Resources),
		Patch:                    spec.Resource,
		Restore:                  false,
		Dynamic:                  dynamic,
		ControllerSocketIdentity: o.resourceControllerSocketIdentity,
	})
	if err != nil {
		if errors.Is(err, sandboxcfg.ErrInvalidResourceRequest) {
			return nil, fmt.Errorf("%w: %v", api.ErrBadRequest, err)
		}
		return nil, fmt.Errorf("orch: resolve sandbox resources: %w", err)
	}
	network, err := o.resolveNetwork(tmpl.Profile, spec.Network, o.cfg.Sandbox.Network.Hostname)
	if err != nil {
		return nil, err
	}
	return &launchPreparation{Spec: spec, Network: network, Resources: resources}, nil
}

// --- configsock.Provider ---

// sandboxConfigPath is where the per-sandbox SANDBOX_CONFIG yaml is written.
func (o *Orchestrator) sandboxConfigPath(sb *types.Sandbox) string {
	return sb.RunDir + "/" + sb.ID + ".yaml"
}

func (o *Orchestrator) sandboxParams(sb *types.Sandbox, tmpl types.TemplateID, spec sandboxcfg.SandboxSpec, network sandboxcfg.NetworkSpec, resources rtconfig.ResourcesConfig) sandboxcfg.Params {
	return sandboxcfg.Params{
		Sandbox: sb, Template: tmpl,
		Runtime:        o.cfg.Sandbox.Boot.Runtime,
		Kernel:         o.cfg.Sandbox.Boot.Kernel,
		OverlayDiffTpl: o.cfg.Sandbox.Boot.OverlayDiffTemplate,
		TapFD:          sandboxTapFD(o.vs.TapFD(sb.VswitchPort)), EnvVars: sb.Env,
		Resources:   resources,
		Usage:       o.cfg.Sandbox.Usage.Runtime(),
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
	o.networkAllocationMu.Lock()
	defer o.networkAllocationMu.Unlock()
	if len(o.detachedPortsPending) != 0 {
		return nil, fmt.Errorf("orch: network allocation blocked while detached ownership awaits durable cleanup")
	}
	return o.vs.Attach(ctx, vswitch.AttachReq{
		InnerIP:          ip.String(),
		TransitGatewayIP: network.TransitGatewayIP,
		TransitGeneveVNI: network.TransitGeneveVNI,
		TransitMAC:       network.TransitMAC,
	})
}

// LaunchSpecFor is retained as a pure compatibility/test helper for sandbox
// launch-spec construction. Managed runners use the exact-run task plane, and
// builders use their own exact-run bootstrap. This method never opens an
// artifact.
func (o *Orchestrator) LaunchSpecFor(ctx context.Context, configID string) (*configsock.LaunchSpec, string, bool, error) {
	kind, id, found := strings.Cut(configID, ":")
	if !found {
		return nil, "", false, nil
	}
	switch kind {
	case "sandbox":
		return o.sandboxLaunchSpec(ctx, id)
	default:
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
	locations, err := o.singleRefLocations(tmpl.Ref)
	if err != nil {
		return nil, "", false, err
	}
	spec := o.sandboxFinalLaunchSpec(sb, tmpl, cfgSpec, locations)
	spec.Env = sandboxTaskEnv(sb)
	return spec, sb.PidFile(), true, nil
}

// SandboxTaskAuth implements the non-secret half of task bootstrap. The store
// query selects only run_dir and exact ownership columns; no tenant credential
// is decrypted before SO_PEERCRED and pidfile authentication succeeds.
func (o *Orchestrator) SandboxTaskAuth(ctx context.Context, sandboxID, runID string) (configsock.SandboxTaskAuth, bool, error) {
	attempt, ok := o.launches.Lookup(sandboxID)
	if !ok || attempt.RunID() != runID {
		return configsock.SandboxTaskAuth{}, false, nil
	}
	if _, found, err := o.st.StartingTaskIdentity(ctx, sandboxID, runID); err != nil || !found {
		return configsock.SandboxTaskAuth{}, found, err
	}
	return configsock.SandboxTaskAuth{PidFile: nodepath.RunnerPID(o.cfg.Paths.RunRoot, runID)}, true, nil
}

// SandboxTaskSpecFor is called by configsock only after exact-run peer
// authentication. Reading the durable row here may decrypt MANIFEST_KEY solely
// for delivery into this one tenant-bound task process.
func (o *Orchestrator) SandboxTaskSpecFor(ctx context.Context, sandboxID, runID string) (*configsock.SandboxTaskSpec, bool, error) {
	attempt, ok := o.launches.Lookup(sandboxID)
	if !ok || attempt.RunID() != runID {
		return nil, false, nil
	}
	sb, err := o.st.Get(ctx, sandboxID)
	if err != nil || sb == nil {
		return nil, false, err
	}
	if sb.State != types.StateStarting || sb.RunID != runID {
		return nil, false, nil
	}
	tmpl, err := types.ParseTemplateID(sb.TemplateID)
	if err != nil {
		return nil, false, err
	}
	cfgSpec, err := sandboxcfg.ParseSpec(sb.Metadata)
	if err != nil {
		return nil, false, err
	}
	response := &configsock.SandboxTaskSpec{
		SandboxID: sandboxID,
		RunID:     runID,
		Workdir:   sb.RunDir,
		Env:       sandboxTaskEnv(sb),
	}
	source := sandboxcfg.SourceForLaunch(sb, tmpl)
	if source.Empty() {
		locations, err := o.singleRefLocations(tmpl.Ref)
		if err != nil {
			return nil, false, err
		}
		response.Final = o.sandboxFinalLaunchSpec(sb, tmpl, cfgSpec, locations)
		return response, true, nil
	}
	deadline := attempt.Deadline()
	if deadline.IsZero() {
		return nil, false, errors.New("orch: sandbox task launch deadline is not initialized")
	}
	manifestConfig := o.cfg.ManifestConfig
	if manifestConfig != "" && !filepath.IsAbs(manifestConfig) {
		manifestConfig, err = filepath.Abs(manifestConfig)
		if err != nil {
			return nil, false, fmt.Errorf("orch: absolute manifest config path: %w", err)
		}
	}
	checkpointDir := filepath.Join(sb.BaseDir, "checkpoint")
	taskRootRef, err := normalizeSandboxTaskRootRef(source.Ref, checkpointDir)
	if err != nil {
		return nil, false, err
	}
	response.Prepare = &configsock.ArtifactPrepareSpec{
		RunID:                    runID,
		RootSandboxRef:           source.SandboxRef,
		RootSourceKind:           string(source.Kind),
		RootRef:                  taskRootRef,
		LaunchMode:               string(sb.LaunchMode),
		ManifestConfig:           manifestConfig,
		RefLocationParent:        o.cfg.Checkpoint.Remote.RefLocationParent,
		RelativeDir:              checkpointDir,
		MaxRefs:                  maxRequiredArtifactRefs,
		AbsoluteDeadlineUnixNano: deadline.UnixNano(),
	}
	return response, true, nil
}

func normalizeSandboxTaskRootRef(raw, relativeDir string) (string, error) {
	if strings.HasPrefix(raw, "manifest://") || strings.HasPrefix(raw, "file://") {
		return raw, nil
	}
	if filepath.IsAbs(raw) {
		return filepath.Clean(raw), nil
	}
	if !filepath.IsAbs(relativeDir) {
		return "", fmt.Errorf("orch: relative local artifact root requires an absolute artifact directory")
	}
	return filepath.Clean(filepath.Join(relativeDir, raw)), nil
}

func sandboxTaskEnv(sb *types.Sandbox) map[string]string {
	return map[string]string{
		"MANIFEST_KEY": sb.ManifestKey,
	}
}

func (o *Orchestrator) CompleteSandboxPrepare(ctx context.Context, sandboxID, runID string, summary configsock.ArtifactPrepareSummary) (*configsock.LaunchSpec, error) {
	attempt, ok := o.launches.Lookup(sandboxID)
	if !ok || attempt.RunID() != runID {
		return nil, configsock.RejectArtifactPrepare(errLaunchOwnershipLost)
	}
	sb, err := o.st.Get(ctx, sandboxID)
	if err != nil {
		return nil, err
	}
	if sb == nil || sb.RunID != runID || (sb.State != types.StateStarting && sb.State != types.StateRunning) {
		return nil, configsock.RejectArtifactPrepare(errLaunchOwnershipLost)
	}
	tmpl, err := types.ParseTemplateID(sb.TemplateID)
	if err != nil {
		return nil, configsock.RejectArtifactPrepare(err)
	}
	if err := validatePreparedPair(summary, sandboxcfg.SourceForLaunch(sb, tmpl)); err != nil {
		return nil, configsock.RejectArtifactPrepare(err)
	}
	replay, err := attempt.SubmitPrepare(runID, summary)
	if err != nil {
		return nil, configsock.RejectArtifactPrepare(err)
	}
	if replay {
		o.log.Info("sandbox task artifact prepare replay", "sid", sandboxID, "run_id", runID,
			"task_artifact_prepare_replay_total", 1)
	}
	final, err := attempt.WaitFinalSpec(ctx)
	if err != nil {
		return nil, err
	}
	current, err := o.st.Get(ctx, sandboxID)
	if err != nil {
		return nil, err
	}
	if current == nil || current.RunID != runID || (current.State != types.StateStarting && current.State != types.StateRunning) ||
		current.TemplateID != sb.TemplateID || current.CreatedUnix != sb.CreatedUnix ||
		validatePreparedPair(summary, sandboxcfg.SourceForLaunch(current, tmpl)) != nil {
		return nil, configsock.RejectArtifactPrepare(errLaunchOwnershipLost)
	}
	return final, nil
}

func (o *Orchestrator) singleRefLocations(raw string) (map[string]string, error) {
	locations := map[string]string{}
	if raw == "" {
		return locations, nil
	}
	if err := o.addRefLocation(locations, raw); err != nil {
		return nil, err
	}
	return locations, nil
}

func (o *Orchestrator) sandboxFinalLaunchSpec(sb *types.Sandbox, tmpl types.TemplateID, cfgSpec sandboxcfg.SandboxSpec, locations map[string]string) *configsock.LaunchSpec {
	// The fully resolved network was already rendered by the unique launch
	// worker. Artifact selection remains task-local and is never included here.
	p := o.sandboxParams(sb, tmpl, cfgSpec, sandboxcfg.NetworkSpec{}, rtconfig.ResourcesConfig{})
	// --run-root pins sandbox-ctl's socket/staging dir (ch.sock, ctl.sock, …) to the
	// ordinary Sandbox roots, so the persisted RunDir/BaseDir match the exact
	// directories sandbox-ctl owns and local control commands find ctl.sock.
	args := []string{
		"run", "--sandbox-id", sb.ID,
		"--path-id", sb.ID,
		"--config", o.sandboxConfigPath(sb),
		"--manifest-config", o.cfg.ManifestConfig,
		"--run-root", nodepath.SandboxRunRoot(o.cfg.Paths.RunRoot),
		"--base-root", nodepath.SandboxBaseRoot(o.cfg.Paths.BaseRoot),
		// Each output carries its own explicit identity fields. The runtime
		// interprets only the generic target syntax, not these business labels.
		"--log-to", sandboxJournalTarget("sandbox-ctl", sb),
		"--stdout-to", sandboxJournalTarget(configsock.RunnerLogTag, sb),
		"--stderr-to", sandboxJournalTarget(configsock.RunnerLogTag, sb),
		"--console", sandboxJournalTarget(configsock.ConsoleTag, sb),
	}
	args = appendRefLocationArgs(args, locations)
	for _, c := range p.ConnectSpecs() {
		args = append(args, "--connect", c)
	}
	return &configsock.LaunchSpec{
		Exec:    o.executables.SandboxCtl(),
		Args:    args,
		Workdir: sb.RunDir,
	}
}

// --- reaper / reconcile ---

// Reaper enforces active idle TTLs and bounded terminal-history retention:
// idle past deadline -> auto-suspend, then owner-free dead/ready/error rows
// past their configured diagnostic window -> exact durable delete.
func (o *Orchestrator) Reaper(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case tick := <-t.C:
			// Collect past-deadline sandboxes (read-only scan), then pause them
			// after the scan — pauseSandbox writes the store, which must not run
			// while RangeByState's read cursor is open.
			now := tick.Unix()
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
			if err := o.queueMissingRunSessionChecks(ctx); err != nil {
				o.log.Warn("reaper run-session maintenance", "err", err)
			}
			if n, err := o.st.PruneExpiredKeyPairs(ctx); err != nil {
				o.log.Warn("reaper prune key pairs", "err", err)
			} else if n > 0 {
				o.log.Info("reaper pruned expired key pairs", "n", n)
			}
			if err := o.reapTerminalHistory(ctx, tick); err != nil {
				o.log.Warn("reaper prune terminal history", "err", err)
			}
		}
	}
}

func (o *Orchestrator) queueMissingRunSessionChecks(ctx context.Context) error {
	var runIDs []string
	collect := func(sb *types.Sandbox) error {
		if sb.RunID == "" {
			return nil
		}
		if sb.ExecutionResult != nil || !o.runSessionActive(runKindSandbox, sb.RunID) {
			runIDs = append(runIDs, sb.RunID)
		}
		return nil
	}
	for _, state := range []types.State{types.StateStarting, types.StateRunning, types.StatePaused, types.StateDeleting} {
		if err := o.st.RangeByState(ctx, state, collect); err != nil {
			return err
		}
	}
	for _, runID := range runIDs {
		o.startSandboxRunEndCheck(runID)
	}
	return nil
}

// Reconcile is the complete restart path used by tests and embedded callers.
// The conductor starts its config socket between ReconcileSandboxes and
// ReconcileBuilds so an already-running builder can never finish into an absent
// result endpoint during adoption.
func (o *Orchestrator) Reconcile(ctx context.Context) error {
	if err := o.ReconcileSandboxes(ctx); err != nil {
		return err
	}
	return o.ReconcileBuilds(ctx)
}

// ReconcileSandboxes adopts/cleans sandboxes after an orchestrator restart,
// using the systemd unit set as the liveness authority. Interrupted fresh
// creates become dead. Interrupted resumes release their old exact runner and
// network ownership but remain starting with their durable launch_mode; run-pool
// startup retries that same accepted cold or memory decision.
func (o *Orchestrator) ReconcileSandboxes(ctx context.Context) error {
	units, err := o.listRunUnits(ctx, runKindSandbox)
	if err != nil {
		return err
	}
	alive := map[string]bool{}
	for _, u := range units {
		if id := o.unitToRunID(u.Name); id != "" {
			o.runs.restore(id, u.Name)
		}
		if u.ActiveState == "active" || u.ActiveState == "activating" {
			alive[o.unitToRunID(u.Name)] = true
		}
	}
	// Adopt live sandboxes in-memory inline (o.cache is not a store write);
	// collect the dead/result-owned ones and tear them down after the scan, since
	// teardown + terminal writes must not run while the read cursor is open.
	var deleting, paused, acceptedStarting, interrupted, acceptedRunning, dead, deadHistory []*types.Sandbox
	knownRuns := make(map[string]bool)
	if err := o.st.RangeByState(ctx, types.StateDeleting, func(sb *types.Sandbox) error {
		if sb.RunID != "" {
			knownRuns[sb.RunID] = true
		}
		deleting = append(deleting, sb)
		return nil
	}); err != nil {
		return err
	}
	if err := o.st.RangeByState(ctx, types.StatePaused, func(sb *types.Sandbox) error {
		if sb.RunID != "" {
			knownRuns[sb.RunID] = true
		}
		paused = append(paused, sb)
		return nil
	}); err != nil {
		return err
	}
	if err := o.st.RangeByState(ctx, types.StateStarting, func(sb *types.Sandbox) error {
		if sb.RunID != "" {
			knownRuns[sb.RunID] = true
		}
		if sb.ExecutionResult != nil {
			acceptedStarting = append(acceptedStarting, sb)
		} else {
			interrupted = append(interrupted, sb)
		}
		return nil
	}); err != nil {
		return err
	}
	if err := o.st.RangeByState(ctx, types.StateRunning, func(sb *types.Sandbox) error {
		if sb.RunID != "" {
			knownRuns[sb.RunID] = true
		}
		if sb.ExecutionResult != nil {
			acceptedRunning = append(acceptedRunning, sb)
		} else if sb.RunID != "" && alive[sb.RunID] {
			o.cache(sb) // re-adopt: route + TTL already in store
			o.observeSandboxUpsert(sb)
		} else {
			dead = append(dead, sb)
		}
		return nil
	}); err != nil {
		return err
	}
	if err := o.st.RangeByState(ctx, types.StateDead, func(sb *types.Sandbox) error {
		if sb.RunID != "" {
			knownRuns[sb.RunID] = true
		}
		deadHistory = append(deadHistory, sb)
		return nil
	}); err != nil {
		return err
	}
	for _, sb := range deleting {
		o.log.Info("reconcile: resume deleting sandbox finalizer", "sid", sb.ID, "run_id", sb.RunID, "port", sb.VswitchPort)
		if err := o.finalizeSandboxDeleteOnce(ctx, sb.ID); err != nil {
			return fmt.Errorf("reconcile: finalize deleting sandbox %s: %w", sb.ID, err)
		}
	}
	for _, sb := range paused {
		o.log.Info("reconcile: verify paused ownership cleanup", "sid", sb.ID, "run_id", sb.RunID, "port", sb.VswitchPort)
		if err := o.cleanupPausedOwnership(ctx, sb); err != nil {
			return fmt.Errorf("reconcile: cleanup paused sandbox %s: %w", sb.ID, err)
		}
		o.cache(sb)
		o.observeSandboxUpsert(sb)
	}
	for _, sb := range acceptedStarting {
		o.log.Info("reconcile: accepted result for starting sandbox", "sid", sb.ID, "run_id", sb.RunID, "launch_mode", sb.LaunchMode)
		if err := o.recoverAcceptedStartingResult(ctx, sb); err != nil {
			return fmt.Errorf("reconcile: recover accepted starting result %s: %w", sb.ID, err)
		}
	}
	for _, sb := range interrupted {
		resume := sb.ResumeSource.Valid()
		target := types.StateDead
		if resume {
			target = types.StateStarting
		}
		o.log.Info("reconcile: interrupted sandbox launch", "sid", sb.ID, "target", target, "launch_mode", sb.LaunchMode)
		if err := o.teardownPersistedOwnership(ctx, sb, !resume); err != nil {
			return fmt.Errorf("reconcile: cleanup interrupted sandbox %s: %w", sb.ID, err)
		}
		if resume {
			changed, resetErr := o.st.ResetStartingOwnershipForRecovery(ctx, sb.ID, sb.RunID)
			if resetErr != nil {
				return resetErr
			}
			if !changed {
				return fmt.Errorf("reconcile: interrupted resume ownership changed for %s", sb.ID)
			}
			o.runs.forget(sb.RunID)
			o.releaseDetachedPortFence(sb.VswitchPort)
			recovered, getErr := o.st.Get(ctx, sb.ID)
			if getErr != nil {
				return getErr
			}
			if recovered == nil || recovered.State != types.StateStarting || recovered.RunID != "" || recovered.LaunchMode != sb.LaunchMode {
				return fmt.Errorf("reconcile: recovered resume %s lost durable starting mode", sb.ID)
			}
			o.markDeadlineIntent(sb.ID)
			o.queueRecoveredResume(recovered)
			o.observeSandboxUpsert(recovered)
			continue
		}
		changed, err := o.st.RollbackStartingDead(ctx, sb)
		if err != nil {
			return err
		}
		if !changed {
			return fmt.Errorf("reconcile: interrupted launch ownership changed for %s", sb.ID)
		}
		o.runs.forget(sb.RunID)
		o.releaseDetachedPortFence(sb.VswitchPort)
		if changed {
			updated, getErr := o.st.Get(ctx, sb.ID)
			if getErr != nil {
				return getErr
			}
			if updated != nil {
				o.observeSandboxUpsert(updated)
			}
		}
	}
	for _, sb := range acceptedRunning {
		o.log.Info("reconcile: accepted result for running sandbox", "sid", sb.ID, "run_id", sb.RunID)
		if err := o.finalizeSandboxResultOnce(ctx, sb.ID, sb.RunID); err != nil {
			return fmt.Errorf("reconcile: cleanup accepted result sandbox %s: %w", sb.ID, err)
		}
	}
	for _, sb := range dead {
		o.log.Info("reconcile: dead sandbox", "sid", sb.ID)
		if err := o.teardownPersistedOwnership(ctx, sb, true); err != nil {
			return fmt.Errorf("reconcile: cleanup dead sandbox %s: %w", sb.ID, err)
		}
		changed, err := o.st.CommitSandboxDead(ctx, sb)
		if err != nil {
			return err
		}
		if !changed {
			return fmt.Errorf("reconcile: running sandbox %s changed before dead commit", sb.ID)
		}
		o.runs.forget(sb.RunID)
		o.releaseDetachedPortFence(sb.VswitchPort)
		updated, err := o.st.Get(ctx, sb.ID)
		if err != nil {
			return err
		}
		if updated != nil {
			o.observeSandboxUpsert(updated)
		}
	}
	for _, sb := range deadHistory {
		o.observeSandboxUpsert(sb)
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
		o.runs.forget(runID)
	}
	return nil
}

func (o *Orchestrator) recoverAcceptedStartingResult(ctx context.Context, observed *types.Sandbox) error {
	if observed == nil || observed.RunID == "" || observed.ExecutionResult == nil {
		return nil
	}
	unlock := o.lifecycle.Lock(observed.ID)
	defer unlock()

	current, err := o.st.Get(ctx, observed.ID)
	if err != nil {
		return err
	}
	if current == nil || current.State != types.StateStarting || current.RunID != observed.RunID ||
		current.ExecutionResult == nil || current.ExecutionResult.RunID != observed.RunID {
		return nil
	}
	resume := current.ResumeSource.Valid()
	if err := o.teardownPersistedOwnership(ctx, current, !resume); err != nil {
		return err
	}
	var changed bool
	if resume {
		changed, err = o.st.RollbackStartingPaused(ctx, current)
	} else {
		changed, err = o.st.RollbackStartingDead(ctx, current)
	}
	if err != nil {
		return err
	}
	if !changed {
		return fmt.Errorf("orch: accepted starting result ownership changed for %s", current.ID)
	}
	o.runs.forget(current.RunID)
	o.releaseDetachedPortFence(current.VswitchPort)
	updated, err := o.st.Get(ctx, current.ID)
	if err != nil {
		return err
	}
	if updated == nil {
		return nil
	}
	if resume {
		o.cache(updated)
	} else {
		o.uncache(updated.ID)
	}
	o.observeSandboxUpsert(updated)
	return nil
}

func (o *Orchestrator) queueRecoveredResume(sb *types.Sandbox) {
	if sb == nil {
		return
	}
	o.recoveryMu.Lock()
	o.recoveredResumes = append(o.recoveredResumes, cloneSandbox(sb))
	o.recoveryMu.Unlock()
}

func (o *Orchestrator) startRecoveredResumes(ctx context.Context) error {
	o.recoveryMu.Lock()
	recovered := o.recoveredResumes
	o.recoveredResumes = nil
	o.recoveryMu.Unlock()
	for i, sb := range recovered {
		if err := o.startRecoveredResume(ctx, sb); err != nil {
			o.recoveryMu.Lock()
			o.recoveredResumes = append(recovered[i:], o.recoveredResumes...)
			o.recoveryMu.Unlock()
			return fmt.Errorf("start recovered resume %s: %w", sb.ID, err)
		}
	}
	return nil
}

func (o *Orchestrator) startRecoveredResume(ctx context.Context, recovered *types.Sandbox) error {
	if recovered == nil || recovered.ID == "" {
		return errors.New("recovered resume is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	tmpl, err := types.ParseTemplateID(recovered.TemplateID)
	if err != nil {
		return err
	}
	preparation, err := o.prepareSandboxLaunch(ctx, recovered, tmpl)
	if err != nil {
		return err
	}

	unlock := o.lifecycle.Lock(recovered.ID)
	defer unlock()
	current, err := o.st.Get(ctx, recovered.ID)
	if err != nil {
		return err
	}
	if current == nil || current.State != types.StateStarting || current.RunID != "" ||
		current.ResumeSource != recovered.ResumeSource || current.LaunchMode != recovered.LaunchMode {
		return errLaunchOwnershipLost
	}
	attempt, err := o.launches.Claim(o.launchContext(), current.ID, launchResume)
	if err != nil {
		return err
	}
	attempt.SetAcceptedAt(time.Now())
	attempt.SetLaunchMode(current.LaunchMode)
	o.markDeadlineIntent(current.ID)
	o.cache(current)
	o.publishUpsert(current)
	o.observeSandboxUpsert(current)
	work := cloneSandbox(current)
	o.launches.Start(attempt, func(launchCtx context.Context, owner *launchAttempt) error {
		return o.runLaunch(launchCtx, owner, work, tmpl, preparation)
	})
	return nil
}

// ReconcileBuilds adopts live builder units only after the local config socket
// is accepting phase/result reports. It must run before new Build work is
// admitted to the run pool.
func (o *Orchestrator) ReconcileBuilds(ctx context.Context) error {
	if err := o.reconcileBuilds(ctx); err != nil {
		return err
	}
	o.buildRecoveryReadyOnce.Do(func() {
		if o.buildRecoveryReady != nil {
			close(o.buildRecoveryReady)
		}
	})
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
		if u.Name == unit && builderUnitMayHaveProcesses(u.ActiveState) {
			return true
		}
	}
	return false
}

func builderUnitMayHaveProcesses(state string) bool {
	switch state {
	case "active", "activating", "reloading", "deactivating":
		return true
	default:
		return false
	}
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

// teardownPersistedOwnership releases resources in dependency order without
// mutating the durable row. On failure it leaves later resources and the cache
// untouched so the caller can preserve the row and retry. On success the caller
// must immediately commit its terminal state or restore the cache if that write
// fails.
func (o *Orchestrator) teardownPersistedOwnership(ctx context.Context, sb *types.Sandbox, includeBase bool) error {
	if err := o.validateSandboxCleanupPaths(sb); err != nil {
		return err
	}
	progress := launchCleanupProgress{
		port:   sb.VswitchPort,
		runDir: sb.RunDir,
	}
	if includeBase {
		progress.baseDir = sb.BaseDir
	}
	if sb.RunID != "" {
		progress.runID = sb.RunID
		progress.unit = o.runnerUnit(sb.RunID)
	}
	if err := progress.step(ctx, o, true); err != nil {
		return err
	}
	o.uncache(sb.ID)
	return nil
}

// capture consumes the producer's complete JSON result before publishing a
// paused row. Local references remain content-identified basenames; execution
// resolves them in this sandbox's checkpoint directory.
func (o *Orchestrator) capture(ctx context.Context, sb *types.Sandbox, request sandboxcfg.CaptureRequest) (sandboxcfg.CaptureResult, error) {
	if err := validateCaptureRequest(request); err != nil {
		return sandboxcfg.CaptureResult{}, err
	}
	source, err := o.snapshotLocal(ctx, sb, request)
	if err != nil {
		return sandboxcfg.CaptureResult{}, err
	}
	return sandboxcfg.CaptureResult{Source: source}, nil
}

func (o *Orchestrator) snapshotLocal(ctx context.Context, sb *types.Sandbox, request sandboxcfg.CaptureRequest) (types.ResumeSource, error) {
	dir := filepath.Join(sb.BaseDir, "checkpoint")
	command := "snapshot"
	kind := types.ResumeSourceSnapshot
	if request.Kind == types.CaptureSandbox {
		command, kind = "export", types.ResumeSourceSandbox
	}
	args := []string{command, "--json", "--path-id", sb.ID, "--output", dir, "--mode", o.cfg.Checkpoint.Mode, "--run-root", nodepath.SandboxRunRoot(o.cfg.Paths.RunRoot)}
	if request.Kind == types.CaptureSnapshot {
		args = appendSnapshotPolicyArgs(args, request.SnapshotPolicy)
	}
	cmd := exec.CommandContext(ctx, o.executables.SandboxCtl(), args...)
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	if err := cmd.Run(); err != nil {
		return types.ResumeSource{}, fmt.Errorf("orch: %s %s (mode %s): %w: %s", command, sb.ID, o.cfg.Checkpoint.Mode, err, errb.String())
	}
	return decodeCaptureSource(out.Bytes(), kind)
}

// decodeCaptureSource is shared by capture adapters regardless of whether the
// producer writes local files, a Bundle, or uploads Manifest objects.
func decodeCaptureSource(data []byte, kind types.ResumeSourceKind) (types.ResumeSource, error) {
	report, err := artifact.DecodeCaptureReport(data, artifactRole(kind))
	if err != nil {
		return types.ResumeSource{}, fmt.Errorf("orch: %s capture result: %w", kind, err)
	}
	source := types.ResumeSource{Kind: kind, Ref: report.SandboxRef}
	if kind == types.ResumeSourceSnapshot {
		source.Ref, source.SandboxRef = report.SnapshotRef, report.SandboxRef
	}
	return source, nil
}

func appendSnapshotPolicyArgs(args []string, policy sandboxcfg.SnapshotPolicy) []string {
	if policy.MergeRef != nil {
		args = append(args, fmt.Sprintf("--merge-ref=%t", *policy.MergeRef))
	}
	if policy.DropCaches != nil {
		args = append(args, fmt.Sprintf("--drop-caches=%t", *policy.DropCaches))
	}
	return args
}

// promote publishes a local Sandbox or Snapshot graph without booting it. The configured
// publisher is either manifest storage or a named ref location.
func (o *Orchestrator) promote(ctx context.Context, sb *types.Sandbox, source types.ResumeSource) (artifact.PublishReport, error) {
	if !source.Valid() {
		return artifact.PublishReport{}, fmt.Errorf("orch: promote %s: invalid resume source", sb.ID)
	}
	args := []string{"publish", "--json", "--quiet", "--manifest-config", o.cfg.ManifestConfig}
	if o.cfg.Checkpoint.Remote.RefLocationParent != "" {
		// Publication location name: the sandbox's stable identity. The name
		// is the directory key, so every publication of one logical entity —
		// across renames (import with a new target id), node migrations, and
		// cluster generations — converges on one directory where
		// content-addressed files accumulate as versions (same-content files
		// are deduplicated by the publisher). Rows sharing a stable id (an
		// identity-preserving copy) publish into that one directory too.
		locName := reflocation.PublicationName(sb.StableID())
		uri, err := o.cfg.Checkpoint.RefLocationURI(locName)
		if err != nil {
			return artifact.PublishReport{}, fmt.Errorf("orch: promote %s: %w", sb.ID, err)
		}
		args = append(args, "--to-ref-location", locName+"="+uri)
	}
	root := source.Ref
	if !types.IsPortableRef(root) {
		dir, err := o.ownedLocalArtifactDir(sb, source)
		if err != nil {
			return artifact.PublishReport{}, err
		}
		// Resolve only the artifact argument in its checkpoint context. Keep
		// the process cwd so relative Manifest config/storage paths retain
		// their configured meaning; public reports still project basenames.
		if strings.HasPrefix(root, "file://") {
			ref, err := manifest.ParseRef(root)
			if err != nil {
				return artifact.PublishReport{}, err
			}
			ref.Path = filepath.Join(dir, ref.Path)
			root = ref.String()
		}
	}
	args = append(args, root)
	cmd := exec.CommandContext(ctx, o.executables.SandboxCtl(), args...)
	cmd.Env = append(os.Environ(), "MANIFEST_KEY="+sb.ManifestKey)
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	if err := cmd.Run(); err != nil {
		return artifact.PublishReport{}, fmt.Errorf("orch: promote %s: %w: %s", sb.ID, err, errb.String())
	}
	report, err := artifact.DecodePublishReport(out.Bytes(), artifactRole(source.Kind))
	if err != nil {
		return artifact.PublishReport{}, fmt.Errorf("orch: promote: %w", err)
	}
	return report, nil
}

// udsClient builds an HTTP client for envd. The request context carries the
// shared launch timeout.
func udsClient(sock string) *http.Client {
	return &http.Client{
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
