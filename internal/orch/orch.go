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

	mu             sync.Mutex
	reg            map[string]*types.Sandbox // in-memory cache (hot path: Route/LaunchSpecFor)
	clusterCreates map[string]struct{}       // cluster creates claimed before async launch

	sf        flightGroup    // per-sid single-flight for resume (dedup concurrent data-plane wakeups)
	lifecycle keyedLockGroup // serialize resume against destructive/deadline mutations for one sid

	deadlineIntentMu sync.Mutex
	deadlineIntents  map[string]struct{} // paused sandboxes whose next resume must preserve an explicit deadline
	resumeRequestMu  sync.Mutex
	resumeRequests   map[string]*resumeRequestState // outstanding requests and the latest Pause/Delete fence

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

	clusterCtx context.Context // node-link async work lifetime (set by serve); nil = background
	probe      ResourceProbe   // node water level for cluster heartbeat (set by serve when resource_listen on); nil = none

	clusterBuildMu sync.Mutex
	clusterBuilds  map[string]*clusterBuild   // build_id -> transient cluster image-pull creds (§7.5)
	buildEvents    chan *routesync.BuildEvent // node -> registry build state, drained by the node-link client
}

// clusterBuild is a registry-driven build's transient image-pull context. Cluster
// identity remains opaque in Build.Metadata and is never interpreted here.
type clusterBuild struct {
	imageRepo    string
	registryAuth string
}

func New(cfg *config.Config, st *store.Store, lc launcher.Launcher, vs vsClient, log *slog.Logger) *Orchestrator {
	o := &Orchestrator{
		cfg: cfg, st: st, lc: lc, vs: vs, log: log,
		sandboxReadyTimeout: 60 * time.Second,
		reg:                 map[string]*types.Sandbox{},
		clusterCreates:      map[string]struct{}{},
		deadlineIntents:     map[string]struct{}{},
		subs:                map[int]chan routesync.Event{},
		routeFP:             uuid.NewString(),
		pend:                map[string]*pendingBuild{},
		clusterBuilds:       map[string]*clusterBuild{},
		buildEvents:         make(chan *routesync.BuildEvent, 64),
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
	// Layer the template's declared config under this create request. Restore is
	// deliberately request-scoped, so a template metadata value is not inherited.
	var templateMetadata map[string]string
	if tb := o.templateBuild(ctx, req.APIKey, req.TemplateID); tb != nil && len(tb.Metadata) > 0 {
		templateMetadata = tb.Metadata
	}
	meta := sandboxcfg.MergeCreateMetadata(templateMetadata, req.Metadata)
	// Validate the create restore policy before allocating an
	// identity, minting credentials, creating directories, attaching networking,
	// or starting a process. Normalization also gives every later trust boundary
	// one canonical value to parse.
	meta, err = sandboxcfg.NormalizeRestoreMetadata(meta)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", api.ErrBadRequest, err)
	}
	credentials, meta, err := sandboxcfg.ExtractCredentials(meta)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", api.ErrBadRequest, err)
	}
	if err := validateSandboxCredentialOverrides(tmpl.Profile, credentials); err != nil {
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
		State:        types.StateRunning,
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
	if err := o.launch(ctx, sb, tmpl); err != nil {
		o.teardown(context.Background(), sb)
		return nil, err
	}
	o.publishUpsert(sb) // tell external proxies about the new route
	return sb, nil
}

// launch prepares dirs + network, writes the sandbox config file, starts the unit
// (node-ctl run-sandbox -> sandbox-ctl), waits for readiness and provisions
// envd. The non-secret config lands at <run-dir>/<sid>.yaml; the secret manifest
// key rides in the run-sandbox LaunchSpec env (LaunchSpecFor). The cgroup is the
// unit's own (--cgroup-adopt). Used by Create and Connect(resume).
func (o *Orchestrator) launch(ctx context.Context, sb *types.Sandbox, tmpl types.TemplateID) error {
	for _, d := range []string{sb.RunDir, sb.BaseDir} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			return fmt.Errorf("orch: mkdir %s: %w", d, err)
		}
	}
	spec, err := sandboxcfg.ParseSpec(sb.Metadata)
	if err != nil {
		return err
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
		return err
	}
	port, err := o.attachNetwork(ctx, network)
	if err != nil {
		return err
	}
	sb.VswitchPort, sb.FloatingIP, sb.PortMAC, sb.InnerIP = port.Port, port.FloatingIP, port.MAC, network.InnerIP

	p := o.sandboxParams(sb, tmpl, spec, network)
	// Pin capacity to the snapshot the runtime froze (read above). Template
	// snapshots are self-describing and may have been taken at a different budget
	// than this node's create defaults (e.g. the build pipeline's builder.vcpu/memory).
	if snap.HasCapacity {
		p.VCPU, p.Memory = snap.CapCPU, snap.CapMem
	}
	if err := p.WriteYAML(o.sandboxConfigPath(sb)); err != nil {
		return err
	}
	if _, err := o.runnerPool.Assign(ctx, sb.ID, func(runID string) error {
		sb.RunID = runID
		if err := o.st.Put(ctx, sb); err != nil {
			return err
		}
		o.cache(sb)
		return nil
	}); err != nil {
		return err
	}
	if tmpl.Profile == types.ProfileE2B {
		// Headroom for a cold microVM boot + envd ready; FC mode (mmds.enabled) adds the
		// MMDS poll handshake, and a remote-snapshot restore (migration/fork) is heavier
		// than a warm img cold-boot.
		readyTimeout := o.sandboxReadyTimeout
		if readyTimeout <= 0 {
			readyTimeout = 60 * time.Second
		}
		if err := o.waitReady(ctx, sb, readyTimeout); err != nil {
			return err
		}
		if err := o.envdInit(ctx, sb); err != nil {
			o.log.Warn("envd /init", "sid", sb.ID, "err", err)
		}
	}
	return nil
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
	o.cancelResumeRequests(id)
	o.teardown(ctx, sb)
	if err := o.st.Delete(ctx, id); err != nil {
		return false, err
	}
	o.clearDeadlineIntent(id)
	o.uncache(id)
	o.publishDelete(id) // tell external proxies the route is gone
	return true, nil
}

func (o *Orchestrator) Pause(ctx context.Context, id, apiKey string) error {
	unlock := o.lifecycle.Lock(id)
	defer unlock()

	sb, err := o.st.Get(ctx, id)
	if err != nil {
		return err
	}
	if !ownsSandbox(sb, apiKey) {
		return api.ErrNotFound
	}
	o.cancelResumeRequests(id)
	return o.pauseSandboxLocked(ctx, sb)
}

// pauseSandbox snapshots a running sandbox and stops it — the work behind Pause
// and the reaper's auto-suspend (no api key: the caller has already authorized).
func (o *Orchestrator) pauseSandbox(ctx context.Context, sb *types.Sandbox) error {
	unlock := o.lifecycle.Lock(sb.ID)
	defer unlock()
	o.cancelResumeRequests(sb.ID)

	current, err := o.st.Get(ctx, sb.ID)
	if err != nil {
		return err
	}
	if current == nil {
		return api.ErrNotFound
	}
	return o.pauseSandboxLocked(ctx, current)
}

func (o *Orchestrator) pauseSandboxLocked(ctx context.Context, sb *types.Sandbox) error {
	if sb.State == types.StatePaused {
		return api.ErrAlreadyPaused
	}
	ref, err := o.snapshot(ctx, sb)
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

func (o *Orchestrator) Connect(ctx context.Context, id, apiKey, migrationToken string, timeoutSec int) (*types.Sandbox, error) {
	sb, err := o.prepareStandaloneTarget(ctx, id, apiKey, migrationToken)
	if err != nil {
		return nil, err
	}
	if timeoutSec <= 0 {
		if sb.State == types.StatePaused {
			// A credential-only Connect remains non-blocking even when another
			// caller already owns the asynchronous resume flight.
			o.scheduleResume(id)
		}
		return sb, nil
	}

	unlock := o.lifecycle.Lock(id)
	// Re-read under the per-sandbox lifecycle boundary: an asynchronous resume or
	// delete may have completed since the initial existence/import decision.
	if sb, err = o.st.Get(ctx, id); err != nil {
		unlock()
		return nil, err
	}
	if !ownsSandbox(sb, apiKey) {
		unlock()
		return nil, api.ErrNotFound
	}
	requestedDeadline := time.Now().Add(time.Duration(timeoutSec) * time.Second).Unix()
	if s := o.mutateCached(id, func(nb *types.Sandbox) { nb.DeadlineUnix = requestedDeadline }); s != nil {
		sb = s // cached: published a fresh snapshot with the new deadline
	} else {
		sb.DeadlineUnix = requestedDeadline // not cached (fresh, unpublished) — safe in place
	}
	if err := o.st.SetDeadline(ctx, id, requestedDeadline); err != nil {
		unlock()
		return nil, err
	}
	if sb.State == types.StatePaused {
		o.markDeadlineIntent(id)
	}
	shouldResume := sb.State == types.StatePaused
	unlock()
	if shouldResume {
		// Import and credential lookup are complete before this point. Resume is
		// deliberately asynchronous, but still shares the per-sandbox flight with
		// data-plane wakes and other Connect calls.
		o.scheduleResume(id)
	}
	return sb, nil
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

func (o *Orchestrator) scheduleResume(id string) {
	request := o.newResumeRequest(id)
	go func() {
		defer o.releaseResumeRequest(request)
		ctx := o.asyncCtx()
		if err := o.resumeSandboxRequest(ctx, request); err != nil {
			o.log.Error("sandbox connect resume", "sid", id, "err", err)
		}
	}()
}

func (o *Orchestrator) publishPausedAfterResumeFailure(ctx context.Context, id string) {
	sb, err := o.st.Get(ctx, id)
	if err != nil {
		o.log.Error("sandbox connect reload after resume failure", "sid", id, "err", err)
		return
	}
	if sb != nil && sb.State == types.StatePaused {
		o.cache(sb)
		o.publishUpsert(sb)
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
	if cached := o.mutateCached(id, func(nb *types.Sandbox) { nb.DeadlineUnix = deadline }); cached != nil {
		sb = cached
	} else {
		sb.DeadlineUnix = deadline
	}
	if err := o.st.SetDeadline(ctx, id, deadline); err != nil {
		return false, err
	}
	if sb.State == types.StatePaused {
		o.markDeadlineIntent(id)
	}
	return true, nil
}

// resume restarts a paused sandbox from its snapshot (no api_key needed: the
// manifest key comes from the store via the config-socket).
func (o *Orchestrator) resume(ctx context.Context, sb *types.Sandbox, preserveDeadline bool) error {
	tmpl, err := types.ParseTemplateID(sb.TemplateID)
	if err != nil {
		return err
	}
	// Operate on a private copy. launch sets the network fields and publishes via
	// o.cache, swapping the cached pointer in one locked step — a published cache
	// entry is never mutated in place, so concurrent readers (Route, MMDS lookups)
	// always observe a consistent snapshot and there is no field-level data race.
	nb := *sb
	nb.State = types.StateRunning
	// Re-arm the running TTL: a resumed sandbox runs for timeout_sec more. Its stored
	// deadline is from before the pause (already passed), so without this the reaper
	// would immediately re-suspend it.
	if !preserveDeadline && o.cfg.Sandbox.TimeoutSec > 0 {
		nb.DeadlineUnix = time.Now().Add(time.Duration(o.cfg.Sandbox.TimeoutSec) * time.Second).Unix()
	}
	if err := o.launch(ctx, &nb, tmpl); err != nil {
		return errors.Join(err, o.rollbackFailedResume(sb, &nb))
	}
	if !preserveDeadline && o.cfg.Sandbox.TimeoutSec > 0 {
		_ = o.st.SetDeadline(ctx, nb.ID, nb.DeadlineUnix)
	}
	o.publishUpsert(&nb) // unparks any proxy holding a request for this sandbox
	return nil
}

func (o *Orchestrator) rollbackFailedResume(original, attempted *types.Sandbox) error {
	if attempted == nil {
		return nil
	}
	ctx := context.Background()
	o.teardown(ctx, attempted)
	// launch persists the new runner from inside the pool assignment callback.
	// Before that point the stored row is still the original paused record and
	// needs no state rollback.
	if original == nil || attempted.RunID == "" || attempted.RunID == original.RunID {
		return nil
	}
	_, err := o.st.CASRunState(ctx, attempted.ID, attempted.RunID, types.StateRunning, types.StatePaused)
	return err
}

// --- proxy.Router (internal mode) ---

// Route resolves a canonical target for the in-process proxy. A paused sandbox is
// auto-resumed on the spot (single-flight: concurrent data-plane requests collapse
// to one resume). The forwarding decision is shared with the external route table
// via proxy.RouteForTarget.
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
	if sb.State == types.StatePaused { // auto-resume on data-plane traffic
		if err := o.resumeSandbox(ctx, sandboxID); err != nil {
			return proxy.Route{}, err
		}
		if sb = o.lookup(sandboxID); sb == nil {
			return proxy.Route{Kind: proxy.KindNotFound}, nil
		}
	}
	return proxy.RouteForTarget(
		string(sb.Profile), sb.EnvdUDS, sb.CiUDS, sb.FloatingIP,
		sb.EnvdAccessToken, sb.ForwardAccessToken, target,
	), nil
}

// resumeIfPaused (run under the per-sid single-flight and lifecycle lock) resumes
// sid only if it is still paused — a loser of the race finds it already running.
func (o *Orchestrator) resumeIfPaused(ctx context.Context, sid string, preserveDeadline bool) error {
	sb := o.lookup(sid)
	if sb == nil {
		s, _ := o.st.Get(ctx, sid)
		if s == nil {
			return nil
		}
		o.cache(s)
		sb = s
	}
	if sb.State != types.StatePaused {
		return nil
	}
	if err := o.resume(ctx, sb, preserveDeadline); err != nil {
		o.publishPausedAfterResumeFailure(context.WithoutCancel(ctx), sid)
		return err
	}
	return nil
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
// systemd unit set as the liveness authority.
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
	var dead []*types.Sandbox
	knownRuns := make(map[string]bool)
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
	for _, sb := range dead {
		o.log.Info("reconcile: dead sandbox", "sid", sb.ID)
		o.teardown(ctx, sb)
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

func (o *Orchestrator) cache(sb *types.Sandbox) {
	o.mu.Lock()
	o.reg[sb.ID] = sb
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
	nb := *cur
	fn(&nb)
	o.reg[id] = &nb
	return &nb
}

// ByFloatingIP maps a guest's (SNAT'd) source floating IP to its running sandbox id,
// for the in-process MMDS service (proxy_mode=internal). Implements mmds.Source (PUT
// stage). Running only: a paused sandbox's slot/floating IP is freed and may be reused
// by another running sandbox, so matching paused rows would be ambiguous.
func (o *Orchestrator) ByFloatingIP(ip string) (sandboxID string, ok bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, sb := range o.reg {
		if sb.FloatingIP == ip && sb.State == types.StateRunning {
			return sb.ID, true
		}
	}
	return "", false
}

// SandboxInfo returns sid's current template id + access token (mmds.Source, GET stage).
func (o *Orchestrator) SandboxInfo(sid string) (templateID, accessToken string, ok bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if sb, ok := o.reg[sid]; ok && sb.State == types.StateRunning {
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

func (o *Orchestrator) teardown(ctx context.Context, sb *types.Sandbox) {
	// The sandbox runs in its systemd unit's own cgroup (sandbox-ctl --cgroup-adopt),
	// and the unit is KillMode=control-group, so StopUnit SIGKILLs every straggler
	// (cloud-hypervisor included). No separate cgroup drain/rmdir is needed.
	if sb.RunID != "" {
		_ = o.lc.Stop(ctx, o.runnerUnit(sb.RunID))
		_ = o.lc.ResetFailed(ctx, o.runnerUnit(sb.RunID))
	}
	_ = o.vs.Detach(ctx, sb.VswitchPort)
	_ = os.RemoveAll(sb.RunDir)
	o.uncache(sb.ID)
}

// snapshot pauses+captures the running sandbox via sandbox-ctl and returns its
// restore ref. The client dials <run-root>/<sid>/ctl.sock; the running
// snapshot captures sb per the configured checkpoint mode and returns the restore
// ref to persist: a canonical portable ref or a local bundle path
// (local — node-bound; the default). sandbox-ctl performs the work with its own
// boot-time manifest config; we pass the resolved binary + the run root.
func (o *Orchestrator) snapshot(ctx context.Context, sb *types.Sandbox) (string, error) {
	// Named-location publishing is deliberately outside the VM pause/capture
	// operation. Pause writes a local bundle; export or template finalization
	// later upgrades it to the configured portable location.
	if o.cfg.Checkpoint.Mode == config.CheckpointLocal || o.cfg.Checkpoint.Remote.RefLocationParent != "" {
		return o.snapshotLocal(ctx, sb)
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
func (o *Orchestrator) snapshotLocal(ctx context.Context, sb *types.Sandbox) (string, error) {
	dir := filepath.Join(o.cfg.Checkpoint.LocalDir, sb.ID)
	cmd := exec.CommandContext(ctx, o.cfg.SandboxCtl(), "snapshot",
		"--sandbox-id", sb.ID, "--output", dir, "--run-root", o.cfg.Paths.RunRoot)
	var errb bytes.Buffer
	cmd.Stderr = &errb
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("orch: snapshot %s (local): %w: %s", sb.ID, err, errb.String())
	}
	return filepath.Join(dir, sb.ID+".snapshot"), nil
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

// udsClient builds an HTTP client that dials a unix socket (the envd --connect UDS).
func udsClient(sock string) *http.Client {
	return &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, "unix", sock)
			},
		},
	}
}

func (o *Orchestrator) waitReady(ctx context.Context, sb *types.Sandbox, timeout time.Duration) error {
	cl := udsClient(sb.EnvdUDS)
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://envd/health", nil)
		resp, err := cl.Do(req)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == 204 || resp.StatusCode == 200 {
				return nil
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("orch: envd not ready for %s", sb.ID)
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
	payload := map[string]any{
		"envVars":        sb.Env,
		"defaultUser":    "user",
		"defaultWorkdir": "/home/user",
		"timestamp":      time.Now().UTC().Format(time.RFC3339),
	}
	if o.cfg.MMDS.Enabled {
		payload["accessToken"] = sb.EnvdAccessToken
	}
	body, _ := json.Marshal(payload)
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, "http://envd/init", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := udsClient(sb.EnvdUDS).Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("orch: envd /init status %d", resp.StatusCode)
	}
	return nil
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
