// Package orch is the orchestrator core. It ties together the store, systemd
// launcher, vswitch and config rendering, and implements api.Core (control
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
	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/filestore"
	"github.com/kuasar-sandbox/orchestrator/internal/keys"
	"github.com/kuasar-sandbox/orchestrator/internal/launcher"
	"github.com/kuasar-sandbox/orchestrator/internal/nodeexec"
	"github.com/kuasar-sandbox/orchestrator/internal/proxy"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/orchestrator/internal/util"
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

	mu  sync.Mutex
	reg map[string]*types.Sandbox // in-memory cache (hot path: Route/LaunchSpecFor)

	sf        flightGroup // per-sid single-flight for resume (dedup concurrent data-plane wakeups)
	lifecycle serialGroup // serializes resume, delete, and Binding replacement for the same SID

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

	clusterCtx  context.Context         // node-link async work lifetime (set by serve); nil = background
	keyResolver NodeKeyMaterialResolver // optional resolver for Provider-backed key references
}

func New(cfg *config.Config, st *store.Store, lc launcher.Launcher, vs vsClient, log *slog.Logger) *Orchestrator {
	o := &Orchestrator{
		cfg: cfg, st: st, lc: lc, vs: vs, log: log,
		reg: map[string]*types.Sandbox{}, subs: map[int]chan routesync.Event{},
		routeFP: uuid.NewString(), pend: map[string]*pendingBuild{},
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
	lease, err := o.resolveAllowed(ctx, req.APIKey)
	if err != nil {
		return nil, err
	}
	if lease.AuthKey == "" {
		return nil, api.ErrNotAllowed
	}
	tmpl, err := types.ParseTemplateID(req.TemplateID)
	if err != nil {
		// Not a <profile>-<kind>-<key> id — resolve the transient register id the SDK
		// reports (BuildInfo.template_id), or a build name/alias, to its persist id.
		persist := o.resolveTemplateAlias(ctx, req.APIKey, req.TemplateID)
		if persist == "" {
			return nil, err
		}
		if tmpl, err = types.ParseTemplateID(persist); err != nil {
			return nil, err
		}
	}
	id, err := uuid.NewV7()
	if err != nil {
		return nil, fmt.Errorf("orch: new id: %w", err)
	}
	sid := id.String()
	envdTok, _ := keys.MintToken()
	trafTok, _ := keys.MintToken()

	// Layer the template's declared config (builds.metadata_json) under the create's
	// own config — create wins per namespace. Best-effort: a self-describing or
	// foreign template may have no local build record (then it's just the create's).
	meta := req.Metadata
	if tb := o.templateBuild(ctx, req.APIKey, req.TemplateID); tb != nil && len(tb.Metadata) > 0 {
		meta = sandboxcfg.MergeMetadata(tb.Metadata, req.Metadata)
	}
	meta = clusterstate.WithoutSystemMetadata(meta)

	sb := &types.Sandbox{
		ID:                 sid,
		TemplateID:         tmpl.String(), // canonical persist id (resolved from a transient/alias ref)
		State:              types.StateRunning,
		RunDir:             o.cfg.Paths.RunRoot + "/" + sid,
		BaseDir:            o.cfg.Paths.BaseRoot + "/" + sid,
		AuthKey:            lease.AuthKey,
		ManifestKey:        lease.ManifestKey,
		EnvdAccessToken:    envdTok,
		TrafficAccessToken: trafTok,
		Metadata:           meta,
		Env:                req.EnvVars,
		CreatedUnix:        time.Now().Unix(),
		DeadlineUnix:       time.Now().Add(time.Duration(req.TimeoutSec) * time.Second).Unix(),
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
	// inherited inner_ip / transit_* reaches allocInnerIP + vswitch.Attach.
	var snap snapInfo
	if ref := sandboxcfg.RestoreRefFor(sb, tmpl); ref != "" {
		snap = o.snapshotConfig(ctx, sb, ref)
		spec.Network = sandboxcfg.MergeNetwork(snap.Network, spec.Network)
	}
	plainIP, cidrIP, err := o.allocInnerIP(tmpl.Profile, spec.Network.InnerIP)
	if err != nil {
		return err
	}
	port, err := o.vs.Attach(ctx, vswitch.AttachReq{
		InnerIP:          plainIP,
		TransitGatewayIP: spec.Network.TransitGatewayIP,
		TransitGeneveVNI: spec.Network.TransitGeneveVNI,
		TransitMAC:       spec.Network.TransitMAC,
	})
	if err != nil {
		return err
	}
	sb.VswitchPort, sb.FloatingIP, sb.PortMAC, sb.InnerIP = port.Port, port.FloatingIP, port.MAC, cidrIP

	p := o.sandboxParams(sb, tmpl, spec)
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
		if err := o.waitReady(ctx, sb, 60*time.Second); err != nil {
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
		if verifyKey(apiKey, sb.AuthKey) {
			out = append(out, sb)
		}
	}
	return out, next, nil
}

func (o *Orchestrator) Kill(ctx context.Context, id, apiKey string) (bool, error) {
	sb, err := o.st.Get(ctx, id)
	if err != nil {
		return false, err
	}
	if !ownsSandbox(sb, apiKey) {
		return false, nil
	}
	o.teardown(ctx, sb)
	_ = o.st.Delete(ctx, id)
	o.uncache(id)
	o.publishDelete(id) // tell external proxies the route is gone
	return true, nil
}

func (o *Orchestrator) Pause(ctx context.Context, id, apiKey string) error {
	return o.lifecycle.Do(id, func() error {
		sb, err := o.st.Get(ctx, id)
		if err != nil {
			return err
		}
		if !ownsSandbox(sb, apiKey) {
			return api.ErrNotFound
		}
		return o.pauseSandboxLocked(ctx, sb)
	})
}

// pauseSandbox snapshots a running sandbox and stops it — the work behind Pause
// and the reaper's auto-suspend (no api key: the caller has already authorized).
func (o *Orchestrator) pauseSandbox(ctx context.Context, sb *types.Sandbox) error {
	if sb == nil || sb.ID == "" {
		return api.ErrNotFound
	}
	return o.lifecycle.Do(sb.ID, func() error {
		current, err := o.st.Get(ctx, sb.ID)
		if err != nil {
			return err
		}
		if current == nil {
			return api.ErrNotFound
		}
		return o.pauseSandboxLocked(ctx, current)
	})
}

func (o *Orchestrator) pauseSandboxLocked(ctx context.Context, sb *types.Sandbox) error {
	if sb.State == types.StatePaused {
		if managed, err := o.commitManagedSandboxState(ctx, sb, string(clusterstate.WorkflowRoutePaused), ""); err != nil {
			return err
		} else if managed {
			return nil
		}
		return api.ErrAlreadyPaused
	}
	ref, err := o.snapshot(ctx, sb)
	if err != nil {
		return err
	}
	if sb.RunID != "" {
		_ = o.lc.Stop(ctx, o.runnerUnit(sb.RunID))
		_ = o.lc.ResetFailed(ctx, o.runnerUnit(sb.RunID))
	}
	_ = o.vs.Detach(ctx, sb.VswitchPort)
	sb.SnapshotRef = ref
	sb.State = types.StatePaused
	managed, err := o.commitManagedSandboxState(ctx, sb, string(clusterstate.WorkflowRoutePaused), "")
	if err != nil {
		return err
	}
	if !managed {
		if err := o.st.SetSnapshotRef(ctx, sb.ID, ref); err != nil {
			return err
		}
		if err := o.st.SetState(ctx, sb.ID, types.StatePaused); err != nil {
			return err
		}
	}
	o.cache(sb)
	o.publishUpsert(sb) // proxies keep the (now paused) route so traffic triggers a Wake
	return nil
}

func (o *Orchestrator) Connect(ctx context.Context, id, apiKey, migrationToken string, timeoutSec int) (*types.Sandbox, error) {
	sb, err := o.st.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	// Auto-migrate: connecting to a sandbox absent on this node with a migration
	// token (api_headers X-Kuasar-Migration-Token) imports it (paused row) then
	// resumes — one SDK call does export's counterpart. ImportSandbox checks the
	// tenant key + token fingerprint + runtime digest.
	if sb == nil && migrationToken != "" {
		imported, ierr := o.ImportSandbox(ctx, apiKey, migrationToken)
		if ierr != nil {
			return nil, ierr
		}
		id = imported
		if sb, err = o.st.Get(ctx, id); err != nil {
			return nil, err
		}
	}
	if !ownsSandbox(sb, apiKey) {
		return nil, api.ErrNotFound
	}
	if sb.State == types.StatePaused {
		// Route the resume through the same per-sid single-flight the data plane
		// uses, so a /connect racing data-plane traffic (or another /connect)
		// collapses to one resume+launch instead of double-allocating the port or
		// starting the unit twice. resumeIfPaused re-checks "still paused?" inside
		// the flight, so the losers are no-ops.
		resumeRequest, err := currentSandboxRouteRequest(sb, 0)
		if err != nil {
			return nil, err
		}
		if err := o.sf.Do(id, func() error {
			return o.lifecycle.Do(id, func() error { return o.resumeIfPaused(ctx, id, resumeRequest) })
		}); err != nil {
			return nil, err
		}
		// Re-read the now-running snapshot the flight published; never mutate the
		// cached pointer in place.
		if r := o.lookup(id); r != nil {
			sb = r
		} else if sb, err = o.st.Get(ctx, id); err != nil || sb == nil {
			return nil, api.ErrNotFound
		}
		if sb.State != types.StateRunning {
			return nil, fmt.Errorf("%w: sandbox state changed during resume", api.ErrConflict)
		}
	}
	if timeoutSec > 0 {
		found, err := o.SetTimeout(ctx, id, apiKey, timeoutSec)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, api.ErrNotFound
		}
		sb, err = o.st.Get(ctx, id)
		if err != nil || sb == nil {
			return nil, errors.Join(err, api.ErrNotFound)
		}
	}
	return sb, nil
}

func (o *Orchestrator) SetTimeout(ctx context.Context, id, apiKey string, timeoutSec int) (bool, error) {
	found := false
	err := o.lifecycle.Do(id, func() error {
		sb, err := o.st.Get(ctx, id)
		if err != nil {
			return err
		}
		if !ownsSandbox(sb, apiKey) {
			return nil
		}
		found = true
		sb.DeadlineUnix = time.Now().Add(time.Duration(timeoutSec) * time.Second).Unix()
		if err := o.st.SetDeadline(ctx, id, sb.DeadlineUnix); err != nil {
			return err
		}
		state := string(clusterstate.WorkflowRouteReady)
		if sb.State == types.StatePaused {
			state = string(clusterstate.WorkflowRoutePaused)
		}
		if _, err = o.commitManagedSandboxState(ctx, sb, state, ""); err != nil {
			return err
		}
		o.cache(sb)
		return nil
	})
	return found, err
}

// resume restarts a paused sandbox from its snapshot (no api_key needed: the
// manifest key comes from the store via the config-socket).
func (o *Orchestrator) resume(ctx context.Context, sb *types.Sandbox) error {
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
	if o.cfg.Sandbox.TimeoutSec > 0 {
		nb.DeadlineUnix = time.Now().Add(time.Duration(o.cfg.Sandbox.TimeoutSec) * time.Second).Unix()
	}
	if err := o.launch(ctx, &nb, tmpl); err != nil {
		return err
	}
	if err := o.st.SetState(ctx, nb.ID, types.StateRunning); err != nil {
		return err
	}
	if o.cfg.Sandbox.TimeoutSec > 0 {
		_ = o.st.SetDeadline(ctx, nb.ID, nb.DeadlineUnix)
	}
	if _, err := o.commitManagedSandboxState(ctx, &nb, string(clusterstate.WorkflowRouteReady), ""); err != nil {
		return err
	}
	o.publishUpsert(&nb) // unparks any proxy holding a request for this sandbox
	return nil
}

func (o *Orchestrator) commitManagedSandboxState(ctx context.Context, sandbox *types.Sandbox, state, reason string) (bool, error) {
	if sandbox == nil || sandbox.ID == "" {
		return false, nil
	}
	workflow, err := o.st.GetNodeWorkflow(ctx, clusterstate.ExecutionKindSandbox, sandbox.ID)
	if err != nil || workflow == nil {
		return false, err
	}
	spec, err := clusterstate.ParseSandboxDispatchSpec(workflow.DispatchSpec)
	if err != nil {
		return true, err
	}
	var previous *clusterstate.SandboxPresentationV1
	if workflow.LatestEvent != nil {
		previous = workflow.LatestEvent.Presentation
	}
	presentation, err := o.sandboxPresentation(sandbox, previous)
	if err != nil {
		return true, err
	}
	_, err = o.st.CommitSandboxEvent(ctx, sandbox, nodeexec.EventUpdate{
		State: state, TargetPort: spec.TargetPort, SnapshotRef: sandbox.SnapshotRef,
		Reason: reason, Presentation: &presentation,
	})
	return true, err
}

func (o *Orchestrator) sandboxPresentation(
	sandbox *types.Sandbox,
	previous *clusterstate.SandboxPresentationV1,
) (clusterstate.SandboxPresentationV1, error) {
	if sandbox == nil || sandbox.ID == "" {
		return clusterstate.SandboxPresentationV1{}, errors.New("orch: Sandbox presentation requires an object")
	}
	cpuCount, memoryMB, diskSizeMB, err := o.sandboxLaunchResources(sandbox)
	if err != nil {
		if previous == nil || previous.Validate() != nil {
			return clusterstate.SandboxPresentationV1{}, err
		}
		cpuCount, memoryMB, diskSizeMB = previous.CPUCount, previous.MemoryMB, previous.DiskSizeMB
	}
	end := sandbox.DeadlineUnix
	if end < sandbox.CreatedUnix {
		end = sandbox.CreatedUnix
	}
	presentation := clusterstate.SandboxPresentationV1{
		CPUCount: cpuCount, MemoryMB: memoryMB, DiskSizeMB: diskSizeMB,
		EnvdVersion: api.EnvdVersion(sandbox.Profile()),
		StartedAt:   sandbox.CreatedUnix, EndAt: end,
		Metadata: clusterstate.WithoutSystemMetadata(sandbox.Metadata),
	}
	return presentation, presentation.Validate()
}

func (o *Orchestrator) sandboxLaunchResources(sandbox *types.Sandbox) (int, int, int, error) {
	launchConfig, err := rtconfig.Load(o.sandboxConfigPath(sandbox))
	if err != nil {
		return 0, 0, 0, fmt.Errorf("orch: load Sandbox launch resources: %w", err)
	}
	memoryBytes, err := util.ParseSize(launchConfig.Resources.Capacity.Memory)
	if err != nil || launchConfig.Resources.Capacity.CPU <= 0 || memoryBytes == 0 {
		return 0, 0, 0, errors.Join(err, errors.New("orch: invalid Sandbox launch capacity"))
	}
	diffPath := filepath.Join(sandbox.BaseDir, sandbox.ID+".overlay.diff")
	var diffURI, diffTemplateURI string
	if launchConfig.SingleDisk() {
		diffURI = launchConfig.Boot.Root.Diff
		diffTemplateURI = launchConfig.Boot.Root.DiffTemplate
	} else if launchConfig.Boot.Root.Overlay != nil {
		diffURI = launchConfig.Boot.Root.Overlay.Diff
		diffTemplateURI = launchConfig.Boot.Root.Overlay.DiffTemplate
	}
	if diffURI != "" {
		scheme, path, ok := rtconfig.SchemeAndPath(diffURI)
		if !ok || scheme != "file" {
			return 0, 0, 0, errors.New("orch: Sandbox launch diff is not a local file")
		}
		diffPath = path
	}
	diffInfo, err := os.Stat(diffPath)
	if err != nil || diffInfo.Size() <= 0 {
		diffErr := errors.Join(err, errors.New("orch: Sandbox launch diff is unavailable"))
		scheme, templatePath, ok := rtconfig.SchemeAndPath(diffTemplateURI)
		if !ok || scheme != "file" {
			return 0, 0, 0, diffErr
		}
		diffInfo, err = os.Stat(templatePath)
		if err != nil || diffInfo.Size() <= 0 {
			return 0, 0, 0, errors.Join(diffErr, err, errors.New("orch: Sandbox launch diff template is unavailable"))
		}
	}
	return launchConfig.Resources.Capacity.CPU, int(memoryBytes >> 20), int(diffInfo.Size() >> 20), nil
}

// --- proxy.Router (internal mode) ---

// Route resolves a (sid, port) for the in-process proxy. A paused sandbox is
// auto-resumed on the spot (single-flight: concurrent data-plane requests collapse
// to one resume). The forwarding decision is shared with the external route table
// via proxy.RouteForTarget.
func (o *Orchestrator) Route(ctx context.Context, request proxy.RouteRequest) (proxy.Route, error) {
	sb := o.lookup(request.SandboxID)
	if sb == nil {
		s, _ := o.st.Get(ctx, request.SandboxID)
		if s == nil {
			return proxy.Route{Kind: proxy.KindNotFound}, nil
		}
		sb = o.cacheIfAbsent(s)
	}
	if kind, failed := validateSandboxRouteFence(sb, request); failed {
		return proxy.Route{Kind: kind}, nil
	}
	if sb.State == types.StatePaused { // auto-resume on data-plane traffic
		if err := o.sf.Do(request.SandboxID, func() error {
			return o.lifecycle.Do(request.SandboxID, func() error {
				return o.resumeIfPaused(ctx, request.SandboxID, request)
			})
		}); err != nil && !errors.Is(err, errSandboxRouteFenceChanged) {
			return proxy.Route{}, err
		}
		if sb = o.lookup(request.SandboxID); sb == nil {
			return proxy.Route{Kind: proxy.KindNotFound}, nil
		}
		if kind, failed := validateSandboxRouteFence(sb, request); failed {
			return proxy.Route{Kind: kind}, nil
		}
	}
	if sb.State != types.StateRunning {
		return proxy.Route{Kind: proxy.KindRouteInactive}, nil
	}
	return proxy.RouteForTarget(string(sb.Profile()), sb.EnvdUDS, sb.CiUDS, sb.FloatingIP, sb.EnvdAccessToken, request.Port), nil
}

// resumeIfPaused (run under the per-sid single-flight) resumes sid only if it is
// still paused — a loser of the race finds it already running and returns.
func (o *Orchestrator) resumeIfPaused(ctx context.Context, sid string, request proxy.RouteRequest) error {
	sb := o.lookup(sid)
	if sb == nil {
		s, _ := o.st.Get(ctx, sid)
		if s == nil {
			return nil
		}
		sb = o.cacheIfAbsent(s)
	}
	if _, failed := validateSandboxRouteFence(sb, request); failed {
		return errSandboxRouteFenceChanged
	}
	if sb.State != types.StatePaused {
		return nil
	}
	return o.resume(ctx, sb)
}

var errSandboxRouteFenceChanged = fmt.Errorf("%w: execution Binding changed during resume", api.ErrConflict)

func validateSandboxRouteFence(sb *types.Sandbox, request proxy.RouteRequest) (proxy.Kind, bool) {
	managed, nodeID, nodeEpoch, registryGeneration, digest, err := sandboxRouteFence(sb)
	if err != nil {
		return proxy.KindWrongBinding, true
	}
	return proxy.RouteFenceFailure(request, managed, nodeID, nodeEpoch, registryGeneration, digest)
}

func sandboxRouteFence(sb *types.Sandbox) (bool, string, uint64, string, string, error) {
	if sb == nil {
		return false, "", 0, "", "", nil
	}
	opaque := sb.Metadata[clusterstate.ObjectMetadataKey]
	if opaque == "" {
		return false, "", 0, "", "", nil
	}
	binding, err := clusterstate.DecodeExecutionBinding(opaque)
	if err != nil {
		return true, "", 0, "", "", err
	}
	if binding.Kind != clusterstate.ExecutionKindSandbox || binding.ObjectID != sb.ID {
		return true, "", 0, "", "", fmt.Errorf("cluster: execution binding does not identify sandbox %q", sb.ID)
	}
	digest, err := clusterstate.ExecutionBindingDigest(opaque)
	if err != nil {
		return true, "", 0, "", "", err
	}
	return true, binding.NodeID, binding.NodeEpoch, binding.RegistryGeneration, digest, nil
}

func currentSandboxRouteRequest(sb *types.Sandbox, port int) (proxy.RouteRequest, error) {
	request := proxy.RouteRequest{Port: port}
	if sb != nil {
		request.SandboxID = sb.ID
	}
	managed, nodeID, nodeEpoch, registryGeneration, digest, err := sandboxRouteFence(sb)
	if err != nil {
		return proxy.RouteRequest{}, err
	}
	if managed {
		request.ExpectedNodeID = nodeID
		request.ExpectedNodeEpoch = nodeEpoch
		request.ExpectedRegistryGeneration = registryGeneration
		request.ExpectedBindingDigest = digest
	}
	return request, nil
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

// snapshotConfig reads resources.capacity + the kuasar-sandbox.network metadata from a
// snapshot ref's embedded snapshot.cfg (`sandbox-ctl info --json`; reads only the
// trailing ZIP, a few KB even via manifest://). Best-effort: any probe/parse failure
// yields a zero snapInfo and the launch proceeds with node defaults (the runtime
// stays the capacity enforcer).
func (o *Orchestrator) snapshotConfig(ctx context.Context, sb *types.Sandbox, ref string) snapInfo {
	info, err := o.readSnapshotConfig(ctx, sb, ref)
	if err != nil {
		o.log.Warn("snapshot config probe failed; using node defaults", "sid", sb.ID, "ref", ref, "err", err)
		return snapInfo{}
	}
	return info
}

// readSnapshotConfig is the strict form used before node-local Admission. A
// restore must not reserve resources until its frozen capacity is known.
func (o *Orchestrator) readSnapshotConfig(ctx context.Context, sb *types.Sandbox, ref string) (snapInfo, error) {
	var info snapInfo
	args := []string{"info", "--json"}
	if strings.HasPrefix(ref, "manifest://") {
		args = append(args, "--manifest-config", o.cfg.ManifestConfig)
	}
	cmd := exec.CommandContext(ctx, o.cfg.SandboxCtl(), append(args, ref)...)
	cmd.Env = append(os.Environ(), "MANIFEST_KEY="+sb.ManifestKey)
	out, err := cmd.Output()
	if err != nil {
		return info, fmt.Errorf("probe snapshot config: %w", err)
	}
	// info --json marshals restore.SnapshotCfg by Go field name (capitalized).
	var cfg struct {
		Resources struct {
			Capacity struct {
				CPU    int    `json:"CPU"`
				Memory string `json:"Memory"`
			} `json:"Capacity"`
		} `json:"Resources"`
		Metadata map[string]string `json:"Metadata"`
	}
	if err := json.Unmarshal(out, &cfg); err != nil {
		return info, fmt.Errorf("parse snapshot config: %w", err)
	}
	if cfg.Resources.Capacity.CPU > 0 && cfg.Resources.Capacity.Memory != "" {
		info.CapCPU, info.CapMem, info.HasCapacity = cfg.Resources.Capacity.CPU, cfg.Resources.Capacity.Memory, true
	}
	if nraw := strings.TrimSpace(cfg.Metadata[sandboxcfg.NsNetwork]); nraw != "" {
		_ = json.Unmarshal([]byte(nraw), &info.Network) // best-effort; malformed -> zero network
	}
	return info, nil
}

// --- configsock.Provider ---

// sandboxConfigPath is where the per-sandbox SANDBOX_CONFIG yaml is written.
func (o *Orchestrator) sandboxConfigPath(sb *types.Sandbox) string {
	return sb.RunDir + "/" + sb.ID + ".yaml"
}

func (o *Orchestrator) sandboxParams(sb *types.Sandbox, tmpl types.TemplateID, spec sandboxcfg.SandboxSpec) sandboxcfg.Params {
	return sandboxcfg.Params{
		Sandbox: sb, Template: tmpl,
		Runtime:        o.cfg.Sandbox.Boot.Runtime,
		Kernel:         o.cfg.Sandbox.Boot.Kernel,
		OverlayDiffTpl: o.cfg.Sandbox.Boot.OverlayDiffTemplate,
		TapFD:          sandboxTapFD(o.vs.TapFD(sb.VswitchPort)), EnvVars: sb.Env,
		VCPU: o.cfg.Sandbox.Resources.VCPU, Memory: o.cfg.Sandbox.Resources.Memory, ControllerSocket: o.cfg.Sandbox.Resources.ControlSocket,
		Network:     o.resolveNetwork(sb, tmpl, spec.Network),
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

// resolveNetwork merges the tenant network override with profile/node defaults into
// the resolved logical network used both for the guest config and for the
// snapshot-borne metadata (kuasar-sandbox.network). inner_ip is the assigned CIDR
// (set at attach). On restore the caller fills missing fields from the snapshot
// before this (Stage 3); here a tenant value still wins over the node default.
func (o *Orchestrator) resolveNetwork(sb *types.Sandbox, tmpl types.TemplateID, ov sandboxcfg.NetworkSpec) sandboxcfg.NetworkSpec {
	dns := o.cfg.Sandbox.Network.DNS
	if len(ov.DNS) > 0 {
		dns = ov.DNS
	}
	return sandboxcfg.NetworkSpec{
		Hostname:         firstNonEmpty(ov.Hostname, o.cfg.Sandbox.Network.Hostname),
		DNS:              dns,
		InnerIP:          sb.InnerIP, // assigned CIDR (vswitch attach)
		Nexthop:          firstNonEmpty(ov.Nexthop, o.innerGateway(tmpl.Profile)),
		TransitGatewayIP: ov.TransitGatewayIP,
		TransitGeneveVNI: ov.TransitGeneveVNI,
		TransitMAC:       ov.TransitMAC,
	}
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
	p := o.sandboxParams(sb, tmpl, cfgSpec)
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
	if r := p.RestoreRef(); r != "" {
		args = append(args, "--restore", r)
		if o.cfg.Sandbox.Restore.FileRefs == config.RestoreFileRefsTrust {
			args = append(args, "--restore-file-refs", config.RestoreFileRefsTrust)
		}
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
			if n, err := o.st.PruneExpiredKeyLeases(ctx); err != nil {
				o.log.Warn("reaper prune key leases", "err", err)
			} else if n > 0 {
				o.log.Info("reaper pruned expired key leases", "n", n)
			}
		}
	}
}

// Reconcile adopts/cleans sandboxes after an orchestrator restart, using the
// systemd unit set as the liveness authority.
func (o *Orchestrator) Reconcile(ctx context.Context) error {
	builders, err := o.lc.List(ctx, o.builderPattern())
	if err != nil {
		return err
	}
	for _, unit := range builders {
		if !strings.HasPrefix(unit.Name, strings.TrimSuffix(o.cfg.Units.Builder, ".service")) {
			continue
		}
		if unit.ActiveState == "active" || unit.ActiveState == "activating" || unit.ActiveState == "failed" {
			o.log.Info("reconcile: orphan builder", "unit", unit.Name)
			_ = o.lc.Stop(ctx, unit.Name)
			_ = o.lc.ResetFailed(ctx, unit.Name)
		}
	}
	building, err := o.st.BuildsByStatus(ctx, types.BuildBuilding)
	if err != nil {
		return err
	}
	for _, build := range building {
		workflow, err := o.st.GetNodeWorkflow(ctx, clusterstate.ExecutionKindBuild, build.BuildID)
		if err != nil {
			return err
		}
		if workflow == nil {
			_, _ = o.st.CASBuildStatus(ctx, build.BuildID, types.BuildBuilding, types.BuildWaiting)
		}
	}

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

// cacheIfAbsent publishes a store read only while no newer in-memory snapshot
// exists. A delayed reader must not replace a running snapshot with the paused
// row it loaded before a concurrent resume committed.
func (o *Orchestrator) cacheIfAbsent(sb *types.Sandbox) *types.Sandbox {
	o.mu.Lock()
	defer o.mu.Unlock()
	if current := o.reg[sb.ID]; current != nil {
		return current
	}
	o.reg[sb.ID] = sb
	return sb
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

// snapshot pauses+captures the running sandbox via sandbox-ctl and returns the
// snapshot manifest key. The client dials <run-root>/<sid>/ctl.sock; the running
// snapshot captures sb per the configured checkpoint mode and returns the restore
// ref to persist: "manifest://<key>" (remote — portable) or a local bundle path
// (local — node-bound; the default). sandbox-ctl performs the work with its own
// boot-time manifest config; we pass the resolved binary + the run root.
func (o *Orchestrator) snapshot(ctx context.Context, sb *types.Sandbox) (string, error) {
	if o.cfg.Checkpoint.Mode == config.CheckpointLocal {
		return o.snapshotLocal(ctx, sb)
	}
	key, err := o.snapshotRemote(ctx, sb)
	if err != nil {
		return "", err
	}
	return "manifest://" + key, nil
}

// snapshotRemote uploads the snapshot to the manifest store; stdout is the bare
// 64-hex manifest key. Used for remote checkpoints and (always) template builds.
func (o *Orchestrator) snapshotRemote(ctx context.Context, sb *types.Sandbox) (string, error) {
	cmd := exec.CommandContext(ctx, o.cfg.SandboxCtl(), "snapshot",
		"--sandbox-id", sb.ID, "--upload", "--run-root", o.cfg.Paths.RunRoot)
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("orch: snapshot %s: %w: %s", sb.ID, err, errb.String())
	}
	return strings.TrimSpace(out.String()), nil
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

// promote uploads a LOCAL checkpoint bundle to the manifest store WITHOUT booting
// (sandbox-ctl upload-snapshot), returning "manifest://<key>". The tenant key
// rides in MANIFEST_KEY; the base lower chain must already be remote. Used by
// export-sandbox to make a node-bound checkpoint portable.
func (o *Orchestrator) promote(ctx context.Context, sb *types.Sandbox, localPath string) (string, error) {
	// Flags before the positional: sandbox-ctl upload-snapshot parses with Go's flag,
	// which stops at the first positional — a leading <path> would drop --manifest-config.
	cmd := exec.CommandContext(ctx, o.cfg.SandboxCtl(), "upload-snapshot",
		"--manifest-config", o.cfg.ManifestConfig, "--quiet", localPath)
	cmd.Env = append(os.Environ(), "MANIFEST_KEY="+sb.ManifestKey)
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("orch: promote %s: %w: %s", sb.ID, err, errb.String())
	}
	return strings.TrimSpace(out.String()), nil // manifest://<key>
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

// allocInnerIP returns the guest's inner IP (plain, for vswitch attach) and its CIDR
// (for Network.IP). override (the per-instance inner_ip, "" = none) wins over the
// profile default: e2b 169.254.0.21/30 (envd port-forward needs the /30 + gateway),
// bare 169.254.1.1/31. The inner IP is fixed per profile — every sandbox reuses it;
// identity is the per-slot floating IP, and the eBPF datapath keys on slot/ifindex.
func (o *Orchestrator) allocInnerIP(profile types.Profile, override string) (plain, cidr string, err error) {
	cidr = firstNonEmpty(override, o.profileNet(profile).InnerIP)
	ip, _, err := net.ParseCIDR(cidr)
	if err != nil {
		return "", "", fmt.Errorf("orch: sandbox inner_ip %q: %w", cidr, err)
	}
	return ip.String(), cidr, nil
}

// innerGateway returns the guest's default-route next-hop for the profile. The
// vswitch ARP-proxies it, so the guest reaches everything off its subnet through it
// (the proxy/floatingip reply path + egress via host NAT).
func (o *Orchestrator) innerGateway(profile types.Profile) string {
	return o.profileNet(profile).Nexthop
}
