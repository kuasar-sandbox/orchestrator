// Package orch is the orchestrator core. It ties together the store, systemd
// launcher, vswitch and config generation, and implements api.Core (control
// plane), proxy.Router (data plane) and configsock.Provider (dynamic config).
package orch

import (
	"bytes"
	"context"
	"encoding/json"
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

	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/api"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/config"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/filestore"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/keys"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/launcher"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/proxy"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/store"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/types"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/vswitch"
)

type Orchestrator struct {
	cfg *config.Config
	st  *store.Store
	lc  launcher.Launcher
	vs  *vswitch.CLI
	log *slog.Logger

	mu  sync.Mutex
	reg map[string]*types.Sandbox // in-memory cache (hot path: Route/LaunchSpecFor)

	sf flightGroup // per-sid single-flight for resume (dedup concurrent data-plane wakeups)

	subsMu sync.Mutex
	subs   map[int]chan routesync.Event // route-change subscribers (routesync clients)
	subSeq int

	pendMu sync.Mutex
	pend   map[string]*pendingBuild // builds whose unit is running (BuildSpecFor source)

	files *filestore.Store // COPY build-context object store; nil = unconfigured (COPY → 501)
}

func New(cfg *config.Config, st *store.Store, lc launcher.Launcher, vs *vswitch.CLI, log *slog.Logger) *Orchestrator {
	o := &Orchestrator{
		cfg: cfg, st: st, lc: lc, vs: vs, log: log,
		reg:  map[string]*types.Sandbox{},
		subs: map[int]chan routesync.Event{},
		pend: map[string]*pendingBuild{},
	}
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

// --- api.Core ---

func (o *Orchestrator) Create(ctx context.Context, req api.CreateReq) (*types.Sandbox, error) {
	if req.TimeoutSec <= 0 {
		req.TimeoutSec = o.cfg.Sandbox.TimeoutSec // default TTL (sandbox.timeout_sec)
	}
	manifestKey, err := o.resolveAllowed(ctx, req.APIKey)
	if err != nil {
		return nil, err
	}
	if manifestKey == "" {
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

	sb := &types.Sandbox{
		ID:                 sid,
		TemplateID:         tmpl.String(), // canonical persist id (resolved from a transient/alias ref)
		State:              types.StateRunning,
		RunDir:             o.cfg.Paths.RunRoot + "/" + sid,
		BaseDir:            o.cfg.Paths.BaseRoot + "/" + sid,
		ManifestKey:        manifestKey,
		EnvdAccessToken:    envdTok,
		TrafficAccessToken: trafTok,
		Metadata:           req.Metadata,
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
// (orchestrator-ctl run-sandbox -> sandbox-ctl), waits for readiness and provisions
// envd. The non-secret config lands at <run-dir>/<sid>.yaml; the secret manifest
// key rides in the run-sandbox LaunchSpec env (LaunchSpecFor). The cgroup is the
// unit's own (--cgroup-adopt). Used by Create and Connect(resume).
func (o *Orchestrator) launch(ctx context.Context, sb *types.Sandbox, tmpl types.TemplateID) error {
	for _, d := range []string{sb.RunDir, sb.BaseDir} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			return fmt.Errorf("orch: mkdir %s: %w", d, err)
		}
	}
	ov, err := parseOverrides(sb.Metadata)
	if err != nil {
		return err
	}
	plainIP, cidrIP, err := o.allocInnerIP(tmpl.Profile, ov.InnerIP)
	if err != nil {
		return err
	}
	port, err := o.vs.Attach(ctx, vswitch.AttachReq{
		InnerIP:          plainIP,
		TransitGatewayIP: ov.TransitGatewayIP,
		TransitGeneveVNI: ov.TransitGeneveVNI,
		TransitMAC:       ov.TransitMAC,
	})
	if err != nil {
		return err
	}
	sb.VswitchPort, sb.FloatingIP, sb.PortMAC, sb.InnerIP = port.Port, port.FloatingIP, port.MAC, cidrIP

	p := o.sandboxParams(sb, tmpl, ov)
	// A restore must declare the exact capacity the snapshot froze —
	// snapshot.cfg is authoritative and the runtime refuses a mismatch
	// rather than resize a resumed VM. Template snapshots are self-
	// describing and may have been taken at a different budget than this
	// node's create defaults (e.g. the build pipeline's builder.vcpu/memory).
	if ref := p.RestoreRef(); ref != "" {
		if cpu, mem, ok := o.snapshotCapacity(ctx, sb, ref); ok {
			p.VCPU, p.Memory = cpu, mem
		}
	}
	if err := p.WriteYAML(o.sandboxConfigPath(sb)); err != nil {
		return err
	}
	if err := o.st.Put(ctx, sb); err != nil {
		return err
	}
	o.cache(sb)

	if err := o.lc.Start(ctx, o.runnerUnit(sb.ID)); err != nil {
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
		if verifyKey(apiKey, sb.ManifestKey) {
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
	sb, err := o.st.Get(ctx, id)
	if err != nil {
		return err
	}
	if !ownsSandbox(sb, apiKey) {
		return api.ErrNotFound
	}
	return o.pauseSandbox(ctx, sb)
}

// pauseSandbox snapshots a running sandbox and stops it — the work behind Pause
// and the reaper's auto-suspend (no api key: the caller has already authorized).
func (o *Orchestrator) pauseSandbox(ctx context.Context, sb *types.Sandbox) error {
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
	_ = o.lc.Stop(ctx, o.runnerUnit(sb.ID))
	_ = o.lc.ResetFailed(ctx, o.runnerUnit(sb.ID))
	_ = o.vs.Detach(ctx, sb.VswitchPort)
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
		if imported != id {
			_ = o.st.Delete(ctx, imported) // token was for a different sandbox; don't leave it
			return nil, fmt.Errorf("connect %s: migration token is for sandbox %s", id, imported)
		}
		if sb, err = o.st.Get(ctx, id); err != nil {
			return nil, err
		}
	}
	if !ownsSandbox(sb, apiKey) {
		return nil, api.ErrNotFound
	}
	if sb.State == types.StatePaused {
		if err := o.resume(ctx, sb); err != nil {
			return nil, err
		}
	}
	if timeoutSec > 0 {
		sb.DeadlineUnix = time.Now().Add(time.Duration(timeoutSec) * time.Second).Unix()
		_ = o.st.SetDeadline(ctx, id, sb.DeadlineUnix)
	}
	return sb, nil
}

func (o *Orchestrator) SetTimeout(ctx context.Context, id, apiKey string, timeoutSec int) (bool, error) {
	sb, err := o.st.Get(ctx, id)
	if err != nil {
		return false, err
	}
	if !ownsSandbox(sb, apiKey) {
		return false, nil
	}
	sb.DeadlineUnix = time.Now().Add(time.Duration(timeoutSec) * time.Second).Unix()
	return true, o.st.SetDeadline(ctx, id, sb.DeadlineUnix)
}

// resume restarts a paused sandbox from its snapshot (no api_key needed: the
// manifest key comes from the store via the config-socket).
func (o *Orchestrator) resume(ctx context.Context, sb *types.Sandbox) error {
	tmpl, err := types.ParseTemplateID(sb.TemplateID)
	if err != nil {
		return err
	}
	sb.State = types.StateRunning
	if err := o.launch(ctx, sb, tmpl); err != nil {
		return err
	}
	if err := o.st.SetState(ctx, sb.ID, types.StateRunning); err != nil {
		return err
	}
	o.publishUpsert(sb) // unparks any proxy holding a request for this sandbox
	return nil
}

// --- proxy.Router (internal mode) ---

// Route resolves a (sid, port) for the in-process proxy. A paused sandbox is
// auto-resumed on the spot (single-flight: concurrent data-plane requests collapse
// to one resume). The forwarding decision is shared with the external route table
// via proxy.RouteForTarget.
func (o *Orchestrator) Route(ctx context.Context, sandboxID string, port int) (proxy.Route, error) {
	sb := o.lookup(sandboxID)
	if sb == nil {
		s, _ := o.st.Get(ctx, sandboxID)
		if s == nil {
			return proxy.Route{Kind: proxy.KindNotFound}, nil
		}
		sb = s
		o.cache(sb)
	}
	if sb.State == types.StatePaused { // auto-resume on data-plane traffic
		if err := o.sf.Do(sandboxID, func() error { return o.resumeIfPaused(ctx, sandboxID) }); err != nil {
			return proxy.Route{}, err
		}
		if sb = o.lookup(sandboxID); sb == nil {
			return proxy.Route{Kind: proxy.KindNotFound}, nil
		}
	}
	return proxy.RouteForTarget(string(sb.Profile()), sb.EnvdUDS, sb.CiUDS, sb.FloatingIP, sb.EnvdAccessToken, port), nil
}

// resumeIfPaused (run under the per-sid single-flight) resumes sid only if it is
// still paused — a loser of the race finds it already running and returns.
func (o *Orchestrator) resumeIfPaused(ctx context.Context, sid string) error {
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
	return o.resume(ctx, sb)
}

// snapshotCapacity reads resources.capacity from a snapshot ref's embedded
// snapshot.cfg (`sandbox-ctl info --json`; reads only the trailing ZIP, a few
// KB even via manifest://). Returns ok=false on any failure — the launch then
// proceeds with the node defaults and the runtime stays the enforcer.
func (o *Orchestrator) snapshotCapacity(ctx context.Context, sb *types.Sandbox, ref string) (int, string, bool) {
	args := []string{"info", "--json"}
	if strings.HasPrefix(ref, "manifest://") {
		args = append(args, "--manifest-config", o.cfg.ManifestConfig)
	}
	cmd := exec.CommandContext(ctx, o.cfg.SandboxCtl(), append(args, ref)...)
	cmd.Env = append(os.Environ(), "MANIFEST_KEY="+sb.ManifestKey)
	out, err := cmd.Output()
	if err != nil {
		o.log.Warn("snapshot capacity probe failed; using node defaults", "sid", sb.ID, "ref", ref, "err", err)
		return 0, "", false
	}
	var cfg struct {
		Resources struct {
			Capacity struct {
				CPU    int    `json:"CPU"`
				Memory string `json:"Memory"`
			} `json:"Capacity"`
		} `json:"Resources"`
	}
	if err := json.Unmarshal(out, &cfg); err != nil ||
		cfg.Resources.Capacity.CPU <= 0 || cfg.Resources.Capacity.Memory == "" {
		o.log.Warn("snapshot capacity parse failed; using node defaults", "sid", sb.ID, "ref", ref, "err", err)
		return 0, "", false
	}
	return cfg.Resources.Capacity.CPU, cfg.Resources.Capacity.Memory, true
}

// --- configsock.Provider ---

// sandboxConfigPath is where the per-sandbox SANDBOX_CONFIG yaml is written.
func (o *Orchestrator) sandboxConfigPath(sb *types.Sandbox) string {
	return sb.RunDir + "/" + sb.ID + ".yaml"
}

func (o *Orchestrator) sandboxParams(sb *types.Sandbox, tmpl types.TemplateID, ov sandboxOverride) sandboxcfg.Params {
	dns := o.cfg.Sandbox.Network.DNS
	if len(ov.DNS) > 0 {
		dns = ov.DNS
	}
	return sandboxcfg.Params{
		Sandbox: sb, Template: tmpl,
		RuntimeE2B: o.cfg.Sandbox.Boot.RuntimeE2B, RuntimeBase: o.cfg.Sandbox.Boot.RuntimeBase, Kernel: o.cfg.Sandbox.Boot.Kernel,
		OverlayDiffTpl: o.cfg.Sandbox.Boot.OverlayDiffTemplate,
		TapFDExec:      o.vs.TapFDExec(sb.VswitchPort), EnvVars: sb.Env,
		VCPU: o.cfg.Sandbox.Resources.VCPU, Memory: o.cfg.Sandbox.Resources.Memory, ControllerSocket: o.cfg.Sandbox.Resources.ControlSocket,
		Nexthop:     firstNonEmpty(ov.Nexthop, o.innerGateway(tmpl.Profile)),
		Hostname:    firstNonEmpty(ov.Hostname, o.cfg.Sandbox.Network.Hostname),
		DNS:         dns,
		MMDSEnabled: o.cfg.MMDS.Enabled,
	}
}

// LaunchSpecFor resolves a launcher config-id ("sandbox:<sid>" | "build:<bid>")
// to the LaunchSpec the launcher exec-replaces into. The secret manifest key rides in
// LaunchSpec.Env; the bulky non-secret config is the file referenced by the args.
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
	ov, err := parseOverrides(sb.Metadata)
	if err != nil {
		return nil, "", false, err
	}
	p := o.sandboxParams(sb, tmpl, ov)
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
		// Route the sandbox's stdio + kernel dmesg to journald under this runner
		// unit (sandbox-runner@<sid>): app stdout/stderr tagged "sandbox", guest
		// dmesg tagged "console" — host-only telemetry, queryable per-sandbox via
		// `journalctl -u sandbox-runner@<sid>.service [SYSLOG_IDENTIFIER=…]`.
		// sandbox-ctl exec-replaces run-sandbox into this unit's cgroup, so
		// journald stamps the right _SYSTEMD_UNIT automatically.
		"--stdout-to", "journald=" + configsock.RunnerLogTag,
		"--stderr-to", "journald=" + configsock.RunnerLogTag,
		"--console", "journald=" + configsock.ConsoleTag,
	}
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
		Env:     map[string]string{"MANIFEST_KEY": sb.ManifestKey},
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
			running, _ := o.st.ListByState(ctx, types.StateRunning)
			now := time.Now().Unix()
			for _, sb := range running {
				if sb.DeadlineUnix > 0 && now >= sb.DeadlineUnix {
					if err := o.pauseSandbox(ctx, sb); err != nil {
						o.log.Warn("reaper pause", "sid", sb.ID, "err", err)
					}
				}
			}
			if n, err := o.st.PruneExpiredManifestKeys(ctx); err != nil {
				o.log.Warn("reaper prune manifest keys", "err", err)
			} else if n > 0 {
				o.log.Info("reaper pruned expired manifest keys", "n", n)
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
			alive[o.unitToSID(u.Name)] = true
		}
	}
	running, _ := o.st.ListByState(ctx, types.StateRunning)
	for _, sb := range running {
		if alive[sb.ID] {
			o.cache(sb) // re-adopt: route + TTL already in store
			continue
		}
		o.log.Info("reconcile: dead sandbox", "sid", sb.ID)
		o.teardown(ctx, sb)
		_ = o.st.SetState(ctx, sb.ID, types.StateDead)
	}
	return nil
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

func (o *Orchestrator) teardown(ctx context.Context, sb *types.Sandbox) {
	// The sandbox runs in its systemd unit's own cgroup (sandbox-ctl --cgroup-adopt),
	// and the unit is KillMode=control-group, so StopUnit SIGKILLs every straggler
	// (cloud-hypervisor included). No separate cgroup drain/rmdir is needed.
	_ = o.lc.Stop(ctx, o.runnerUnit(sb.ID))
	_ = o.lc.ResetFailed(ctx, o.runnerUnit(sb.ID))
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
