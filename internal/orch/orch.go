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
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/api"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/config"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/configsock"
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
}

func New(cfg *config.Config, st *store.Store, lc launcher.Launcher, vs *vswitch.CLI, log *slog.Logger) *Orchestrator {
	return &Orchestrator{
		cfg: cfg, st: st, lc: lc, vs: vs, log: log,
		reg:  map[string]*types.Sandbox{},
		subs: map[int]chan routesync.Event{},
	}
}

// --- api.Core ---

func (o *Orchestrator) Create(ctx context.Context, req api.CreateReq) (*types.Sandbox, error) {
	manifestKey, err := o.resolveAllowed(ctx, req.APIKey)
	if err != nil {
		return nil, err
	}
	if manifestKey == "" {
		return nil, api.ErrNotAllowed
	}
	tmpl, err := types.ParseTemplateID(req.TemplateID)
	if err != nil {
		return nil, err
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
		TemplateID:         req.TemplateID,
		State:              types.StateRunning,
		RunDir:             o.cfg.RunRoot + "/" + sid,
		BaseDir:            o.cfg.BaseRoot + "/" + sid,
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
// (orchestrator-ctl run-task -> sandbox-ctl), waits for readiness and provisions
// envd. The non-secret config lands at <run-dir>/<sid>.yaml; the secret manifest
// key rides in the run-task LaunchSpec env (LaunchSpecFor). The cgroup is the
// unit's own (--cgroup-adopt). Used by Create and Connect(resume).
func (o *Orchestrator) launch(ctx context.Context, sb *types.Sandbox, tmpl types.TemplateID) error {
	for _, d := range []string{sb.RunDir, sb.BaseDir} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			return fmt.Errorf("orch: mkdir %s: %w", d, err)
		}
	}
	plainIP, cidrIP, err := o.allocInnerIP(ctx)
	if err != nil {
		return err
	}
	port, err := o.vs.Attach(ctx, plainIP)
	if err != nil {
		return err
	}
	sb.VswitchPort, sb.FloatingIP, sb.PortMAC, sb.InnerIP = port.Port, port.FloatingIP, port.MAC, cidrIP

	if err := o.sandboxParams(sb, tmpl).WriteYAML(o.sandboxConfigPath(sb)); err != nil {
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
		if err := o.waitReady(ctx, sb, 30*time.Second); err != nil {
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

func (o *Orchestrator) Connect(ctx context.Context, id, apiKey string, timeoutSec int) (*types.Sandbox, error) {
	sb, err := o.st.Get(ctx, id)
	if err != nil {
		return nil, err
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

// --- configsock.Provider ---

// sandboxConfigPath is where the per-sandbox SANDBOX_CONFIG yaml is written.
func (o *Orchestrator) sandboxConfigPath(sb *types.Sandbox) string {
	return sb.RunDir + "/" + sb.ID + ".yaml"
}

func (o *Orchestrator) sandboxParams(sb *types.Sandbox, tmpl types.TemplateID) sandboxcfg.Params {
	return sandboxcfg.Params{
		Sandbox: sb, Template: tmpl,
		RuntimeE2B: o.cfg.RuntimeE2B, RuntimeBase: o.cfg.RuntimeBase, Kernel: o.cfg.Kernel,
		OverlayDiffTpl: o.cfg.OverlayDiffTemplate,
		TapFDExec:      o.vs.TapFDExec(sb.VswitchPort), EnvVars: sb.Env,
		VCPU: o.cfg.DefaultVCPU, Memory: o.cfg.DefaultMemory, ControllerSocket: o.cfg.ResourceSocket,
	}
}

// LaunchSpecFor resolves a run-task config-id ("sandbox:<sid>" | "build:<bid>")
// to the LaunchSpec run-task exec-replaces into. The secret manifest key rides in
// LaunchSpec.Env; the bulky non-secret config is the file referenced by the args.
func (o *Orchestrator) LaunchSpecFor(ctx context.Context, configID string) (*configsock.LaunchSpec, string, bool, error) {
	kind, id, found := strings.Cut(configID, ":")
	if !found {
		return nil, "", false, nil
	}
	switch kind {
	case "sandbox":
		return o.sandboxLaunchSpec(ctx, id)
	case "build":
		return o.buildLaunchSpec(ctx, id)
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
	p := o.sandboxParams(sb, tmpl)
	// --run-root pins sandbox-ctl's socket/staging dir (ch.sock, ctl.sock, …) to the
	// orchestrator's run root, so RunDir == cfg.RunRoot/<sid> and the snapshot client
	// (also --run-root cfg.RunRoot) finds ctl.sock. Without it sandbox-ctl defaults to
	// /run/sandbox, splitting the dirs (snapshot/pause then can't reach ctl.sock).
	args := []string{
		"run", "--sandbox-id", sb.ID,
		"--config", o.sandboxConfigPath(sb),
		"--manifest-config", o.cfg.ManifestCfg,
		"--run-root", o.cfg.RunRoot,
		"--cgroup-adopt",
	}
	if r := p.RestoreRef(); r != "" {
		args = append(args, "--restore", r)
	}
	for _, c := range p.ConnectSpecs() {
		args = append(args, "--connect", c)
	}
	spec := &configsock.LaunchSpec{
		Exec:    o.cfg.Bin(o.cfg.SandboxCtl),
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
// sandbox-ctl performs the manifest-store upload with its own boot-time config,
// so the client only needs the resolved binary + the run root (not the default
// /run/sandbox). stdout is the snapshot manifest key.
func (o *Orchestrator) snapshot(ctx context.Context, sb *types.Sandbox) (string, error) {
	cmd := exec.CommandContext(ctx, o.cfg.Bin(o.cfg.SandboxCtl), "snapshot",
		"--sandbox-id", sb.ID, "--upload", "--run-root", o.cfg.RunRoot)
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("orch: snapshot %s: %w: %s", sb.ID, err, errb.String())
	}
	return strings.TrimSpace(out.String()), nil
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

// envdInit provisions envd after boot/restore: sets the access token (first /init
// under -isnotfc accepts it without MMDS), env vars, default user/workdir, time.
// Body keys per envd spec (camelCase); success = 204.
func (o *Orchestrator) envdInit(ctx context.Context, sb *types.Sandbox) error {
	payload := map[string]any{
		"accessToken":    sb.EnvdAccessToken,
		"envVars":        sb.Env,
		"defaultUser":    "user",
		"defaultWorkdir": "/home/user",
		"timestamp":      time.Now().UTC().Format(time.RFC3339),
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

// allocInnerIP picks the next free guest inner IP in cfg.InnerCIDR (skipping the
// network address and .1). Returns the plain IP (for vswitch attach) and the
// CIDR form (for Network.IP).
func (o *Orchestrator) allocInnerIP(ctx context.Context) (plain, cidr string, err error) {
	_, ipnet, err := net.ParseCIDR(o.cfg.InnerCIDR)
	if err != nil {
		return "", "", fmt.Errorf("orch: inner_cidr %q: %w", o.cfg.InnerCIDR, err)
	}
	used := map[string]bool{}
	for _, st := range []types.State{types.StateRunning, types.StatePaused} {
		list, _ := o.st.ListByState(ctx, st)
		for _, sb := range list {
			if sb.InnerIP == "" {
				continue
			}
			if ip, _, e := net.ParseCIDR(sb.InnerIP); e == nil {
				used[ip.String()] = true
			}
		}
	}
	ones, _ := ipnet.Mask.Size()
	ip := make(net.IP, len(ipnet.IP))
	copy(ip, ipnet.IP)
	incIP(ip)
	incIP(ip) // skip network + .1 (gateway)
	for ipnet.Contains(ip) {
		if !used[ip.String()] {
			return ip.String(), fmt.Sprintf("%s/%d", ip.String(), ones), nil
		}
		incIP(ip)
	}
	return "", "", fmt.Errorf("orch: inner_cidr %s exhausted", o.cfg.InnerCIDR)
}

func incIP(ip net.IP) {
	for j := len(ip) - 1; j >= 0; j-- {
		ip[j]++
		if ip[j] != 0 {
			break
		}
	}
}
