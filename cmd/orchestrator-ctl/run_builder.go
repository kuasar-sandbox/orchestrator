package main

// run-builder is the ExecStart of sandbox-builder@<bid>.service: the build
// pipeline orchestrator. It fetches the BuildSpec over the config-socket and
// drives up to three phases, each a microVM it spawns as a DIRECT child
// (sandbox-ctl run, in this unit's cgroup), reusing ONE pre-attached network
// slot sequentially:
//
//	A import   — an EMPTY single-disk sandbox on the builder runtime flavor
//	             (flatten-ctl/mkfs.erofs ride /opt/sandbox-runtime, bind-
//	             mounted into any rootfs); flatten-ctl pulls the image WITH
//	             TENANT creds (exec env, never host-side) over the tenant
//	             network, flattens, and streams the tarstream image artifact
//	             back over exec stdio.
//	B steps    — an e2b-shaped sandbox whose root is the (local or template)
//	             base image; RUN steps execute via sandbox-ctl exec with the
//	             accumulated ENV/WORKDIR/USER context (ARG substitutes only);
//	             then flatten-ctl exports the rootfs (its tmpdir/output home
//	             is a self-bind mountpoint, excluded by --skip-mounts) and
//	             streams the new image artifact back.
//	C template — a PRODUCTION-runtime sandbox cold-booted from the final
//	             image (the runtime ref freezes into the snapshot — template
//	             children must not inherit the builder toolchain); startCmd
//	             launches detached in-guest, readyCmd polls to success, then
//	             sandbox-ctl snapshot writes the local bundle.
//
// The finale uploads what was produced — platform credentials appear ONLY
// here: an image-only build runs `manifest-ctl store image.img`; a snapshot
// build runs ONE `sandbox-ctl upload-snapshot` (it auto-uploads every local
// artifact the snapshot.cfg references, the base image included, and
// rewrites the refs to manifest://). The result JSON goes to stdout, which
// the unit captures to <bid>.result for the orchestrator.

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/configsock"
	"gopkg.in/yaml.v3"
)

func runBuilder(args []string, log *slog.Logger) error {
	fs := flag.NewFlagSet("run-builder", flag.ExitOnError)
	pidfile := fs.String("pidfile", "", "pidfile to lock+write (TASK_PIDFILE)")
	socket := fs.String("config-socket", "", "config-socket UDS (TASK_CONFIG_SOCKET)")
	bid := fs.String("build-id", "", "build id (TASK_BUILD_ID)")
	_ = fs.Parse(args)
	envDefault(pidfile, "TASK_PIDFILE")
	envDefault(socket, "TASK_CONFIG_SOCKET")
	envDefault(bid, "TASK_BUILD_ID")
	if *socket == "" || *bid == "" {
		return fmt.Errorf("run-builder: --config-socket and --build-id required")
	}
	if *pidfile != "" {
		if err := lockPidfile(*pidfile); err != nil {
			return err
		}
	}
	spec, err := configsock.FetchBuildSpec(*socket, "build:"+*bid)
	if err != nil {
		return fmt.Errorf("fetch build spec: %w", err)
	}

	p := &buildPipeline{spec: spec, log: log}
	res := p.run()
	out, _ := json.Marshal(res)
	fmt.Println(string(out)) // stdout → <bid>.result (unit StandardOutput)
	if res.Error != "" {
		return fmt.Errorf("build failed: %s", res.Error)
	}
	return nil
}

// pipelineResult mirrors orch.buildResult.
type pipelineResult struct {
	ImageKey    string `json:"image_key,omitempty"`
	SnapshotKey string `json:"snapshot_key,omitempty"`
	StartCmd    string `json:"start_cmd,omitempty"`
	ReadyCmd    string `json:"ready_cmd,omitempty"`
	Error       string `json:"error,omitempty"`
}

type buildPipeline struct {
	spec *configsock.BuildSpec
	log  *slog.Logger

	ctx    context.Context
	cancel context.CancelFunc

	imagePath string // workdir/image.img once a local image exists
	baseRef   string // phase B/C boot.root.base ("file://..." | "manifest://...")
	startCmd  string // effective (request else template-inherited)
	readyCmd  string
}

const guestFlatten = "/opt/sandbox-runtime/bin/flatten-ctl"

func (p *buildPipeline) run() (res pipelineResult) {
	s := p.spec
	p.ctx, p.cancel = context.WithTimeout(context.Background(),
		time.Duration(s.Timeouts.TotalSec)*time.Second)
	defer p.cancel()
	fail := func(err error) pipelineResult {
		p.log.Error("build", "bid", s.BuildID, "err", err)
		return pipelineResult{Error: err.Error()}
	}

	p.startCmd, p.readyCmd = s.StartCmd, s.ReadyCmd
	if err := p.resolveBase(); err != nil {
		return fail(err)
	}

	if s.FromImage != "" {
		if err := p.phaseImport(); err != nil {
			return fail(fmt.Errorf("import: %w", err))
		}
	}
	if len(s.Steps) > 0 {
		if err := p.phaseSteps(); err != nil {
			return fail(fmt.Errorf("steps: %w", err))
		}
	}
	var bundle string
	if p.startCmd != "" {
		b, err := p.phaseTemplate()
		if err != nil {
			return fail(fmt.Errorf("template: %w", err))
		}
		bundle = b
	}

	// Finale: upload what was produced (the only place platform creds act).
	switch {
	case bundle != "":
		key, err := p.uploadSnapshot(bundle)
		if err != nil {
			return fail(fmt.Errorf("upload snapshot: %w", err))
		}
		res.SnapshotKey = key
	case p.imagePath != "":
		key, err := p.uploadImage()
		if err != nil {
			return fail(fmt.Errorf("upload image: %w", err))
		}
		res.ImageKey = key
	default:
		return fail(fmt.Errorf("nothing produced (no image, no snapshot)"))
	}
	res.StartCmd, res.ReadyCmd = p.startCmd, p.readyCmd
	return res
}

// resolveBase fixes the phase B/C base ref and inherits start/ready from a
// base template's snapshot.cfg metadata (e2b.start_cmd / e2b.ready_cmd).
func (p *buildPipeline) resolveBase() error {
	s := p.spec
	switch {
	case s.FromTemplate == "":
		return nil // base = the imported local image (set by phaseImport)
	case s.FromTemplateKind == "img":
		p.baseRef = "manifest://" + s.FromTemplate
		return nil
	default: // snp: the snapshot.cfg names the image + carries start/ready
		out, err := p.hostCmdEnv(s.Env, p.spec.Paths.SandboxCtl,
			"info", "--json", "--manifest-config", s.Paths.ManifestConfig,
			"manifest://"+s.FromTemplate)
		if err != nil {
			return fmt.Errorf("read base template cfg: %w", err)
		}
		var cfg struct {
			Metadata map[string]string `json:"Metadata"`
			Boot     struct {
				Root struct {
					BaseRef string `json:"BaseRef"`
				} `json:"Root"`
			} `json:"Boot"`
		}
		if err := json.Unmarshal(out, &cfg); err != nil {
			return fmt.Errorf("parse base template cfg: %w", err)
		}
		if cfg.Boot.Root.BaseRef == "" {
			return fmt.Errorf("base template %s has no base image ref", s.FromTemplate)
		}
		p.baseRef = cfg.Boot.Root.BaseRef
		if p.startCmd == "" {
			p.startCmd = cfg.Metadata["e2b.start_cmd"]
		}
		if p.readyCmd == "" {
			p.readyCmd = cfg.Metadata["e2b.ready_cmd"]
		}
		return nil
	}
}

// --- phase A: import -------------------------------------------------------

func (p *buildPipeline) phaseImport() error {
	s := p.spec
	sb, err := p.startSandbox("a", p.importYAML(), nil)
	if err != nil {
		return err
	}
	defer sb.teardown()
	if err := sb.waitExecReady(p.ctx, 60*time.Second); err != nil {
		return err
	}

	args := []string{"export", "--no-progress", "--tmpdir", "/pull"}
	if s.Insecure {
		args = append(args, "--insecure")
	}
	if s.Platform != "" {
		args = append(args, "--platform", s.Platform)
	}
	args = append(args, "--output", "-", s.FromImage)

	p.imagePath = filepath.Join(s.Workdir, "image.img")
	ctx, cancel := context.WithTimeout(p.ctx, time.Duration(s.Timeouts.PullSec)*time.Second)
	defer cancel()
	p.log.Info("import: pulling in-guest", "image", s.FromImage)
	if err := sb.exec(ctx, execOpts{env: p.tenantEnv(), stdoutTo: p.imagePath},
		append([]string{guestFlatten}, args...)...); err != nil {
		return err
	}
	if st, err := os.Stat(p.imagePath); err != nil || st.Size() == 0 {
		return fmt.Errorf("no image artifact produced")
	}
	p.baseRef = "file://" + p.imagePath
	p.log.Info("import: image artifact ready", "path", p.imagePath)
	return nil
}

// tenantEnv is the FLATTEN_* (registry credential) subset of the spec env —
// the only env that ever enters a guest.
func (p *buildPipeline) tenantEnv() []string {
	var out []string
	for k, v := range p.spec.Env {
		if strings.HasPrefix(k, "FLATTEN_") {
			out = append(out, k+"="+v)
		}
	}
	return out
}

// --- phase B: steps --------------------------------------------------------

// stepCtx is the Dockerfile-ish build context the host accumulates.
type stepCtx struct {
	env     map[string]string // ENV: persisted into the image config
	args    map[string]string // ARG: substitution only
	workdir string
	user    string
}

func (p *buildPipeline) phaseSteps() error {
	s := p.spec
	sb, err := p.startSandbox("b", p.stepsYAML(), nil)
	if err != nil {
		return err
	}
	defer sb.teardown()
	if err := sb.waitExecReady(p.ctx, 90*time.Second); err != nil {
		return err
	}

	ctxv := &stepCtx{env: map[string]string{}, args: map[string]string{}}
	for i, st := range s.Steps {
		if err := p.applyStep(sb, ctxv, i, st); err != nil {
			return err
		}
	}

	// Export the built rootfs: tmpdir/output live under a self-bind
	// mountpoint so --skip-mounts excludes them (and the toolchain mount).
	if err := sb.exec(p.ctx, execOpts{}, guestFlatten, "mountpoint", "/.kuasar-build"); err != nil {
		return fmt.Errorf("mountpoint: %w", err)
	}
	cfgJSON, err := p.mergedRuntimeConfig(ctxv)
	if err != nil {
		return err
	}
	cfgPath := filepath.Join(s.Workdir, "config.json")
	if err := os.WriteFile(cfgPath, cfgJSON, 0o600); err != nil {
		return err
	}
	if err := sb.exec(p.ctx, execOpts{stdinFrom: cfgPath},
		"/bin/sh", "-c", "cat > /.kuasar-build/config.json"); err != nil {
		return fmt.Errorf("write runtime config: %w", err)
	}
	newImg := filepath.Join(s.Workdir, "image.new.img")
	ctx, cancel := context.WithTimeout(p.ctx, time.Duration(s.Timeouts.PullSec)*time.Second)
	defer cancel()
	p.log.Info("steps: exporting rootfs")
	if err := sb.exec(ctx, execOpts{stdoutTo: newImg},
		guestFlatten, "export", "--no-progress", "--skip-mounts",
		"--runtime-config", "/.kuasar-build/config.json",
		"--tmpdir", "/.kuasar-build", "--output", "-", "/"); err != nil {
		return err
	}
	p.imagePath = filepath.Join(s.Workdir, "image.img")
	if err := os.Rename(newImg, p.imagePath); err != nil {
		return err
	}
	p.baseRef = "file://" + p.imagePath
	return nil
}

// applyStep executes one build step. RUN goes to the guest; the rest
// transform the host-side context.
func (p *buildPipeline) applyStep(sb *phaseSandbox, c *stepCtx, i int, st configsock.BuildStep) error {
	sub := func(v string) string { // ARG/ENV ${k} substitution
		for k, val := range c.args {
			v = strings.ReplaceAll(v, "${"+k+"}", val)
			v = strings.ReplaceAll(v, "$"+k, val)
		}
		return v
	}
	kv := func() (string, string) { // "K=V" or [K, V] arg shapes
		if len(st.Args) >= 2 {
			return st.Args[0], sub(st.Args[1])
		}
		if len(st.Args) == 1 {
			if k, v, ok := strings.Cut(st.Args[0], "="); ok {
				return k, sub(v)
			}
			return st.Args[0], ""
		}
		return "", ""
	}
	switch strings.ToUpper(st.Type) {
	case "RUN":
		cmd := sub(strings.Join(st.Args, " "))
		p.log.Info("step", "n", i, "run", cmd)
		var env []string
		for k, v := range c.env {
			env = append(env, k+"="+v)
		}
		ctx, cancel := context.WithTimeout(p.ctx, time.Duration(p.spec.Timeouts.StepSec)*time.Second)
		defer cancel()
		opts := execOpts{env: env, cwd: c.workdir, user: c.user}
		if err := sb.exec(ctx, opts, "/bin/sh", "-c", cmd); err != nil {
			return fmt.Errorf("step %d (RUN %s): %w", i, cmd, err)
		}
	case "ENV":
		k, v := kv()
		if k != "" {
			c.env[k] = v
		}
	case "ARG":
		k, v := kv()
		if k != "" {
			c.args[k] = v
		}
	case "WORKDIR":
		if len(st.Args) > 0 {
			c.workdir = sub(st.Args[0])
		}
	case "USER":
		if len(st.Args) > 0 {
			c.user = sub(st.Args[0])
		}
	default:
		return fmt.Errorf("step %d: unsupported type %q", i, st.Type)
	}
	return nil
}

// mergedRuntimeConfig reads the base image's runtime config (through the
// artifact or the manifest store) and overlays the accumulated
// ENV/WORKDIR/USER.
func (p *buildPipeline) mergedRuntimeConfig(c *stepCtx) ([]byte, error) {
	target := p.baseRef
	if p.imagePath != "" {
		target = p.imagePath
	} else {
		target = strings.TrimPrefix(target, "file://")
	}
	args := []string{"info", "--json"}
	if strings.HasPrefix(target, "manifest://") {
		args = append(args, "--manifest-config", p.spec.Paths.ManifestConfig)
	}
	out, err := p.hostCmdEnv(p.spec.Env, p.spec.Paths.FlattenCtl, append(args, target)...)
	if err != nil {
		return nil, fmt.Errorf("read base runtime config: %w", err)
	}
	var info struct {
		Config map[string]any `json:"config"`
	}
	if err := json.Unmarshal(out, &info); err != nil {
		return nil, fmt.Errorf("parse base runtime config: %w", err)
	}
	cfg := info.Config
	if cfg == nil {
		cfg = map[string]any{}
	}
	if len(c.env) > 0 {
		var envs []string
		if cur, ok := cfg["Env"].([]any); ok {
			for _, e := range cur {
				if s, ok := e.(string); ok {
					if k, _, ok := strings.Cut(s, "="); !ok || c.env[k] == "" {
						envs = append(envs, s)
					}
				}
			}
		}
		for k, v := range c.env {
			envs = append(envs, k+"="+v)
		}
		cfg["Env"] = envs
	}
	if c.workdir != "" {
		cfg["WorkingDir"] = c.workdir
	}
	if c.user != "" {
		cfg["User"] = c.user
	}
	return json.Marshal(cfg)
}

// --- phase C: template snapshot ---------------------------------------------

func (p *buildPipeline) phaseTemplate() (string, error) {
	s := p.spec
	envdUDS := filepath.Join(s.Workdir, "envd.sock")
	sb, err := p.startSandbox("c", p.templateYAML(),
		[]string{envdUDS + ":127.0.0.1:49983"})
	if err != nil {
		return "", err
	}
	defer sb.teardown()

	if err := p.waitEnvd(envdUDS, 90*time.Second); err != nil {
		return "", err
	}
	if err := p.envdInit(envdUDS); err != nil {
		return "", fmt.Errorf("envd /init: %w", err)
	}

	// startCmd: detached in-guest (nohup + redirect — no host-side stdio
	// coupling survives into the snapshot), e2b defaults (user "user",
	// /home/user).
	esc := strings.ReplaceAll(p.startCmd, "'", `'\''`)
	start := fmt.Sprintf("nohup /bin/sh -lc '%s' >/tmp/.kuasar-start.log 2>&1 &", esc)
	p.log.Info("template: starting", "cmd", p.startCmd)
	if err := sb.exec(p.ctx, execOpts{user: "user", cwd: "/home/user"},
		"/bin/sh", "-c", start); err != nil {
		return "", fmt.Errorf("startCmd: %w", err)
	}

	// readyCmd: poll to success; without one, settle for a fixed wait.
	if p.readyCmd != "" {
		deadline := time.Now().Add(time.Duration(s.Timeouts.ReadySec) * time.Second)
		for {
			err := sb.exec(p.ctx, execOpts{user: "user", cwd: "/home/user"},
				"/bin/sh", "-lc", p.readyCmd)
			if err == nil {
				break
			}
			if time.Now().After(deadline) {
				return "", fmt.Errorf("readyCmd never succeeded within %ds: %w", s.Timeouts.ReadySec, err)
			}
			select {
			case <-p.ctx.Done():
				return "", p.ctx.Err()
			case <-time.After(time.Second):
			}
		}
		p.log.Info("template: ready")
	} else {
		select {
		case <-p.ctx.Done():
			return "", p.ctx.Err()
		case <-time.After(10 * time.Second):
		}
	}

	out, err := p.hostCmdEnv(s.Env, s.Paths.SandboxCtl, "snapshot",
		"--sandbox-id", sb.sid, "--output", s.Workdir, "--run-root", sb.runRoot)
	if err != nil {
		return "", fmt.Errorf("snapshot: %w (%s)", err, firstLine(out))
	}
	bundle := filepath.Join(s.Workdir, sb.sid+".snapshot")
	if _, err := os.Stat(bundle); err != nil {
		return "", fmt.Errorf("snapshot bundle missing: %w", err)
	}
	p.log.Info("template: snapshot taken", "bundle", bundle)
	return bundle, nil
}

// waitEnvd polls envd's /health over its forwarded UDS.
func (p *buildPipeline) waitEnvd(uds string, timeout time.Duration) error {
	cl := udsHTTP(uds)
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		req, _ := http.NewRequestWithContext(p.ctx, http.MethodGet, "http://envd/health", nil)
		if resp, err := cl.Do(req); err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 || resp.StatusCode == 204 {
				return nil
			}
		}
		select {
		case <-p.ctx.Done():
			return p.ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
	return fmt.Errorf("envd not ready within %s", timeout)
}

// envdInit provisions envd before the snapshot, matching the deployment
// posture: with MMDS the access token freezes into the template (children
// re-key via MMDS at create); without it envd stays non-secure.
func (p *buildPipeline) envdInit(uds string) error {
	payload := map[string]any{
		"envVars":        map[string]string{},
		"defaultUser":    "user",
		"defaultWorkdir": "/home/user",
		"timestamp":      time.Now().UTC().Format(time.RFC3339),
	}
	if p.spec.MMDSEnabled && p.spec.EnvdToken != "" {
		payload["accessToken"] = p.spec.EnvdToken
	}
	body, _ := json.Marshal(payload)
	req, _ := http.NewRequestWithContext(p.ctx, http.MethodPost, "http://envd/init", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := udsHTTP(uds).Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	return nil
}

func udsHTTP(uds string) *http.Client {
	return &http.Client{
		Timeout: 15 * time.Second,
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, "unix", uds)
			},
		},
	}
}

// --- finale: uploads ---------------------------------------------------------

func (p *buildPipeline) uploadImage() (string, error) {
	out, err := p.hostCmdEnv(p.spec.Env, p.spec.Paths.ManifestCtl,
		"store", "--no-progress", "--manifest-config", p.spec.Paths.ManifestConfig, p.imagePath)
	if err != nil {
		return "", fmt.Errorf("%w (%s)", err, firstLine(out))
	}
	key := strings.TrimSpace(string(out))
	if len(key) != 64 {
		return "", fmt.Errorf("manifest-ctl store output %q (want 64-hex key)", key)
	}
	p.log.Info("uploaded image", "key", key)
	return key, nil
}

func (p *buildPipeline) uploadSnapshot(bundle string) (string, error) {
	// upload-snapshot auto-uploads every local artifact the snapshot.cfg
	// references (the base image is a bundle-dir sibling) and rewrites
	// the refs to manifest:// — one command finishes the build.
	out, err := p.hostCmdEnv(p.spec.Env, p.spec.Paths.SandboxCtl,
		"upload-snapshot", "--manifest-config", p.spec.Paths.ManifestConfig, "--quiet", bundle)
	if err != nil {
		return "", fmt.Errorf("%w (%s)", err, firstLine(out))
	}
	key := strings.TrimSpace(string(out))
	key = strings.TrimPrefix(key, "manifest://")
	if len(key) != 64 {
		return "", fmt.Errorf("upload-snapshot output %q (want 64-hex key)", key)
	}
	p.log.Info("uploaded snapshot", "key", key)
	return key, nil
}

// --- sandbox child management ------------------------------------------------

type phaseSandbox struct {
	p       *buildPipeline
	sid     string
	runRoot string
	cmd     *exec.Cmd
	done    chan error
}

// startSandbox writes the phase yaml and spawns `sandbox-ctl run` as a
// direct child (this unit's cgroup). connect lists optional UDS forwards.
func (p *buildPipeline) startSandbox(phase string, doc map[string]any, connect []string) (*phaseSandbox, error) {
	s := p.spec
	sid := "bp-" + phase + "-" + shortBID(s.BuildID)
	runRoot := filepath.Join(s.Workdir, "run")
	if err := os.MkdirAll(filepath.Join(runRoot, sid), 0o700); err != nil {
		return nil, err
	}
	yamlPath := filepath.Join(s.Workdir, phase+".yaml")
	b, err := yaml.Marshal(doc)
	if err != nil {
		return nil, err
	}
	if err := os.WriteFile(yamlPath, b, 0o600); err != nil {
		return nil, err
	}

	args := []string{"run", "--config", yamlPath, "--run-root", runRoot, "--sandbox-id", sid}
	if strings.HasPrefix(p.baseRef, "manifest://") || s.FromTemplateKind != "" {
		args = append(args, "--manifest-config", s.Paths.ManifestConfig)
	}
	for _, c := range connect {
		args = append(args, "--connect", c)
	}
	cmd := exec.Command(s.Paths.SandboxCtl, args...)
	cmd.Env = append(os.Environ(), "MANIFEST_KEY="+s.Env["MANIFEST_KEY"])
	logf, err := os.Create(filepath.Join(s.Workdir, phase+".log"))
	if err != nil {
		return nil, err
	}
	cmd.Stdout, cmd.Stderr = logf, logf
	if err := cmd.Start(); err != nil {
		logf.Close()
		return nil, fmt.Errorf("spawn sandbox-ctl: %w", err)
	}
	sb := &phaseSandbox{p: p, sid: sid, runRoot: runRoot, cmd: cmd, done: make(chan error, 1)}
	go func() {
		sb.done <- cmd.Wait()
		logf.Close()
	}()
	p.log.Info("phase sandbox up", "phase", phase, "sid", sid)
	return sb, nil
}

// waitExecReady polls a cheap in-guest command until the control plane
// answers (boot complete). flatten-ctl mountpoint doubles as the probe —
// it exists in the builder runtime on ANY rootfs, empty ones included.
// Each probe is time-boxed: a half-up control plane accepts the dial but
// never answers, and an unbounded exec would absorb the whole build budget.
func (sb *phaseSandbox) waitExecReady(ctx context.Context, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		probeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		err := sb.exec(probeCtx, execOpts{quiet: true}, guestFlatten, "mountpoint", "/.probe")
		cancel()
		if err == nil {
			return nil
		}
		select {
		case e := <-sb.done:
			return fmt.Errorf("sandbox exited during boot: %v", e)
		default:
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("sandbox not exec-ready within %s: %v", timeout, err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(300 * time.Millisecond):
		}
	}
}

type execOpts struct {
	env       []string // KEY=VALUE pairs forwarded as --env
	cwd       string
	user      string
	stdoutTo  string // write command stdout to this host file (artifact streaming)
	stdinFrom string // feed command stdin from this host file
	quiet     bool   // suppress stderr (readiness probes)
}

// exec runs one command in the guest via sandbox-ctl exec and returns the
// guest exit status as an error when non-zero.
func (sb *phaseSandbox) exec(ctx context.Context, o execOpts, argv ...string) error {
	args := []string{"exec", "--sandbox-id", sb.sid, "--run-root", sb.runRoot}
	for _, e := range o.env {
		args = append(args, "--env", e)
	}
	if o.cwd != "" {
		args = append(args, "--cwd", o.cwd)
	}
	if o.user != "" {
		args = append(args, "--user", o.user)
	}
	if o.stdoutTo != "" {
		args = append(args, "--stdout-to", o.stdoutTo)
	}
	if o.stdinFrom != "" {
		args = append(args, "--stdin-from", o.stdinFrom)
	}
	args = append(args, "--")
	args = append(args, argv...)
	cmd := exec.CommandContext(ctx, sb.p.spec.Paths.SandboxCtl, args...)
	var errBuf bytes.Buffer
	if !o.quiet {
		cmd.Stderr = io.MultiWriter(os.Stderr, &errBuf)
	} else {
		cmd.Stderr = &errBuf
	}
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("guest %s: %w (%s)", argv[0], err, firstLine(errBuf.Bytes()))
	}
	return nil
}

// teardown stops the phase sandbox: SIGTERM, then SIGKILL after a grace
// period.
func (sb *phaseSandbox) teardown() {
	if sb.cmd.Process == nil {
		return
	}
	_ = sb.cmd.Process.Signal(syscall.SIGTERM)
	select {
	case <-sb.done:
	case <-time.After(20 * time.Second):
		_ = sb.cmd.Process.Kill()
		<-sb.done
	}
	sb.p.log.Info("phase sandbox down", "sid", sb.sid)
}

// --- yaml renderers -----------------------------------------------------------

func (p *buildPipeline) networkDoc() map[string]any {
	n := p.spec.Net
	doc := map[string]any{
		"tapfd":    map[string]any{"exec": n.TapFDExec},
		"hostname": n.Hostname,
	}
	if n.MAC != "" {
		doc["mac"] = n.MAC
	}
	if n.InnerIP != "" {
		doc["ip"] = n.InnerIP
		if n.Nexthop != "" {
			doc["nexthop"] = n.Nexthop
		}
	}
	return doc
}

func (p *buildPipeline) resourcesDoc() map[string]any {
	return map[string]any{
		"capacity":    map[string]any{"cpu": p.spec.VCPU, "memory": p.spec.Memory},
		"allocatable": map[string]any{"cpu": float64(p.spec.VCPU), "memory": p.spec.Memory},
	}
}

func (p *buildPipeline) dnsFiles() []map[string]any {
	if len(p.spec.Net.DNS) == 0 {
		return nil
	}
	var b strings.Builder
	for _, ns := range p.spec.Net.DNS {
		b.WriteString("nameserver " + ns + "\n")
	}
	return []map[string]any{{"path": "/etc/resolv.conf", "content": b.String(), "mode": "0644"}}
}

// importYAML: an EMPTY single-disk sandbox (no base image at all) on the
// builder runtime flavor — the writable ext4 root doubles as the pull
// scratch. launch.placeholder anchors it; the toolchain rides the
// /opt/sandbox-runtime projection.
func (p *buildPipeline) importYAML() map[string]any {
	s := p.spec
	doc := map[string]any{
		"resources": p.resourcesDoc(),
		"network":   p.networkDoc(),
		"boot": map[string]any{
			"kernel":  "file://" + s.Paths.Kernel,
			"runtime": "file://" + s.Paths.RuntimeBuilder,
			"root": map[string]any{
				"diff_template": "file://" + s.Paths.BuilderDiffTpl,
			},
		},
		"launch": map[string]any{"placeholder": true},
	}
	if f := p.dnsFiles(); f != nil {
		doc["files"] = f
	}
	return doc
}

// stepsYAML: the base image as root, a big writable upper (steps delta +
// export scratch), the builder runtime for the toolchain, a placeholder
// anchor (steps run via exec sessions).
func (p *buildPipeline) stepsYAML() map[string]any {
	s := p.spec
	doc := map[string]any{
		"resources": p.resourcesDoc(),
		"network":   p.networkDoc(),
		"boot": map[string]any{
			"kernel":  "file://" + s.Paths.Kernel,
			"runtime": "file://" + s.Paths.RuntimeBuilder,
			"root": map[string]any{
				"base": p.baseRef,
				"overlay": map[string]any{
					"diff_template": "file://" + s.Paths.BuilderDiffTpl,
				},
			},
		},
		"launch": map[string]any{"placeholder": true},
	}
	if f := p.dnsFiles(); f != nil {
		doc["files"] = f
	}
	return doc
}

// templateYAML: a PRODUCTION e2b sandbox — the e2b runtime flavor (frozen
// into the snapshot as runtime_ref), envd as the app (FC mode per the
// deployment's MMDS posture), and the e2b start/ready metadata recorded
// into snapshot.cfg so the template is self-describing.
func (p *buildPipeline) templateYAML() map[string]any {
	s := p.spec
	envdArgs := []string{"-isnotfc", "-port", "49983"}
	if s.MMDSEnabled {
		envdArgs = []string{"-port", "49983"}
	}
	meta := map[string]string{"e2b.start_cmd": p.startCmd}
	if p.readyCmd != "" {
		meta["e2b.ready_cmd"] = p.readyCmd
	}
	doc := map[string]any{
		"resources": p.resourcesDoc(),
		"network":   p.networkDoc(),
		"metadata":  meta,
		"boot": map[string]any{
			"kernel":  "file://" + s.Paths.Kernel,
			"runtime": "file://" + s.Paths.RuntimeE2B,
			"root": map[string]any{
				"base": p.baseRef,
				"overlay": map[string]any{
					"diff_template": "file://" + s.Paths.OverlayDiffTpl,
				},
			},
		},
		"launch": map[string]any{
			"exec":    "/opt/sandbox-runtime/bin/envd",
			"args":    envdArgs,
			"user":    "0:0",
			"restart": "always",
		},
	}
	if f := p.dnsFiles(); f != nil {
		doc["files"] = f
	}
	return doc
}

// --- helpers -------------------------------------------------------------------

// hostCmdEnv runs a host-side tool with the BuildSpec env (MANIFEST_KEY +
// tenant registry creds) layered over the unit environment — every host
// invocation gets it, since any of them may resolve manifest:// refs.
func (p *buildPipeline) hostCmdEnv(env map[string]string, bin string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(p.ctx, bin, args...)
	cmd.Env = os.Environ()
	for k, v := range env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	if err := cmd.Run(); err != nil {
		return errb.Bytes(), fmt.Errorf("%s %s: %w", filepath.Base(bin), args[0], err)
	}
	return out.Bytes(), nil
}

func firstLine(b []byte) string {
	s := strings.TrimSpace(string(b))
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		s = s[:i]
	}
	if len(s) > 200 {
		s = s[:200]
	}
	return s
}

func shortBID(s string) string {
	if len(s) > 8 {
		return s[:8]
	}
	return s
}
