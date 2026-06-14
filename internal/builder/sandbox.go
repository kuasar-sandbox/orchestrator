package builder

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"gopkg.in/yaml.v3"
)

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

// execOpts shapes a sandbox-ctl exec invocation — the PLATFORM channel
// (flatten-ctl pulls/exports, config injection, artifact streaming,
// probes). e2b-semantic commands (steps/startCmd/readyCmd) go through
// envd instead (envdExec).
type execOpts struct {
	env       []string // KEY=VALUE pairs forwarded as --env (tenant FLATTEN_*)
	stdoutTo  string   // write command stdout to this host file (artifact streaming)
	stdinFrom string   // feed command stdin from this host file
	quiet     bool     // suppress stderr (readiness probes)
}

// exec runs one command in the guest via sandbox-ctl exec and returns the
// guest exit status as an error when non-zero.
func (sb *phaseSandbox) exec(ctx context.Context, o execOpts, argv ...string) error {
	args := []string{"exec", "--sandbox-id", sb.sid, "--run-root", sb.runRoot}
	for _, e := range o.env {
		args = append(args, "--env", e)
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
