package builder

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
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

	args := []string{"run", "--config", yamlPath, "--run-root", runRoot, "--sandbox-id", sid,
		// App stdio + kernel dmesg → journald straight from sandbox-ctl (it's
		// our child, in this run-id unit's cgroup). App output is tagged "build"
		// with KUASAR_BUILD_ID for SDK-visible build logs; kernel output is tagged
		// "console" for host-only diagnostics. No per-phase log file.
		"--stdout-to", "journald=" + buildTag,
		"--stderr-to", "journald=" + buildTag,
		"--console", "journald=" + consoleTag}
	args = append(args, "--manifest-config", s.Paths.ManifestConfig)
	args = appendRefLocationArgs(args, s.RefLocations)
	for _, c := range connect {
		args = append(args, "--connect", c)
	}
	cmd := exec.Command(s.Paths.SandboxCtl, args...)
	cmd.Env = append(os.Environ(),
		"MANIFEST_KEY="+s.Env["MANIFEST_KEY"],
		"KUASAR_RUN_ID="+s.RunID,
		"KUASAR_BUILD_ID="+s.BuildID,
	)
	// sandbox-ctl's own process stdio (the app/kernel are off on journald) inherit
	// run-builder's stderr → builder unit journal for host diagnostics.
	cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("spawn sandbox-ctl: %w", err)
	}
	sb := &phaseSandbox{p: p, sid: sid, runRoot: runRoot, cmd: cmd, done: make(chan error, 1)}
	go func() {
		sb.done <- cmd.Wait()
	}()
	p.log.Info("phase sandbox up", "phase", phase, "sid", sid)
	return sb, nil
}

func appendRefLocationArgs(args []string, locations map[string]string) []string {
	names := make([]string, 0, len(locations))
	for name := range locations {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		args = append(args, "--ref-location", name+"="+locations[name])
	}
	return args
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
	stdoutTo  string   // --stdout-to: host file (artifact) or journald=<tag>
	stderrTo  string   // --stderr-to: host file or journald=<tag> ("" = capture for the error tail)
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
	if o.stderrTo != "" {
		// Guest stderr → its sink (e.g. journald=build). sandbox-ctl exec's
		// own process stderr still lands in errBuf for a (generic) error tail.
		args = append(args, "--stderr-to", o.stderrTo)
	}
	if o.stdinFrom != "" {
		args = append(args, "--stdin-from", o.stdinFrom)
	}
	args = append(args, "--")
	args = append(args, argv...)
	cmd := exec.CommandContext(ctx, sb.p.spec.Paths.SandboxCtl, args...)
	var errBuf bytes.Buffer
	if o.quiet {
		cmd.Stderr = &errBuf
	} else {
		cmd.Stderr = io.MultiWriter(os.Stderr, &errBuf)
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
