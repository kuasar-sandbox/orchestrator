package builder

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base32"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"golang.org/x/sys/unix"
	"gopkg.in/yaml.v3"
)

// --- sandbox child management ------------------------------------------------

type phaseSandbox struct {
	p              *buildPipeline
	sid            string
	runRoot        string
	cmd            *exec.Cmd
	done           chan struct{}
	waitMu         sync.Mutex
	waitErr        error
	readyR         *os.File
	readyCloseOnce sync.Once
}

// startSandbox writes the phase yaml and spawns `sandbox-ctl run` as a
// direct child (this unit's cgroup). connect lists optional UDS forwards.
func (p *buildPipeline) startSandbox(phase string, doc map[string]any, connect []string) (*phaseSandbox, error) {
	s := p.spec
	sid := phaseSandboxID(phase, s.BuildID)
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
	if p.vmmCgroup == nil || p.vmmCgroup.Fd() < 3 {
		return nil, fmt.Errorf("phase %s has no trusted VMM cgroup descriptor", phase)
	}
	cgroupChildFD := 3 + len(cmd.ExtraFiles)
	cmd.ExtraFiles = append(cmd.ExtraFiles, p.vmmCgroup)
	cmd.Args = append(cmd.Args, fmt.Sprintf("--cgroup-path=fd=%d", cgroupChildFD))
	readyR, readyW, _, err := attachReadinessPipe(cmd)
	if err != nil {
		return nil, fmt.Errorf("create readiness pipe: %w", err)
	}
	cmd.Env = append(os.Environ(),
		"MANIFEST_KEY="+s.Env["MANIFEST_KEY"],
		"KUASAR_RUN_ID="+s.RunID,
		"KUASAR_BUILD_ID="+s.BuildID,
	)
	// sandbox-ctl's own process stdio (the app/kernel are off on journald) inherit
	// run-builder's stderr → builder unit journal for host diagnostics.
	cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
	if err := cmd.Start(); err != nil {
		_ = readyR.Close()
		_ = readyW.Close()
		return nil, fmt.Errorf("spawn sandbox-ctl: %w", err)
	}
	// os/exec has duplicated ExtraFiles into the child. Drop the parent's writer
	// immediately so every pre-ready child exit is observable as EOF by readyR.
	_ = readyW.Close()
	sb := &phaseSandbox{
		p: p, sid: sid, runRoot: runRoot, cmd: cmd,
		done: make(chan struct{}), readyR: readyR,
	}
	go func() {
		err := cmd.Wait()
		sb.waitMu.Lock()
		sb.waitErr = err
		sb.waitMu.Unlock()
		close(sb.done)
	}()
	p.log.Info("phase sandbox up", "phase", phase, "sid", sid)
	return sb, nil
}

const phaseSandboxDigestBytes = 12

var phaseSandboxIDEncoding = base32.HexEncoding.WithPadding(base32.NoPadding)

func phaseSandboxID(phase, buildID string) string {
	// Build IDs are opaque at this boundary (cluster registrations are not
	// required to be UUIDs). Hash the complete identity so it cannot inject a
	// path and Builds sharing a short prefix still receive distinct ordinary
	// Sandbox IDs. A 96-bit digest remains collision-resistant while its compact
	// base32 form leaves room for sandbox runtime UDS names under sun_path.
	sum := sha256.Sum256([]byte(buildID))
	digest := phaseSandboxIDEncoding.EncodeToString(sum[:phaseSandboxDigestBytes])
	return fmt.Sprintf("bp-%s-%s", phase, strings.ToLower(digest))
}

// attachReadinessPipe gives the writer the next os/exec child descriptor. The
// number is derived from the pre-existing ExtraFiles rather than assuming fd 3.
func attachReadinessPipe(cmd *exec.Cmd) (reader, writer *os.File, childFD int, err error) {
	reader, writer, err = os.Pipe()
	if err != nil {
		return nil, nil, 0, err
	}
	childFD = 3 + len(cmd.ExtraFiles)
	cmd.ExtraFiles = append(cmd.ExtraFiles, writer)
	cmd.Args = append(cmd.Args, fmt.Sprintf("--ready-fd=%d", childFD))
	return reader, writer, childFD, nil
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

func (sb *phaseSandbox) closeReady() {
	sb.readyCloseOnce.Do(func() {
		if sb.readyR != nil {
			_ = sb.readyR.Close()
		}
	})
}

func (sb *phaseSandbox) waitError() error {
	sb.waitMu.Lock()
	defer sb.waitMu.Unlock()
	return sb.waitErr
}

func (sb *phaseSandbox) childExitError() error {
	if err := sb.waitError(); err != nil {
		return fmt.Errorf("sandbox exited during boot: %w", err)
	}
	return fmt.Errorf("sandbox exited during boot")
}

// waitRuntimeReady consumes the same exact control_ready -> ready -> EOF wire
// as the main runner. Closing readyR is the cancellation mechanism for the
// parser goroutine; child exit is independently observable through done.
func (sb *phaseSandbox) waitRuntimeReady(ctx context.Context) error {
	if sb.readyR == nil {
		return fmt.Errorf("sandbox readiness pipe is unavailable")
	}
	parsed := make(chan error, 1)
	go func() { parsed <- configsock.ReadReadiness(sb.readyR) }()

	select {
	case err := <-parsed:
		sb.closeReady()
		if err != nil {
			select {
			case <-sb.done:
				return sb.childExitError()
			default:
			}
			return fmt.Errorf("sandbox runtime readiness: %w", err)
		}
		select {
		case <-sb.done:
			return sb.childExitError()
		default:
			return nil
		}
	case <-sb.done:
		sb.closeReady()
		<-parsed
		return sb.childExitError()
	case <-ctx.Done():
		sb.closeReady()
		<-parsed
		return fmt.Errorf("sandbox runtime readiness: %w", ctx.Err())
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
	sb.closeReady()
	select {
	case <-sb.done:
		sb.p.log.Info("phase sandbox down", "sid", sb.sid)
		return
	default:
	}
	if sb.cmd.Process == nil {
		return
	}
	_ = sb.cmd.Process.Signal(syscall.SIGTERM)
	timer := time.NewTimer(20 * time.Second)
	defer timer.Stop()
	select {
	case <-sb.done:
	case <-timer.C:
		_ = sb.cmd.Process.Kill()
		<-sb.done
	}
	sb.p.log.Info("phase sandbox down", "sid", sb.sid)
}

// requirePhaseVMMCgroupEmpty proves that the ordinary sandbox has fully left
// the delegated VMM cgroup before the build can publish phase completion and
// start the next phase. cgroup.events' populated bit covers the complete
// subtree, unlike cgroup.procs which lists only direct processes. The directory
// descriptor is the same trusted cgroup FD handed to sandbox-ctl; no
// tenant-controlled path is resolved here.
func (p *buildPipeline) requirePhaseVMMCgroupEmpty() error {
	if p.vmmCgroup == nil {
		// Tests that exercise only phase reporting do not spawn a sandbox. The
		// production entry point rejects a missing descriptor before Run.
		return nil
	}
	fd, err := unix.Openat(int(p.vmmCgroup.Fd()), "cgroup.events", unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return fmt.Errorf("verify phase VMM cgroup cleanup: open cgroup.events: %w", err)
	}
	f := os.NewFile(uintptr(fd), "phase-vmm-cgroup.events")
	if f == nil {
		_ = unix.Close(fd)
		return fmt.Errorf("verify phase VMM cgroup cleanup: adopt cgroup.events descriptor")
	}
	defer f.Close()
	events, err := io.ReadAll(f)
	if err != nil {
		return fmt.Errorf("verify phase VMM cgroup cleanup: read cgroup.events: %w", err)
	}
	fields := bytes.Fields(events)
	for index := 0; index+1 < len(fields); index += 2 {
		if bytes.Equal(fields[index], []byte("populated")) {
			if bytes.Equal(fields[index+1], []byte("0")) {
				return nil
			}
			if bytes.Equal(fields[index+1], []byte("1")) {
				return fmt.Errorf("verify phase VMM cgroup cleanup: cgroup subtree is still populated")
			}
			break
		}
	}
	return fmt.Errorf("verify phase VMM cgroup cleanup: malformed cgroup.events")
}
