package main

// Shared scaffold for the in-unit sandbox launcher: lock the task pidfile
// (double-start guard), fetch a LaunchSpec over the config-socket, then exec-replace
// into the target so it inherits this PID (the unit's main pid + cgroup).

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sys/unix"

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/taskartifact"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// launchTask fetches the already-authenticated exact-run bootstrap, performs
// optional task-local artifact preparation, and exec-replaces into the target.
// runAssignedSandbox has locked the task pidfile before this function is called.
func launchTask(ctx context.Context, stopContext func(), socket, sandboxID, runID string, ready, vmmCgroup *os.File, log *slog.Logger) error {
	return launchTaskWith(ctx, stopContext, socket, sandboxID, runID, ready, vmmCgroup, taskLaunchOps{
		fetchBootstrap:  configsock.FetchSandboxTaskSpec,
		prepareArtifact: taskartifact.Prepare,
		completePrepare: configsock.CompleteSandboxPrepare,
		setenv:          os.Setenv,
		chdir:           os.Chdir,
		exec:            syscall.Exec,
		log:             log,
	})
}

type taskLaunchOps struct {
	fetchBootstrap  func(context.Context, string, string, string) (*configsock.SandboxTaskSpec, error)
	prepareArtifact func(context.Context, configsock.ArtifactPrepareSpec) (*taskartifact.Result, error)
	completePrepare func(context.Context, string, string, string, configsock.ArtifactPrepareSummary) (*configsock.LaunchSpec, error)
	setenv          func(string, string) error
	chdir           func(string) error
	exec            func(string, []string, []string) error
	log             *slog.Logger
}

func launchTaskWith(ctx context.Context, stopContext func(), socket, sandboxID, runID string, ready, vmmCgroup *os.File, ops taskLaunchOps) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if stopContext == nil {
		stopContext = func() {}
	}
	if ops.log == nil {
		ops.log = slog.Default()
	}
	bootstrap, err := ops.fetchBootstrap(ctx, socket, sandboxID, runID)
	if err != nil {
		return fmt.Errorf("fetch sandbox task bootstrap: %w", err)
	}
	if bootstrap.SandboxID != sandboxID || bootstrap.RunID != runID {
		return fmt.Errorf("sandbox task bootstrap identity mismatch")
	}
	if (bootstrap.Final == nil) == (bootstrap.Prepare == nil) {
		return fmt.Errorf("sandbox task bootstrap must contain exactly one of final or prepare")
	}
	if bootstrap.Prepare != nil && bootstrap.Prepare.RunID != runID {
		return fmt.Errorf("sandbox preparation RunID mismatch")
	}
	if _, ok := bootstrap.Env["MANIFEST_KEY"]; !ok {
		return fmt.Errorf("sandbox task bootstrap has no authoritative manifest key")
	}
	if err := installTaskEnvironment(bootstrap.Env, ops.setenv); err != nil {
		return err
	}

	taskCtx := ctx
	cancelDeadline := func() {}
	if bootstrap.Prepare != nil {
		deadline := time.Unix(0, bootstrap.Prepare.AbsoluteDeadlineUnixNano)
		if bootstrap.Prepare.AbsoluteDeadlineUnixNano <= 0 || !deadline.After(time.Now()) {
			return fmt.Errorf("sandbox task bootstrap launch deadline has expired")
		}
		taskCtx, cancelDeadline = context.WithDeadline(ctx, deadline)
	}
	defer cancelDeadline()

	spec := bootstrap.Final
	locations := map[string]string{}
	var preparedSource types.ResumeSource
	if bootstrap.Prepare != nil {
		result, err := ops.prepareArtifact(taskCtx, *bootstrap.Prepare)
		if err != nil {
			ops.log.Error("sandbox task artifact prepare failed", "sid", sandboxID, "run_id", runID,
				"task_artifact_prepare_error_total", 1, "stage", "artifact_prepare", "err", err)
			return err
		}
		locations = result.RefLocationURIs
		preparedSource = result.PreparedSource
		ops.log.Info("sandbox task artifact prepared", "sid", sandboxID, "run_id", runID,
			"task_artifact_prepare_duration", result.PrepareDuration,
			"task_artifact_config_read_duration", result.ConfigReadDuration,
			"task_artifact_ref_count", result.Summary.RequiredRefCount)
		spec, err = completeSandboxPrepareWithRetry(taskCtx, socket, sandboxID, runID, result.Summary, ops.completePrepare)
		if err != nil {
			return fmt.Errorf("complete sandbox artifact preparation: %w", err)
		}
	}
	if spec == nil {
		return fmt.Errorf("sandbox task has no final launch spec")
	}
	if spec.Exec == "" {
		return fmt.Errorf("launch spec has no exec")
	}
	if vmmCgroup == nil || vmmCgroup.Fd() < 3 {
		return fmt.Errorf("launch task has no inheritable vmm cgroup descriptor")
	}
	if arg, ok := launchSpecCgroupArg(spec.Args); ok {
		return fmt.Errorf("launch spec must not set node-owned cgroup argument %q", arg)
	}
	if len(spec.Args) == 0 || spec.Args[0] != "run" {
		return fmt.Errorf("launch spec must invoke sandbox-ctl run")
	}
	if arg, ok := launchSpecArtifactArg(spec.Args); ok {
		return fmt.Errorf("launch spec must not set task-owned artifact argument %q", arg)
	}
	workdir := spec.Workdir
	if workdir == "" {
		workdir = bootstrap.Workdir
	}
	if workdir != "" {
		if err := ops.chdir(workdir); err != nil {
			return fmt.Errorf("chdir %s: %w", workdir, err)
		}
	}
	argv := []string{spec.Exec, "run", fmt.Sprintf("--cgroup-path=fd=%d", vmmCgroup.Fd())}
	if ready != nil {
		fd := int(ready.Fd())
		if fd < 3 {
			return fmt.Errorf("readiness fd %d is not inheritable", fd)
		}
		argv = append(argv, fmt.Sprintf("--ready-fd=%d", fd))
	}
	argv = append(argv, spec.Args[1:]...)
	if !preparedSource.Empty() {
		switch preparedSource.Kind {
		case types.ResumeSourceSandbox:
			argv = append(argv, "--from", preparedSource.Ref)
		case types.ResumeSourceSnapshot:
			argv = append(argv, "--restore", preparedSource.Ref)
		default:
			return fmt.Errorf("task prepared unsupported source kind %q", preparedSource.Kind)
		}
	}
	argv = appendRefLocationArgs(argv, locations)
	authoritativeEnv := mergeAuthoritativeEnv(spec.Env, bootstrap.Env)
	env := taskEnv(authoritativeEnv)
	// These are the last fallible operations before exec. If exec itself fails,
	// runAssignedSandbox's defers close the now-inheritable descriptors.
	cancelDeadline()
	stopContext()
	if err := clearCloseOnExec(vmmCgroup); err != nil {
		return fmt.Errorf("make vmm cgroup descriptor inheritable: %w", err)
	}
	if ready != nil {
		if err := clearCloseOnExec(ready); err != nil {
			return err
		}
	}
	return ops.exec(spec.Exec, argv, env)
}

func completeSandboxPrepareWithRetry(
	ctx context.Context,
	socket, sandboxID, runID string,
	summary configsock.ArtifactPrepareSummary,
	complete func(context.Context, string, string, string, configsock.ArtifactPrepareSummary) (*configsock.LaunchSpec, error),
) (*configsock.LaunchSpec, error) {
	delay := 50 * time.Millisecond
	for {
		spec, err := complete(ctx, socket, sandboxID, runID, summary)
		if err == nil || !configsock.IsRetryableError(err) {
			return spec, err
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return nil, ctx.Err()
		case <-timer.C:
		}
		if delay < time.Second {
			delay *= 2
			if delay > time.Second {
				delay = time.Second
			}
		}
	}
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

func installTaskEnvironment(env map[string]string, setenv func(string, string) error) error {
	if setenv == nil {
		return fmt.Errorf("task environment installer is not configured")
	}
	keys := make([]string, 0, len(env))
	for key := range env {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if key == "" || strings.ContainsRune(key, '=') || strings.IndexByte(key, 0) >= 0 || strings.IndexByte(env[key], 0) >= 0 {
			return fmt.Errorf("invalid task environment key %q", key)
		}
		if err := setenv(key, env[key]); err != nil {
			return fmt.Errorf("set task environment %s: %w", key, err)
		}
	}
	return nil
}

func mergeAuthoritativeEnv(base, authoritative map[string]string) map[string]string {
	out := make(map[string]string, len(base)+len(authoritative))
	for key, value := range base {
		out[key] = value
	}
	for key, value := range authoritative {
		out[key] = value
	}
	return out
}

func launchSpecCgroupArg(args []string) (string, bool) {
	for _, arg := range args {
		if arg == "--cgroup-path" || strings.HasPrefix(arg, "--cgroup-path=") ||
			arg == "--cgroup-adopt" || strings.HasPrefix(arg, "--cgroup-adopt=") {
			return arg, true
		}
	}
	return "", false
}

func launchSpecArtifactArg(args []string) (string, bool) {
	for _, arg := range args {
		if arg == "--from" || strings.HasPrefix(arg, "--from=") ||
			arg == "--restore" || strings.HasPrefix(arg, "--restore=") {
			return arg, true
		}
	}
	return "", false
}

func clearCloseOnExec(f *os.File) error {
	flags, err := unix.FcntlInt(f.Fd(), unix.F_GETFD, 0)
	if err != nil {
		return fmt.Errorf("get descriptor flags: %w", err)
	}
	if _, err := unix.FcntlInt(f.Fd(), unix.F_SETFD, flags&^unix.FD_CLOEXEC); err != nil {
		return fmt.Errorf("clear descriptor close-on-exec: %w", err)
	}
	return nil
}

func envDefault(p *string, key string) {
	if *p == "" {
		*p = os.Getenv(key)
	}
}

// lockPidfile opens path, takes a non-blocking exclusive POSIX lock (fails if another
// instance already holds it — the double-start guard), writes our pid, and clears
// FD_CLOEXEC so the lock survives execve into the target (the fd stays open in the
// same-PID process; the lock releases on process exit). It is never closed and never
// unlinked — the launcher does not clean up the pidfile.
func lockPidfile(path string) error {
	fd, err := unix.Open(path, unix.O_RDWR|unix.O_CREAT, 0o600)
	if err != nil {
		return fmt.Errorf("open pidfile %s: %w", path, err)
	}
	lk := unix.Flock_t{Type: unix.F_WRLCK, Whence: 0, Start: 0, Len: 0}
	if err := unix.FcntlFlock(uintptr(fd), unix.F_SETLK, &lk); err != nil {
		unix.Close(fd)
		return fmt.Errorf("pidfile %s locked (task already running?): %w", path, err)
	}
	if err := unix.Ftruncate(fd, 0); err != nil {
		unix.Close(fd)
		return fmt.Errorf("truncate pidfile: %w", err)
	}
	if _, err := unix.Pwrite(fd, []byte(strconv.Itoa(os.Getpid())+"\n"), 0); err != nil {
		unix.Close(fd)
		return fmt.Errorf("write pidfile: %w", err)
	}
	if flags, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0); err == nil {
		_, _ = unix.FcntlInt(uintptr(fd), unix.F_SETFD, flags&^unix.FD_CLOEXEC)
	}
	return nil
}

// taskEnv returns the child environment: the inherited env minus the TASK_* bootstrap
// vars, plus the spec's added env (secrets like MANIFEST_KEY).
func taskEnv(add map[string]string) []string {
	skip := map[string]bool{
		"TASK_PIDFILE": true, "TASK_CONFIG_SOCKET": true,
		"TASK_RUN_ID": true, "TASK_SANDBOX_ID": true, "TASK_BUILD_ID": true,
	}
	out := make([]string, 0, len(os.Environ())+len(add))
	for _, kv := range os.Environ() {
		if i := strings.IndexByte(kv, '='); i >= 0 {
			key := kv[:i]
			if skip[key] {
				continue
			}
			if _, overridden := add[key]; overridden {
				continue
			}
		}
		out = append(out, kv)
	}
	keys := make([]string, 0, len(add))
	for key := range add {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, k := range keys {
		v := add[k]
		out = append(out, k+"="+v)
	}
	return out
}
