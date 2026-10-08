package main

// Shared scaffold for the assigned sandbox runner and bounded result reporting.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unicode/utf8"

	"golang.org/x/sys/unix"

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxsdk"
	"github.com/kuasar-sandbox/orchestrator/internal/taskartifact"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// launchTask fetches the already-authenticated exact-run bootstrap, performs
// optional task-local artifact preparation, and owns the SDK lifecycle.
// runAssignedSandbox has locked the run pidfile before this function is called.
func launchTask(ctx context.Context, stopContext func(), socket, sandboxID, runID string, ready, vmmCgroup *os.File, log *slog.Logger) error {
	return launchTaskWithExecution(ctx, stopContext, socket, sandboxID, runID, ready, vmmCgroup, log, executeSandboxAndReport)
}

func launchTaskWithExecution(ctx context.Context, stopContext func(), socket, sandboxID, runID string, ready, vmmCgroup *os.File, log *slog.Logger,
	execute func(context.Context, string, string, string, configsock.LaunchSpec, types.ResumeSource, map[string]string, map[string]string, *os.File, *os.File) error) error {
	err := launchTaskWith(ctx, stopContext, socket, sandboxID, runID, ready, vmmCgroup, taskLaunchOps{
		fetchBootstrap:  configsock.FetchSandboxTaskSpec,
		prepareArtifact: taskartifact.Prepare,
		completePrepare: configsock.CompleteSandboxPrepare,
		setenv:          os.Setenv,
		execute: func(ctx context.Context, spec configsock.LaunchSpec, source types.ResumeSource, locations, env map[string]string, vmmCgroup, ready *os.File) error {
			return execute(ctx, socket, sandboxID, runID, spec, source, locations, env, vmmCgroup, ready)
		},
		log: log,
	})
	var reported sandboxRunReportedError
	if err != nil && !errors.As(err, &reported) {
		if ready != nil {
			_ = ready.Close()
		}
		result := sandboxExecutionResult(sandboxID, runID, types.SandboxResultPrepare, err)
		if reportErr := postSandboxResultWithRetry(socket, sandboxID, runID, result); reportErr != nil {
			return sandboxRunReportedError{err: errors.Join(err, reportErr)}
		}
	}
	return err
}

type taskLaunchOps struct {
	fetchBootstrap  func(context.Context, string, string, string) (*configsock.SandboxTaskSpec, error)
	prepareArtifact func(context.Context, configsock.ArtifactPrepareSpec) (*taskartifact.Result, error)
	completePrepare func(context.Context, string, string, string, configsock.ArtifactPrepareSummary) (*configsock.LaunchSpec, error)
	setenv          func(string, string) error
	execute         func(context.Context, configsock.LaunchSpec, types.ResumeSource, map[string]string, map[string]string, *os.File, *os.File) error
	log             *slog.Logger
}

func launchTaskWith(ctx context.Context, stopContext func(), socket, sandboxID, runID string, ready, vmmCgroup *os.File, ops taskLaunchOps) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if stopContext == nil {
		stopContext = func() {}
	}
	defer stopContext()
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
	// taskartifact still uses the existing narrow bootstrap environment.
	// The SDK receives explicit credentials and never installs environment.
	if bootstrap.Prepare != nil {
		if err := installTaskEnvironment(bootstrap.Env, ops.setenv); err != nil {
			return err
		}
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
	final := *spec
	if final.Workdir == "" {
		final.Workdir = bootstrap.Workdir
	}
	authoritativeEnv := mergeAuthoritativeEnv(spec.Env, bootstrap.Env)
	if err := validateTaskEnvironment(authoritativeEnv); err != nil {
		return err
	}
	env := make(map[string]string)
	for _, entry := range taskEnv(authoritativeEnv) {
		key, value, ok := strings.Cut(entry, "=")
		if ok {
			env[key] = value
		}
	}
	// The preparation deadline ends here. Keep the signal lifetime context
	// alive until SDK cleanup and result publication are complete.
	cancelDeadline()
	if ops.execute == nil {
		return errors.New("sandbox SDK executor is not configured")
	}
	return ops.execute(ctx, final, preparedSource, locations, env, vmmCgroup, ready)
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

func validateTaskEnvironment(env map[string]string) error {
	for key, value := range env {
		if key == "" || strings.ContainsAny(key, "=\x00") || strings.ContainsRune(value, 0) {
			return fmt.Errorf("invalid task environment key %q", key)
		}
	}
	return nil
}

func installTaskEnvironment(env map[string]string, setenv func(string, string) error) error {
	if err := validateTaskEnvironment(env); err != nil {
		return err
	}
	if setenv == nil {
		return fmt.Errorf("task environment installer is not configured")
	}
	keys := make([]string, 0, len(env))
	for key := range env {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
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

func envDefault(p *string, key string) {
	if *p == "" {
		*p = os.Getenv(key)
	}
}

// lockPidfile opens path, takes a non-blocking exclusive POSIX lock (fails if another
// instance already holds it — the double-start guard), and writes our pid. The
// descriptor stays open in the parent to retain the lock, but is close-on-exec so
// sandbox-ctl children cannot inherit the parent-owned RunID/Build pidfile. It is
// never closed and never unlinked — the launcher does not clean up the pidfile.
func lockPidfile(path string) error {
	fd, err := unix.Open(path, unix.O_RDWR|unix.O_CREAT|unix.O_CLOEXEC, 0o600)
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

type sandboxRunReportedError struct{ err error }

func (e sandboxRunReportedError) Error() string { return e.err.Error() }
func (e sandboxRunReportedError) Unwrap() error { return e.err }

func executeSandboxAndReport(ctx context.Context, socket, sandboxID, runID string, spec configsock.LaunchSpec, source types.ResumeSource, locations, env map[string]string, cgroup, ready *os.File) error {
	return executeSandboxAndReportWith(ctx, sandboxID, runID, spec, source, locations, env, cgroup, ready,
		sandboxsdk.Execute, func(result configsock.SandboxExecutionResult) error {
			return postSandboxResultWithRetry(socket, sandboxID, runID, result)
		})
}

func executeSandboxAndReportWith(ctx context.Context, sandboxID, runID string, spec configsock.LaunchSpec, source types.ResumeSource, locations, env map[string]string, cgroup, ready *os.File,
	execute func(context.Context, *sandboxsdk.Input, *os.File, *sandboxsdk.Readiness) (int, error), report func(configsock.SandboxExecutionResult) error) error {
	readiness := sandboxsdk.NewReadiness(ready)
	input, err := sandboxsdk.Parse(spec, source, locations, env, sandboxID)
	stage := types.SandboxResultStart
	code := 1
	if err == nil {
		// Entering the library replaces successfully starting sandbox-ctl.
		// CLI config/start/runtime failures were all child exit results.
		stage = types.SandboxResultRun
		code, err = execute(ctx, input, cgroup, readiness)
		if err != nil && code <= 0 {
			code = 1
		}
		code &= 255 // match the former CLI process exit status
		// SDK diagnostics have already been routed and drained. Match the
		// former child error at node-ctl's boundary, without relabeling the
		// detailed runtime error as a second node-ctl diagnostic.
		err = nil
		if code != 0 {
			err = fmt.Errorf("exit status %d", code)
		}
	}
	_ = readiness.Close()
	result := sandboxExecutionResult(sandboxID, runID, stage, err)
	if stage == types.SandboxResultRun {
		result.ExitCode = &code
		result.Error = ""
		if code != 0 {
			result.Error = fmt.Sprintf("exit status %d", code)
		}
	}
	reportErr := report(result)
	if err != nil || reportErr != nil {
		return sandboxRunReportedError{err: errors.Join(err, reportErr)}
	}
	return nil
}

func sandboxExecutionResult(sandboxID, runID string, stage types.SandboxExecutionStage, err error) configsock.SandboxExecutionResult {
	result := configsock.SandboxExecutionResult{SID: sandboxID, RunID: runID, Stage: stage}
	if err == nil {
		code := 0
		result.ExitCode = &code
		return result
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		if status, ok := exitErr.Sys().(syscall.WaitStatus); ok {
			if status.Exited() {
				code := status.ExitStatus()
				result.ExitCode = &code
			} else if status.Signaled() {
				result.Signal = status.Signal().String()
			}
		}
	}
	result.Error = sanitizeSandboxResultError(err.Error())
	return result
}

func sanitizeSandboxResultError(message string) string {
	message = strings.Map(func(r rune) rune {
		if r == 0 || r == '\r' || r == '\n' || r == '\t' {
			return ' '
		}
		if r < 0x20 {
			return -1
		}
		return r
	}, message)
	message = strings.TrimSpace(message)
	if len(message) > 1024 {
		end := 1024
		// strings.Map produced valid UTF-8. Do not split its last rune: JSON
		// replaces invalid bytes with U+FFFD and could expand the wire value
		// past the store's 1024-byte result bound, losing durable acceptance.
		for !utf8.RuneStart(message[end]) {
			end--
		}
		message = message[:end]
	}
	return message
}

func postSandboxResultWithRetry(socket, sandboxID, runID string, result configsock.SandboxExecutionResult) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	delay := 100 * time.Millisecond
	var last error
	for attempt := 0; attempt < 5; attempt++ {
		callCtx, callCancel := context.WithTimeout(ctx, 5*time.Second)
		err := configsock.PostSandboxResultContext(callCtx, socket, runID, sandboxID, result)
		callCancel()
		if err == nil || !configsock.IsRetryableError(err) {
			return err
		}
		last = err
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return errors.Join(ctx.Err(), last)
		case <-timer.C:
		}
		if delay < time.Second {
			delay *= 2
			if delay > time.Second {
				delay = time.Second
			}
		}
	}
	return last
}
