package main

// run-builder is the ExecStart of sandbox-builder@<run-id>.service: the build
// pipeline orchestrator. It waits for a build assignment, fetches the BuildSpec and
// hands it to internal/builder, which drives the target-selected work. Runtime
// phases are microVMs spawned as DIRECT children (sandbox-ctl run, in this
// unit's cgroup), reusing ONE pre-attached network slot sequentially:
//
//	A import   — an EMPTY single-disk sandbox on the single guest runtime
//	             (flatten-ctl/mkfs.erofs ride /opt/sandbox-runtime, bind-
//	             mounted into any rootfs); flatten-ctl pulls the image WITH
//	             TENANT creds (exec env, never host-side) over the tenant
//	             network, flattens, and streams the tarstream image artifact
//	             back over exec stdio.
//	B steps    — an e2b-shaped sandbox whose root is the (local or template)
//	             base image, with envd as the app; RUN steps execute THROUGH
//	             ENVD (the e2b exec channel, envdExec) with the accumulated
//	             ENV/WORKDIR/USER context seeded from the base image config
//	             (ARG substitutes only); then flatten-ctl exports the rootfs
//	             (its tmpdir/output home is a self-bind mountpoint, excluded
//	             by --skip-mounts) and streams the new image artifact back.
//	C memory   — only for target={sandbox,memory:true}: a production-runtime
//	             cold Sandbox using the final image as a complete replacement
//	             boot. Optional startCmd runs through envd; readyCmd polls every
//	             2s, or no readyCmd waits a fixed 20 seconds, before the memory
//	             snapshot is captured.
//
// Two guest channels, deliberately distinct: e2b-SEMANTIC commands
// (steps/startCmd/readyCmd) go through envd exactly as e2b's own template
// build does; PLATFORM plumbing (flatten-ctl pulls/exports, config
// injection, artifact streaming, probes) goes through sandbox-ctl exec,
// which works on any rootfs and carries raw stdio.
//
// Image targets publish the final image directly. A top-level Sandbox E is
// assembled without another VM and streamed into its final publisher. Memory
// Sandbox targets publish the final image before C, then publish the resulting
// S -> E graph. The result returns over the config-socket with its resolved
// target and exactly one ref.

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/builder"
	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/nodepath"
	"github.com/kuasar-sandbox/orchestrator/internal/regcreds"
	"github.com/kuasar-sandbox/orchestrator/internal/taskartifact"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func runBuilder(args []string, log *slog.Logger) error {
	fs := flag.NewFlagSet("run-builder", flag.ExitOnError)
	pidfile := fs.String("pidfile", "", "pidfile to lock+write (TASK_PIDFILE)")
	socket := fs.String("config-socket", "", "config-socket UDS (TASK_CONFIG_SOCKET)")
	runID := fs.String("run-id", "", "run id (TASK_RUN_ID)")
	_ = fs.Parse(args)
	envDefault(pidfile, "TASK_PIDFILE")
	envDefault(socket, "TASK_CONFIG_SOCKET")
	envDefault(runID, "TASK_RUN_ID")
	if *pidfile == "" || *socket == "" || *runID == "" {
		return fmt.Errorf("run-builder: --pidfile, --config-socket, and --run-id required")
	}
	if err := lockPidfile(*pidfile); err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	vmmCgroup, err := prepareBuilderCgroup(*runID)
	if err != nil {
		return fmt.Errorf("prepare builder cgroup: %w", err)
	}
	defer vmmCgroup.Close()
	var bid string
	err = retryBuildConfigSocket(ctx, log, "assignment", func(callCtx context.Context) error {
		var callErr error
		bid, callErr = configsock.WaitAssignment(callCtx, *socket, "build", *runID)
		return callErr
	})
	if err != nil {
		return fmt.Errorf("wait assignment: %w", err)
	}
	if err := lockPidfile(builderAssignmentPidfile(*pidfile, bid)); err != nil {
		return err
	}
	var bootstrap *configsock.BuildTaskSpec
	err = retryBuildConfigSocket(ctx, log, "build bootstrap", func(callCtx context.Context) error {
		var callErr error
		bootstrap, callErr = configsock.FetchBuildTaskSpec(callCtx, *socket, bid, *runID)
		return callErr
	})
	if err != nil {
		postErr := retryBuildConfigSocket(ctx, log, "result", func(callCtx context.Context) error {
			return configsock.PostBuildResultContext(callCtx, *socket, *runID, bid, configsock.BuildResult{Error: err.Error()})
		})
		if postErr != nil {
			return fmt.Errorf("fetch build spec: %w (post result: %v)", err, postErr)
		}
		return fmt.Errorf("fetch build spec: %w", err)
	}
	if bootstrap.BuildID != bid || bootstrap.RunID != *runID {
		return fmt.Errorf("build task bootstrap identity mismatch")
	}
	if bootstrap.Prepare != nil && bootstrap.Prepare.RunID != *runID {
		return fmt.Errorf("build preparation RunID mismatch")
	}
	if (bootstrap.Final == nil) == (bootstrap.Prepare == nil) {
		return fmt.Errorf("build task bootstrap must contain exactly one of final or prepare")
	}
	if err := installBuildTaskEnvironment(bootstrap.Env, os.Setenv, os.Unsetenv); err != nil {
		return err
	}

	taskCtx := ctx
	cancelDeadline := func() {}
	// workCtx stops before taskCtx so every preparation/phase failure still has
	// a bounded tail in which the exact-run result can be durably reported.
	workCtx := ctx
	cancelWork := func() {}
	deadlineUnixNano := buildTaskAbsoluteDeadline(bootstrap)
	if deadlineUnixNano > 0 {
		deadline := time.Unix(0, deadlineUnixNano)
		if !deadline.After(time.Now()) {
			return fmt.Errorf("build task bootstrap deadline has expired")
		}
		taskCtx, cancelDeadline = context.WithDeadline(ctx, deadline)
		workCtx, cancelWork = context.WithDeadline(taskCtx, builder.PreResultDeadline(deadline, time.Now()))
	} else if bootstrap.Prepare != nil {
		return fmt.Errorf("build task source bootstrap has no absolute deadline")
	}
	defer cancelDeadline()
	defer cancelWork()

	spec := bootstrap.Final
	var prepared *taskartifact.Result
	if bootstrap.Prepare != nil {
		prepared, err = taskartifact.Prepare(workCtx, *bootstrap.Prepare)
		if err == nil {
			log.Info("build task artifact prepared", "bid", bid, "run_id", *runID,
				"task_artifact_prepare_duration", prepared.PrepareDuration,
				"task_artifact_config_read_duration", prepared.ConfigReadDuration,
				"task_artifact_ref_count", prepared.Summary.RequiredRefCount)
			err = retryBuildConfigSocket(workCtx, log, "build prepare", func(callCtx context.Context) error {
				var callErr error
				spec, callErr = configsock.CompleteBuildPrepare(callCtx, *socket, bid, *runID, prepared.Summary)
				return callErr
			})
		}
		if err != nil {
			log.Error("build task artifact prepare failed", "bid", bid, "run_id", *runID,
				"task_artifact_prepare_error_total", 1, "stage", "artifact_prepare", "err", err)
			postErr := retryBuildConfigSocket(taskCtx, log, "result", func(callCtx context.Context) error {
				return configsock.PostBuildResultContext(callCtx, *socket, *runID, bid, configsock.BuildResult{
					Error: err.Error(), FailureStage: "artifact_prepare",
				})
			})
			if postErr != nil {
				return fmt.Errorf("prepare build source: %w (post result: %v)", err, postErr)
			}
			return fmt.Errorf("prepare build source: %w", err)
		}
	}
	if spec == nil {
		return fmt.Errorf("build task has no final build spec")
	}
	if spec.BuildID != bid || spec.RunID != *runID {
		return fmt.Errorf("final build spec identity mismatch")
	}
	spec.Env = mergeAuthoritativeEnv(spec.Env, bootstrap.Env)
	if prepared != nil {
		if prepared.PreparedSource.Kind != types.ResumeSourceSandbox || !prepared.PreparedSource.Valid() || prepared.SourceSandboxConfig == nil {
			return fmt.Errorf("prepared build source is not a complete Sandbox E")
		}
		spec.SourceSandboxRef = prepared.PreparedSource.Ref
		spec.SourceSandboxConfig = prepared.SourceSandboxConfig
		spec.SourceImageConfig = append([]byte(nil), prepared.SourceImageConfig...)
		spec.RefLocations = prepared.RefLocationURIs
	}
	if !filepath.IsAbs(spec.RunDir) || !filepath.IsAbs(spec.BaseDir) {
		return fmt.Errorf("final build spec requires absolute run_dir and base_dir")
	}
	if err := os.Chdir(spec.RunDir); err != nil {
		return fmt.Errorf("chdir %s: %w", spec.RunDir, err)
	}

	reportPhase := func(phase, sandboxID, state string) error {
		return retryBuildConfigSocket(workCtx, log, "phase", func(callCtx context.Context) error {
			return configsock.PostBuildPhaseContext(callCtx, *socket, *runID, bid, phase, sandboxID, state)
		})
	}
	res := builder.Run(workCtx, spec, vmmCgroup, reportPhase, log)
	post := configsock.BuildResult{
		Target: res.Target, ImageRef: res.ImageRef, SandboxRef: res.SandboxRef, SnapshotRef: res.SnapshotRef,
		StartCmd: res.StartCmd, ReadyCmd: res.ReadyCmd, Error: res.Error,
	}
	if err := retryBuildConfigSocket(taskCtx, log, "result", func(callCtx context.Context) error {
		return configsock.PostBuildResultContext(callCtx, *socket, *runID, bid, post)
	}); err != nil {
		return fmt.Errorf("post build result: %w", err)
	}
	if res.Error != "" {
		return fmt.Errorf("build failed: %s", res.Error)
	}
	return nil
}

// installBuildTaskEnvironment gives the task-local artifact reader only the
// process-wide authority it needs. Registry credentials remain in the
// authenticated bootstrap map and are merged into BuildSpec.Env for explicit
// host/guest calls; they must never become ambient phase-process environment.
func installBuildTaskEnvironment(
	env map[string]string,
	setenv func(string, string) error,
	unsetenv func(string) error,
) error {
	if _, ok := env["MANIFEST_KEY"]; !ok {
		return fmt.Errorf("build task bootstrap has no authoritative manifest key")
	}
	if setenv == nil {
		return fmt.Errorf("build task environment installer is not configured")
	}
	if unsetenv == nil {
		return fmt.Errorf("build task environment cleaner is not configured")
	}
	for _, key := range []string{regcreds.EnvToken, regcreds.EnvUsername, regcreds.EnvPassword} {
		if err := unsetenv(key); err != nil {
			return fmt.Errorf("clear inherited build credential %s: %w", key, err)
		}
	}
	return installTaskEnvironment(env, func(key, value string) error {
		if key != "MANIFEST_KEY" {
			return nil
		}
		return setenv(key, value)
	})
}

func buildTaskAbsoluteDeadline(task *configsock.BuildTaskSpec) int64 {
	if task == nil {
		return 0
	}
	if task.Prepare != nil {
		return task.Prepare.AbsoluteDeadlineUnixNano
	}
	if task.Final != nil {
		return task.Final.Timeouts.AbsoluteDeadlineUnixNano
	}
	return 0
}

func retryBuildConfigSocket(ctx context.Context, log *slog.Logger, operation string, call func(context.Context) error) error {
	delay := 20 * time.Millisecond
	for attempt := 1; ; attempt++ {
		err := call(ctx)
		if err == nil {
			return nil
		}
		if !configsock.IsRetryableError(err) {
			return err
		}
		if attempt == 1 {
			log.Warn("builder config-socket operation temporarily unavailable; retrying", "operation", operation, "err", err)
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
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

func builderAssignmentPidfile(runPidfile, buildID string) string {
	runRoot := filepath.Dir(filepath.Dir(runPidfile))
	return filepath.Join(nodepath.BuildRunDir(runRoot, buildID), "builder.pid")
}
