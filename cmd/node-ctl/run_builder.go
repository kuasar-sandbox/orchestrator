package main

// run-builder is the ExecStart of sandbox-builder@<run-id>.service: the build
// pipeline orchestrator. It waits for a build assignment, fetches the BuildSpec and
// hands it to internal/builder, which drives up to three phases, each a
// microVM it spawns as a DIRECT child (sandbox-ctl run, in this unit's
// cgroup), reusing ONE pre-attached network slot sequentially:
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
//	C template — a PRODUCTION-runtime sandbox cold-booted from the final
//	             image (the runtime ref freezes into the snapshot — template
//	             children must not inherit the builder toolchain); startCmd
//	             launches THROUGH ENVD and stays an envd-MANAGED process in
//	             the snapshot (the stream is held until ready, then dropped
//	             — envd never kills on stream loss), readyCmd polls every 2s
//	             to success, then sandbox-ctl snapshot writes the bundle.
//
// Two guest channels, deliberately distinct: e2b-SEMANTIC commands
// (steps/startCmd/readyCmd) go through envd exactly as e2b's own template
// build does; PLATFORM plumbing (flatten-ctl pulls/exports, config
// injection, artifact streaming, probes) goes through sandbox-ctl exec,
// which works on any rootfs and carries raw stdio.
//
// The finale uploads what was produced — platform credentials appear ONLY
// here: an image-only build runs `manifest-ctl store image.img`; a snapshot build
// runs ONE `sandbox-ctl upload-snapshot` (it publishes every local artifact the
// snapshot.cfg references, the base image included, to the configured portable
// backend). The result returns over the config-socket.

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
	"github.com/kuasar-sandbox/orchestrator/internal/tasksnapshot"
	"github.com/kuasar-sandbox/sandboxer/pkg/restore"
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
	if (bootstrap.Final == nil) == (bootstrap.Prepare == nil) {
		return fmt.Errorf("build task bootstrap must contain exactly one of final or prepare")
	}
	if _, ok := bootstrap.Env["MANIFEST_KEY"]; !ok {
		return fmt.Errorf("build task bootstrap has no authoritative manifest key")
	}
	if err := installTaskEnvironment(bootstrap.Env, os.Setenv); err != nil {
		return err
	}

	taskCtx := ctx
	cancelDeadline := func() {}
	if bootstrap.Prepare != nil {
		deadline := time.Unix(0, bootstrap.Prepare.AbsoluteDeadlineUnixNano)
		if bootstrap.Prepare.AbsoluteDeadlineUnixNano <= 0 || !deadline.After(time.Now()) {
			return fmt.Errorf("build task bootstrap deadline has expired")
		}
		taskCtx, cancelDeadline = context.WithDeadline(ctx, deadline)
	}
	defer cancelDeadline()

	spec := bootstrap.Final
	var prepared *tasksnapshot.Result
	var localPreparation *configsock.BuildSnapshotPreparation
	if bootstrap.Prepare != nil {
		prepared, err = tasksnapshot.Prepare(taskCtx, *bootstrap.Prepare)
		if err == nil {
			localPreparation, err = buildSnapshotPreparation(prepared.RootCfg)
		}
		if err == nil {
			log.Info("build task snapshot prepared", "bid", bid, "run_id", *runID,
				"task_snapshot_prepare_duration", prepared.PrepareDuration,
				"task_snapshot_cfg_read_duration", prepared.ConfigReadDuration,
				"task_snapshot_ref_count", prepared.Summary.RequiredRefCount)
			err = retryBuildConfigSocket(taskCtx, log, "build prepare", func(callCtx context.Context) error {
				var callErr error
				spec, callErr = configsock.CompleteBuildPrepare(callCtx, *socket, bid, *runID, prepared.Summary)
				return callErr
			})
		}
		if err != nil {
			log.Error("build task snapshot prepare failed", "bid", bid, "run_id", *runID,
				"task_snapshot_prepare_error_total", 1, "stage", "snapshot_prepare", "err", err)
			postErr := retryBuildConfigSocket(taskCtx, log, "result", func(callCtx context.Context) error {
				return configsock.PostBuildResultContext(callCtx, *socket, *runID, bid, configsock.BuildResult{Error: err.Error()})
			})
			if postErr != nil {
				return fmt.Errorf("prepare build snapshot: %w (post result: %v)", err, postErr)
			}
			return fmt.Errorf("prepare build snapshot: %w", err)
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
		spec.SnapshotPreparation = localPreparation
		spec.RefLocations = prepared.RefLocationURIs
	}
	if spec.Workdir != "" {
		if err := os.Chdir(spec.Workdir); err != nil {
			return fmt.Errorf("chdir %s: %w", spec.Workdir, err)
		}
	}

	reportPhase := func(phase, sandboxID, state string) error {
		return retryBuildConfigSocket(taskCtx, log, "phase", func(callCtx context.Context) error {
			return configsock.PostBuildPhaseContext(callCtx, *socket, *runID, bid, phase, sandboxID, state)
		})
	}
	res := builder.Run(spec, vmmCgroup, reportPhase, log)
	post := configsock.BuildResult{
		ImageRef: res.ImageRef, SnapshotRef: res.SnapshotRef,
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

func buildSnapshotPreparation(cfg *restore.SnapshotCfg) (*configsock.BuildSnapshotPreparation, error) {
	if cfg == nil {
		return nil, fmt.Errorf("build snapshot root config is nil")
	}
	if cfg.Boot.Root.BaseRef == "" {
		return nil, fmt.Errorf("build snapshot has no base image ref (boot.root.base_ref)")
	}
	prepared := &configsock.BuildSnapshotPreparation{
		BaseRef:  cfg.Boot.Root.BaseRef,
		StartCmd: cfg.Metadata["e2b.start_cmd"],
		ReadyCmd: cfg.Metadata["e2b.ready_cmd"],
	}
	if cfg.Boot.Root.Overlay != nil {
		prepared.OverlayBase = cfg.Boot.Root.Overlay.Base
		prepared.OverlayBaseFromRefs = append([]string(nil), cfg.Boot.Root.Overlay.BaseFromRefs...)
	}
	return prepared, nil
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
	return configsock.BuildPidfile(runRoot, buildID)
}
