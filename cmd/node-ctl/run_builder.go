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
// runs ONE `sandbox-ctl upload-snapshot` (it auto-uploads every local artifact the
// snapshot.cfg references, the base image included, and rewrites the refs to
// manifest://). The result returns over the config-socket.

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/kuasar-sandbox/orchestrator/internal/builder"
	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
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
	if *socket == "" || *runID == "" {
		return fmt.Errorf("run-builder: --config-socket and --run-id required")
	}
	if *pidfile != "" {
		if err := lockPidfile(*pidfile); err != nil {
			return err
		}
	}
	bid, err := configsock.WaitAssignment(context.Background(), *socket, "build", *runID)
	if err != nil {
		return fmt.Errorf("wait assignment: %w", err)
	}
	if *pidfile == "" {
		return fmt.Errorf("run-builder: --pidfile required for build assignment")
	}
	runRoot := filepath.Dir(filepath.Dir(*pidfile))
	if err := lockPidfile(filepath.Join(runRoot, bid, bid+".pid")); err != nil {
		return err
	}
	spec, err := configsock.FetchBuildSpec(*socket, "build:"+bid)
	if err != nil {
		postErr := configsock.PostBuildResult(*socket, *runID, bid, configsock.BuildResult{Error: err.Error()})
		if postErr != nil {
			return fmt.Errorf("fetch build spec: %w (post result: %v)", err, postErr)
		}
		return fmt.Errorf("fetch build spec: %w", err)
	}
	if spec.Workdir != "" {
		if err := os.Chdir(spec.Workdir); err != nil {
			return fmt.Errorf("chdir %s: %w", spec.Workdir, err)
		}
	}

	res := builder.Run(spec, log)
	post := configsock.BuildResult{
		ImageKey: res.ImageKey, SnapshotKey: res.SnapshotKey,
		StartCmd: res.StartCmd, ReadyCmd: res.ReadyCmd, Error: res.Error,
	}
	if err := configsock.PostBuildResult(*socket, *runID, bid, post); err != nil {
		return fmt.Errorf("post build result: %w", err)
	}
	if res.Error != "" {
		return fmt.Errorf("build failed: %s", res.Error)
	}
	return nil
}
