package main

// A go test -c binary supplies the existing E2E launch fault/observation seam.
// Only a sibling runner-e2e-workdir marker activates it. Production node-ctl
// contains neither the marker reader nor injection modes. The harness retains
// the actual RunID lock, session, assignment, preparation, signals and reporting.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxsdk"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func runnerE2EProcess() {
	exe, err := os.Executable()
	if err != nil {
		return
	}
	marker, err := os.ReadFile(filepath.Join(filepath.Dir(exe), "runner-e2e-workdir"))
	if errors.Is(err, os.ErrNotExist) {
		return
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	work := strings.TrimSpace(string(marker))
	if !filepath.IsAbs(work) || filepath.Dir(exe) != filepath.Join(work, "orch-bin") {
		fmt.Fprintln(os.Stderr, "invalid runner E2E workdir marker")
		os.Exit(2)
	}
	if len(os.Args) < 2 || os.Args[1] != "run-sandbox" {
		main()
		os.Exit(0)
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	err = runSandboxWithTask(os.Args[2:], logger, func(ctx context.Context, stop func(), socket, sid, runID string, ready, cgroup *os.File, log *slog.Logger) error {
		return launchTaskWithExecution(ctx, stop, socket, sid, runID, ready, cgroup, log,
			func(ctx context.Context, socket, sid, runID string, spec configsock.LaunchSpec, source types.ResumeSource, locations, env map[string]string, cgroup, ready *os.File) error {
				if err := recordRunnerE2ELaunch(work, spec, source, locations); err != nil {
					return err
				}
				return executeSandboxAndReportWith(ctx, sid, runID, spec, source, locations, env, cgroup, ready,
					func(ctx context.Context, input *sandboxsdk.Input, cg *os.File, readiness *sandboxsdk.Readiness) (int, error) {
						return executeRunnerE2E(ctx, work, input, cg, ready, readiness)
					}, func(result configsock.SandboxExecutionResult) error {
						return postSandboxResultWithRetry(socket, sid, runID, result)
					})
			})
	})
	if err != nil {
		logger.Error("node-ctl", "cmd", "run-sandbox", "err", err)
		os.Exit(1)
	}
	os.Exit(0)
}
