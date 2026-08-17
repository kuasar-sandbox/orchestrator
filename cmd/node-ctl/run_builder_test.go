package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
)

func TestBuilderAssignmentPidfileMatchesBuildSpecRuntimeIdentity(t *testing.T) {
	runRoot := filepath.Join(t.TempDir(), "run")
	runPidfile := filepath.Join(runRoot, "runs", "br-test.pid")
	buildID := strings.Repeat("b", 36)

	got := builderAssignmentPidfile(runPidfile, buildID)
	want := configsock.BuildPidfile(runRoot, buildID)
	if got != want {
		t.Fatalf("assignment pidfile = %q, want %q", got, want)
	}
	if filepath.Base(got) != "builder.pid" || filepath.Dir(got) != configsock.BuildRuntimeDir(runRoot, buildID) {
		t.Fatalf("assignment pidfile does not use compact build runtime identity: %q", got)
	}
}

func TestRetryBuildReportRetriesConfigSocketTransportFailure(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	missingSocket := filepath.Join(t.TempDir(), "restarting.sock")
	var attempts atomic.Int32
	err := retryBuildReport(context.Background(), log, "phase", func(ctx context.Context) error {
		if attempts.Add(1) < 3 {
			return configsock.PostBuildPhaseContext(ctx, missingSocket,
				"br-retry", "build-retry", "a", "bp-a-retry", "starting")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("retryBuildReport: %v", err)
	}
	if got := attempts.Load(); got != 3 {
		t.Fatalf("report attempts = %d, want 3", got)
	}
}

func TestRetryBuildReportDoesNotRetryProviderRejection(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	want := errors.New("execution ownership lost")
	var attempts atomic.Int32
	err := retryBuildReport(context.Background(), log, "result", func(context.Context) error {
		attempts.Add(1)
		return want
	})
	if !errors.Is(err, want) || attempts.Load() != 1 {
		t.Fatalf("provider rejection = %v after %d attempts", err, attempts.Load())
	}
}
