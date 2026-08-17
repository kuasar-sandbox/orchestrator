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

func TestRetryBuildConfigSocketRetriesReportTransportFailure(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	missingSocket := filepath.Join(t.TempDir(), "restarting.sock")
	var attempts atomic.Int32
	err := retryBuildConfigSocket(context.Background(), log, "phase", func(ctx context.Context) error {
		if attempts.Add(1) < 3 {
			return configsock.PostBuildPhaseContext(ctx, missingSocket,
				"br-retry", "build-retry", "a", "bp-a-retry", "starting")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("retryBuildConfigSocket: %v", err)
	}
	if got := attempts.Load(); got != 3 {
		t.Fatalf("report attempts = %d, want 3", got)
	}
}

func TestRetryBuildConfigSocketRetriesAssignmentAndSpecTransportFailures(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	missingSocket := filepath.Join(t.TempDir(), "restarting.sock")
	for name, interrupted := range map[string]func(context.Context) error{
		"assignment": func(ctx context.Context) error {
			_, err := configsock.WaitAssignment(ctx, missingSocket, "build", "br-retry")
			return err
		},
		"build spec": func(ctx context.Context) error {
			_, err := configsock.FetchBuildSpecContext(ctx, missingSocket, "build:retry")
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			var attempts atomic.Int32
			err := retryBuildConfigSocket(context.Background(), log, name, func(ctx context.Context) error {
				if attempts.Add(1) < 3 {
					return interrupted(ctx)
				}
				return nil
			})
			if err != nil || attempts.Load() != 3 {
				t.Fatalf("%s retry = %v after %d attempts", name, err, attempts.Load())
			}
		})
	}
}

func TestRetryBuildConfigSocketDoesNotRetryProviderRejection(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	want := errors.New("execution ownership lost")
	var attempts atomic.Int32
	err := retryBuildConfigSocket(context.Background(), log, "result", func(context.Context) error {
		attempts.Add(1)
		return want
	})
	if !errors.Is(err, want) || attempts.Load() != 1 {
		t.Fatalf("provider rejection = %v after %d attempts", err, attempts.Load())
	}
}
