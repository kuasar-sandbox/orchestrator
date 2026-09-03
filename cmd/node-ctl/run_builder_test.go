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
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/builder"
	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/nodepath"
)

func TestBuilderAssignmentPidfileMatchesBuildSpecRuntimeIdentity(t *testing.T) {
	runRoot := filepath.Join(t.TempDir(), "run")
	runPidfile := nodepath.RunnerPID(runRoot, "br-test")
	buildID := strings.Repeat("b", 36)

	got := builderAssignmentPidfile(runPidfile, buildID)
	want := filepath.Join(nodepath.BuildRunDir(runRoot, buildID), "builder.pid")
	if got != want {
		t.Fatalf("assignment pidfile = %q, want %q", got, want)
	}
	if filepath.Base(got) != "builder.pid" || filepath.Dir(got) != nodepath.BuildRunDir(runRoot, buildID) {
		t.Fatalf("assignment pidfile does not use BuildRunDir: %q", got)
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
		"build bootstrap": func(ctx context.Context) error {
			_, err := configsock.FetchBuildTaskSpec(ctx, missingSocket, "retry", "br-retry")
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

func TestBuildTaskAbsoluteDeadlineCoversFastAndPreparedSourcePaths(t *testing.T) {
	if got := buildTaskAbsoluteDeadline(&configsock.BuildTaskSpec{
		Final: &configsock.BuildSpec{Timeouts: configsock.BuildTimeouts{AbsoluteDeadlineUnixNano: 11}},
	}); got != 11 {
		t.Fatalf("fast path deadline = %d, want 11", got)
	}
	if got := buildTaskAbsoluteDeadline(&configsock.BuildTaskSpec{
		Prepare: &configsock.ArtifactPrepareSpec{AbsoluteDeadlineUnixNano: 22},
		Final:   &configsock.BuildSpec{Timeouts: configsock.BuildTimeouts{AbsoluteDeadlineUnixNano: 33}},
	}); got != 22 {
		t.Fatalf("prepared source path deadline = %d, want 22", got)
	}
}

func TestBuildTaskWorkDeadlineReservesFinalResultTail(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	absolute := now.Add(time.Minute)
	if got, want := builder.PreResultDeadline(absolute, now), absolute.Add(-5*time.Second); !got.Equal(want) {
		t.Fatalf("work deadline = %v, want %v", got, want)
	}
	shortAbsolute := now.Add(4 * time.Second)
	if got, want := builder.PreResultDeadline(shortAbsolute, now), now.Add(2*time.Second); !got.Equal(want) {
		t.Fatalf("short work deadline = %v, want %v", got, want)
	}
}

func TestInstallBuildTaskEnvironmentKeepsRegistryCredentialsNonAmbient(t *testing.T) {
	processEnv := map[string]string{
		"MANIFEST_KEY":              "inherited-manifest-key",
		"FLATTEN_REGISTRY_TOKEN":    "inherited-token",
		"FLATTEN_REGISTRY_USERNAME": "inherited-user",
		"FLATTEN_REGISTRY_PASSWORD": "inherited-password",
	}
	bootstrapEnv := map[string]string{
		"MANIFEST_KEY":              "authoritative-manifest-key",
		"FLATTEN_REGISTRY_TOKEN":    "task-token",
		"FLATTEN_REGISTRY_USERNAME": "task-user",
		"FLATTEN_REGISTRY_PASSWORD": "task-password",
		"BUILD_NON_SECRET":          "retained-only-in-spec",
	}
	setenv := func(key, value string) error {
		processEnv[key] = value
		return nil
	}
	unsetenv := func(key string) error {
		delete(processEnv, key)
		return nil
	}

	if err := installBuildTaskEnvironment(bootstrapEnv, setenv, unsetenv); err != nil {
		t.Fatal(err)
	}
	if got := processEnv["MANIFEST_KEY"]; got != "authoritative-manifest-key" {
		t.Fatalf("process MANIFEST_KEY = %q", got)
	}
	for _, key := range []string{"FLATTEN_REGISTRY_TOKEN", "FLATTEN_REGISTRY_USERNAME", "FLATTEN_REGISTRY_PASSWORD"} {
		if value, ok := processEnv[key]; ok {
			t.Fatalf("registry credential %s remained ambient as %q", key, value)
		}
	}
	if _, ok := processEnv["BUILD_NON_SECRET"]; ok {
		t.Fatal("non-reader bootstrap environment was installed process-wide")
	}
	if got := mergeAuthoritativeEnv(nil, bootstrapEnv)["FLATTEN_REGISTRY_TOKEN"]; got != "task-token" {
		t.Fatalf("registry credential was not retained for explicit spec use: %q", got)
	}
}
