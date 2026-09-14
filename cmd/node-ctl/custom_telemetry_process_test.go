package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/app/telemetry"
)

const customTelemetryProcessMode = "KUASAR_CUSTOM_TELEMETRY_PROCESS_TEST"

func customTelemetryProcess() {
	switch os.Getenv(customTelemetryProcessMode) {
	case "node-ctl":
		_ = os.Setenv(customTelemetryProcessMode, "xtelemetry")
		logger := slog.New(slog.NewTextHandler(io.Discard, nil))
		if err := telemetryCmd([]string{"serve", "--config", os.Getenv("KUASAR_CUSTOM_TELEMETRY_CONFIG")}, logger); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(97)
		}
		os.Exit(98)
	case "xtelemetry":
		app := telemetry.New(telemetry.Hooks{Configure: func(_ context.Context, cfg *telemetry.Config, _ *telemetry.Runtime) error {
			fmt.Printf("%d\n%s\n%s\n", os.Getpid(), cfg.Telemetry.Scrape.Interval, cfg.ProxyNetNS)
			return errors.New("stop before telemetry core")
		}})
		err := app.RunContext(context.Background())
		if err == nil || !strings.Contains(err.Error(), "stop before telemetry core") {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(99)
		}
		os.Exit(0)
	}
}

func TestNodeCtlExecsCustomTelemetryInPlace(t *testing.T) {
	source, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	executable := filepath.Join(directory, "xtelemetry")
	copyExecutable(t, source, executable)
	configPath := filepath.Join(directory, "telemetry.yaml")
	body := "proxy_netns: sandbox-proxy\npaths:\n  telemetry_executable: " + executable + "\ntelemetry:\n  scrape:\n    interval: 7s\n"
	if err := os.WriteFile(configPath, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	command := exec.CommandContext(ctx, source, "-test.run=^$")
	command.Env = append(os.Environ(), customTelemetryProcessMode+"=node-ctl", "KUASAR_CUSTOM_TELEMETRY_CONFIG="+configPath)
	var output strings.Builder
	command.Stdout, command.Stderr = &output, &output
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	wantPID := command.Process.Pid
	if err := command.Wait(); err != nil {
		t.Fatalf("external telemetry: %v\n%s", err, output.String())
	}
	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	if len(lines) != 3 || lines[0] != strconv.Itoa(wantPID) || lines[1] != "7s" || lines[2] != "sandbox-proxy" {
		t.Fatalf("sealed config/PID not preserved: %q (PID %d)", output.String(), wantPID)
	}
}
