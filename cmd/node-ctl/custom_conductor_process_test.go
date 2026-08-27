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

	"github.com/kuasar-sandbox/orchestrator/app/conductor"
)

const customConductorProcessMode = "KUASAR_CUSTOM_CONDUCTOR_PROCESS_TEST"

func TestMain(m *testing.M) {
	switch os.Getenv(customConductorProcessMode) {
	case "node-ctl":
		_ = os.Setenv(customConductorProcessMode, "xconductor")
		logger := slog.New(slog.NewTextHandler(io.Discard, nil))
		if err := runConductor([]string{"--config", os.Getenv("KUASAR_CUSTOM_CONDUCTOR_CONFIG")}, logger); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(91)
		}
		os.Exit(92)
	case "xconductor":
		app := conductor.New(conductor.Hooks{Configure: func(_ context.Context, cfg *conductor.Config, _ *conductor.Runtime) error {
			fmt.Printf("%d\n%s\n", os.Getpid(), cfg.Cluster.Labels["process-test"])
			return errors.New("stop before core")
		}})
		err := app.RunContext(context.Background())
		if err == nil || !strings.Contains(err.Error(), "stop before core") {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(93)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func TestNodeCtlExecsCustomConductorAppInPlace(t *testing.T) {
	dir := t.TempDir()
	source, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	component := filepath.Join(dir, "xconductor")
	copyExecutable(t, source, component)
	configPath := filepath.Join(dir, "conductor.yaml")
	body := "paths:\n  conductor_executable: " + component + "\ncluster:\n  labels:\n    process-test: bootstrap-received\n"
	if err := os.WriteFile(configPath, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	command := exec.Command(source, "-test.run=^$")
	command.Env = append(os.Environ(),
		customConductorProcessMode+"=node-ctl",
		"KUASAR_CUSTOM_CONDUCTOR_CONFIG="+configPath,
	)
	var output strings.Builder
	command.Stdout = &output
	command.Stderr = &output
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	wantPID := command.Process.Pid
	if err := command.Wait(); err != nil {
		t.Fatalf("custom conductor process: %v\n%s", err, output.String())
	}
	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	if len(lines) != 2 || lines[1] != "bootstrap-received" {
		t.Fatalf("output=%q", output.String())
	}
	gotPID, err := strconv.Atoi(lines[0])
	if err != nil {
		t.Fatal(err)
	}
	if gotPID != wantPID {
		t.Fatalf("xconductor PID=%d, node-ctl PID=%d", gotPID, wantPID)
	}
}

func copyExecutable(t *testing.T, source, destination string) {
	t.Helper()
	input, err := os.Open(source)
	if err != nil {
		t.Fatal(err)
	}
	defer input.Close()
	output, err := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o500)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(output, input); err != nil {
		output.Close()
		t.Fatal(err)
	}
	if err := output.Close(); err != nil {
		t.Fatal(err)
	}
}
