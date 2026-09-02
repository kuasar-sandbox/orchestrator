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
	customproxy "github.com/kuasar-sandbox/orchestrator/app/proxy"
)

const (
	customConductorProcessMode = "KUASAR_CUSTOM_CONDUCTOR_PROCESS_TEST"
	customProxyProcessMode     = "KUASAR_CUSTOM_PROXY_PROCESS_TEST"
)

func TestMain(m *testing.M) {
	switch os.Getenv(customProxyProcessMode) {
	case "node-ctl":
		_ = os.Setenv(customProxyProcessMode, "xproxy")
		logger := slog.New(slog.NewTextHandler(io.Discard, nil))
		if err := runProxy([]string{"--config", os.Getenv("KUASAR_CUSTOM_PROXY_CONFIG")}, logger); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(94)
		}
		os.Exit(95)
	case "xproxy":
		app := customproxy.New(customproxy.Hooks{
			Configure: func(_ context.Context, cfg *customproxy.Config) error {
				fmt.Printf("%d\n%s\n", os.Getpid(), cfg.Auth)
				return nil
			},
			BindRuntime: func(_ context.Context, process customproxy.Process, _ *customproxy.Runtime) error {
				fmt.Printf("%s\n", process.Role)
				return errors.New("stop before proxy core")
			},
		})
		err := app.RunContext(context.Background())
		if err == nil || !strings.Contains(err.Error(), "stop before proxy core") {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(96)
		}
		os.Exit(0)
	}
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

func TestNodeCtlExecsCustomProxyMasterInPlace(t *testing.T) {
	directory := t.TempDir()
	source, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	component := filepath.Join(directory, "xproxy")
	copyExecutable(t, source, component)
	configPath := filepath.Join(directory, "proxy.yaml")
	body := "paths:\n  proxy_executable: " + component + "\n  run_root: " + filepath.Join(directory, "run") + "\ndata_listen: 127.0.0.1:8443\nauth: log\n"
	if err := os.WriteFile(configPath, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	command := exec.Command(source, "-test.run=^$")
	command.Env = append(os.Environ(),
		customProxyProcessMode+"=node-ctl",
		"KUASAR_CUSTOM_PROXY_CONFIG="+configPath,
	)
	var output strings.Builder
	command.Stdout = &output
	command.Stderr = &output
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	wantPID := command.Process.Pid
	if err := command.Wait(); err != nil {
		t.Fatalf("custom proxy process: %v\n%s", err, output.String())
	}
	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	if len(lines) != 3 || lines[1] != "log" || lines[2] != "master" {
		t.Fatalf("output=%q", output.String())
	}
	gotPID, err := strconv.Atoi(lines[0])
	if err != nil {
		t.Fatal(err)
	}
	if gotPID != wantPID {
		t.Fatalf("xproxy PID=%d, node-ctl PID=%d", gotPID, wantPID)
	}
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
