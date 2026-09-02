package main

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCustomConductorExecFailureDoesNotFallbackBuiltIn(t *testing.T) {
	dir := shortNodeCtlTestDir(t)
	component := filepath.Join(dir, "xconductor")
	if err := os.WriteFile(component, []byte("not an executable image"), 0o500); err != nil {
		t.Fatal(err)
	}
	database := filepath.Join(dir, "must-not-exist.db")
	configPath := filepath.Join(dir, "conductor.yaml")
	body := "paths:\n" +
		"  conductor_executable: " + component + "\n" +
		"  run_root: " + filepath.Join(dir, "run") + "\n" +
		"  base_root: " + filepath.Join(dir, "base") + "\n" +
		"  db_path: " + database + "\n"
	if err := os.WriteFile(configPath, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	err := runConductor([]string{"--config", configPath}, logger)
	if err == nil || !strings.Contains(err.Error(), "exec custom conductor") {
		t.Fatalf("runConductor error=%v", err)
	}
	if _, err := os.Stat(database); !os.IsNotExist(err) {
		t.Fatalf("built-in fallback touched database: %v", err)
	}
}

func TestCustomProxyExecFailureDoesNotFallbackBuiltIn(t *testing.T) {
	directory := t.TempDir()
	component := filepath.Join(directory, "xproxy")
	if err := os.WriteFile(component, []byte("not an executable image"), 0o500); err != nil {
		t.Fatal(err)
	}
	sharedMemory := filepath.Join(directory, "must-not-exist.shm")
	configPath := filepath.Join(directory, "proxy.yaml")
	body := "paths:\n" +
		"  proxy_executable: " + component + "\n" +
		"  run_root: " + filepath.Join(directory, "run") + "\n" +
		"config_socket: " + filepath.Join(directory, "node-ctl.sock") + "\n" +
		"shm_path: " + sharedMemory + "\n"
	if err := os.WriteFile(configPath, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	err := runProxy([]string{"--config", configPath}, logger)
	if err == nil || !strings.Contains(err.Error(), "exec custom proxy") {
		t.Fatalf("runProxy error=%v", err)
	}
	if _, err := os.Stat(sharedMemory); !os.IsNotExist(err) {
		t.Fatalf("built-in fallback touched shared memory: %v", err)
	}
}

func TestBuiltInConductorFinalAndRuntimeValidationPrecedeCoreSideEffects(t *testing.T) {
	for name, body := range map[string]string{
		"final":            "paths:\n  db_path: %s\n",
		"runtime material": "api: { domain: built-in.test }\npaths:\n  db_path: %s\nsandbox:\n  boot: { kernel: /kernel, runtime: /runtime }\n",
	} {
		t.Run(name, func(t *testing.T) {
			t.Setenv("NODE_CONFIG_ENCRYPTION_KEY", "")
			database := filepath.Join(t.TempDir(), "must-not-exist.db")
			configPath := filepath.Join(t.TempDir(), "conductor.yaml")
			if err := os.WriteFile(configPath, []byte(fmt.Sprintf(body, database)), 0o600); err != nil {
				t.Fatal(err)
			}
			err := runConductor([]string{"--config", configPath}, slog.New(slog.NewTextHandler(io.Discard, nil)))
			if err == nil {
				t.Fatal("runConductor succeeded")
			}
			if name == "final" && !strings.Contains(err.Error(), "api.domain") {
				t.Fatalf("final validation error = %v", err)
			}
			if name == "runtime material" && !strings.Contains(err.Error(), "encryption keys") {
				t.Fatalf("runtime material error = %v", err)
			}
			if _, statErr := os.Stat(database); !os.IsNotExist(statErr) {
				t.Fatalf("validation failure touched database: %v", statErr)
			}
		})
	}
}

func TestBuiltInProxyFinalValidationPrecedesSharedMemoryCreation(t *testing.T) {
	directory := t.TempDir()
	sharedMemory := filepath.Join(directory, "must-not-exist.shm")
	configPath := filepath.Join(directory, "proxy.yaml")
	if err := os.WriteFile(configPath, []byte("shm_path: "+sharedMemory+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	err := runProxy([]string{"--config", configPath}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err == nil || !strings.Contains(err.Error(), "paths.run_root") {
		t.Fatalf("runProxy final validation error = %v", err)
	}
	if _, statErr := os.Stat(sharedMemory); !os.IsNotExist(statErr) {
		t.Fatalf("final validation touched shared memory: %v", statErr)
	}
}

func TestProxyWorkerFlagIsNotAPublicEntryPoint(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	err := runProxy([]string{"--worker"}, logger)
	if err == nil || !strings.Contains(err.Error(), "flag provided but not defined") {
		t.Fatalf("runProxy --worker error=%v", err)
	}
}
