package main

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCustomConductorExecFailureDoesNotFallbackBuiltIn(t *testing.T) {
	dir := t.TempDir()
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

func TestProxyWorkerFlagIsNotAPublicEntryPoint(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	err := runProxy([]string{"--worker"}, logger)
	if err == nil || !strings.Contains(err.Error(), "flag provided but not defined") {
		t.Fatalf("runProxy --worker error=%v", err)
	}
}
