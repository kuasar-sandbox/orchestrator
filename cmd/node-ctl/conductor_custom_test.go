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
