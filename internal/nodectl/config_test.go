package nodectl

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/config"
)

func TestResolveIgnoresMissingOrCorruptDeprecatedState(t *testing.T) {
	statePath := filepath.Join(t.TempDir(), "state.json")
	corrupt := []byte("{not-json\n")
	if err := os.WriteFile(statePath, corrupt, 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := &config.ResourceListenConfig{
		StatePath: statePath,
		Resources: config.ResourceHostConfig{
			PhysicalMemory: "32GiB", PhysicalCPU: "8",
			HostReserved: config.ResourceHostReserved{Memory: "1GiB", CPU: 1},
		},
	}
	if _, err := Resolve(cfg); err != nil {
		t.Fatalf("Resolve consulted deprecated state_path: %v", err)
	}
	after, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != string(corrupt) {
		t.Fatalf("Resolve modified deprecated state file: %q", after)
	}

	cfg.StatePath = filepath.Join(t.TempDir(), "missing.json")
	if _, err := Resolve(cfg); err != nil {
		t.Fatalf("Resolve consulted missing deprecated state_path: %v", err)
	}
}
