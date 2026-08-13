package nodectl

import (
	"path/filepath"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/config"
)

func TestResolveCanonicalizesControllerSocket(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	cfg := &config.ResourceListenConfig{Socket: "./controller.sock"}
	cfg.Resources.PhysicalMemory = "32GiB"
	cfg.Resources.PhysicalCPU = "8"
	cfg.Resources.HostReserved.Memory = "1GiB"
	cfg.Resources.HostReserved.CPU = 1
	resolved, err := Resolve(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if want := filepath.Join(dir, "controller.sock"); resolved.Listen != want {
		t.Fatalf("resolved controller socket = %q, want %q", resolved.Listen, want)
	}
}
