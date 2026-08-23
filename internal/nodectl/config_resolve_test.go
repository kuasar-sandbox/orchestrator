package nodectl

import (
	"math"
	"os"
	"path/filepath"
	"strings"
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
	if resolved.SocketIdentity != resolved.Listen {
		t.Fatalf("socket identity = %q, want %q", resolved.SocketIdentity, resolved.Listen)
	}
}

func TestResolveCanonicalizesControllerSocketParentSymlink(t *testing.T) {
	root := t.TempDir()
	realDir := filepath.Join(root, "real")
	if err := os.Mkdir(realDir, 0o755); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(root, "alias")
	if err := os.Symlink(realDir, alias); err != nil {
		t.Fatal(err)
	}
	cfg := &config.ResourceListenConfig{Socket: filepath.Join(alias, "controller.sock")}
	cfg.Resources.PhysicalMemory = "32GiB"
	cfg.Resources.PhysicalCPU = "8"
	cfg.Resources.HostReserved.Memory = "1GiB"
	cfg.Resources.HostReserved.CPU = 1
	resolved, err := Resolve(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if want := filepath.Join(alias, "controller.sock"); resolved.Listen != want {
		t.Fatalf("bind socket = %q, want %q", resolved.Listen, want)
	}
	if want := filepath.Join(realDir, "controller.sock"); resolved.SocketIdentity != want {
		t.Fatalf("socket identity = %q, want %q", resolved.SocketIdentity, want)
	}
}

func TestResolveRejectsControllerSocketSymlink(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "controller.sock")
	alias := filepath.Join(root, "controller-alias.sock")
	if err := os.Symlink(target, alias); err != nil {
		t.Fatal(err)
	}
	cfg := &config.ResourceListenConfig{Socket: alias}
	cfg.Resources.PhysicalMemory = "32GiB"
	cfg.Resources.PhysicalCPU = "8"
	cfg.Resources.HostReserved.Memory = "1GiB"
	cfg.Resources.HostReserved.CPU = 1
	if _, err := Resolve(cfg); err == nil || !strings.Contains(err.Error(), "is a symlink") {
		t.Fatalf("socket symlink error = %v", err)
	}
}

func TestResolveRetainsShortBindableSocketAlias(t *testing.T) {
	root := t.TempDir()
	realDir := root
	for len(filepath.Join(realDir, "controller.sock")) <= 120 {
		realDir = filepath.Join(realDir, "deep-component")
	}
	if err := os.MkdirAll(realDir, 0o755); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(root, "short")
	if err := os.Symlink(realDir, alias); err != nil {
		t.Fatal(err)
	}
	cfg := &config.ResourceListenConfig{Socket: filepath.Join(alias, "controller.sock")}
	cfg.Resources.PhysicalMemory = "32GiB"
	cfg.Resources.PhysicalCPU = "8"
	cfg.Resources.HostReserved.Memory = "1GiB"
	cfg.Resources.HostReserved.CPU = 1
	resolved, err := Resolve(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Listen != cfg.Socket {
		t.Fatalf("bind path expanded to %q, want short alias %q", resolved.Listen, cfg.Socket)
	}
	if len(resolved.SocketIdentity) <= 107 {
		t.Fatalf("test identity is not beyond AF_UNIX limit: %q", resolved.SocketIdentity)
	}
}

func TestResolveRejectsUnsafeResourcePoolArithmetic(t *testing.T) {
	base := func() *config.ResourceListenConfig {
		cfg := &config.ResourceListenConfig{}
		cfg.Resources.PhysicalMemory = "32GiB"
		cfg.Resources.PhysicalCPU = "8"
		cfg.Resources.HostReserved.Memory = "1GiB"
		cfg.Resources.HostReserved.CPU = 1
		return cfg
	}
	tests := map[string]struct {
		mutate func(*config.ResourceListenConfig)
		want   string
	}{
		"host memory equals physical": {
			mutate: func(cfg *config.ResourceListenConfig) { cfg.Resources.HostReserved.Memory = "32GiB" },
			want:   "must be < physical_memory",
		},
		"host memory exceeds physical": {
			mutate: func(cfg *config.ResourceListenConfig) { cfg.Resources.HostReserved.Memory = "33GiB" },
			want:   "must be < physical_memory",
		},
		"operational factor is NaN": {
			mutate: func(cfg *config.ResourceListenConfig) { cfg.Watermarks.OperationalMarginFactor = math.NaN() },
			want:   "operational_margin_factor",
		},
		"zone factors are reversed": {
			mutate: func(cfg *config.ResourceListenConfig) {
				cfg.Watermarks.LowFactor, cfg.Watermarks.HighFactor = .9, .8
			},
			want: "low_factor < high_factor",
		},
		"emergency factor exceeds unit interval": {
			mutate: func(cfg *config.ResourceListenConfig) { cfg.Watermarks.EmergencyFactor = 1.1 },
			want:   "emergency_factor",
		},
		"grant factor exceeds unit interval": {
			mutate: func(cfg *config.ResourceListenConfig) { cfg.RateLimits.MemoryGrantPerSecFactor = 1.1 },
			want:   "memory_grant_per_sec_factor",
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			cfg := base()
			test.mutate(cfg)
			if _, err := Resolve(cfg); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Resolve error = %v, want %q", err, test.want)
			}
		})
	}
}
