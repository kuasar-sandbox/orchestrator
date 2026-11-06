package orch

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/config"
)

func TestResourcePoolDropins(t *testing.T) {
	dir := t.TempDir()
	source := filepath.Join(t.TempDir(), "numa.conf")
	content := "[Service]\nCPUAffinity=0 1\nNUMAPolicy=bind\nNUMAMask=0\n"
	if err := os.WriteFile(source, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{Units: config.UnitsConfig{Dir: dir, RunnerPools: []config.RunPoolConfig{{Unit: "r@.service", ResourcePool: "numa0"}, {Unit: "r@.service", ResourcePool: "numa0"}}, BuilderPools: []config.RunPoolConfig{{Unit: "b@.service", ResourcePool: "numa0"}}}, ResourceListen: &config.ResourceListenConfig{Pools: []config.ResourcePoolConfig{{Name: "numa0", UnitDropins: []string{source}}}}}
	lc := &reconcileLauncher{}
	o := New(cfg, nil, lc, nil, slog.Default())
	if err := o.InstallUnits(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, unit := range []string{"r@.service", "b@.service"} {
		got, err := os.ReadFile(filepath.Join(dir, unit+".d", "50-kuasar-resource-pool-0000.conf"))
		if err != nil || string(got) != content {
			t.Fatalf("%s: %q %v", unit, got, err)
		}
	}
	admin := filepath.Join(dir, "r@.service.d", "20-admin.conf")
	if err := os.WriteFile(admin, []byte("[Service]\nNice=1\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := o.InstallUnits(context.Background()); err != nil {
		t.Fatal(err)
	}
	if lc.reloads != 1 {
		t.Fatal("unchanged dropins reloaded")
	}
	cfg.ResourceListen.Pools[0].UnitDropins = nil
	if err := o.InstallUnits(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(admin); err != nil {
		t.Fatal("administrator fragment removed", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "r@.service.d", "50-kuasar-resource-pool-0000.conf")); !os.IsNotExist(err) {
		t.Fatal("stale fragment retained")
	}
	no := false
	cfg.Units.Install = &no
	cfg.ResourceListen.Pools[0].UnitDropins = []string{source + ".missing"}
	if err := o.InstallUnits(context.Background()); err != nil {
		t.Fatal("install=false read source", err)
	}
}
