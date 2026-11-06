package orch

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/launcher"
)

func TestResourcePoolSystemdChild(t *testing.T) {
	if os.Getenv("KUASAR_POOL_UNIT_CHILD") != "1" {
		t.Skip("systemd fixture child")
	}
	b := make([]byte, 32<<20)
	for i := 0; i < len(b); i += os.Getpagesize() {
		b[i] = 1
	}
	if err := os.WriteFile(filepath.Join(os.Getenv("KUASAR_POOL_READY_DIR"), strconv.Itoa(os.Getpid())), []byte("ready"), 0600); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Minute)
	runtime.KeepAlive(b)
}

// Opt-in real systemd regression. Generated templates and slices live in an
// isolated directory; only uniquely named templates/drop-ins are linked into
// /run. The fixed deployment slices are never installed or changed.
func TestResourcePoolSystemdPlacement(t *testing.T) {
	if os.Getenv("REQUIRE_RESOURCE_POOL_SYSTEMD") != "1" {
		t.Skip("requires explicit privileged systemd fixture")
	}
	if os.Geteuid() != 0 {
		t.Fatal("systemd fixture requires root")
	}
	nodes := strings.Fields(os.Getenv("RESOURCE_POOL_NUMA_NODES"))
	if len(nodes) == 0 {
		nodes = []string{"0"}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	lc, err := launcher.NewSystemd(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer lc.Close()
	root := t.TempDir()
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{Units: config.UnitsConfig{Dir: filepath.Join(root, "units")}, ResourceListen: &config.ResourceListenConfig{}}
	cfg.Paths.RunRoot = root
	prefix := fmt.Sprintf("kuasarpooltest%d", os.Getpid())
	type fixture struct{ template, node, cpu string }
	var fixtures []fixture
	for _, node := range nodes {
		if _, err := strconv.ParseUint(node, 10, 16); err != nil {
			t.Fatal(err)
		}
		cpus, err := os.ReadFile("/sys/devices/system/node/node" + node + "/cpulist")
		if err != nil {
			t.Fatal(err)
		}
		cpu := strings.FieldsFunc(strings.TrimSpace(string(cpus)), func(r rune) bool { return r == ',' || r == '-' })[0]
		source := filepath.Join(root, "numa"+node+".conf")
		content := fmt.Sprintf("[Service]\nExecStart=\nExecStart=%s -test.run=TestResourcePoolSystemdChild\nExecStopPost=\nSlice=system.slice\nEnvironment=KUASAR_POOL_UNIT_CHILD=1 KUASAR_POOL_READY_DIR=%s\nCPUAffinity=\nCPUAffinity=%s\nAllowedCPUs=\nAllowedCPUs=%s\nNUMAPolicy=bind\nNUMAMask=\nNUMAMask=%s\nAllowedMemoryNodes=\nAllowedMemoryNodes=%s\n", binary, root, cpu, cpu, node, node)
		if err := os.WriteFile(source, []byte(content), 0600); err != nil {
			t.Fatal(err)
		}
		name := "numa" + node
		memory := "64MiB"
		cfg.ResourceListen.Pools = append(cfg.ResourceListen.Pools, config.ResourcePoolConfig{Name: name, Allocatable: config.ResourceAllocatable{Memory: &memory}, UnitDropins: []string{source}})
		for _, kind := range []string{"runner", "builder"} {
			unit := prefix + kind + node + "@.service"
			pool := config.RunPoolConfig{Name: kind + node, ResourcePool: name, Unit: unit}
			if kind == "runner" {
				cfg.Units.RunnerPools = append(cfg.Units.RunnerPools, pool)
			} else {
				cfg.Units.BuilderPools = append(cfg.Units.BuilderPools, pool)
			}
			fixtures = append(fixtures, fixture{unit, node, cpu})
		}
	}
	o := New(cfg, nil, lc, nil, slog.Default())
	if err := o.InstallUnits(ctx); err != nil {
		t.Fatal(err)
	}
	var links, started []string
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		for _, unit := range started {
			if err := lc.Stop(cleanup, unit); err != nil {
				t.Error(err)
			}
			_ = lc.ResetFailed(cleanup, unit)
		}
		for _, link := range links {
			if err := os.Remove(link); err != nil {
				t.Error(err)
			}
		}
		if err := lc.Reload(cleanup); err != nil {
			t.Error(err)
		}
	}()
	for _, f := range fixtures {
		for _, suffix := range []string{"", ".d"} {
			link := filepath.Join("/run/systemd/system", f.template+suffix)
			if err := os.Symlink(filepath.Join(cfg.Units.Dir, f.template+suffix), link); err != nil {
				t.Fatal(err)
			}
			links = append(links, link)
		}
	}
	if err := lc.Reload(ctx); err != nil {
		t.Fatal(err)
	}
	show := func(unit, property string) string {
		out, err := exec.CommandContext(ctx, "systemctl", "show", "--value", "-p", property, unit).CombinedOutput()
		if err != nil {
			t.Fatalf("show %s: %s %v", property, out, err)
		}
		return strings.TrimSpace(string(out))
	}
	for _, f := range fixtures {
		unit := instanceUnit(f.template, "probe")
		started = append(started, unit)
		if err := lc.Start(ctx, unit); err != nil {
			t.Fatal(err)
		}
		pid := show(unit, "MainPID")
		for {
			if _, err := os.Stat(filepath.Join(root, pid)); err == nil {
				break
			}
			select {
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			case <-time.After(20 * time.Millisecond):
			}
		}
		status, err := os.ReadFile("/proc/" + pid + "/status")
		if err != nil {
			t.Fatal(err)
		}
		for key, want := range map[string]string{"Cpus_allowed_list:": f.cpu, "Mems_allowed_list:": f.node} {
			found := false
			for _, line := range strings.Split(string(status), "\n") {
				if strings.HasPrefix(line, key) {
					found = strings.TrimSpace(strings.TrimPrefix(line, key)) == want
				}
			}
			if !found {
				t.Fatalf("%s %s does not equal %s", unit, key, want)
			}
		}
		maps, err := os.ReadFile("/proc/" + pid + "/numa_maps")
		if err != nil {
			t.Fatal(err)
		}
		pages := 0
		for _, line := range strings.Split(string(maps), "\n") {
			if !strings.Contains(line, " bind:"+f.node+" ") || !strings.Contains(line, " anon=") {
				continue
			}
			for _, field := range strings.Fields(line) {
				if n, ok := strings.CutPrefix(field, "N"+f.node+"="); ok {
					count, _ := strconv.Atoi(n)
					pages += count
				}
			}
		}
		if pages*os.Getpagesize() < 32<<20 {
			t.Fatalf("%s anonymous pages not placed on node %s: %d pages", unit, f.node, pages)
		}
		cg := "/sys/fs/cgroup" + show(unit, "ControlGroup")
		for _, file := range []string{"memory.max", "cpu.max"} {
			value, err := os.ReadFile(filepath.Join(cg, file))
			if err != nil || !strings.HasPrefix(string(value), "max") {
				t.Fatalf("unexpected outer limit %s=%q (%v)", file, value, err)
			}
		}
		if !strings.Contains(show(unit, "DropInPaths"), "50-kuasar-resource-pool-0000.conf") {
			t.Fatal("managed resource drop-in not loaded")
		}
		t.Logf("%s pid=%s CPU=%s NUMA=%s anonymous=%d bytes; outer CPU/memory unlimited", unit, pid, f.cpu, f.node, pages*os.Getpagesize())
	}
}
