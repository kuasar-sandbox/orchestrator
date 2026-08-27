package orch

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/configresolve"
	"github.com/kuasar-sandbox/orchestrator/internal/launcher"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func requireCollectModeInUnitSection(t *testing.T, name, unit string) {
	t.Helper()
	if !strings.HasPrefix(unit, "[Unit]\n") {
		t.Fatalf("%s unit does not start with [Unit]:\n%s", name, unit)
	}
	service := strings.Index(unit, "\n[Service]\n")
	if service < 0 {
		t.Fatalf("%s unit has no [Service] section:\n%s", name, unit)
	}
	const setting = "CollectMode=inactive-or-failed"
	if !strings.Contains(unit[:service], "\n"+setting+"\n") {
		t.Fatalf("%s [Unit] section missing %q:\n%s", name, setting, unit[:service])
	}
	if strings.Count(unit, setting) != 1 {
		t.Fatalf("%s unit must contain exactly one %q:\n%s", name, setting, unit)
	}
}

func TestGeneratedUnitsUseRunIDAssignment(t *testing.T) {
	o := &Orchestrator{cfg: &config.Config{
		Paths: config.PathsConfig{
			RunRoot:      "/run/kuasar-test",
			ConfigSocket: "/run/kuasar-test/node-ctl.socket",
		},
	}}
	runner := o.runnerUnitFile()
	requireCollectModeInUnitSection(t, "runner", runner)
	for _, want := range []string{
		"WorkingDirectory=/run/kuasar-test",
		"run-sandbox --pidfile=/run/kuasar-test/runs/%i.pid",
		"--run-id=%i",
		"ExecStopPost=/bin/rm -f /run/kuasar-test/runs/%i.pid",
		"KillMode=control-group",
		"Delegate=yes",
	} {
		if !strings.Contains(runner, want) {
			t.Fatalf("runner unit missing %q:\n%s", want, runner)
		}
	}
	if strings.Contains(runner, "DelegateSubgroup=") {
		t.Fatalf("runner unit depends on non-portable DelegateSubgroup:\n%s", runner)
	}

	builder := o.builderUnitFile()
	requireCollectModeInUnitSection(t, "builder", builder)
	for _, want := range []string{
		"WorkingDirectory=/run/kuasar-test",
		"run-builder --pidfile=/run/kuasar-test/runs/%i.pid",
		"--run-id=%i",
		"ExecStopPost=/bin/rm -f /run/kuasar-test/runs/%i.pid",
	} {
		if !strings.Contains(builder, want) {
			t.Fatalf("builder unit missing %q:\n%s", want, builder)
		}
	}
	for _, obsolete := range []string{"--build-id=", "StandardOutput=file:", "TimeoutStartSec="} {
		if strings.Contains(builder, obsolete) {
			t.Fatalf("builder unit retains obsolete %q:\n%s", obsolete, builder)
		}
	}
	if !strings.Contains(builder, "Slice=sandbox-builder.slice") || !strings.Contains(builder, "Delegate=yes") {
		t.Fatalf("builder unit lost delegated aggregate-slice placement:\n%s", builder)
	}
}

func TestGeneratedUnitsUseExactBootstrappingNodeCtl(t *testing.T) {
	o := &Orchestrator{cfg: &config.Config{Paths: config.PathsConfig{
		RunRoot:      "/run/kuasar-test",
		ConfigSocket: "/run/kuasar-test/node-ctl.socket",
	}}}
	o.SetExecutables(configresolve.ExecutablesForNodeCtl("/opt/kuasar/node-ctl"))
	for name, unit := range map[string]string{
		"runner":  o.runnerUnitFile(),
		"builder": o.builderUnitFile(),
	} {
		if !strings.Contains(unit, "ExecStart=/opt/kuasar/node-ctl run-") {
			t.Fatalf("%s unit did not retain exact node-ctl path:\n%s", name, unit)
		}
	}
}

func TestBuilderResourcePropertiesAndAggregateSliceCaps(t *testing.T) {
	resources := types.BuildResources{CPU: 2501, Memory: 3 << 30}
	properties, err := builderResourceProperties(resources)
	if err != nil {
		t.Fatal(err)
	}
	if properties.CPUQuotaPerSecUSec != 2_501_000 || properties.MemoryMax != 3<<30 {
		t.Fatalf("runtime properties = %+v", properties)
	}
	cpu := config.CPUCores("2.501")
	memory := "3GiB"
	o := &Orchestrator{cfg: &config.Config{Builder: config.BuilderConfig{
		Admission: config.BuilderAdmissionConfig{Execution: &config.BuildAdmissionLimitConfig{
			Resources: config.BuildAdmissionResourcesConfig{CPU: &cpu, Memory: &memory},
		}},
	}}}
	if got, want := o.builderSliceCaps(), "CPUQuota=250.1%\nMemoryMax=3221225472\n"; got != want {
		t.Fatalf("builder slice caps = %q, want %q", got, want)
	}
}

func TestBuilderUnitMayHaveProcesses(t *testing.T) {
	for _, test := range []struct {
		state string
		want  bool
	}{
		{"active", true},
		{"activating", true},
		{"reloading", true},
		{"deactivating", true},
		{"inactive", false},
		{"failed", false},
		{"", false},
	} {
		if got := builderUnitMayHaveProcesses(test.state); got != test.want {
			t.Errorf("builderUnitMayHaveProcesses(%q) = %t, want %t", test.state, got, test.want)
		}
	}
}

func TestInstallUnitsVerifiesOperatorManagedBuilderSlice(t *testing.T) {
	install := false
	cpu := config.CPUCores("2.5")
	memory := "3GiB"
	lc := &reconcileLauncher{resources: launcher.ResourceProperties{
		CPUQuotaPerSecUSec: 2_500_000,
		MemoryMax:          3 << 30,
	}}
	o := &Orchestrator{
		cfg: &config.Config{
			Units: config.UnitsConfig{Install: &install},
			Builder: config.BuilderConfig{Admission: config.BuilderAdmissionConfig{
				Execution: &config.BuildAdmissionLimitConfig{Resources: config.BuildAdmissionResourcesConfig{
					CPU: &cpu, Memory: &memory,
				}},
			}},
		},
		lc: lc, log: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	if err := o.InstallUnits(context.Background()); err != nil {
		t.Fatalf("verified operator-managed slice: %v", err)
	}
	lc.resources.MemoryMax--
	if err := o.InstallUnits(context.Background()); err == nil || !strings.Contains(err.Error(), "MemoryMax") {
		t.Fatalf("mismatched operator-managed slice error = %v", err)
	}
}

func TestInstallUnitsWritesBuilderAggregateLimits(t *testing.T) {
	install := true
	cpu := config.CPUCores("4")
	memory := "8GiB"
	dir := t.TempDir()
	lc := &reconcileLauncher{resources: launcher.ResourceProperties{
		CPUQuotaPerSecUSec: 4_000_000,
		MemoryMax:          8 << 30,
	}}
	o := &Orchestrator{
		cfg: &config.Config{
			Paths: config.PathsConfig{RunRoot: t.TempDir(), ConfigSocket: filepath.Join(t.TempDir(), "ctl.sock")},
			Units: config.UnitsConfig{
				Install: &install, Dir: dir,
				Runner: "sandbox-runner@.service", Builder: "sandbox-builder@.service",
			},
			Builder: config.BuilderConfig{Admission: config.BuilderAdmissionConfig{
				Execution: &config.BuildAdmissionLimitConfig{Resources: config.BuildAdmissionResourcesConfig{
					CPU: &cpu, Memory: &memory,
				}},
			}},
		},
		lc: lc, log: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	if err := o.InstallUnits(context.Background()); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(dir, "sandbox-builder.slice"))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"[Slice]", "CPUQuota=400%", "MemoryMax=8589934592"} {
		if !strings.Contains(string(b), want) {
			t.Fatalf("builder slice missing %q:\n%s", want, b)
		}
	}
}
