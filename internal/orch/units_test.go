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
		"run-sandbox --pidfile=/run/kuasar-test/runners/%i.pid",
		"--run-id=%i",
		"ExecStopPost=/bin/rm -f /run/kuasar-test/runners/%i.pid",
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
		"run-builder --pidfile=/run/kuasar-test/runners/%i.pid",
		"--run-id=%i",
		"ExecStopPost=/bin/rm -f /run/kuasar-test/runners/%i.pid",
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

func TestInstallUnitsLeavesOperatorManagedFilesUntouched(t *testing.T) {
	install := false
	dir := t.TempDir()
	path := filepath.Join(dir, "sandbox-builder.slice")
	original := "[Slice]\nCPUQuota=50%\nMemoryMax=256M\n"
	if err := os.WriteFile(path, []byte(original), 0644); err != nil {
		t.Fatal(err)
	}
	cpu, memory := config.CPUCores("2.5"), "3GiB"
	o := &Orchestrator{cfg: &config.Config{
		Units: config.UnitsConfig{Install: &install, Dir: dir},
		Builder: config.BuilderConfig{Admission: config.BuilderAdmissionConfig{
			Execution: &config.BuildAdmissionLimitConfig{Resources: config.BuildAdmissionResourcesConfig{CPU: &cpu, Memory: &memory}},
		}},
	}} // No Launcher: even a read or reload would panic.
	if err := o.InstallUnits(context.Background()); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(path)
	if err != nil || string(got) != original {
		t.Fatalf("operator file changed: %q, %v", got, err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 1 {
		t.Fatalf("generated operator files: %v, %v", entries, err)
	}
}

func TestInstallUnitsRemovesGeneratedParentResourcePolicy(t *testing.T) {
	install := true
	cpu, memory := config.CPUCores("4"), "8GiB"
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "sandbox-builder.slice"), []byte("[Slice]\nCPUQuota=400%\nMemoryMax=8589934592\n"), 0644); err != nil {
		t.Fatal(err)
	}
	lc := &reconcileLauncher{}
	o := &Orchestrator{
		cfg: &config.Config{
			Paths: config.PathsConfig{RunRoot: t.TempDir(), ConfigSocket: filepath.Join(t.TempDir(), "ctl.sock")},
			Units: config.UnitsConfig{Install: &install, Dir: dir, Runner: "sandbox-runner@.service", Builder: "sandbox-builder@.service"},
			Builder: config.BuilderConfig{Admission: config.BuilderAdmissionConfig{
				Execution: &config.BuildAdmissionLimitConfig{Resources: config.BuildAdmissionResourcesConfig{CPU: &cpu, Memory: &memory}},
			}},
		}, lc: lc, log: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	for i := 0; i < 2; i++ {
		if err := o.InstallUnits(context.Background()); err != nil {
			t.Fatal(err)
		}
		if lc.reloads != 1 {
			t.Fatalf("reload count=%d", lc.reloads)
		}
	}
	for _, name := range []string{"sandbox-runner@.service", "sandbox-builder@.service", "sandbox-runner.slice", "sandbox-builder.slice"} {
		b, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		for _, setting := range []string{"CPUQuota", "CPUWeight", "MemoryMax", "MemoryHigh", "MemoryLow", "MemoryMin"} {
			if strings.Contains(string(b), setting) {
				t.Fatalf("%s generated parent policy %s: %s", name, setting, b)
			}
		}
		if strings.HasSuffix(name, ".slice") && !strings.HasSuffix(string(b), "[Slice]\n") {
			t.Fatalf("unexpected slice: %s", b)
		}
	}
}
