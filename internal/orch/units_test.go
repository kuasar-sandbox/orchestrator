package orch

import (
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/config"
)

func TestGeneratedUnitsUseRunIDAssignment(t *testing.T) {
	o := &Orchestrator{cfg: &config.Config{
		Paths: config.PathsConfig{
			RunRoot:      "/run/kuasar-test",
			ConfigSocket: "/run/kuasar-test/node-ctl.socket",
		},
	}}
	runner := o.runnerUnitFile()
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
}
