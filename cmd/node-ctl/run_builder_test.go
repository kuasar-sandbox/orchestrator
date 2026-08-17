package main

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
)

func TestBuilderAssignmentPidfileMatchesBuildSpecRuntimeIdentity(t *testing.T) {
	runRoot := filepath.Join(t.TempDir(), "run")
	runPidfile := filepath.Join(runRoot, "runs", "br-test.pid")
	buildID := strings.Repeat("b", 36)

	got := builderAssignmentPidfile(runPidfile, buildID)
	want := configsock.BuildPidfile(runRoot, buildID)
	if got != want {
		t.Fatalf("assignment pidfile = %q, want %q", got, want)
	}
	if filepath.Base(got) != "builder.pid" || filepath.Dir(got) != configsock.BuildRuntimeDir(runRoot, buildID) {
		t.Fatalf("assignment pidfile does not use compact build runtime identity: %q", got)
	}
}
