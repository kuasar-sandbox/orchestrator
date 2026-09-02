package nodepath

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestNodePaths(t *testing.T) {
	runRoot := filepath.Join(t.TempDir(), "custom-run")
	baseRoot := filepath.Join(t.TempDir(), "custom-base")
	if got, want := RunnerPID(runRoot, "run-1"), filepath.Join(runRoot, "runners", "run-1.pid"); got != want {
		t.Fatalf("RunnerPID = %q, want %q", got, want)
	}
	if got, want := SandboxRunRoot(runRoot), filepath.Join(runRoot, "sandboxes"); got != want {
		t.Fatalf("SandboxRunRoot = %q, want %q", got, want)
	}
	if got, want := SandboxBaseRoot(baseRoot), filepath.Join(baseRoot, "sandboxes"); got != want {
		t.Fatalf("SandboxBaseRoot = %q, want %q", got, want)
	}
	if got, want := SandboxRunDir(runRoot, "sid"), filepath.Join(runRoot, "sandboxes", "sid"); got != want {
		t.Fatalf("SandboxRunDir = %q, want %q", got, want)
	}
	if got, want := SandboxBaseDir(baseRoot, "sid"), filepath.Join(baseRoot, "sandboxes", "sid"); got != want {
		t.Fatalf("SandboxBaseDir = %q, want %q", got, want)
	}
	if got, want := BuildRunDir(runRoot, "MiXeD_-9"), filepath.Join(runRoot, "builds", "MiXeD_-9"); got != want {
		t.Fatalf("BuildRunDir = %q, want %q", got, want)
	}
	if got, want := BuildBaseDir(baseRoot, "MiXeD_-9"), filepath.Join(baseRoot, "builds", "MiXeD_-9"); got != want {
		t.Fatalf("BuildBaseDir = %q, want %q", got, want)
	}
	if got, want := BuildCheckpointDir(baseRoot, "MiXeD_-9"), filepath.Join(baseRoot, "builds", "MiXeD_-9", "checkpoint"); got != want {
		t.Fatalf("BuildCheckpointDir = %q, want %q", got, want)
	}
}

func TestMaximumBuildIDLeavesDefaultPhaseSocketsWithinUnixLimit(t *testing.T) {
	buildID := strings.Repeat("Z", 48)
	for _, socket := range []string{
		filepath.Join(BuildRunDir("/run/sandbox", buildID), "c", "ctl.sock"),
		filepath.Join(BuildRunDir("/run/sandbox", buildID), "b", "envd-steps.sock"),
		filepath.Join(BuildRunDir("/run/sandbox", buildID), "c", "envd.sock"),
	} {
		// sockaddr_un.sun_path is 108 bytes on Linux, including the terminating NUL.
		if len(socket) >= 108 {
			t.Fatalf("maximum BuildID socket path has %d bytes: %q", len(socket), socket)
		}
	}
}
