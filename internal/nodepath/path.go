// Package nodepath is the single authority for node-local object paths.
package nodepath

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

const (
	runnersDir   = "runners"
	sandboxesDir = "sandboxes"
	buildsDir    = "builds"

	// MaxUnixSocketPathBytes leaves the terminating NUL required by Linux's
	// 108-byte sockaddr_un.sun_path.
	MaxUnixSocketPathBytes = 107
)

// RunnerRoot returns the directory containing run-id pool pidfiles.
func RunnerRoot(runRoot string) string { return filepath.Join(runRoot, runnersDir) }

// RunnerPID returns the pidfile for one runner execution identity.
func RunnerPID(runRoot, runID string) string {
	return filepath.Join(RunnerRoot(runRoot), runID+".pid")
}

// SandboxRunRoot returns the calling root passed to sandbox-ctl for ordinary
// Sandbox volatile state.
func SandboxRunRoot(runRoot string) string { return filepath.Join(runRoot, sandboxesDir) }

// SandboxBaseRoot returns the calling root passed to sandbox-ctl for ordinary
// Sandbox persistent state.
func SandboxBaseRoot(baseRoot string) string { return filepath.Join(baseRoot, sandboxesDir) }

// SandboxRunDir returns one ordinary Sandbox's exact volatile directory.
func SandboxRunDir(runRoot, sandboxID string) string {
	return filepath.Join(SandboxRunRoot(runRoot), sandboxID)
}

// SandboxBaseDir returns one ordinary Sandbox's exact persistent directory.
func SandboxBaseDir(baseRoot, sandboxID string) string {
	return filepath.Join(SandboxBaseRoot(baseRoot), sandboxID)
}

// BuildRunDir returns one Build's exact volatile directory. BuildID is used
// verbatim and must have passed types.ValidateBuildID at the input boundary.
func BuildRunDir(runRoot, buildID string) string {
	return filepath.Join(runRoot, buildsDir, buildID)
}

// BuildBaseDir returns one Build's exact persistent directory. BuildID is used
// verbatim and must have passed types.ValidateBuildID at the input boundary.
func BuildBaseDir(baseRoot, buildID string) string {
	return filepath.Join(baseRoot, buildsDir, buildID)
}

// BuildCheckpointDir contains Build images and Sandbox/Snapshot artifacts.
func BuildCheckpointDir(baseRoot, buildID string) string {
	return filepath.Join(BuildBaseDir(baseRoot, buildID), "checkpoint")
}

// ValidateBuildRunRoot verifies that the fixed maximum BuildID still leaves
// room for the longest phase socket beneath this node RunRoot. BuildID's
// canonical limit remains independent of operator configuration.
func ValidateBuildRunRoot(runRoot string) error {
	path := maximumBuildPhaseSocketPath(runRoot)
	if len(path) > MaxUnixSocketPathBytes {
		return fmt.Errorf("maximum Build phase socket path is %d bytes (limit %d): %s",
			len(path), MaxUnixSocketPathBytes, path)
	}
	return nil
}

func maximumBuildPhaseSocketPath(runRoot string) string {
	buildID := strings.Repeat("B", types.MaxBuildIDBytes)
	return filepath.Join(BuildRunDir(runRoot, buildID), "b", "envd-steps.sock")
}
