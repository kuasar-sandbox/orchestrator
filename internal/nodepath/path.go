// Package nodepath is the single authority for node-local object paths.
package nodepath

import "path/filepath"

const (
	runnersDir   = "runners"
	sandboxesDir = "sandboxes"
	buildsDir    = "builds"
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
