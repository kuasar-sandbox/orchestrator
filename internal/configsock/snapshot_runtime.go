package configsock

import "github.com/kuasar-sandbox/orchestrator/internal/types"

// SnapshotRuntimeCheckRequest is the non-secret stdin contract for a short-lived
// tenant helper. The conductor passes MANIFEST_KEY only in the child's environment.
// This is import admission, not a runner assignment or a launch preparation result.
type SnapshotRuntimeCheckRequest struct {
	Source            types.ResumeSource `json:"source"`
	Runtime           string             `json:"runtime"`
	ManifestConfig    string             `json:"manifest_config,omitempty"`
	RefLocationParent string             `json:"ref_location_parent,omitempty"`
	RelativeDir       string             `json:"relative_dir,omitempty"`
}

const (
	SnapshotRuntimeCompatible     = "compatible"
	SnapshotRuntimeIncompatible   = "incompatible"
	SnapshotRuntimeSourceMismatch = "source_mismatch"
	SnapshotRuntimeCheckError     = "error"
)

// SnapshotRuntimeCheckResult returns no portable config, key, or reusable launch
// proof. Restore must still validate its actual runtime file when it executes.
type SnapshotRuntimeCheckResult struct {
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}
