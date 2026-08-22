package configsock

import "errors"

const SnapshotPrepareSchemaVersion = 1

// SandboxTaskRequest identifies one exact assigned sandbox-runner incarnation.
type SandboxTaskRequest struct {
	SandboxID string `json:"sandbox_id"`
	RunID     string `json:"run_id"`
	Version   int    `json:"version,omitempty"`
}

// SnapshotPrepareSpec is the non-policy input a tenant-bound runner needs to
// read one root snapshot.cfg in-process. AbsoluteDeadlineUnixNano is one launch
// budget shared by task preparation, host preparation, exec, and readiness.
type SnapshotPrepareSpec struct {
	RootRef                  string `json:"root_ref"`
	ManifestConfig           string `json:"manifest_config,omitempty"`
	RefLocationParent        string `json:"ref_location_parent,omitempty"`
	RelativeDir              string `json:"relative_dir,omitempty"`
	MaxRefs                  int    `json:"max_refs"`
	AbsoluteDeadlineUnixNano int64  `json:"absolute_deadline_unix_nano"`
}

// SandboxTaskSpec is the authenticated bootstrap response. Exactly one of
// Final and Prepare is non-nil. Cold image launches receive Final immediately;
// restore launches complete Prepare and then receive a final LaunchSpec.
type SandboxTaskSpec struct {
	SandboxID string               `json:"sandbox_id"`
	RunID     string               `json:"run_id"`
	Workdir   string               `json:"workdir"`
	Env       map[string]string    `json:"env,omitempty"`
	Final     *LaunchSpec          `json:"final,omitempty"`
	Prepare   *SnapshotPrepareSpec `json:"prepare,omitempty"`
	Error     string               `json:"error,omitempty"`
}

type SnapshotCapacity struct {
	CPU    int    `json:"cpu"`
	Memory string `json:"memory"`
}

// SnapshotPrepareSummary is the non-secret immutable handoff from a runner to
// the one launch worker for its exact run. ResolutionDigest is an idempotency
// fingerprint, not an authentication credential.
type SnapshotPrepareSummary struct {
	SchemaVersion      int              `json:"schema_version"`
	Capacity           SnapshotCapacity `json:"capacity"`
	RawNetworkMetadata string           `json:"raw_network_metadata,omitempty"`
	ResolutionDigest   string           `json:"resolution_digest"`
	RequiredRefCount   int              `json:"required_ref_count"`
}

type SnapshotPrepareRequest struct {
	SandboxID string                 `json:"sandbox_id"`
	RunID     string                 `json:"run_id"`
	Summary   SnapshotPrepareSummary `json:"summary"`
}

type SnapshotPrepareResponse struct {
	Final *LaunchSpec `json:"final,omitempty"`
	Error string      `json:"error,omitempty"`
}

// SandboxTaskAuth is the non-secret identity needed to authenticate an exact
// assigned runner before the secret-bearing SandboxTaskSpec provider is called.
type SandboxTaskAuth struct {
	PidFile string
}

// SnapshotPrepareRejection marks a definitive stale/conflicting completion.
// The task must not retry it as a transient conductor failure.
type SnapshotPrepareRejection struct{ Err error }

func (e *SnapshotPrepareRejection) Error() string { return e.Err.Error() }
func (e *SnapshotPrepareRejection) Unwrap() error { return e.Err }

func RejectSnapshotPrepare(err error) error {
	if err == nil {
		return nil
	}
	return &SnapshotPrepareRejection{Err: err}
}

func IsSnapshotPrepareRejection(err error) bool {
	var rejection *SnapshotPrepareRejection
	return errors.As(err, &rejection)
}
