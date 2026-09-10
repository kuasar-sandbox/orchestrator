package configsock

import "errors"

// BuildTaskSchemaVersion gates the BuildSpec wire contract independently from
// ArtifactPrepareSchemaVersion. Version 6 requires source command presence for
// target-specific preparation. Version 5 separates checkpoint-location policy
// from image-class Bundle publication and intentionally removes the old broad
// publish_location_parent field.
const BuildTaskSchemaVersion = 6

// BuildTaskRequest identifies one exact assigned run-builder incarnation.
type BuildTaskRequest struct {
	BuildID string `json:"build_id"`
	RunID   string `json:"run_id"`
	Version int    `json:"version,omitempty"`
}

// BuildTaskSpec is the authenticated task bootstrap. Sandbox/Snapshot template
// sources receive Prepare and later complete the two-stage handoff. FromImage
// and image-template builds receive Final in this same, single RPC.
type BuildTaskSpec struct {
	BuildID string               `json:"build_id"`
	RunID   string               `json:"run_id"`
	Env     map[string]string    `json:"env,omitempty"`
	Final   *BuildSpec           `json:"final,omitempty"`
	Prepare *ArtifactPrepareSpec `json:"prepare,omitempty"`
	Error   string               `json:"error,omitempty"`
}

// BuildTaskAuth is the non-secret identity used before the provider is allowed
// to load a secret-bearing BuildTaskSpec.
type BuildTaskAuth struct {
	PidFile string
}

type BuildPrepareRequest struct {
	BuildID string                 `json:"build_id"`
	RunID   string                 `json:"run_id"`
	Version int                    `json:"version,omitempty"`
	Summary ArtifactPrepareSummary `json:"summary"`
}

type BuildPrepareResponse struct {
	Final *BuildSpec `json:"final,omitempty"`
	Error string     `json:"error,omitempty"`
}

// BuildPrepareRejection marks stale ownership or a conflicting completion.
// The config-socket maps it to 409 so the task does not retry indefinitely.
type BuildPrepareRejection struct{ Err error }

func (e *BuildPrepareRejection) Error() string { return e.Err.Error() }
func (e *BuildPrepareRejection) Unwrap() error { return e.Err }

func RejectBuildPrepare(err error) error {
	if err == nil {
		return nil
	}
	return &BuildPrepareRejection{Err: err}
}

func IsBuildPrepareRejection(err error) bool {
	var rejection *BuildPrepareRejection
	return errors.As(err, &rejection)
}
