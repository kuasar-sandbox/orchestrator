package extension

// SandboxOperationKind identifies one hookable lifecycle admission.
type SandboxOperationKind string

const (
	SandboxOperationCreate SandboxOperationKind = "create"
	SandboxOperationPause  SandboxOperationKind = "pause"
	SandboxOperationResume SandboxOperationKind = "resume"
	SandboxOperationDelete SandboxOperationKind = "delete"
)

// SandboxOperationOrigin identifies the core entry point that admitted an
// operation. Cluster denotes a canonical node-link command; no cluster
// Extension is implied.
type SandboxOperationOrigin string

const (
	SandboxOriginDirect  SandboxOperationOrigin = "direct"
	SandboxOriginCluster SandboxOperationOrigin = "cluster"
	SandboxOriginProxy   SandboxOperationOrigin = "proxy"
	SandboxOriginExec    SandboxOperationOrigin = "exec"
)

// SandboxOperation is an operation-specific mutable copy. Exactly one request
// field is non-nil. ID, Kind, Origin, and SandboxID are core-owned envelope
// identity and modifications are rejected.
type SandboxOperation struct {
	ID        string
	Kind      SandboxOperationKind
	Origin    SandboxOperationOrigin
	SandboxID string

	Current *SandboxView

	Create *SandboxCreateRequest
	Pause  *SandboxPauseRequest
	Resume *SandboxResumeRequest
	Delete *SandboxDeleteRequest
}

// SandboxCreateRequest contains request-scoped create inputs. Profile is
// derived during preliminary template resolution; the final TemplateID must
// still resolve to that profile. MMDS preserves whether a top-level MMDS value
// was supplied. All maps and pointers are independent copies.
type SandboxCreateRequest struct {
	TemplateID      string
	Profile         Profile
	TimeoutSeconds  int
	Metadata        map[string]string
	Env             map[string]string
	Secure          bool
	AutoPauseMemory *bool
	MMDS            *string
}

type CaptureKind string

const (
	CaptureKindSnapshot CaptureKind = "snapshot"
	CaptureKindSandbox  CaptureKind = "sandbox"
)

// SandboxPauseRequest contains the core-owned capture selector plus
// action-scoped checkpoint overrides. CaptureKind is observable but immutable;
// nil policy fields inherit sandbox and node policy.
type SandboxPauseRequest struct {
	CaptureKind          CaptureKind
	CheckpointMergeRef   *bool
	CheckpointDropCaches *bool
}

type ResumeMode string

const (
	ResumeModeAuto   ResumeMode = "auto"
	ResumeModeMemory ResumeMode = "memory"
	ResumeModeCold   ResumeMode = "cold"
)

type ResumeTrigger string

const (
	ResumeTriggerConnect     ResumeTrigger = "connect"
	ResumeTriggerWake        ResumeTrigger = "wake"
	ResumeTriggerRoute       ResumeTrigger = "route"
	ResumeTriggerExec        ResumeTrigger = "exec"
	ResumeTriggerExecSession ResumeTrigger = "exec-session"
)

// SandboxResumeRequest contains the normalized caller deadline intent plus the
// observable, core-owned Mode and Trigger. A nil deadline means the core's
// existing resume-deadline policy applies.
type SandboxResumeRequest struct {
	RequestedDeadlineUnix *int64
	Mode                  ResumeMode
	Trigger               ResumeTrigger
}

// SandboxDeleteRequest describes an ordinary explicit delete. Mandatory
// rollback, reconciliation, reaper, and shutdown cleanup bypass this Hook.
type SandboxDeleteRequest struct {
	Reason string
}
