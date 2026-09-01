package extension

import "context"

// SandboxState is the durable sandbox lifecycle state.
type SandboxState string

const (
	SandboxStateStarting SandboxState = "starting"
	SandboxStateRunning  SandboxState = "running"
	SandboxStatePaused   SandboxState = "paused"
	SandboxStateDead     SandboxState = "dead"
)

// ArtifactLocation classifies a persisted resume artifact reference. Location
// is independent of whether the artifact is a Sandbox or Snapshot.
type ArtifactLocation string

const (
	ArtifactLocationNone   ArtifactLocation = ""
	ArtifactLocationLocal  ArtifactLocation = "local"
	ArtifactLocationRemote ArtifactLocation = "remote"
)

type ResumeSourceKind string

const (
	ResumeSourceSandbox  ResumeSourceKind = "sandbox"
	ResumeSourceSnapshot ResumeSourceKind = "snapshot"
)

type LaunchMode string

const (
	LaunchModeImage  LaunchMode = "image"
	LaunchModeCold   LaunchMode = "cold"
	LaunchModeMemory LaunchMode = "memory"
)

// SandboxClusterView is the durable cluster ownership projection. A nil value
// on SandboxView means the sandbox is node-local.
type SandboxClusterView struct {
	Group    string
	RouteKey string
}

// SandboxView is an independent, non-secret projection of one durable
// sandbox. Maps and pointers are deep-copied for every Get and Watch callback.
// Runtime fields are node-local and belong to the current incarnation.
type SandboxView struct {
	ID                     string // current node-local sandbox ID
	StableID               string // identity preserved across node-local ID changes
	Profile                Profile
	TemplateID             string
	State                  SandboxState
	RunID                  string
	CreatedUnix            int64
	DeadlineUnix           int64
	Metadata               map[string]string
	Cluster                *SandboxClusterView
	RunDir                 string
	BaseDir                string
	EnvdUDS                string
	CIUDS                  string
	FloatingIP             string
	InnerIP                string
	VSwitchPort            string
	PortMAC                string
	ResumeSourceKind       ResumeSourceKind
	ResumeSourceRef        string
	ArtifactLocation       ArtifactLocation
	AutoPauseMemory        bool
	LaunchMode             LaunchMode
	APISecretFingerprint   string
	ManifestKeyFingerprint string
}

// SandboxEventKind identifies a snapshot marker or live sandbox change.
type SandboxEventKind string

const (
	SandboxSyncBegin SandboxEventKind = "sync_begin"
	SandboxUpsert    SandboxEventKind = "upsert"
	SandboxDelete    SandboxEventKind = "delete"
	SandboxSyncEnd   SandboxEventKind = "sync_end"
)

// SandboxEvent belongs to one generation. View is present for Upsert and, when
// available, Delete; SandboxID is always set for object events.
type SandboxEvent struct {
	Generation uint64
	Kind       SandboxEventKind
	SandboxID  string
	View       *SandboxView
}

// SandboxSource provides point reads and a generation-based convergent Watch.
// A complete generation begins with SandboxSyncBegin and ends with
// SandboxSyncEnd. An incomplete generation must be discarded. Slow consumers
// are automatically given a new full generation; intermediate changes may be
// skipped, duplicate events are allowed, and the latest full resync converges
// to durable state. A callback error terminates Watch. Context cancellation
// returns ctx.Err(). Watch is not a durable audit stream.
type SandboxSource interface {
	Get(context.Context, string) (SandboxView, bool, error)
	Watch(context.Context, func(SandboxEvent) error) error
}
