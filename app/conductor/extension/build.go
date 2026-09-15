package extension

import "context"

// BuildKind is the terminal artifact kind. It is empty until a Build resolves
// its requested target and completes successfully.
type BuildKind string

const (
	BuildKindImage    BuildKind = "img"
	BuildKindSandbox  BuildKind = "sbx"
	BuildKindSnapshot BuildKind = "snp"
)

// BuildTargetKind is the registration-time public output family.
type BuildTargetKind string

const (
	BuildTargetImage   BuildTargetKind = "image"
	BuildTargetSandbox BuildTargetKind = "sandbox"
)

// BuildTarget is an immutable requested target. A nil Target in BuildOptions
// means automatic resolution from the final effective start/ready commands.
type BuildTarget struct {
	Kind   BuildTargetKind
	Memory bool
}

// BuildState is the durable build lifecycle state.
type BuildState string

const (
	BuildStateRegistered BuildState = "registered"
	BuildStateWaiting    BuildState = "waiting"
	BuildStateBuilding   BuildState = "building"
	BuildStateReady      BuildState = "ready"
	BuildStateError      BuildState = "error"
)

// BuildResources is the build admission and execution resource vector. CPU is
// milli-CPU; Memory and Storage are bytes.
type BuildResources struct {
	CPU     int64
	Memory  int64
	Storage int64
}

// BuildStep is one independent build-step projection.
type BuildStep struct {
	Type      string
	Args      []string
	FilesHash string
	Force     bool
}

// BuildOptions contains durable, non-secret build-only controls.
type BuildOptions struct {
	Target    *BuildTarget
	Resources *BuildResources
	Referer   *BuildRefererOptions
	Registry  *BuildRegistryOptions
}

type BuildRefererOptions struct {
	Enabled   *bool
	Writeback *bool
}

type BuildRegistryOptions struct {
	TLS *BuildRegistryTLSOptions
}

type BuildRegistryTLSOptions struct {
	CABundlePEM        string
	InsecureSkipVerify bool
}

// BuildView is an independent, non-secret projection of one durable Build.
// Every map, slice, and pointer is deep-copied for each Get and Watch callback.
type BuildView struct {
	BuildID                string
	TemplateID             string
	PersistID              string
	Profile                Profile
	Kind                   BuildKind
	State                  BuildState
	Reason                 string
	CreatedUnix            int64
	WaitingUnix            int64
	ExecutionClaimedUnix   int64
	Names                  []string
	Aliases                []string
	FromImage              string
	FromTemplate           string
	Resources              BuildResources
	Steps                  []BuildStep
	StartCommand           string
	ReadyCommand           string
	Metadata               map[string]string
	Builder                BuildOptions
	RunID                  string
	ExecutionClaimed       bool
	Phase                  string
	PhaseSandboxID         string
	RuntimeVSwitchPort     string
	RuntimeFloatingIP      string
	RuntimePortMAC         string
	ClusterGroup           string
	APISecretFingerprint   string
	ManifestKeyFingerprint string
}

// BuildEventKind identifies a snapshot marker or live current-set change.
type BuildEventKind string

const (
	BuildSyncBegin BuildEventKind = "sync_begin"
	BuildUpsert    BuildEventKind = "upsert"
	BuildRemove    BuildEventKind = "remove"
	BuildSyncEnd   BuildEventKind = "sync_end"
)

// BuildEvent belongs to one generation. View is present for Upsert and Remove;
// BuildID is always set for object events. Reason is set on error removals.
type BuildEvent struct {
	Generation uint64
	Kind       BuildEventKind
	BuildID    string
	View       *BuildView
	Reason     string
}

// BuildSource provides point reads of any known Build and a convergent Watch of
// the current registered/waiting/building/ready set. Error rows are excluded
// from full snapshots. A live transition to error emits BuildRemove with the
// final view, but that removal is not durable and can be missed when a watcher
// resyncs. A known error Build remains available through Get.
//
// Watch uses the same complete-generation rules as SandboxSource, returns a
// callback error unchanged, and returns ctx.Err() on cancellation.
type BuildSource interface {
	Get(context.Context, string) (BuildView, bool, error)
	Watch(context.Context, func(BuildEvent) error) error
}
