package metadata

import (
	"context"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

type RouteKey struct{ Group, RouteKey string }
type BuildKey struct{ Group, BuildID string }

// Cursor is scoped to one typed listing, not a long-lived MVCC snapshot.
type Cursor string
type Page[T any] struct {
	Items []T
	Next  Cursor
}

// Invalidation is a one-shot instruction to reread. Cancel waits until the
// underlying observation is reclaimed. It is not an event history.
type Invalidation interface {
	Done() <-chan struct{}
	Err() error
	Cancel()
}
type ObservationAdmission interface {
	ReserveObservation(context.Context) (context.Context, func(), error)
}

type Resolution struct {
	Route Versioned[SandboxRecord]
	Node  Versioned[NodeProfileRecord]
}

type ReportDisposition uint8

const (
	ReportIgnored ReportDisposition = iota
	ReportWritten
	ReportOrphan
)

type RouteReportResult struct {
	Disposition ReportDisposition
	Record      Versioned[SandboxRecord]
	Reason      string
}

type RouteRepository interface {
	ReadRoute(context.Context, RouteKey) (Versioned[SandboxRecord], error)
	ReadResolution(context.Context, RouteKey) (Resolution, error)
	CompareAndPutRoute(context.Context, RouteKey, Version, SandboxRecord) (Version, error)
	DeleteRoute(context.Context, RouteKey, Version) error
	DeleteRouteAndOwnership(context.Context, RouteKey, Version, string, NodeSandboxRef) error
	ApplyRouteDelete(context.Context, Registration, Version, NodeSandboxRef, *SandboxRecord) error
	CommitIntentAndOwnership(context.Context, Version, SandboxRecord, NodeSandboxRef) (Version, error)
	CommitGroupIntent(context.Context, Version, SandboxRecord, NodeSandboxRef, GroupAdmission) (Version, error)
	RetireRejectedGroupIntent(context.Context, Registration, Version, NodeSandboxRef, *SandboxRecord) error
	RollbackIntent(context.Context, RouteKey, Version, *SandboxRecord, bool) (Version, error)
	ApplyRouteReport(context.Context, Registration, NodeSandboxRef, routesync.RouteEntry) (RouteReportResult, error)
	ReadAndObserveRoute(context.Context, RouteKey) (Versioned[SandboxRecord], Invalidation, error)
	ListRoutes(context.Context, string, Cursor, int) (Page[Versioned[SandboxRecord]], error)
}

type BuildRepository interface {
	ReadBuild(context.Context, BuildKey) (Versioned[BuildRecord], error)
	CompareAndPutBuild(context.Context, BuildKey, Version, BuildRecord) (Version, error)
	DeleteBuild(context.Context, BuildKey, Version) error
	CommitRegistrationIntent(context.Context, BuildRecord, NodeBuildRef) (Version, error)
	CommitGroupRegistrationIntent(context.Context, BuildRecord, NodeBuildRef, GroupAdmission) (Version, error)
	RetireRejectedGroupBuild(context.Context, Registration, Version, NodeBuildRef) error
	AcceptGroupBuildRegistration(context.Context, Registration, BuildKey, string, *types.BuildTarget) (Versioned[BuildRecord], error)
	AcceptBuildRegistration(context.Context, BuildKey, string, string, *types.BuildTarget) (Versioned[BuildRecord], error)
	EnsureRegistrationOwnership(context.Context, BuildKey, string, string) error
	RejectRegistrationIntent(context.Context, BuildKey, string, string) error
	ApplyBuildReport(context.Context, Registration, NodeBuildRef, routesync.BuildEvent) error
	DeleteBuildProjection(context.Context, Registration, NodeBuildRef) error
	ListBuilds(context.Context, string, Cursor, int) (Page[Versioned[BuildRecord]], error)
	ListBuildsByTemplate(context.Context, string, string, Cursor, int) (Page[Versioned[BuildRecord]], error)
}

type Registration struct {
	NodeID    string
	LinkOwner string
	Fence     Version
}

// NodeSnapshot contains both the mutable profile version and the immutable
// registration fence. A stream must retain its original Registration for all
// subsequent heartbeats; a fresh read does not authorize a new session.
type NodeSnapshot struct {
	Exists       bool
	Version      Version
	Value        NodeProfileRecord
	Registration Registration
	Reaping      bool
}

type ReapClaim struct {
	NodeID         string
	LinkOwner      string
	ProfileVersion Version
	Fence          Version
}

type NodeRepository interface {
	ReadNodeProfile(context.Context, string) (NodeSnapshot, error)
	Register(context.Context, Version, NodeProfileRecord) (NodeSnapshot, error)
	UpdateProfile(context.Context, Registration, Version, NodeProfileRecord) (Version, error)
	BeginReap(context.Context, string, Version) (ReapClaim, error)
	CleanupSandbox(context.Context, ReapClaim, Versioned[NodeSandboxRef], Version, bool) error
	CleanupBuild(context.Context, ReapClaim, Versioned[NodeBuildRef], Version) error
	CleanupKeyPair(context.Context, ReapClaim, Versioned[NodeKeyPair]) error
	FinalizeReap(context.Context, ReapClaim) (bool, error)
	ListReaping(context.Context, Cursor, int) (Page[ReapClaim], error)
	// ListExpiredNodeProfiles examines at most one bounded profile page and
	// returns live profiles whose positive heartbeat is at or before cutoffUnix.
	// An empty Items page can still have Next. Candidates are observations only;
	// BeginReap must compare their original Version after checking connection state.
	ListExpiredNodeProfiles(context.Context, int64, Cursor, int) (Page[Versioned[NodeProfileRecord]], error)
}

type OwnershipRepository interface {
	ReadSandboxOwner(context.Context, string, string) (Versioned[NodeSandboxRef], error)
	ReadBuildOwner(context.Context, string, string) (Versioned[NodeBuildRef], error)
	EnsureSandboxOwner(context.Context, string, NodeSandboxRef) error
	EnsureBuildOwner(context.Context, string, NodeBuildRef) error
	RemoveSandboxOwner(context.Context, string, string, Version) error
	RemoveBuildOwner(context.Context, string, string, Version) error
	ListSandboxOwners(context.Context, string, Cursor, int) (Page[Versioned[NodeSandboxRef]], error)
	ListBuildOwners(context.Context, string, Cursor, int) (Page[Versioned[NodeBuildRef]], error)
}

type KeyPairRepository interface {
	PutKeyPair(context.Context, string, NodeKeyPair) (Versioned[NodeKeyPair], error)
	AckKeyPair(context.Context, string, NodeKeyPair) (bool, error)
	DropKeyPair(context.Context, string, string, Version) error
	ListKeyPairs(context.Context, string, Cursor, int) (Page[Versioned[NodeKeyPair]], error)
}

const DirectoryBuckets = 256

type DirectoryToken [DirectoryBuckets]Version
type DirectoryView struct {
	Nodes []NodeListEntry
	Token DirectoryToken
}
type NodeDirectory interface {
	Load(context.Context) (DirectoryView, error)
	Observe(context.Context, DirectoryToken) (Invalidation, error)
}

type ImportSourceRepository interface {
	Acquire(context.Context, string, string, string, time.Duration) (PlacerImportSourceState, bool, error)
	Check(context.Context, string, string, string, uint64) (bool, error)
	Checkpoint(context.Context, string, string, string, uint64, string, bool, string) (PlacerImportSourceState, error)
}

// ConnectionOwners is business connection routing. Storage replicas never
// determine a Node connection's owner.
type ConnectionOwners interface {
	LocalID() string
	Candidates(context.Context, string) ([]string, error)
}

// Repositories groups constructor dependencies; consumers retain only the narrow
// capability they use. It is not a general transaction/KV interface.
type Repositories struct {
	Groups       GroupSupportRepository
	Routes       RouteRepository
	Builds       BuildRepository
	Nodes        NodeRepository
	Ownership    OwnershipRepository
	KeyPairs     KeyPairRepository
	Directory    NodeDirectory
	Imports      ImportSourceRepository
	Observations ObservationAdmission
}
