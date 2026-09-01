package extension

import "context"

// Profile is a sandbox runtime profile.
type Profile string

const (
	ProfileE2B  Profile = "e2b"
	ProfileBare Profile = "bare"
)

// RouteState is the current lifecycle state carried by a proxy route.
type RouteState string

const (
	RouteStateStarting RouteState = "starting"
	RouteStateRunning  RouteState = "running"
	RouteStatePaused   RouteState = "paused"
	RouteStateDead     RouteState = "dead"
)

// ArtifactLocation describes whether a retained E/S artifact is node-bound or
// portable. The empty value means the row owns no artifact.
type ArtifactLocation string

const (
	ArtifactLocationNone   ArtifactLocation = ""
	ArtifactLocationLocal  ArtifactLocation = "local"
	ArtifactLocationRemote ArtifactLocation = "remote"
)

// RouteSyncState reports the master route source's relationship to the
// conductor route stream.
type RouteSyncState string

const (
	RouteSyncInitializing RouteSyncState = "initializing"
	RouteSyncSyncing      RouteSyncState = "syncing"
	RouteSyncSynced       RouteSyncState = "synced"
	RouteSyncStale        RouteSyncState = "stale"
)

// RouteView is an independent, non-secret projection of one current route.
// Revision changes when the core route table publishes a new lifecycle
// incarnation. Mutating this value cannot affect proxy state.
type RouteView struct {
	SandboxID              string // node-local route lookup key
	StableID               string // identity preserved across node-local ID changes
	Profile                Profile
	TemplateID             string
	State                  RouteState
	RunID                  string
	EnvdUDS                string
	CIUDS                  string
	FloatingIP             string
	ArtifactLocation       ArtifactLocation
	APISecretFingerprint   string
	ManifestKeyFingerprint string
	Revision               uint64
}

// RouteEventKind identifies one event in a Watch generation.
type RouteEventKind string

const (
	RouteSyncBegin RouteEventKind = "sync_begin"
	RouteUpsert    RouteEventKind = "upsert"
	RouteDelete    RouteEventKind = "delete"
	RouteSyncEnd   RouteEventKind = "sync_end"
	RouteSyncLost  RouteEventKind = "sync_lost"
)

// RouteEvent belongs to exactly one consumer-local generation. View is set for
// upsert and delete events and is an independent value.
type RouteEvent struct {
	Generation uint64
	Kind       RouteEventKind
	SandboxID  string
	View       *RouteView
}

// RouteSource exposes the current master route projection. Watch uses
// generation-based eventual convergence: a complete generation starts with
// RouteSyncBegin and ends with RouteSyncEnd, followed by ordered live changes.
// A lagging watcher or invalid source generation abandons that generation and
// starts another full snapshot. Intermediate changes may be missed or repeated;
// Watch is not a durable audit stream. Context cancellation returns ctx.Err().
type RouteSource interface {
	Get(context.Context, string) (RouteView, bool, error)
	Watch(context.Context, func(RouteEvent) error) error
	SyncState() RouteSyncState
}
