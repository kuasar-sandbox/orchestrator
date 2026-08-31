package conductor

import conductorextension "github.com/kuasar-sandbox/orchestrator/app/conductor/extension"

type Extension = conductorextension.Extension
type Host = conductorextension.Host
type Profile = conductorextension.Profile
type SandboxSource = conductorextension.SandboxSource
type SandboxView = conductorextension.SandboxView
type SandboxClusterView = conductorextension.SandboxClusterView
type SandboxState = conductorextension.SandboxState
type SnapshotLocation = conductorextension.SnapshotLocation
type SandboxEvent = conductorextension.SandboxEvent
type SandboxEventKind = conductorextension.SandboxEventKind
type BuildSource = conductorextension.BuildSource
type BuildView = conductorextension.BuildView
type BuildKind = conductorextension.BuildKind
type BuildState = conductorextension.BuildState
type BuildResources = conductorextension.BuildResources
type BuildStep = conductorextension.BuildStep
type BuildOptions = conductorextension.BuildOptions
type BuildRefererOptions = conductorextension.BuildRefererOptions
type BuildRegistryOptions = conductorextension.BuildRegistryOptions
type BuildRegistryTLSOptions = conductorextension.BuildRegistryTLSOptions
type BuildEvent = conductorextension.BuildEvent
type BuildEventKind = conductorextension.BuildEventKind

const (
	ProfileE2B  = conductorextension.ProfileE2B
	ProfileBare = conductorextension.ProfileBare

	SandboxStateStarting = conductorextension.SandboxStateStarting
	SandboxStateRunning  = conductorextension.SandboxStateRunning
	SandboxStatePaused   = conductorextension.SandboxStatePaused
	SandboxStateDead     = conductorextension.SandboxStateDead

	SnapshotLocationNone   = conductorextension.SnapshotLocationNone
	SnapshotLocationLocal  = conductorextension.SnapshotLocationLocal
	SnapshotLocationRemote = conductorextension.SnapshotLocationRemote

	SandboxSyncBegin = conductorextension.SandboxSyncBegin
	SandboxUpsert    = conductorextension.SandboxUpsert
	SandboxDelete    = conductorextension.SandboxDelete
	SandboxSyncEnd   = conductorextension.SandboxSyncEnd

	BuildKindImage    = conductorextension.BuildKindImage
	BuildKindSnapshot = conductorextension.BuildKindSnapshot

	BuildStateRegistered = conductorextension.BuildStateRegistered
	BuildStateWaiting    = conductorextension.BuildStateWaiting
	BuildStateBuilding   = conductorextension.BuildStateBuilding
	BuildStateReady      = conductorextension.BuildStateReady
	BuildStateError      = conductorextension.BuildStateError

	BuildSyncBegin = conductorextension.BuildSyncBegin
	BuildUpsert    = conductorextension.BuildUpsert
	BuildRemove    = conductorextension.BuildRemove
	BuildSyncEnd   = conductorextension.BuildSyncEnd
)
