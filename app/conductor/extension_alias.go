package conductor

import conductorextension "github.com/kuasar-sandbox/orchestrator/app/conductor/extension"

type Extension = conductorextension.Extension
type Host = conductorextension.Host
type StatsReader = conductorextension.StatsReader
type StatsRequest = conductorextension.StatsRequest
type SandboxStats = conductorextension.SandboxStats
type ResourceStats = conductorextension.ResourceStats
type TrafficStats = conductorextension.TrafficStats
type TrafficCounters = conductorextension.TrafficCounters
type TrafficInflight = conductorextension.TrafficInflight
type ServiceTrafficStats = conductorextension.ServiceTrafficStats
type UsageQuery = conductorextension.UsageQuery
type APIWrapper = conductorextension.APIWrapper
type SandboxHook = conductorextension.SandboxHook
type BuildHook = conductorextension.BuildHook
type Profile = conductorextension.Profile
type SandboxSource = conductorextension.SandboxSource
type SandboxView = conductorextension.SandboxView
type SandboxClusterView = conductorextension.SandboxClusterView
type SandboxState = conductorextension.SandboxState
type ArtifactLocation = conductorextension.ArtifactLocation
type ResumeSourceKind = conductorextension.ResumeSourceKind
type LaunchMode = conductorextension.LaunchMode
type SandboxEvent = conductorextension.SandboxEvent
type SandboxEventKind = conductorextension.SandboxEventKind
type BuildSource = conductorextension.BuildSource
type BuildView = conductorextension.BuildView
type BuildKind = conductorextension.BuildKind
type BuildState = conductorextension.BuildState
type BuildResources = conductorextension.BuildResources
type BuildStep = conductorextension.BuildStep
type BuildOptions = conductorextension.BuildOptions
type BuildTarget = conductorextension.BuildTarget
type BuildTargetKind = conductorextension.BuildTargetKind
type BuildRefererOptions = conductorextension.BuildRefererOptions
type BuildRegistryOptions = conductorextension.BuildRegistryOptions
type BuildRegistryTLSOptions = conductorextension.BuildRegistryTLSOptions
type BuildEvent = conductorextension.BuildEvent
type BuildEventKind = conductorextension.BuildEventKind
type SandboxOperation = conductorextension.SandboxOperation
type SandboxOperationKind = conductorextension.SandboxOperationKind
type SandboxOperationOrigin = conductorextension.SandboxOperationOrigin
type SandboxCreateRequest = conductorextension.SandboxCreateRequest
type SandboxPauseRequest = conductorextension.SandboxPauseRequest
type SandboxResumeRequest = conductorextension.SandboxResumeRequest
type CaptureKind = conductorextension.CaptureKind
type ResumeMode = conductorextension.ResumeMode
type ResumeTrigger = conductorextension.ResumeTrigger
type SandboxDeleteRequest = conductorextension.SandboxDeleteRequest
type BuildOperation = conductorextension.BuildOperation
type BuildOperationKind = conductorextension.BuildOperationKind
type BuildOperationOrigin = conductorextension.BuildOperationOrigin
type BuildRegisterRequest = conductorextension.BuildRegisterRequest
type BuildTriggerRequest = conductorextension.BuildTriggerRequest
type BuildResourcePatch = conductorextension.BuildResourcePatch

var ErrRejected = conductorextension.ErrRejected

const (
	ProfileE2B  = conductorextension.ProfileE2B
	ProfileBare = conductorextension.ProfileBare

	SandboxStateStarting = conductorextension.SandboxStateStarting
	SandboxStateRunning  = conductorextension.SandboxStateRunning
	SandboxStatePaused   = conductorextension.SandboxStatePaused
	SandboxStateDeleting = conductorextension.SandboxStateDeleting
	SandboxStateDead     = conductorextension.SandboxStateDead

	ArtifactLocationNone   = conductorextension.ArtifactLocationNone
	ArtifactLocationLocal  = conductorextension.ArtifactLocationLocal
	ArtifactLocationRemote = conductorextension.ArtifactLocationRemote

	ResumeSourceSandbox  = conductorextension.ResumeSourceSandbox
	ResumeSourceSnapshot = conductorextension.ResumeSourceSnapshot

	LaunchModeImage  = conductorextension.LaunchModeImage
	LaunchModeCold   = conductorextension.LaunchModeCold
	LaunchModeMemory = conductorextension.LaunchModeMemory

	SandboxSyncBegin = conductorextension.SandboxSyncBegin
	SandboxUpsert    = conductorextension.SandboxUpsert
	SandboxDelete    = conductorextension.SandboxDelete
	SandboxSyncEnd   = conductorextension.SandboxSyncEnd

	BuildKindImage     = conductorextension.BuildKindImage
	BuildKindSandbox   = conductorextension.BuildKindSandbox
	BuildKindSnapshot  = conductorextension.BuildKindSnapshot
	BuildTargetImage   = conductorextension.BuildTargetImage
	BuildTargetSandbox = conductorextension.BuildTargetSandbox

	BuildStateRegistered = conductorextension.BuildStateRegistered
	BuildStateWaiting    = conductorextension.BuildStateWaiting
	BuildStateBuilding   = conductorextension.BuildStateBuilding
	BuildStateReady      = conductorextension.BuildStateReady
	BuildStateError      = conductorextension.BuildStateError

	BuildSyncBegin = conductorextension.BuildSyncBegin
	BuildUpsert    = conductorextension.BuildUpsert
	BuildRemove    = conductorextension.BuildRemove
	BuildSyncEnd   = conductorextension.BuildSyncEnd

	SandboxOperationCreate = conductorextension.SandboxOperationCreate
	SandboxOperationPause  = conductorextension.SandboxOperationPause
	SandboxOperationResume = conductorextension.SandboxOperationResume
	SandboxOperationDelete = conductorextension.SandboxOperationDelete

	CaptureKindSnapshot = conductorextension.CaptureKindSnapshot
	CaptureKindSandbox  = conductorextension.CaptureKindSandbox

	ResumeModeAuto   = conductorextension.ResumeModeAuto
	ResumeModeMemory = conductorextension.ResumeModeMemory
	ResumeModeCold   = conductorextension.ResumeModeCold

	ResumeTriggerConnect     = conductorextension.ResumeTriggerConnect
	ResumeTriggerWake        = conductorextension.ResumeTriggerWake
	ResumeTriggerRoute       = conductorextension.ResumeTriggerRoute
	ResumeTriggerExec        = conductorextension.ResumeTriggerExec
	ResumeTriggerExecSession = conductorextension.ResumeTriggerExecSession

	SandboxOriginDirect  = conductorextension.SandboxOriginDirect
	SandboxOriginCluster = conductorextension.SandboxOriginCluster
	SandboxOriginProxy   = conductorextension.SandboxOriginProxy
	SandboxOriginExec    = conductorextension.SandboxOriginExec

	BuildOperationRegister = conductorextension.BuildOperationRegister
	BuildOperationTrigger  = conductorextension.BuildOperationTrigger

	BuildOriginDirect  = conductorextension.BuildOriginDirect
	BuildOriginCluster = conductorextension.BuildOriginCluster
)
