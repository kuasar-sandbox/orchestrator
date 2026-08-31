package proxy

import proxyextension "github.com/kuasar-sandbox/orchestrator/app/proxy/extension"

// MasterExtension and its related contracts are aliases for the leaf extension
// package, allowing custom proxies to use either import style.
type MasterExtension = proxyextension.MasterExtension
type MasterHost = proxyextension.MasterHost
type ManagementWrapper = proxyextension.ManagementWrapper
type RouteSource = proxyextension.RouteSource
type Profile = proxyextension.Profile
type RouteState = proxyextension.RouteState
type SnapshotLocation = proxyextension.SnapshotLocation
type RouteView = proxyextension.RouteView
type RouteEvent = proxyextension.RouteEvent
type RouteEventKind = proxyextension.RouteEventKind
type RouteSyncState = proxyextension.RouteSyncState
type TrafficSource = proxyextension.TrafficSource
type TrafficView = proxyextension.TrafficView
type TrafficInflight = proxyextension.TrafficInflight
type ServiceTrafficView = proxyextension.ServiceTrafficView

const (
	ProfileE2B  = proxyextension.ProfileE2B
	ProfileBare = proxyextension.ProfileBare

	RouteStateStarting = proxyextension.RouteStateStarting
	RouteStateRunning  = proxyextension.RouteStateRunning
	RouteStatePaused   = proxyextension.RouteStatePaused
	RouteStateDead     = proxyextension.RouteStateDead

	SnapshotLocationNone   = proxyextension.SnapshotLocationNone
	SnapshotLocationLocal  = proxyextension.SnapshotLocationLocal
	SnapshotLocationRemote = proxyextension.SnapshotLocationRemote

	RouteSyncInitializing = proxyextension.RouteSyncInitializing
	RouteSyncSyncing      = proxyextension.RouteSyncSyncing
	RouteSyncSynced       = proxyextension.RouteSyncSynced
	RouteSyncStale        = proxyextension.RouteSyncStale

	RouteSyncBegin = proxyextension.RouteSyncBegin
	RouteUpsert    = proxyextension.RouteUpsert
	RouteDelete    = proxyextension.RouteDelete
	RouteSyncEnd   = proxyextension.RouteSyncEnd
	RouteSyncLost  = proxyextension.RouteSyncLost
)

var (
	ErrTrafficUnavailable = proxyextension.ErrTrafficUnavailable
	ErrTrafficConflict    = proxyextension.ErrTrafficConflict
)
