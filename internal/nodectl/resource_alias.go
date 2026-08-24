// Package nodectl implements node-level reservation admission, allocation,
// rebuildable recovery inventory and audit. Balloon, cgroup and guest-memory
// policy remain sandbox-local.
//
// The wire protocol and the client it speaks are defined once in
// sandboxer/pkg/resource (sandbox-ctl is the client). This file
// re-exports those protocol symbols under their local names so the
// controller and its CLI reference the protocol without churn; the
// definitions live in sandboxer/pkg/resource.
package nodectl

import "github.com/kuasar-sandbox/sandboxer/pkg/resource"

// Protocol types + client (defined in sandboxer/pkg/resource).
type (
	Message           = resource.Message
	Client            = resource.Client
	AdmitParams       = resource.AdmitParams
	AdmitResult       = resource.AdmitResult
	HeartbeatResult   = resource.HeartbeatResult
	AdminStatusResult = resource.AdminStatusResult
	ResourcesView     = resource.ResourcesView
	ReservationView   = resource.ReservationView
)

// Protocol constants.
const (
	DefaultSocket            = resource.DefaultSocket
	MaxMessageBytes          = resource.MaxMessageBytes
	DefaultAdminListPageSize = resource.DefaultAdminListPageSize

	TypeAdmit          = resource.TypeAdmit
	TypeAdmitResponse  = resource.TypeAdmitResponse
	TypeSettled        = resource.TypeSettled
	TypeRequestBudget  = resource.TypeRequestBudget
	TypeBudgetResponse = resource.TypeBudgetResponse
	TypeOOMReport      = resource.TypeOOMReport
	TypeHeartbeat      = resource.TypeHeartbeat
	TypeRelease        = resource.TypeRelease
	TypeAck            = resource.TypeAck
	TypeStateSync      = resource.TypeStateSync
	TypeError          = resource.TypeError
	TypeAdminDrain     = resource.TypeAdminDrain
	TypeAdminStatus    = resource.TypeAdminStatus
	TypeAdminList      = resource.TypeAdminList

	FeatureStateSyncV1 = resource.FeatureStateSyncV1

	StatusAdmitted = resource.StatusAdmitted
	StatusQueued   = resource.StatusQueued
	StatusRejected = resource.StatusRejected

	UrgencyLow    = resource.UrgencyLow
	UrgencyNormal = resource.UrgencyNormal
	UrgencyHigh   = resource.UrgencyHigh

	DeadlineAdmit         = resource.DeadlineAdmit
	DeadlineSettled       = resource.DeadlineSettled
	DeadlineRequestBudget = resource.DeadlineRequestBudget
	DeadlineHeartbeat     = resource.DeadlineHeartbeat
	DeadlineRelease       = resource.DeadlineRelease
)

// Protocol framing functions.
var (
	WriteMessage = resource.WriteMessage
	ReadMessage  = resource.ReadMessage
)
