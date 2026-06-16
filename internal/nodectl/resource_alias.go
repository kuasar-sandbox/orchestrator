// Package nodectl is the reference node-level resource controller — the
// "sentinel" daemon's brains: admission, allocation, per-sandbox state,
// budget reclaim, persistence and audit.
//
// The wire protocol and the client it speaks are defined once in
// sandbox-runtime/pkg/resource (sandbox-ctl is the client). This file
// re-exports those protocol symbols under their local names so the
// controller and its CLI reference the protocol without churn; the
// definitions live in sandbox-runtime.
package nodectl

import "github.com/kuasar-sandbox/sandbox-runtime/pkg/resource"

// Protocol types + client (defined in sandbox-runtime/pkg/resource).
type (
	Message           = resource.Message
	Client            = resource.Client
	AdmitParams       = resource.AdmitParams
	AdmitResult       = resource.AdmitResult
	HeartbeatResult   = resource.HeartbeatResult
	AdminStatusResult = resource.AdminStatusResult
)

// Protocol constants.
const (
	DefaultSocket   = resource.DefaultSocket
	MaxMessageBytes = resource.MaxMessageBytes

	TypeAdmit          = resource.TypeAdmit
	TypeAdmitResponse  = resource.TypeAdmitResponse
	TypeSettled        = resource.TypeSettled
	TypeRequestBudget  = resource.TypeRequestBudget
	TypeBudgetResponse = resource.TypeBudgetResponse
	TypeOOMReport      = resource.TypeOOMReport
	TypeHeartbeat      = resource.TypeHeartbeat
	TypeRelease        = resource.TypeRelease
	TypeAck            = resource.TypeAck
	TypeReclaimRequest = resource.TypeReclaimRequest
	TypeReclaimDone    = resource.TypeReclaimDone
	TypeUpdateConfig   = resource.TypeUpdateConfig
	TypeReattach       = resource.TypeReattach
	TypeError          = resource.TypeError
	TypeAdminDrain     = resource.TypeAdminDrain
	TypeAdminGrant     = resource.TypeAdminGrant
	TypeAdminReclaim   = resource.TypeAdminReclaim
	TypeAdminStatus    = resource.TypeAdminStatus

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
