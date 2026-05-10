// Package nodectl implements the sandbox resource control protocol and
// its reference controller daemon (node-ctl).
//
// See docs/node.md for the protocol contract and the per-sandbox state
// machine. Other implementations of the controller role are free to
// exist; this package is the canonical reference.
package nodectl

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"
)

// DefaultSocket is the canonical UDS path the controller listens on.
const DefaultSocket = "/run/sandbox-resource.sock"

// MaxMessageBytes bounds the JSON payload size for one message.
const MaxMessageBytes = 64 * 1024

// Message types. See docs/node.md §5.2.
const (
	TypeAdmit          = "admit"
	TypeAdmitResponse  = "admit_response"
	TypeSettled        = "settled"
	TypeRequestBudget  = "request_budget"
	TypeBudgetResponse = "budget_response"
	TypeOOMReport      = "oom_report"
	TypeHeartbeat      = "heartbeat"
	TypeRelease        = "release"
	TypeAck            = "ack"
	TypeReclaimRequest = "reclaim_request"
	TypeReclaimDone    = "reclaim_done"
	TypeUpdateConfig   = "update_config"
	TypeReattach       = "reattach"
	TypeError          = "error"

	// Admin verbs (no per-sandbox token needed; identifies target sandbox
	// by SandboxID field). Used by node-ctl drain/grant/reclaim CLI.
	TypeAdminDrain   = "admin_drain"
	TypeAdminGrant   = "admin_grant"
	TypeAdminReclaim = "admin_reclaim"
	TypeAdminStatus  = "admin_status"
)

// Admission status values.
const (
	StatusAdmitted = "admitted"
	StatusQueued   = "queued"
	StatusRejected = "rejected"
)

// Urgency levels for RequestBudget.
const (
	UrgencyLow    = "low"
	UrgencyNormal = "normal"
	UrgencyHigh   = "high"
)

// Default per-message deadlines.
const (
	DeadlineAdmit         = 30 * time.Second
	DeadlineSettled       = 5 * time.Second
	DeadlineRequestBudget = 5 * time.Second
	DeadlineHeartbeat     = 5 * time.Second
	DeadlineRelease       = 5 * time.Second
)

// Message is the typed envelope. Only fields relevant to Type are
// populated. Length-prefix-JSON wire format matches the launch protocol
// in pkg/sandbox/proto for consistency.
type Message struct {
	Type  string `json:"type"`
	Token string `json:"token,omitempty"`

	// Admit (sandbox-ctl → controller).
	SandboxID             string  `json:"sandbox_id,omitempty"`
	CapacityMemoryBytes   uint64  `json:"capacity_memory_bytes,omitempty"`
	CapacityCPU           int     `json:"capacity_cpu,omitempty"`
	FloorMemoryBytes      uint64  `json:"floor_memory_bytes,omitempty"`
	FloorCPU              float64 `json:"floor_cpu,omitempty"`
	StartupBudgetMemory   uint64  `json:"startup_budget_memory,omitempty"`
	AllocatableAtSnapshot uint64  `json:"allocatable_at_snapshot,omitempty"`
	CgroupPath            string  `json:"cgroup_path,omitempty"`

	// AdmitResponse (controller → sandbox-ctl).
	Status              string `json:"status,omitempty"`
	GrantedInitialAlloc uint64 `json:"granted_initial_alloc,omitempty"`
	QueuedETAMs         int64  `json:"queued_eta_ms,omitempty"`

	// Settled / Heartbeat.
	CurrentRSS          uint64 `json:"current_rss,omitempty"`
	CurrentCPUUsec      uint64 `json:"current_cpu_usec,omitempty"`
	RecentHighCount     uint64 `json:"recent_high_count,omitempty"`
	CPUThrottledPeriods uint64 `json:"cpu_throttled_periods,omitempty"`

	// RequestBudget / BudgetResponse.
	CurrentAlloc   uint64 `json:"current_alloc,omitempty"`
	RequestedDelta uint64 `json:"requested_delta,omitempty"`
	Urgency        string `json:"urgency,omitempty"`
	GrantedDelta   uint64 `json:"granted_delta,omitempty"`
	NewAllocatable uint64 `json:"new_allocatable,omitempty"`
	CooldownMs     int64  `json:"cooldown_ms,omitempty"`

	// OOMReport.
	OOMCount  uint64 `json:"oom_count,omitempty"`
	KilledPID int    `json:"killed_pid,omitempty"`
	KilledRSS uint64 `json:"killed_rss,omitempty"`

	// ReclaimRequest (controller → sandbox-ctl).
	TargetAllocatable uint64 `json:"target_allocatable,omitempty"`
	DeadlineMs        int64  `json:"deadline_ms,omitempty"`

	// AdminDrain.
	Drain bool `json:"drain,omitempty"`

	// AdminStatus response.
	Zone           string `json:"zone,omitempty"`
	NodeAllocated  uint64 `json:"node_allocated_memory,omitempty"`
	AllocatablePool uint64 `json:"allocatable_pool_memory,omitempty"`
	ReservationCount int   `json:"reservation_count,omitempty"`
	Drained        bool   `json:"drained,omitempty"`

	// Generic.
	Reason string `json:"reason,omitempty"`
	Msg    string `json:"msg,omitempty"`
}

// WriteMessage writes one message in length-prefix-JSON wire format:
//
//	[4 bytes LE length] [JSON payload]
func WriteMessage(w io.Writer, m *Message) error {
	payload, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("nodectl: marshal: %w", err)
	}
	if len(payload) > MaxMessageBytes {
		return fmt.Errorf("nodectl: payload %d > max %d", len(payload), MaxMessageBytes)
	}
	var hdr [4]byte
	binary.LittleEndian.PutUint32(hdr[:], uint32(len(payload)))
	if _, err := w.Write(hdr[:]); err != nil {
		return fmt.Errorf("nodectl: write header: %w", err)
	}
	if _, err := w.Write(payload); err != nil {
		return fmt.Errorf("nodectl: write payload: %w", err)
	}
	return nil
}

// ReadMessage reads exactly one message.
func ReadMessage(r io.Reader) (*Message, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, err
	}
	n := binary.LittleEndian.Uint32(hdr[:])
	if n == 0 {
		return nil, errors.New("nodectl: zero-length payload")
	}
	if n > MaxMessageBytes {
		return nil, fmt.Errorf("nodectl: payload %d > max %d", n, MaxMessageBytes)
	}
	payload := make([]byte, n)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, fmt.Errorf("nodectl: read payload: %w", err)
	}
	var m Message
	if err := json.Unmarshal(payload, &m); err != nil {
		return nil, fmt.Errorf("nodectl: unmarshal: %w", err)
	}
	return &m, nil
}
