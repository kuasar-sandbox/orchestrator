package extension

import (
	"context"
	"encoding/json"
	"time"

	"github.com/kuasar-sandbox/orchestrator/config"
)

// ResourceStats is the native sandbox resource contract. Missing observations
// are omitted independently; a non-nil zero is an actual observed zero.
// CPUSeconds is a decimal JSON number so a uint64 microsecond source is not
// rounded through float64 before it reaches the native API consumer.
type ResourceStats struct {
	CPUCapacity    *float64     `json:"cpuCapacity,omitempty"`
	CPUAllocatable *float64     `json:"cpuAllocatable,omitempty"`
	MemoryCapacity *uint64      `json:"memoryCapacity,omitempty"`
	MemoryHeadroom *uint64      `json:"memoryHeadroom,omitempty"`
	MemoryReserved *uint64      `json:"memoryReserved,omitempty"`
	MemoryUsed     *uint64      `json:"memoryUsed,omitempty"`
	CPUSeconds     *json.Number `json:"cpuSeconds,omitempty"`
	TimestampUnix  *int64       `json:"timestampUnix,omitempty"`
}

type TrafficInflight struct {
	Parking uint64 `json:"parking"`
	Egress  uint64 `json:"egress"`
}

type ServiceTrafficStats struct {
	Parking   uint64     `json:"parking"`
	Egress    uint64     `json:"egress"`
	IdleSince *time.Time `json:"idleSince,omitempty"`
}

type TrafficStats struct {
	State       string                         `json:"state"`
	MaxInflight config.MaxInflight             `json:"maxInflight"`
	Inflight    TrafficInflight                `json:"inflight"`
	IdleSince   *time.Time                     `json:"idleSince,omitempty"`
	Services    map[string]ServiceTrafficStats `json:"services"`
}

const (
	MaxStatsSandboxes     = 64
	MaxStatsConcurrency   = 8
	MaxStatsResponseBytes = 4 << 20
	StatsTimeout          = 5 * time.Second
)

// StatsRequest selects native sections for exact SandboxIDs already discovered
// from the existing object/RouteEntry source. It carries no paths or credentials.
type StatsRequest struct {
	SandboxIDs []string   `json:"sandboxIDs"`
	Sections   []string   `json:"sections"` // resource, traffic, usage
	Usage      UsageQuery `json:"usage,omitempty"`
}

// SandboxStats is the trusted local/extension envelope. StableID is only a
// correlation label. Each selected section has the same body as its native API.
type SandboxStats struct {
	SandboxID string          `json:"sandboxID"`
	StableID  string          `json:"stableID,omitempty"`
	Resource  *ResourceStats  `json:"resource,omitempty"`
	Traffic   *TrafficStats   `json:"traffic,omitempty"`
	Usage     json.RawMessage `json:"usage,omitempty"`
}

// StatsReader is available to trusted in-process extensions and the live
// telemetry plugin. The bounded batch fails if a requested object/section
// cannot be read; it never turns a failed source into zero or a stale result.
type StatsReader interface {
	ReadStats(context.Context, StatsRequest) ([]SandboxStats, error)
}
