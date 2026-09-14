package extension

import "encoding/json"

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
