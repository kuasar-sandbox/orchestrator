package telemetry

import "time"

const (
	SandboxIDAttribute = "sandbox.id"
	StableIDAttribute  = "sandbox.stable_id"
)

var resourceMetrics = [...]struct {
	name string
	unit string
}{
	{"sandbox.cpu.count", "{cpu}"},
	{"sandbox.cpu.used", "%"},
	{"sandbox.memory.total", "By"},
	{"sandbox.memory.used", "By"},
	{"sandbox.memory.cache", "By"},
	{"sandbox.disk.total", "By"},
	{"sandbox.disk.used", "By"},
}

// The seven-field mapping belongs only to the envd receiver and E2B adapter.
// Generic readers/exporters have no field enumeration or source restriction.
type e2bField uint8

const (
	e2bCPUCount e2bField = iota
	e2bCPUUsedPct
	e2bMemTotal
	e2bMemUsed
	e2bMemCache
	e2bDiskTotal
	e2bDiskUsed
	e2bFieldCount
)

type e2bPoint struct {
	Timestamp time.Time
	Field     e2bField
	Value     float64
}
