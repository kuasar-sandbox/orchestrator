package nodectl

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/util"
	"github.com/kuasar-sandbox/sandboxer/pkg/resource"
)

// Resolved is the post-default, post-parse form of the resource controller's
// configuration (config.ResourceListenConfig): sizes expanded, "auto" values
// detected, durations parsed. node-ctl conductor serve builds it via Resolve when it hosts
// the controller in-process (resource_listen, node-resource.md §3). There is no
// standalone daemon and no separate config file — the tuning is inlined in serve's
// config and consumed directly here.
type Resolved struct {
	Listen          string // absolute bind/dial path; may retain a short parent alias
	SocketIdentity  string // canonical owner/lease inventory identity
	StatePath       string
	CgroupScanPaths []string

	PhysicalMemory uint64
	PhysicalCPU    uint64 // milli-cpu
	HostReserved   Resources

	Watermarks Watermarks

	MemoryGrantPerSecBytes uint64

	Admission AdmissionPolicy
	Allocator AllocatorPolicy

	RecoverDuration time.Duration
	CooldownPeriods int

	LogLevel  string
	AuditPath string
}

// Resolve expands a config.ResourceListenConfig into the controller's runtime
// form: it defaults the block, auto-detects "auto" memory/cpu, computes the
// allocatable pool + grant rate, and parses durations. An empty Socket falls back
// to the protocol default (DefaultSocket, shared with sandbox-ctl). Returns an
// error if any field is malformed.
func Resolve(c *config.ResourceListenConfig) (*Resolved, error) {
	c.ApplyDefaults()

	listen := c.Socket
	if listen == "" {
		listen = DefaultSocket
	}
	identity, err := canonicalControllerSocket(listen)
	if err != nil {
		return nil, fmt.Errorf("controller socket: %w", err)
	}
	listen, err = filepath.Abs(listen)
	if err != nil {
		return nil, fmt.Errorf("controller socket bind path: %w", err)
	}
	out := &Resolved{
		Listen:          listen,
		SocketIdentity:  identity,
		StatePath:       c.StatePath,
		CgroupScanPaths: c.CgroupScanPaths,
		LogLevel:        c.LogLevel,
		AuditPath:       c.AuditPath,
		CooldownPeriods: c.Dampening.CooldownPeriods,
	}

	if c.Resources.PhysicalMemory == "auto" || c.Resources.PhysicalMemory == "" {
		mem, err := readMemTotalBytes()
		if err != nil {
			return nil, fmt.Errorf("physical_memory auto-detect: %w", err)
		}
		out.PhysicalMemory = mem
	} else {
		mem, err := util.ParseSize(c.Resources.PhysicalMemory)
		if err != nil {
			return nil, fmt.Errorf("physical_memory: %w", err)
		}
		out.PhysicalMemory = mem
	}

	if c.Resources.PhysicalCPU == "auto" || c.Resources.PhysicalCPU == "" {
		out.PhysicalCPU = uint64(runtime.NumCPU()) * 1000
	} else {
		var cores int
		if _, err := fmt.Sscanf(c.Resources.PhysicalCPU, "%d", &cores); err != nil {
			return nil, fmt.Errorf("physical_cpu: %w", err)
		}
		out.PhysicalCPU = uint64(cores) * 1000
	}

	hostMem, err := util.ParseSize(c.Resources.HostReserved.Memory)
	if err != nil {
		return nil, fmt.Errorf("host_reserved.memory: %w", err)
	}
	out.HostReserved = Resources{
		MemoryBytes: hostMem,
		CPUMilli:    uint64(c.Resources.HostReserved.CPU * 1000),
	}

	out.Watermarks = Watermarks{
		OperationalMarginFactor: c.Watermarks.OperationalMarginFactor,
		HighFactor:              c.Watermarks.HighFactor,
		LowFactor:               c.Watermarks.LowFactor,
		EmergencyFactor:         c.Watermarks.EmergencyFactor,
		StartupFactor:           c.Watermarks.StartupFactor,
	}
	if out.Watermarks.StartupFactor <= out.Watermarks.EmergencyFactor ||
		out.Watermarks.StartupFactor > 1.0 {
		return nil, fmt.Errorf("watermarks.startup_factor (%g) must satisfy "+
			"emergency_factor (%g) < startup_factor ≤ 1.0",
			out.Watermarks.StartupFactor, out.Watermarks.EmergencyFactor)
	}

	pool := uint64(float64(out.PhysicalMemory-out.HostReserved.MemoryBytes) *
		(1 - out.Watermarks.OperationalMarginFactor))
	out.MemoryGrantPerSecBytes = uint64(float64(pool) * c.RateLimits.MemoryGrantPerSecFactor)

	startupTTL, err := time.ParseDuration(c.Admission.StartupTTL)
	if err != nil {
		return nil, fmt.Errorf("admission.startup_ttl: %w", err)
	}
	queueTTL, err := time.ParseDuration(c.Admission.QueueTTL)
	if err != nil {
		return nil, fmt.Errorf("admission.queue_ttl: %w", err)
	}
	queueDepth := c.Admission.QueueMaxDepth
	if queueDepth <= 0 {
		queueDepth = 256
	}
	out.Admission = AdmissionPolicy{
		Rate:          c.Admission.Rate,
		Burst:         c.Admission.Burst,
		StartupTTL:    startupTTL,
		QueueTTL:      queueTTL,
		QueueMaxDepth: queueDepth,
	}

	out.Allocator = AllocatorPolicy{
		MemoryGrantPerSecBytes: out.MemoryGrantPerSecBytes,
		MinGrantStep:           4 << 20,   // 4 MiB
		MaxGrantStep:           512 << 20, // 512 MiB
	}

	if c.Dampening.RecoverDuration != "" {
		rd, err := time.ParseDuration(c.Dampening.RecoverDuration)
		if err != nil {
			return nil, fmt.Errorf("dampening.recover_duration: %w", err)
		}
		out.RecoverDuration = rd
	}

	return out, nil
}

func canonicalControllerSocket(socket string) (string, error) {
	return resource.CanonicalSocketPath(socket)
}

func readMemTotalBytes() (uint64, error) {
	data, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0, err
	}
	// Parse "MemTotal:    16384000 kB"
	var total uint64
	lines := splitLines(data)
	for _, line := range lines {
		var label string
		var n uint64
		var unit string
		if _, err := fmt.Sscanf(line, "%s %d %s", &label, &n, &unit); err == nil && label == "MemTotal:" {
			if unit == "kB" {
				total = n * 1024
			} else {
				total = n
			}
			break
		}
	}
	if total == 0 {
		return 0, fmt.Errorf("MemTotal not found in /proc/meminfo")
	}
	return total, nil
}

func splitLines(data []byte) []string {
	var lines []string
	var start int
	for i, b := range data {
		if b == '\n' {
			lines = append(lines, string(data[start:i]))
			start = i + 1
		}
	}
	if start < len(data) {
		lines = append(lines, string(data[start:]))
	}
	return lines
}
