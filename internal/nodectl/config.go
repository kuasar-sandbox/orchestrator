package nodectl

import (
	"fmt"
	"os"
	"runtime"
	"time"

	"github.com/fullof-work/mass-sandbox/pkg/config"
	"gopkg.in/yaml.v3"
)

// DaemonConfig is the YAML schema for /etc/node-ctl/node-ctl.yaml.
type DaemonConfig struct {
	Listen    string `yaml:"listen"`
	StatePath string `yaml:"state_path"`
	CgroupScanPaths []string `yaml:"cgroup_scan_paths"`

	Resources ResourcesConfig `yaml:"resources"`
	Watermarks WatermarksConfig `yaml:"watermarks"`
	RateLimits RateLimitsConfig `yaml:"rate_limits"`
	Admission  AdmissionConfig  `yaml:"admission"`
	Dampening  DampeningConfig  `yaml:"dampening"`
	Logging    LoggingConfig    `yaml:"logging"`
}

type ResourcesConfig struct {
	PhysicalMemory string `yaml:"physical_memory"` // "auto" or size string
	PhysicalCPU    string `yaml:"physical_cpu"`    // "auto" or core count
	HostReserved   HostReserved `yaml:"host_reserved"`
}

type HostReserved struct {
	Memory string  `yaml:"memory"`
	CPU    float64 `yaml:"cpu"`
}

type WatermarksConfig struct {
	OperationalMarginFactor float64 `yaml:"operational_margin_factor"`
	HighFactor              float64 `yaml:"high_factor"`
	LowFactor               float64 `yaml:"low_factor"`
	EmergencyFactor         float64 `yaml:"emergency_factor"`
}

type RateLimitsConfig struct {
	MemoryGrantPerSecFactor float64 `yaml:"memory_grant_per_sec_factor"`
}

type AdmissionConfig struct {
	Rate                  int    `yaml:"rate"`
	Burst                 int    `yaml:"burst"`
	MaxConcurrentCreating int    `yaml:"max_concurrent_creating"`
	StartupTTL            string `yaml:"startup_ttl"`
	QueueTTL              string `yaml:"queue_ttl"`
}

type DampeningConfig struct {
	RecoverDuration string `yaml:"recover_duration"`
	CooldownPeriods int    `yaml:"cooldown_periods"`
}

type LoggingConfig struct {
	Level     string `yaml:"level"`
	AuditPath string `yaml:"audit_path"`
}

// LoadDaemonConfig reads a node-ctl.yaml and applies defaults.
func LoadDaemonConfig(path string) (*DaemonConfig, error) {
	c := DefaultConfig()
	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("nodectl: read %s: %w", path, err)
		}
		if err := yaml.Unmarshal(data, c); err != nil {
			return nil, fmt.Errorf("nodectl: parse %s: %w", path, err)
		}
	}
	return c, nil
}

// DefaultConfig returns the canonical defaults from docs/node.md §3.1.
func DefaultConfig() *DaemonConfig {
	return &DaemonConfig{
		Listen:    DefaultSocket,
		StatePath: "/run/node-ctl/state.json",
		CgroupScanPaths: []string{"/sys/fs/cgroup/sandboxes"},
		Resources: ResourcesConfig{
			PhysicalMemory: "auto",
			PhysicalCPU:    "auto",
			HostReserved: HostReserved{
				Memory: "16GiB",
				CPU:    1.5,
			},
		},
		Watermarks: WatermarksConfig{
			OperationalMarginFactor: 0.10,
			HighFactor:              0.85,
			LowFactor:               0.70,
			EmergencyFactor:         0.05,
		},
		RateLimits: RateLimitsConfig{
			MemoryGrantPerSecFactor: 0.05,
		},
		Admission: AdmissionConfig{
			Rate:                  4,
			Burst:                 16,
			MaxConcurrentCreating: 16,
			StartupTTL:            "300s",
			QueueTTL:              "60s",
		},
		Dampening: DampeningConfig{
			RecoverDuration: "60s",
			CooldownPeriods: 10,
		},
		Logging: LoggingConfig{
			Level:     "info",
			AuditPath: "/run/node-ctl/audit.log",
		},
	}
}

// Resolved is the post-parse, post-default form of DaemonConfig with
// numbers expanded and durations parsed.
type Resolved struct {
	Listen    string
	StatePath string
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

// Resolve expands "auto" values and parses durations. Returns an error
// if any field is malformed.
func (c *DaemonConfig) Resolve() (*Resolved, error) {
	out := &Resolved{
		Listen:          c.Listen,
		StatePath:       c.StatePath,
		CgroupScanPaths: c.CgroupScanPaths,
		LogLevel:        c.Logging.Level,
		AuditPath:       c.Logging.AuditPath,
		CooldownPeriods: c.Dampening.CooldownPeriods,
	}

	if c.Resources.PhysicalMemory == "auto" || c.Resources.PhysicalMemory == "" {
		mem, err := readMemTotalBytes()
		if err != nil {
			return nil, fmt.Errorf("physical_memory auto-detect: %w", err)
		}
		out.PhysicalMemory = mem
	} else {
		mem, err := config.ParseSize(c.Resources.PhysicalMemory)
		if err != nil {
			return nil, fmt.Errorf("physical_memory: %w", err)
		}
		out.PhysicalMemory = mem
	}

	if c.Resources.PhysicalCPU == "auto" || c.Resources.PhysicalCPU == "" {
		out.PhysicalCPU = uint64(runtime.NumCPU()) * 1000
	} else {
		// Parse as integer cores → milli.
		var cores int
		if _, err := fmt.Sscanf(c.Resources.PhysicalCPU, "%d", &cores); err != nil {
			return nil, fmt.Errorf("physical_cpu: %w", err)
		}
		out.PhysicalCPU = uint64(cores) * 1000
	}

	hostMem, err := config.ParseSize(c.Resources.HostReserved.Memory)
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
	out.Admission = AdmissionPolicy{
		Rate:                  c.Admission.Rate,
		Burst:                 c.Admission.Burst,
		MaxConcurrentCreating: c.Admission.MaxConcurrentCreating,
		StartupTTL:            startupTTL,
		QueueTTL:              queueTTL,
	}

	out.Allocator = AllocatorPolicy{
		MemoryGrantPerSecBytes: out.MemoryGrantPerSecBytes,
		MinGrantStep:           4 << 20,    // 4 MiB
		MaxGrantStep:           512 << 20,  // 512 MiB
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
