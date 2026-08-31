// Package config defines the public declarative configuration for the node
// conductor and external proxy.
//
// The YAML is grouped by concern: api / proxy / paths / units / sandbox (the
// sandbox-instance defaults, sub-grouped resources/network/boot) / builder /
// checkpoint (paused-state tiering), plus a few singular top-level references
// (encryption_key, manifest_config). Runtime material and executable discovery
// are deliberately outside this package.
package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/buildcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/mmdssvc"
	"github.com/kuasar-sandbox/orchestrator/internal/reflocation"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"gopkg.in/yaml.v3"
)

// Proxy modes select how the node serves sandbox data-plane traffic
// (<port>-<sid>.<domain>). See docs/proxy.md §5 (deployment modes);
// docs/orchestrator.md §9 covers how serve assembles the chosen mode.
const (
	ProxyInternal = "internal" // in-process proxy (default)
	ProxyExternal = "external" // offloaded to node-ctl proxy master + workers
	ProxyOff      = "off"      // data plane disabled on this node
)

// Data-plane auth modes: the proxy validates X-Access-Token (= envdAccessToken).
const (
	AuthOff     = "off"     // no validation
	AuthLog     = "log"     // validate + log mismatches, but still forward
	AuthEnforce = "enforce" // reject mismatches with 401 (default)
)

// Checkpoint modes select where a paused sandbox's snapshot lands.
const (
	CheckpointLocal  = "local"  // sandbox-ctl snapshot --mode local → node-local tarstream (default; node-bound)
	CheckpointBundle = "bundle" // sandbox-ctl snapshot --mode bundle → node-local Manifest Bundle (node-bound)
)

// Conductor is the declarative configuration consumed by the node conductor.
// Runtime objects, discovered executable paths, and provider state never live
// in this value, so it can be safely cloned and serialized for component
// bootstrap.
type Conductor struct {
	API     APIConfig     `yaml:"api" json:"api"`         // north control plane + TLS
	Proxy   ProxyConfig   `yaml:"proxy" json:"proxy"`     // data-plane proxy
	Paths   PathsConfig   `yaml:"paths" json:"paths"`     // node-local dirs / sockets
	Units   UnitsConfig   `yaml:"units" json:"units"`     // systemd unit management
	Sandbox SandboxConfig `yaml:"sandbox" json:"sandbox"` // sandbox-instance defaults
	Builder BuilderConfig `yaml:"builder" json:"builder"` // build-instance settings
	// ResourceListen optionally hosts the node resource controller in-process
	// (resource_listen, node-resource.md); absent/disabled => sandboxes use static
	// cgroup. The controller's tuning is inlined here — there is no second file.
	ResourceListen *ResourceListenConfig `yaml:"resource_listen,omitempty" json:"resource_listen,omitempty"`
	// Checkpoint is the paused-state tiering policy (parallel to sandbox).
	Checkpoint CheckpointConfig `yaml:"checkpoint" json:"checkpoint"`
	// MMDS is the optional envd metadata service (re-keys envd to fresh per-identity
	// tokens). Disabled => envd runs non-secure and the proxy is the sole data-plane gate.
	MMDS MMDSConfig `yaml:"mmds" json:"mmds"`
	// Cluster connects this node to a cluster-ctl registry over node-link
	// (node.md §10); empty = standalone single-node.
	Cluster ClusterConfig `yaml:"cluster" json:"cluster"`
	// Singular top-level references.
	EncryptionKey  string `yaml:"encryption_key" json:"encryption_key"`   // credential at-rest AES-256 (":"-sep, first active); or NODE_CONFIG_ENCRYPTION_KEY env
	ManifestConfig string `yaml:"manifest_config" json:"manifest_config"` // remote manifest store config (path ref; shared by sandbox + builder)
}

// Clone returns a deep copy. Hooks may retain and mutate the value they
// receive; Apps clone again after Configure so those mutations cannot race
// with the running core.
func (c *Conductor) Clone() *Conductor {
	if c == nil {
		return nil
	}
	out := *c
	out.Units.Install = clonePtr(c.Units.Install)
	out.Cluster.Labels = cloneMap(c.Cluster.Labels)
	out.Sandbox.Network.DNS = cloneSlice(c.Sandbox.Network.DNS)
	out.Sandbox.Resources.Allocatable.CPU = clonePtr(c.Sandbox.Resources.Allocatable.CPU)
	out.Sandbox.Resources.Allocatable.Memory = clonePtr(c.Sandbox.Resources.Allocatable.Memory)
	if c.Sandbox.Resources.Startup != nil {
		startup := *c.Sandbox.Resources.Startup
		out.Sandbox.Resources.Startup = &startup
	}
	if c.Sandbox.Resources.WatermarkHigh != nil {
		watermark := *c.Sandbox.Resources.WatermarkHigh
		watermark.Ratio = clonePtr(c.Sandbox.Resources.WatermarkHigh.Ratio)
		out.Sandbox.Resources.WatermarkHigh = &watermark
	}
	out.Builder.Admission.Registration = cloneBuildAdmissionLimit(c.Builder.Admission.Registration)
	out.Builder.Admission.Execution = cloneBuildAdmissionLimit(c.Builder.Admission.Execution)
	out.Builder.Referer.Fallback = clonePtr(c.Builder.Referer.Fallback)
	out.Builder.Referer.Writeback = clonePtr(c.Builder.Referer.Writeback)
	if c.Builder.FilesStorage != nil {
		files := *c.Builder.FilesStorage
		out.Builder.FilesStorage = &files
	}
	if c.ResourceListen != nil {
		resource := *c.ResourceListen
		resource.CgroupScanPaths = cloneSlice(c.ResourceListen.CgroupScanPaths)
		out.ResourceListen = &resource
	}
	out.Checkpoint.MergeRef = clonePtr(c.Checkpoint.MergeRef)
	out.Checkpoint.DropCaches = clonePtr(c.Checkpoint.DropCaches)
	out.MMDS.Routes.ReservedPathPrefixes = cloneSlice(c.MMDS.Routes.ReservedPathPrefixes)
	out.MMDS.Services = cloneMap(c.MMDS.Services)
	return &out
}

func clonePtr[T any](in *T) *T {
	if in == nil {
		return nil
	}
	out := *in
	return &out
}

func cloneSlice[T any](in []T) []T {
	if in == nil {
		return nil
	}
	return append([]T(nil), in...)
}

func cloneMap[K comparable, V any](in map[K]V) map[K]V {
	if in == nil {
		return nil
	}
	out := make(map[K]V, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

// ResourceListenConfig hosts the in-process node resource controller inside serve
// (the resource_listen sub-server, node-resource.md). Absent / Enabled=false =>
// sandboxes use static cgroup. The controller's tuning is inlined here — there is
// no second config file; nodectl.Resolve consumes this struct directly.
type ResourceListenConfig struct {
	Enabled         bool                     `yaml:"enabled" json:"enabled"`
	Socket          string                   `yaml:"socket" json:"socket"`                       // controller UDS; "" = pkg/resource.DefaultSocket (sandbox-ctl's default)
	StatePath       string                   `yaml:"state_path" json:"state_path"`               // deprecated and ignored; retained only so older YAML still parses
	CgroupScanPaths []string                 `yaml:"cgroup_scan_paths" json:"cgroup_scan_paths"` // restart recovery roots for populated sandbox cgroups
	Resources       ResourceHostConfig       `yaml:"resources" json:"resources"`                 // node physical capacity + host reservation
	Watermarks      ResourceWatermarksConfig `yaml:"watermarks" json:"watermarks"`               // zone thresholds (fractions of allocatable pool)
	RateLimits      ResourceRateLimitsConfig `yaml:"rate_limits" json:"rate_limits"`             // memory grant rate limit
	Admission       ResourceAdmissionConfig  `yaml:"admission" json:"admission"`                 // admit token bucket + queue
	LogLevel        string                   `yaml:"log_level" json:"log_level"`                 // info (default)
}

// ResourceHostConfig is the node's physical capacity and the host's own reservation.
type ResourceHostConfig struct {
	PhysicalMemory string               `yaml:"physical_memory" json:"physical_memory"` // "auto" (/proc/meminfo) or a size string
	PhysicalCPU    string               `yaml:"physical_cpu" json:"physical_cpu"`       // "auto" (nproc) or an integer core count
	HostReserved   ResourceHostReserved `yaml:"host_reserved" json:"host_reserved"`     // kernel + node daemons reservation
}

// ResourceHostReserved is the memory/CPU carved out for the host itself.
type ResourceHostReserved struct {
	Memory string  `yaml:"memory" json:"memory"` // default 16GiB
	CPU    float64 `yaml:"cpu" json:"cpu"`       // cores; default 1.5
}

// ResourceWatermarksConfig sets the zone thresholds as fractions of the allocatable
// pool (node-resource.md §3.2 / §4).
type ResourceWatermarksConfig struct {
	OperationalMarginFactor float64 `yaml:"operational_margin_factor" json:"operational_margin_factor"` // default 0.10
	HighFactor              float64 `yaml:"high_factor" json:"high_factor"`                             // red zone start; default 0.85
	LowFactor               float64 `yaml:"low_factor" json:"low_factor"`                               // yellow zone start; default 0.70
	EmergencyFactor         float64 `yaml:"emergency_factor" json:"emergency_factor"`                   // emergency pool; default 0.05
	StartupFactor           float64 `yaml:"startup_factor" json:"startup_factor"`                       // startup_pool = pool × this; default 0.50 (emergency < startup ≤ 1.0)
}

// ResourceRateLimitsConfig bounds the memory grant rate.
type ResourceRateLimitsConfig struct {
	MemoryGrantPerSecFactor float64 `yaml:"memory_grant_per_sec_factor" json:"memory_grant_per_sec_factor"` // × allocatable pool; default 0.05
}

// ResourceAdmissionConfig is the admit token bucket + short-block queue.
type ResourceAdmissionConfig struct {
	Rate          int    `yaml:"rate" json:"rate"`                       // tokens/s; default 4
	Burst         int    `yaml:"burst" json:"burst"`                     // bucket capacity; default 16
	StartupTTL    string `yaml:"startup_ttl" json:"startup_ttl"`         // admit→settled deadline; default 30s
	QueueTTL      string `yaml:"queue_ttl" json:"queue_ttl"`             // max short-block wait; default 30s
	QueueMaxDepth int    `yaml:"queue_max_depth" json:"queue_max_depth"` // queue capacity; default 256
}

// ApplyDefaults fills the controller tuning defaults (node-resource.md §3.2). The
// socket default (pkg/resource.DefaultSocket) is applied by nodectl.Resolve, which
// owns that protocol constant. Exported so nodectl.Resolve can default a config
// block built outside config.Load (e.g. in tests).
func (r *ResourceListenConfig) ApplyDefaults() {
	if len(r.CgroupScanPaths) == 0 {
		r.CgroupScanPaths = []string{
			"/sys/fs/cgroup/sandbox.slice/sandbox-runner.slice",
			"/sys/fs/cgroup/sandbox.slice/sandbox-builder.slice",
		}
	}
	if r.Resources.PhysicalMemory == "" {
		r.Resources.PhysicalMemory = "auto"
	}
	if r.Resources.PhysicalCPU == "" {
		r.Resources.PhysicalCPU = "auto"
	}
	if r.Resources.HostReserved.Memory == "" {
		r.Resources.HostReserved.Memory = "16GiB"
	}
	if r.Resources.HostReserved.CPU == 0 {
		r.Resources.HostReserved.CPU = 1.5
	}
	if r.Watermarks.OperationalMarginFactor == 0 {
		r.Watermarks.OperationalMarginFactor = 0.10
	}
	if r.Watermarks.HighFactor == 0 {
		r.Watermarks.HighFactor = 0.85
	}
	if r.Watermarks.LowFactor == 0 {
		r.Watermarks.LowFactor = 0.70
	}
	if r.Watermarks.EmergencyFactor == 0 {
		r.Watermarks.EmergencyFactor = 0.05
	}
	if r.Watermarks.StartupFactor == 0 {
		r.Watermarks.StartupFactor = 0.50
	}
	if r.RateLimits.MemoryGrantPerSecFactor == 0 {
		r.RateLimits.MemoryGrantPerSecFactor = 0.05
	}
	if r.Admission.Rate == 0 {
		r.Admission.Rate = 4
	}
	if r.Admission.Burst == 0 {
		r.Admission.Burst = 16
	}
	if r.Admission.StartupTTL == "" {
		r.Admission.StartupTTL = "30s"
	}
	if r.Admission.QueueTTL == "" {
		r.Admission.QueueTTL = "30s"
	}
	if r.Admission.QueueMaxDepth == 0 {
		r.Admission.QueueMaxDepth = 256
	}
	if r.LogLevel == "" {
		r.LogLevel = "info"
	}
}

// ClusterConfig connects this node to a cluster-ctl registry over node_link
// (node.md §10). Empty NodeLink.Endpoint = standalone single-node (no cluster).
type ClusterConfig struct {
	NodeLink          ClusterNodeLink   `yaml:"node_link" json:"node_link"`                   // how to reach registry node_link
	NodeID            string            `yaml:"node_id" json:"node_id"`                       // this node's id; "" = hostname
	Labels            map[string]string `yaml:"labels" json:"labels"`                         // zone / pool / slot / node (nodeSelectors)
	DataEndpoint      string            `yaml:"data_endpoint" json:"data_endpoint"`           // host:port the router forwards the data plane to
	HeartbeatInterval string            `yaml:"heartbeat_interval" json:"heartbeat_interval"` // node-link heartbeat period; "" = 10s
}

// ClusterNodeLink is how the node dials the registry's node_link listener.
type ClusterNodeLink struct {
	Endpoint string      `yaml:"endpoint" json:"endpoint"` // registry node_link addr host:port; "" = standalone
	TLS      TLSMaterial `yaml:"tls" json:"tls"`           // node-link client mTLS; empty = plain h2c
}

// TLSMaterial is cert/key + CA (mirrors clustercfg.TLS) for the node-link client.
type TLSMaterial struct {
	Cert string `yaml:"cert" json:"cert"`
	Key  string `yaml:"key" json:"key"`
	CA   string `yaml:"ca" json:"ca"` // CA that verifies the registry's server cert
}

// APIConfig is the north control plane + TLS.
type APIConfig struct {
	Domain string    `yaml:"domain" json:"domain"` // e.g. sandboxes.example.com
	Listen string    `yaml:"listen" json:"listen"` // ":443" (https); dev ":3000" http
	TLS    TLSConfig `yaml:"tls" json:"tls"`       // wildcard cert for *.<domain> + api.<domain>
}

// TLSConfig is the wildcard TLS material.
type TLSConfig struct {
	Cert string `yaml:"cert" json:"cert"`
	Key  string `yaml:"key" json:"key"`
}

// ProxyConfig is the data-plane proxy. mode ∈ {internal,external,off}. In external
// mode conductor forwards fallback data-plane requests to the proxy master's
// registered UDS; the proxy master owns data-plane listener sockets.
type ProxyConfig struct {
	Mode          string `yaml:"mode" json:"mode"`                     // internal (default) | external | off
	DataListen    string `yaml:"data_listen" json:"data_listen"`       // dedicated data-plane listener; "" = share api.listen
	ProxyNetNS    string `yaml:"proxy_netns" json:"proxy_netns"`       // optional forwarding netns for floatingip TCP dials and internal MMDS listen
	ParkTimeout   string `yaml:"park_timeout" json:"park_timeout"`     // hold a data-plane request awaiting route/resume; default 30s
	Auth          string `yaml:"auth" json:"auth"`                     // off | log | enforce (default): validate X-Access-Token
	MetricsListen string `yaml:"metrics_listen" json:"metrics_listen"` // optional Prometheus text endpoint; "" = off
}

// MMDSConfig is the optional Firecracker-MMDS-v2 metadata service the orchestrator
// serves so envd (run in FC mode, i.e. without -isnotfc) can re-key its access token
// to a fresh per-identity value at /init — required for snapshot-fork data-plane auth.
// enabled=false keeps envd in -isnotfc (non-secure); the proxy then enforces
// X-Access-Token as the sole gate. The MMDS is hosted by the proxy component
// (internal: serve binds Listen; external: the proxy master receives Listen from
// the conductor and shares that listener fd with workers). envd
// hard-codes 169.254.169.254:80, so the vswitch's --mgmt-service translates that VIP to
// Listen in its datapath (no iptables); a loopback Listen needs route_localnet=1 on the
// mgmt dev.
type MMDSConfig struct {
	Enabled  bool                                `yaml:"enabled" json:"enabled"` // false (default) => -isnotfc + proxy-only auth
	Listen   string                              `yaml:"listen" json:"listen"`   // MMDS listener (the vswitch mgmt-service target); default 127.0.0.1:19254
	Routes   MMDSRoutesConfig                    `yaml:"routes" json:"routes"`
	Services map[string]MMDSServiceRegistryEntry `yaml:"services" json:"services"`
}

// MMDSRoutesConfig is the conductor-owned admission policy for tenant MMDS
// routes. Defaults are applied only to omitted zero values; explicit negative
// limits survive defaulting and fail validation.
type MMDSRoutesConfig struct {
	Enabled              bool     `yaml:"enabled" json:"enabled"`
	MaxRoutesPerSandbox  int      `yaml:"max_routes_per_sandbox" json:"max_routes_per_sandbox"`
	MaxNamespaceBytes    int      `yaml:"max_namespace_bytes" json:"max_namespace_bytes"`
	MaxStaticBodyBytes   int      `yaml:"max_static_body_bytes" json:"max_static_body_bytes"`
	MaxSecretValueBytes  int      `yaml:"max_secret_value_bytes" json:"max_secret_value_bytes"`
	ReservedPathPrefixes []string `yaml:"reserved_path_prefixes" json:"reserved_path_prefixes"`
}

// MMDSServiceRegistryEntry names one operator-controlled local service. V1
// accepts only unix:// absolute paths; request timeout and response bounds are
// fixed implementation constants, not tenant or YAML policy.
type MMDSServiceRegistryEntry struct {
	Endpoint string `yaml:"endpoint" json:"endpoint"`
}

func (c MMDSConfig) ServiceEndpoints() map[string]string {
	if len(c.Services) == 0 {
		return nil
	}
	out := make(map[string]string, len(c.Services))
	for name, service := range c.Services {
		out[name] = service.Endpoint
	}
	return out
}

// PathsConfig holds node-local directories and sockets.
type PathsConfig struct {
	ConductorExecutable string `yaml:"conductor_executable,omitempty" json:"conductor_executable,omitempty"` // custom conductor binary; empty uses node-ctl's built-in app
	RunRoot             string `yaml:"run_root" json:"run_root"`                                             // default /run/sandbox (tmpfs)
	BaseRoot            string `yaml:"base_root" json:"base_root"`                                           // default /var/lib/sandbox (persistent)
	DBPath              string `yaml:"db_path" json:"db_path"`                                               // default <base_root>/node-ctl.db
	ConfigSocket        string `yaml:"config_socket" json:"config_socket"`                                   // default /run/sandbox/node-ctl.socket
	AdminPidfile        string `yaml:"admin_pidfile" json:"admin_pidfile"`                                   // optional PID allowlist (multi-line) gating the socket admin plane; "" => socket perms (same-uid/root) only
	PluginPidfile       string `yaml:"plugin_pidfile" json:"plugin_pidfile"`                                 // optional PID allowlist (multi-line) gating the socket plugin plane (proxy/agent registration); "" => socket perms only
}

// UnitsConfig manages the systemd template units (generated + installed at startup).
type UnitsConfig struct {
	Dir             string `yaml:"dir" json:"dir"`                             // default /etc/systemd/system
	Runner          string `yaml:"runner" json:"runner"`                       // default sandbox-runner@.service
	Builder         string `yaml:"builder" json:"builder"`                     // default sandbox-builder@.service
	RunnerPoolSize  int    `yaml:"runner_pool_size" json:"runner_pool_size"`   // idle prestarted runner units; 0 = disabled
	BuilderPoolSize int    `yaml:"builder_pool_size" json:"builder_pool_size"` // idle prestarted builder units; 0 = disabled
	PoolWaitTimeout string `yaml:"pool_wait_timeout" json:"pool_wait_timeout"` // StartUnit -> WaitAssignment deadline; default 5s
	Install         *bool  `yaml:"install" json:"install"`                     // default true; false = manage out of band
}

func (u UnitsConfig) PoolWaitDuration() time.Duration {
	d, err := time.ParseDuration(u.PoolWaitTimeout)
	if err != nil || d <= 0 {
		return 5 * time.Second
	}
	return d
}

// SandboxConfig is the sandbox-instance defaults, sub-grouped for clarity.
type SandboxConfig struct {
	TimeoutSec int             `yaml:"timeout_sec" json:"timeout_sec"` // default TTL; default 300
	Capacity   int             `yaml:"capacity" json:"capacity"`       // max sandboxes this node admits (cluster headroom denominator, §4.2); 0 = unbounded
	Resources  ResourcesConfig `yaml:"resources" json:"resources"`     // capacity + resource control
	Network    NetworkConfig   `yaml:"network" json:"network"`         // vswitch + inner IP
	Boot       BootConfig      `yaml:"boot" json:"boot"`               // boot artifacts (kernel / guest runtime / overlay)
}

// ResourcesConfig is the conductor-owned sandbox resource policy. It is a
// public value schema and deliberately omits sandbox runtime control, deflate,
// and sensor fields.
type ResourcesConfig struct {
	Capacity      ResourceCapacity       `yaml:"capacity" json:"capacity"`
	Allocatable   ResourceAllocatable    `yaml:"allocatable" json:"allocatable"`
	Startup       *ResourceStartup       `yaml:"startup,omitempty" json:"startup,omitempty"`
	Overhead      ResourceOverhead       `yaml:"overhead" json:"overhead"`
	WatermarkHigh *ResourceWatermarkHigh `yaml:"watermark_high,omitempty" json:"watermark_high,omitempty"`
}

// UnmarshalJSON preserves the public schema's strict nested JSON decoding.
// Pointer presence carries allocatable-memory semantics directly; no separate
// parsing state is reconstructed here.
func (r *ResourcesConfig) UnmarshalJSON(raw []byte) error {
	type wire ResourcesConfig
	var decoded wire
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&decoded); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return fmt.Errorf("multiple JSON values are not allowed")
		}
		return err
	}
	*r = ResourcesConfig(decoded)
	return nil
}

type ResourceCapacity struct {
	CPU    int    `yaml:"cpu" json:"cpu"`
	Memory string `yaml:"memory" json:"memory"`
}

type ResourceAllocatable struct {
	CPU *float64 `yaml:"cpu,omitempty" json:"cpu,omitempty"`
	// Memory is nil when the operator or Configure hook inherits the 256MiB
	// default. A non-nil value is explicit, including an explicit "256MiB".
	Memory *string `yaml:"memory,omitempty" json:"memory,omitempty"`
}

// SetMemory makes value an explicit allocatable-memory policy.
func (a *ResourceAllocatable) SetMemory(value string) { a.Memory = &value }

// InheritMemory restores the internal 256MiB default and clamp semantics.
func (a *ResourceAllocatable) InheritMemory() { a.Memory = nil }

type ResourceStartup struct {
	Memory string `yaml:"memory" json:"memory"`
}

type ResourceOverhead struct {
	Memory string `yaml:"memory" json:"memory"`
}

type ResourceWatermarkHigh struct {
	Ratio *float64 `yaml:"ratio,omitempty" json:"ratio,omitempty"`
}

func (r *ResourcesConfig) applyDefaults() {
	policy := r.nodeResourcePolicy()
	policy.ApplyDefaults()
	r.setNodeResourcePolicy(policy)
}

func (r ResourcesConfig) nodeResourcePolicy() sandboxcfg.NodeResourcePolicy {
	policy := sandboxcfg.NodeResourcePolicy{
		Capacity: sandboxcfg.NodeCapacityPolicy{
			CPU: r.Capacity.CPU, Memory: r.Capacity.Memory,
		},
		Allocatable: sandboxcfg.NodeAllocatablePolicy{
			CPU: clonePtr(r.Allocatable.CPU), Memory: clonePtr(r.Allocatable.Memory),
		},
		Startup: func() *sandboxcfg.NodeStartupPolicy {
			if r.Startup == nil {
				return nil
			}
			return &sandboxcfg.NodeStartupPolicy{Memory: r.Startup.Memory}
		}(),
		Overhead: sandboxcfg.NodeOverheadPolicy{Memory: r.Overhead.Memory},
		WatermarkHigh: func() *sandboxcfg.NodeWatermarkHighPolicy {
			if r.WatermarkHigh == nil {
				return nil
			}
			return &sandboxcfg.NodeWatermarkHighPolicy{Ratio: r.WatermarkHigh.Ratio}
		}(),
	}
	return policy
}

func (r *ResourcesConfig) setNodeResourcePolicy(policy sandboxcfg.NodeResourcePolicy) {
	r.Capacity = ResourceCapacity{CPU: policy.Capacity.CPU, Memory: policy.Capacity.Memory}
	r.Allocatable = ResourceAllocatable{
		CPU: clonePtr(policy.Allocatable.CPU), Memory: clonePtr(policy.Allocatable.Memory),
	}
	if policy.Startup == nil {
		r.Startup = nil
	} else {
		r.Startup = &ResourceStartup{Memory: policy.Startup.Memory}
	}
	r.Overhead = ResourceOverhead{Memory: policy.Overhead.Memory}
	if policy.WatermarkHigh == nil {
		r.WatermarkHigh = nil
	} else {
		r.WatermarkHigh = &ResourceWatermarkHigh{Ratio: policy.WatermarkHigh.Ratio}
	}
}

// MemoryMiB parses Memory ("2GiB", "512MiB", "2G", "512M", or a plain byte count)
// into whole MiB, for surfacing the VM's memory in e2b list/get responses. Returns
// 0 if unset or unparseable (the value is informational, not an allocation knob).
func (r ResourcesConfig) MemoryMiB() int { return parseMiB(r.Capacity.Memory) }

func parseMiB(s string) int {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0
	}
	mul := 1.0 / (1 << 20) // plain number = bytes -> MiB
	for _, u := range []struct {
		suf string
		m   float64
	}{
		{"GiB", 1 << 10}, {"Gi", 1 << 10}, {"G", 1 << 10},
		{"MiB", 1}, {"Mi", 1}, {"M", 1},
		{"KiB", 1.0 / (1 << 10)}, {"Ki", 1.0 / (1 << 10)}, {"K", 1.0 / (1 << 10)},
		{"B", 1.0 / (1 << 20)},
	} {
		if strings.HasSuffix(s, u.suf) {
			s = strings.TrimSuffix(s, u.suf)
			mul = u.m
			break
		}
	}
	v, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
	if err != nil {
		return 0
	}
	return int(v * mul)
}

// NetworkConfig is the per-sandbox network: vswitch, guest hostname/DNS (provisioned
// into the guest via SANDBOX_CONFIG network.hostname + files:), and the per-profile
// guest inner IP / default-route gateway.
type NetworkConfig struct {
	Switch      string     `yaml:"switch" json:"switch"`             // vswitch name, e.g. "sw0"
	TapFDSocket string     `yaml:"tapfd_socket" json:"tapfd_socket"` // optional persistent connector TAPFD/1 UDS; empty = CLI attach/open/detach
	Hostname    string     `yaml:"hostname" json:"hostname"`         // guest hostname (sethostname + /etc/hosts entry); default "sandbox"
	DNS         []string   `yaml:"dns" json:"dns"`                   // /etc/resolv.conf nameservers injected into the guest
	E2B         ProfileNet `yaml:"e2b" json:"e2b"`                   // e2b profile inner IP / gateway (envd port-forward needs the /30)
	Bare        ProfileNet `yaml:"bare" json:"bare"`                 // bare profile inner IP / gateway
}

// ProfileNet is one profile's guest inner IP (CIDR) and default-route gateway,
// reused for every sandbox of that profile (identity is the per-slot floating IP).
type ProfileNet struct {
	InnerIP string `yaml:"inner_ip" json:"inner_ip"` // guest NIC CIDR, e.g. "169.254.0.21/30"
	Nexthop string `yaml:"nexthop" json:"nexthop"`   // default-route gateway, e.g. "169.254.0.22"
}

// BootConfig is the boot artifacts that define a sandbox instance.
type BootConfig struct {
	Kernel              string `yaml:"kernel" json:"kernel"`                               // vmlinux path
	Runtime             string `yaml:"runtime" json:"runtime"`                             // guest runtime erofs
	OverlayDiffTemplate string `yaml:"overlay_diff_template" json:"overlay_diff_template"` // pre-formatted ext4 seeding the cold-boot overlay upper
}

// BuilderConfig is the build-instance settings. Durable registration/execution
// admission uses each immutable BuildResources vector; execution CPU/memory are
// additionally enforced on the aggregate slice and each builder service.
// Builds run INSIDE build sandboxes (tenant network + isolation): import and
// step execution happen in microVMs booted from the same guest runtime; only artifact
// streaming and the final uploads run on the host (run-builder).
type BuilderConfig struct {
	Admission        BuilderAdmissionConfig `yaml:"admission" json:"admission"`
	RegistrationTTL  string                 `yaml:"registration_ttl" json:"registration_ttl"`   // default 1h
	QueueTTL         string                 `yaml:"queue_ttl" json:"queue_ttl"`                 // default 30m
	InsecureRegistry bool                   `yaml:"insecure_registry" json:"insecure_registry"` // pull base images over plain HTTP (dev/local registry)
	Platform         string                 `yaml:"platform" json:"platform"`                   // e.g. "linux/amd64"; "" = host default
	// ImageURIMask is the image the e2b CLI pushes its client-built rootfs to,
	// with {templateID}/{buildID} tokens (must match the CLI's E2B_IMAGE_URI_MASK,
	// AND be reachable from inside a build sandbox — the pull runs in the guest);
	// when a build trigger omits fromImage, it is derived from this.
	ImageURIMask string `yaml:"image_uri_mask" json:"image_uri_mask"`

	Referer BuilderRefererConfig `yaml:"referer" json:"referer"`

	// DiffTemplate is the pre-formatted ext4 seeding a build sandbox's
	// writable disk (pull cache + steps delta + export scratch): size it
	// 2-3x the largest expected image (the ext4 size is fixed at mkfs).
	DiffTemplate string `yaml:"diff_template" json:"diff_template"`
	// Phase timeouts (seconds): image pull+flatten, one RUN step, the
	// readyCmd poll budget, and the whole build. Defaults 600/600/120/1800.
	PullTimeoutSec  int `yaml:"pull_timeout_sec" json:"pull_timeout_sec"`
	StepTimeoutSec  int `yaml:"step_timeout_sec" json:"step_timeout_sec"`
	ReadyTimeoutSec int `yaml:"ready_timeout_sec" json:"ready_timeout_sec"`
	TotalTimeoutSec int `yaml:"total_timeout_sec" json:"total_timeout_sec"`

	// FilesStorage is the S3/OBS object store backing COPY build contexts:
	// the client uploads the (gzipped tar) context straight to the bucket via
	// a presigned PUT, and the build sandbox fetches it via a presigned GET.
	// Unset → COPY steps are rejected (501). For local/single-node without a
	// cloud object store, point it at a versitygw gateway. (§11)
	FilesStorage *FilesStorageConfig `yaml:"files_storage" json:"files_storage"`
}

type BuilderAdmissionConfig struct {
	Registration *BuildAdmissionLimitConfig `yaml:"registration,omitempty" json:"registration,omitempty"`
	Execution    *BuildAdmissionLimitConfig `yaml:"execution,omitempty" json:"execution,omitempty"`
}

type BuildAdmissionLimitConfig struct {
	MaxBuilds *int64                        `yaml:"max_builds,omitempty" json:"max_builds,omitempty"`
	Resources BuildAdmissionResourcesConfig `yaml:"resources,omitempty" json:"resources,omitempty"`
}

type BuildAdmissionResourcesConfig struct {
	CPU     *CPUCores `yaml:"cpu,omitempty" json:"cpu,omitempty"`
	Memory  *string   `yaml:"memory,omitempty" json:"memory,omitempty"`
	Storage *string   `yaml:"storage,omitempty" json:"storage,omitempty"`
}

// CPUCores preserves the exact YAML decimal so conversion to milli-CPU can
// conservatively round upward instead of first passing through float64.
type CPUCores string

func (c CPUCores) MarshalJSON() ([]byte, error) {
	raw := []byte(c)
	if len(raw) == 0 || !json.Valid(raw) {
		return nil, fmt.Errorf("CPU cores must be a JSON number")
	}
	var number json.Number
	if err := json.Unmarshal(raw, &number); err != nil {
		return nil, fmt.Errorf("CPU cores must be a JSON number")
	}
	return raw, nil
}

func (c *CPUCores) UnmarshalJSON(raw []byte) error {
	var number json.Number
	if err := json.Unmarshal(raw, &number); err != nil {
		return fmt.Errorf("CPU cores must be a JSON number")
	}
	*c = CPUCores(number.String())
	return nil
}

func (c *CPUCores) UnmarshalYAML(node *yaml.Node) error {
	if node == nil || node.Kind != yaml.ScalarNode || (node.Tag != "!!int" && node.Tag != "!!float") {
		return fmt.Errorf("CPU cores must be a numeric scalar")
	}
	*c = CPUCores(node.Value)
	return nil
}

func (c CPUCores) MarshalYAML() (any, error) {
	return &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!float", Value: string(c)}, nil
}

type resolvedBuildResources struct {
	CPU     int64
	Memory  int64
	Storage int64
}

type resolvedBuildAdmission struct {
	MaxBuilds int64
	Resources resolvedBuildResources
}

func (c BuildAdmissionLimitConfig) resolved(path string) (resolvedBuildAdmission, error) {
	var out resolvedBuildAdmission
	if c.MaxBuilds != nil {
		if *c.MaxBuilds <= 0 {
			return out, fmt.Errorf("%s.max_builds must be > 0", path)
		}
		out.MaxBuilds = *c.MaxBuilds
	}
	if c.Resources.CPU != nil {
		number := json.Number(*c.Resources.CPU)
		value, err := buildcfg.CPUCoresToMilli(path+".resources.cpu", number)
		if err != nil {
			return out, err
		}
		out.Resources.CPU = value
	}
	for _, size := range []struct {
		name string
		raw  *string
		dst  *int64
	}{
		{"memory", c.Resources.Memory, &out.Resources.Memory},
		{"storage", c.Resources.Storage, &out.Resources.Storage},
	} {
		if size.raw == nil {
			continue
		}
		value, err := buildcfg.SizeBytes(path+".resources."+size.name, *size.raw)
		if err != nil {
			return out, err
		}
		*size.dst = value
	}
	return out, nil
}

func cloneBuildAdmissionLimit(in *BuildAdmissionLimitConfig) *BuildAdmissionLimitConfig {
	if in == nil {
		return nil
	}
	out := *in
	if in.MaxBuilds != nil {
		value := *in.MaxBuilds
		out.MaxBuilds = &value
	}
	if in.Resources.CPU != nil {
		value := *in.Resources.CPU
		out.Resources.CPU = &value
	}
	if in.Resources.Memory != nil {
		value := *in.Resources.Memory
		out.Resources.Memory = &value
	}
	if in.Resources.Storage != nil {
		value := *in.Resources.Storage
		out.Resources.Storage = &value
	}
	return &out
}

func (b BuilderConfig) registrationLimit() (resolvedBuildAdmission, error) {
	if b.Admission.Registration == nil {
		return resolvedBuildAdmission{}, fmt.Errorf("builder.admission.registration is unresolved")
	}
	return b.Admission.Registration.resolved("builder.admission.registration")
}

func (b BuilderConfig) executionLimit() (resolvedBuildAdmission, error) {
	if b.Admission.Execution == nil {
		return resolvedBuildAdmission{}, fmt.Errorf("builder.admission.execution is unresolved")
	}
	return b.Admission.Execution.resolved("builder.admission.execution")
}

func (b BuilderConfig) RegistrationTTLDur() time.Duration {
	d, _ := time.ParseDuration(b.RegistrationTTL)
	return d
}

func (b BuilderConfig) QueueTTLDur() time.Duration {
	d, _ := time.ParseDuration(b.QueueTTL)
	return d
}

type BuilderRefererConfig struct {
	Enabled   bool   `yaml:"enabled" json:"enabled"`
	Fallback  *bool  `yaml:"fallback" json:"fallback"`
	Writeback *bool  `yaml:"writeback" json:"writeback"`
	Desc      string `yaml:"desc" json:"desc"`
	Key       string `yaml:"key" json:"key"`
	Validity  string `yaml:"validity" json:"validity"`
}

func (r BuilderRefererConfig) FallbackEnabled() bool {
	return r.Fallback == nil || *r.Fallback
}

func (r BuilderRefererConfig) WritebackEnabled() bool {
	return r.Writeback == nil || *r.Writeback
}

// FilesStorageConfig configures the COPY build-context object store. The
// orchestrator only ever PRESIGNS (PUT for the client, GET for the build) and
// HEADs (presence) — bytes never transit the control plane.
type FilesStorageConfig struct {
	Endpoint  string `yaml:"endpoint" json:"endpoint"`     // S3 endpoint; empty = AWS default
	Region    string `yaml:"region" json:"region"`         // e.g. "cn-north-4" / "us-east-1"
	Bucket    string `yaml:"bucket" json:"bucket"`         // required
	Prefix    string `yaml:"prefix" json:"prefix"`         // optional key prefix
	AccessKey string `yaml:"access_key" json:"access_key"` // empty → AWS default chain (env / instance role)
	SecretKey string `yaml:"secret_key" json:"secret_key"` // paired with access_key
	// ForcePathStyle selects path-style addressing (host/bucket/key). Default
	// false (virtual-host, what AWS S3 / OBS use); versitygw / minio need true.
	ForcePathStyle bool `yaml:"force_path_style" json:"force_path_style"`
	// PresignExpiry bounds the upload (PUT) URL the client receives; default
	// 1h. The build-side GET is presigned for total_timeout_sec + headroom.
	PresignExpiry string `yaml:"presign_expiry" json:"presign_expiry"`
}

// PresignExpiryDur parses presign_expiry (default 1h on empty / parse error).
func (f *FilesStorageConfig) PresignExpiryDur() time.Duration {
	if f.PresignExpiry != "" {
		if d, err := time.ParseDuration(f.PresignExpiry); err == nil && d > 0 {
			return d
		}
	}
	return time.Hour
}

// CheckpointConfig is the paused-state capture policy. Both capture modes may
// explicitly override sandbox-ctl's merge-ref and drop-caches defaults.
type CheckpointConfig struct {
	Mode       string                 `yaml:"mode" json:"mode"`               // local (default) | bundle
	LocalDir   string                 `yaml:"local_dir" json:"local_dir"`     // local checkpoint files dir; default /var/lib/sandbox-saved
	MergeRef   *bool                  `yaml:"merge_ref" json:"merge_ref"`     // nil delegates to sandbox-ctl
	DropCaches *bool                  `yaml:"drop_caches" json:"drop_caches"` // nil delegates to sandbox-ctl
	Remote     CheckpointRemoteConfig `yaml:"remote" json:"remote"`
}

type CheckpointRemoteConfig struct {
	RefLocationParent string `yaml:"ref_location_parent" json:"ref_location_parent"`
}

// RefLocationURI derives the node-local path for a publication name
// (reflocation.PublicationName — the entity id plus its publication date).
// The name is self-contained inside the portable ref, so any node resolves
// the identical path; the parent never enters a portable ref, TemplateID, or
// migration token. Bucketing rules live in the reflocation package; GC policy
// and deletion flow are out of scope for this repo.
func (c CheckpointConfig) RefLocationURI(name string) (string, error) {
	location, err := reflocation.Resolve(c.Remote.RefLocationParent, name)
	if err != nil {
		return "", err
	}
	return location.URI, nil
}

// LoadConductor reads, strictly decodes, defaults, and declaratively validates
// a conductor configuration file. Startup callers must separately invoke
// ValidateConductorFinal after any custom Configure hook.
func LoadConductor(path string) (*Conductor, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("config: read %s: %w", path, err)
	}
	defer f.Close()
	c, err := DecodeConductor(f)
	if err != nil {
		return nil, fmt.Errorf("config: parse %s: %w", path, err)
	}
	return c, nil
}

// DecodeConductor strictly decodes one YAML document, applies declarative
// defaults, and validates provided values. It does not inspect ambient runtime
// material, component files, or final startup requirements.
func DecodeConductor(r io.Reader) (*Conductor, error) {
	b, err := io.ReadAll(io.LimitReader(r, maxConfigBytes+1))
	if err != nil {
		return nil, err
	}
	if len(b) > maxConfigBytes {
		return nil, fmt.Errorf("configuration exceeds %d bytes", maxConfigBytes)
	}
	if err := rejectBuilderAdmissionNulls(b); err != nil {
		return nil, err
	}
	var c Conductor
	if err := decodeKnownYAML(b, &c); err != nil {
		return nil, err
	}
	c.applyDefaults()
	if err := c.validateDeclarative(); err != nil {
		return nil, err
	}
	return &c, nil
}

const maxConfigBytes = 4 << 20

// rejectBuilderAdmissionNulls preserves the distinction between an omitted
// admission block/leaf and an explicitly null one. yaml.v3 otherwise maps both
// forms to nil pointers, which would incorrectly turn invalid operator input
// into defaults or an unlimited resource dimension.
func rejectBuilderAdmissionNulls(raw []byte) error {
	var document yaml.Node
	if err := yaml.Unmarshal(raw, &document); err != nil {
		return err
	}
	if len(document.Content) == 0 {
		return nil
	}
	var walk func(*yaml.Node, string) error
	walk = func(node *yaml.Node, path string) error {
		if node.Kind == yaml.AliasNode {
			node = node.Alias
		}
		if node.Tag == "!!null" {
			return fmt.Errorf("%s must not be null", path)
		}
		if node.Kind != yaml.MappingNode {
			return nil
		}
		for i := 0; i+1 < len(node.Content); i += 2 {
			key, value := node.Content[i], node.Content[i+1]
			childPath := key.Value
			if path != "" {
				childPath = path + "." + key.Value
			}
			if err := walk(value, childPath); err != nil {
				return err
			}
		}
		return nil
	}
	root := document.Content[0]
	if root.Kind != yaml.MappingNode {
		return nil
	}
	for i := 0; i+1 < len(root.Content); i += 2 {
		if root.Content[i].Value != "builder" {
			continue
		}
		builder := root.Content[i+1]
		if builder.Kind == yaml.AliasNode {
			builder = builder.Alias
		}
		if builder.Tag == "!!null" {
			return fmt.Errorf("builder must not be null")
		}
		if builder.Kind != yaml.MappingNode {
			return nil
		}
		for j := 0; j+1 < len(builder.Content); j += 2 {
			if builder.Content[j].Value == "admission" {
				return walk(builder.Content[j+1], "builder.admission")
			}
		}
	}
	return nil
}

func (c *Conductor) applyDefaults() {
	def := func(p *string, v string) {
		if *p == "" {
			*p = v
		}
	}
	defBool := func(p **bool, v bool) {
		if *p == nil {
			*p = &v
		}
	}
	def(&c.API.Listen, ":443")
	def(&c.Proxy.Mode, ProxyInternal)
	def(&c.Proxy.Auth, AuthEnforce)
	def(&c.Proxy.ParkTimeout, "30s")
	def(&c.ManifestConfig, "/opt/sandbox/manifest.yaml")
	def(&c.Paths.RunRoot, "/run/sandbox")
	def(&c.Paths.BaseRoot, "/var/lib/sandbox")
	def(&c.Paths.ConfigSocket, "/run/sandbox/node-ctl.socket")
	if c.Paths.DBPath == "" {
		c.Paths.DBPath = filepath.Join(c.Paths.BaseRoot, "node-ctl.db")
	}
	def(&c.Units.Dir, "/etc/systemd/system")
	def(&c.Units.Runner, "sandbox-runner@.service")
	def(&c.Units.Builder, "sandbox-builder@.service")
	def(&c.Units.PoolWaitTimeout, "5s")
	if c.Units.Install == nil {
		t := true
		c.Units.Install = &t
	}
	if c.Sandbox.TimeoutSec == 0 {
		c.Sandbox.TimeoutSec = 300
	}
	c.Sandbox.Resources.applyDefaults()
	def(&c.Sandbox.Network.Switch, "sw0")
	def(&c.Sandbox.Network.Hostname, "sandbox")
	if len(c.Sandbox.Network.DNS) == 0 {
		c.Sandbox.Network.DNS = []string{"169.254.169.253"}
	}
	def(&c.Sandbox.Network.E2B.InnerIP, "169.254.0.21/30")
	def(&c.Sandbox.Network.E2B.Nexthop, "169.254.0.22")
	def(&c.Sandbox.Network.Bare.InnerIP, "169.254.1.1/31")
	def(&c.Sandbox.Network.Bare.Nexthop, "169.254.1.0")
	if c.Builder.Admission.Execution == nil {
		defaultMax := int64(2)
		c.Builder.Admission.Execution = &BuildAdmissionLimitConfig{MaxBuilds: &defaultMax}
	}
	if c.Builder.Admission.Registration == nil {
		c.Builder.Admission.Registration = cloneBuildAdmissionLimit(c.Builder.Admission.Execution)
	}
	def(&c.Builder.RegistrationTTL, "1h")
	def(&c.Builder.QueueTTL, "30m")
	if c.Builder.PullTimeoutSec <= 0 {
		c.Builder.PullTimeoutSec = 600
	}
	if c.Builder.StepTimeoutSec <= 0 {
		c.Builder.StepTimeoutSec = 600
	}
	if c.Builder.ReadyTimeoutSec <= 0 {
		c.Builder.ReadyTimeoutSec = 120
	}
	if c.Builder.TotalTimeoutSec <= 0 {
		c.Builder.TotalTimeoutSec = 1800
	}
	defBool(&c.Builder.Referer.Fallback, true)
	defBool(&c.Builder.Referer.Writeback, true)
	if c.Builder.Referer.Key == "" {
		c.Builder.Referer.Key = c.Builder.Referer.Desc
	}
	def(&c.Checkpoint.Mode, CheckpointLocal)
	def(&c.Checkpoint.LocalDir, "/var/lib/sandbox-saved")
	def(&c.MMDS.Listen, "127.0.0.1:19254")
	if c.MMDS.Routes.MaxRoutesPerSandbox == 0 {
		c.MMDS.Routes.MaxRoutesPerSandbox = 32
	}
	if c.MMDS.Routes.MaxNamespaceBytes == 0 {
		c.MMDS.Routes.MaxNamespaceBytes = 64 * 1024
	}
	if c.MMDS.Routes.MaxStaticBodyBytes == 0 {
		c.MMDS.Routes.MaxStaticBodyBytes = 16 * 1024
	}
	if c.MMDS.Routes.MaxSecretValueBytes == 0 {
		c.MMDS.Routes.MaxSecretValueBytes = 16 * 1024
	}
	if c.MMDS.Routes.ReservedPathPrefixes == nil {
		c.MMDS.Routes.ReservedPathPrefixes = []string{"/latest/api/", "/internal/"}
	}
	if c.ResourceListen != nil {
		c.ResourceListen.ApplyDefaults()
	}
}

// ParkTimeoutDur parses proxy.park_timeout (default 30s on any parse error).
func (c *Conductor) ParkTimeoutDur() time.Duration {
	d, err := time.ParseDuration(c.Proxy.ParkTimeout)
	if err != nil || d <= 0 {
		return 30 * time.Second
	}
	return d
}

// ValidateConductorFinal validates the fully configured declarative value
// without applying defaults or resolving runtime material. Custom Apps call it
// after Configure; a Runtime encryption-key provider may satisfy material
// requirements separately.
func ValidateConductorFinal(c *Conductor) error {
	if c == nil {
		return fmt.Errorf("config: conductor is required")
	}
	if c.Paths.ConductorExecutable != "" && !filepath.IsAbs(c.Paths.ConductorExecutable) {
		return fmt.Errorf("config: paths.conductor_executable must be absolute")
	}
	if c.API.Domain == "" {
		return fmt.Errorf("config: api.domain is required")
	}
	if c.Sandbox.Boot.Kernel == "" {
		return fmt.Errorf("config: sandbox.boot.kernel is required")
	}
	if c.Sandbox.Boot.Runtime == "" {
		return fmt.Errorf("config: sandbox.boot.runtime is required")
	}
	return c.validateDeclarative()
}

func (c *Conductor) validateDeclarative() error {
	if c.Paths.ConductorExecutable != "" && !filepath.IsAbs(c.Paths.ConductorExecutable) {
		return fmt.Errorf("config: paths.conductor_executable must be absolute")
	}
	dynamicResources := c.ResourceListen != nil && c.ResourceListen.Enabled
	if err := sandboxcfg.ValidateNodeResourcePolicy(c.Sandbox.Resources.nodeResourcePolicy(), dynamicResources); err != nil {
		return fmt.Errorf("config: %w", err)
	}
	if c.Sandbox.Network.TapFDSocket != "" && !filepath.IsAbs(c.Sandbox.Network.TapFDSocket) {
		return fmt.Errorf("config: sandbox.network.tapfd_socket must be absolute")
	}
	registration, err := c.Builder.registrationLimit()
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}
	execution, err := c.Builder.executionLimit()
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}
	for _, dimension := range []struct {
		name         string
		registration int64
		execution    int64
	}{
		{"max_builds", registration.MaxBuilds, execution.MaxBuilds},
		{"resources.cpu", registration.Resources.CPU, execution.Resources.CPU},
		{"resources.memory", registration.Resources.Memory, execution.Resources.Memory},
		{"resources.storage", registration.Resources.Storage, execution.Resources.Storage},
	} {
		if dimension.registration > 0 && dimension.execution > 0 && dimension.registration < dimension.execution {
			return fmt.Errorf("config: builder.admission.registration.%s must be >= builder.admission.execution.%s", dimension.name, dimension.name)
		}
	}
	for name, raw := range map[string]string{
		"builder.registration_ttl": c.Builder.RegistrationTTL,
		"builder.queue_ttl":        c.Builder.QueueTTL,
	} {
		duration, err := time.ParseDuration(raw)
		if err != nil || duration <= 0 {
			return fmt.Errorf("config: %s must be a positive duration", name)
		}
	}
	if c.Builder.Referer.Enabled && c.Builder.Referer.Desc == "" {
		return fmt.Errorf("config: builder.referer.desc is required when builder.referer.enabled=true")
	}
	if c.Builder.Referer.Validity != "" {
		validity, err := time.ParseDuration(c.Builder.Referer.Validity)
		if err != nil {
			return fmt.Errorf("config: builder.referer.validity %q: %w", c.Builder.Referer.Validity, err)
		}
		if validity <= 0 {
			return fmt.Errorf("config: builder.referer.validity must be positive")
		}
	}
	if c.Units.RunnerPoolSize < 0 {
		return fmt.Errorf("config: units.runner_pool_size must be >= 0")
	}
	if c.Units.BuilderPoolSize < 0 {
		return fmt.Errorf("config: units.builder_pool_size must be >= 0")
	}
	if c.Units.BuilderPoolSize > 0 &&
		(execution.Resources.CPU > 0 || execution.Resources.Memory > 0) {
		return fmt.Errorf("config: units.builder_pool_size must be 0 when builder.admission.execution configures CPU or memory; idle builders are not execution-admitted")
	}
	poolWait, err := time.ParseDuration(c.Units.PoolWaitTimeout)
	if err != nil {
		return fmt.Errorf("config: units.pool_wait_timeout %q: %w", c.Units.PoolWaitTimeout, err)
	}
	if poolWait <= 0 {
		return fmt.Errorf("config: units.pool_wait_timeout must be > 0")
	}
	switch c.Checkpoint.Mode {
	case CheckpointLocal, CheckpointBundle:
	default:
		return fmt.Errorf("config: checkpoint.mode %q (want local|bundle)", c.Checkpoint.Mode)
	}
	if parent := c.Checkpoint.Remote.RefLocationParent; parent != "" {
		if _, err := parseAbsoluteFileURI(parent); err != nil {
			return fmt.Errorf("config: checkpoint.remote.ref_location_parent: %w", err)
		}
	}
	if err := c.validateMMDSRoutes(); err != nil {
		return err
	}
	return c.validateProxy()
}

func (c *Conductor) validateMMDSRoutes() error {
	limits := []struct {
		name  string
		value int
	}{
		{"mmds.routes.max_routes_per_sandbox", c.MMDS.Routes.MaxRoutesPerSandbox},
		{"mmds.routes.max_namespace_bytes", c.MMDS.Routes.MaxNamespaceBytes},
		{"mmds.routes.max_static_body_bytes", c.MMDS.Routes.MaxStaticBodyBytes},
		{"mmds.routes.max_secret_value_bytes", c.MMDS.Routes.MaxSecretValueBytes},
	}
	for _, limit := range limits {
		if limit.value <= 0 {
			return fmt.Errorf("config: %s must be positive", limit.name)
		}
	}
	if err := sandboxcfg.ValidateMMDSReservedPathPrefixes(c.MMDS.Routes.ReservedPathPrefixes); err != nil {
		return fmt.Errorf("config: mmds.routes.reserved_path_prefixes: %w", err)
	}
	for name, service := range c.MMDS.Services {
		if name == "" {
			return fmt.Errorf("config: mmds.services contains an empty service name")
		}
		if _, err := mmdssvc.UnixSocketPath(service.Endpoint); err != nil {
			return fmt.Errorf("config: mmds.services[%q].endpoint: %w", name, err)
		}
	}
	return nil
}

func parseAbsoluteFileURI(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return nil, err
	}
	if u.Scheme != "file" || u.Host != "" || u.Path == "" || !path.IsAbs(u.Path) ||
		u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Opaque != "" {
		return nil, fmt.Errorf("%q must be an absolute file:// URI without host, query, or fragment", raw)
	}
	return u, nil
}

// validateProxy checks the proxy-mode + mmds invariants.
func (c *Conductor) validateProxy() error {
	switch c.Proxy.Mode {
	case ProxyInternal, ProxyExternal, ProxyOff:
	default:
		return fmt.Errorf("config: proxy.mode %q (want internal|external|off)", c.Proxy.Mode)
	}
	switch c.Proxy.Auth {
	case AuthOff, AuthLog, AuthEnforce:
	default:
		return fmt.Errorf("config: proxy.auth %q (want off|log|enforce)", c.Proxy.Auth)
	}
	// proxy.mode=external needs no static socket list: the proxy master registers
	// its proxy_socket on the config-socket plugin plane, so there is nothing to
	// require here.
	if c.Proxy.ProxyNetNS != "" && c.Proxy.Mode != ProxyInternal {
		return fmt.Errorf("config: proxy.proxy_netns requires proxy.mode=internal (external mode uses proxy.yaml proxy_netns)")
	}

	// MMDS off => envd is non-secure, so the proxy must be the enforcing sole gate.
	if !c.MMDS.Enabled && c.Proxy.Auth != AuthEnforce {
		return fmt.Errorf("config: mmds.enabled=false requires proxy.auth=enforce (envd runs non-secure; the proxy is the only data-plane gate)")
	}
	if c.MMDS.Enabled && c.Proxy.Mode == ProxyOff {
		return fmt.Errorf("config: mmds.enabled=true requires proxy.mode!=off (the MMDS service is hosted by the proxy)")
	}
	if c.MMDS.Routes.Enabled && !c.MMDS.Enabled {
		return fmt.Errorf("config: mmds.routes.enabled=true requires mmds.enabled=true")
	}
	if f := c.Builder.FilesStorage; f != nil && f.Bucket == "" {
		return fmt.Errorf("config: builder.files_storage.bucket is required when files_storage is set")
	}
	return nil
}

// Proxy is the external data-plane proxy master's declarative config
// (node-ctl proxy serve --config <this>). The master owns the routesync
// subscription, shared route table, listener fds, and worker supervision. Serve
// still pushes the authoritative auth/park policy over the registration stream;
// local values are bootstrap fallbacks until that handshake completes.
type Proxy struct {
	ConfigSocket  string           `yaml:"config_socket" json:"config_socket"`   // serve control socket to register + sync on (= serve paths.config_socket)
	Paths         ProxyPathsConfig `yaml:"paths" json:"paths"`                   // node-local paths used directly by proxy workers
	DataListen    string           `yaml:"data_listen" json:"data_listen"`       // data-plane ingress; "" = UDS-only proxyForwarder
	ProxyNetNS    string           `yaml:"proxy_netns" json:"proxy_netns"`       // optional forwarding netns for floatingip TCP dials and conductor-pushed MMDS listen
	ProxySocket   string           `yaml:"proxy_socket" json:"proxy_socket"`     // UDS registered for conductor proxyForwarder; default <dir(config_socket)>/proxy.sock
	StatsSocket   string           `yaml:"stats_socket" json:"stats_socket"`     // master-only traffic stats UDS; default <dir(config_socket)>/proxy-stats.sock
	ShmPath       string           `yaml:"shm_path" json:"shm_path"`             // shared route table path; default <dir(config_socket)>/proxy-routes.shm
	RouteCapacity int              `yaml:"route_capacity" json:"route_capacity"` // fixed shared route slots; default 65536
	Workers       int              `yaml:"workers" json:"workers"`               // worker processes supervised by this master; default 1
	TLS           TLSConfig        `yaml:"tls" json:"tls"`                       // data-plane listener cert (= serve's wildcard); "" = h2c
	Auth          string           `yaml:"auth" json:"auth"`                     // bootstrap fallback until serve pushes policy: off|log|enforce (default enforce)
	ParkTimeout   string           `yaml:"park_timeout" json:"park_timeout"`     // bootstrap fallback; default 30s
	MetricsListen string           `yaml:"metrics_listen" json:"metrics_listen"` // master metrics endpoint; aggregates worker data-plane counters
}

// Clone returns a deep copy of the external proxy configuration.
func (p *Proxy) Clone() *Proxy {
	if p == nil {
		return nil
	}
	out := *p
	return &out
}

// ProxyPathsConfig contains only paths consumed by the external proxy. It is
// deliberately separate from the conductor's broader PathsConfig.
type ProxyPathsConfig struct {
	ProxyExecutable string `yaml:"proxy_executable,omitempty" json:"proxy_executable,omitempty"` // custom proxy binary; empty uses node-ctl's built-in app
	RunRoot         string `yaml:"run_root" json:"run_root"`                                     // sandbox runtime root containing <sid>/ctl.sock; required
}

// LoadProxy reads the proxy master config, applies defaults, and validates
// provided declarative values. Startup callers separately validate final
// requirements after any custom Configure hook.
func LoadProxy(path string) (*Proxy, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("proxy config: read %s: %w", path, err)
	}
	defer f.Close()
	p, err := DecodeProxy(f)
	if err != nil {
		return nil, fmt.Errorf("proxy config: parse %s: %w", path, err)
	}
	return p, nil
}

// DecodeProxy strictly decodes one proxy YAML document, applies declarative
// defaults, and validates provided values without selecting a startup mode or
// inspecting a component executable.
func DecodeProxy(r io.Reader) (*Proxy, error) {
	b, err := io.ReadAll(io.LimitReader(r, maxConfigBytes+1))
	if err != nil {
		return nil, err
	}
	if len(b) > maxConfigBytes {
		return nil, fmt.Errorf("configuration exceeds %d bytes", maxConfigBytes)
	}
	var p Proxy
	if err := decodeKnownYAML(b, &p); err != nil {
		return nil, err
	}
	p.applyDefaults()
	if err := p.validateDeclarative(); err != nil {
		return nil, err
	}
	return &p, nil
}

func (p *Proxy) applyDefaults() {
	if p.ConfigSocket == "" {
		p.ConfigSocket = "/run/sandbox/node-ctl.socket"
	}
	if p.ProxySocket == "" {
		p.ProxySocket = filepath.Join(filepath.Dir(p.ConfigSocket), "proxy.sock")
	}
	if p.StatsSocket == "" {
		p.StatsSocket = filepath.Join(filepath.Dir(p.ConfigSocket), "proxy-stats.sock")
	}
	if p.ShmPath == "" {
		p.ShmPath = filepath.Join(filepath.Dir(p.ConfigSocket), "proxy-routes.shm")
	}
	if p.Workers == 0 {
		p.Workers = 1
	}
	if p.RouteCapacity == 0 {
		p.RouteCapacity = 65536
	}
	if p.Auth == "" {
		p.Auth = AuthEnforce
	}
	if p.ParkTimeout == "" {
		p.ParkTimeout = "30s"
	}
}

// ValidateProxyFinal validates a fully configured external proxy without
// applying defaults. Custom proxy Apps call it after their master-only
// Configure hook and before freezing the effective worker configuration.
func ValidateProxyFinal(p *Proxy) error {
	if p == nil {
		return fmt.Errorf("proxy config: proxy is required")
	}
	if p.Paths.ProxyExecutable != "" && !filepath.IsAbs(p.Paths.ProxyExecutable) {
		return fmt.Errorf("proxy config: paths.proxy_executable must be absolute")
	}
	if p.Paths.RunRoot == "" {
		return fmt.Errorf("proxy config: paths.run_root is required")
	}
	return p.validateDeclarative()
}

func (p *Proxy) validateDeclarative() error {
	if p.Paths.ProxyExecutable != "" && !filepath.IsAbs(p.Paths.ProxyExecutable) {
		return fmt.Errorf("proxy config: paths.proxy_executable must be absolute")
	}
	switch p.Auth {
	case AuthOff, AuthLog, AuthEnforce:
	default:
		return fmt.Errorf("proxy config: auth %q (want off|log|enforce)", p.Auth)
	}
	if _, err := time.ParseDuration(p.ParkTimeout); err != nil {
		return fmt.Errorf("proxy config: park_timeout %q: %w", p.ParkTimeout, err)
	}
	if p.Workers <= 0 {
		return fmt.Errorf("proxy config: workers must be positive")
	}
	if p.RouteCapacity <= 0 {
		return fmt.Errorf("proxy config: route_capacity must be positive")
	}
	if !filepath.IsAbs(p.StatsSocket) {
		return fmt.Errorf("proxy config: stats_socket must be an absolute path")
	}
	for name, path := range map[string]string{
		"config_socket": p.ConfigSocket,
		"proxy_socket":  p.ProxySocket,
		"shm_path":      p.ShmPath,
	} {
		if filepath.Clean(path) == filepath.Clean(p.StatsSocket) {
			return fmt.Errorf("proxy config: stats_socket conflicts with %s", name)
		}
	}
	return nil
}

// ParkTimeoutDur parses the worker's park_timeout fallback (default 30s).
func (p *Proxy) ParkTimeoutDur() time.Duration {
	d, err := time.ParseDuration(p.ParkTimeout)
	if err != nil || d <= 0 {
		return 30 * time.Second
	}
	return d
}

func decodeKnownYAML(b []byte, out any) error {
	dec := yaml.NewDecoder(bytes.NewReader(b))
	dec.KnownFields(true)
	if err := dec.Decode(out); err != nil {
		return err
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		if err == nil {
			return fmt.Errorf("multiple YAML documents are not allowed")
		}
		return err
	}
	return nil
}
