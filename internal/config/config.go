// Package config loads node-ctl's node-local configuration.
//
// The YAML is grouped by concern: api / proxy / paths / units / sandbox (the
// sandbox-instance defaults, sub-grouped resources/network/boot) / builder /
// checkpoint (paused-state tiering), plus a few singular top-level references
// (encryption_key, manifest_config). External binaries are NOT configured —
// they are auto-discovered next to the orchestrator binary, then on PATH.
package config

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/mmdssvc"
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
	CheckpointLocal  = "local"  // sandbox-ctl snapshot --output → node-local files (default; node-bound)
	CheckpointRemote = "remote" // deprecated compatibility mode; legacy routing remains unchanged
)

// Auto-discovered external binary names (resolved via Config.Bin against the
// orchestrator binary's dir, then PATH). They are intentionally not config keys.
const (
	BinSandboxCtl   = "sandbox-ctl"
	BinConnectorCtl = "connector-ctl"
	BinFlattenCtl   = "flatten-ctl"
	BinNodeCtl      = "node-ctl"
)

// Config is the grouped node-local configuration.
type Config struct {
	API     APIConfig     `yaml:"api"`     // north control plane + TLS
	Proxy   ProxyConfig   `yaml:"proxy"`   // data-plane proxy
	Paths   PathsConfig   `yaml:"paths"`   // node-local dirs / sockets
	Units   UnitsConfig   `yaml:"units"`   // systemd unit management
	Sandbox SandboxConfig `yaml:"sandbox"` // sandbox-instance defaults
	Builder BuilderConfig `yaml:"builder"` // build-instance settings
	// ResourceListen optionally hosts the node resource controller in-process
	// (resource_listen, node-resource.md); absent/disabled => sandboxes use static
	// cgroup. The controller's tuning is inlined here — there is no second file.
	ResourceListen *ResourceListenConfig `yaml:"resource_listen,omitempty"`
	// Checkpoint is the paused-state tiering policy (parallel to sandbox).
	Checkpoint CheckpointConfig `yaml:"checkpoint"`
	// MMDS is the optional envd metadata service (re-keys envd to fresh per-identity
	// tokens). Disabled => envd runs non-secure and the proxy is the sole data-plane gate.
	MMDS MMDSConfig `yaml:"mmds"`
	// Cluster connects this node to a cluster-ctl registry over node-link
	// (node.md §10); empty = standalone single-node.
	Cluster ClusterConfig `yaml:"cluster"`
	// Singular top-level references.
	EncryptionKey  string `yaml:"encryption_key"`  // credential at-rest AES-256 (":"-sep, first active); or NODE_CONFIG_ENCRYPTION_KEY env
	ManifestConfig string `yaml:"manifest_config"` // remote manifest store config (path ref; shared by sandbox + builder)

	execDir string // auto: dir of os.Executable(); used by Bin (not a YAML field)
}

// ResourceListenConfig hosts the in-process node resource controller inside serve
// (the resource_listen sub-server, node-resource.md). Absent / Enabled=false =>
// sandboxes use static cgroup. The controller's tuning is inlined here — there is
// no second config file; nodectl.Resolve consumes this struct directly.
type ResourceListenConfig struct {
	Enabled         bool                     `yaml:"enabled"`
	Socket          string                   `yaml:"socket"`            // controller UDS; "" = pkg/resource.DefaultSocket (sandbox-ctl's default)
	StatePath       string                   `yaml:"state_path"`        // deprecated and ignored; retained only so older YAML still parses
	AuditPath       string                   `yaml:"audit_path"`        // audit log (tmpfs); default /run/node-ctl/audit.log
	CgroupScanPaths []string                 `yaml:"cgroup_scan_paths"` // restart recovery roots for populated sandbox cgroups
	Resources       ResourceHostConfig       `yaml:"resources"`         // node physical capacity + host reservation
	Watermarks      ResourceWatermarksConfig `yaml:"watermarks"`        // zone thresholds (fractions of allocatable pool)
	RateLimits      ResourceRateLimitsConfig `yaml:"rate_limits"`       // memory grant rate limit
	Admission       ResourceAdmissionConfig  `yaml:"admission"`         // admit token bucket + queue
	Dampening       ResourceDampeningConfig  `yaml:"dampening"`         // oscillation damping
	LogLevel        string                   `yaml:"log_level"`         // info (default)
}

// ResourceHostConfig is the node's physical capacity and the host's own reservation.
type ResourceHostConfig struct {
	PhysicalMemory string               `yaml:"physical_memory"` // "auto" (/proc/meminfo) or a size string
	PhysicalCPU    string               `yaml:"physical_cpu"`    // "auto" (nproc) or an integer core count
	HostReserved   ResourceHostReserved `yaml:"host_reserved"`   // kernel + node daemons reservation
}

// ResourceHostReserved is the memory/CPU carved out for the host itself.
type ResourceHostReserved struct {
	Memory string  `yaml:"memory"` // default 16GiB
	CPU    float64 `yaml:"cpu"`    // cores; default 1.5
}

// ResourceWatermarksConfig sets the zone thresholds as fractions of the allocatable
// pool (node-resource.md §3.2 / §4).
type ResourceWatermarksConfig struct {
	OperationalMarginFactor float64 `yaml:"operational_margin_factor"` // default 0.10
	HighFactor              float64 `yaml:"high_factor"`               // red zone start; default 0.85
	LowFactor               float64 `yaml:"low_factor"`                // yellow zone start; default 0.70
	EmergencyFactor         float64 `yaml:"emergency_factor"`          // emergency pool; default 0.05
	StartupFactor           float64 `yaml:"startup_factor"`            // startup_pool = pool × this; default 0.50 (emergency < startup ≤ 1.0)
}

// ResourceRateLimitsConfig bounds the memory grant rate.
type ResourceRateLimitsConfig struct {
	MemoryGrantPerSecFactor float64 `yaml:"memory_grant_per_sec_factor"` // × allocatable pool; default 0.05
}

// ResourceAdmissionConfig is the admit token bucket + short-block queue.
type ResourceAdmissionConfig struct {
	Rate          int    `yaml:"rate"`            // tokens/s; default 4
	Burst         int    `yaml:"burst"`           // bucket capacity; default 16
	StartupTTL    string `yaml:"startup_ttl"`     // admit→settled deadline; default 30s
	QueueTTL      string `yaml:"queue_ttl"`       // max short-block wait; default 30s
	QueueMaxDepth int    `yaml:"queue_max_depth"` // queue capacity; default 256
}

// ResourceDampeningConfig damps zone oscillation (does not enter sandbox.yaml).
type ResourceDampeningConfig struct {
	RecoverDuration string `yaml:"recover_duration"` // burst→settled observation; default 60s
	CooldownPeriods int    `yaml:"cooldown_periods"` // × 100ms; default 10
}

// ApplyDefaults fills the controller tuning defaults (node-resource.md §3.2). The
// socket default (pkg/resource.DefaultSocket) is applied by nodectl.Resolve, which
// owns that protocol constant. Exported so nodectl.Resolve can default a config
// block built outside config.Load (e.g. in tests).
func (r *ResourceListenConfig) ApplyDefaults() {
	if r.AuditPath == "" {
		r.AuditPath = "/run/node-ctl/audit.log"
	}
	if len(r.CgroupScanPaths) == 0 {
		r.CgroupScanPaths = []string{"/sys/fs/cgroup/sandbox.slice/sandbox-runner.slice"}
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
	if r.Dampening.RecoverDuration == "" {
		r.Dampening.RecoverDuration = "60s"
	}
	if r.Dampening.CooldownPeriods == 0 {
		r.Dampening.CooldownPeriods = 10
	}
	if r.LogLevel == "" {
		r.LogLevel = "info"
	}
}

// ClusterConfig connects this node to a cluster-ctl registry over node_link
// (node.md §10). Empty NodeLink.Endpoint = standalone single-node (no cluster).
type ClusterConfig struct {
	NodeLink          ClusterNodeLink   `yaml:"node_link"`          // how to reach registry node_link
	NodeID            string            `yaml:"node_id"`            // this node's id; "" = hostname
	Labels            map[string]string `yaml:"labels"`             // zone / pool / slot / node (nodeSelectors)
	DataEndpoint      string            `yaml:"data_endpoint"`      // host:port the router forwards the data plane to
	HeartbeatInterval string            `yaml:"heartbeat_interval"` // node-link heartbeat period; "" = 10s
}

// ClusterNodeLink is how the node dials the registry's node_link listener.
type ClusterNodeLink struct {
	Endpoint string      `yaml:"endpoint"` // registry node_link addr host:port; "" = standalone
	TLS      TLSMaterial `yaml:"tls"`      // node-link client mTLS; empty = plain h2c
}

// TLSMaterial is cert/key + CA (mirrors clustercfg.TLS) for the node-link client.
type TLSMaterial struct {
	Cert string `yaml:"cert"`
	Key  string `yaml:"key"`
	CA   string `yaml:"ca"` // CA that verifies the registry's server cert
}

// APIConfig is the north control plane + TLS.
type APIConfig struct {
	Domain string    `yaml:"domain"` // e.g. sandboxes.example.com
	Listen string    `yaml:"listen"` // ":443" (https); dev ":3000" http
	TLS    TLSConfig `yaml:"tls"`    // wildcard cert for *.<domain> + api.<domain>
}

// TLSConfig is the wildcard TLS material.
type TLSConfig struct {
	Cert string `yaml:"cert"`
	Key  string `yaml:"key"`
}

// ProxyConfig is the data-plane proxy. mode ∈ {internal,external,off}. In external
// mode conductor forwards fallback data-plane requests to the proxy master's
// registered UDS; the proxy master owns data-plane listener sockets.
type ProxyConfig struct {
	Mode          string `yaml:"mode"`           // internal (default) | external | off
	DataListen    string `yaml:"data_listen"`    // dedicated data-plane listener; "" = share api.listen
	ProxyNetNS    string `yaml:"proxy_netns"`    // optional forwarding netns for floatingip TCP dials and internal MMDS listen
	ParkTimeout   string `yaml:"park_timeout"`   // hold a data-plane request awaiting route/resume; default 30s
	Auth          string `yaml:"auth"`           // off | log | enforce (default): validate X-Access-Token
	MetricsListen string `yaml:"metrics_listen"` // optional Prometheus text endpoint; "" = off
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
	Enabled  bool                                `yaml:"enabled"` // false (default) => -isnotfc + proxy-only auth
	Listen   string                              `yaml:"listen"`  // MMDS listener (the vswitch mgmt-service target); default 127.0.0.1:19254
	Routes   MMDSRoutesConfig                    `yaml:"routes"`
	Services map[string]MMDSServiceRegistryEntry `yaml:"services"`
}

// MMDSRoutesConfig is the conductor-owned admission policy for tenant MMDS
// routes. Defaults are applied only to omitted zero values; explicit negative
// limits survive defaulting and fail validation.
type MMDSRoutesConfig struct {
	Enabled              bool     `yaml:"enabled"`
	MaxRoutesPerSandbox  int      `yaml:"max_routes_per_sandbox"`
	MaxNamespaceBytes    int      `yaml:"max_namespace_bytes"`
	MaxStaticBodyBytes   int      `yaml:"max_static_body_bytes"`
	MaxSecretValueBytes  int      `yaml:"max_secret_value_bytes"`
	ReservedPathPrefixes []string `yaml:"reserved_path_prefixes"`
}

// MMDSServiceRegistryEntry names one operator-controlled local service. V1
// accepts only unix:// absolute paths; request timeout and response bounds are
// fixed implementation constants, not tenant or YAML policy.
type MMDSServiceRegistryEntry struct {
	Endpoint string `yaml:"endpoint"`
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
	RunRoot       string `yaml:"run_root"`       // default /run/sandbox (tmpfs)
	BaseRoot      string `yaml:"base_root"`      // default /var/lib/sandbox (persistent)
	DBPath        string `yaml:"db_path"`        // default <base_root>/node-ctl.db
	ConfigSocket  string `yaml:"config_socket"`  // default /run/sandbox/node-ctl.socket
	AdminPidfile  string `yaml:"admin_pidfile"`  // optional PID allowlist (multi-line) gating the socket admin plane; "" => socket perms (same-uid/root) only
	PluginPidfile string `yaml:"plugin_pidfile"` // optional PID allowlist (multi-line) gating the socket plugin plane (proxy/agent registration); "" => socket perms only
}

// UnitsConfig manages the systemd template units (generated + installed at startup).
type UnitsConfig struct {
	Dir             string `yaml:"dir"`               // default /etc/systemd/system
	Runner          string `yaml:"runner"`            // default sandbox-runner@.service
	Builder         string `yaml:"builder"`           // default sandbox-builder@.service
	RunnerPoolSize  int    `yaml:"runner_pool_size"`  // idle prestarted runner units; 0 = disabled
	BuilderPoolSize int    `yaml:"builder_pool_size"` // idle prestarted builder units; 0 = disabled
	PoolWaitTimeout string `yaml:"pool_wait_timeout"` // StartUnit -> WaitAssignment deadline; default 5s
	Install         *bool  `yaml:"install"`           // default true; false = manage out of band
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
	TimeoutSec int             `yaml:"timeout_sec"` // default TTL; default 300
	Capacity   int             `yaml:"capacity"`    // max sandboxes this node admits (cluster headroom denominator, §4.2); 0 = unbounded
	Resources  ResourcesConfig `yaml:"resources"`   // capacity + resource control
	Network    NetworkConfig   `yaml:"network"`     // vswitch + inner IP
	Boot       BootConfig      `yaml:"boot"`        // boot artifacts (kernel / guest runtime / overlay)
}

// ResourcesConfig is the conductor-owned sandbox resource policy. Its
// underlying schema is shared with sandboxcfg's resolver but deliberately does
// not expose sandboxer's runtime control, deflate, watermark, or sensor fields.
type ResourcesConfig sandboxcfg.NodeResourcePolicy

func (r *ResourcesConfig) applyDefaults() {
	policy := sandboxcfg.NodeResourcePolicy(*r)
	policy.ApplyDefaults()
	*r = ResourcesConfig(policy)
}

// Policy returns the resolver input without introducing a second resource
// schema in internal/config.
func (r ResourcesConfig) Policy() sandboxcfg.NodeResourcePolicy {
	return sandboxcfg.NodeResourcePolicy(r)
}

// MemoryMiB parses Memory ("2GiB", "512MiB", "2G", "512M", or a plain byte count)
// into whole MiB, for surfacing the VM's memory in e2b list/get responses. Returns
// 0 if unset or unparseable (the value is informational, not an allocation knob).
func (r ResourcesConfig) MemoryMiB() int { return parseMiB(r.Policy().Capacity.Memory) }

// MemoryMiB parses the build sandbox's memory into whole MiB (cluster build pool).
func (b BuilderConfig) MemoryMiB() int { return parseMiB(b.Memory) }

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
	Switch      string     `yaml:"switch"`       // vswitch name, e.g. "sw0"
	TapFDSocket string     `yaml:"tapfd_socket"` // optional persistent connector TAPFD/1 UDS; empty = CLI attach/open/detach
	Hostname    string     `yaml:"hostname"`     // guest hostname (sethostname + /etc/hosts entry); default "sandbox"
	DNS         []string   `yaml:"dns"`          // /etc/resolv.conf nameservers injected into the guest
	E2B         ProfileNet `yaml:"e2b"`          // e2b profile inner IP / gateway (envd port-forward needs the /30)
	Bare        ProfileNet `yaml:"bare"`         // bare profile inner IP / gateway
}

// ProfileNet is one profile's guest inner IP (CIDR) and default-route gateway,
// reused for every sandbox of that profile (identity is the per-slot floating IP).
type ProfileNet struct {
	InnerIP string `yaml:"inner_ip"` // guest NIC CIDR, e.g. "169.254.0.21/30"
	Nexthop string `yaml:"nexthop"`  // default-route gateway, e.g. "169.254.0.22"
}

// BootConfig is the boot artifacts that define a sandbox instance.
type BootConfig struct {
	Kernel              string `yaml:"kernel"`                // vmlinux path
	Runtime             string `yaml:"runtime"`               // guest runtime erofs
	OverlayDiffTemplate string `yaml:"overlay_diff_template"` // pre-formatted ext4 seeding the cold-boot overlay upper
}

// BuilderConfig is the build-instance settings. Concurrency is admitted in
// node-ctl; the CPU/memory ceiling is applied to sandbox-builder.slice.
// Builds run INSIDE build sandboxes (tenant network + isolation): import and
// step execution happen in microVMs booted from the same guest runtime; only artifact
// streaming and the final uploads run on the host (run-builder).
type BuilderConfig struct {
	MaxConcurrent    int    `yaml:"max_concurrent"`    // default 2
	CPUQuota         string `yaml:"cpu_quota"`         // e.g. "200%"; "" = unset
	MemoryMax        string `yaml:"memory_max"`        // e.g. "8G"; "" = unset
	InsecureRegistry bool   `yaml:"insecure_registry"` // pull base images over plain HTTP (dev/local registry)
	Platform         string `yaml:"platform"`          // e.g. "linux/amd64"; "" = host default
	// ImageURIMask is the image the e2b CLI pushes its client-built rootfs to,
	// with {templateID}/{buildID} tokens (must match the CLI's E2B_IMAGE_URI_MASK,
	// AND be reachable from inside a build sandbox — the pull runs in the guest);
	// when a build trigger omits fromImage, it is derived from this.
	ImageURIMask string `yaml:"image_uri_mask"`

	Referer BuilderRefererConfig `yaml:"referer"`

	// DiffTemplate is the pre-formatted ext4 seeding a build sandbox's
	// writable disk (pull cache + steps delta + export scratch): size it
	// 2-3x the largest expected image (the ext4 size is fixed at mkfs).
	DiffTemplate string `yaml:"diff_template"`
	// VCPU / Memory are the build sandbox's capacity (defaults 2 / "4GiB").
	VCPU   int    `yaml:"vcpu"`
	Memory string `yaml:"memory"`
	// Phase timeouts (seconds): image pull+flatten, one RUN step, the
	// readyCmd poll budget, and the whole build. Defaults 600/600/120/1800.
	PullTimeoutSec  int `yaml:"pull_timeout_sec"`
	StepTimeoutSec  int `yaml:"step_timeout_sec"`
	ReadyTimeoutSec int `yaml:"ready_timeout_sec"`
	TotalTimeoutSec int `yaml:"total_timeout_sec"`

	// FilesStorage is the S3/OBS object store backing COPY build contexts:
	// the client uploads the (gzipped tar) context straight to the bucket via
	// a presigned PUT, and the build sandbox fetches it via a presigned GET.
	// Unset → COPY steps are rejected (501). For local/single-node without a
	// cloud object store, point it at a versitygw gateway. (§11)
	FilesStorage *FilesStorageConfig `yaml:"files_storage"`
}

type BuilderRefererConfig struct {
	Enabled   bool   `yaml:"enabled"`
	Fallback  *bool  `yaml:"fallback"`
	Writeback *bool  `yaml:"writeback"`
	Desc      string `yaml:"desc"`
	Key       string `yaml:"key"`
	Validity  string `yaml:"validity"`
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
	Endpoint  string `yaml:"endpoint"`   // S3 endpoint; empty = AWS default
	Region    string `yaml:"region"`     // e.g. "cn-north-4" / "us-east-1"
	Bucket    string `yaml:"bucket"`     // required
	Prefix    string `yaml:"prefix"`     // optional key prefix
	AccessKey string `yaml:"access_key"` // empty → AWS default chain (env / instance role)
	SecretKey string `yaml:"secret_key"` // paired with access_key
	// ForcePathStyle selects path-style addressing (host/bucket/key). Default
	// false (virtual-host, what AWS S3 / OBS use); versitygw / minio need true.
	ForcePathStyle bool `yaml:"force_path_style"`
	// PresignExpiry bounds the upload (PUT) URL the client receives; default
	// 1h. The build-side GET is presigned for total_timeout_sec + headroom.
	PresignExpiry string `yaml:"presign_expiry"`
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

// CheckpointConfig is the paused-state capture policy. Local capture may
// explicitly override sandbox-ctl's merge-ref and drop-caches defaults. Remote
// mode is retained only for compatibility and does not accept those overrides.
type CheckpointConfig struct {
	Mode       string                 `yaml:"mode"`        // local (default) | remote (deprecated)
	LocalDir   string                 `yaml:"local_dir"`   // local checkpoint files dir; default /var/lib/sandbox-saved
	MergeRef   *bool                  `yaml:"merge_ref"`   // nil delegates to sandbox-ctl
	DropCaches *bool                  `yaml:"drop_caches"` // nil delegates to sandbox-ctl
	Remote     CheckpointRemoteConfig `yaml:"remote"`
}

type CheckpointRemoteConfig struct {
	RefLocationParent string `yaml:"ref_location_parent"`
}

// RefLocationURI derives the node-local path for a logical location name. The
// parent never enters a portable ref, TemplateID, or migration token.
func (c CheckpointConfig) RefLocationURI(name string) (string, error) {
	if name == "" || path.Base(name) != name || strings.ContainsAny(name, `/\\`) || name == "." || name == ".." {
		return "", fmt.Errorf("invalid ref location name %q", name)
	}
	if c.Remote.RefLocationParent == "" {
		return "", fmt.Errorf("checkpoint.remote.ref_location_parent is not configured")
	}
	u, err := parseAbsoluteFileURI(c.Remote.RefLocationParent)
	if err != nil {
		return "", err
	}
	digest := fmt.Sprintf("%x", sha256.Sum256([]byte(name)))
	u.Path = path.Join(u.Path, digest[:2], digest[2:4], name)
	u.RawPath = ""
	return u.String(), nil
}

// Load reads the config file and applies defaults.
func Load(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: read %s: %w", path, err)
	}
	var c Config
	if err := decodeKnownYAML(b, &c); err != nil {
		return nil, fmt.Errorf("config: parse %s: %w", path, err)
	}
	c.applyDefaults()
	return &c, c.validate()
}

func (c *Config) applyDefaults() {
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
	def(&c.Builder.Memory, "4GiB")
	if c.Builder.VCPU <= 0 {
		c.Builder.VCPU = 2
	}
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
	if c.Builder.MaxConcurrent <= 0 {
		c.Builder.MaxConcurrent = 2
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
	if exe, err := os.Executable(); err == nil {
		c.execDir = filepath.Dir(exe)
	}
}

// EncryptionKeySpec returns the effective encryption-key set: the
// NODE_CONFIG_ENCRYPTION_KEY env when set, else the config's encryption_key
// (":"-separated 64-hex keys, first = active).
func (c *Config) EncryptionKeySpec() string {
	if v := os.Getenv("NODE_CONFIG_ENCRYPTION_KEY"); v != "" {
		return v
	}
	return c.EncryptionKey
}

// Bin resolves an external binary name to a path: absolute names pass through,
// bare names resolve against the orchestrator binary's dir (auto-discovered),
// else fall back to the bare name (PATH lookup by the caller's exec).
func (c *Config) Bin(name string) string {
	if filepath.IsAbs(name) {
		return name
	}
	if c.execDir != "" {
		return filepath.Join(c.execDir, filepath.Base(name))
	}
	return name
}

// Resolved external-binary paths (auto-discovered via Bin; not configurable).
func (c *Config) SandboxCtl() string      { return c.Bin(BinSandboxCtl) }
func (c *Config) ManifestCtl() string     { return c.Bin("manifest-ctl") }
func (c *Config) ConnectorCtl() string    { return c.Bin(BinConnectorCtl) }
func (c *Config) FlattenCtl() string      { return c.Bin(BinFlattenCtl) }
func (c *Config) OrchestratorCtl() string { return c.Bin(BinNodeCtl) }

// ParkTimeoutDur parses proxy.park_timeout (default 30s on any parse error).
func (c *Config) ParkTimeoutDur() time.Duration {
	d, err := time.ParseDuration(c.Proxy.ParkTimeout)
	if err != nil || d <= 0 {
		return 30 * time.Second
	}
	return d
}

func (c *Config) validate() error {
	if c.API.Domain == "" {
		return fmt.Errorf("config: api.domain is required")
	}
	if c.EncryptionKeySpec() == "" {
		return fmt.Errorf("config: encryption_key (or NODE_CONFIG_ENCRYPTION_KEY env) is required")
	}
	if c.Sandbox.Boot.Kernel == "" {
		return fmt.Errorf("config: sandbox.boot.kernel is required")
	}
	if c.Sandbox.Boot.Runtime == "" {
		return fmt.Errorf("config: sandbox.boot.runtime is required")
	}
	dynamicResources := c.ResourceListen != nil && c.ResourceListen.Enabled
	if err := sandboxcfg.ValidateNodeResourcePolicy(c.Sandbox.Resources.Policy(), dynamicResources); err != nil {
		return fmt.Errorf("config: %w", err)
	}
	if c.Sandbox.Network.TapFDSocket != "" && !filepath.IsAbs(c.Sandbox.Network.TapFDSocket) {
		return fmt.Errorf("config: sandbox.network.tapfd_socket must be absolute")
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
	poolWait, err := time.ParseDuration(c.Units.PoolWaitTimeout)
	if err != nil {
		return fmt.Errorf("config: units.pool_wait_timeout %q: %w", c.Units.PoolWaitTimeout, err)
	}
	if poolWait <= 0 {
		return fmt.Errorf("config: units.pool_wait_timeout must be > 0")
	}
	switch c.Checkpoint.Mode {
	case CheckpointLocal, CheckpointRemote:
	default:
		return fmt.Errorf("config: checkpoint.mode %q (want local|remote)", c.Checkpoint.Mode)
	}
	if c.Checkpoint.Mode == CheckpointRemote &&
		(c.Checkpoint.MergeRef != nil || c.Checkpoint.DropCaches != nil) {
		return fmt.Errorf("config: checkpoint merge_ref/drop_caches require checkpoint.mode=local")
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

func (c *Config) validateMMDSRoutes() error {
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
func (c *Config) validateProxy() error {
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

// ProxyFileConfig is the external data-plane proxy master's config
// (node-ctl proxy serve --config <this>). The master owns the routesync
// subscription, shared route table, listener fds, and worker supervision. Serve
// still pushes the authoritative auth/park policy over the registration stream;
// local values are bootstrap fallbacks until that handshake completes.
type ProxyFileConfig struct {
	ConfigSocket  string           `yaml:"config_socket"`  // serve control socket to register + sync on (= serve paths.config_socket)
	Paths         ProxyPathsConfig `yaml:"paths"`          // node-local paths used directly by proxy workers
	DataListen    string           `yaml:"data_listen"`    // data-plane ingress; "" = UDS-only proxyForwarder
	ProxyNetNS    string           `yaml:"proxy_netns"`    // optional forwarding netns for floatingip TCP dials and conductor-pushed MMDS listen
	ProxySocket   string           `yaml:"proxy_socket"`   // UDS registered for conductor proxyForwarder; default <dir(config_socket)>/proxy.sock
	StatsSocket   string           `yaml:"stats_socket"`   // master-only traffic stats UDS; default <dir(config_socket)>/proxy-stats.sock
	ShmPath       string           `yaml:"shm_path"`       // shared route table path; default <dir(config_socket)>/proxy-routes.shm
	RouteCapacity int              `yaml:"route_capacity"` // fixed shared route slots; default 65536
	Workers       int              `yaml:"workers"`        // worker processes supervised by this master; default 1
	TLS           TLSConfig        `yaml:"tls"`            // data-plane listener cert (= serve's wildcard); "" = h2c
	Auth          string           `yaml:"auth"`           // bootstrap fallback until serve pushes policy: off|log|enforce (default enforce)
	ParkTimeout   string           `yaml:"park_timeout"`   // bootstrap fallback; default 30s
	MetricsListen string           `yaml:"metrics_listen"` // master metrics endpoint; aggregates worker data-plane counters
}

// ProxyPathsConfig contains only paths consumed by the external proxy. It is
// deliberately separate from the conductor's broader PathsConfig.
type ProxyPathsConfig struct {
	RunRoot string `yaml:"run_root"` // sandbox runtime root containing <sid>/ctl.sock; required
}

// LoadProxy reads the proxy master config, applies defaults, and validates.
func LoadProxy(path string) (*ProxyFileConfig, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("proxy config: read %s: %w", path, err)
	}
	var p ProxyFileConfig
	if err := decodeKnownYAML(b, &p); err != nil {
		return nil, fmt.Errorf("proxy config: parse %s: %w", path, err)
	}
	p.applyDefaults()
	return &p, p.validate()
}

func (p *ProxyFileConfig) applyDefaults() {
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

func (p *ProxyFileConfig) validate() error {
	if p.Paths.RunRoot == "" {
		return fmt.Errorf("proxy config: paths.run_root is required")
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
func (p *ProxyFileConfig) ParkTimeoutDur() time.Duration {
	d, err := time.ParseDuration(p.ParkTimeout)
	if err != nil || d <= 0 {
		return 30 * time.Second
	}
	return d
}

func decodeKnownYAML(b []byte, out any) error {
	dec := yaml.NewDecoder(bytes.NewReader(b))
	dec.KnownFields(true)
	return dec.Decode(out)
}
