// Package config loads node-ctl's node-local configuration.
//
// The YAML is grouped by concern: api / proxy / paths / units / sandbox (the
// sandbox-instance defaults, sub-grouped resources/network/boot) / builder /
// checkpoint (paused-state tiering), plus a few singular top-level references
// (encryption_key, manifest_config). External binaries are NOT configured —
// they are auto-discovered next to the orchestrator binary, then on PATH.
package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Proxy modes select how the node serves sandbox data-plane traffic
// (<port>-<sid>.<domain>). See docs/proxy.md §5 (deployment modes);
// docs/orchestrator.md §9 covers how serve assembles the chosen mode.
const (
	ProxyInternal = "internal" // in-process proxy (default)
	ProxyExternal = "external" // offloaded to node-ctl proxy worker processes
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
	CheckpointRemote = "remote" // sandbox-ctl snapshot --upload → manifest (portable; = a template)
)

// Auto-discovered external binary names (resolved via Config.Bin against the
// orchestrator binary's dir, then PATH). They are intentionally not config keys.
const (
	BinSandboxCtl = "sandbox-ctl"
	BinVswitchCtl = "vswitch-ctl"
	BinFlattenCtl = "flatten-ctl"
	BinNodeCtl    = "node-ctl"
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
	// (resource_listen, node-resource.md); disabled => sandboxes use static cgroup.
	ResourceListen ResourceListenConfig `yaml:"resource_listen"`
	// Checkpoint is the paused-state tiering policy (parallel to sandbox).
	Checkpoint CheckpointConfig `yaml:"checkpoint"`
	// MMDS is the optional envd metadata service (re-keys envd to fresh per-identity
	// tokens). Disabled => envd runs non-secure and the proxy is the sole data-plane gate.
	MMDS MMDSConfig `yaml:"mmds"`
	// Cluster connects this node to a cluster-ctl registry over node-link
	// (node.md §10); empty = standalone single-node.
	Cluster ClusterConfig `yaml:"cluster"`
	// Singular top-level references.
	EncryptionKey  string `yaml:"encryption_key"`  // manifest_key at-rest AES-256 (":"-sep, first active); or NODE_CTL_ENCRYPTION_KEY env
	ManifestConfig string `yaml:"manifest_config"` // remote manifest store config (path ref; shared by sandbox + builder)

	execDir string // auto: dir of os.Executable(); used by Bin (not a YAML field)
}

// ResourceListenConfig optionally hosts the node resource controller in-process
// inside serve (the resource_listen sub-server, node-resource.md). Disabled by
// default; sandboxes then use static cgroup.
type ResourceListenConfig struct {
	Enabled bool   `yaml:"enabled"`
	Socket  string `yaml:"socket"` // controller UDS; "" = nodectl default
	Config  string `yaml:"config"` // resource controller yaml; "" = built-in defaults
}

// ClusterConfig connects this node to a cluster-ctl registry over node-link
// (node.md §10). Empty Registry = standalone single-node (no cluster).
type ClusterConfig struct {
	Registry          string            `yaml:"registry"`           // registry node-link addr host:port; "" = standalone
	NodeID            string            `yaml:"node_id"`            // this node's id; "" = hostname
	Labels            map[string]string `yaml:"labels"`             // zone / pool / slot / node (nodeSelectors)
	DataEndpoint      string            `yaml:"data_endpoint"`      // host:port the router forwards the data plane to
	HeartbeatInterval string            `yaml:"heartbeat_interval"` // node-link heartbeat period; "" = 10s
	TLSCert           string            `yaml:"tls_cert"`           // node-link client cert (mTLS); "" = plain h2c
	TLSKey            string            `yaml:"tls_key"`
	TLSCA             string            `yaml:"tls_ca"` // CA that verifies the registry's server cert
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
// mode the orchestrator dials each sockets UDS and pushes the route table; the
// proxy workers own the data-plane listener. data_listen "" shares api.listen.
type ProxyConfig struct {
	Mode          string `yaml:"mode"`           // internal (default) | external | off
	DataListen    string `yaml:"data_listen"`    // dedicated data-plane listener; "" = share api.listen
	ParkTimeout   string `yaml:"park_timeout"`   // hold a data-plane request awaiting route/resume; default 30s
	Auth          string `yaml:"auth"`           // off | log | enforce (default): validate X-Access-Token
	MetricsListen string `yaml:"metrics_listen"` // optional Prometheus text endpoint; "" = off
}

// MMDSConfig is the optional Firecracker-MMDS-v2 metadata service the orchestrator
// serves so envd (run in FC mode, i.e. without -isnotfc) can re-key its access token
// to a fresh per-identity value at /init — required for snapshot-fork data-plane auth.
// enabled=false keeps envd in -isnotfc (non-secure); the proxy then enforces
// X-Access-Token as the sole gate. The MMDS is hosted by the proxy component
// (internal: serve binds Listen; external: the proxy worker via --mmds-listen). envd
// hard-codes 169.254.169.254:80, so the vswitch's --mgmt-service translates that VIP to
// Listen in its datapath (no iptables); a loopback Listen needs route_localnet=1 on the
// mgmt dev.
type MMDSConfig struct {
	Enabled bool   `yaml:"enabled"` // false (default) => -isnotfc + proxy-only auth
	Listen  string `yaml:"listen"`  // MMDS listener (the vswitch mgmt-service target); default 127.0.0.1:19254
}

// PathsConfig holds node-local directories and sockets.
type PathsConfig struct {
	RunRoot      string `yaml:"run_root"`      // default /run/sandbox (tmpfs)
	BaseRoot     string `yaml:"base_root"`     // default /var/lib/sandbox (persistent)
	DBPath       string `yaml:"db_path"`       // default <base_root>/node-ctl.db
	ConfigSocket  string `yaml:"config_socket"`  // default /run/sandbox/node-ctl.socket
	AdminPidfile  string `yaml:"admin_pidfile"`  // optional PID allowlist (multi-line) gating the socket admin plane; "" => socket perms (same-uid/root) only
	PluginPidfile string `yaml:"plugin_pidfile"` // optional PID allowlist (multi-line) gating the socket plugin plane (proxy/agent registration); "" => socket perms only
}

// UnitsConfig manages the systemd template units (generated + installed at startup).
type UnitsConfig struct {
	Dir     string `yaml:"dir"`     // default /etc/systemd/system
	Runner  string `yaml:"runner"`  // default sandbox-runner@.service
	Builder string `yaml:"builder"` // default sandbox-builder@.service
	Install *bool  `yaml:"install"` // default true; false = manage out of band
}

// SandboxConfig is the sandbox-instance defaults, sub-grouped for clarity.
type SandboxConfig struct {
	TimeoutSec int             `yaml:"timeout_sec"` // default TTL; default 300
	Resources  ResourcesConfig `yaml:"resources"`   // capacity + resource control
	Network    NetworkConfig   `yaml:"network"`     // vswitch + inner IP
	Boot       BootConfig      `yaml:"boot"`        // boot artifacts (kernel / guest runtime / overlay)
}

// ResourcesConfig is per-sandbox capacity + resource-control wiring.
type ResourcesConfig struct {
	VCPU          int    `yaml:"vcpu"`           // default 2
	Memory        string `yaml:"memory"`         // default "2GiB"
	ControlSocket string `yaml:"control_socket"` // sentinel resource-controller UDS; "" = static cgroup mode
}

// MemoryMiB parses Memory ("2GiB", "512MiB", "2G", "512M", or a plain byte count)
// into whole MiB, for surfacing the VM's memory in e2b list/get responses. Returns
// 0 if unset or unparseable (the value is informational, not an allocation knob).
func (r ResourcesConfig) MemoryMiB() int { return parseMiB(r.Memory) }

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
	Switch   string     `yaml:"switch"`   // vswitch name, e.g. "sw0"
	Hostname string     `yaml:"hostname"` // guest hostname (sethostname + /etc/hosts entry); default "sandbox"
	DNS      []string   `yaml:"dns"`      // /etc/resolv.conf nameservers injected into the guest
	E2B      ProfileNet `yaml:"e2b"`      // e2b profile inner IP / gateway (envd port-forward needs the /30)
	Bare     ProfileNet `yaml:"bare"`     // bare profile inner IP / gateway
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
	RuntimeE2B          string `yaml:"runtime_e2b"`           // e2b profile guest runtime erofs
	RuntimeBase         string `yaml:"runtime_base"`          // bare profile guest runtime erofs
	OverlayDiffTemplate string `yaml:"overlay_diff_template"` // pre-formatted ext4 seeding the cold-boot overlay upper
}

// BuilderConfig is the build-instance settings. Concurrency is admitted in
// node-ctl; the CPU/memory ceiling is applied to sandbox-builder.slice.
// Builds run INSIDE build sandboxes (tenant network + isolation): import and
// step execution happen in microVMs booted from runtime_builder; only artifact
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

	// RuntimeBuilder is the build-sandbox guest runtime erofs
	// (sandbox-runtime-builder.erofs: e2b flavor + flatten-ctl + mkfs.erofs).
	RuntimeBuilder string `yaml:"runtime_builder"`
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

// FilesStorageConfig configures the COPY build-context object store. The
// orchestrator only ever PRESIGNS (PUT for the client, GET for the build) and
// HEADs (presence) — bytes never transit the control plane.
type FilesStorageConfig struct {
	Endpoint  string `yaml:"endpoint"`   // S3 endpoint; empty = AWS default
	Region    string `yaml:"region"`     // e.g. "cn-north-4" / "us-east-1"
	Bucket    string `yaml:"bucket"`      // required
	Prefix    string `yaml:"prefix"`      // optional key prefix
	AccessKey string `yaml:"access_key"`  // empty → AWS default chain (env / instance role)
	SecretKey string `yaml:"secret_key"`  // paired with access_key
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

// CheckpointConfig is the paused-state tiering policy. A sandbox pause writes its
// snapshot either to node-local files (mode=local, default; restorable only on
// this node — matches the sandbox-bound lifecycle) or straight to the remote
// manifest store (mode=remote; portable = a template). A local checkpoint is
// promoted to remote on demand via `node-ctl export-sandbox`.
type CheckpointConfig struct {
	Mode     string `yaml:"mode"`      // local (default) | remote
	LocalDir string `yaml:"local_dir"` // local checkpoint files dir (mode=local); default /var/lib/sandbox-saved
}

// Load reads the config file and applies defaults.
func Load(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: read %s: %w", path, err)
	}
	var c Config
	if err := yaml.Unmarshal(b, &c); err != nil {
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
	if c.Units.Install == nil {
		t := true
		c.Units.Install = &t
	}
	if c.Sandbox.TimeoutSec == 0 {
		c.Sandbox.TimeoutSec = 300
	}
	if c.Sandbox.Resources.VCPU == 0 {
		c.Sandbox.Resources.VCPU = 2
	}
	def(&c.Sandbox.Resources.Memory, "2GiB")
	// Sandbox.Resources.ControlSocket is intentionally NOT defaulted: empty =
	// static cgroup mode (the sandbox adopts its systemd unit's own cgroup via
	// --cgroup-adopt). The node-ctl resource controller is opt-in.
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
	def(&c.Checkpoint.Mode, CheckpointLocal)
	def(&c.Checkpoint.LocalDir, "/var/lib/sandbox-saved")
	def(&c.MMDS.Listen, "127.0.0.1:19254")
	if exe, err := os.Executable(); err == nil {
		c.execDir = filepath.Dir(exe)
	}
}

// EncryptionKeySpec returns the effective encryption-key set: the
// NODE_CTL_ENCRYPTION_KEY env when set, else the config's encryption_key
// (":"-separated 64-hex keys, first = active).
func (c *Config) EncryptionKeySpec() string {
	if v := os.Getenv("NODE_CTL_ENCRYPTION_KEY"); v != "" {
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
func (c *Config) VswitchCtl() string      { return c.Bin(BinVswitchCtl) }
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
		return fmt.Errorf("config: encryption_key (or NODE_CTL_ENCRYPTION_KEY env) is required")
	}
	switch c.Checkpoint.Mode {
	case CheckpointLocal, CheckpointRemote:
	default:
		return fmt.Errorf("config: checkpoint.mode %q (want local|remote)", c.Checkpoint.Mode)
	}
	return c.ValidateProxy()
}

// ValidateProxy checks the proxy-mode fields. Exported so serve can re-check after
// applying --proxy / --proxy-socket flag overrides.
func (c *Config) ValidateProxy() error {
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
	// proxy.mode=external needs no static socket list: proxy workers register
	// themselves on the config-socket plugin plane (the gateway forwards to the
	// live registered set), so there is nothing to require here.

	// MMDS off => envd is non-secure, so the proxy must be the enforcing sole gate.
	if !c.MMDS.Enabled && c.Proxy.Auth != AuthEnforce {
		return fmt.Errorf("config: mmds.enabled=false requires proxy.auth=enforce (envd runs non-secure; the proxy is the only data-plane gate)")
	}
	if c.MMDS.Enabled && c.Proxy.Mode == ProxyOff {
		return fmt.Errorf("config: mmds.enabled=true requires proxy.mode!=off (the MMDS service is hosted by the proxy)")
	}
	if f := c.Builder.FilesStorage; f != nil && f.Bucket == "" {
		return fmt.Errorf("config: builder.files_storage.bucket is required when files_storage is set")
	}
	return nil
}
