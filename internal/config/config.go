// Package config loads orchestrator-ctl's node-local configuration.
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
	"time"

	"gopkg.in/yaml.v3"
)

// Proxy modes select how the node serves sandbox data-plane traffic
// (<port>-<sid>.<domain>). See docs/orchestrator.md §"proxy 部署模式".
const (
	ProxyInternal = "internal" // in-process proxy (default)
	ProxyExternal = "external" // offloaded to orchestrator-ctl proxy worker processes
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
	BinSandboxCtl      = "sandbox-ctl"
	BinVswitchCtl      = "vswitch-ctl"
	BinFlattenCtl      = "flatten-ctl"
	BinOrchestratorCtl = "orchestrator-ctl"
)

// Config is the grouped node-local configuration.
type Config struct {
	API     APIConfig     `yaml:"api"`     // north control plane + TLS
	Proxy   ProxyConfig   `yaml:"proxy"`   // data-plane proxy
	Paths   PathsConfig   `yaml:"paths"`   // node-local dirs / sockets
	Units   UnitsConfig   `yaml:"units"`   // systemd unit management
	Sandbox SandboxConfig `yaml:"sandbox"` // sandbox-instance defaults
	Builder BuilderConfig `yaml:"builder"` // build-instance settings
	// Checkpoint is the paused-state tiering policy (parallel to sandbox).
	Checkpoint CheckpointConfig `yaml:"checkpoint"`
	// Singular top-level references.
	EncryptionKey  string `yaml:"encryption_key"`  // manifest_key at-rest AES-256 (":"-sep, first active); or ORCHESTRATOR_ENCRYPTION_KEY env
	ManifestConfig string `yaml:"manifest_config"` // remote manifest store config (path ref; shared by sandbox + builder)

	execDir string // auto: dir of os.Executable(); used by Bin (not a YAML field)
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
	Mode          string   `yaml:"mode"`           // internal (default) | external | off
	Sockets       []string `yaml:"sockets"`        // external: routesync UDS paths the orchestrator dials
	DataListen    string   `yaml:"data_listen"`    // dedicated data-plane listener; "" = share api.listen
	ParkTimeout   string   `yaml:"park_timeout"`   // hold a data-plane request awaiting route/resume; default 30s
	Auth          string   `yaml:"auth"`           // off | log | enforce (default): validate X-Access-Token
	MetricsListen string   `yaml:"metrics_listen"` // optional Prometheus text endpoint; "" = off
}

// PathsConfig holds node-local directories and sockets.
type PathsConfig struct {
	RunRoot      string `yaml:"run_root"`      // default /run/sandbox (tmpfs)
	BaseRoot     string `yaml:"base_root"`     // default /var/lib/sandbox (persistent)
	DBPath       string `yaml:"db_path"`       // default <base_root>/orchestrator.db
	ConfigSocket string `yaml:"config_socket"` // default /run/sandbox/orchestrator.socket
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
// orchestrator-ctl; the CPU/memory ceiling is applied to sandbox-builder.slice.
type BuilderConfig struct {
	MaxConcurrent    int    `yaml:"max_concurrent"`    // default 2
	CPUQuota         string `yaml:"cpu_quota"`         // e.g. "200%"; "" = unset
	MemoryMax        string `yaml:"memory_max"`        // e.g. "8G"; "" = unset
	InsecureRegistry bool   `yaml:"insecure_registry"` // pull base images over plain HTTP (dev/local registry)
	Platform         string `yaml:"platform"`          // e.g. "linux/amd64"; "" = host default
	// ImageURIMask is the image the e2b CLI pushes its client-built rootfs to,
	// with {templateID}/{buildID} tokens (must match the CLI's E2B_IMAGE_URI_MASK);
	// when a build trigger omits fromImage, it is derived from this.
	ImageURIMask string `yaml:"image_uri_mask"`
}

// CheckpointConfig is the paused-state tiering policy. A sandbox pause writes its
// snapshot either to node-local files (mode=local, default; restorable only on
// this node — matches the sandbox-bound lifecycle) or straight to the remote
// manifest store (mode=remote; portable = a template). A local checkpoint is
// promoted to remote on demand via `orchestrator-ctl export-sandbox`.
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
	def(&c.Paths.ConfigSocket, "/run/sandbox/orchestrator.socket")
	if c.Paths.DBPath == "" {
		c.Paths.DBPath = filepath.Join(c.Paths.BaseRoot, "orchestrator.db")
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
	// --cgroup-adopt). The sandbox-sentinel resource controller is opt-in.
	def(&c.Sandbox.Network.Switch, "sw0")
	def(&c.Sandbox.Network.Hostname, "sandbox")
	if len(c.Sandbox.Network.DNS) == 0 {
		c.Sandbox.Network.DNS = []string{"169.254.169.253"}
	}
	def(&c.Sandbox.Network.E2B.InnerIP, "169.254.0.21/30")
	def(&c.Sandbox.Network.E2B.Nexthop, "169.254.0.22")
	def(&c.Sandbox.Network.Bare.InnerIP, "169.254.1.1/31")
	def(&c.Sandbox.Network.Bare.Nexthop, "169.254.1.0")
	if c.Builder.MaxConcurrent <= 0 {
		c.Builder.MaxConcurrent = 2
	}
	def(&c.Checkpoint.Mode, CheckpointLocal)
	def(&c.Checkpoint.LocalDir, "/var/lib/sandbox-saved")
	if exe, err := os.Executable(); err == nil {
		c.execDir = filepath.Dir(exe)
	}
}

// EncryptionKeySpec returns the effective encryption-key set: the
// ORCHESTRATOR_ENCRYPTION_KEY env when set, else the config's encryption_key
// (":"-separated 64-hex keys, first = active).
func (c *Config) EncryptionKeySpec() string {
	if v := os.Getenv("ORCHESTRATOR_ENCRYPTION_KEY"); v != "" {
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
func (c *Config) VswitchCtl() string      { return c.Bin(BinVswitchCtl) }
func (c *Config) FlattenCtl() string      { return c.Bin(BinFlattenCtl) }
func (c *Config) OrchestratorCtl() string { return c.Bin(BinOrchestratorCtl) }

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
		return fmt.Errorf("config: encryption_key (or ORCHESTRATOR_ENCRYPTION_KEY env) is required")
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
	if c.Proxy.Mode == ProxyExternal && len(c.Proxy.Sockets) == 0 {
		return fmt.Errorf("config: proxy.mode=external requires proxy.sockets (or --proxy-socket)")
	}
	return nil
}
