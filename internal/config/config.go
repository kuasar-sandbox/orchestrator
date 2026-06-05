// Package config loads orchestrator-ctl's node-local configuration.
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

type Config struct {
	// North API / proxy.
	Domain  string `yaml:"domain"`   // e.g. sandboxes.example.com
	Listen  string `yaml:"listen"`   // e.g. ":443" (https); dev: ":3000" http
	TLSCert string `yaml:"tls_cert"` // wildcard cert for *.<domain> + api.<domain>
	TLSKey  string `yaml:"tls_key"`
	// Data-plane proxy. proxy_mode ∈ {internal,external,off}. In external mode the
	// orchestrator dials each proxy_sockets UDS and pushes the route table; the
	// proxy worker processes (orchestrator-ctl proxy) own the data-plane listener
	// (SO_REUSEPORT). data_listen is the dedicated data-plane address; "" makes the
	// data plane share `listen` (Host-split). data_plane_auth controls X-Access-Token
	// validation in the proxy. metrics_listen optionally serves a metrics endpoint.
	ProxyMode     string   `yaml:"proxy_mode"`      // internal (default) | external | off
	ProxySockets  []string `yaml:"proxy_sockets"`   // external: routesync UDS paths the orchestrator dials
	DataListen    string   `yaml:"data_listen"`     // dedicated data-plane listener; "" = share `listen`
	ParkTimeout   string   `yaml:"park_timeout"`    // hold a data-plane request awaiting route/resume; default 30s
	DataPlaneAuth string   `yaml:"data_plane_auth"` // off | log | enforce (default enforce)
	MetricsListen string   `yaml:"metrics_listen"`  // optional Prometheus text endpoint; "" = off
	// Secrets. Per-tenant manifest keys (the root secrets) are stored encrypted
	// at rest under encryption_key: ":"-separated 64-hex AES-256 keys, the first
	// active (new writes), the rest standby (decrypt during rotation). The
	// ORCHESTRATOR_ENCRYPTION_KEY env overrides this. api_key auth derives from
	// the manifest keys (internal/apikey) — there is no api_keys allowlist here;
	// which manifest keys may create/build is the manifest_keys table.
	EncryptionKey string `yaml:"encryption_key"`
	// Paths.
	RunRoot     string `yaml:"run_root"`          // default /run/sandbox
	BaseRoot    string `yaml:"base_root"`         // default /var/lib/sandbox
	ManifestCfg string `yaml:"manifest_config"`   // /opt/sandbox/manifest.yaml (shared, key empty)
	RuntimeE2B  string `yaml:"runtime_e2b_erofs"` // /opt/sandbox/runtime/.../sandbox-runtime-e2b.erofs
	RuntimeBase string `yaml:"runtime_erofs"`     // base runtime for bare
	Kernel      string `yaml:"kernel"`            // vmlinux path
	// Pre-formatted empty ext4 seeding the writable overlay upper on cold boot
	// (boot.root.overlay.diff_template). Deployment-provided; e.g.
	// /opt/sandbox/overlay-templates/basic-1G.ext4. Required for img templates.
	OverlayDiffTemplate string `yaml:"overlay_diff_template"`
	ConfigSocket        string `yaml:"config_socket"` // default /run/orchestrator-ctl.socket
	DBPath              string `yaml:"db_path"`       // default <base_root>/orchestrator.db
	// Networking (vswitch).
	Switch    string `yaml:"switch"`     // vswitch name, e.g. "sw0"
	InnerCIDR string `yaml:"inner_cidr"` // guest inner-IP pool, e.g. "10.42.0.0/16"
	// Sandbox spec (e2b templates have no size; use node defaults).
	DefaultVCPU   int    `yaml:"default_vcpu"`   // default 2
	DefaultMemory string `yaml:"default_memory"` // default "2GiB"
	// Resource control: sentinel UDS put into SANDBOX_CONFIG resources.control.controller
	// (sandbox-ctl dials it internally; orchestrator does not). "" = static cgroup mode.
	ResourceSocket string `yaml:"resource_socket"` // default /run/sandbox-resource.sock
	// Behaviour.
	DefaultTimeoutSec int `yaml:"default_timeout_sec"` // default 300
	// Builder resource pool. Concurrency is admitted in orchestrator-ctl; the
	// CPU/memory ceiling is applied to sandbox-builder.slice (cgroup) at install.
	BuilderMaxConcurrent int    `yaml:"builder_max_concurrent"` // default 2
	BuilderCPUQuota      string `yaml:"builder_cpu_quota"`      // e.g. "200%"; "" = unset
	BuilderMemoryMax     string `yaml:"builder_memory_max"`     // e.g. "8G"; "" = unset
	// Builder registry pull settings, applied to each build's flatten config.
	BuilderInsecure bool   `yaml:"builder_insecure_registry"` // pull base images over plain HTTP (dev/local registry)
	BuilderPlatform string `yaml:"builder_platform"`          // e.g. "linux/amd64"; "" = host default
	// Image the e2b CLI pushes its client-built rootfs to, as a template with
	// {templateID}/{buildID} tokens (must match the CLI's E2B_IMAGE_URI_MASK).
	// When a build trigger omits fromImage, it is derived from this. e.g.
	// "docker.<domain>/e2b/custom-envs/{templateID}:{buildID}".
	BuilderImageURIMask string `yaml:"builder_image_uri_mask"`
	// External binaries (overridable for testing).
	SandboxCtl      string `yaml:"sandbox_ctl_bin"` // default sandbox-ctl (PATH)
	VswitchCtl      string `yaml:"vswitch_ctl_bin"`
	FlattenCtl      string `yaml:"flatten_ctl_bin"`
	ManifestCl      string `yaml:"manifest_ctl_bin"`
	MkfsErofs       string `yaml:"mkfs_erofs_bin"`
	FsckErofs       string `yaml:"fsck_erofs_bin"`
	OrchestratorCtl string `yaml:"orchestrator_ctl_bin"` // builder unit ExecStart shim; default = running binary
	// Systemd unit management. orchestrator-ctl generates + installs these template
	// units (+ their slices) at startup and daemon-reloads; set install_units=false
	// to manage them out of band. Names/dir/ExecDir are configurable; ExecDir (where
	// sandbox-ctl/orchestrator-ctl live) defaults to the orchestrator binary's dir.
	UnitDir      string `yaml:"unit_dir"`      // default /etc/systemd/system
	RunnerUnit   string `yaml:"runner_unit"`   // default sandbox-runner@.service
	BuilderUnit  string `yaml:"builder_unit"`  // default sandbox-builder@.service
	ExecDir      string `yaml:"exec_dir"`      // dir holding sandbox-ctl/orchestrator-ctl; default = binary dir
	InstallUnits *bool  `yaml:"install_units"` // default true
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
	def(&c.RunRoot, "/run/sandbox")
	def(&c.BaseRoot, "/var/lib/sandbox")
	def(&c.ManifestCfg, "/opt/sandbox/manifest.yaml")
	def(&c.ConfigSocket, "/run/sandbox/orchestrator.socket")
	def(&c.Listen, ":443")
	def(&c.ProxyMode, ProxyInternal)
	def(&c.DataPlaneAuth, AuthEnforce)
	def(&c.ParkTimeout, "30s")
	def(&c.SandboxCtl, "sandbox-ctl")
	def(&c.VswitchCtl, "vswitch-ctl")
	def(&c.FlattenCtl, "flatten-ctl")
	def(&c.ManifestCl, "manifest-ctl")
	def(&c.MkfsErofs, "mkfs.erofs")
	def(&c.FsckErofs, "fsck.erofs")
	if c.DBPath == "" {
		c.DBPath = filepath.Join(c.BaseRoot, "orchestrator.db")
	}
	if c.DefaultTimeoutSec == 0 {
		c.DefaultTimeoutSec = 300
	}
	def(&c.Switch, "sw0")
	def(&c.InnerCIDR, "10.42.0.0/16")
	// ResourceSocket is intentionally NOT defaulted: empty = static cgroup mode
	// (the sandbox adopts its systemd unit's own cgroup via --cgroup-adopt). The
	// sandbox-sentinel resource controller is opt-in; set resource_socket to enable.
	def(&c.DefaultMemory, "2GiB")
	if c.DefaultVCPU == 0 {
		c.DefaultVCPU = 2
	}
	def(&c.UnitDir, "/etc/systemd/system")
	def(&c.RunnerUnit, "sandbox-runner@.service")
	def(&c.BuilderUnit, "sandbox-builder@.service")
	if exe, err := os.Executable(); err == nil {
		def(&c.ExecDir, filepath.Dir(exe))
		def(&c.OrchestratorCtl, exe)
	}
	def(&c.OrchestratorCtl, "orchestrator-ctl")
	if c.InstallUnits == nil {
		t := true
		c.InstallUnits = &t
	}
	if c.BuilderMaxConcurrent <= 0 {
		c.BuilderMaxConcurrent = 2
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

// Bin resolves an external binary name to an absolute path: absolute names pass
// through, bare names resolve against ExecDir (the orchestrator binary's dir).
func (c *Config) Bin(name string) string {
	if filepath.IsAbs(name) {
		return name
	}
	if c.ExecDir != "" {
		return filepath.Join(c.ExecDir, filepath.Base(name))
	}
	return name
}

// ParkTimeoutDur parses park_timeout (default 30s on any parse error).
func (c *Config) ParkTimeoutDur() time.Duration {
	d, err := time.ParseDuration(c.ParkTimeout)
	if err != nil || d <= 0 {
		return 30 * time.Second
	}
	return d
}

func (c *Config) validate() error {
	if c.Domain == "" {
		return fmt.Errorf("config: domain is required")
	}
	if c.EncryptionKeySpec() == "" {
		return fmt.Errorf("config: encryption_key (or ORCHESTRATOR_ENCRYPTION_KEY env) is required")
	}
	return c.ValidateProxy()
}

// ValidateProxy checks the proxy-mode fields. Exported so serve can re-check after
// applying --proxy / --proxy-socket flag overrides.
func (c *Config) ValidateProxy() error {
	switch c.ProxyMode {
	case ProxyInternal, ProxyExternal, ProxyOff:
	default:
		return fmt.Errorf("config: proxy_mode %q (want internal|external|off)", c.ProxyMode)
	}
	switch c.DataPlaneAuth {
	case AuthOff, AuthLog, AuthEnforce:
	default:
		return fmt.Errorf("config: data_plane_auth %q (want off|log|enforce)", c.DataPlaneAuth)
	}
	if c.ProxyMode == ProxyExternal && len(c.ProxySockets) == 0 {
		return fmt.Errorf("config: proxy_mode=external requires proxy_sockets (or --proxy-socket)")
	}
	return nil
}
