// Package sandboxcfg renders the three mode-specific sandbox-ctl configuration
// contracts. Image launches receive a complete cold config, Sandbox E launches
// receive only ApplyFromRules host/instance fields, and Snapshot S restores
// receive only ApplyRestoreRules host fields. The orchestrator serves the YAML
// through the config socket; MANIFEST_KEY remains a separate task-local secret.
package sandboxcfg

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strings"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
	"gopkg.in/yaml.v3"
)

// Namespaced metadata keys carrying the per-instance config override. Each value is
// a JSON object; an absent key falls back to orchestrator/profile defaults.
const (
	NsResource    = "kuasar-sandbox.resource"
	NsNetwork     = "kuasar-sandbox.network"
	NsLaunch      = "kuasar-sandbox.launch"
	NsInit        = "kuasar-sandbox.init"
	NsMounts      = "kuasar-sandbox.mounts"
	NsFiles       = "kuasar-sandbox.files"
	NsMetadata    = "kuasar-sandbox.metadata"
	NsRestore     = "kuasar-sandbox.restore"
	NsCredentials = "kuasar-sandbox.credentials"
	NsCheckpoint  = "kuasar-sandbox.checkpoint"
	NsMMDS        = "kuasar-sandbox.mmds"
	NsTraffic     = "kuasar-sandbox.traffic"
)

// NetworkSpec is the orchestrator's LOGICAL network model — broader than the guest
// config.NetworkConfig. hostname/nexthop flow into the guest network; inner_ip +
// transit_* are consumed at vswitch.Attach; dns renders into /etc/resolv.conf. The
// resolved form is also injected into SANDBOX_CONFIG.metadata so it rides snapshots.
type NetworkSpec struct {
	Hostname         string   `json:"hostname,omitempty" yaml:"hostname,omitempty"`
	DNS              []string `json:"dns,omitempty" yaml:"dns,omitempty"`
	InnerIP          string   `json:"inner_ip,omitempty" yaml:"inner_ip,omitempty"` // CIDR
	Nexthop          string   `json:"nexthop,omitempty" yaml:"nexthop,omitempty"`
	TransitGatewayIP string   `json:"transit_gateway_ip,omitempty" yaml:"transit_gateway_ip,omitempty"`
	TransitGeneveVNI uint32   `json:"transit_geneve_vni,omitempty" yaml:"transit_geneve_vni,omitempty"`
	TransitMAC       string   `json:"transit_mac,omitempty" yaml:"transit_mac,omitempty"`
}

// RestoreSpec is the tenant-selectable restore prefetch request.
type RestoreSpec struct {
	Prefetch string `json:"prefetch,omitempty"`
}

// TapFD is the node-managed tapfd transport rendered into runtime config.
type TapFD struct {
	Exec    []string
	Socket  string
	Request string
	Timeout string
}

func (t TapFD) runtime() rtconfig.TapFDConfig {
	return rtconfig.TapFDConfig{
		Exec:    append([]string(nil), t.Exec...),
		Socket:  t.Socket,
		Request: t.Request,
		Timeout: t.Timeout,
	}
}

// SandboxSpec is the tenant-controllable config subset, parsed from the
// kuasar-sandbox.<ns> metadata keys. It excludes node-managed fields (boot,
// resources.control, network.tapfd) — those are the orchestrator's.
type SandboxSpec struct {
	Resource ResourcePatch
	Traffic  TrafficPatch
	Network  NetworkSpec
	Restore  RestoreSpec
	Launch   *rtconfig.LaunchConfig // bare profile only; e2b rejects (envd owns launch)
	// LaunchCgroupControl preserves whether the boolean was explicitly present.
	// LaunchConfig alone cannot distinguish an omitted value from false when a
	// source Sandbox contributes lower-priority launch defaults.
	LaunchCgroupControl *bool `json:"launch_cgroup_control,omitempty"`
	Init                []rtconfig.InitConfig
	Mounts              []rtconfig.MountConfig
	Files               []rtconfig.FileConfig
	Metadata            map[string]string // -> SANDBOX_CONFIG.metadata passthrough (e.g. e2b.start_cmd)
}

// ParseSpec decodes the kuasar-sandbox.<ns> metadata keys. Values are JSON; we parse
// them with the YAML decoder so the runtime config's yaml tags (snake_case, e.g.
// stop_signal) apply uniformly (JSON is a subset of YAML). Absent keys leave zero
// values. Network format is checked here (fail fast on tenant input); deeper checks
// are sandbox-ctl's.
func ParseSpec(meta map[string]string) (SandboxSpec, error) {
	var s SandboxSpec
	dec := func(ns string, into any) error {
		raw := strings.TrimSpace(meta[ns])
		if raw == "" {
			return nil
		}
		if err := yaml.Unmarshal([]byte(raw), into); err != nil {
			return fmt.Errorf("sandboxcfg: metadata[%q] is not valid JSON: %w", ns, err)
		}
		return nil
	}
	if raw, present := meta[NsResource]; present {
		var err error
		s.Resource, err = ParseResourcePatch(raw)
		if err != nil {
			return s, err
		}
	}
	if raw, present := meta[NsTraffic]; present {
		var err error
		s.Traffic, err = ParseTrafficPatch(raw)
		if err != nil {
			return s, err
		}
	}
	if err := dec(NsNetwork, &s.Network); err != nil {
		return s, err
	}
	if raw, ok := meta[NsRestore]; ok {
		var err error
		s.Restore, err = parseRestore(raw)
		if err != nil {
			return s, err
		}
	}
	if raw := strings.TrimSpace(meta[NsLaunch]); raw != "" {
		var l rtconfig.LaunchConfig
		if err := yaml.Unmarshal([]byte(raw), &l); err != nil {
			return s, fmt.Errorf("sandboxcfg: metadata[%q] is not valid JSON: %w", NsLaunch, err)
		}
		var presence struct {
			CgroupControl *bool `yaml:"cgroup_control"`
		}
		if err := yaml.Unmarshal([]byte(raw), &presence); err != nil {
			return s, fmt.Errorf("sandboxcfg: metadata[%q] is not valid JSON: %w", NsLaunch, err)
		}
		s.Launch = &l
		s.LaunchCgroupControl = presence.CgroupControl
	}
	if err := dec(NsInit, &s.Init); err != nil {
		return s, err
	}
	if err := dec(NsMounts, &s.Mounts); err != nil {
		return s, err
	}
	if err := dec(NsFiles, &s.Files); err != nil {
		return s, err
	}
	if err := dec(NsMetadata, &s.Metadata); err != nil {
		return s, err
	}
	if err := s.Network.validate(); err != nil {
		return s, err
	}
	return s, nil
}

// NormalizeRestoreMetadata validates and canonicalizes only the restore
// namespace. An absent namespace is left absent. When it is present, the value
// must be one JSON object containing only an optional prefetch field whose value
// is "off" or "memory". When the namespace is present, the returned map is a
// clone so callers never mutate a request, template, or stored record while
// validating it.
func NormalizeRestoreMetadata(meta map[string]string) (map[string]string, error) {
	raw, ok := meta[NsRestore]
	if !ok {
		return meta, nil
	}
	restore, err := parseRestore(raw)
	if err != nil {
		return nil, err
	}
	b, err := json.Marshal(restore)
	if err != nil {
		return nil, fmt.Errorf("sandboxcfg: metadata[%q]: %w", NsRestore, err)
	}
	out := make(map[string]string, len(meta))
	for k, v := range meta {
		out[k] = v
	}
	out[NsRestore] = string(b)
	return out, nil
}

func parseRestore(raw string) (RestoreSpec, error) {
	var restore RestoreSpec
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" || trimmed[0] != '{' {
		return restore, fmt.Errorf("sandboxcfg: metadata[%q] must be a JSON object", NsRestore)
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal([]byte(trimmed), &object); err != nil {
		return restore, fmt.Errorf("sandboxcfg: metadata[%q] is not a valid JSON object: %w", NsRestore, err)
	}
	for field := range object {
		if field != "prefetch" {
			return restore, fmt.Errorf("sandboxcfg: metadata[%q] contains unknown field %q", NsRestore, field)
		}
	}
	prefetch, ok := object["prefetch"]
	if !ok {
		return restore, nil
	}
	var value any
	if err := json.Unmarshal(prefetch, &value); err != nil {
		return restore, fmt.Errorf("sandboxcfg: metadata[%q].prefetch is invalid: %w", NsRestore, err)
	}
	mode, ok := value.(string)
	if !ok {
		return restore, fmt.Errorf("sandboxcfg: metadata[%q].prefetch must be a string", NsRestore)
	}
	if mode == "" {
		return restore, fmt.Errorf("sandboxcfg: metadata[%q].prefetch %q invalid (want off|memory)", NsRestore, mode)
	}
	if _, err := rtconfig.ParsePrefetchMode(mode); err != nil {
		return restore, fmt.Errorf("sandboxcfg: metadata[%q].prefetch %q invalid (want off|memory)", NsRestore, mode)
	}
	restore.Prefetch = mode
	return restore, nil
}

// MergeMetadata overlays over onto base per key (over wins). Resource and
// traffic are exceptions: their validated leaves are overlaid independently and
// persisted as canonical JSON. Both layers are parsed before overlay so an
// invalid lower-priority value cannot be hidden by a valid higher-priority one.
func MergeMetadata(base, over map[string]string) (map[string]string, error) {
	out := mergeStr(base, over)
	baseRaw, basePresent := base[NsResource]
	overRaw, overPresent := over[NsResource]
	if basePresent || overPresent {
		var basePatch, overPatch ResourcePatch
		var err error
		if basePresent {
			basePatch, err = ParseResourcePatch(baseRaw)
			if err != nil {
				return nil, err
			}
		}
		if overPresent {
			overPatch, err = ParseResourcePatch(overRaw)
			if err != nil {
				return nil, err
			}
		}
		merged, err := MergeResourcePatch(basePatch, overPatch)
		if err != nil {
			return nil, err
		}
		canonical, err := MarshalResourcePatch(merged)
		if err != nil {
			return nil, err
		}
		if out == nil {
			out = map[string]string{}
		}
		out[NsResource] = canonical
	}

	baseTrafficRaw, baseTrafficPresent := base[NsTraffic]
	overTrafficRaw, overTrafficPresent := over[NsTraffic]
	if baseTrafficPresent || overTrafficPresent {
		var basePatch, overPatch TrafficPatch
		var err error
		if baseTrafficPresent {
			basePatch, err = ParseTrafficPatch(baseTrafficRaw)
			if err != nil {
				return nil, err
			}
		}
		if overTrafficPresent {
			overPatch, err = ParseTrafficPatch(overTrafficRaw)
			if err != nil {
				return nil, err
			}
		}
		canonical, err := MarshalTrafficPatch(MergeTrafficPatch(basePatch, overPatch))
		if err != nil {
			return nil, err
		}
		if out == nil {
			out = map[string]string{}
		}
		out[NsTraffic] = canonical
	}
	return out, nil
}

// MergeCreateMetadata layers request configuration over template, group, or
// placement defaults while keeping restore policy, credentials, and checkpoint
// policy request-scoped. Only namespaces explicitly present in request are
// admitted for those values.
func MergeCreateMetadata(defaults, request map[string]string) (map[string]string, error) {
	out, err := MergeMetadata(defaults, request)
	if err != nil {
		return nil, err
	}
	for _, ns := range []string{NsRestore, NsCredentials, NsCheckpoint, NsMMDS} {
		delete(out, ns)
		if raw, ok := request[ns]; ok {
			if out == nil {
				out = map[string]string{}
			}
			out[ns] = raw
		}
	}
	return out, nil
}

// validate format-checks the tenant network fields (CIDR / IP / MAC).
func (n NetworkSpec) validate() error {
	if n.InnerIP != "" {
		if _, _, err := net.ParseCIDR(n.InnerIP); err != nil {
			return fmt.Errorf("sandboxcfg: network.inner_ip %q is not a CIDR: %w", n.InnerIP, err)
		}
	}
	if n.Nexthop != "" && net.ParseIP(n.Nexthop) == nil {
		return fmt.Errorf("sandboxcfg: network.nexthop %q is not an IP", n.Nexthop)
	}
	if n.TransitGatewayIP != "" && net.ParseIP(n.TransitGatewayIP) == nil {
		return fmt.Errorf("sandboxcfg: network.transit_gateway_ip %q is not an IP", n.TransitGatewayIP)
	}
	if n.TransitMAC != "" {
		if _, err := net.ParseMAC(n.TransitMAC); err != nil {
			return fmt.Errorf("sandboxcfg: network.transit_mac %q is not a MAC: %w", n.TransitMAC, err)
		}
	}
	return nil
}

// ValidateNetworkSpec validates one already-decoded logical network. Artifact
// preparation uses this after strict task-local decoding so no raw tenant
// metadata needs to cross into the conductor.
func ValidateNetworkSpec(n NetworkSpec) error {
	return n.validate()
}

// IsZero reports whether the tenant supplied no network fields.
func (n NetworkSpec) IsZero() bool {
	return n.Hostname == "" && len(n.DNS) == 0 && n.InnerIP == "" && n.Nexthop == "" &&
		n.TransitGatewayIP == "" && n.TransitGeneveVNI == 0 && n.TransitMAC == ""
}

// Params is everything needed to render one sandbox's config: the node-managed base
// inputs plus the resolved logical network and the parsed tenant Spec.
type Params struct {
	Sandbox        *types.Sandbox
	Template       types.TemplateID
	PreparedSource types.ResumeSource         // task-local selected E/S; empty for image launch
	ArtifactDisks  types.ArtifactDiskTopology // task-projected shape for host-owned active bindings
	Runtime        string                     // erofs path (file path, no scheme)
	Kernel         string                     // vmlinux path
	OverlayDiffTpl string                     // pre-formatted ext4 seeding the cold-boot overlay upper (file path)
	TapFD          TapFD
	EnvVars        map[string]string        // create-time launch env
	Resources      rtconfig.ResourcesConfig // fully resolved node + tenant + restore policy
	Network        NetworkSpec              // resolved logical network (request over artifact over node defaults)
	MMDSEnabled    bool
	Spec           SandboxSpec // parsed tenant override except resources, resolved above
}

// WriteYAML renders the SANDBOX_CONFIG and writes it to path (0600).
func (p Params) WriteYAML(path string) error {
	b, err := p.BuildYAML()
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o600)
}

// ImageColdConfig is the complete config accepted by an image launch. Image
// creation is the only path allowed to define an immutable disk graph.
type ImageColdConfig rtconfig.SandboxConfig

type hostActiveOverlayConfig struct {
	Diff         string `yaml:"diff,omitempty"`
	DiffTemplate string `yaml:"diff_template,omitempty"`
	DiffSize     string `yaml:"diff_size,omitempty"`
}

type hostActiveRootConfig struct {
	Diff         string                   `yaml:"diff,omitempty"`
	DiffTemplate string                   `yaml:"diff_template,omitempty"`
	DiffSize     string                   `yaml:"diff_size,omitempty"`
	Overlay      *hostActiveOverlayConfig `yaml:"overlay,omitempty"`
}

type hostActiveDiskConfig struct {
	Name         string                   `yaml:"name"`
	Diff         string                   `yaml:"diff,omitempty"`
	DiffTemplate string                   `yaml:"diff_template,omitempty"`
	DiffSize     string                   `yaml:"diff_size,omitempty"`
	Overlay      *hostActiveOverlayConfig `yaml:"overlay,omitempty"`
}

type hostBootConfig struct {
	Kernel  string                 `yaml:"kernel"`
	Runtime string                 `yaml:"runtime"`
	Root    hostActiveRootConfig   `yaml:"root"`
	Disks   []hostActiveDiskConfig `yaml:"disks,omitempty"`
}

// SandboxHostConfig is the presence-aware host/instance document for
// sandbox-ctl run --from. Its root/disk DTOs can express only host-owned active
// diff fields plus the artifact-projected name/order/topology; immutable graph
// fields are unrepresentable.
type SandboxHostConfig struct {
	Resources      rtconfig.ResourcesConfig `yaml:"resources"`
	Network        rtconfig.NetworkConfig   `yaml:"network"`
	Boot           hostBootConfig           `yaml:"boot"`
	Restore        rtconfig.RestoreConfig   `yaml:"restore,omitempty"`
	Timeouts       rtconfig.TimeoutsConfig  `yaml:"timeouts,omitempty"`
	Launch         *rtconfig.LaunchConfig   `yaml:"launch,omitempty"`
	Mounts         []rtconfig.MountConfig   `yaml:"mounts,omitempty"`
	Files          []rtconfig.FileConfig    `yaml:"files,omitempty"`
	EphemeralFiles []rtconfig.FileConfig    `yaml:"ephemeral_files,omitempty"`
	Init           []rtconfig.InitConfig    `yaml:"init,omitempty"`
	Metadata       map[string]string        `yaml:"metadata,omitempty"`
}

// SnapshotHostConfig is the host-only document for sandbox-ctl run --restore.
// Its type has no launch, file, init, plugin, mount, metadata, cmdline, or disk
// graph field, making those forbidden values unrepresentable here.
type SnapshotHostConfig struct {
	Resources rtconfig.ResourcesConfig `yaml:"resources"`
	Network   rtconfig.NetworkConfig   `yaml:"network"`
	Boot      hostBootConfig           `yaml:"boot"`
	Restore   rtconfig.RestoreConfig   `yaml:"restore,omitempty"`
	Timeouts  rtconfig.TimeoutsConfig  `yaml:"timeouts,omitempty"`
}

// BuildYAML dispatches to one of three mode-specific DTO renderers. LaunchMode
// is already resolved and durable before this method is called.
func (p Params) BuildYAML() ([]byte, error) {
	if p.Sandbox == nil {
		return nil, fmt.Errorf("sandboxcfg: missing sandbox")
	}
	var (
		config any
		err    error
	)
	switch p.Sandbox.LaunchMode {
	case types.LaunchImage:
		config, err = p.BuildImageColdConfig()
	case types.LaunchCold:
		config, err = p.BuildSandboxHostConfig()
	case types.LaunchMemory:
		config, err = p.BuildSnapshotHostConfig()
	default:
		return nil, fmt.Errorf("sandboxcfg: unsupported launch mode %q", p.Sandbox.LaunchMode)
	}
	if err != nil {
		return nil, err
	}
	return yaml.Marshal(config)
}

// BuildImageColdConfig renders the full image-launch contract.
func (p Params) BuildImageColdConfig() (*ImageColdConfig, error) {
	c, err := p.buildImageColdConfig()
	if err != nil {
		return nil, err
	}
	result := ImageColdConfig(*c)
	return &result, nil
}

func (p Params) buildImageColdConfig() (*rtconfig.SandboxConfig, error) {
	c := &rtconfig.SandboxConfig{}

	// Resources were resolved and cross-validated before any network attach or
	// runner assignment. The renderer installs that single authoritative value;
	// run-sandbox adds only the inherited cgroup capability at exec time.
	c.Resources = p.Resources

	// --- boot ---
	c.Boot.Kernel = "file://" + p.Kernel
	c.Boot.Runtime = "file://" + p.Runtime
	c.Boot.Root.Overlay = &rtconfig.OverlayConfig{}
	// boot.root.base is the read-only rootfs. Cold boot (img) = the flattened image
	// manifest; restore (snp/resume) lets snapshot.cfg fill it (omit).
	if p.Template.Kind == types.KindImg {
		c.Boot.Root.Base = p.Template.Ref
	}
	// Restore policy is host-only and meaningful only when this invocation has a
	// restore ref. Keep image cold boots free of restore configuration while
	// retaining the policy in Sandbox.Metadata for a later pause/resume.
	if p.RestoreRef() != "" {
		c.Restore.Prefetch = p.Spec.Restore.Prefetch
	}
	// Cold boot needs a pre-formatted ext4 source for the writable upper; restore
	// gets the overlay chain from the snapshot.
	if p.RestoreRef() == "" && p.OverlayDiffTpl != "" {
		c.Boot.Root.Overlay.DiffTemplate = "file://" + p.OverlayDiffTpl
	}

	// --- network (guest side) ---
	c.Network = p.buildRuntimeNetwork()

	// --- launch ---
	if err := p.buildLaunch(c); err != nil {
		return nil, err
	}

	// --- init / mounts (tenant pass-through) ---
	c.Init = p.Spec.Init
	c.Mounts = p.Spec.Mounts

	// --- files ---
	// Tenant files are persistent C0 input. Node-derived /etc/hosts and
	// /etc/resolv.conf are invocation-local cold-start input and must never become
	// part of the portable artifact graph.
	c.Files = append([]rtconfig.FileConfig(nil), p.Spec.Files...)
	c.EphemeralFiles = guestFiles(p.Network.Hostname, p.Network.DNS)

	// --- metadata: tenant passthrough + the resolved logical network ---
	c.Metadata = p.buildMetadata()
	return c, nil
}

// BuildSandboxHostConfig renders only fields admitted by ApplyFromRules.
func (p Params) BuildSandboxHostConfig() (*SandboxHostConfig, error) {
	if p.Sandbox == nil || p.Sandbox.LaunchMode != types.LaunchCold {
		return nil, fmt.Errorf("sandboxcfg: Sandbox host config requires cold launch mode")
	}
	boot, err := p.buildArtifactHostBoot()
	if err != nil {
		return nil, err
	}
	c := &SandboxHostConfig{
		Resources:      p.Resources,
		Network:        p.buildRuntimeNetwork(),
		Boot:           boot,
		Restore:        rtconfig.RestoreConfig{Prefetch: p.Spec.Restore.Prefetch},
		EphemeralFiles: guestFiles(p.Network.Hostname, p.Network.DNS),
	}
	// A new Sandbox-template Create may intentionally persist request launch,
	// env, files, init, mounts, and metadata into the next C1. A resume from a
	// durable paused source keeps E's C0 authoritative and emits none of them.
	freshTemplateCreate := p.Sandbox.ResumeSource.Empty() && p.Template.Kind == types.KindSbx
	if freshTemplateCreate {
		launchContainer := &rtconfig.SandboxConfig{}
		if err := p.buildLaunch(launchContainer); err != nil {
			return nil, err
		}
		c.Launch = &launchContainer.Launch
		c.Mounts = append([]rtconfig.MountConfig(nil), p.Spec.Mounts...)
		c.Files = append([]rtconfig.FileConfig(nil), p.Spec.Files...)
		c.Init = append([]rtconfig.InitConfig(nil), p.Spec.Init...)
		c.Metadata = p.buildMetadata()
	}
	return c, nil
}

// BuildSnapshotHostConfig renders only fields admitted by ApplyRestoreRules.
func (p Params) BuildSnapshotHostConfig() (*SnapshotHostConfig, error) {
	if p.Sandbox == nil || p.Sandbox.LaunchMode != types.LaunchMemory {
		return nil, fmt.Errorf("sandboxcfg: Snapshot host config requires memory launch mode")
	}
	boot, err := p.buildArtifactHostBoot()
	if err != nil {
		return nil, err
	}
	return &SnapshotHostConfig{
		Resources: p.Resources,
		Network:   p.buildRuntimeNetwork(),
		Boot:      boot,
		Restore:   rtconfig.RestoreConfig{Prefetch: p.Spec.Restore.Prefetch},
	}, nil
}

func (p Params) buildArtifactHostBoot() (hostBootConfig, error) {
	boot := hostBootConfig{Kernel: "file://" + p.Kernel, Runtime: "file://" + p.Runtime}
	topology := p.ArtifactDisks
	if topology.Root.Name != "" || !topology.Root.Mode.Valid() {
		return hostBootConfig{}, fmt.Errorf("sandboxcfg: invalid artifact root disk shape")
	}
	if len(topology.Disks) > rtconfig.MaxDataDisks {
		return hostBootConfig{}, fmt.Errorf("sandboxcfg: artifact has %d data disks, max %d", len(topology.Disks), rtconfig.MaxDataDisks)
	}
	root, err := p.hostActiveRoot(topology.Root)
	if err != nil {
		return hostBootConfig{}, fmt.Errorf("sandboxcfg: artifact root: %w", err)
	}
	boot.Root = root
	seen := make(map[string]struct{}, len(topology.Disks))
	boot.Disks = make([]hostActiveDiskConfig, len(topology.Disks))
	for i, shape := range topology.Disks {
		if shape.Name == "" || !shape.Mode.Valid() {
			return hostBootConfig{}, fmt.Errorf("sandboxcfg: invalid artifact data disk %d shape", i)
		}
		if _, duplicate := seen[shape.Name]; duplicate {
			return hostBootConfig{}, fmt.Errorf("sandboxcfg: duplicate artifact data disk name %q", shape.Name)
		}
		seen[shape.Name] = struct{}{}
		active, err := p.hostActiveRoot(shape)
		if err != nil {
			return hostBootConfig{}, fmt.Errorf("sandboxcfg: artifact data disk %q: %w", shape.Name, err)
		}
		boot.Disks[i] = hostActiveDiskConfig{
			Name: shape.Name, Diff: active.Diff, DiffTemplate: active.DiffTemplate,
			DiffSize: active.DiffSize, Overlay: active.Overlay,
		}
	}
	return boot, nil
}

func (p Params) hostActiveRoot(shape types.ArtifactDiskShape) (hostActiveRootConfig, error) {
	var template string
	if !shape.HasActiveBase {
		if p.OverlayDiffTpl == "" {
			return hostActiveRootConfig{}, fmt.Errorf("formatted diff template is required without an active artifact base")
		}
		template = "file://" + p.OverlayDiffTpl
	}
	switch shape.Mode {
	case types.ArtifactDiskSingle:
		return hostActiveRootConfig{DiffTemplate: template}, nil
	case types.ArtifactDiskOverlay:
		return hostActiveRootConfig{Overlay: &hostActiveOverlayConfig{DiffTemplate: template}}, nil
	default:
		return hostActiveRootConfig{}, fmt.Errorf("unsupported disk mode %q", shape.Mode)
	}
}

func (p Params) buildRuntimeNetwork() rtconfig.NetworkConfig {
	tapFD := p.TapFD.runtime()
	network := rtconfig.NetworkConfig{TapFD: &tapFD}
	if p.Sandbox == nil {
		return network
	}
	if p.Sandbox.PortMAC != "" {
		network.MAC = p.Sandbox.PortMAC
	}
	if p.Sandbox.InnerIP != "" {
		network.IP = p.Sandbox.InnerIP
		if p.Network.Nexthop != "" {
			network.Nexthop = p.Network.Nexthop
		}
	}
	if p.Network.Hostname != "" {
		network.Hostname = p.Network.Hostname
	}
	return network
}

// buildLaunch fills launch. e2b is envd-owned (a tenant launch override is rejected);
// bare uses the image entrypoint by default, overlaid by the tenant launch spec.
func (p Params) buildLaunch(c *rtconfig.SandboxConfig) error {
	if err := ValidateLaunchForProfile(p.Template.Profile, p.Spec.Launch); err != nil {
		return err
	}
	if p.Template.Profile == types.ProfileE2B {
		// FC mode (drop -isnotfc) when MMDS is enabled so envd polls the metadata
		// service for the access-token hash and accepts re-keying at /init.
		args := []string{"-isnotfc", "-port", "49983"}
		if p.MMDSEnabled {
			args = []string{"-port", "49983"}
		}
		// envd must run as root (it setuids into the image user per exec); shares
		// sandbox-init's PID namespace so PID 1 reaps the workload's orphans.
		c.Launch = rtconfig.LaunchConfig{
			Exec: "/opt/sandbox-runtime/bin/envd", Args: args, Env: p.EnvVars,
			Restart: "always", User: "0:0", PIDNamespace: "shared", CgroupControl: true,
		}
		return nil
	}
	// bare: empty exec -> sandbox-init runs the flattened image's entrypoint.
	c.Launch = rtconfig.LaunchConfig{Env: p.EnvVars, Restart: "always"}
	if s := p.Spec.Launch; s != nil {
		if s.Exec != "" {
			c.Launch.Exec = s.Exec
		}
		if len(s.Args) > 0 {
			c.Launch.Args = s.Args
		}
		if s.Workdir != "" {
			c.Launch.Workdir = s.Workdir
		}
		if s.Restart != "" {
			c.Launch.Restart = s.Restart
		}
		if s.User != "" {
			c.Launch.User = s.User
		}
		if s.StopSignal != "" {
			c.Launch.StopSignal = s.StopSignal
		}
		c.Launch.CgroupControl = s.CgroupControl
		if len(s.Plugin) > 0 {
			c.Launch.Plugin = s.Plugin
		}
		c.Launch.Env = mergeStr(p.EnvVars, s.Env) // tenant launch env overrides create env
	}
	return nil
}

// ValidateLaunchForProfile applies the same profile ownership rule at request
// normalization and at final rendering. e2b's launch is platform-owned by
// envd; bare is the only profile that accepts a tenant launch override.
func ValidateLaunchForProfile(profile types.Profile, launch *rtconfig.LaunchConfig) error {
	if !profile.Valid() {
		return fmt.Errorf("sandboxcfg: unknown profile %q", profile)
	}
	if profile == types.ProfileE2B && launch != nil {
		return fmt.Errorf("sandboxcfg: launch override is not allowed for the e2b profile (envd owns launch)")
	}
	return nil
}

// buildMetadata is the tenant passthrough (e.g. e2b.start_cmd) plus the resolved
// logical network injected under kuasar-sandbox.network so it rides the snapshot
// (self-describing for restore/migration). Returns nil when empty.
func (p Params) buildMetadata() map[string]string {
	m := map[string]string{}
	for k, v := range p.Spec.Metadata {
		m[k] = v
	}
	if !p.Network.IsZero() {
		if nb, err := json.Marshal(p.Network); err == nil {
			m[NsNetwork] = string(nb)
		}
	}
	if len(m) == 0 {
		return nil
	}
	return m
}

// mergeStr returns base overlaid by over (over wins); nil-safe.
func mergeStr(base, over map[string]string) map[string]string {
	if len(base) == 0 && len(over) == 0 {
		return nil
	}
	m := make(map[string]string, len(base)+len(over))
	for k, v := range base {
		m[k] = v
	}
	for k, v := range over {
		m[k] = v
	}
	return m
}

// guestFiles builds the files: entries provisioning /etc/hosts (so getfqdn(hostname)
// resolves locally instead of stalling on DNS) and /etc/resolv.conf into the guest.
func guestFiles(hostname string, dns []string) []rtconfig.FileConfig {
	var files []rtconfig.FileConfig
	if hostname != "" {
		hosts := "127.0.0.1\tlocalhost\n127.0.1.1\t" + hostname + "\n::1\tlocalhost ip6-localhost ip6-loopback\n"
		files = append(files, rtconfig.FileConfig{Path: "/etc/hosts", Content: hosts, Mode: "0644"})
	}
	if len(dns) > 0 {
		var b strings.Builder
		for _, ns := range dns {
			b.WriteString("nameserver " + ns + "\n")
		}
		files = append(files, rtconfig.FileConfig{Path: "/etc/resolv.conf", Content: b.String(), Mode: "0644"})
	}
	return files
}

// RestoreRefFor is the memory Snapshot ref sandbox-ctl should restore from.
// Cold launches, including S+cold, never pass a Snapshot to run --restore.
func RestoreRefFor(sb *types.Sandbox, tmpl types.TemplateID) string {
	if sb != nil && sb.LaunchMode == types.LaunchMemory && sb.ResumeSource.Kind == types.ResumeSourceSnapshot {
		return sb.ResumeSource.Ref
	}
	if sb != nil && sb.LaunchMode == types.LaunchMemory && sb.ResumeSource.Empty() && tmpl.Kind == types.KindSnp {
		return tmpl.Ref
	}
	return ""
}

// RestoreRef is RestoreRefFor for this Params.
func (p Params) RestoreRef() string { return RestoreRefFor(p.Sandbox, p.Template) }

// FromRef is the prepared Sandbox artifact used by a cold --from launch.
func (p Params) FromRef() string {
	if p.Sandbox == nil || p.Sandbox.LaunchMode != types.LaunchCold {
		return ""
	}
	if p.PreparedSource.Kind == types.ResumeSourceSandbox {
		return p.PreparedSource.Ref
	}
	if p.Sandbox.ResumeSource.Kind == types.ResumeSourceSandbox {
		return p.Sandbox.ResumeSource.Ref
	}
	if p.Sandbox.ResumeSource.Empty() && p.Template.Kind == types.KindSbx {
		return p.Template.Ref
	}
	return ""
}

// SourceForLaunch returns the durable root artifact that the tenant task must
// prepare. Fresh image launches have no Artifact source.
func SourceForLaunch(sb *types.Sandbox, tmpl types.TemplateID) types.ResumeSource {
	if sb == nil {
		return types.ResumeSource{}
	}
	if sb.ResumeSource.Valid() {
		return sb.ResumeSource
	}
	switch {
	case sb.LaunchMode == types.LaunchCold && tmpl.Kind == types.KindSbx:
		return types.ResumeSource{Kind: types.ResumeSourceSandbox, Ref: tmpl.Ref}
	case sb.LaunchMode == types.LaunchMemory && tmpl.Kind == types.KindSnp:
		return types.ResumeSource{Kind: types.ResumeSourceSnapshot, Ref: tmpl.Ref}
	default:
		return types.ResumeSource{}
	}
}

// MergeNetwork overlays over (the explicit / create network) onto base (the
// snapshot-inherited network) field by field: explicit wins, the snapshot fills what
// create left unset (node defaults fill the rest at resolve time). This is point 7's
// precedence — explicit create config over inherited snapshot config.
func MergeNetwork(base, over NetworkSpec) NetworkSpec {
	out := base
	if over.Hostname != "" {
		out.Hostname = over.Hostname
	}
	if len(over.DNS) > 0 {
		out.DNS = over.DNS
	}
	if over.InnerIP != "" {
		out.InnerIP = over.InnerIP
	}
	if over.Nexthop != "" {
		out.Nexthop = over.Nexthop
	}
	if over.TransitGatewayIP != "" {
		out.TransitGatewayIP = over.TransitGatewayIP
	}
	if over.TransitGeneveVNI != 0 {
		out.TransitGeneveVNI = over.TransitGeneveVNI
	}
	if over.TransitMAC != "" {
		out.TransitMAC = over.TransitMAC
	}
	return out
}

// ConnectSpecs are the UDS<->guest forwards sandbox-ctl should open. e2b exposes
// envd's control port (49983) and the code-interpreter port (49999) as UDSes the
// proxy dials; bare profiles expose none.
func (p Params) ConnectSpecs() []string {
	if p.Template.Profile != types.ProfileE2B {
		return nil
	}
	return []string{
		p.Sandbox.EnvdUDS + ":127.0.0.1:49983",
		p.Sandbox.CiUDS + ":127.0.0.1:49999",
	}
}
