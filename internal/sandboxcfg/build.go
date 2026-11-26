package sandboxcfg

import (
	"encoding/json"
	"fmt"
	"github.com/kuasar-sandbox/orchestrator/internal/workspace"
	"slices"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
	"gopkg.in/yaml.v3"
)

const (
	E2BStartCommandMetadata = "e2b.start_cmd"
	E2BReadyCommandMetadata = "e2b.ready_cmd"
)

// BuildColdParams is the shared cold-config projection used by both a
// top-level Sandbox E target and the initial C0 of a memory target. Source is
// a task-local E and contributes portable non-boot defaults plus its cmdline
// when the node has not selected one.
type BuildColdParams struct {
	Profile          types.Profile
	ImageRef         string
	Source           *rtconfig.PortableSandboxConfig
	Kernel           string
	Cmdline          *string
	Runtime          string
	OverlayDiffTpl   string
	TapFD            TapFD
	PortMAC          string
	InnerIP          string
	EnvVars          map[string]string
	Resources        rtconfig.ResourcesConfig
	Timeouts         rtconfig.TimeoutsConfig
	Network          NetworkSpec
	MMDSEnabled      bool
	Spec             SandboxSpec
	NamespacePresent map[string]bool
	StartCmd         string
	ReadyCmd         string
}

// BuildColdConfig projects one ordinary explicit image cold configuration,
// then layers source-E non-boot defaults below registration options. Its result
// is the sole input for both direct top-level E assembly and Phase C.
func (p BuildColdParams) BuildColdConfig() (*rtconfig.SandboxConfig, error) {
	if !p.Profile.Valid() {
		return nil, fmt.Errorf("sandboxcfg: unknown build profile %q", p.Profile)
	}
	if p.ImageRef == "" {
		return nil, fmt.Errorf("sandboxcfg: build final image ref is required")
	}
	fake := &types.Sandbox{
		ID: "build-c", Profile: p.Profile, LaunchMode: types.LaunchImage,
		PortMAC: p.PortMAC, InnerIP: p.InnerIP,
	}
	ordinary := Params{
		Sandbox:  fake,
		Template: types.TemplateID{Profile: p.Profile, Kind: types.KindImg, Ref: p.ImageRef},
		Runtime:  p.Runtime, Kernel: p.Kernel, Cmdline: p.Cmdline, OverlayDiffTpl: p.OverlayDiffTpl,
		TapFD: p.TapFD, EnvVars: p.EnvVars, Resources: p.Resources,
		Timeouts: p.Timeouts,
		Network:  p.Network, MMDSEnabled: p.MMDSEnabled, Spec: p.Spec,
	}
	projected, err := ordinary.buildImageColdConfig()
	if err != nil {
		return nil, err
	}
	if p.Source == nil {
		projected.Metadata = setBuildCommands(projected.Metadata, p.StartCmd, p.ReadyCmd)
		return projected, nil
	}
	if err := p.Source.Validate(); err != nil {
		return nil, fmt.Errorf("sandboxcfg: build source E: %w", err)
	}
	if p.Cmdline == nil {
		projected.Boot.Cmdline = p.Source.Boot.Cmdline
	}
	captured := workspace.Captured(p.Source)
	if len(p.Source.Boot.Disks) > 0 && len(captured) == 0 || len(p.Spec.WorkspacePlan) < len(captured) {
		return nil, fmt.Errorf("sandboxcfg: build source E data disks are not supported")
	}

	// Portable and runtime schemas share the persistent non-boot fields.
	// The source is already authoritative for its Env and primary layout.
	raw, err := yaml.Marshal(p.Source)
	if err != nil {
		return nil, fmt.Errorf("sandboxcfg: encode source defaults: %w", err)
	}
	var merged rtconfig.SandboxConfig
	if err := yaml.Unmarshal(raw, &merged); err != nil {
		return nil, fmt.Errorf("sandboxcfg: decode source defaults: %w", err)
	}
	merged.Resources = projected.Resources
	merged.Network = projected.Network
	// The provider and L3 identity are invocation-owned, while the guest-facing
	// interface name is portable source topology. Build Register has no second
	// interface authority, so retain it exactly as ordinary run --from does.
	merged.Network.Interface = p.Source.Network.Interface
	sourceDisks := merged.Boot.Disks
	merged.Boot = projected.Boot
	if len(sourceDisks) > 0 {
		// The YAML host schema intentionally omits the authenticated binding
		// and pure-block flag. Preserve both from the selected native E.
		merged.Boot.Disks = sourceDisks
		for i := range sourceDisks {
			sourceDisks[i].PureBlock = p.Source.Boot.Disks[i].PureBlock
			sourceDisks[i].Binding = p.Source.Boot.Disks[i].Binding
		}
	}
	merged.Timeouts = projected.Timeouts
	merged.Restore = projected.Restore
	merged.EphemeralFiles = projected.EphemeralFiles

	if p.Profile == types.ProfileE2B {
		mode := merged.Launch.EffectiveSystemd()
		if p.Spec.HasSystemdOverride() {
			mode = p.Spec.Launch.Systemd
		}
		merged.Launch = newE2BLaunch(mode, mergeStr(merged.Launch.Env, p.EnvVars), p.MMDSEnabled)
	} else {
		mergeBareBuildLaunch(&merged.Launch, p.Spec.Launch, p.Spec.LaunchCgroupControl, p.EnvVars)
	}
	if p.NamespacePresent[NsMounts] {
		merged.Mounts = append([]rtconfig.MountConfig(nil), p.Spec.Mounts...)
	}
	if len(captured) > 0 {
		// Disk projections are source-owned, including disk ordinals unrelated to
		// workspaces. Keep their relative order when replacing non-disk mounts.
		if p.NamespacePresent[NsMounts] {
			mounts := slices.Clone(merged.Mounts)
			for _, m := range p.Source.Mounts {
				if m.Type == "disk" {
					mounts = append(mounts, m)
				}
			}
			merged.Mounts = mounts
		}
		if err := merged.ValidateMountTopology(merged.Mounts); err != nil {
			return nil, err
		}
	}
	if p.NamespacePresent[NsFiles] {
		merged.Files = append([]rtconfig.FileConfig(nil), p.Spec.Files...)
	}
	if p.NamespacePresent[NsInit] {
		merged.Init = append([]rtconfig.InitConfig(nil), p.Spec.Init...)
	}
	if p.NamespacePresent[NsMetadata] {
		merged.Metadata = mergeStr(nil, p.Spec.Metadata)
	}
	merged.Metadata = workspace.SetTopology(merged.Metadata, p.Spec.WorkspacePlan)
	if !p.Network.IsZero() {
		network, err := json.Marshal(p.Network)
		if err != nil {
			return nil, fmt.Errorf("sandboxcfg: marshal build network: %w", err)
		}
		if merged.Metadata == nil {
			merged.Metadata = map[string]string{}
		}
		merged.Metadata[NsNetwork] = string(network)
	}
	merged.Metadata = setBuildCommands(merged.Metadata, p.StartCmd, p.ReadyCmd)
	merged.Launch.Systemd = merged.Launch.EffectiveSystemd()
	return &merged, nil
}

func mergeBareBuildLaunch(dst *rtconfig.LaunchConfig, override *rtconfig.LaunchConfig, cgroupControl *bool, env map[string]string) {
	dst.Env = mergeStr(dst.Env, env)
	if override == nil {
		return
	}
	if override.Systemd != "" {
		dst.Systemd = override.Systemd
	}
	if override.Placeholder {
		dst.Placeholder = true
		dst.Exec, dst.Args = "", nil
	}
	if override.PIDNamespace != "" {
		dst.PIDNamespace = override.PIDNamespace
	}
	if override.StopGracePeriod != "" {
		dst.StopGracePeriod = override.StopGracePeriod
	}
	if override.StartTimeout != "" {
		dst.StartTimeout = override.StartTimeout
	}
	if override.Exec != "" {
		dst.Exec = override.Exec
	}
	if len(override.Args) > 0 {
		dst.Args = append([]string(nil), override.Args...)
	}
	if override.Workdir != "" {
		dst.Workdir = override.Workdir
	}
	if override.Restart != "" {
		dst.Restart = override.Restart
	}
	if override.User != "" {
		dst.User = override.User
	}
	if override.StopSignal != "" {
		dst.StopSignal = override.StopSignal
	}
	if explicit := cgroupControl; explicit != nil {
		dst.CgroupControl = *explicit
	}
	if len(override.Plugin) > 0 {
		dst.Plugin = append([]rtconfig.PluginConfig(nil), override.Plugin...)
	}
	dst.Env = mergeStr(dst.Env, override.Env)
}

func setBuildCommands(metadata map[string]string, start, ready string) map[string]string {
	if metadata == nil && (start != "" || ready != "") {
		metadata = map[string]string{}
	}
	if start == "" {
		delete(metadata, E2BStartCommandMetadata)
	} else {
		metadata[E2BStartCommandMetadata] = start
	}
	if ready == "" {
		delete(metadata, E2BReadyCommandMetadata)
	} else {
		metadata[E2BReadyCommandMetadata] = ready
	}
	return metadata
}

// MarshalBuildColdConfig keeps every portable collection explicitly present.
// This is important for run --from --replace-boot: an intentionally empty
// collection must clear a source default rather than disappear through
// omitempty. The same bytes are also accepted by direct top-level E assembly.
func MarshalBuildColdConfig(cfg *rtconfig.SandboxConfig) ([]byte, error) {
	if cfg == nil {
		return nil, fmt.Errorf("sandboxcfg: nil build cold config")
	}
	raw, err := yaml.Marshal(cfg)
	if err != nil {
		return nil, err
	}
	var document map[string]any
	if err := yaml.Unmarshal(raw, &document); err != nil {
		return nil, err
	}
	// run --from applies persistent non-boot fields by YAML presence. Emit the
	// complete persistent launch surface so false/empty values clear source-E
	// defaults instead of disappearing through omitempty. Direct top-level E
	// assembly reads the same projection, keeping both target paths equivalent.
	document["launch"] = completeLaunchMap(cfg.Launch)
	// DeflateOnOOM is the sole optional portable resource leaf. An explicit null
	// clears a source value when the shared projection has no settled value,
	// while sandboxer's host policy fields remain untouched.
	if cfg.Resources.Allocatable.DeflateOnOOM == nil {
		resources, _ := document["resources"].(map[string]any)
		allocatable, ok := resources["allocatable"].(map[string]any)
		if !ok {
			return nil, fmt.Errorf("sandboxcfg: generated build config lacks resources.allocatable mapping")
		}
		allocatable["deflate_on_oom"] = nil
	}
	document["mounts"] = cfg.Mounts
	document["files"] = cfg.Files
	document["init"] = cfg.Init
	document["metadata"] = cfg.Metadata
	return yaml.Marshal(document)
}
