package sandboxcfg

import (
	"encoding/json"
	"fmt"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
	"gopkg.in/yaml.v3"
)

const (
	E2BStartCommandMetadata = "e2b.start_cmd"
	E2BReadyCommandMetadata = "e2b.ready_cmd"
)

// BuildColdParams is the shared cold-config projection used by both an
// offline Sandbox-E target and the initial C0 of a memory target. Source is a
// task-local E and contributes only portable non-boot defaults.
type BuildColdParams struct {
	Profile          types.Profile
	ImageRef         string
	Source           *rtconfig.PortableSandboxConfig
	Kernel           string
	Runtime          string
	OverlayDiffTpl   string
	TapFD            TapFD
	PortMAC          string
	InnerIP          string
	EnvVars          map[string]string
	Resources        rtconfig.ResourcesConfig
	Network          NetworkSpec
	MMDSEnabled      bool
	Spec             SandboxSpec
	NamespacePresent map[string]bool
	StartCmd         string
	ReadyCmd         string
}

// BuildColdConfig projects one ordinary explicit image cold configuration,
// then layers source-E non-boot defaults below registration options. Its result
// is the sole input for both offline export and Phase C.
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
		Runtime:  p.Runtime, Kernel: p.Kernel, OverlayDiffTpl: p.OverlayDiffTpl,
		TapFD: p.TapFD, EnvVars: p.EnvVars, Resources: p.Resources,
		Network: p.Network, MMDSEnabled: p.MMDSEnabled, Spec: p.Spec,
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
	if len(p.Source.Boot.Disks) != 0 {
		return nil, fmt.Errorf("sandboxcfg: build source E data disks are not supported")
	}

	// Portable and runtime schemas intentionally share field spellings for
	// non-host state. A YAML round trip is preferable to a second manual field
	// allowlist and is followed by overwriting every host/boot-owned field.
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
	merged.Boot = projected.Boot
	merged.Timeouts = projected.Timeouts
	merged.Restore = projected.Restore
	merged.EphemeralFiles = projected.EphemeralFiles

	if p.Profile == types.ProfileE2B {
		launch := projected.Launch
		launch.Env = mergeStr(merged.Launch.Env, projected.Launch.Env)
		merged.Launch = launch
	} else {
		mergeBareBuildLaunch(&merged.Launch, p.Spec.Launch, p.Spec.LaunchCgroupControl, p.EnvVars)
	}
	if p.NamespacePresent[NsMounts] {
		merged.Mounts = append([]rtconfig.MountConfig(nil), p.Spec.Mounts...)
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
	return &merged, nil
}

func mergeBareBuildLaunch(dst *rtconfig.LaunchConfig, override *rtconfig.LaunchConfig, cgroupControl *bool, env map[string]string) {
	dst.Env = mergeStr(dst.Env, env)
	if override == nil {
		return
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
// omitempty. The same bytes are also accepted by offline export.
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
	// defaults instead of disappearing through omitempty. Offline export reads
	// the same document directly, which keeps both target paths equivalent.
	document["launch"] = map[string]any{
		"exec": cfg.Launch.Exec, "args": cfg.Launch.Args, "env": cfg.Launch.Env,
		"ephemeral_env": cfg.Launch.EphemeralEnv, "workdir": cfg.Launch.Workdir,
		"restart": cfg.Launch.Restart, "cgroup_control": cfg.Launch.CgroupControl,
		"placeholder": cfg.Launch.Placeholder, "pid_namespace": cfg.Launch.PIDNamespace,
		"plugin": cfg.Launch.Plugin, "user": cfg.Launch.User,
		"stop_signal": cfg.Launch.StopSignal, "stop_grace_period": cfg.Launch.StopGracePeriod,
		"start_timeout": cfg.Launch.StartTimeout,
	}
	// DeflateOnOOM is the sole optional portable resource leaf. An explicit null
	// clears a source value when the shared projection has no settled value,
	// while sandboxer's host policy fields remain untouched.
	document["resources"] = map[string]any{
		"capacity": cfg.Resources.Capacity,
		"allocatable": map[string]any{
			"cpu": cfg.Resources.Allocatable.CPU, "memory": cfg.Resources.Allocatable.Memory,
			"deflate_on_oom": cfg.Resources.Allocatable.DeflateOnOOM,
		},
		"control": cfg.Resources.Control, "overhead": cfg.Resources.Overhead,
		"watermark_high": cfg.Resources.WatermarkHigh, "startup": cfg.Resources.Startup,
	}
	document["mounts"] = cfg.Mounts
	document["files"] = cfg.Files
	document["init"] = cfg.Init
	document["metadata"] = cfg.Metadata
	return yaml.Marshal(document)
}
