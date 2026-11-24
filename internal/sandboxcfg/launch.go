package sandboxcfg

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
	"gopkg.in/yaml.v3"
)

const (
	envdExec          = "/opt/sandbox-runtime/bin/envd"
	directCgroupRoot  = "/sys/fs/cgroup"
	systemdCgroupRoot = "/run/sandbox-init/cgroup"
)

func parseLaunch(raw string) (*rtconfig.LaunchConfig, map[string]bool, *bool, error) {
	object, err := decodeJSONObject(NsLaunch, []byte(raw))
	if err != nil {
		return nil, nil, nil, err
	}
	keys := make(map[string]bool, len(object))
	for k := range object {
		keys[k] = true
	}
	if value, ok := object["systemd"]; ok {
		var mode string
		if err := decodeLeaf(NsLaunch+".systemd", value, &mode); err != nil {
			return nil, nil, nil, err
		}
		if !validSystemdMode(mode) {
			return nil, nil, nil, fmt.Errorf("%s.systemd must be off|auto|runtime|provided", NsLaunch)
		}
	}
	var launch rtconfig.LaunchConfig
	if err := yaml.Unmarshal([]byte(raw), &launch); err != nil {
		return nil, nil, nil, fmt.Errorf("%s: %w", NsLaunch, err)
	}
	var cg *bool
	if value, ok := object["cgroup_control"]; ok {
		var enabled bool
		if err := json.Unmarshal(value, &enabled); err != nil {
			return nil, nil, nil, fmt.Errorf("%s.cgroup_control: %w", NsLaunch, err)
		}
		cg = &enabled
	}
	return &launch, keys, cg, nil
}
func validSystemdMode(s string) bool {
	switch s {
	case "off", "auto", "runtime", "provided":
		return true
	}
	return false
}

// E2B owns every Launch field except the one systemd policy.
func ValidateLaunchForProfile(profile types.Profile, spec SandboxSpec) error {
	if !profile.Valid() {
		return fmt.Errorf("sandboxcfg: unknown profile %q", profile)
	}
	if spec.Launch != nil && spec.Launch.Systemd != "" && !validSystemdMode(spec.Launch.Systemd) {
		return fmt.Errorf("sandboxcfg: launch.systemd must be off|auto|runtime|provided")
	}
	if profile != types.ProfileE2B {
		return nil
	}
	for key := range spec.LaunchKeys {
		if key != "systemd" {
			return fmt.Errorf("sandboxcfg: e2b launch only permits systemd; field %q is product-owned", key)
		}
	}
	if spec.Launch != nil {
		other := *spec.Launch
		other.Systemd = ""
		if !reflect.DeepEqual(other, rtconfig.LaunchConfig{}) || spec.LaunchCgroupControl != nil {
			return fmt.Errorf("sandboxcfg: e2b launch only permits systemd")
		}
	}
	return nil
}
func (s SandboxSpec) HasSystemdOverride() bool {
	return s.LaunchKeys["systemd"] || s.Launch != nil && s.Launch.Systemd != ""
}

// The original product layout is retained. systemd changes only PID 1 and
// envd's cgroup access path, never the envd primary role.
func newE2BLaunch(mode string, env map[string]string, mmds bool) rtconfig.LaunchConfig {
	if mode == "" {
		mode = "off"
	}
	args := []string{"-isnotfc", "-port", "49983"}
	if mmds {
		args = []string{"-port", "49983"}
	}
	if mode != "off" {
		args = append(args, "-cgroup-root", systemdCgroupRoot)
	}
	return rtconfig.LaunchConfig{
		Systemd: mode, Exec: envdExec, Args: args, Env: mergeStr(nil, env),
		Restart: "always", User: "0:0", PIDNamespace: "shared", CgroupControl: true,
	}
}
func envdCgroupRoot(mode string) string {
	if mode == "" || mode == "off" {
		return directCgroupRoot
	}
	return systemdCgroupRoot
}

// LaunchProjection is an ordinary Host YAML document. The retained primary's
// environment is already materialized by Sandboxer's OCI/Launch merge.
type LaunchProjection struct{ Config []byte }

func completeLaunchMap(launch rtconfig.LaunchConfig) map[string]any {
	return map[string]any{
		"systemd": launch.EffectiveSystemd(),
		"exec":    launch.Exec, "args": launch.Args, "env": mergeStr(nil, launch.Env),
		"ephemeral_env": launch.EphemeralEnv, "workdir": launch.Workdir,
		"restart": launch.Restart, "cgroup_control": launch.CgroupControl,
		"placeholder": launch.Placeholder, "pid_namespace": launch.PIDNamespace,
		"plugin": launch.Plugin, "user": launch.User,
		"stop_signal": launch.StopSignal, "stop_grace_period": launch.StopGracePeriod,
		"start_timeout": launch.StartTimeout,
	}
}

// Only keys that appeared in tenant input override a prepared source E.
func (s SandboxSpec) launchOverlay(env map[string]string) map[string]any {
	result := map[string]any{}
	if s.Launch != nil {
		fields := completeLaunchMap(*s.Launch)
		if s.LaunchKeys != nil {
			for key := range s.LaunchKeys {
				if value, ok := fields[key]; ok {
					result[key] = value
				}
			}
		} else {
			for key, value := range fields {
				if key != "systemd" && value != nil && !reflect.ValueOf(value).IsZero() {
					result[key] = value
				}
			}
			if s.Launch.Systemd != "" {
				result["systemd"] = s.Launch.Systemd
			}
		}
		if s.LaunchCgroupControl != nil {
			result["cgroup_control"] = *s.LaunchCgroupControl
		}
		env = mergeStr(env, s.Launch.Env)
	}
	if len(env) > 0 {
		result["env"] = env
	}
	return result
}

// ProjectSourceLaunch retains E's original process topology and merges the
// explicit request before updating the product-owned envd cgroup flag. It
// introduces neither plugin migration nor PATH-origin metadata.
func ProjectSourceLaunch(raw []byte, profile types.Profile, source *rtconfig.PortableSandboxConfig) (*LaunchProjection, error) {
	if source == nil {
		return nil, fmt.Errorf("sandboxcfg: source launch projection requires prepared Sandbox config")
	}
	if !profile.Valid() {
		return nil, fmt.Errorf("sandboxcfg: unknown profile %q", profile)
	}
	host, presence, err := rtconfig.LoadConfigBytesWithPresence(raw)
	if err != nil {
		return nil, err
	}
	if presence.Has("launch.env") {
		host.Launch.Env = mergeStr(source.Launch.Env, host.Launch.Env)
	}
	merged, _, err := rtconfig.ApplyFromRules(source, host, presence, rtconfig.ApplyFromOptions{})
	if err != nil {
		return nil, fmt.Errorf("sandboxcfg: merge prepared source: %w", err)
	}
	if profile == types.ProfileE2B && presence.Has("launch.systemd") {
		if err := projectExistingEnvdRoot(&merged.Launch); err != nil {
			return nil, err
		}
	}
	return MarshalLaunchProjection(raw, merged.Launch)
}

// ProjectExistingEnvdRoot only accepts the historical, product-owned primary.
// Existing Sandbox E artifacts may be created with direct mode; explicit
// systemd override changes a known -cgroup-root parameter and nothing else.
func projectExistingEnvdRoot(launch *rtconfig.LaunchConfig) error {
	if launch.Exec != envdExec || launch.Placeholder || len(launch.Plugin) != 0 {
		return fmt.Errorf("sandboxcfg: E2B source must retain envd as primary; build a new cold definition")
	}
	args := append([]string(nil), launch.Args...)
	seen := false
	for i := 0; i < len(args); i++ {
		arg := args[i]
		name, value, inline := strings.Cut(strings.TrimLeft(arg, "-"), "=")
		if !strings.HasPrefix(arg, "-") {
			return fmt.Errorf("sandboxcfg: unknown E2B envd argument %q", arg)
		}
		switch name {
		case "isnotfc":
			if inline && value != "true" && value != "false" {
				return fmt.Errorf("sandboxcfg: invalid E2B envd isnotfc argument")
			}
		case "port", "cgroup-root":
			if !inline {
				if i+1 == len(args) {
					return fmt.Errorf("sandboxcfg: missing E2B envd %s value", name)
				}
				i++
				value = args[i]
			}
			if name == "port" {
				if _, err := strconv.ParseUint(value, 10, 16); err != nil {
					return fmt.Errorf("sandboxcfg: invalid E2B envd port")
				}
				continue
			}
			if seen || value == "" || strings.HasPrefix(value, "-") {
				return fmt.Errorf("sandboxcfg: ambiguous E2B cgroup root")
			}
			seen = true
			if inline {
				args[i] = strings.SplitN(arg, "=", 2)[0] + "=" + envdCgroupRoot(launch.EffectiveSystemd())
			} else {
				args[i] = envdCgroupRoot(launch.EffectiveSystemd())
			}
		default:
			return fmt.Errorf("sandboxcfg: unknown E2B envd argument %q", arg)
		}
	}
	if !seen && launch.EffectiveSystemd() != "off" {
		args = append(args, "-cgroup-root", systemdCgroupRoot)
	}
	launch.Args = args
	return nil
}
func MarshalLaunchProjection(raw []byte, launch rtconfig.LaunchConfig) (*LaunchProjection, error) {
	var document map[string]any
	if err := yaml.Unmarshal(raw, &document); err != nil {
		return nil, err
	}
	document["launch"] = completeLaunchMap(launch)
	encoded, err := yaml.Marshal(document)
	if err != nil {
		return nil, err
	}
	return &LaunchProjection{Config: encoded}, nil
}
func ProjectSourceLaunchFile(path string, profile types.Profile, source *rtconfig.PortableSandboxConfig) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	projection, err := ProjectSourceLaunch(raw, profile, source)
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".source-launch-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(projection.Config); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), path)
}
