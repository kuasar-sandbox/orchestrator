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
	systemdToolsPath  = "/run/sandbox-runtime/systemd/bin"
	defaultExecPath   = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
)

func parseLaunch(raw string) (*rtconfig.LaunchConfig, map[string]bool, *bool, error) {
	object, err := decodeJSONObject(NsLaunch, []byte(raw))
	if err != nil {
		return nil, nil, nil, err
	}
	keys := make(map[string]bool, len(object))
	for key := range object {
		keys[key] = true
	}
	if value, present := object["systemd"]; present {
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
	var cgroup *bool
	if value, present := object["cgroup_control"]; present {
		var enabled bool
		if err := json.Unmarshal(value, &enabled); err != nil {
			return nil, nil, nil, fmt.Errorf("%s.cgroup_control: %w", NsLaunch, err)
		}
		cgroup = &enabled
	}
	return &launch, keys, cgroup, nil
}

func validSystemdMode(mode string) bool {
	switch mode {
	case "off", "auto", "runtime", "provided":
		return true
	default:
		return false
	}
}

// ValidateLaunchForProfile uses the original object's keys, including zero
// values. LaunchKeys is carried with SandboxSpec across build/task JSON.
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
	// Also protect internal callers that construct a typed spec directly.
	if spec.Launch != nil {
		other := *spec.Launch
		other.Systemd = ""
		if !reflect.DeepEqual(other, rtconfig.LaunchConfig{}) || spec.LaunchCgroupControl != nil {
			return fmt.Errorf("sandboxcfg: e2b launch only permits systemd")
		}
	}
	return nil
}

// HasSystemdOverride distinguishes inheritance from explicit off. Historical
// create metadata must not be consulted when resuming a durable E/S source.
func (s SandboxSpec) HasSystemdOverride() bool {
	return s.LaunchKeys["systemd"] || s.Launch != nil && s.Launch.Systemd != ""
}

func newE2BLaunch(mode string, env map[string]string, mmds bool) rtconfig.LaunchConfig {
	if mode == "" {
		mode = "off"
	}
	args := []string{"-isnotfc", "-port", "49983"}
	if mmds {
		args = []string{"-port", "49983"}
	}
	args = append(args, "-cgroup-root", envdCgroupRoot(mode))
	return rtconfig.LaunchConfig{
		Systemd: mode, Placeholder: true, Env: mergeStr(nil, env),
		Restart: "always", PIDNamespace: "shared", CgroupControl: true,
		Plugin: []rtconfig.PluginConfig{{Exec: envdExec, Args: args,
			Env: mergeStr(nil, env), User: "0:0", Restart: "always"}},
	}
}

func envdCgroupRoot(mode string) string {
	if mode == "" || mode == "off" {
		return directCgroupRoot
	}
	return systemdCgroupRoot
}

// ProjectNewE2BEnvironment materializes the product envd environment from the
// same final image used for boot. Ordinary plugins keep their independent Env
// semantics. Image defaults are below source/request values; explicit PATH,
// including empty, remains authoritative.
func ProjectNewE2BEnvironment(launch *rtconfig.LaunchConfig, imageEnv map[string]string) error {
	if launch.Exec != "" || !launch.Placeholder || len(launch.Plugin) != 1 || launch.Plugin[0].Exec != envdExec {
		return fmt.Errorf("sandboxcfg: new E2B environment requires the product placeholder/envd layout")
	}
	defaultPath := launch.PersistentSystemdPathDefault()
	launch.Env = mergeStr(imageEnv, launch.Env)
	if launch.Env == nil {
		launch.Env = map[string]string{}
	}
	if defaultPath {
		if _, present := launch.Env["PATH"]; !present {
			launch.Env["PATH"] = defaultExecPath
		}
	}
	launch.SetSystemdPathDefault(defaultPath)
	launch.Plugin[0].Env = mergeStr(launch.Env, launch.Plugin[0].Env)
	if defaultPath {
		launch.Plugin[0].Env["PATH"] = envdDefaultPath(launch.EffectiveSystemd(), launch.Env["PATH"])
	}
	launch.Plugin[0].SetSystemdPathDefault(defaultPath)
	return nil
}

func envdDefaultPath(mode, path string) string {
	if mode != "off" && mode != "" {
		// Only runtime projects this directory. In provided mode an absent
		// directory is skipped by normal PATH search; image directories remain.
		return systemdToolsPath + ":" + path
	}
	return path
}

// LaunchProjection binds trusted PATH provenance to the exact ordinary host
// document consumed by sandbox-ctl. The provenance is handed off privately;
// its internal keys never become tenant-settable Launch fields.
type LaunchProjection struct {
	Config     []byte
	Provenance []byte
}

// ProjectImageLaunch finishes a fresh image E2B launch inside the tenant task.
// The immutable ref is checked against the actual rendered boot config before
// any environment is published; no image metadata crosses the conductor.
func ProjectImageLaunch(raw []byte, imageRef string, imageEnv map[string]string) (*LaunchProjection, error) {
	host, _, err := rtconfig.LoadConfigBytesWithPresence(raw)
	if err != nil {
		return nil, err
	}
	if host.Boot.Root.Base != imageRef {
		return nil, fmt.Errorf("sandboxcfg: image launch projection does not match boot.root.base")
	}
	if err := ProjectNewE2BEnvironment(&host.Launch, imageEnv); err != nil {
		return nil, err
	}
	return MarshalLaunchProjection(raw, host.Launch)
}

func completeLaunchMap(launch rtconfig.LaunchConfig) map[string]any {
	env := mergeStr(nil, launch.Env)
	// This is an ordinary host overlay, which cannot carry trusted artifact
	// provenance. --from restores this inherited default from E; direct image
	// startup materializes it again from the same image. Explicit PATH, including
	// empty, stays present. Top-level E assembly receives the typed config.
	if launch.PersistentSystemdPathDefault() {
		delete(env, "PATH")
	}
	return map[string]any{
		"systemd": launch.EffectiveSystemd(),
		"exec":    launch.Exec, "args": launch.Args, "env": env,
		"ephemeral_env": launch.EphemeralEnv, "workdir": launch.Workdir,
		"restart": launch.Restart, "cgroup_control": launch.CgroupControl,
		"placeholder": launch.Placeholder, "pid_namespace": launch.PIDNamespace,
		"plugin": launch.Plugin, "user": launch.User,
		"stop_signal": launch.StopSignal, "stop_grace_period": launch.StopGracePeriod,
		"start_timeout": launch.StartTimeout,
	}
}

// launchOverlay emits only tenant-present fields and invocation env. In
// particular an absent mode, placeholder or cgroup boolean cannot clear E.
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
			// Programmatic callers have no raw JSON object. Nonzero fields are
			// overrides; explicit booleans use the existing presence pointer.
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

// ProjectSourceLaunch retains the authenticated source's full process layout.
// The result remains a host/instance document, not an immutable boot definition.
// Call only for a fresh E create, after task-local preparation and before spawn.
func ProjectSourceLaunch(raw []byte, profile types.Profile, source *rtconfig.PortableSandboxConfig) (*LaunchProjection, error) {
	if source == nil {
		return nil, fmt.Errorf("sandboxcfg: source launch projection requires the prepared Sandbox config")
	}
	if !profile.Valid() {
		return nil, fmt.Errorf("sandboxcfg: unknown profile %q", profile)
	}
	host, presence, err := rtconfig.LoadConfigBytesWithPresence(raw)
	if err != nil {
		return nil, err
	}
	requestEnv := mergeStr(nil, host.Launch.Env)
	if presence.Has("launch.env") {
		_, explicitPath := host.Launch.Env["PATH"]
		host.Launch.Env = mergeStr(source.Launch.Env, host.Launch.Env)
		if source.Launch.SystemdPathDefault && !explicitPath {
			delete(host.Launch.Env, "PATH")
		}
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
	if profile == types.ProfileE2B {
		projectSourceEnvdEnvironment(&merged.Launch, source.Launch.Systemd, requestEnv, presence.Has("launch.systemd"))
	}
	return MarshalLaunchProjection(raw, merged.Launch)
}

func projectSourceEnvdEnvironment(launch *rtconfig.LaunchConfig, sourceMode string, request map[string]string, modeOverride bool) {
	// Old envd-as-primary already receives the merged Launch.Env. Only the
	// recognized product plugin receives instance overrides. Other plugins and
	// unknown saved layouts remain authoritative.
	if launch.Exec != "" || !launch.Placeholder {
		return
	}
	index, count := -1, 0
	for i := range launch.Plugin {
		if launch.Plugin[i].Exec == envdExec {
			index, count = i, count+1
		}
	}
	if count != 1 {
		return
	}
	plugin := &launch.Plugin[index]
	defaultPath := plugin.PersistentSystemdPathDefault()
	plugin.Env = mergeStr(plugin.Env, request)
	if _, explicit := request["PATH"]; explicit {
		plugin.SetSystemdPathDefault(false)
		return
	}
	if defaultPath && modeOverride {
		path, present := plugin.Env["PATH"]
		if !present {
			path = defaultExecPath
		}
		// Only a non-off product definition added this one leading component.
		// The trusted marker grants the transformation; string equality never
		// grants provenance. Preserve the original image PATH byte-for-byte,
		// including its own occurrence of the same directory or empty entries.
		if sourceMode != "" && sourceMode != "off" {
			path = strings.TrimPrefix(path, systemdToolsPath+":")
		}
		if plugin.Env == nil {
			plugin.Env = make(map[string]string)
		}
		plugin.Env["PATH"] = envdDefaultPath(launch.EffectiveSystemd(), path)
	}
	plugin.SetSystemdPathDefault(defaultPath)
}

// MarshalLaunchProjection preserves a fully merged trusted Launch across the
// private task-to-CLI boundary; ordinary YAML still contains no internal fields.
func MarshalLaunchProjection(raw []byte, launch rtconfig.LaunchConfig) (*LaunchProjection, error) {
	var document map[string]any
	if err := yaml.Unmarshal(raw, &document); err != nil {
		return nil, err
	}
	document["launch"] = completeLaunchMap(launch)
	// Unlike a plain tenant overlay, this task projection has a digest-bound
	// private provenance envelope. Keep the concrete default PATH so the child
	// does not need to reread or infer the selected OCI image's environment.
	document["launch"].(map[string]any)["env"] = mergeStr(nil, launch.Env)
	encoded, err := yaml.Marshal(document)
	if err != nil {
		return nil, err
	}
	provenance, err := rtconfig.MarshalConfigProvenance(encoded, launch)
	if err != nil {
		return nil, err
	}
	return &LaunchProjection{Config: encoded, Provenance: provenance}, nil
}

func projectExistingEnvdRoot(launch *rtconfig.LaunchConfig) error {
	plugin := -1
	count := 0
	for i := range launch.Plugin {
		if launch.Plugin[i].Exec == envdExec {
			plugin, count = i, count+1
		}
	}
	var args *[]string
	switch {
	case launch.Exec == envdExec && !launch.Placeholder && count == 0:
		args = &launch.Args
	case launch.Exec == "" && launch.Placeholder && count == 1:
		args = &launch.Plugin[plugin].Args
	default:
		return fmt.Errorf("sandboxcfg: cannot change systemd for an unknown or ambiguous E2B envd layout; build a new cold definition")
	}
	result := append([]string(nil), (*args)...)
	seen := false
	for i := 0; i < len(result); i++ {
		arg := result[i]
		name, value, inline := strings.Cut(strings.TrimPrefix(strings.TrimPrefix(arg, "-"), "-"), "=")
		if !strings.HasPrefix(arg, "-") {
			return fmt.Errorf("sandboxcfg: unknown E2B envd arguments; build a new cold definition")
		}
		switch name {
		case "isnotfc":
			if inline && value != "true" && value != "false" {
				return fmt.Errorf("sandboxcfg: invalid E2B envd isnotfc argument; build a new cold definition")
			}
		case "port", "cgroup-root":
			if !inline {
				if i+1 == len(result) {
					return fmt.Errorf("sandboxcfg: missing E2B envd %s value; build a new cold definition", name)
				}
				i++
				value = result[i]
			}
			if name == "port" {
				if _, err := strconv.ParseUint(value, 10, 16); err != nil {
					return fmt.Errorf("sandboxcfg: invalid E2B envd port value; build a new cold definition")
				}
				continue
			}
			if seen || value == "" || strings.HasPrefix(value, "-") {
				return fmt.Errorf("sandboxcfg: ambiguous E2B envd cgroup-root; build a new cold definition")
			}
			seen = true
			if inline {
				result[i] = strings.SplitN(arg, "=", 2)[0] + "=" + envdCgroupRoot(launch.EffectiveSystemd())
			} else {
				result[i] = envdCgroupRoot(launch.EffectiveSystemd())
			}
		default:
			// Unknown flag arity and positional arguments cannot safely be
			// rewritten: Go flags stop parsing at the first positional or --.
			return fmt.Errorf("sandboxcfg: unknown E2B envd argument %q; build a new cold definition", arg)
		}
	}
	if !seen {
		result = append(result, "-cgroup-root", envdCgroupRoot(launch.EffectiveSystemd()))
	}
	*args = result
	return nil
}

func ProjectSourceLaunchFile(path string, profile types.Profile, source *rtconfig.PortableSandboxConfig) ([]byte, error) {
	return projectLaunchFile(path, func(raw []byte) (*LaunchProjection, error) { return ProjectSourceLaunch(raw, profile, source) })
}

func ProjectImageLaunchFile(path, imageRef string, imageEnv map[string]string) ([]byte, error) {
	return projectLaunchFile(path, func(raw []byte) (*LaunchProjection, error) { return ProjectImageLaunch(raw, imageRef, imageEnv) })
}

func projectLaunchFile(path string, project func([]byte) (*LaunchProjection, error)) ([]byte, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	projection, err := project(raw)
	if err != nil {
		return nil, err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".source-launch-*")
	if err != nil {
		return nil, err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(projection.Config); err != nil {
		_ = tmp.Close()
		return nil, err
	}
	if err := tmp.Close(); err != nil {
		return nil, err
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return nil, err
	}
	return projection.Provenance, nil
}
