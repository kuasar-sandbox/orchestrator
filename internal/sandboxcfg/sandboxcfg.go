// Package sandboxcfg renders one sandbox's config: the SANDBOX_CONFIG YAML plus
// the restore ref and connect forwards. Nothing is written to disk — the
// orchestrator serves these to sandbox-ctl over the config-socket at startup
// (see internal/configsock). The manifest key is delivered there too, separately.
//
// Schema tracks sandbox-runtime pkg/sandbox/config.go (SandboxConfig).
package sandboxcfg

import (
	"os"

	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/types"
	"gopkg.in/yaml.v3"
)

// Params is everything needed to render one sandbox's config.
type Params struct {
	Sandbox          *types.Sandbox
	Template         types.TemplateID
	RuntimeE2B       string            // erofs path for e2b profile (file path, no scheme)
	RuntimeBase      string            // erofs path for bare profile
	Kernel           string            // vmlinux path
	OverlayDiffTpl   string            // pre-formatted ext4 seeding the cold-boot overlay upper (file path)
	TapFDExec        []string          // Network.TapFD.Exec argv
	EnvVars          map[string]string // launch env
	VCPU             int               // resources.capacity.cpu
	Memory           string            // resources.capacity.memory, e.g. "2GiB"
	ControllerSocket string            // resources.control.controller (sentinel UDS; "" = static cgroup)
}

// WriteYAML renders the SANDBOX_CONFIG and writes it to path (0600). The
// orchestrator writes this file before starting the unit; sandbox-ctl reads it
// via --config. It is non-secret (the manifest key is delivered via env).
func (p Params) WriteYAML(path string) error {
	b, err := p.BuildYAML()
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o600)
}

// BuildYAML renders the SANDBOX_CONFIG document.
func (p Params) BuildYAML() ([]byte, error) {
	runtime := p.RuntimeBase
	var launchExec string // bare: empty -> sandbox-init runs the flattened image's entrypoint
	var launchArgs []string
	if p.Template.Profile == types.ProfileE2B {
		runtime = p.RuntimeE2B
		// envd is injected at /opt/sandbox-runtime/bin/envd, which sandbox-init
		// auto-bind-mounts into the guest at the same path (see build.Runtime).
		launchExec = "/opt/sandbox-runtime/bin/envd"
		launchArgs = []string{"-isnotfc", "-port", "49983"}
	}

	// No cgroup_path here: the sandbox adopts its systemd unit's own cgroup via
	// sandbox-ctl --cgroup-adopt, so the path is resolved at launch, not assigned.
	control := map[string]any{}
	if p.ControllerSocket != "" {
		control["controller"] = p.ControllerSocket // dynamic resource mode (sandbox-ctl <-> sentinel)
	}

	overlay := map[string]any{
		// diff omitted -> sandbox-ctl auto-creates <base-dir>/<sid>.overlay.diff,
		// seeded from diff_template below (a fresh blank diff is not a valid fs).
	}
	root := map[string]any{"overlay": overlay}
	// boot.root.base is the read-only rootfs image. For cold boot (img) it is the
	// flattened image manifest. For restore (snp) the snapshot provides it; we omit
	// it so snapshot.cfg fills it in.
	if p.Template.Kind == types.KindImg {
		root["base"] = p.Template.ManifestRef()
	}
	// Cold boot needs a pre-formatted ext4 source for the writable upper. On
	// restore the snapshot supplies the overlay chain, so only seed for cold boot.
	if p.RestoreRef() == "" && p.OverlayDiffTpl != "" {
		overlay["diff_template"] = "file://" + p.OverlayDiffTpl
	}

	network := map[string]any{
		"tapfd": map[string]any{"exec": p.TapFDExec},
	}
	if p.Sandbox.PortMAC != "" {
		network["mac"] = p.Sandbox.PortMAC
	}
	if p.Sandbox.InnerIP != "" {
		network["ip"] = p.Sandbox.InnerIP // CIDR form
	}

	doc := map[string]any{
		"resources": map[string]any{
			"capacity":    map[string]any{"cpu": p.VCPU, "memory": p.Memory},
			"allocatable": map[string]any{"cpu": float64(p.VCPU), "memory": p.Memory},
			"control":     control,
		},
		"network": network,
		"boot": map[string]any{
			"kernel":  "file://" + p.Kernel,
			"runtime": "file://" + runtime,
			"root":    root,
		},
		"launch": map[string]any{
			"exec":    launchExec,
			"args":    launchArgs,
			"env":     p.EnvVars,
			"restart": restartPolicy(p.Template.Profile),
		},
	}
	return yaml.Marshal(doc)
}

func restartPolicy(pr types.Profile) string {
	// e2b: envd is infrastructure (lifetime is TTL/kill-driven). bare default:
	// long-lived service. Both "always"; run-to-completion ("never") is opt-in.
	return "always"
}

// RestoreRef is the snapshot ref sandbox-ctl should restore from, or "" for a
// cold boot. snp templates restore the build snapshot; a resumed sandbox restores
// the latest pause snapshot.
func (p Params) RestoreRef() string {
	if p.Template.Kind != types.KindSnp {
		return ""
	}
	if p.Sandbox.SnapshotRef != "" { // resume from the latest pause snapshot
		return "manifest://" + p.Sandbox.SnapshotRef
	}
	return p.Template.ManifestRef()
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
