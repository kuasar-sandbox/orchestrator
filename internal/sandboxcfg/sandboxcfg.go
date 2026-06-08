// Package sandboxcfg renders one sandbox's config: the SANDBOX_CONFIG YAML plus
// the restore ref and connect forwards. Nothing is written to disk — the
// orchestrator serves these to sandbox-ctl over the config-socket at startup
// (see internal/configsock). The manifest key is delivered there too, separately.
//
// Schema tracks sandbox-runtime pkg/sandbox/config.go (SandboxConfig).
package sandboxcfg

import (
	"os"
	"strings"

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
	Nexthop          string            // network.nexthop: default-route gateway; "" = no default route
	Hostname         string            // network.hostname: guest hostname (sethostname + /etc/hosts entry)
	DNS              []string          // /etc/resolv.conf nameservers injected via files:
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
	var launchUser string // bare: empty -> sandbox-init falls back to image config User else root
	if p.Template.Profile == types.ProfileE2B {
		runtime = p.RuntimeE2B
		// envd is injected at /opt/sandbox-runtime/bin/envd, which sandbox-init
		// auto-bind-mounts into the guest at the same path (see build.Runtime).
		launchExec = "/opt/sandbox-runtime/bin/envd"
		launchArgs = []string{"-isnotfc", "-port", "49983"}
		// envd is e2b infrastructure: it must run as root so it can setuid into
		// the image's user when running workload commands. Without this, an image
		// that sets Config.User (e.g. e2bdev/code-interpreter = user/1000) would
		// launch envd non-root -> it lacks CAP_SETGID/SETUID -> every exec fails
		// with EPERM ("fork/exec /bin/sh: operation not permitted"). The workload
		// still runs as the image's user: the exec request carries the target user.
		launchUser = "0:0"
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
		if p.Nexthop != "" {
			// Default route via the inner-CIDR gateway (the reserved .1). The vswitch
			// ARP-proxies it and extracts all off-subnet traffic, so this is what lets
			// the guest reply to off-subnet sources (proxy/mgmt floatingip path) and
			// egress to the internet (host NAT). Without it the guest can only reach
			// its own /16 and every host->floatingip:port connection times out.
			network["nexthop"] = p.Nexthop
		}
	}
	if p.Hostname != "" {
		network["hostname"] = p.Hostname
	}
	// Provision /etc/hosts + /etc/resolv.conf into the guest via the files: mechanism
	// (tmpfs+bind; applied at launch AND restore). Flattened docker images ship neither
	// (docker injects them only at container runtime); without /etc/hosts the guest's
	// getfqdn(hostname) stalls ~20s on DNS, breaking servers that resolve the hostname
	// at startup (e.g. Python http.server.server_bind). See docs/orchestrator.md §10.
	files := guestFiles(p.Hostname, p.DNS)

	launch := map[string]any{
		"exec":    launchExec,
		"args":    launchArgs,
		"env":     p.EnvVars,
		"restart": restartPolicy(p.Template.Profile),
	}
	if launchUser != "" {
		launch["user"] = launchUser
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
		"launch": launch,
	}
	if len(files) > 0 {
		doc["files"] = files
	}
	return yaml.Marshal(doc)
}

// guestFiles builds the files: entries provisioning /etc/hosts (so getfqdn(hostname)
// resolves locally instead of stalling on DNS) and /etc/resolv.conf into the guest.
func guestFiles(hostname string, dns []string) []map[string]any {
	var files []map[string]any
	if hostname != "" {
		hosts := "127.0.0.1\tlocalhost\n127.0.1.1\t" + hostname + "\n::1\tlocalhost ip6-localhost ip6-loopback\n"
		files = append(files, map[string]any{"path": "/etc/hosts", "content": hosts, "mode": "0644"})
	}
	if len(dns) > 0 {
		var b strings.Builder
		for _, ns := range dns {
			b.WriteString("nameserver " + ns + "\n")
		}
		files = append(files, map[string]any{"path": "/etc/resolv.conf", "content": b.String(), "mode": "0644"})
	}
	return files
}

func restartPolicy(pr types.Profile) string {
	// e2b: envd is infrastructure (lifetime is TTL/kill-driven). bare default:
	// long-lived service. Both "always"; run-to-completion ("never") is opt-in.
	return "always"
}

// RestoreRef is the snapshot ref sandbox-ctl should restore from, or "" for a
// cold boot. A resumed sandbox — regardless of its template kind — restores from
// its latest pause snapshot; otherwise a snp template cold-starts by restoring its
// build snapshot, and an img template cold-boots.
func (p Params) RestoreRef() string {
	// Resume: a paused sandbox (img OR snp) has a pause snapshot to restore. This
	// MUST be checked before the kind, else a paused img sandbox cold-boots and
	// loses all guest state written since boot.
	if ref := p.Sandbox.SnapshotRef; ref != "" {
		// SnapshotRef is a full ref: "manifest://<key>" (remote checkpoint) or a
		// local bundle path (local checkpoint). A scheme-less, non-path value is
		// an older bare manifest key — treat it as manifest:// (back-compat).
		if strings.Contains(ref, "://") || strings.HasPrefix(ref, "/") {
			return ref
		}
		return "manifest://" + ref
	}
	// snp template cold-start = restore the build snapshot.
	if p.Template.Kind == types.KindSnp {
		return p.Template.ManifestRef()
	}
	// img template cold boot.
	return ""
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
