package builder

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
)

// --- yaml renderers -----------------------------------------------------------

func (p *buildPipeline) networkDoc() map[string]any {
	n := p.spec.Net
	doc := map[string]any{
		"tapfd":    tapFDDoc(n.TapFD),
		"hostname": n.Hostname,
	}
	if n.MAC != "" {
		doc["mac"] = n.MAC
	}
	if n.InnerIP != "" {
		doc["ip"] = n.InnerIP
		if n.Nexthop != "" {
			doc["nexthop"] = n.Nexthop
		}
	}
	return doc
}

func tapFDDoc(t configsock.TapFDConfig) map[string]any {
	doc := map[string]any{}
	if len(t.Exec) > 0 {
		doc["exec"] = t.Exec
	}
	if t.Socket != "" {
		doc["socket"] = t.Socket
	}
	if t.Request != "" {
		doc["request"] = t.Request
	}
	if t.Timeout != "" {
		doc["timeout"] = t.Timeout
	}
	return doc
}

func (p *buildPipeline) resourcesDoc() map[string]any {
	return map[string]any{
		"capacity":    map[string]any{"cpu": p.spec.VCPU, "memory": p.spec.Memory},
		"allocatable": map[string]any{"cpu": float64(p.spec.VCPU), "memory": p.spec.Memory},
	}
}

func (p *buildPipeline) dnsFiles() []map[string]any {
	if len(p.spec.Net.DNS) == 0 {
		return nil
	}
	var b strings.Builder
	for _, ns := range p.spec.Net.DNS {
		b.WriteString("nameserver " + ns + "\n")
	}
	return []map[string]any{{"path": "/etc/resolv.conf", "content": b.String(), "mode": "0644"}}
}

// flattenConfigFiles projects the per-build registry CA bundle (inline PEM)
// and a generated flatten-ctl config YAML into the Phase A import sandbox when
// a registry TLS policy is configured. flatten-ctl reads the config via
// --config (tls.ca_cert points at the projected CA bundle). Files land under
// /run (tmpfs, never on the build root disk) and are read-only (0444).
// Returns (nil, nil) when no TLS policy is set. A generation error is returned
// rather than silently degrading to "no TLS config".
func (p *buildPipeline) flattenConfigFiles() ([]map[string]any, error) {
	tls := p.spec.RegistryTLS
	if tls == nil {
		return nil, nil
	}
	var files []map[string]any

	// Project the CA bundle (if configured). Content is inline PEM carried in
	// the BuildSpec — no host file is read here.
	if tls.CABundlePEM != "" {
		files = append(files, map[string]any{
			"path":      guestCACert,
			"content":   tls.CABundlePEM,
			"mode":      "0444",
			"read_only": true,
		})
	}

	// Generate and project the flatten-ctl config YAML.
	tlsCfg := map[string]any{}
	if tls.CABundlePEM != "" {
		tlsCfg["ca_cert"] = guestCACert
	}
	if tls.InsecureSkipVerify {
		tlsCfg["insecure_skip_verify"] = true
	}
	b, err := yaml.Marshal(map[string]any{"tls": tlsCfg})
	if err != nil {
		return nil, fmt.Errorf("marshal flatten-ctl tls config: %w", err)
	}
	files = append(files, map[string]any{
		"path":      guestFlattenCfg,
		"content":   string(b),
		"mode":      "0444",
		"read_only": true,
	})
	return files, nil
}

// importYAML: an EMPTY single-disk sandbox (no base image at all). The writable
// ext4 root doubles as the pull scratch. launch.placeholder anchors it; the toolchain rides the
// /opt/sandbox-runtime projection.
func (p *buildPipeline) importYAML() (map[string]any, error) {
	s := p.spec
	doc := map[string]any{
		"resources": p.resourcesDoc(),
		"network":   p.networkDoc(),
		"boot": map[string]any{
			"kernel":  "file://" + s.Paths.Kernel,
			"runtime": "file://" + s.Paths.Runtime,
			"root": map[string]any{
				"diff_template": "file://" + s.Paths.BuilderDiffTpl,
			},
		},
		"launch": map[string]any{"placeholder": true},
	}
	files := p.dnsFiles()
	tlsFiles, err := p.flattenConfigFiles()
	if err != nil {
		return nil, err
	}
	files = append(files, tlsFiles...)
	if len(files) > 0 {
		doc["files"] = files
	}
	return doc, nil
}

// rootDoc renders boot.root for the steps/template phases: the base image plus
// a writable overlay seeded from diffTpl. When building fromTemplate, the
// template's accumulated overlay (p.overlayBase) is stacked read-only beneath
// the fresh writable layer, so the build sees the template's filesystem.
func (p *buildPipeline) rootDoc(diffTpl string) map[string]any {
	overlay := map[string]any{"diff_template": diffTpl}
	if p.overlayBase != "" {
		overlay["base"] = p.overlayBase
	}
	if len(p.overlayBaseFromRefs) > 0 {
		overlay["base_from_refs"] = p.overlayBaseFromRefs
	}
	return map[string]any{"base": p.baseRef, "overlay": overlay}
}

// stepsYAML: the base image as root, a big writable upper (steps delta +
// export scratch), the runtime-projected toolchain, and envd as
// the app — RUN steps go through the e2b exec channel. This envd is a
// build tool, not a tenant data plane: always -isnotfc, never
// /init-armed (its in-memory state dies with the phase; its only disk
// footprint, /run/e2b, sits on a tmpfs mount the export skips).
func (p *buildPipeline) stepsYAML() map[string]any {
	s := p.spec
	doc := map[string]any{
		"resources": p.resourcesDoc(),
		"network":   p.networkDoc(),
		"boot": map[string]any{
			"kernel":  "file://" + s.Paths.Kernel,
			"runtime": "file://" + s.Paths.Runtime,
			"root":    p.rootDoc("file://" + s.Paths.BuilderDiffTpl),
		},
		"launch": map[string]any{
			"exec":           "/opt/sandbox-runtime/bin/envd",
			"args":           []string{"-isnotfc", "-port", "49983"},
			"user":           "0:0",
			"restart":        "always",
			"cgroup_control": true,
			// Share sandbox-init's PID namespace (as production does) so orphaned
			// descendants of RUN steps are reaped by PID 1 rather than zombie-ing
			// under envd during the build.
			"pid_namespace": "shared",
		},
	}
	if f := p.dnsFiles(); f != nil {
		doc["files"] = f
	}
	return doc
}

// templateYAML: a PRODUCTION e2b sandbox — the runtime is frozen into the
// snapshot as runtime_ref, envd as the app (FC mode per the
// deployment's MMDS posture), and the e2b start/ready metadata recorded
// into snapshot.cfg so the template is self-describing.
func (p *buildPipeline) templateYAML() (map[string]any, error) {
	s := p.spec
	envdArgs := []string{"-isnotfc", "-port", "49983"}
	if s.MMDSEnabled {
		envdArgs = []string{"-port", "49983"}
	}
	meta := map[string]string{"e2b.start_cmd": p.startCmd}
	if p.readyCmd != "" {
		meta["e2b.ready_cmd"] = p.readyCmd
	}
	networkJSON, err := json.Marshal(s.TemplateNetwork)
	if err != nil {
		return nil, fmt.Errorf("marshal template network metadata: %w", err)
	}
	meta[sandboxcfg.NsNetwork] = string(networkJSON)
	doc := map[string]any{
		"resources": p.resourcesDoc(),
		"network":   p.networkDoc(),
		"metadata":  meta,
		"boot": map[string]any{
			"kernel":  "file://" + s.Paths.Kernel,
			"runtime": "file://" + s.Paths.Runtime,
			"root":    p.rootDoc("file://" + s.Paths.OverlayDiffTpl),
		},
		"launch": map[string]any{
			"exec":           "/opt/sandbox-runtime/bin/envd",
			"args":           envdArgs,
			"user":           "0:0",
			"restart":        "always",
			"cgroup_control": true,
			// Production e2b posture (matches sandboxcfg's launch config): envd is
			// not a PID-1-style reaper, so share sandbox-init's PID namespace —
			// PID 1 reaps orphaned descendants of guest commands instead of them
			// piling up as zombies under envd. The snapshot freezes this, so every
			// sandbox created from the template inherits the reaper invariant.
			"pid_namespace": "shared",
		},
	}
	if f := p.dnsFiles(); f != nil {
		doc["files"] = f
	}
	return doc, nil
}

// --- helpers -------------------------------------------------------------------

// hostCmdEnv runs a host-side tool with the BuildSpec env (MANIFEST_KEY +
// tenant registry creds) layered over the unit environment — every host
// invocation gets it, since any of them may resolve manifest:// refs.
func (p *buildPipeline) hostCmdEnv(env map[string]string, bin string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(p.ctx, bin, args...)
	cmd.Env = os.Environ()
	for k, v := range env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	if err := cmd.Run(); err != nil {
		return errb.Bytes(), fmt.Errorf("%s %s: %w", filepath.Base(bin), args[0], err)
	}
	return out.Bytes(), nil
}

func firstLine(b []byte) string {
	s := strings.TrimSpace(string(b))
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		s = s[:i]
	}
	if len(s) > 200 {
		s = s[:200]
	}
	return s
}

func shortBID(s string) string {
	if len(s) > 8 {
		return s[:8]
	}
	return s
}
