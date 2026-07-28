// Package sandboxcfg renders one sandbox's config: the SANDBOX_CONFIG YAML plus
// the restore ref and connect forwards. Nothing is written to disk by the caller
// path that matters — the orchestrator serves these to sandbox-ctl over the
// config-socket at startup (see internal/configsock). The manifest key is delivered
// there too, separately.
//
// The config IS sandbox-runtime's config.SandboxConfig (imported, single source of
// truth — no hand-rolled schema to drift). The orchestrator builds the node-managed
// base (boot / tapfd / control / capacity / resolved network) and overlays the
// tenant-controllable SandboxSpec parsed from the kuasar-sandbox.<ns> metadata keys.
// Deep validation stays in sandbox-ctl (which has the complete picture incl. the
// --cgroup-adopt cgroup path + the snapshot); here we only format-check tenant input.
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

// ResourceSpec is the tenant resource override. Capacity is honored only for img
// templates; snp restore pins capacity to the snapshot (the runtime refuses a
// mismatch), so the caller drops a snp capacity override.
type ResourceSpec struct {
	Capacity    *rtconfig.CapacityConfig    `json:"capacity,omitempty" yaml:"capacity,omitempty"`
	Allocatable *rtconfig.AllocatableConfig `json:"allocatable,omitempty" yaml:"allocatable,omitempty"`
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
	Resource ResourceSpec
	Network  NetworkSpec
	Restore  RestoreSpec
	Launch   *rtconfig.LaunchConfig // bare profile only; e2b rejects (envd owns launch)
	Init     []rtconfig.InitConfig
	Mounts   []rtconfig.MountConfig
	Files    []rtconfig.FileConfig
	Metadata map[string]string // -> SANDBOX_CONFIG.metadata passthrough (e.g. e2b.start_cmd)
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
	if err := dec(NsResource, &s.Resource); err != nil {
		return s, err
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
		s.Launch = &l
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

// capacityJSON is the canonical snake_case JSON shape for the resource capacity,
// matching the config yaml tags (rtconfig.CapacityConfig has no json tags). Used to
// emit a well-formed kuasar-sandbox.resource value from register/trigger cpu/memory.
type capacityJSON struct {
	CPU    int    `json:"cpu,omitempty"`
	Memory string `json:"memory,omitempty"`
}

// SetCapacity folds an e2b register/trigger cpuCount/memoryMB into meta as the
// resource namespace capacity (it wins over a resource header — the capacity-only
// resource overwrites any prior resource value). Zero cpu and memory => no-op.
// memoryMiB is treated as MiB (the config size unit). Returns the merged map.
func SetCapacity(meta map[string]string, cpu, memoryMiB int) map[string]string {
	if cpu <= 0 && memoryMiB <= 0 {
		return meta
	}
	var cap capacityJSON
	if cpu > 0 {
		cap.CPU = cpu
	}
	if memoryMiB > 0 {
		cap.Memory = fmt.Sprintf("%dMiB", memoryMiB)
	}
	b, err := json.Marshal(struct {
		Capacity capacityJSON `json:"capacity"`
	}{cap})
	if err != nil {
		return meta
	}
	if meta == nil {
		meta = map[string]string{}
	}
	meta[NsResource] = string(b)
	return meta
}

// MergeMetadata overlays over onto base per key (over wins) — used to layer a
// create's config namespaces over a template's. Returns nil when both are empty.
func MergeMetadata(base, over map[string]string) map[string]string {
	return mergeStr(base, over)
}

// MergeCreateMetadata layers request configuration over template, group, or
// placement defaults while keeping restore policy and credentials request-scoped.
// Only namespaces explicitly present in request are admitted for those values.
func MergeCreateMetadata(defaults, request map[string]string) map[string]string {
	out := mergeStr(defaults, request)
	for _, ns := range []string{NsRestore, NsCredentials} {
		delete(out, ns)
		if raw, ok := request[ns]; ok {
			if out == nil {
				out = map[string]string{}
			}
			out[ns] = raw
		}
	}
	return out
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

// IsZero reports whether the tenant supplied no network fields.
func (n NetworkSpec) IsZero() bool {
	return n.Hostname == "" && len(n.DNS) == 0 && n.InnerIP == "" && n.Nexthop == "" &&
		n.TransitGatewayIP == "" && n.TransitGeneveVNI == 0 && n.TransitMAC == ""
}

// Params is everything needed to render one sandbox's config: the node-managed base
// inputs plus the resolved logical network and the parsed tenant Spec.
type Params struct {
	Sandbox          *types.Sandbox
	Template         types.TemplateID
	Runtime          string // erofs path (file path, no scheme)
	Kernel           string // vmlinux path
	OverlayDiffTpl   string // pre-formatted ext4 seeding the cold-boot overlay upper (file path)
	TapFD            TapFD
	EnvVars          map[string]string // create-time launch env
	VCPU             int               // resources.capacity.cpu (already resolved: snapshot-pinned for restore)
	Memory           string            // resources.capacity.memory
	ControllerSocket string            // resources.control.controller (sentinel UDS; "" = static cgroup)
	Network          NetworkSpec       // RESOLVED logical network (create ?? snapshot ?? defaults)
	MMDSEnabled      bool
	Spec             SandboxSpec // parsed tenant override (launch[bare]/init/mounts/files/metadata/resource)
}

// WriteYAML renders the SANDBOX_CONFIG and writes it to path (0600).
func (p Params) WriteYAML(path string) error {
	b, err := p.BuildYAML()
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o600)
}

// BuildYAML renders the SANDBOX_CONFIG document from a config.SandboxConfig.
func (p Params) BuildYAML() ([]byte, error) {
	c, err := p.build()
	if err != nil {
		return nil, err
	}
	return yaml.Marshal(c)
}

func (p Params) build() (*rtconfig.SandboxConfig, error) {
	c := &rtconfig.SandboxConfig{}

	// --- resources ---
	c.Resources.Capacity = rtconfig.CapacityConfig{CPU: p.VCPU, Memory: p.Memory}
	// Capacity override applies only to img cold boot; snp restore pins to the
	// snapshot (the caller already drops a snp override before reaching here).
	if p.Template.Kind == types.KindImg && p.Spec.Resource.Capacity != nil {
		if cap := p.Spec.Resource.Capacity; cap.CPU > 0 {
			c.Resources.Capacity.CPU = cap.CPU
		}
		if p.Spec.Resource.Capacity.Memory != "" {
			c.Resources.Capacity.Memory = p.Spec.Resource.Capacity.Memory
		}
	}
	c.Resources.Allocatable = rtconfig.AllocatableConfig{CPU: float64(c.Resources.Capacity.CPU), Memory: c.Resources.Capacity.Memory}
	if a := p.Spec.Resource.Allocatable; a != nil {
		if a.CPU > 0 {
			c.Resources.Allocatable.CPU = a.CPU
		}
		if a.Memory != "" {
			c.Resources.Allocatable.Memory = a.Memory
		}
	}
	if p.ControllerSocket != "" {
		// cgroup_path + adopt are resolved by sandbox-ctl --cgroup-adopt at runtime.
		c.Resources.Control.Controller = p.ControllerSocket
	}

	// --- boot ---
	c.Boot.Kernel = "file://" + p.Kernel
	c.Boot.Runtime = "file://" + p.Runtime
	c.Boot.Root.Overlay = &rtconfig.OverlayConfig{}
	// boot.root.base is the read-only rootfs. Cold boot (img) = the flattened image
	// manifest; restore (snp/resume) lets snapshot.cfg fill it (omit).
	if p.Template.Kind == types.KindImg {
		c.Boot.Root.Base = p.Template.ManifestRef()
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
	tapFD := p.TapFD.runtime()
	c.Network.TapFD = &tapFD
	if p.Sandbox.PortMAC != "" {
		c.Network.MAC = p.Sandbox.PortMAC
	}
	if p.Sandbox.InnerIP != "" {
		c.Network.IP = p.Sandbox.InnerIP // CIDR form
		// Default route via the inner-CIDR gateway; the vswitch ARP-proxies it and
		// extracts off-subnet traffic (proxy/mgmt floatingip + internet egress).
		if p.Network.Nexthop != "" {
			c.Network.Nexthop = p.Network.Nexthop
		}
	}
	if p.Network.Hostname != "" {
		c.Network.Hostname = p.Network.Hostname
	}

	// --- launch ---
	if err := p.buildLaunch(c); err != nil {
		return nil, err
	}

	// --- init / mounts (tenant pass-through) ---
	c.Init = p.Spec.Init
	c.Mounts = p.Spec.Mounts

	// --- files: orchestrator-derived (/etc/hosts, /etc/resolv.conf) + tenant files ---
	// (tmpfs+bind, re-applied at launch AND restore; flattened images ship neither,
	// so without /etc/hosts getfqdn(hostname) stalls ~20s on DNS.)
	c.Files = append(guestFiles(p.Network.Hostname, p.Network.DNS), p.Spec.Files...)

	// --- metadata: tenant passthrough + the resolved logical network ---
	c.Metadata = p.buildMetadata()
	return c, nil
}

// buildLaunch fills launch. e2b is envd-owned (a tenant launch override is rejected);
// bare uses the image entrypoint by default, overlaid by the tenant launch spec.
func (p Params) buildLaunch(c *rtconfig.SandboxConfig) error {
	if p.Template.Profile == types.ProfileE2B {
		if p.Spec.Launch != nil {
			return fmt.Errorf("sandboxcfg: launch override is not allowed for the e2b profile (envd owns launch)")
		}
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
			Restart: "always", User: "0:0", PIDNamespace: "shared",
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
		if len(s.Plugin) > 0 {
			c.Launch.Plugin = s.Plugin
		}
		c.Launch.Env = mergeStr(p.EnvVars, s.Env) // tenant launch env overrides create env
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

// RestoreRefFor is the snapshot ref sandbox-ctl should restore from, or "" for a
// cold boot. A resumed sandbox (img OR snp) restores from its latest pause snapshot;
// otherwise a snp template cold-starts by restoring its build snapshot, and an img
// template cold-boots. Exported so the orchestrator can resolve it before rendering
// (to read the snapshot's inherited config).
func RestoreRefFor(sb *types.Sandbox, tmpl types.TemplateID) string {
	if ref := sb.SnapshotRef; ref != "" {
		// SnapshotRef is a full ref: "manifest://<key>" or a local bundle path. A
		// scheme-less, non-path value is an older bare manifest key (back-compat).
		if strings.Contains(ref, "://") || strings.HasPrefix(ref, "/") {
			return ref
		}
		return "manifest://" + ref
	}
	if tmpl.Kind == types.KindSnp {
		return tmpl.ManifestRef()
	}
	return ""
}

// RestoreRef is RestoreRefFor for this Params.
func (p Params) RestoreRef() string { return RestoreRefFor(p.Sandbox, p.Template) }

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
