package sandboxcfg

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
)

func TestParseSpecNamespaces(t *testing.T) {
	// Absent / unrelated metadata => zero value, no error.
	if s, err := ParseSpec(nil); err != nil || !s.Network.IsZero() || s.Launch != nil {
		t.Fatalf("absent: %+v %v", s, err)
	}
	if s, err := ParseSpec(map[string]string{"foo": "bar"}); err != nil || !s.Network.IsZero() {
		t.Fatalf("unrelated key: %+v %v", s, err)
	}

	meta := map[string]string{
		NsNetwork:  `{"hostname":"h1","dns":["1.1.1.1","8.8.8.8"],"inner_ip":"10.0.0.5/30","nexthop":"10.0.0.4","transit_gateway_ip":"172.16.0.1","transit_geneve_vni":4242,"transit_mac":"02:00:00:00:00:09"}`,
		NsResource: `{"capacity":{"cpu":4,"memory":"8GiB"}}`,
		NsRestore:  `{"prefetch":"memory"}`,
		NsLaunch:   `{"exec":"/app","args":["-x"],"restart":"always","stop_signal":"SIGINT","user":"1000:1000","cgroup_control":true}`,
		NsMounts:   `[{"target":"/data","type":"tmpfs"}]`,
		NsFiles:    `[{"path":"/etc/app.conf","content":"k=v","mode":"0644"}]`,
		NsInit:     `[{"exec":"/bin/setup","args":["--once"]}]`,
		NsMetadata: `{"e2b.start_cmd":"npm run start"}`,
	}
	s, err := ParseSpec(meta)
	if err != nil {
		t.Fatalf("valid: %v", err)
	}
	if s.Network.Hostname != "h1" || len(s.Network.DNS) != 2 || s.Network.InnerIP != "10.0.0.5/30" ||
		s.Network.Nexthop != "10.0.0.4" || s.Network.TransitGatewayIP != "172.16.0.1" ||
		s.Network.TransitGeneveVNI != 4242 || s.Network.TransitMAC != "02:00:00:00:00:09" {
		t.Fatalf("network parsed wrong: %+v", s.Network)
	}
	if s.Resource.Capacity == nil || s.Resource.Capacity.CPU == nil || *s.Resource.Capacity.CPU != 4 ||
		s.Resource.Capacity.Memory == nil || *s.Resource.Capacity.Memory != "8GiB" {
		t.Fatalf("resource parsed wrong: %+v", s.Resource)
	}
	if s.Restore.Prefetch != "memory" {
		t.Fatalf("restore parsed wrong: %+v", s.Restore)
	}
	// stop_signal (snake_case) must bind via the runtime config's yaml tags.
	if s.Launch == nil || s.Launch.Exec != "/app" || s.Launch.StopSignal != "SIGINT" ||
		s.Launch.User != "1000:1000" || !s.Launch.CgroupControl ||
		s.LaunchCgroupControl == nil || !*s.LaunchCgroupControl {
		t.Fatalf("launch parsed wrong: %+v", s.Launch)
	}
	omitted, err := ParseSpec(map[string]string{NsLaunch: `{"exec":"/new-app"}`})
	if err != nil || omitted.LaunchCgroupControl != nil {
		t.Fatalf("omitted launch.cgroup_control presence = %v, %v", omitted.LaunchCgroupControl, err)
	}
	explicitFalse, err := ParseSpec(map[string]string{NsLaunch: `{"cgroup_control":false}`})
	if err != nil || explicitFalse.LaunchCgroupControl == nil || *explicitFalse.LaunchCgroupControl {
		t.Fatalf("explicit false launch.cgroup_control presence = %v, %v", explicitFalse.LaunchCgroupControl, err)
	}
	if len(s.Mounts) != 1 || s.Mounts[0].Target != "/data" || s.Mounts[0].Type != "tmpfs" {
		t.Fatalf("mounts parsed wrong: %+v", s.Mounts)
	}
	if len(s.Files) != 1 || s.Files[0].Path != "/etc/app.conf" {
		t.Fatalf("files parsed wrong: %+v", s.Files)
	}
	if len(s.Init) != 1 || s.Init[0].Exec != "/bin/setup" {
		t.Fatalf("init parsed wrong: %+v", s.Init)
	}
	if s.Metadata["e2b.start_cmd"] != "npm run start" {
		t.Fatalf("metadata parsed wrong: %+v", s.Metadata)
	}
}

func TestNormalizeRestoreMetadata(t *testing.T) {
	if got, err := NormalizeRestoreMetadata(nil); err != nil || got != nil {
		t.Fatalf("absent restore: got=%v err=%v", got, err)
	}
	original := map[string]string{NsRestore: " { \"prefetch\" : \"memory\" } ", "keep": "value"}
	got, err := NormalizeRestoreMetadata(original)
	if err != nil {
		t.Fatal(err)
	}
	if got[NsRestore] != `{"prefetch":"memory"}` || got["keep"] != "value" {
		t.Fatalf("normalized metadata = %+v", got)
	}
	if original[NsRestore] == got[NsRestore] {
		t.Fatal("normalization mutated or reused the caller's restore value")
	}
	if got, err := NormalizeRestoreMetadata(map[string]string{NsRestore: `{}`}); err != nil || got[NsRestore] != `{}` {
		t.Fatalf("missing prefetch: got=%v err=%v", got, err)
	}

	bad := map[string]string{
		"empty":           ``,
		"null-object":     `null`,
		"array":           `[]`,
		"invalid-json":    `{`,
		"trailing":        `{"prefetch":"memory"} {}`,
		"unknown":         `{"other":true}`,
		"unknown-field":   `{"unknown":true}`,
		"wrong-case":      `{"Prefetch":"memory"}`,
		"bad-enum":        `{"prefetch":"disk"}`,
		"empty-enum":      `{"prefetch":""}`,
		"non-string":      `{"prefetch":true}`,
		"null-prefetch":   `{"prefetch":null}`,
		"object-prefetch": `{"prefetch":{}}`,
	}
	for name, raw := range bad {
		t.Run(name, func(t *testing.T) {
			if _, err := NormalizeRestoreMetadata(map[string]string{NsRestore: raw}); err == nil {
				t.Fatalf("accepted invalid restore value %q", raw)
			}
		})
	}
}

func TestParseSpecNetworkValidation(t *testing.T) {
	for name, bad := range map[string]string{
		"bad-json":    `{not json`,
		"bad-cidr":    `{"inner_ip":"10.0.0.5"}`, // a plain IP, not a CIDR
		"bad-nexthop": `{"nexthop":"nope"}`,
		"bad-gateway": `{"transit_gateway_ip":"999.1.1.1"}`,
		"bad-mac":     `{"transit_mac":"zz:zz"}`,
	} {
		if _, err := ParseSpec(map[string]string{NsNetwork: bad}); err == nil {
			t.Errorf("%s: expected a validation error", name)
		}
	}
}

func baseParams(profile types.Profile) Params {
	tmpl := types.TemplateID{Profile: profile, Kind: types.KindImg, Ref: "manifest://" + strings.Repeat("a", 64)}
	return Params{
		Sandbox: &types.Sandbox{
			ID: "s1", TemplateID: tmpl.String(), LaunchMode: types.LaunchImage,
			InnerIP: "10.0.0.5/30", PortMAC: "02:00:00:00:00:01",
		},
		Template: tmpl, Runtime: "/r/sandbox-runtime.bundle", Kernel: "/r/vmlinux",
		Resources: rtconfig.ResourcesConfig{
			Capacity:    rtconfig.CapacityConfig{CPU: 2, Memory: "2GiB"},
			Allocatable: rtconfig.AllocatableConfig{CPU: 2, Memory: "256MiB", DeflateOnOOM: boolPtr(true)},
			Overhead:    &rtconfig.OverheadConfig{Memory: "32MiB"},
		},
	}
}

func TestBuildE2BForbidsLaunch(t *testing.T) {
	p := baseParams(types.ProfileE2B)
	p.Spec.Launch = &rtconfig.LaunchConfig{Exec: "/evil"}
	if _, err := p.BuildYAML(); err == nil {
		t.Fatal("e2b profile must reject a launch override (envd owns launch)")
	}
	// bare accepts it.
	pb := baseParams(types.ProfileBare)
	pb.Spec.Launch = &rtconfig.LaunchConfig{Exec: "/app"}
	if _, err := pb.BuildYAML(); err != nil {
		t.Fatalf("bare launch override should be accepted: %v", err)
	}
}

func TestMarshalBuildColdConfigClearsOptionalSourceFields(t *testing.T) {
	const digestA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const digestB = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	deflate := true
	source := &rtconfig.PortableSandboxConfig{
		Version: rtconfig.PortableSandboxConfigVersion,
		Resources: rtconfig.PortableResourcesConfig{
			Capacity: rtconfig.CapacityConfig{CPU: 2, Memory: "2GiB"},
			Allocatable: rtconfig.AllocatableConfig{
				CPU: 2, Memory: "2GiB", DeflateOnOOM: &deflate,
			},
		},
		Network: rtconfig.PortableNetworkConfig{Enabled: true, Interface: "eth0"},
		Boot: rtconfig.PortableBootConfig{
			Kernel:  "file://old-vmlinux@digest:" + digestA,
			Runtime: "file://old-runtime@digest:" + digestB,
			Root: rtconfig.PortableRootConfig{
				Base:    "file://old-root@digest:" + digestA,
				Overlay: &rtconfig.PortableOverlayConfig{Base: "self"},
			},
		},
		Launch: rtconfig.PortableLaunchConfig{
			Env: map[string]string{"OLD": "value"}, Restart: "always",
			Placeholder: true, PIDNamespace: "private",
			Plugin: []rtconfig.PluginConfig{{Exec: "/old/plugin"}},
			User:   "1000:1000", StopSignal: "SIGINT", StopGracePeriod: "30s",
		},
		Mounts:   []rtconfig.MountConfig{{Target: "/old", Type: "tmpfs"}},
		Files:    []rtconfig.FileConfig{{Path: "/old", Content: "old"}},
		Init:     []rtconfig.InitConfig{{Exec: "/old/init"}},
		Metadata: map[string]string{"old": "value"},
	}
	if err := source.Validate(); err != nil {
		t.Fatal(err)
	}
	host := &rtconfig.SandboxConfig{
		Resources: rtconfig.ResourcesConfig{
			Capacity:    rtconfig.CapacityConfig{CPU: 2, Memory: "2GiB"},
			Allocatable: rtconfig.AllocatableConfig{CPU: 2, Memory: "2GiB"},
		},
		Network: rtconfig.NetworkConfig{
			TAP: "tap-build", IP: "192.0.2.2/24", Interface: "eth0",
		},
		Boot: rtconfig.BootConfig{
			Kernel: "file:///new-vmlinux", Runtime: "file:///new-runtime",
			Root: rtconfig.RootConfig{
				Base:    "manifest://" + digestA,
				Overlay: &rtconfig.OverlayConfig{DiffTemplate: "file:///new-diff"},
			},
		},
		Launch: rtconfig.LaunchConfig{
			Exec: "/opt/sandbox-runtime/bin/envd", Args: []string{"-port", "49983"},
			Env: map[string]string{"NEW": "value"}, Restart: "always",
			CgroupControl: true, PIDNamespace: "shared",
		},
	}
	document, err := MarshalBuildColdConfig(host)
	if err != nil {
		t.Fatal(err)
	}
	loaded, presence, err := rtconfig.LoadConfigBytesWithPresence(document)
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{
		"launch.cgroup_control", "launch.placeholder", "launch.pid_namespace",
		"launch.plugin", "launch.user", "launch.stop_signal",
		"launch.stop_grace_period", "resources.allocatable.deflate_on_oom",
		"mounts", "files", "init", "metadata",
	} {
		if !presence.Has(path) {
			t.Errorf("replacement document omitted %s:\n%s", path, document)
		}
	}
	runtime, c0, err := rtconfig.ApplyFromRules(
		source, loaded, presence, rtconfig.ApplyFromOptions{ReplaceBoot: true},
	)
	if err != nil {
		t.Fatal(err)
	}
	if c0 != nil {
		t.Fatalf("replacement retained a source-derived C0: %#v", c0)
	}
	if runtime.Launch.Placeholder || len(runtime.Launch.Plugin) != 0 ||
		runtime.Launch.User != "" || runtime.Launch.StopSignal != "" ||
		runtime.Launch.StopGracePeriod != "" {
		t.Fatalf("optional source launch fields survived replacement: %#v", runtime.Launch)
	}
	if runtime.Launch.PIDNamespace != "shared" || !runtime.Launch.CgroupControl {
		t.Fatalf("replacement launch policy was not applied: %#v", runtime.Launch)
	}
	if runtime.Resources.Allocatable.DeflateOnOOM != nil {
		t.Fatalf("source deflate_on_oom survived explicit null: %#v", runtime.Resources.Allocatable)
	}
	if len(runtime.Mounts) != 0 || len(runtime.Files) != 0 || len(runtime.Init) != 0 ||
		len(runtime.Metadata) != 0 {
		t.Fatalf("source collections survived replacement: %#v", runtime)
	}
}

func TestMergeBareBuildLaunchPreservesOmittedCgroupControl(t *testing.T) {
	dst := rtconfig.LaunchConfig{Exec: "/source", CgroupControl: true}
	mergeBareBuildLaunch(&dst, &rtconfig.LaunchConfig{Exec: "/registered"}, nil, nil)
	if dst.Exec != "/registered" || !dst.CgroupControl {
		t.Fatalf("partial launch override erased source boolean: %+v", dst)
	}

	explicitFalse := false
	mergeBareBuildLaunch(&dst, &rtconfig.LaunchConfig{Exec: "/registered"}, &explicitFalse, nil)
	if dst.CgroupControl {
		t.Fatalf("explicit launch.cgroup_control=false was ignored: %+v", dst)
	}
}

func TestBuildLaunchCgroupControl(t *testing.T) {
	e2b := baseParams(types.ProfileE2B)
	e2bConfig, err := e2b.buildImageColdConfig()
	if err != nil {
		t.Fatal(err)
	}
	if !e2bConfig.Launch.CgroupControl {
		t.Fatal("e2b launch.cgroup_control = false, want true")
	}
	if got := strings.Join(e2bConfig.Launch.Args, " "); got != "-isnotfc -port 49983" {
		t.Fatalf("e2b envd args = %q", got)
	}
	e2b.MMDSEnabled = true
	e2bConfig, err = e2b.buildImageColdConfig()
	if err != nil {
		t.Fatal(err)
	}
	if !e2bConfig.Launch.CgroupControl || strings.Join(e2bConfig.Launch.Args, " ") != "-port 49983" {
		t.Fatalf("MMDS e2b launch = %+v", e2bConfig.Launch)
	}

	bare := baseParams(types.ProfileBare)
	bareConfig, err := bare.buildImageColdConfig()
	if err != nil {
		t.Fatal(err)
	}
	if bareConfig.Launch.CgroupControl {
		t.Fatal("bare default launch.cgroup_control = true, want false")
	}

	bare.Spec.Launch = &rtconfig.LaunchConfig{CgroupControl: true}
	bareConfig, err = bare.buildImageColdConfig()
	if err != nil {
		t.Fatal(err)
	}
	if !bareConfig.Launch.CgroupControl {
		t.Fatal("bare explicit launch.cgroup_control = false, want true")
	}
}

func TestBuildUsesResolvedControllerIdentity(t *testing.T) {
	p := baseParams(types.ProfileE2B)
	p.OverlayDiffTpl = "/r/overlay.ext4"
	p.TapFD.Exec = []string{"/r/open-tap"}
	p.Resources.Allocatable.CPU = 1.5
	p.Resources.Control.Controller = "/real/run/controller.sock"
	p.Resources.Startup = &rtconfig.StartupConfig{Memory: "2GiB"}
	cfg, err := p.buildImageColdConfig()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Resources.Control.Controller != "/real/run/controller.sock" {
		t.Fatalf("controller socket = %q", cfg.Resources.Control.Controller)
	}
	if cfg.Resources.Control.CgroupPath != "" || cfg.Resources.Control.CgroupFD != 0 {
		t.Fatalf("renderer injected cgroup capability: %+v", cfg.Resources.Control)
	}
	if err := cfg.ValidateColdProjection(); err != nil {
		t.Fatalf("offline projection rejected runtime-only resource policy: %v", err)
	}
}

func TestBuildInjectsNetworkMetadata(t *testing.T) {
	p := baseParams(types.ProfileE2B)
	p.Network = NetworkSpec{Hostname: "h1", DNS: []string{"1.1.1.1"}, InnerIP: "10.0.0.5/30", Nexthop: "10.0.0.4"}
	p.Spec.Files = []rtconfig.FileConfig{{Path: "/etc/tenant", Content: "persistent"}}
	b, err := p.BuildYAML()
	if err != nil {
		t.Fatal(err)
	}
	out := string(b)
	// The resolved logical network rides the snapshot via metadata["kuasar-sandbox.network"].
	if !strings.Contains(out, "kuasar-sandbox.network") || !strings.Contains(out, "h1") {
		t.Fatalf("network not injected into metadata:\n%s", out)
	}
	cfg, presence, err := rtconfig.LoadConfigBytesWithPresence(b)
	if err != nil {
		t.Fatal(err)
	}
	if !presence.Has("files") || !presence.Has("ephemeral_files") || len(cfg.Files) != 1 || cfg.Files[0].Path != "/etc/tenant" {
		t.Fatalf("image file persistence split is wrong: files=%+v ephemeral=%+v\n%s", cfg.Files, cfg.EphemeralFiles, out)
	}
	if len(cfg.EphemeralFiles) != 2 || cfg.EphemeralFiles[0].Path != "/etc/hosts" || cfg.EphemeralFiles[1].Path != "/etc/resolv.conf" {
		t.Fatalf("node network files are not cold-only: %+v\n%s", cfg.EphemeralFiles, out)
	}
}

func TestBuildRendersTapFDSocket(t *testing.T) {
	p := baseParams(types.ProfileE2B)
	p.TapFD = TapFD{
		Socket:  "/run/kuasar/connector/sw0/tapfd.sock",
		Request: "VSWITCH=sw0 PORT=7",
	}
	b, err := p.BuildYAML()
	if err != nil {
		t.Fatal(err)
	}
	out := string(b)
	for _, want := range []string{
		"tapfd:",
		"socket: /run/kuasar/connector/sw0/tapfd.sock",
		"request: VSWITCH=sw0 PORT=7",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("rendered yaml missing %q:\n%s", want, out)
		}
	}
}

func TestBuildRendersPrefetchOnlyForRestore(t *testing.T) {
	cold := baseParams(types.ProfileE2B)
	cold.Spec.Restore.Prefetch = "memory"
	b, err := cold.BuildYAML()
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), "prefetch:") || strings.Contains(string(b), "restore:") {
		t.Fatalf("cold boot rendered restore policy:\n%s", b)
	}

	restore := baseParams(types.ProfileE2B)
	restore.Template.Kind = types.KindSnp
	restore.Sandbox.TemplateID = restore.Template.String()
	restore.Sandbox.LaunchMode = types.LaunchMemory
	restore.ArtifactDisks = types.ArtifactDiskTopology{
		Root: types.ArtifactDiskShape{Mode: types.ArtifactDiskOverlay, HasActiveBase: true},
	}
	restore.Spec.Restore.Prefetch = "memory"
	b, err = restore.BuildYAML()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), "restore:") || !strings.Contains(string(b), "prefetch: memory") {
		t.Fatalf("snapshot restore omitted prefetch policy:\n%s", b)
	}

	resume := baseParams(types.ProfileE2B)
	resume.Sandbox.ResumeSource = types.ResumeSource{
		Kind: types.ResumeSourceSnapshot, Ref: "manifest://" + strings.Repeat("b", 64),
	}
	resume.Sandbox.LaunchMode = types.LaunchMemory
	resume.ArtifactDisks = types.ArtifactDiskTopology{
		Root: types.ArtifactDiskShape{Mode: types.ArtifactDiskOverlay, HasActiveBase: true},
	}
	resume.Spec.Restore.Prefetch = "memory"
	b, err = resume.BuildYAML()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), "restore:") || !strings.Contains(string(b), "prefetch: memory") {
		t.Fatalf("paused image resume omitted prefetch policy:\n%s", b)
	}

	restore.Spec.Restore.Prefetch = "off"
	b, err = restore.BuildYAML()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), `prefetch: "off"`) {
		t.Fatalf("snapshot restore omitted explicit off policy:\n%s", b)
	}
}

func TestApplyCapacityRoundTrip(t *testing.T) {
	// cpu/memory fold into a well-formed resource namespace ParseSpec reads back.
	meta, err := ApplyCapacity(nil, 4, 8192)
	if err != nil {
		t.Fatal(err)
	}
	s, err := ParseSpec(meta)
	if err != nil {
		t.Fatal(err)
	}
	if s.Resource.Capacity == nil || s.Resource.Capacity.CPU == nil || *s.Resource.Capacity.CPU != 4 ||
		s.Resource.Capacity.Memory == nil || *s.Resource.Capacity.Memory != "8192MiB" {
		t.Fatalf("capacity round-trip: %+v", s.Resource.Capacity)
	}
	if got, err := ApplyCapacity(nil, 0, 0); err != nil || got != nil {
		t.Fatal("zero cpu/memory must be a no-op")
	}
}

func TestMergeNetworkExplicitWins(t *testing.T) {
	// snapshot-inherited network; create explicitly sets only hostname.
	snap := NetworkSpec{Hostname: "snap-host", Nexthop: "10.0.0.4", TransitGeneveVNI: 100, DNS: []string{"9.9.9.9"}}
	create := NetworkSpec{Hostname: "create-host"}
	m := MergeNetwork(snap, create)
	if m.Hostname != "create-host" {
		t.Fatalf("explicit create hostname must win: %q", m.Hostname)
	}
	if m.Nexthop != "10.0.0.4" || m.TransitGeneveVNI != 100 || len(m.DNS) != 1 {
		t.Fatalf("snapshot must fill fields create left unset: %+v", m)
	}
}

func TestMergeMetadataOverWins(t *testing.T) {
	base := map[string]string{NsNetwork: "from-template", NsLaunch: "tmpl-launch"}
	over := map[string]string{NsNetwork: "from-create"}
	m, err := MergeMetadata(base, over)
	if err != nil {
		t.Fatal(err)
	}
	if m[NsNetwork] != "from-create" || m[NsLaunch] != "tmpl-launch" {
		t.Fatalf("create should win per namespace, template fills the rest: %+v", m)
	}
}

func TestMergeCreateMetadataKeepsRestoreRequestScoped(t *testing.T) {
	defaults := map[string]string{
		NsNetwork: "from-defaults", NsRestore: `{"prefetch":"memory"}`,
	}
	withoutRestore, err := MergeCreateMetadata(defaults, map[string]string{NsLaunch: "from-request"})
	if err != nil {
		t.Fatal(err)
	}
	if withoutRestore[NsNetwork] != "from-defaults" || withoutRestore[NsLaunch] != "from-request" {
		t.Fatalf("normal create metadata did not merge: %+v", withoutRestore)
	}
	if _, ok := withoutRestore[NsRestore]; ok {
		t.Fatalf("restore leaked from defaults: %+v", withoutRestore)
	}
	withRestore, err := MergeCreateMetadata(defaults, map[string]string{NsRestore: `{"prefetch":"off"}`})
	if err != nil {
		t.Fatal(err)
	}
	if withRestore[NsRestore] != `{"prefetch":"off"}` {
		t.Fatalf("explicit create restore did not win: %+v", withRestore)
	}
}

func TestBuildInstallsResolvedResources(t *testing.T) {
	p := baseParams(types.ProfileBare)
	p.Resources.Capacity = rtconfig.CapacityConfig{CPU: 8, Memory: "16GiB"}
	b, err := p.BuildYAML()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), "cpu: 8") || !strings.Contains(string(b), "16GiB") {
		t.Fatalf("img capacity override not applied:\n%s", b)
	}
	for _, forbidden := range []string{"controller:", "startup:", "cgroup_path:", "watermark_high:", "sensor:"} {
		if strings.Contains(string(b), forbidden) {
			t.Fatalf("static renderer emitted %q:\n%s", forbidden, b)
		}
	}
}

func rendererPortableConfig() *rtconfig.PortableSandboxConfig {
	deflate := true
	return &rtconfig.PortableSandboxConfig{
		Version: rtconfig.PortableSandboxConfigVersion,
		Resources: rtconfig.PortableResourcesConfig{
			Capacity: rtconfig.CapacityConfig{CPU: 2, Memory: "2GiB"},
			Allocatable: rtconfig.AllocatableConfig{
				CPU: 2, Memory: "256MiB", DeflateOnOOM: &deflate,
			},
		},
		Network: rtconfig.PortableNetworkConfig{Enabled: true, Interface: "eth0"},
		Boot: rtconfig.PortableBootConfig{
			Kernel:  "file://vmlinux@digest:" + strings.Repeat("a", 64),
			Runtime: "file://runtime@digest:" + strings.Repeat("b", 64),
			Root: rtconfig.PortableRootConfig{
				Base: "manifest://" + strings.Repeat("c", 64),
				Overlay: &rtconfig.PortableOverlayConfig{
					Base: "self",
				},
			},
		},
		Launch: rtconfig.PortableLaunchConfig{
			Exec: "/artifact/app", Env: map[string]string{"ARTIFACT": "yes"},
			Workdir: "/artifact", Restart: "never",
		},
		Files:    []rtconfig.FileConfig{{Path: "/etc/artifact", Content: "artifact"}},
		Metadata: map[string]string{"owner": "artifact"},
	}
}

func artifactRendererParams(mode types.LaunchMode) Params {
	p := baseParams(types.ProfileBare)
	p.Sandbox.LaunchMode = mode
	p.Sandbox.ResumeSource = types.ResumeSource{
		Kind: types.ResumeSourceSandbox, Ref: "manifest://" + strings.Repeat("c", 64),
	}
	if mode == types.LaunchMemory {
		p.Sandbox.ResumeSource.Kind = types.ResumeSourceSnapshot
	}
	p.ArtifactDisks = types.ArtifactDiskTopology{
		Root: types.ArtifactDiskShape{Mode: types.ArtifactDiskOverlay, HasActiveBase: true},
	}
	p.OverlayDiffTpl = "/r/overlay.ext4"
	p.TapFD = TapFD{Socket: "/run/connector/tapfd.sock", Request: "VSWITCH=sw0 PORT=7"}
	p.Network = NetworkSpec{Hostname: "node-host", DNS: []string{"1.1.1.1"}, Nexthop: "10.0.0.4"}
	return p
}

func TestGeneratedSandboxHostConfigSatisfiesApplyFromRules(t *testing.T) {
	p := artifactRendererParams(types.LaunchCold)
	p.EnvVars = map[string]string{"ROW": "must-not-rebind"}
	p.Spec.Files = []rtconfig.FileConfig{{Path: "/etc/row", Content: "must-not-rebind"}}
	body, err := p.BuildYAML()
	if err != nil {
		t.Fatal(err)
	}
	host, presence, err := loadMergedHostConfig(t, body)
	if err != nil {
		t.Fatal(err)
	}
	if presence.Any("launch") || presence.Has("files") || presence.Any("boot.disks") {
		t.Fatalf("paused E host renderer leaked persistent/protected fields:\n%s", body)
	}
	for _, field := range []string{
		"boot.root.base", "boot.root.base_from_refs",
		"boot.root.overlay.base", "boot.root.overlay.base_from_refs",
	} {
		if presence.Any(field) {
			t.Fatalf("paused E host renderer emitted immutable %s:\n%s", field, body)
		}
	}
	if strings.Contains(string(body), "diff_template:") {
		t.Fatalf("captured Sandbox disk was replaced with a node template:\n%s", body)
	}
	if !presence.Has("ephemeral_files") {
		t.Fatalf("cold network files are not ephemeral:\n%s", body)
	}
	runtime, c0, err := rtconfig.ApplyFromRules(rendererPortableConfig(), host, presence, rtconfig.ApplyFromOptions{})
	if err != nil {
		t.Fatalf("ApplyFromRules rejected generated config: %v\n%s", err, body)
	}
	if runtime.Launch.Env["ARTIFACT"] != "yes" || c0.Metadata["owner"] != "artifact" {
		t.Fatalf("Sandbox C0 was not authoritative: runtime=%+v c0=%+v", runtime.Launch, c0.Metadata)
	}
}

func TestGeneratedSnapshotHostConfigSatisfiesApplyRestoreRules(t *testing.T) {
	p := artifactRendererParams(types.LaunchMemory)
	p.EnvVars = map[string]string{"ROW": "forbidden"}
	p.Spec.Files = []rtconfig.FileConfig{{Path: "/etc/row", Content: "forbidden"}}
	body, err := p.BuildYAML()
	if err != nil {
		t.Fatal(err)
	}
	host, presence, err := loadMergedHostConfig(t, body)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"launch", "files", "ephemeral_files", "init", "mounts", "metadata", "boot.cmdline", "boot.disks"} {
		if presence.Any(field) {
			t.Fatalf("Snapshot host renderer emitted forbidden %s:\n%s", field, body)
		}
	}
	for _, field := range []string{
		"boot.root.base", "boot.root.base_from_refs",
		"boot.root.overlay.base", "boot.root.overlay.base_from_refs",
	} {
		if presence.Any(field) {
			t.Fatalf("Snapshot host renderer emitted immutable %s:\n%s", field, body)
		}
	}
	runtime, c0, err := rtconfig.ApplyRestoreRules(rendererPortableConfig(), host, presence)
	if err != nil {
		t.Fatalf("ApplyRestoreRules rejected generated config: %v\n%s", err, body)
	}
	if runtime.Launch.Env["ARTIFACT"] != "yes" || c0.Metadata["owner"] != "artifact" {
		t.Fatalf("Snapshot restore changed C0: runtime=%+v c0=%+v", runtime.Launch, c0.Metadata)
	}
}

func loadMergedHostConfig(t testing.TB, body []byte) (*rtconfig.SandboxConfig, rtconfig.FieldPresence, error) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "host.yaml")
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatal(err)
	}
	return rtconfig.LoadMergedWithPresence([]string{path})
}

func TestFreshSandboxTemplateUsesIntentionalPersistentOverrides(t *testing.T) {
	p := artifactRendererParams(types.LaunchCold)
	p.Template.Kind = types.KindSbx
	p.Template.Ref = "manifest://" + strings.Repeat("d", 64)
	p.Sandbox.TemplateID = p.Template.String()
	p.Sandbox.ResumeSource = types.ResumeSource{}
	p.EnvVars = map[string]string{"CREATE": "persistent"}
	p.Spec.Files = []rtconfig.FileConfig{{Path: "/etc/create", Content: "persistent"}}
	body, err := p.BuildYAML()
	if err != nil {
		t.Fatal(err)
	}
	_, presence, err := rtconfig.LoadConfigBytesWithPresence(body)
	if err != nil {
		t.Fatal(err)
	}
	if !presence.Has("launch.env") || !presence.Has("files") || !presence.Has("ephemeral_files") {
		t.Fatalf("fresh Sandbox overrides have wrong persistence:\n%s", body)
	}
}

func TestArtifactHostRenderersPreserveMultiDataDiskTopology(t *testing.T) {
	artifact := rendererPortableConfig()
	artifact.Boot.Disks = []rtconfig.PortableDiskConfig{
		{
			Name: "data-a",
			PortableRootConfig: rtconfig.PortableRootConfig{
				Base: "file://data-a.image@digest:" + strings.Repeat("c", 64),
			},
		},
		{
			Name: "data-b",
			PortableRootConfig: rtconfig.PortableRootConfig{
				Base: "file://data-b.image@digest:" + strings.Repeat("d", 64),
				Overlay: &rtconfig.PortableOverlayConfig{
					Base: "file://data-b.overlay@digest:" + strings.Repeat("e", 64),
				},
			},
		},
	}
	artifact.Mounts = []rtconfig.MountConfig{
		{Target: "/mnt/a", Type: "disk", Source: "data-a", Options: "ro"},
		{Target: "/mnt/b", Type: "disk", Source: "data-b"},
	}
	if err := artifact.Validate(); err != nil {
		t.Fatal(err)
	}

	for _, mode := range []types.LaunchMode{types.LaunchCold, types.LaunchMemory} {
		t.Run(string(mode), func(t *testing.T) {
			p := artifactRendererParams(mode)
			p.ArtifactDisks = types.ArtifactDiskTopology{
				Root: types.ArtifactDiskShape{Mode: types.ArtifactDiskOverlay, HasActiveBase: true},
				Disks: []types.ArtifactDiskShape{
					{Name: "data-a", Mode: types.ArtifactDiskSingle, HasActiveBase: true},
					{Name: "data-b", Mode: types.ArtifactDiskOverlay, HasActiveBase: true},
				},
			}
			body, err := p.BuildYAML()
			if err != nil {
				t.Fatal(err)
			}
			host, presence, err := rtconfig.LoadConfigBytesWithPresence(body)
			if err != nil {
				t.Fatal(err)
			}
			if !presence.Any("boot.disks") || presence.Has("mounts") {
				t.Fatalf("host renderer omitted active bindings or claimed artifact mounts:\n%s", body)
			}
			for _, forbidden := range []string{"base:", "base_from_refs:"} {
				if strings.Contains(string(body), forbidden) {
					t.Fatalf("host renderer emitted immutable disk field %q:\n%s", forbidden, body)
				}
			}
			var runtime *rtconfig.SandboxConfig
			var c0 *rtconfig.PortableSandboxConfig
			switch mode {
			case types.LaunchCold:
				runtime, c0, err = rtconfig.ApplyFromRules(artifact, host, presence, rtconfig.ApplyFromOptions{})
			case types.LaunchMemory:
				runtime, c0, err = rtconfig.ApplyRestoreRules(artifact, host, presence)
			}
			if err != nil {
				t.Fatalf("apply %s host config: %v\n%s", mode, err, body)
			}
			if len(c0.Boot.Disks) != 2 || c0.Boot.Disks[0].Name != "data-a" || c0.Boot.Disks[1].Name != "data-b" ||
				len(runtime.Boot.Disks) != 2 || runtime.Boot.Disks[0].Name != "data-a" || runtime.Boot.Disks[1].Name != "data-b" ||
				len(c0.Mounts) != 2 || c0.Mounts[0].Target != "/mnt/a" || c0.Mounts[1].Target != "/mnt/b" {
				t.Fatalf("artifact topology changed: runtime=%+v c0=%+v", runtime.Boot.Disks, c0)
			}
		})
	}
}

func TestArtifactHostRendererSeedsOnlyUncapturedActiveDisk(t *testing.T) {
	p := artifactRendererParams(types.LaunchCold)
	p.ArtifactDisks = types.ArtifactDiskTopology{
		Root: types.ArtifactDiskShape{Mode: types.ArtifactDiskOverlay},
		Disks: []types.ArtifactDiskShape{
			{Name: "data", Mode: types.ArtifactDiskSingle, HasActiveBase: true},
		},
	}
	body, err := p.BuildYAML()
	if err != nil {
		t.Fatal(err)
	}
	host, presence, err := loadMergedHostConfig(t, body)
	if err != nil {
		t.Fatal(err)
	}
	if !presence.Has("boot.root.overlay.diff_template") || host.Boot.Root.Overlay == nil ||
		host.Boot.Root.Overlay.DiffTemplate != "file:///r/overlay.ext4" {
		t.Fatalf("uncaptured root has no formatted active upper:\n%s", body)
	}
	if len(host.Boot.Disks) != 1 || host.Boot.Disks[0].DiffTemplate != "" {
		t.Fatalf("captured data disk was reseeded: %+v\n%s", host.Boot.Disks, body)
	}
}

func TestArtifactHostRendererRequiresTemplateForUncapturedDisk(t *testing.T) {
	p := artifactRendererParams(types.LaunchCold)
	p.ArtifactDisks.Root.HasActiveBase = false
	p.OverlayDiffTpl = ""
	if _, err := p.BuildYAML(); err == nil || !strings.Contains(err.Error(), "formatted diff template") {
		t.Fatalf("missing template error = %v", err)
	}
}
