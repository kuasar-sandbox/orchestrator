package builder

import (
	"reflect"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
	"gopkg.in/yaml.v3"
)

func builderCmdlineSource() *rtconfig.PortableSandboxConfig {
	return &rtconfig.PortableSandboxConfig{
		Version: rtconfig.PortableSandboxConfigVersion,
		Resources: rtconfig.PortableResourcesConfig{
			Capacity:    rtconfig.CapacityConfig{CPU: 1, Memory: "128MiB"},
			Allocatable: rtconfig.AllocatableConfig{CPU: 1, Memory: "128MiB"},
		},
		Network: rtconfig.PortableNetworkConfig{Enabled: true, Interface: "eth0"},
		Boot: rtconfig.PortableBootConfig{
			Kernel:  "file://old-kernel@digest:" + strings.Repeat("b", 64),
			Runtime: "file://runtime@digest:" + strings.Repeat("c", 64),
			Cmdline: "panic=10 old_kernel_option=1",
			Root: rtconfig.PortableRootConfig{
				Base:    "self",
				Overlay: &rtconfig.PortableOverlayConfig{},
			},
		},
		Launch: rtconfig.PortableLaunchConfig{Exec: "/bin/true", Workdir: "/", Restart: "never"},
	}
}

func TestBuildPhaseCmdlinePreservesPresenceAndSourceGraph(t *testing.T) {
	empty, selected := "", "panic=0 new_kernel_option=1"
	for _, tc := range []struct {
		name    string
		cmdline *string
	}{
		{name: "inherit"},
		{name: "clear", cmdline: &empty},
		{name: "replace", cmdline: &selected},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := builderCmdlineSource()
			p := &buildPipeline{baseRef: "manifest://" + strings.Repeat("a", 64), spec: &configsock.BuildSpec{
				BootCmdline: tc.cmdline,
				Paths: configsock.BuildPaths{
					Kernel: "/new-kernel", Runtime: "/runtime", BuilderDiffTpl: "mkfs://ext4?size=16MiB",
				},
				Net:                 configsock.BuildNet{TapFD: configsock.TapFDConfig{Exec: []string{"/bin/true"}}},
				Resources:           rtconfig.ResourcesConfig{Capacity: source.Resources.Capacity, Allocatable: source.Resources.Allocatable},
				SourceSandboxConfig: source,
			}}
			importDoc, err := p.importYAML()
			if err != nil {
				t.Fatal(err)
			}
			for phase, doc := range map[string]map[string]any{
				"import": importDoc, "image-steps": mustStepsYAML(t, p, nil), "source-steps": mustSourceStepsYAML(t, p),
			} {
				raw, err := yaml.Marshal(doc)
				if err != nil {
					t.Fatal(err)
				}
				host, presence, err := rtconfig.LoadConfigBytesWithPresence(raw)
				if err != nil {
					t.Fatal(err)
				}
				if presence.Has("boot.cmdline") != (tc.cmdline != nil) {
					t.Fatalf("%s lost cmdline presence:\n%s", phase, raw)
				}
				if tc.cmdline != nil && host.Boot.Cmdline != *tc.cmdline {
					t.Fatalf("%s cmdline = %q, want %q", phase, host.Boot.Cmdline, *tc.cmdline)
				}
				if phase != "source-steps" {
					continue
				}
				runtime, derived, err := rtconfig.ApplyFromRules(source, host, presence, rtconfig.ApplyFromOptions{})
				if err != nil {
					t.Fatal(err)
				}
				want := source.Boot.Cmdline
				if tc.cmdline != nil {
					want = *tc.cmdline
				}
				if runtime.Boot.Kernel != "file:///new-kernel" || runtime.Boot.Cmdline != want || derived.Boot.Cmdline != want {
					t.Fatalf("source steps selected boot: runtime=%+v derived=%+v", runtime.Boot, derived.Boot)
				}
				if !reflect.DeepEqual(derived.Boot.Root, source.Boot.Root) || source.Boot.Cmdline != "panic=10 old_kernel_option=1" {
					t.Fatal("cmdline override changed the source or its disk graph")
				}
			}
		})
	}
}

func TestBuildCmdlineMatchesSandboxAndMemoryTargets(t *testing.T) {
	empty, selected := "", "panic=0 new_kernel_option=1"
	for _, withSource := range []bool{false, true} {
		for _, tc := range []struct {
			name    string
			cmdline *string
		}{
			{name: "inherit"},
			{name: "clear", cmdline: &empty},
			{name: "replace", cmdline: &selected},
		} {
			name := "image/" + tc.name
			if withSource {
				name = "sandbox/" + tc.name
			}
			t.Run(name, func(t *testing.T) {
				p := &buildPipeline{profile: types.ProfileE2B, baseRef: "manifest://" + strings.Repeat("a", 64), spec: &configsock.BuildSpec{
					BootCmdline: tc.cmdline,
					Paths: configsock.BuildPaths{
						Kernel: "/new-kernel", Runtime: "/runtime", OverlayDiffTpl: "mkfs://ext4?size=16MiB",
					},
					Net: configsock.BuildNet{TapFD: configsock.TapFDConfig{Exec: []string{"/bin/true"}}},
					SandboxResources: rtconfig.ResourcesConfig{
						Capacity:    rtconfig.CapacityConfig{CPU: 1, Memory: "128MiB"},
						Allocatable: rtconfig.AllocatableConfig{CPU: 1, Memory: "128MiB"},
					},
				}}
				want := ""
				if withSource {
					p.spec.SourceSandboxRef = "manifest://" + strings.Repeat("e", 64)
					p.spec.SourceSandboxConfig = builderCmdlineSource()
					want = p.spec.SourceSandboxConfig.Boot.Cmdline
				}
				if tc.cmdline != nil {
					want = *tc.cmdline
				}
				cfg, err := p.buildColdConfig()
				if err != nil {
					t.Fatal(err)
				}
				cfg.ApplyDefaults()
				portable, err := rtconfig.ProjectPortableCold(cfg, rtconfig.PortableProjection{
					KernelRef: "file://new-kernel@digest:" + strings.Repeat("f", 64), RuntimeRef: "file://runtime@digest:" + strings.Repeat("c", 64),
				})
				if err != nil {
					t.Fatal(err)
				}
				document, from, replace, err := p.templateBoot()
				if err != nil {
					t.Fatal(err)
				}
				phaseC, presence, err := rtconfig.LoadConfigBytesWithPresence(document.Config)
				if err != nil {
					t.Fatal(err)
				}
				if from != p.spec.SourceSandboxRef || replace != withSource {
					t.Fatalf("phase C from=%q replace=%t", from, replace)
				}
				if withSource {
					phaseC, _, err = rtconfig.ApplyFromRules(p.spec.SourceSandboxConfig, phaseC, presence, rtconfig.ApplyFromOptions{ReplaceBoot: true})
					if err != nil {
						t.Fatal(err)
					}
					if p.spec.SourceSandboxConfig.Boot.Cmdline != "panic=10 old_kernel_option=1" {
						t.Fatal("build changed source cmdline")
					}
				}
				if portable.Boot.Cmdline != want || phaseC.Boot.Cmdline != want || phaseC.Boot.Kernel != cfg.Boot.Kernel {
					t.Fatalf("E cmdline=%q Phase C boot=%+v, want cmdline=%q kernel=%q", portable.Boot.Cmdline, phaseC.Boot, want, cfg.Boot.Kernel)
				}
			})
		}
	}
}
