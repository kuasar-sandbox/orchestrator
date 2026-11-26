package sandboxcfg

import (
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
)

func TestKernelCmdlineAcrossLaunchModes(t *testing.T) {
	const inherited = "loglevel=3"
	const replacement = `loglevel=4 example.value="two words"`
	for _, tc := range []struct {
		name    string
		cmdline *string
	}{
		{name: "inherit"},
		{name: "clear", cmdline: new("")},
		{name: "replace", cmdline: new(replacement)},
	} {
		for _, mode := range []types.LaunchMode{types.LaunchImage, types.LaunchCold, types.LaunchMemory} {
			t.Run(tc.name+"/"+string(mode), func(t *testing.T) {
				p := artifactRendererParams(mode)
				if mode == types.LaunchImage {
					p = baseParams(types.ProfileBare)
				}
				p.Kernel, p.Cmdline = "/node/new-vmlinux", tc.cmdline
				body, err := p.BuildYAML()
				if err != nil {
					t.Fatal(err)
				}
				host, presence, err := rtconfig.LoadConfigBytesWithPresence(body)
				if err != nil {
					t.Fatal(err)
				}
				source := rendererPortableConfig()
				source.Boot.Cmdline = inherited
				var runtime *rtconfig.SandboxConfig
				var c0 *rtconfig.PortableSandboxConfig
				want := inherited
				switch mode {
				case types.LaunchImage:
					runtime, want = host, ""
					if tc.cmdline != nil {
						want = *tc.cmdline
					}
				case types.LaunchCold:
					if presence.Has("boot.cmdline") != (tc.cmdline != nil) {
						t.Fatalf("cold cmdline presence lost:\n%s", body)
					}
					if tc.cmdline != nil {
						want = *tc.cmdline
					}
					runtime, c0, err = rtconfig.ApplyFromRules(source, host, presence, rtconfig.ApplyFromOptions{})
				case types.LaunchMemory:
					if presence.Has("boot.kernel") || presence.Has("boot.cmdline") {
						t.Fatalf("restore emitted cold kernel parameters:\n%s", body)
					}
					runtime, c0, err = rtconfig.ApplyRestoreRules(source, host, presence)
				}
				if err != nil {
					t.Fatal(err)
				}
				if runtime.Boot.Cmdline != want || (c0 != nil && c0.Boot.Cmdline != want) {
					t.Fatalf("kernel parameters diverged: runtime=%q C0=%+v want=%q", runtime.Boot.Cmdline, c0, want)
				}
				if mode != types.LaunchMemory && runtime.Boot.Kernel != "file://"+p.Kernel {
					t.Fatalf("kernel selection lost: %+v", runtime.Boot)
				}
				if source.Boot.Cmdline != inherited {
					t.Fatal("source parameters were mutated")
				}
			})
		}
	}
}
