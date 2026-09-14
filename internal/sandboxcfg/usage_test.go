package sandboxcfg

import (
	"bytes"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
	"gopkg.in/yaml.v3"
)

func TestNodeUsagePolicyAllLaunchModesAndPortableExclusion(t *testing.T) {
	for _, mode := range []types.LaunchMode{types.LaunchImage, types.LaunchCold, types.LaunchMemory} {
		for _, enabled := range []bool{false, true} {
			p := artifactRendererParams(mode)
			if mode == types.LaunchImage {
				p = baseParams(types.ProfileBare)
			}
			p.Usage = rtconfig.UsageConfig{Enabled: enabled, SampleInterval: "3s", FlushInterval: "9m"}
			body, err := p.BuildYAML()
			if err != nil {
				t.Fatal(mode, err)
			}
			host, presence, err := rtconfig.LoadConfigBytesWithPresence(body)
			if err != nil {
				t.Fatal(mode, string(body), err)
			}
			if host.Usage != p.Usage || !presence.Has("usage.enabled") {
				t.Fatal("node policy lost", mode, string(body))
			}
			var runtime *rtconfig.SandboxConfig
			switch mode {
			case types.LaunchCold:
				runtime, _, err = rtconfig.ApplyFromRules(rendererPortableConfig(), host, presence, rtconfig.ApplyFromOptions{})
			case types.LaunchMemory:
				runtime, _, err = rtconfig.ApplyRestoreRules(rendererPortableConfig(), host, presence)
			default:
				runtime = host
			}
			if err != nil || runtime.Usage != p.Usage {
				t.Fatal("runtime policy lost", mode, err)
			}
			// Portable types already exclude host usage; preserve that contract while
			// reusing the native apply paths instead of inheriting an earlier node.
			portable, err := yaml.Marshal(rendererPortableConfig())
			if err != nil || bytes.Contains(portable, []byte("usage:")) {
				t.Fatal("usage entered portable artifact", err)
			}
		}
	}
}
