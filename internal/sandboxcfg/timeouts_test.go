package sandboxcfg

import (
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
	"gopkg.in/yaml.v3"
)

func TestNodeStartupBudgetSurvivesAllHostRenderers(t *testing.T) {
	for _, mode := range []types.LaunchMode{types.LaunchImage, types.LaunchCold, types.LaunchMemory} {
		t.Run(string(mode), func(t *testing.T) {
			p := baseParams(types.ProfileBare)
			if mode != types.LaunchImage {
				p = artifactRendererParams(mode)
			}
			p.Timeouts = rtconfig.TimeoutsConfig{AppStart: "3s"}
			body, err := p.BuildYAML()
			if err != nil {
				t.Fatal(err)
			}
			host, presence, err := rtconfig.LoadConfigBytesWithPresence(body)
			if err != nil || host.Timeouts.AppStart != "3s" {
				t.Fatal("rendered timeout", err, string(body))
			}
			if mode == types.LaunchImage {
				return
			}
			var runtime *rtconfig.SandboxConfig
			var portable *rtconfig.PortableSandboxConfig
			if mode == types.LaunchCold {
				runtime, portable, err = rtconfig.ApplyFromRules(rendererPortableConfig(), host, presence, rtconfig.ApplyFromOptions{})
			} else {
				runtime, portable, err = rtconfig.ApplyRestoreRules(rendererPortableConfig(), host, presence)
			}
			if err != nil || runtime.Timeouts.AppStart != "3s" {
				t.Fatal("apply host policy", err)
			}
			encoded, err := yaml.Marshal(portable)
			if err != nil || strings.Contains(string(encoded), "app_start") {
				t.Fatal("host policy leaked into artifact", err)
			}
		})
	}
}
