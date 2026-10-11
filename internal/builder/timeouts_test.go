package builder

import (
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
	"gopkg.in/yaml.v3"
)

func TestRuntimeStartupBudgetReachesEveryBuildPhase(t *testing.T) {
	p := &buildPipeline{
		profile: types.ProfileBare, baseRef: "manifest://" + strings.Repeat("a", 64),
		spec: &configsock.BuildSpec{
			RuntimeTimeouts: rtconfig.TimeoutsConfig{AppStart: "3s"},
			Paths:           configsock.BuildPaths{Kernel: "/k", Runtime: "/r", OverlayDiffTpl: "/d"},
		},
	}
	a, err := p.importYAML()
	if err != nil {
		t.Fatal(err)
	}
	for name, doc := range map[string]map[string]any{"import": a, "steps": p.stepsYAML(), "source_steps": p.sourceStepsYAML()} {
		body, err := yaml.Marshal(doc)
		if err != nil {
			t.Fatal(err)
		}
		var cfg rtconfig.SandboxConfig
		if err := yaml.Unmarshal(body, &cfg); err != nil || cfg.Timeouts.AppStart != "3s" {
			t.Fatal(name, err, string(body))
		}
	}
	cfg := decodeBuildColdConfig(t, p)
	if cfg.Timeouts.AppStart != "3s" {
		t.Fatal("phase C timeout", cfg.Timeouts)
	}
}
