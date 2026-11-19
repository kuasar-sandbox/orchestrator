package builder

import (
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/reflocation"
	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
)

func TestBuilderRenderersPreserveTemplateReference(t *testing.T) {
	key := strings.Repeat("a", 64)
	for _, ref := range []string{
		"mkfs://ext4?size=16777216", "file:///seed.ext4",
		"file:///seed.tar@digest:" + key, "file:///seed.tar@hmac:" + key,
		"file:///seed.bundle@manifest:" + key, "manifest://" + key,
	} {
		t.Run(ref, func(t *testing.T) {
			p := &buildPipeline{spec: &configsock.BuildSpec{Paths: configsock.BuildPaths{BuilderDiffTpl: ref}}}
			importDoc, err := p.importYAML()
			if err != nil {
				t.Fatal(err)
			}
			if got := importDoc["boot"].(map[string]any)["root"].(map[string]any)["diff_template"]; got != ref {
				t.Fatalf("import template = %v", got)
			}
			steps := mustStepsYAML(t, p, nil)
			if got := steps["boot"].(map[string]any)["root"].(map[string]any)["overlay"].(map[string]any)["diff_template"]; got != ref {
				t.Fatalf("steps template = %v", got)
			}
			from := mustSourceStepsYAML(t, p)
			if got := from["boot"].(map[string]any)["root"].(map[string]any)["diff_template"]; got != ref {
				t.Fatalf("source template = %v", got)
			}
		})
	}
}

func TestSourceStepsKeepsCapturedActiveBase(t *testing.T) {
	for _, overlay := range []bool{false, true} {
		for _, configured := range []string{"mkfs://ext4?size=32MiB", "file://missing.ext4@location:unavailable", "invalid://template"} {
			cfg := &rtconfig.PortableSandboxConfig{}
			if overlay {
				cfg.Boot.Root = rtconfig.PortableRootConfig{Base: "self", Overlay: &rtconfig.PortableOverlayConfig{Base: "manifest://" + strings.Repeat("1", 64)}}
			} else {
				cfg.Boot.Root.Base = "manifest://" + strings.Repeat("2", 64)
			}
			p := &buildPipeline{spec: &configsock.BuildSpec{Paths: configsock.BuildPaths{BuilderDiffTpl: configured}, SourceSandboxConfig: cfg}}
			doc, err := p.sourceStepsYAML()
			if err != nil {
				t.Fatalf("captured base probed unused %q: %v", configured, err)
			}
			active := doc["boot"].(map[string]any)["root"].(map[string]any)
			if overlay {
				active = active["overlay"].(map[string]any)
			}
			if _, present := active["diff_template"]; present || len(p.spec.RefLocations) != 0 {
				t.Fatalf("captured base was given a replacement template: %v %v", active, p.spec.RefLocations)
			}
		}
	}
	// An EROFS lower is a different device; its empty ext4 upper still needs T.
	p := &buildPipeline{spec: &configsock.BuildSpec{
		Paths:               configsock.BuildPaths{BuilderDiffTpl: "mkfs://ext4?size=16MiB"},
		SourceSandboxConfig: &rtconfig.PortableSandboxConfig{Boot: rtconfig.PortableBootConfig{Root: rtconfig.PortableRootConfig{Base: "self", Overlay: &rtconfig.PortableOverlayConfig{}}}},
	}}
	doc := mustSourceStepsYAML(t, p)
	if got := doc["boot"].(map[string]any)["root"].(map[string]any)["overlay"].(map[string]any)["diff_template"]; got != "mkfs://ext4?size=16777216" {
		t.Fatalf("empty upper did not select configured mkfs: %v", got)
	}
}

func TestSelectedBuilderTemplateAddsNamedLocation(t *testing.T) {
	p := &buildPipeline{spec: &configsock.BuildSpec{
		Paths:                       configsock.BuildPaths{BuilderDiffTpl: "file://seed.ext4@location:seeds"},
		CheckpointRefLocationParent: "file:///managed",
	}}
	if _, err := p.importYAML(); err != nil {
		t.Fatal(err)
	}
	want, err := reflocation.Resolve("file:///managed", "seeds")
	if err != nil {
		t.Fatal(err)
	}
	if p.spec.RefLocations["seeds"] != want.URI {
		t.Fatalf("selected template location not passed to sandbox-ctl: %v", p.spec.RefLocations)
	}
}
