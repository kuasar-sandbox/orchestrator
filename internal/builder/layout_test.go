package builder

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
)

func TestBuildArtifactPathsSeparateVolatileAndPersistentData(t *testing.T) {
	runDir := filepath.Join(t.TempDir(), "run", "builds", "Build_Mixed-1")
	baseDir := filepath.Join(t.TempDir(), "base", "builds", "Build_Mixed-1")
	p := &buildPipeline{spec: &configsock.BuildSpec{RunDir: runDir, BaseDir: baseDir}}

	want := map[string]string{
		"phase run":  filepath.Join(runDir, "b"),
		"phase base": filepath.Join(baseDir, "b"),
		"checkpoint": filepath.Join(baseDir, "checkpoint"),
		"image":      filepath.Join(baseDir, "checkpoint", "image.img"),
		"next image": filepath.Join(baseDir, "checkpoint", "image.next.img"),
	}
	got := map[string]string{
		"phase run":  p.phaseRunDir("b"),
		"phase base": p.phaseBaseDir("b"),
		"checkpoint": p.checkpointDir(),
		"image":      p.imageFile(),
		"next image": p.nextImageFile(),
	}
	for name, path := range got {
		if path != want[name] {
			t.Errorf("%s = %q, want %q", name, path, want[name])
		}
	}
	if !pathWithin(runDir, got["phase run"]) {
		t.Errorf("phase RunDir escaped BuildRunDir: %q", got["phase run"])
	}
	for _, name := range []string{"phase base", "checkpoint", "image", "next image"} {
		if pathWithin(runDir, got[name]) || !pathWithin(baseDir, got[name]) {
			t.Errorf("%s is not confined to BuildBaseDir: %q", name, got[name])
		}
	}
}

func pathWithin(root, path string) bool {
	rel, err := filepath.Rel(root, path)
	return err == nil && rel != ".." && !filepath.IsAbs(rel) && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}
