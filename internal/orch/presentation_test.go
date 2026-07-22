package orch

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestSandboxPresentationUsesLaunchResourcesAndDurableFallback(t *testing.T) {
	dir := t.TempDir()
	sandbox := &types.Sandbox{
		ID: "sandbox-1", TemplateID: "e2b-img-" + strings.Repeat("a", 64),
		RunDir: filepath.Join(dir, "run"), BaseDir: filepath.Join(dir, "base"),
		CreatedUnix: 10, DeadlineUnix: 20,
		Metadata: map[string]string{"tenant": "first", clusterstate.ObjectMetadataKey: "hidden"},
	}
	if err := os.MkdirAll(sandbox.RunDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(sandbox.BaseDir, 0o700); err != nil {
		t.Fatal(err)
	}
	templatePath := filepath.Join(dir, "overlay-template.ext4")
	template, err := os.Create(templatePath)
	if err != nil {
		t.Fatal(err)
	}
	if err := template.Truncate(17 << 20); err != nil {
		template.Close()
		t.Fatal(err)
	}
	if err := template.Close(); err != nil {
		t.Fatal(err)
	}
	launchConfig := []byte(fmt.Sprintf(
		"resources:\n  capacity:\n    cpu: 3\n    memory: 3GiB\nboot:\n  root:\n    overlay:\n      diff_template: file://%s\n",
		templatePath,
	))
	if err := os.WriteFile(filepath.Join(sandbox.RunDir, sandbox.ID+".yaml"), launchConfig, 0o600); err != nil {
		t.Fatal(err)
	}
	diffPath := filepath.Join(sandbox.BaseDir, sandbox.ID+".overlay.diff")
	orchestrator := &Orchestrator{cfg: &config.Config{}}
	first, err := orchestrator.sandboxPresentation(sandbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	if first.CPUCount != 3 || first.MemoryMB != 3072 || first.DiskSizeMB != 17 ||
		first.Metadata["tenant"] != "first" || first.Metadata[clusterstate.ObjectMetadataKey] != "" {
		t.Fatalf("launch presentation = %+v", first)
	}
	diff, err := os.Create(diffPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := diff.Truncate(19 << 20); err != nil {
		diff.Close()
		t.Fatal(err)
	}
	if err := diff.Close(); err != nil {
		t.Fatal(err)
	}
	actual, err := orchestrator.sandboxPresentation(sandbox, nil)
	if err != nil || actual.DiskSizeMB != 19 {
		t.Fatalf("actual launch disk presentation = %+v, %v", actual, err)
	}

	if err := os.Remove(filepath.Join(sandbox.RunDir, sandbox.ID+".yaml")); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(diffPath); err != nil {
		t.Fatal(err)
	}
	sandbox.DeadlineUnix = 30
	sandbox.Metadata["tenant"] = "updated"
	second, err := orchestrator.sandboxPresentation(sandbox, &actual)
	if err != nil {
		t.Fatal(err)
	}
	if second.CPUCount != actual.CPUCount || second.MemoryMB != actual.MemoryMB || second.DiskSizeMB != actual.DiskSizeMB ||
		second.EndAt != 30 || second.Metadata["tenant"] != "updated" {
		t.Fatalf("durable fallback presentation = %+v", second)
	}
}
