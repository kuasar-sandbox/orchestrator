package config

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

func TestTemplateConfigurationSharesURIContract(t *testing.T) {
	key := strings.Repeat("a", 64)
	for _, value := range []string{
		"mkfs://ext4?size=16MiB",
		"file:///seed.ext4",
		"file:///seed.tar@digest:" + key,
		"file://seed.tar@hmac:" + key + "@location:seeds",
		"file://seeds.bundle@manifest:" + key + "@location:seeds",
		"manifest://" + key,
		filepath.Join(t.TempDir(), "legacy.ext4"),
	} {
		t.Run(value, func(t *testing.T) {
			body := fmt.Sprintf("sandbox:\n  boot:\n    workspace_diff_template: %q\n    overlay_diff_template: %q\nbuilder:\n  diff_template: %q\n", value, value, value)
			cfg, err := DecodeConductor(strings.NewReader(body))
			if err != nil {
				t.Fatal(err)
			}
			if cfg.Sandbox.Boot.WorkspaceDiffTemplate != cfg.Sandbox.Boot.OverlayDiffTemplate || cfg.Builder.DiffTemplate != cfg.Sandbox.Boot.WorkspaceDiffTemplate {
				t.Fatalf("template fields normalized differently: %+v", cfg.Sandbox.Boot)
			}
			if !strings.Contains(cfg.Builder.DiffTemplate, "://") {
				t.Fatalf("legacy path was not normalized: %q", cfg.Builder.DiffTemplate)
			}
		})
	}
}

func TestWorkspaceTemplateConfigurationMigration(t *testing.T) {
	for _, old := range []string{"{kind: disk, version: 1, diff_template: 'file:///seed'}", "null", "{kind: disk, version: 1, base: 'manifest://" + strings.Repeat("a", 64) + "'}"} {
		_, err := DecodeConductor(strings.NewReader("sandbox: {boot: {workspace_default_disk: " + old + "}}"))
		if err == nil || !strings.Contains(err.Error(), "workspace_diff_template") || !strings.Contains(err.Error(), "explicit disk request") {
			t.Fatalf("missing actionable migration for %s: %v", old, err)
		}
	}
	for _, field := range []string{"workspace_diff_template", "overlay_diff_template"} {
		_, err := DecodeConductor(strings.NewReader("sandbox: {boot: {" + field + ": 'mkfs://ext4?size=bad'}}"))
		if err == nil || !strings.Contains(err.Error(), field) {
			t.Fatalf("invalid %s accepted: %v", field, err)
		}
	}
	if _, err := DecodeConductor(strings.NewReader("builder: {diff_template: 'unknown://disk'}")); err == nil {
		t.Fatal("unknown template scheme accepted")
	}
}
