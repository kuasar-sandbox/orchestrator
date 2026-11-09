package main

import (
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/config"
)

func TestConfigCommandExplicitFileList(t *testing.T) {
	for _, tc := range []struct {
		role, base, overlay, want string
	}{
		{"conductor", "api: {domain: layers.test}\nencryption_key: test-key\nsandbox: {boot: {kernel: /kernel, runtime: /runtime}}\ncluster: {labels: {layer: base}}", "cluster: {labels: {layer: override}}", "layer: override"},
		{"proxy", "paths: {run_root: /run/sandbox}\nworkers: 4", "data_listen: ':3443'", "workers: 4"},
		{"telemetry", "query: {backend: local}", "local: {enabled: true}", "backend: local"},
	} {
		t.Run(tc.role, func(t *testing.T) {
			dir := t.TempDir()
			base, overlay, output := filepath.Join(dir, "base.yaml"), filepath.Join(dir, "override.yaml"), filepath.Join(dir, "effective.yaml")
			for path, raw := range map[string]string{base: tc.base, overlay: tc.overlay} {
				if err := os.WriteFile(path, []byte(raw), 0600); err != nil {
					t.Fatal(err)
				}
			}
			list := base + ":" + overlay
			if err := configCmd([]string{tc.role, "--config", list, "-o", output}, slog.Default()); err != nil {
				t.Fatal(err)
			}
			raw, err := os.ReadFile(output)
			if err != nil || !strings.Contains(string(raw), tc.want) {
				t.Fatalf("effective output: error=%v, expected %q", err, tc.want)
			}
			// The serve commands use these same public loading entrypoints.
			switch tc.role {
			case "conductor":
				cfg, err := config.LoadConductor(list)
				if err != nil || cfg.Cluster.Labels["layer"] != "override" {
					t.Fatalf("conductor loader differs: %v", err)
				}
			case "proxy":
				cfg, err := config.LoadProxy(list)
				if err != nil || cfg.Workers != 4 || cfg.DataListen != ":3443" {
					t.Fatalf("proxy loader differs: %v", err)
				}
			case "telemetry":
				cfg, err := config.LoadTelemetry(list)
				if err != nil || !cfg.Local.Enabled || cfg.Query.Backend != "local" {
					t.Fatalf("telemetry loader differs: %v", err)
				}
			}
		})
	}
}
