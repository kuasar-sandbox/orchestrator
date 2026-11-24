package main

import (
	"path/filepath"
	"testing"
)

// Operator-facing generated and shipped configs must be accepted by the same
// command that diagnoses a deployment. Validate the normalized form as well.
func TestRoleConfigExamplesRoundTrip(t *testing.T) {
	for _, role := range []string{"registry", "router", "placer"} {
		t.Run(role, func(t *testing.T) {
			for _, source := range []string{"generated", "deployment"} {
				t.Run(source, func(t *testing.T) {
					dir := t.TempDir()
					input := filepath.Join("..", "..", "deploy", role+".example.yaml")
					if source == "generated" {
						input = filepath.Join(dir, "generated.yaml")
						if err := configCmd([]string{role, "--template", "-o", input}); err != nil {
							t.Fatal(err)
						}
					}
					normalized := filepath.Join(dir, "normalized.yaml")
					if err := configCmd([]string{role, "--config", input, "--resolve", "-o", normalized}); err != nil {
						t.Fatalf("shipped configuration rejected: %v", err)
					}
					if err := configCmd([]string{role, "--config", normalized, "-o", filepath.Join(dir, "roundtrip.yaml")}); err != nil {
						t.Fatalf("normalized configuration rejected: %v", err)
					}
				})
			}
		})
	}
}
