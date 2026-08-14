package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
)

func TestConductorConfigTemplateUsesNestedSandboxResourcePolicy(t *testing.T) {
	start := strings.Index(conductorConfigSkeleton, "sandbox:")
	end := strings.Index(conductorConfigSkeleton[start:], "\nbuilder:")
	if start < 0 || end < 0 {
		t.Fatal("conductor template has no sandbox resource block")
	}
	sandboxBlock := conductorConfigSkeleton[start : start+end]
	for _, want := range []string{
		"capacity: { cpu: 2, memory: 2GiB }",
		"allocatable:",
		"memory: 256MiB",
		"startup: { memory: 512MiB }",
		"overhead: { memory: 32MiB }",
	} {
		if !strings.Contains(sandboxBlock, want) {
			t.Errorf("sandbox template does not contain %q:\n%s", want, sandboxBlock)
		}
	}
	for _, forbidden := range []string{"vcpu:", "control_socket:"} {
		if strings.Contains(sandboxBlock, forbidden) {
			t.Errorf("sandbox template still contains %q:\n%s", forbidden, sandboxBlock)
		}
	}
}

func TestRenderConductorConfigNormalizesNestedResourceDefaults(t *testing.T) {
	path := writeConductorConfig(t, `
api: { domain: config.test }
encryption_key: test-key
sandbox:
  boot: { kernel: /kernel, runtime: /runtime }
`)
	out, err := renderConductorConfig(false, false, path)
	if err != nil {
		t.Fatal(err)
	}
	text := string(out)
	start := strings.Index(text, "sandbox:")
	end := strings.Index(text[start:], "\nbuilder:")
	if start < 0 || end < 0 {
		t.Fatalf("normalized config has no sandbox block:\n%s", text)
	}
	sandboxBlock := text[start : start+end]
	for _, want := range []string{
		"capacity:\n            cpu: 2\n            memory: 2GiB",
		"allocatable:\n            memory: 256MiB",
		"overhead:\n            memory: 32MiB",
	} {
		if !strings.Contains(sandboxBlock, want) {
			t.Errorf("normalized sandbox config does not contain %q:\n%s", want, sandboxBlock)
		}
	}
	if strings.Contains(text, "control_socket:") {
		t.Fatalf("normalized config exposes removed control_socket:\n%s", text)
	}
}

func TestRenderConductorConfigResolvePreservesListenerSpelling(t *testing.T) {
	realParent := filepath.Join(t.TempDir(), "canonical")
	if err := os.Mkdir(realParent, 0o700); err != nil {
		t.Fatal(err)
	}
	aliasParent := filepath.Join(filepath.Dir(realParent), "alias")
	if err := os.Symlink(realParent, aliasParent); err != nil {
		t.Fatal(err)
	}
	listen := filepath.Join(aliasParent, "controller.sock")
	path := writeConductorConfig(t, `
api: { domain: config.test }
encryption_key: test-key
sandbox:
  boot: { kernel: /kernel, runtime: /runtime }
resource_listen:
  enabled: true
  socket: `+listen+`
  resources:
    physical_memory: 64GiB
    physical_cpu: "8"
    host_reserved: { memory: 1GiB, cpu: 1 }
`)
	out, err := renderConductorConfig(false, true, path)
	if err != nil {
		t.Fatal(err)
	}
	text := string(out)
	if !strings.Contains(text, "socket: "+listen) {
		t.Fatalf("--resolve changed bind/dial spelling instead of preserving Listen:\n%s", text)
	}
	if strings.Contains(text, "control_socket:") {
		t.Fatalf("--resolve exposes removed control_socket:\n%s", text)
	}
}

func TestRenderConductorConfigMaterializesClampedInheritedFloor(t *testing.T) {
	path := writeConductorConfig(t, `
api: { domain: config.test }
encryption_key: test-key
sandbox:
  resources:
    capacity: { cpu: 1, memory: 128MiB }
  boot: { kernel: /kernel, runtime: /runtime }
`)
	out, err := renderConductorConfig(false, false, path)
	if err != nil {
		t.Fatal(err)
	}
	normalized := writeConductorConfig(t, string(out))
	reloaded, err := config.Load(normalized)
	if err != nil {
		t.Fatalf("normalized config did not round-trip: %v\n%s", err, out)
	}
	if strings.Count(string(out), "memory: 128MiB") != 1 {
		t.Fatalf("normalized config made the inherited clamp explicit:\n%s", out)
	}
	resolved, err := sandboxcfg.ResolveResources(sandboxcfg.ResourceResolveInput{
		Node: reloaded.Sandbox.Resources.Policy(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := resolved.Allocatable.Memory; got != "128MiB" {
		t.Fatalf("reloaded effective floor = %s, want 128MiB\n%s", got, out)
	}
	patch, err := sandboxcfg.ParseResourcePatch(`{"capacity":{"memory":"512MiB"}}`)
	if err != nil {
		t.Fatal(err)
	}
	raised, err := sandboxcfg.ResolveResources(sandboxcfg.ResourceResolveInput{
		Node: reloaded.Sandbox.Resources.Policy(), Patch: patch,
	})
	if err != nil {
		t.Fatal(err)
	}
	if raised.Allocatable.Memory != "256MiB" {
		t.Fatalf("reloaded inherited default did not follow raised capacity: %+v\n%s", raised, out)
	}
}

func writeConductorConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "conductor.yaml")
	if err := os.WriteFile(path, []byte(strings.TrimSpace(body)+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
