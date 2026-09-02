package main

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/config"
	"github.com/kuasar-sandbox/orchestrator/internal/configresolve"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"gopkg.in/yaml.v3"
)

func TestConductorConfigTemplateUsesNestedSandboxResourcePolicy(t *testing.T) {
	if !strings.Contains(conductorConfigSkeleton, "conductor_executable: /opt/kuasar/bin/xconductor") {
		t.Fatal("conductor template does not document paths.conductor_executable")
	}
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

func TestProxyConfigTemplateDocumentsCustomExecutable(t *testing.T) {
	if !strings.Contains(proxyConfigSkeleton, "proxy_executable: /opt/kuasar/bin/xproxy") {
		t.Fatal("proxy template does not document paths.proxy_executable")
	}
}

func TestConfigTemplatesDescribeSplitEndpointsAndSingleProxyIngress(t *testing.T) {
	for _, want := range []string{"api_endpoint: node.example.com:3000", "data_endpoint: sandbox.example.com:3443"} {
		if !strings.Contains(conductorConfigSkeleton, want) {
			t.Errorf("conductor template does not contain %q", want)
		}
	}
	start := strings.Index(conductorConfigSkeleton, "\nproxy:")
	if start < 0 {
		t.Fatal("conductor template has no proxy policy block")
	}
	end := strings.Index(conductorConfigSkeleton[start+1:], "\nencryption_key:")
	if end < 0 {
		t.Fatal("conductor template has no proxy policy block")
	}
	proxyPolicyBlock := conductorConfigSkeleton[start : start+1+end]
	for _, removed := range []string{"\n  mode:", "\n  data_listen:", "\n  proxy_netns:"} {
		if strings.Contains(proxyPolicyBlock, removed) {
			t.Errorf("conductor template still contains removed Proxy field %q", removed)
		}
	}
	if !strings.Contains(conductorConfigSkeleton, "listen: \":3000\"") {
		t.Fatal("conductor template does not use its distinct plaintext API listen")
	}
	if !strings.Contains(proxyConfigSkeleton, "data_listen: \":3443\"") {
		t.Fatal("proxy template does not document required data_listen")
	}
}

func TestConductorConfigTemplateOmitsRemovedResourceAuditField(t *testing.T) {
	if strings.Contains(conductorConfigSkeleton, "audit_path") {
		t.Fatal("conductor template still contains removed resource_listen.audit_path")
	}
}

func TestConductorConfigTemplateUsesBuilderTwoStageAdmission(t *testing.T) {
	start := strings.Index(conductorConfigSkeleton, "builder:")
	end := strings.Index(conductorConfigSkeleton[start:], "\n# resource_listen:")
	if start < 0 || end < 0 {
		t.Fatal("conductor template has no builder admission block")
	}
	block := conductorConfigSkeleton[start : start+end]
	for _, want := range []string{
		"admission:", "registration:", "execution:", "max_builds: 16",
		"resources: { cpu: 64, memory: 256GiB, storage: 1TiB }",
		"registration_ttl: 1h", "queue_ttl: 30m", "storage is admission-only",
	} {
		if !strings.Contains(block, want) {
			t.Errorf("builder template does not contain %q:\n%s", want, block)
		}
	}
	for _, forbidden := range []string{"max_concurrent:", "cpu_quota:", "memory_max:", "vcpu:"} {
		if strings.Contains(block, forbidden) {
			t.Errorf("builder template still contains %q:\n%s", forbidden, block)
		}
	}
}

func TestConductorConfigTemplateDocumentsBundleCheckpointAndRemotePublication(t *testing.T) {
	start := strings.Index(conductorConfigSkeleton, "checkpoint:")
	end := strings.Index(conductorConfigSkeleton[start:], "\n# mmds:")
	if start < 0 || end < 0 {
		t.Fatal("conductor template has no checkpoint block")
	}
	block := conductorConfigSkeleton[start : start+end]
	for _, want := range []string{"mode: local", "local | bundle", "ref_location_parent:"} {
		if !strings.Contains(block, want) {
			t.Errorf("checkpoint template does not contain %q:\n%s", want, block)
		}
	}
	if strings.Contains(block, "mode=remote") || strings.Contains(block, "remote is deprecated") {
		t.Fatalf("checkpoint template still advertises remote capture mode:\n%s", block)
	}
}

func TestRenderConductorConfigPreservesBundleCheckpoint(t *testing.T) {
	path := writeConductorConfig(t, `
api: { domain: config.test }
encryption_key: test-key
sandbox:
  boot: { kernel: /kernel, runtime: /runtime }
checkpoint:
  mode: bundle
  remote:
    ref_location_parent: file:///mnt/shared/snapshots
`)
	out, err := renderConductorConfig(false, false, path)
	if err != nil {
		t.Fatal(err)
	}
	var rendered config.Conductor
	if err := yaml.Unmarshal(out, &rendered); err != nil {
		t.Fatal(err)
	}
	if rendered.Checkpoint.Mode != config.CheckpointBundle ||
		rendered.Checkpoint.Remote.RefLocationParent != "file:///mnt/shared/snapshots" {
		t.Fatalf("rendered checkpoint = %+v\n%s", rendered.Checkpoint, out)
	}
}

func TestRenderConductorConfigResolvesBuilderAdmissionDefaults(t *testing.T) {
	path := writeConductorConfig(t, `
api: { domain: config.test }
encryption_key: test-key
sandbox:
  boot: { kernel: /kernel, runtime: /runtime }
builder:
  admission:
    execution:
      max_builds: 4
      resources: { cpu: 2.0001, memory: 8GiB }
`)
	out, err := renderConductorConfig(false, true, path)
	if err != nil {
		t.Fatal(err)
	}
	var rendered config.Conductor
	if err := yaml.Unmarshal(out, &rendered); err != nil {
		t.Fatal(err)
	}
	registration, err := configresolve.BuilderRegistrationLimit(rendered.Builder)
	if err != nil {
		t.Fatal(err)
	}
	execution, err := configresolve.BuilderExecutionLimit(rendered.Builder)
	if err != nil {
		t.Fatal(err)
	}
	if registration != execution || execution.MaxBuilds != 4 ||
		execution.Resources.CPU != 2001 || execution.Resources.Memory != 8<<30 {
		t.Fatalf("resolved admission: registration=%+v execution=%+v\n%s", registration, execution, out)
	}
	text := string(out)
	for _, forbidden := range []string{"max_concurrent:", "cpu_quota:", "memory_max:", "vcpu:"} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("resolved config contains %q:\n%s", forbidden, text)
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
		"allocatable: {}",
		"overhead:\n            memory: 32MiB",
	} {
		if !strings.Contains(sandboxBlock, want) {
			t.Errorf("normalized sandbox config does not contain %q:\n%s", want, sandboxBlock)
		}
	}
	if strings.Contains(sandboxBlock, "allocatable:\n            memory:") {
		t.Fatalf("normalized config made inherited allocatable.memory explicit:\n%s", sandboxBlock)
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
	reloaded, err := config.LoadConductor(normalized)
	if err != nil {
		t.Fatalf("normalized config did not round-trip: %v\n%s", err, out)
	}
	if strings.Count(string(out), "memory: 128MiB") != 1 {
		t.Fatalf("normalized config made the inherited clamp explicit:\n%s", out)
	}
	resolved, err := sandboxcfg.ResolveResources(sandboxcfg.ResourceResolveInput{
		Node: configresolve.SandboxResources(reloaded.Sandbox.Resources),
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
		Node: configresolve.SandboxResources(reloaded.Sandbox.Resources), Patch: patch,
	})
	if err != nil {
		t.Fatal(err)
	}
	if raised.Allocatable.Memory != "256MiB" {
		t.Fatalf("reloaded inherited default did not follow raised capacity: %+v\n%s", raised, out)
	}
}

func TestRenderCustomConfigIsBootstrapOnlyAndDoesNotExecute(t *testing.T) {
	custom := filepath.Join(t.TempDir(), "custom-component")
	// Deliberately not a valid executable image. Config diagnostics must only
	// validate file metadata; trying to execute it would fail with ENOEXEC.
	if err := os.WriteFile(custom, []byte("not an executable image\n"), 0o700); err != nil {
		t.Fatal(err)
	}

	conductorPath := writeConductorConfig(t, "paths:\n  conductor_executable: "+custom+"\n")
	conductor, err := renderConductorConfig(false, false, conductorPath)
	if err != nil {
		t.Fatalf("custom conductor bootstrap diagnostic: %v", err)
	}
	if !strings.HasPrefix(string(conductor), "# node-ctl declarative/bootstrap configuration is valid; runtime owner and final validation are deferred to custom conductor startup.\n") {
		t.Fatalf("custom conductor diagnostic did not distinguish bootstrap validation:\n%s", conductor)
	}

	proxyPath := filepath.Join(t.TempDir(), "proxy.yaml")
	if err := os.WriteFile(proxyPath, []byte("paths:\n  proxy_executable: "+custom+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	proxy, err := renderProxyConfig(false, proxyPath)
	if err != nil {
		t.Fatalf("custom proxy bootstrap diagnostic: %v", err)
	}
	if !strings.HasPrefix(string(proxy), "# node-ctl declarative/bootstrap configuration is valid; runtime owner and final validation are deferred to custom proxy startup.\n") {
		t.Fatalf("custom proxy diagnostic did not distinguish bootstrap validation:\n%s", proxy)
	}
}

func TestCustomConfigCommandReportsFinalValidationOnStderr(t *testing.T) {
	custom := filepath.Join(t.TempDir(), "custom-component")
	if err := os.WriteFile(custom, []byte("not an executable image\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	configPath := writeConductorConfig(t, "paths:\n  conductor_executable: "+custom+"\n")
	outputPath := filepath.Join(t.TempDir(), "normalized.yaml")

	oldStderr := os.Stderr
	readEnd, writeEnd, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stderr = writeEnd
	t.Cleanup(func() { os.Stderr = oldStderr })
	err = configCmd([]string{"conductor", "--config", configPath, "-o", outputPath}, nil)
	_ = writeEnd.Close()
	os.Stderr = oldStderr
	if err != nil {
		t.Fatal(err)
	}
	warning, readErr := io.ReadAll(readEnd)
	_ = readEnd.Close()
	if readErr != nil {
		t.Fatal(readErr)
	}
	if !strings.Contains(string(warning), "runtime owner and final validation are deferred to component startup") {
		t.Fatalf("stderr warning = %q", warning)
	}
	if _, err := os.Stat(outputPath); err != nil {
		t.Fatalf("normalized config was not written: %v", err)
	}
}

func TestConfigCommandAppliesFinalValidationOnlyForBuiltInComponents(t *testing.T) {
	conductorPath := writeConductorConfig(t, "{}")
	if _, err := renderConductorConfig(false, false, conductorPath); err == nil || !strings.Contains(err.Error(), "api.domain") {
		t.Fatalf("built-in conductor final validation error = %v", err)
	}
	proxyPath := filepath.Join(t.TempDir(), "proxy.yaml")
	if err := os.WriteFile(proxyPath, []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := renderProxyConfig(false, proxyPath); err == nil || !strings.Contains(err.Error(), "paths.run_root") {
		t.Fatalf("built-in proxy final validation error = %v", err)
	}
	if err := os.WriteFile(proxyPath, []byte("paths:\n  run_root: /run/sandbox\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := renderProxyConfig(false, proxyPath); err == nil || !strings.Contains(err.Error(), "data_listen") {
		t.Fatalf("built-in proxy data_listen validation error = %v", err)
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
