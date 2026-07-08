package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadRejectsOldRuntimeFields(t *testing.T) {
	t.Setenv("NODE_CONFIG_ENCRYPTION_KEY", "")
	path := writeConfig(t, `
api:
  domain: example.test
encryption_key: test-key
sandbox:
  boot:
    kernel: /opt/sandbox/vmlinux
    runtime_e2b: /opt/sandbox/runtime-e2b.erofs
`)

	_, err := Load(path)
	if err == nil {
		t.Fatal("Load succeeded with obsolete runtime_e2b field")
	}
	if !strings.Contains(err.Error(), "runtime_e2b") {
		t.Fatalf("error %q does not mention obsolete field", err)
	}
}

func TestLoadRequiresRuntime(t *testing.T) {
	t.Setenv("NODE_CONFIG_ENCRYPTION_KEY", "")
	path := writeConfig(t, `
api:
  domain: example.test
encryption_key: test-key
sandbox:
  boot:
    kernel: /opt/sandbox/vmlinux
`)

	_, err := Load(path)
	if err == nil {
		t.Fatal("Load succeeded without sandbox.boot.runtime")
	}
	if !strings.Contains(err.Error(), "sandbox.boot.runtime") {
		t.Fatalf("error %q does not mention sandbox.boot.runtime", err)
	}
}

func TestLoadAcceptsSingleRuntime(t *testing.T) {
	t.Setenv("NODE_CONFIG_ENCRYPTION_KEY", "")
	path := writeConfig(t, `
api:
  domain: example.test
encryption_key: test-key
sandbox:
  boot:
    kernel: /opt/sandbox/vmlinux
    runtime: /opt/sandbox/sandbox-runtime.erofs
`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if got := cfg.Sandbox.Boot.Runtime; got != "/opt/sandbox/sandbox-runtime.erofs" {
		t.Fatalf("runtime = %q", got)
	}
}

func TestLoadAcceptsInternalProxyNetNS(t *testing.T) {
	t.Setenv("NODE_CONFIG_ENCRYPTION_KEY", "")
	path := writeConfig(t, `
api:
  domain: example.test
encryption_key: test-key
proxy:
  mode: internal
  proxy_netns: sw0_mgmt
sandbox:
  boot:
    kernel: /opt/sandbox/vmlinux
    runtime: /opt/sandbox/sandbox-runtime.erofs
`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if got := cfg.Proxy.ProxyNetNS; got != "sw0_mgmt" {
		t.Fatalf("proxy.proxy_netns = %q", got)
	}
}

func TestLoadRejectsExternalConductorProxyNetNS(t *testing.T) {
	t.Setenv("NODE_CONFIG_ENCRYPTION_KEY", "")
	path := writeConfig(t, `
api:
  domain: example.test
encryption_key: test-key
proxy:
  mode: external
  proxy_netns: sw0_mgmt
sandbox:
  boot:
    kernel: /opt/sandbox/vmlinux
    runtime: /opt/sandbox/sandbox-runtime.erofs
`)

	_, err := Load(path)
	if err == nil {
		t.Fatal("Load succeeded with proxy.proxy_netns in external mode")
	}
	if !strings.Contains(err.Error(), "proxy.proxy_netns") {
		t.Fatalf("error %q does not mention proxy.proxy_netns", err)
	}
}

func TestLoadProxyAcceptsProxyNetNS(t *testing.T) {
	path := filepath.Join(t.TempDir(), "proxy.yaml")
	if err := os.WriteFile(path, []byte("proxy_netns: sw0_mgmt\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg, err := LoadProxy(path)
	if err != nil {
		t.Fatalf("LoadProxy failed: %v", err)
	}
	if got := cfg.ProxyNetNS; got != "sw0_mgmt" {
		t.Fatalf("proxy_netns = %q", got)
	}
}

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "node.yaml")
	if err := os.WriteFile(path, []byte(strings.TrimSpace(body)+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
