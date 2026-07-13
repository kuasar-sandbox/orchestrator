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

func TestLoadRejectsNonPositivePoolWaitTimeout(t *testing.T) {
	t.Setenv("NODE_CONFIG_ENCRYPTION_KEY", "")
	for _, value := range []string{"0s", "-1s"} {
		t.Run(value, func(t *testing.T) {
			path := writeConfig(t, `
api:
  domain: example.test
encryption_key: test-key
units:
  pool_wait_timeout: `+value+`
sandbox:
  boot:
    kernel: /opt/sandbox/vmlinux
    runtime: /opt/sandbox/sandbox-runtime.erofs
`)
			_, err := Load(path)
			if err == nil || !strings.Contains(err.Error(), "units.pool_wait_timeout") {
				t.Fatalf("Load error = %v, want units.pool_wait_timeout validation", err)
			}
		})
	}
}

func TestLoadSandboxRestoreFileRefsDefaults(t *testing.T) {
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
	if got := cfg.Sandbox.Restore.FileRefs; got != RestoreFileRefsVerify {
		t.Fatalf("sandbox.restore.file_refs default = %q, want %q", got, RestoreFileRefsVerify)
	}
}

func TestLoadRejectsInvalidSandboxRestoreFileRefs(t *testing.T) {
	t.Setenv("NODE_CONFIG_ENCRYPTION_KEY", "")
	path := writeConfig(t, `
api:
  domain: example.test
encryption_key: test-key
sandbox:
  restore:
    file_refs: maybe
  boot:
    kernel: /opt/sandbox/vmlinux
    runtime: /opt/sandbox/sandbox-runtime.erofs
`)

	_, err := Load(path)
	if err == nil {
		t.Fatal("Load succeeded with invalid sandbox.restore.file_refs")
	}
	if !strings.Contains(err.Error(), "sandbox.restore.file_refs") {
		t.Fatalf("error %q does not mention sandbox.restore.file_refs", err)
	}
}

func TestLoadBuilderRefererDefaults(t *testing.T) {
	t.Setenv("NODE_CONFIG_ENCRYPTION_KEY", "")
	path := writeConfig(t, `
api:
  domain: example.test
encryption_key: test-key
sandbox:
  boot:
    kernel: /opt/sandbox/vmlinux
    runtime: /opt/sandbox/sandbox-runtime.erofs
builder:
  referer:
    enabled: true
    desc: acme-prod
`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if !cfg.Builder.Referer.FallbackEnabled() || !cfg.Builder.Referer.WritebackEnabled() {
		t.Fatalf("referer defaults not enabled: %+v", cfg.Builder.Referer)
	}
	if cfg.Builder.Referer.Key != "acme-prod" {
		t.Fatalf("referer key default = %q", cfg.Builder.Referer.Key)
	}
}

func TestLoadRejectsBuilderRefererWithoutDesc(t *testing.T) {
	t.Setenv("NODE_CONFIG_ENCRYPTION_KEY", "")
	path := writeConfig(t, `
api:
  domain: example.test
encryption_key: test-key
sandbox:
  boot:
    kernel: /opt/sandbox/vmlinux
    runtime: /opt/sandbox/sandbox-runtime.erofs
builder:
  referer:
    enabled: true
`)

	_, err := Load(path)
	if err == nil {
		t.Fatal("Load succeeded with referer enabled but no desc")
	}
	if !strings.Contains(err.Error(), "builder.referer.desc") {
		t.Fatalf("error %q does not mention builder.referer.desc", err)
	}
}

func TestLoadRejectsBuilderRefererInvalidValidity(t *testing.T) {
	t.Setenv("NODE_CONFIG_ENCRYPTION_KEY", "")
	for _, validity := range []string{"soon", "0s", "-1h"} {
		t.Run(validity, func(t *testing.T) {
			path := writeConfig(t, `
api:
  domain: example.test
encryption_key: test-key
sandbox:
  boot:
    kernel: /opt/sandbox/vmlinux
    runtime: /opt/sandbox/sandbox-runtime.erofs
builder:
  referer:
    validity: `+validity+`
`)

			_, err := Load(path)
			if err == nil {
				t.Fatal("Load succeeded with invalid referer validity")
			}
			if !strings.Contains(err.Error(), "builder.referer.validity") {
				t.Fatalf("error %q does not mention builder.referer.validity", err)
			}
		})
	}
}

func TestLoadAcceptsTapFDSocket(t *testing.T) {
	t.Setenv("NODE_CONFIG_ENCRYPTION_KEY", "")
	path := writeConfig(t, `
api:
  domain: example.test
encryption_key: test-key
sandbox:
  network:
    tapfd_socket: /run/kuasar/connector/sw0/tapfd.sock
  boot:
    kernel: /opt/sandbox/vmlinux
    runtime: /opt/sandbox/sandbox-runtime.erofs
`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if got := cfg.Sandbox.Network.TapFDSocket; got != "/run/kuasar/connector/sw0/tapfd.sock" {
		t.Fatalf("tapfd_socket = %q", got)
	}
}

func TestLoadRejectsRelativeTapFDSocket(t *testing.T) {
	t.Setenv("NODE_CONFIG_ENCRYPTION_KEY", "")
	path := writeConfig(t, `
api:
  domain: example.test
encryption_key: test-key
sandbox:
  network:
    tapfd_socket: tapfd.sock
  boot:
    kernel: /opt/sandbox/vmlinux
    runtime: /opt/sandbox/sandbox-runtime.erofs
`)

	_, err := Load(path)
	if err == nil {
		t.Fatal("Load succeeded with relative tapfd_socket")
	}
	if !strings.Contains(err.Error(), "tapfd_socket") {
		t.Fatalf("error %q does not mention tapfd_socket", err)
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
