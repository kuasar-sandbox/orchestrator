package config

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCheckpointRefLocationURI(t *testing.T) {
	c := CheckpointConfig{Remote: CheckpointRemoteConfig{RefLocationParent: "file:///mnt/shared/snapshots"}}
	name := "0198f7a1-1234"
	digest := fmt.Sprintf("%x", sha256.Sum256([]byte(name)))
	want := "file:///mnt/shared/snapshots/" + digest[:2] + "/" + digest[2:4] + "/" + name
	got, err := c.RefLocationURI(name)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("RefLocationURI() = %q, want %q", got, want)
	}
}

func TestLoadRejectsInvalidCheckpointRefLocationParent(t *testing.T) {
	path := writeConfig(t, `
api:
  domain: example.test
encryption_key: test-key
sandbox:
  boot:
    kernel: /opt/sandbox/vmlinux
    runtime: /opt/sandbox/sandbox-runtime.bundle
checkpoint:
  remote:
    ref_location_parent: https://example.test/snapshots
`)
	if _, err := Load(path); err == nil || !strings.Contains(err.Error(), "ref_location_parent") {
		t.Fatalf("Load error = %v", err)
	}
}

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

func TestLoadMMDSRoutesDefaults(t *testing.T) {
	t.Setenv("NODE_CONFIG_ENCRYPTION_KEY", "")
	path := writeConfig(t, `
api:
  domain: example.test
encryption_key: test-key
sandbox:
  boot:
    kernel: /opt/sandbox/vmlinux
    runtime: /opt/sandbox/sandbox-runtime.bundle
`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if cfg.MMDS.Routes.Enabled {
		t.Fatal("MMDS.Routes.Enabled should default to false")
	}
	if got, want := cfg.MMDS.Routes.MaxRoutesPerSandbox, 32; got != want {
		t.Fatalf("MaxRoutesPerSandbox = %d, want %d", got, want)
	}
	if got, want := cfg.MMDS.Routes.Secret.MaxPerSandbox, 16; got != want {
		t.Fatalf("Secret.MaxPerSandbox = %d, want %d", got, want)
	}
	if got, want := cfg.MMDS.Routes.Service.MaxPerSandbox, 16; got != want {
		t.Fatalf("Service.MaxPerSandbox = %d, want %d", got, want)
	}
	if got, want := cfg.MMDS.Routes.Static.MaxBodyBytes, 16*1024; got != want {
		t.Fatalf("Static.MaxBodyBytes = %d, want %d", got, want)
	}
	if got, want := cfg.MMDS.Routes.MaxNamespaceBytes, 64*1024; got != want {
		t.Fatalf("MaxNamespaceBytes = %d, want %d", got, want)
	}
	if got, want := cfg.MMDS.Routes.Secret.MaxValueBytes, 16*1024; got != want {
		t.Fatalf("Secret.MaxValueBytes = %d, want %d", got, want)
	}
	if got, want := cfg.MMDS.Routes.Secret.ParkTimeout, "3s"; got != want {
		t.Fatalf("Secret.ParkTimeout = %q, want %q", got, want)
	}
	if got, want := cfg.MMDS.Routes.Secret.ParkTimeoutDur(), 3*time.Second; got != want {
		t.Fatalf("Secret.ParkTimeoutDur() = %v, want %v", got, want)
	}
}

func TestLoadRejectsMalformedMMDSParkTimeout(t *testing.T) {
	t.Setenv("NODE_CONFIG_ENCRYPTION_KEY", "")
	path := writeConfig(t, `
api:
  domain: example.test
encryption_key: test-key
sandbox:
  boot:
    kernel: /opt/sandbox/vmlinux
    runtime: /opt/sandbox/sandbox-runtime.bundle
mmds:
  routes:
    secret:
      park_timeout: soon
`)
	_, err := Load(path)
	if err == nil {
		t.Fatal("Load succeeded with a malformed mmds.routes.secret.park_timeout")
	}
	if !strings.Contains(err.Error(), "park_timeout") {
		t.Fatalf("error %q does not mention park_timeout", err)
	}
}

func TestLoadRejectsMMDSRoutesWithoutMMDSService(t *testing.T) {
	t.Setenv("NODE_CONFIG_ENCRYPTION_KEY", "")
	path := writeConfig(t, `
api:
  domain: example.test
encryption_key: test-key
sandbox:
  boot:
    kernel: /opt/sandbox/vmlinux
    runtime: /opt/sandbox/sandbox-runtime.bundle
mmds:
  routes:
    enabled: true
`)

	_, err := Load(path)
	if err == nil {
		t.Fatal("Load succeeded with MMDS routes enabled while MMDS is disabled")
	}
	if !strings.Contains(err.Error(), "mmds.routes.enabled=true requires mmds.enabled=true") {
		t.Fatalf("error %q does not describe the MMDS dependency", err)
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
    runtime: /opt/sandbox/sandbox-runtime.bundle
`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if got := cfg.Sandbox.Boot.Runtime; got != "/opt/sandbox/sandbox-runtime.bundle" {
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
    runtime: /opt/sandbox/sandbox-runtime.bundle
`)
			_, err := Load(path)
			if err == nil || !strings.Contains(err.Error(), "units.pool_wait_timeout") {
				t.Fatalf("Load error = %v, want units.pool_wait_timeout validation", err)
			}
		})
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
    runtime: /opt/sandbox/sandbox-runtime.bundle
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
    runtime: /opt/sandbox/sandbox-runtime.bundle
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
    runtime: /opt/sandbox/sandbox-runtime.bundle
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
    runtime: /opt/sandbox/sandbox-runtime.bundle
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
    runtime: /opt/sandbox/sandbox-runtime.bundle
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
    runtime: /opt/sandbox/sandbox-runtime.bundle
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
    runtime: /opt/sandbox/sandbox-runtime.bundle
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
	if err := os.WriteFile(path, []byte("paths: { run_root: /run/sandbox }\nproxy_netns: sw0_mgmt\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg, err := LoadProxy(path)
	if err != nil {
		t.Fatalf("LoadProxy failed: %v", err)
	}
	if got := cfg.ProxyNetNS; got != "sw0_mgmt" {
		t.Fatalf("proxy_netns = %q", got)
	}
	if got := cfg.Paths.RunRoot; got != "/run/sandbox" {
		t.Fatalf("paths.run_root = %q", got)
	}
}

func TestLoadProxyRPCTimeoutDefault(t *testing.T) {
	path := filepath.Join(t.TempDir(), "proxy.yaml")
	if err := os.WriteFile(path, []byte("paths: { run_root: /run/sandbox }\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg, err := LoadProxy(path)
	if err != nil {
		t.Fatalf("LoadProxy failed: %v", err)
	}
	if cfg.ProxyRPCTimeout != "2s" {
		t.Fatalf("proxy_rpc_timeout default = %q, want 2s", cfg.ProxyRPCTimeout)
	}
	if got, want := cfg.ProxyRPCTimeoutDur(), 2*time.Second; got != want {
		t.Fatalf("ProxyRPCTimeoutDur() = %v, want %v", got, want)
	}
}

func TestLoadProxyAcceptsRPCTimeout(t *testing.T) {
	path := filepath.Join(t.TempDir(), "proxy.yaml")
	if err := os.WriteFile(path, []byte("paths: { run_root: /run/sandbox }\nproxy_rpc_timeout: 500ms\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg, err := LoadProxy(path)
	if err != nil {
		t.Fatalf("LoadProxy failed: %v", err)
	}
	if got, want := cfg.ProxyRPCTimeoutDur(), 500*time.Millisecond; got != want {
		t.Fatalf("ProxyRPCTimeoutDur() = %v, want %v", got, want)
	}
}

func TestLoadProxyRejectsMalformedRPCTimeout(t *testing.T) {
	path := filepath.Join(t.TempDir(), "proxy.yaml")
	if err := os.WriteFile(path, []byte("paths: { run_root: /run/sandbox }\nproxy_rpc_timeout: soon\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	_, err := LoadProxy(path)
	if err == nil {
		t.Fatal("LoadProxy accepted a malformed proxy_rpc_timeout")
	}
	if !strings.Contains(err.Error(), "proxy_rpc_timeout") {
		t.Fatalf("error %q does not mention proxy_rpc_timeout", err)
	}
}

func TestLoadProxyRequiresRunRoot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "proxy.yaml")
	if err := os.WriteFile(path, []byte("proxy_netns: sw0_mgmt\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	_, err := LoadProxy(path)
	if err == nil {
		t.Fatal("LoadProxy succeeded without run_root")
	}
	if !strings.Contains(err.Error(), "paths.run_root is required") {
		t.Fatalf("error %q does not report required run_root", err)
	}
}

func TestLoadProxyRejectsTopLevelRunRoot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "proxy.yaml")
	if err := os.WriteFile(path, []byte("run_root: /run/sandbox\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	_, err := LoadProxy(path)
	if err == nil {
		t.Fatal("LoadProxy accepted the removed top-level run_root field")
	}
	if !strings.Contains(err.Error(), "field run_root not found") {
		t.Fatalf("error %q does not reject top-level run_root at schema decode", err)
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
