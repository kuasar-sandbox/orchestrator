package config

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
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

func TestLoadCheckpointPolicyTriState(t *testing.T) {
	base := `
api:
  domain: example.test
encryption_key: test-key
sandbox:
  boot:
    kernel: /opt/sandbox/vmlinux
    runtime: /opt/sandbox/sandbox-runtime.bundle
`

	omitted, err := Load(writeConfig(t, base))
	if err != nil {
		t.Fatal(err)
	}
	if omitted.Checkpoint.Mode != CheckpointLocal || omitted.Checkpoint.MergeRef != nil || omitted.Checkpoint.DropCaches != nil {
		t.Fatalf("omitted checkpoint policy = %+v", omitted.Checkpoint)
	}

	nulls, err := Load(writeConfig(t, base+`
checkpoint:
  mode: local
  merge_ref: null
  drop_caches: null
`))
	if err != nil {
		t.Fatal(err)
	}
	if nulls.Checkpoint.MergeRef != nil || nulls.Checkpoint.DropCaches != nil {
		t.Fatalf("YAML null did not remain nil: %+v", nulls.Checkpoint)
	}

	for _, mergeRef := range []bool{false, true} {
		for _, dropCaches := range []bool{false, true} {
			name := fmt.Sprintf("merge_%t_drop_%t", mergeRef, dropCaches)
			t.Run(name, func(t *testing.T) {
				cfg, err := Load(writeConfig(t, base+fmt.Sprintf(`
checkpoint:
  mode: local
  merge_ref: %t
  drop_caches: %t
`, mergeRef, dropCaches)))
				if err != nil {
					t.Fatal(err)
				}
				if cfg.Checkpoint.MergeRef == nil || *cfg.Checkpoint.MergeRef != mergeRef ||
					cfg.Checkpoint.DropCaches == nil || *cfg.Checkpoint.DropCaches != dropCaches {
					t.Fatalf("loaded checkpoint policy = %+v", cfg.Checkpoint)
				}
			})
		}
	}
}

func TestLoadRemoteCheckpointCompatibilityAndPolicyRejection(t *testing.T) {
	base := `
api:
  domain: example.test
encryption_key: test-key
sandbox:
  boot:
    kernel: /opt/sandbox/vmlinux
    runtime: /opt/sandbox/sandbox-runtime.bundle
checkpoint:
  mode: remote
`
	for name, suffix := range map[string]string{
		"legacy direct": "",
		"legacy local parent": `
  remote:
    ref_location_parent: file:///mnt/checkpoints
`,
		"null fields": `
  merge_ref: null
  drop_caches: null
`,
	} {
		t.Run(name, func(t *testing.T) {
			cfg, err := Load(writeConfig(t, base+suffix))
			if err != nil {
				t.Fatalf("Load remote compatibility config: %v", err)
			}
			if cfg.Checkpoint.Mode != CheckpointRemote {
				t.Fatalf("mode = %q", cfg.Checkpoint.Mode)
			}
		})
	}
	for name, suffix := range map[string]string{
		"merge true":     "  merge_ref: true\n",
		"merge false":    "  merge_ref: false\n",
		"drop true":      "  drop_caches: true\n",
		"drop false":     "  drop_caches: false\n",
		"both specified": "  merge_ref: false\n  drop_caches: true\n",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := Load(writeConfig(t, base+suffix))
			if err == nil || !strings.Contains(err.Error(), "require checkpoint.mode=local") {
				t.Fatalf("Load error = %v", err)
			}
		})
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
