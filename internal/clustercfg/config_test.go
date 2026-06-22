package clustercfg

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRouterNeedsDomain(t *testing.T) {
	c := DefaultRouter()
	if err := c.Validate(); err == nil {
		t.Fatal("expected domain-required error")
	}
	c.Domain = "sandboxes.example.com"
	if err := c.Validate(); err != nil {
		t.Fatalf("default router + domain should validate: %v", err)
	}
}

func TestLoadRegistryPartialAppliesDefaults(t *testing.T) {
	path := filepath.Join(t.TempDir(), "registry.yaml")
	os.WriteFile(path, []byte("state:\n  backend: sqlite\n  dsn: /tmp/r.db\nnode_link:\n  listen: \":8800\"\n"), 0o600)
	c, err := LoadRegistry(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	// node_link.listen is set in the file; its siblings must be re-filled from defaults.
	if c.NodeLink.Listen != ":8800" || c.NodeLink.RevisionRetention != 10000 || c.NodeLink.HeartbeatInterval != "10s" {
		t.Fatalf("node_link defaults not filled: %+v", c.NodeLink)
	}
	if c.ControlAPI.Listen == "" || c.Reserve.ParkTimeout != "30s" {
		t.Fatalf("control_api/reserve defaults not filled: %+v %+v", c.ControlAPI, c.Reserve)
	}
	// providers default to store.
	if p, ext, _ := c.SandboxGroup.ProviderFor(ProviderKey); p != ProviderStore || ext {
		t.Fatalf("key provider = %q ext=%v, want store", p, ext)
	}
}

func TestLoadScalerPartialAppliesDefaults(t *testing.T) {
	path := filepath.Join(t.TempDir(), "scaler.yaml")
	os.WriteFile(path, []byte("placement:\n  candidates: 3\n"), 0o600)
	c, err := LoadScaler(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if c.Placement.Candidates != 3 || c.Placement.ZoneAdmitMax != "yellow" || c.Placement.NodeDeadAfter != "30s" {
		t.Fatalf("placement merge wrong: %+v", c.Placement)
	}
	if c.Registry.Endpoint == "" {
		t.Fatal("scaler registry.endpoint default not filled")
	}
}

func TestProviderForExternal(t *testing.T) {
	g := SandboxGroupConfig{Providers: map[string]string{ProviderKey: "external:keysvc:7000"}}
	spec, ext, addr := g.ProviderFor(ProviderKey)
	if !ext || addr != "keysvc:7000" || spec != "external:keysvc:7000" {
		t.Fatalf("external parse wrong: spec=%q ext=%v addr=%q", spec, ext, addr)
	}
	// unset interface defaults to store
	if _, ext, _ := g.ProviderFor(ProviderPlacement); ext {
		t.Fatal("unset provider should default to store")
	}
}

func TestRegistryValidateRejects(t *testing.T) {
	c := DefaultRegistry()
	c.State.Backend = "memory" // no memory mode
	if err := c.Validate(); err == nil {
		t.Fatal("memory backend should be rejected")
	}
	c = DefaultRegistry()
	c.Reserve.ParkTimeout = "notaduration"
	if err := c.Validate(); err == nil {
		t.Fatal("bad duration should be rejected")
	}
	c = DefaultRegistry()
	c.SandboxGroup.Providers[ProviderKey] = "bogus"
	if err := c.Validate(); err == nil {
		t.Fatal("bad provider spec should be rejected")
	}
}
