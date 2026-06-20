package clustercfg

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDefaultNeedsDomain(t *testing.T) {
	c := Default()
	if err := c.Validate(); err == nil {
		t.Fatal("expected domain-required error")
	}
	c.Domain = "sandboxes.example.com"
	if err := c.Validate(); err != nil {
		t.Fatalf("default+domain should validate: %v", err)
	}
}

func TestLoadPartialAppliesDefaults(t *testing.T) {
	path := filepath.Join(t.TempDir(), "c.yaml")
	os.WriteFile(path, []byte("domain: sb.example.com\nstore:\n  kind: sqlite\n  dsn: /tmp/r.db\nscaler:\n  place_candidates: 3\n"), 0o600)
	c, err := Load(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if c.Channel.Listen != ":7700" || c.Channel.RevisionRetention != 10000 {
		t.Fatalf("channel defaults not filled: %+v", c.Channel)
	}
	if c.Op.Listen == "" || c.Reserve.ParkTimeout != "30s" {
		t.Fatalf("op/reserve defaults not filled: %+v %+v", c.Op, c.Reserve)
	}
	if c.Scaler.PlaceCandidates != 3 || c.Scaler.ZoneAdmitMax != "yellow" {
		t.Fatalf("scaler merge wrong: %+v", c.Scaler)
	}
	// providers default to store even though scaler (a sibling) was the only group set
	if p, ext, _ := c.GroupConfig.ProviderFor(ProviderKey); p != ProviderStore || ext {
		t.Fatalf("key provider = %q ext=%v, want store", p, ext)
	}
}

func TestProviderForExternal(t *testing.T) {
	g := GroupConfigConfig{Providers: map[string]string{ProviderKey: "external:keysvc:7000"}}
	spec, ext, addr := g.ProviderFor(ProviderKey)
	if !ext || addr != "keysvc:7000" || spec != "external:keysvc:7000" {
		t.Fatalf("external parse wrong: spec=%q ext=%v addr=%q", spec, ext, addr)
	}
	// unset interface defaults to store
	if _, ext, _ := g.ProviderFor(ProviderPlacement); ext {
		t.Fatal("unset provider should default to store")
	}
}

func TestValidateRejects(t *testing.T) {
	c := Default()
	c.Domain = "d"
	c.Store.Kind = "memory" // no memory mode
	if err := c.Validate(); err == nil {
		t.Fatal("memory store should be rejected")
	}
	c = Default()
	c.Domain = "d"
	c.Reserve.ParkTimeout = "notaduration"
	if err := c.Validate(); err == nil {
		t.Fatal("bad duration should be rejected")
	}
	c = Default()
	c.Domain = "d"
	c.GroupConfig.Providers[ProviderKey] = "bogus"
	if err := c.Validate(); err == nil {
		t.Fatal("bad provider spec should be rejected")
	}
}
