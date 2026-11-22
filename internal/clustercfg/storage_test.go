package clustercfg

import (
	"os"
	"path/filepath"
	"testing"
)

func TestStorageNamespaceDefaultsPreserveLegacyIsolation(t *testing.T) {
	for _, tc := range []struct{ config, want string }{
		{"foundationdb:\n  prefix: isolated/\n", "isolated/records-v2/"},
		{"foundationdb:\n  prefix: isolated/\nstorage:\n  namespace: explicit/\n", "explicit/"},
		{"storage:\n  backend: postgres\n", "kuasar-sandbox/cluster/records-v2/"},
	} {
		path := filepath.Join(t.TempDir(), "registry.yaml")
		if err := os.WriteFile(path, []byte(tc.config), 0600); err != nil {
			t.Fatal(err)
		}
		cfg, err := LoadRegistry(path)
		if err != nil || cfg.Storage.Namespace != tc.want {
			t.Fatalf("namespace: %+v %v, want %s", cfg, err, tc.want)
		}
	}
}

func TestSQLiteRejectsMultipleRegistryMembers(t *testing.T) {
	cfg := DefaultRegistry()
	cfg.Storage.Backend = "sqlite"
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	cfg.Membership.Versions[0].Members = append(cfg.Membership.Versions[0].Members, MembershipMember{ID: "registry-b", Advertise: "http://registry-b:7700", NodeAdvertise: "http://registry-b:7700"})
	if err := cfg.Validate(); err == nil {
		t.Fatal("SQLite accepted a multi-Registry topology")
	}
	cfg.Storage.Backend = "postgres"
	if err := cfg.Validate(); err != nil {
		t.Fatalf("shared PostgreSQL rejected topology: %v", err)
	}
}
