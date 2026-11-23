package clustercfg

import (
	"os"
	"path/filepath"
	"testing"
)

func TestGroupImportConfigCutover(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	for _, field := range []string{"candidates: 2", "zone_admit_max: yellow", "selector_patch_refresh_interval: 1m"} {
		if err := os.WriteFile(path, []byte("placement:\n  "+field+"\n"), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadPlacer(path); err == nil {
			t.Fatalf("retired Placer field accepted: %s", field)
		}
	}
	if err := os.WriteFile(path, []byte("placement:\n  import_refresh_interval: 2m\n"), 0600); err != nil {
		t.Fatal(err)
	}
	placer, err := LoadPlacer(path)
	if err != nil || placer.Placement.ImportRefresh != "2m" || len(placer.ImportGroups) != 0 {
		t.Fatalf("optional imports: %v", err)
	}
	if err := os.WriteFile(path, []byte("placement:\n  candidates: 3\n  zone_admit_max: red\n"), 0600); err != nil {
		t.Fatal(err)
	}
	registry, err := LoadRegistry(path)
	if err != nil || registry.Placement.Candidates != 3 || registry.Placement.ZoneAdmitMax != "red" {
		t.Fatalf("Registry policy: %v", err)
	}
}
