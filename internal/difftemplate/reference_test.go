package difftemplate

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestNormalizeTemplateReferences(t *testing.T) {
	key := strings.Repeat("a", 64)
	path := filepath.Join(t.TempDir(), "disk.ext4")
	for _, tc := range []struct{ input, want string }{
		{"", ""},
		{path, "file://" + path},
		{"file://" + path, "file://" + path},
		{"file:///disk.tar@digest:" + key, "file:///disk.tar@digest:" + key},
		{"file://disk.tar@hmac:" + key + "@location:seeds", "file://disk.tar@hmac:" + key + "@location:seeds"},
		{"file://bundle@manifest:" + key + "@location:seeds", "file://bundle@manifest:" + key + "@location:seeds"},
		{"manifest://" + key, "manifest://" + key},
		{"mkfs://ext4?size=16MiB", "mkfs://ext4?size=16777216"},
	} {
		t.Run(tc.input, func(t *testing.T) {
			got, err := Normalize(tc.input)
			if err != nil || got != tc.want {
				t.Fatalf("Normalize(%q) = %q, %v; want %q", tc.input, got, err, tc.want)
			}
		})
	}
	for _, raw := range []string{"https://host/disk", "mkfs:ext4", "mkfs://ext4?size=bad", "file:///disk@digest:bad"} {
		if got, err := Normalize(raw); err == nil {
			t.Errorf("invalid URI %q became %q", raw, got)
		}
	}
}
