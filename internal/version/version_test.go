package version

import (
	"runtime/debug"
	"testing"
)

func TestIdentity(t *testing.T) {
	const rev = "1234567890abcdef1234567890abcdef12345678"
	for _, tc := range []struct {
		name, release, tag, revision, modified, want string
	}{
		{"release", "v0.2.0", "", rev, "false", "test v0.2.0 (1234567890ab)"},
		{"tag", "v0.2.0", "v0.2.0", rev, "false", "test v0.2.0 (v0.2.0)"},
		{"preview", "v0.2.0-preview.20261126", "", rev, "false", "test v0.2.0-preview.20261126 (1234567890ab)"},
		{"modified", "dev", "", rev, "true", "test dev (1234567890ab-modified)"},
		{"modified tag", "dev", "runtime-v0.2.0", rev, "true", "test dev (runtime-v0.2.0-modified)"},
		{"unknown", "dev", "", "", "", "test dev (unknown)"},
		{"tag without revision", "dev", "v0.2.0", "", "", "test dev (unknown)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			info := &debug.BuildInfo{Settings: []debug.BuildSetting{
				{Key: "vcs.revision", Value: tc.revision},
				{Key: "vcs.modified", Value: tc.modified},
			}}
			if got := identity("test", tc.release, tc.tag, info); got != tc.want {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}
	if got := identity("test", "dev", "", nil); got != "test dev (unknown)" {
		t.Fatal(got)
	}
}
