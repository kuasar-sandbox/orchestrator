//go:build linux && (amd64 || arm64)

package workspaceprobe

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestProbeUsesAllMountTargetsAndCleansGuestTmpfs(t *testing.T) {
	root := t.TempDir()
	alias := filepath.Join(root, "alias with : and whitespace")
	if err := os.Symlink(root, alias); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name    string
		targets []string
		success bool
	}{
		{"symlink and unusual name", []string{root, alias}, true},
		{"unlink-only cleanup", nil, true},
		{"missing second target", []string{root, filepath.Join(root, "missing : target")}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir, err := os.MkdirTemp(root, "probe-")
			if err != nil {
				t.Fatal(err)
			}
			bin := filepath.Join(dir, "probe")
			if err := os.WriteFile(bin, image, 0500); err != nil {
				t.Fatal(err)
			}
			err = exec.Command(bin, append([]string{dir}, tc.targets...)...).Run()
			if (err == nil) != tc.success {
				t.Fatalf("%v: %v", tc.targets, err)
			}
			if _, err := os.Stat(dir); !os.IsNotExist(err) {
				t.Fatal("probe did not remove its staging directory", err)
			}
		})
	}
}

func TestDrainRetriesUnlinkAfterExecTransportFailure(t *testing.T) {
	root := t.TempDir()
	bin := filepath.Join(root, "sandbox-ctl")
	calls := filepath.Join(root, "calls")
	t.Setenv("KS_TEST_PROBE_CALLS", calls)
	// Simulate the link disappearing after extraction, but before the probe
	// is started. The recovery exec receives only its own directory argument.
	fixture := `#!/bin/sh
while [ "$1" != -- ]; do shift; done
shift
case "$1" in
    /opt/sandbox-runtime/bin/flatten-ctl) cat >/dev/null; exit 0;;
esac
printf '%s\n' "$#" "$1" "$2" >> "$KS_TEST_PROBE_CALLS"
[ "$#" = 2 ]
`
	if err := os.WriteFile(bin, []byte(fixture), 0700); err != nil {
		t.Fatal(err)
	}
	if err := Drain(context.Background(), bin, root, "sandbox", []string{"/data"}); err == nil {
		t.Fatal("lost exec reported success")
	}
	data, err := os.ReadFile(calls)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 6 || lines[0] != "3" || lines[3] != "2" || lines[1] != lines[4] || lines[2] != lines[5] || lines[1] != lines[2]+"/probe" {
		t.Fatalf("unexpected recovery exec %q", lines)
	}
}
