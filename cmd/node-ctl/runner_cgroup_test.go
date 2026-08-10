package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

func TestPrepareRunnerCgroupAt(t *testing.T) {
	root := t.TempDir()
	unit := filepath.Join(root, "sandbox-runner.slice", "runner.service")
	mustMkdirAll(t, filepath.Join(unit, "ctl"))
	mustMkdirAll(t, filepath.Join(unit, "vmm"))
	mustWrite(t, filepath.Join(unit, "cgroup.controllers"), "cpuset cpu io memory pids\n")
	mustWrite(t, filepath.Join(unit, "cgroup.subtree_control"), "cpu\n")
	mustWrite(t, filepath.Join(unit, "cgroup.procs"), "")
	mustWrite(t, filepath.Join(unit, "vmm", "cgroup.procs"), "")

	f, err := prepareRunnerCgroupAt(root, []byte("0::/sandbox-runner.slice/runner.service/ctl\n"), func(*os.File) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if flags, err := unix.FcntlInt(f.Fd(), unix.F_GETFD, 0); err != nil || flags&unix.FD_CLOEXEC == 0 {
		t.Fatalf("vmm descriptor CLOEXEC flags=%d err=%v", flags, err)
	}
	if _, err := os.Stat(filepath.Join(unit, "vmm")); err != nil {
		t.Fatalf("vmm cgroup not created: %v", err)
	}
	if got, err := os.ReadFile(filepath.Join(unit, "cgroup.subtree_control")); err != nil || string(got) != "+memory" {
		t.Fatalf("subtree control = %q, %v", got, err)
	}
}

func TestPrepareRunnerCgroupAtFailsClosed(t *testing.T) {
	for _, tc := range []struct {
		name        string
		identity    string
		controllers string
		rootProcs   string
		want        string
	}{
		{name: "not ctl", identity: "0::/slice/runner.service\n", controllers: "cpu memory", want: "not the delegated ctl"},
		{name: "missing memory", identity: "0::/slice/runner.service/ctl\n", controllers: "cpu", want: "does not delegate memory"},
		{name: "root occupied", identity: "0::/slice/runner.service/ctl\n", controllers: "cpu memory", rootProcs: "123\n", want: "contains existing processes"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			unit := filepath.Join(root, "slice", "runner.service")
			mustMkdirAll(t, filepath.Join(unit, "ctl"))
			mustWrite(t, filepath.Join(unit, "cgroup.controllers"), tc.controllers)
			mustWrite(t, filepath.Join(unit, "cgroup.subtree_control"), "")
			mustWrite(t, filepath.Join(unit, "cgroup.procs"), tc.rootProcs)
			_, err := prepareRunnerCgroupAt(root, []byte(tc.identity), func(*os.File) error { return nil })
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestUnifiedCgroupPath(t *testing.T) {
	if got, err := unifiedCgroupPath([]byte("1:name=x:/legacy\n0::/slice/unit/ctl\n")); err != nil || got != "/slice/unit/ctl" {
		t.Fatalf("path = %q, err = %v", got, err)
	}
	for _, input := range []string{"", "0::relative", "0::/a/../ctl", "0::/a/ctl\n0::/b/ctl\n"} {
		if _, err := unifiedCgroupPath([]byte(input)); err == nil {
			t.Fatalf("invalid identity %q accepted", input)
		}
	}
}

func TestValidateCgroup2FDRejectsOrdinaryDirectory(t *testing.T) {
	f, err := os.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := validateCgroup2FD(f); err == nil || !strings.Contains(err.Error(), "not on cgroup v2") {
		t.Fatalf("error = %v", err)
	}
}

func mustMkdirAll(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(path, 0o755); err != nil {
		t.Fatal(err)
	}
}

func mustWrite(t *testing.T, path, value string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(value), 0o644); err != nil {
		t.Fatal(err)
	}
}
