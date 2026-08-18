package main

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

const testRunnerRunID = "sr-00000000-0000-7000-8000-000000000001"

func TestPrepareRunnerCgroupAtMovesServiceRootToCtl(t *testing.T) {
	root, unit, unitRel := newRunnerCgroupFixture(t, "cpuset cpu io memory pids\n", "4242\n", false)
	identity := "0::" + unitRel + "\n"
	moves := 0

	f, err := prepareRunnerCgroupAt(
		root,
		testRunnerRunID,
		func() ([]byte, error) { return []byte(identity), nil },
		func(path string) error {
			moves++
			if want := filepath.Join(unit, "ctl", "cgroup.procs"); path != want {
				t.Fatalf("move path = %q, want %q", path, want)
			}
			mustWrite(t, path, "4242\n")
			mustWrite(t, filepath.Join(unit, "cgroup.procs"), "")
			identity = "0::" + filepath.Join(unitRel, "ctl") + "\n"
			return nil
		},
		func(*os.File) error { return nil },
	)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if moves != 1 {
		t.Fatalf("move count = %d, want 1", moves)
	}
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

func TestPrepareRunnerCgroupAtAcceptsAlreadyPlacedCtl(t *testing.T) {
	root, _, unitRel := newRunnerCgroupFixture(t, "cpu memory\n", "", true)
	f, err := prepareRunnerCgroupAt(
		root,
		testRunnerRunID,
		func() ([]byte, error) { return []byte("0::" + filepath.Join(unitRel, "ctl") + "\n"), nil },
		func(string) error { return errors.New("move must not be called") },
		func(*os.File) error { return nil },
	)
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
}

func TestPrepareRunnerCgroupAtRejectsRemainingRootProcess(t *testing.T) {
	root, unit, unitRel := newRunnerCgroupFixture(t, "cpu memory\n", "4242\n9999\n", false)
	identity := "0::" + unitRel + "\n"
	_, err := prepareRunnerCgroupAt(
		root,
		testRunnerRunID,
		func() ([]byte, error) { return []byte(identity), nil },
		func(path string) error {
			mustWrite(t, path, "4242\n")
			mustWrite(t, filepath.Join(unit, "cgroup.procs"), "9999\n")
			identity = "0::" + filepath.Join(unitRel, "ctl") + "\n"
			return nil
		},
		func(*os.File) error { return nil },
	)
	if err == nil || !strings.Contains(err.Error(), "contains existing processes") {
		t.Fatalf("error = %v, want remaining root process rejection", err)
	}
}

func TestPrepareRunnerCgroupAtRejectsMissingDelegation(t *testing.T) {
	for _, tc := range []struct {
		name        string
		controllers string
		want        string
	}{
		{name: "cpu", controllers: "memory", want: "does not delegate cpu"},
		{name: "memory", controllers: "cpu", want: "does not delegate memory"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root, _, unitRel := newRunnerCgroupFixture(t, tc.controllers, "", true)
			_, err := prepareRunnerCgroupAt(
				root,
				testRunnerRunID,
				func() ([]byte, error) { return []byte("0::" + filepath.Join(unitRel, "ctl") + "\n"), nil },
				func(string) error { return errors.New("move must not be called") },
				func(*os.File) error { return nil },
			)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestPrepareRunnerCgroupAtRejectsFailedMove(t *testing.T) {
	root, unit, unitRel := newRunnerCgroupFixture(t, "cpu memory", "4242\n", false)
	_, err := prepareRunnerCgroupAt(
		root,
		testRunnerRunID,
		func() ([]byte, error) { return []byte("0::" + unitRel + "\n"), nil },
		func(string) error { return errors.New("write denied") },
		func(*os.File) error { return nil },
	)
	if err == nil || !strings.Contains(err.Error(), "move runner process to ctl subgroup: write denied") {
		t.Fatalf("error = %v, want move failure", err)
	}
	if _, statErr := os.Stat(filepath.Join(unit, "ctl")); !os.IsNotExist(statErr) {
		t.Fatalf("failed move retained ctl subgroup: %v", statErr)
	}
}

func TestPrepareRunnerCgroupAtRejectsPostMoveIdentityMismatch(t *testing.T) {
	root, unit, unitRel := newRunnerCgroupFixture(t, "cpu memory", "4242\n", false)
	identity := "0::" + unitRel + "\n"
	_, err := prepareRunnerCgroupAt(
		root,
		testRunnerRunID,
		func() ([]byte, error) { return []byte(identity), nil },
		func(path string) error {
			mustWrite(t, path, "4242\n")
			mustWrite(t, filepath.Join(unit, "cgroup.procs"), "")
			identity = "0::" + filepath.Join(unitRel, "other") + "\n"
			return nil
		},
		func(*os.File) error { return nil },
	)
	if err == nil || !strings.Contains(err.Error(), "after ctl placement") {
		t.Fatalf("error = %v, want post-move identity rejection", err)
	}
}

func TestRunnerUnitCgroupRequiresExactRunAndSlice(t *testing.T) {
	validRoot := "/sandbox.slice/sandbox-runner.slice/custom-runner@" + testRunnerRunID + ".service"
	for _, path := range []string{validRoot, validRoot + "/ctl"} {
		if _, _, err := runnerUnitCgroup(path, testRunnerRunID); err != nil {
			t.Fatalf("valid runner cgroup %q rejected: %v", path, err)
		}
	}
	for _, path := range []string{
		"/sandbox.slice/sandbox-runner.slice/custom-runner@sr-other.service",
		"/sandbox.slice/other.slice/custom-runner@" + testRunnerRunID + ".service",
		"/other.slice/sandbox-runner.slice/custom-runner@" + testRunnerRunID + ".service",
		"/nested/sandbox.slice/sandbox-runner.slice/custom-runner@" + testRunnerRunID + ".service",
		validRoot + "/vmm",
		validRoot + "/ctl/nested",
		"/",
	} {
		if _, _, err := runnerUnitCgroup(path, testRunnerRunID); err == nil {
			t.Fatalf("unexpected runner cgroup %q accepted", path)
		}
	}
}

func TestTaskUnitCgroupAcceptsBuilderOnlyInBuilderSlice(t *testing.T) {
	runID := "br-00000000-0000-7000-8000-000000000001"
	validRoot := "/sandbox.slice/sandbox-builder.slice/sandbox-builder@" + runID + ".service"
	for _, path := range []string{validRoot, validRoot + "/ctl"} {
		if _, _, err := taskUnitCgroup(path, runID, "/sandbox.slice/sandbox-builder.slice"); err != nil {
			t.Fatalf("valid builder cgroup %q rejected: %v", path, err)
		}
	}
	if _, _, err := taskUnitCgroup(validRoot, runID, "/sandbox.slice/sandbox-runner.slice"); err == nil {
		t.Fatalf("builder cgroup %q accepted as a runner", validRoot)
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

func newRunnerCgroupFixture(t *testing.T, controllers, rootProcs string, withCtl bool) (string, string, string) {
	t.Helper()
	root := t.TempDir()
	unitRel := "/sandbox.slice/sandbox-runner.slice/custom-runner@" + testRunnerRunID + ".service"
	unit := filepath.Join(root, strings.TrimPrefix(unitRel, "/"))
	mustMkdirAll(t, filepath.Join(unit, "vmm"))
	mustWrite(t, filepath.Join(unit, "cgroup.controllers"), controllers)
	mustWrite(t, filepath.Join(unit, "cgroup.subtree_control"), "cpu\n")
	mustWrite(t, filepath.Join(unit, "cgroup.procs"), rootProcs)
	mustWrite(t, filepath.Join(unit, "vmm", "cgroup.procs"), "")
	if withCtl {
		mustMkdirAll(t, filepath.Join(unit, "ctl"))
		mustWrite(t, filepath.Join(unit, "ctl", "cgroup.procs"), "4242\n")
	}
	return root, unit, unitRel
}
