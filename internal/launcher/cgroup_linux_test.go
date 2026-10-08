package launcher

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"golang.org/x/sys/unix"
)

const testSandboxUnit = "sandbox-runner-test@sr-00000000-0000-7000-8000-000000000001.service"

func cgroupFixture(t *testing.T) (string, *os.Root, func(string) error) {
	t.Helper()
	dir := t.TempDir()
	for _, path := range []string{testSandboxUnit, testSandboxUnit + "/ctl", testSandboxUnit + "/vmm", "other.service"} {
		if err := os.MkdirAll(filepath.Join(dir, path), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, path, "cgroup.events"), []byte("populated 0\nfrozen 0\n"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { root.Close() })
	// A real cgroup rmdir removes its virtual control files implicitly.
	// Preserve ordinary directory removal semantics for child subgroups.
	remove := func(path string) error {
		if err := root.Remove(filepath.Join(path, "cgroup.events")); err != nil {
			return err
		}
		return root.Remove(path)
	}
	return dir, root, remove
}

func TestPruneEmptyCgroupRemovesOnlyExactUnitBottomUp(t *testing.T) {
	_, root, remove := cgroupFixture(t)
	var removed []string
	err := pruneEmptyCgroup(context.Background(), root, testSandboxUnit, func(path string) error {
		removed = append(removed, path)
		return remove(path)
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{testSandboxUnit + "/ctl", testSandboxUnit + "/vmm", testSandboxUnit}
	if len(removed) != 3 || removed[2] != testSandboxUnit {
		t.Fatalf("unit root removed before its children: %v", removed)
	}
	slices.Sort(removed[:2]) // sibling enumeration order is filesystem-defined
	if !slices.Equal(removed, want) {
		t.Fatalf("removals = %v, want %v", removed, want)
	}
	if _, err := root.Stat("other.service"); err != nil {
		t.Fatalf("unrelated unit changed: %v", err)
	}
	if err := pruneEmptyCgroup(context.Background(), root, testSandboxUnit, remove); err != nil {
		t.Fatalf("already removed unit: %v", err)
	}
}

func TestPruneCgroupRejectsLiveOrUnknownPopulation(t *testing.T) {
	for _, events := range []string{"populated 1\n", "frozen 0\n", "populated unknown\n", "populated 0\npopulated 0\n", "populated 0 extra\n"} {
		t.Run(events, func(t *testing.T) {
			dir, root, remove := cgroupFixture(t)
			if err := os.WriteFile(filepath.Join(dir, testSandboxUnit, "cgroup.events"), []byte(events), 0600); err != nil {
				t.Fatal(err)
			}
			called := false
			err := pruneEmptyCgroup(context.Background(), root, testSandboxUnit, func(path string) error { called = true; return remove(path) })
			if err == nil || called {
				t.Fatalf("unsafe population allowed removal: err=%v removed=%t", err, called)
			}
		})
	}
}

func TestPruneCgroupBusyLeafRetainsRootForRetry(t *testing.T) {
	_, root, remove := cgroupFixture(t)
	err := pruneEmptyCgroup(context.Background(), root, testSandboxUnit, func(path string) error {
		if path == testSandboxUnit+"/vmm" {
			return unix.EBUSY
		}
		return remove(path)
	})
	if !errors.Is(err, unix.EBUSY) {
		t.Fatalf("busy leaf error = %v", err)
	}
	if _, err := root.Stat(testSandboxUnit); err != nil {
		t.Fatalf("busy leaf lost its unit root: %v", err)
	}
	if err := pruneEmptyCgroup(context.Background(), root, testSandboxUnit, remove); err != nil {
		t.Fatalf("retry after leaf drained: %v", err)
	}
}

func TestPruneCgroupRejectsLiveChildAndEscapingLink(t *testing.T) {
	for _, link := range []bool{false, true} {
		dir, root, remove := cgroupFixture(t)
		if link {
			if err := os.Symlink(filepath.Join(dir, "other.service"), filepath.Join(dir, testSandboxUnit, "escape")); err != nil {
				t.Fatal(err)
			}
		} else {
			if err := os.WriteFile(filepath.Join(dir, testSandboxUnit, "vmm/cgroup.events"), []byte("populated 1\n"), 0600); err != nil {
				t.Fatal(err)
			}
		}
		if err := pruneEmptyCgroup(context.Background(), root, testSandboxUnit, remove); err == nil {
			t.Fatalf("unsafe descendant accepted (symlink=%t)", link)
		}
		if _, err := root.Stat(testSandboxUnit); err != nil {
			t.Fatalf("unsafe descendant lost root: %v", err)
		}
		if _, err := root.Stat("other.service"); err != nil {
			t.Fatalf("unrelated unit changed: %v", err)
		}
	}
}

func TestPruneCgroupCancellationAndConcurrentRemoval(t *testing.T) {
	_, root, remove := cgroupFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := pruneEmptyCgroup(ctx, root, testSandboxUnit, remove); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled cleanup = %v", err)
	}
	if err := pruneEmptyCgroup(context.Background(), root, testSandboxUnit, func(path string) error {
		if err := remove(path); err != nil {
			return err
		}
		return os.ErrNotExist // systemd removed it between inspection and rmdir
	}); err != nil {
		t.Fatalf("concurrent systemd removal: %v", err)
	}
}

func TestPruneSandboxCgroupRejectsNonRunnerPaths(t *testing.T) {
	for _, unit := range []string{"", ".", "..", "other.service", "../" + testSandboxUnit, "/" + testSandboxUnit, "sandbox-runner@sr-invalid.service"} {
		if err := (&Systemd{}).PruneSandboxCgroup(context.Background(), unit); err == nil {
			t.Fatalf("invalid instance accepted: %q", unit)
		}
	}
}
