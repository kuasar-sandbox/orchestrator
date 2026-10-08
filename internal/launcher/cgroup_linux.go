package launcher

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"golang.org/x/sys/unix"
)

// SandboxCgroupPruner is implemented by launchers that delegate runner cgroups.
// An inactive/collected systemd unit can still leave an empty delegated subtree.
// Cleanup must remove that subtree before releasing the durable run owner.
type SandboxCgroupPruner interface {
	PruneSandboxCgroup(context.Context, string) error
}

var sandboxInstance = regexp.MustCompile(`^[A-Za-z0-9_.:-]+@sr-[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\.service$`)

func (s *Systemd) PruneSandboxCgroup(ctx context.Context, unit string) error {
	if !sandboxInstance.MatchString(unit) {
		return fmt.Errorf("launcher: invalid sandbox runner instance %q", unit)
	}
	root, err := os.OpenRoot("/sys/fs/cgroup/sandbox.slice/sandbox-runner.slice")
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer root.Close()
	fd, err := root.Open(".")
	if err != nil {
		return err
	}
	var stat unix.Statfs_t
	err = unix.Fstatfs(int(fd.Fd()), &stat)
	fd.Close()
	if err != nil {
		return err
	}
	if stat.Type != unix.CGROUP2_SUPER_MAGIC {
		return fmt.Errorf("launcher: sandbox runner slice is not cgroup v2")
	}
	return pruneEmptyCgroup(ctx, root, unit, root.Remove)
}

// Only rmdir is used: no tasks are killed or moved, no control files are
// changed, and a populated/busy group remains owned for the finalizer's retry.
// Root confines every lookup/removal to the fixed sandbox runner slice.
func pruneEmptyCgroup(ctx context.Context, root *os.Root, path string, remove func(string) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	info, err := root.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("launcher: cgroup %s is not a directory", path)
	}
	events, err := root.ReadFile(filepath.Join(path, "cgroup.events"))
	if err != nil {
		if _, statErr := root.Lstat(path); errors.Is(statErr, os.ErrNotExist) {
			return nil // systemd won the removal race
		}
		return fmt.Errorf("launcher: read cgroup %s: %w", path, err)
	}
	populated := ""
	for _, line := range strings.Split(string(events), "\n") {
		fields := strings.Fields(line)
		if len(fields) > 0 && fields[0] == "populated" {
			if len(fields) != 2 || populated != "" {
				return fmt.Errorf("launcher: invalid cgroup population in %s", path)
			}
			populated = fields[1]
		}
	}
	if populated != "0" {
		return fmt.Errorf("launcher: cgroup %s is populated or has unknown population", path)
	}
	dir, err := root.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	entries, err := dir.ReadDir(-1)
	dir.Close()
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("launcher: unexpected symlink in cgroup %s", path)
		}
		if entry.IsDir() {
			if err := pruneEmptyCgroup(ctx, root, filepath.Join(path, entry.Name()), remove); err != nil {
				return err
			}
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("launcher: remove empty cgroup %s: %w", path, err)
	}
	return nil
}
