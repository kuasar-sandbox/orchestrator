package configresolve

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	"golang.org/x/sys/unix"
)

// ValidateComponentExecutableMetadata performs configuration-time static
// diagnosis without applying the eventual service process's owner policy.
// Runtime dispatch reopens and validates the selected file exactly once in
// OpenComponentExecutable before executing that same file description.
func ValidateComponentExecutableMetadata(component, nodeCtl string) error {
	if component == "" {
		return nil
	}
	file, err := openComponentExecutable(component, nodeCtl, false)
	if err != nil {
		return err
	}
	return file.Close()
}

// OpenComponentExecutable opens and validates the dispatch target as one
// stable file description. Callers that execute a component must execute this
// opened description instead of resolving component again by pathname.
func OpenComponentExecutable(component, nodeCtl string) (*os.File, error) {
	return openComponentExecutable(component, nodeCtl, true)
}

func openComponentExecutable(component, nodeCtl string, validateOwner bool) (*os.File, error) {
	if !filepath.IsAbs(component) {
		return nil, fmt.Errorf("component executable must be absolute")
	}
	fd, err := unix.Open(component, unix.O_PATH|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("open component executable: %w", err)
	}
	file := os.NewFile(uintptr(fd), component)
	if file == nil {
		_ = unix.Close(fd)
		return nil, fmt.Errorf("open component executable: invalid descriptor")
	}
	fail := func(err error) (*os.File, error) {
		_ = file.Close()
		return nil, err
	}
	componentInfo, err := file.Stat()
	if err != nil {
		return fail(fmt.Errorf("stat component executable: %w", err))
	}
	if !componentInfo.Mode().IsRegular() {
		return fail(fmt.Errorf("component executable must be a regular file"))
	}
	if componentInfo.Mode().Perm()&0o111 == 0 {
		return fail(fmt.Errorf("component executable is not executable"))
	}
	if componentInfo.Mode().Perm()&0o022 != 0 {
		return fail(fmt.Errorf("component executable must not be group/world writable"))
	}
	if validateOwner {
		stat, ok := componentInfo.Sys().(*syscall.Stat_t)
		if !ok {
			return fail(fmt.Errorf("component executable ownership is unavailable"))
		}
		if err := validateComponentOwner(stat.Uid, uint32(os.Geteuid())); err != nil {
			return fail(err)
		}
	}
	if nodeCtl == "" {
		return fail(fmt.Errorf("node-ctl executable path is required"))
	}
	nodeInfo, err := os.Stat(nodeCtl)
	if err != nil {
		return fail(fmt.Errorf("stat node-ctl executable: %w", err))
	}
	if os.SameFile(componentInfo, nodeInfo) {
		return fail(fmt.Errorf("component executable must not be the node-ctl executable"))
	}
	return file, nil
}

func validateComponentOwner(owner, euid uint32) error {
	if euid == 0 {
		if owner != 0 {
			return fmt.Errorf("component executable must be root-owned when node-ctl runs as root")
		}
		return nil
	}
	if owner != 0 && owner != euid {
		return fmt.Errorf("component executable must be owned by root or the node-ctl effective user")
	}
	return nil
}
