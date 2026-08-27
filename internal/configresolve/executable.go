package configresolve

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	"golang.org/x/sys/unix"
)

// ValidateComponentExecutable validates the node-ctl dispatch target against
// the exact node-ctl file identity. It follows symlinks into one stable opened
// description and rejects targets writable by less-privileged users.
func ValidateComponentExecutable(component, nodeCtl string) error {
	if component == "" {
		return nil
	}
	file, err := OpenComponentExecutable(component, nodeCtl)
	if err != nil {
		return err
	}
	return file.Close()
}

// OpenComponentExecutable opens and validates the dispatch target as one
// stable file description. Callers that execute a component must execute this
// opened description instead of resolving component again by pathname.
func OpenComponentExecutable(component, nodeCtl string) (*os.File, error) {
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
	stat, ok := componentInfo.Sys().(*syscall.Stat_t)
	if !ok {
		return fail(fmt.Errorf("component executable ownership is unavailable"))
	}
	if !sameComponentOwner(stat.Uid) {
		return fail(fmt.Errorf("component executable must be owned by the node-ctl effective user"))
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

func sameComponentOwner(owner uint32) bool {
	return owner == uint32(os.Geteuid())
}
