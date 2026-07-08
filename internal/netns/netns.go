// Package netns provides the narrow network-namespace operations node-ctl needs.
package netns

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"

	"golang.org/x/sys/unix"
)

const namedNetNSDir = "/var/run/netns"

// NetNS is an open network namespace handle. Holding it pins that namespace.
type NetNS struct {
	spec string
	path string
	file *os.File
}

// Open opens a named network namespace from /var/run/netns, or an absolute
// namespace path such as /proc/<pid>/ns/net.
func Open(spec string) (*NetNS, error) {
	if spec == "" {
		return nil, fmt.Errorf("netns: empty namespace")
	}
	path := spec
	if !filepath.IsAbs(path) {
		path = filepath.Join(namedNetNSDir, spec)
	}
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("netns %s: %w", spec, err)
	}
	f := os.NewFile(uintptr(fd), path)
	if f == nil {
		_ = unix.Close(fd)
		return nil, fmt.Errorf("netns %s: invalid fd", spec)
	}
	return &NetNS{spec: spec, path: path, file: f}, nil
}

// String returns the configured namespace name or path.
func (n *NetNS) String() string {
	if n == nil {
		return ""
	}
	return n.spec
}

// Close releases the namespace handle.
func (n *NetNS) Close() error {
	if n == nil || n.file == nil {
		return nil
	}
	return n.file.Close()
}

// Do runs fn on an OS thread temporarily moved into the namespace, then restores
// the original namespace before unlocking the thread.
func (n *NetNS) Do(fn func() error) (err error) {
	if n == nil {
		return fn()
	}
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	orig, err := unix.Open("/proc/self/ns/net", unix.O_RDONLY|unix.O_CLOEXEC, 0)
	if err != nil {
		return fmt.Errorf("netns %s: open current netns: %w", n.spec, err)
	}
	defer unix.Close(orig)

	if err := unix.Setns(int(n.file.Fd()), unix.CLONE_NEWNET); err != nil {
		return fmt.Errorf("netns %s: setns: %w", n.spec, err)
	}
	defer func() {
		if r := recover(); r != nil {
			_ = unix.Setns(orig, unix.CLONE_NEWNET)
			panic(r)
		}
		if restoreErr := unix.Setns(orig, unix.CLONE_NEWNET); restoreErr != nil {
			if err != nil {
				err = fmt.Errorf("netns %s: restore current netns: %w (original error: %v)", n.spec, restoreErr, err)
				return
			}
			err = fmt.Errorf("netns %s: restore current netns: %w", n.spec, restoreErr)
		}
	}()
	return fn()
}
