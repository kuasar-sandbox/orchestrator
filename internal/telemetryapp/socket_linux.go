package telemetryapp

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/sys/unix"
)

// Hold an exclusive sidecar lock for the lifetime of the query listener. A
// second process cannot unlink a live predecessor. Only stale socket files
// (never regular files or symlinks) can be removed by the lock owner.
func listenQuery(path string) (net.Listener, func(), error) {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, nil, err
	}
	lockFD, err := unix.Open(path+".lock", unix.O_CREAT|unix.O_RDWR|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0600)
	if err != nil {
		return nil, nil, err
	}
	lock := os.NewFile(uintptr(lockFD), path+".lock")
	release := func() { _ = lock.Close() } // Keep the inode: unlinking a flock file races waiters.
	if err := unix.Flock(lockFD, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		release()
		return nil, nil, fmt.Errorf("telemetry query socket already owned: %w", err)
	}
	if info, err := os.Lstat(path); err == nil {
		if info.Mode()&os.ModeSocket == 0 {
			release()
			return nil, nil, errors.New("telemetry query path exists and is not a socket")
		}
		conn, dialErr := net.DialTimeout("unix", path, 100*time.Millisecond)
		if dialErr == nil {
			_ = conn.Close()
			release()
			return nil, nil, errors.New("telemetry query socket already listening")
		}
		if !errors.Is(dialErr, unix.ECONNREFUSED) && !errors.Is(dialErr, os.ErrNotExist) {
			release()
			return nil, nil, dialErr
		}
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			release()
			return nil, nil, err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		release()
		return nil, nil, err
	}
	fd, err := unix.Socket(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		release()
		return nil, nil, err
	}
	file := os.NewFile(uintptr(fd), path)
	defer file.Close()
	if err := unix.Bind(fd, &unix.SockaddrUnix{Name: path}); err != nil {
		release()
		return nil, nil, err
	}
	failed := func(err error) (net.Listener, func(), error) { _ = os.Remove(path); release(); return nil, nil, err }
	// Set permissions before listen, with no world-accessible acceptance window.
	if err := os.Chmod(path, 0600); err != nil {
		return failed(err)
	}
	if err := unix.Listen(fd, 128); err != nil {
		return failed(err)
	}
	listener, err := net.FileListener(file)
	if err != nil {
		return failed(err)
	}
	listener.(*net.UnixListener).SetUnlinkOnClose(true)
	return listener, release, nil
}
