//go:build linux

// Package listenresource validates explicitly selected TCP listener resources.
package listenresource

import (
	"fmt"
	"net"
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

// Identity identifies a Linux socket independently of its descriptor number.
type Identity struct {
	Device uint64
	Inode  uint64
}

// TCP validates without consuming or duplicating file. Callers retain ownership
// on both success and error. No shutdown operation is performed on the socket.
func TCP(file *os.File) (Identity, error) {
	if file == nil {
		return Identity{}, fmt.Errorf("nil listener file")
	}
	raw, err := file.SyscallConn()
	if err != nil {
		return Identity{}, err
	}
	var identity Identity
	var validation error
	if err := raw.Control(func(fd uintptr) { identity, validation = tcp(int(fd), file.Name()) }); err != nil {
		return Identity{}, err
	}
	return identity, validation
}

func tcp(fd int, name string) (Identity, error) {
	typ, err := unix.GetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_TYPE)
	if err != nil || typ != unix.SOCK_STREAM {
		return Identity{}, fmt.Errorf("%s is not a TCP stream socket: %v", name, err)
	}
	accept, err := unix.GetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_ACCEPTCONN)
	if err != nil || accept != 1 {
		return Identity{}, fmt.Errorf("%s is not listening: %v", name, err)
	}
	address, err := unix.Getsockname(fd)
	if err != nil {
		return Identity{}, err
	}
	switch address.(type) {
	case *unix.SockaddrInet4, *unix.SockaddrInet6:
	default:
		return Identity{}, fmt.Errorf("%s is not an IPv4 or IPv6 TCP listener", name)
	}
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return Identity{}, err
	}
	return Identity{Device: uint64(stat.Dev), Inode: stat.Ino}, nil
}

// Dup returns a CLOEXEC file without changing the shared open-file description's
// blocking mode. TCPListener.File followed by File.Fd (including in ExtraFiles)
// can clear O_NONBLOCK in another process already accepting on the same socket.
// SyscallConn plus a raw duplicate avoids that conversion and its stuck Accept.
func Dup(source syscall.Conn, name string) (*os.File, error) {
	raw, err := source.SyscallConn()
	if err != nil {
		return nil, err
	}
	var descriptor int
	var duplication error
	if err := raw.Control(func(fd uintptr) { descriptor, duplication = unix.FcntlInt(fd, unix.F_DUPFD_CLOEXEC, 3) }); err != nil {
		return nil, err
	}
	if duplication != nil {
		return nil, duplication
	}
	return os.NewFile(uintptr(descriptor), name), nil
}

// Listener borrows file and returns an independently owned listener. The safe
// intermediate duplicate prevents net.FileListener's File.Fd call from changing
// the mode of a delivered net.TCPListener.File handle and other active holders.
func Listener(file *os.File) (net.Listener, error) {
	copy, err := Dup(file, file.Name())
	if err != nil {
		return nil, err
	}
	defer copy.Close()
	return net.FileListener(copy)
}

// Validate checks the entire named set, including aliases to the same socket.
// It does not take ownership, mutate the map, or close files on failure.
func Validate(files map[string]*os.File) error {
	seen := make(map[Identity]string, len(files))
	for name, file := range files {
		if err := Name(name); err != nil {
			return err
		}
		identity, err := TCP(file)
		if err != nil {
			return fmt.Errorf("listener %q: %w", name, err)
		}
		if prior, ok := seen[identity]; ok {
			return fmt.Errorf("listeners %q and %q reference the same socket", prior, name)
		}
		seen[identity] = name
	}
	return nil
}

// Close releases only delivered descriptors, at most once per file object.
func Close(files map[string]*os.File) {
	seen := make(map[*os.File]bool, len(files))
	for _, file := range files {
		if file != nil && !seen[file] {
			seen[file] = true
			_ = file.Close()
		}
	}
}
