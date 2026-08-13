package unixcred

import (
	"net"

	"golang.org/x/sys/unix"
)

// PeerPID returns SO_PEERCRED.pid for an accepted Unix stream connection.
func PeerPID(c *net.UnixConn) (int, error) {
	raw, err := c.SyscallConn()
	if err != nil {
		return 0, err
	}
	var ucred *unix.Ucred
	var socketErr error
	if controlErr := raw.Control(func(fd uintptr) {
		ucred, socketErr = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED)
	}); controlErr != nil {
		return 0, controlErr
	}
	if socketErr != nil {
		return 0, socketErr
	}
	return int(ucred.Pid), nil
}
