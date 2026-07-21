package nodectl

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
)

func peerUnifiedCgroup(conn net.Conn) (string, error) {
	syscallConn, ok := conn.(syscall.Conn)
	if !ok {
		return "", errors.New("nodectl: resource client has no kernel peer credentials")
	}
	raw, err := syscallConn.SyscallConn()
	if err != nil {
		return "", err
	}
	var (
		credentials *unix.Ucred
		controlErr  error
	)
	if err := raw.Control(func(fd uintptr) {
		credentials, controlErr = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED)
	}); err != nil {
		return "", err
	}
	if controlErr != nil {
		return "", controlErr
	}
	if credentials == nil || credentials.Pid <= 0 {
		return "", errors.New("nodectl: invalid resource client credentials")
	}
	return processUnifiedCgroup(int(credentials.Pid))
}

func processUnifiedCgroup(pid int) (string, error) {
	data, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "cgroup"))
	if err != nil {
		return "", err
	}
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		path, found := strings.CutPrefix(line, "0::")
		if !found {
			continue
		}
		if path == "" || !strings.HasPrefix(path, "/") {
			return "", errors.New("nodectl: invalid unified cgroup path")
		}
		return filepath.Join("/sys/fs/cgroup", strings.TrimPrefix(path, "/")), nil
	}
	return "", fmt.Errorf("nodectl: process %d is not in a cgroup v2 hierarchy", pid)
}
