//go:build linux

package raftstore

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

type LinuxDMStorageAttestor struct {
	SysfsRoot string
}

// LinuxEphemeralStorageAttestor is restricted to explicitly ephemeral test/dev
// generations. tmpfs has no persistent at-rest bytes and therefore cannot
// leave unencrypted Registry state behind after the mount is destroyed.
type LinuxEphemeralStorageAttestor struct{}

func (LinuxEphemeralStorageAttestor) VerifyEncrypted(paths ...string) error {
	if len(paths) == 0 {
		return errors.New("no storage path supplied")
	}
	for _, path := range paths {
		var stat unix.Statfs_t
		if err := unix.Statfs(path, &stat); err != nil {
			return err
		}
		if stat.Type != unix.TMPFS_MAGIC {
			return fmt.Errorf("%s is not on ephemeral tmpfs", path)
		}
	}
	return nil
}

func (a LinuxDMStorageAttestor) VerifyEncrypted(paths ...string) error {
	if len(paths) == 0 {
		return errors.New("no storage path supplied")
	}
	root := a.SysfsRoot
	if root == "" {
		root = "/sys"
	}
	for _, path := range paths {
		var stat unix.Stat_t
		if err := unix.Stat(path, &stat); err != nil {
			return err
		}
		device := fmt.Sprintf("%d:%d", unix.Major(uint64(stat.Dev)), unix.Minor(uint64(stat.Dev)))
		encrypted, err := dmCryptInDeviceGraph(root, device, make(map[string]struct{}))
		if err != nil {
			return fmt.Errorf("inspect %s backing device: %w", path, err)
		}
		if !encrypted {
			return fmt.Errorf("%s is not backed by a dm-crypt device", path)
		}
	}
	return nil
}

func dmCryptInDeviceGraph(root, device string, visited map[string]struct{}) (bool, error) {
	if err := validateDeviceNumber(device); err != nil {
		return false, err
	}
	if _, found := visited[device]; found {
		return false, nil
	}
	if len(visited) >= 64 {
		return false, errors.New("backing-device graph exceeds safety bound")
	}
	visited[device] = struct{}{}
	devicePath := filepath.Join(root, "dev", "block", device)
	if raw, err := os.ReadFile(filepath.Join(devicePath, "dm", "uuid")); err == nil {
		if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(string(raw))), "CRYPT-") {
			return true, nil
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	slaves, err := os.ReadDir(filepath.Join(devicePath, "slaves"))
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	for _, slave := range slaves {
		raw, err := os.ReadFile(filepath.Join(devicePath, "slaves", slave.Name(), "dev"))
		if err != nil {
			return false, err
		}
		encrypted, err := dmCryptInDeviceGraph(root, strings.TrimSpace(string(raw)), visited)
		if err != nil {
			return false, err
		}
		if encrypted {
			return true, nil
		}
	}
	return false, nil
}

func validateDeviceNumber(value string) error {
	parts := strings.Split(value, ":")
	if len(parts) != 2 {
		return errors.New("invalid device number")
	}
	for _, part := range parts {
		if part == "" {
			return errors.New("invalid device number")
		}
		if _, err := strconv.ParseUint(part, 10, 32); err != nil {
			return errors.New("invalid device number")
		}
	}
	return nil
}
