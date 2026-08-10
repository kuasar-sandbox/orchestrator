package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

const (
	hostCgroupRoot = "/sys/fs/cgroup"
	selfCgroupFile = "/proc/self/cgroup"
)

func prepareRunnerCgroup() (*os.File, error) {
	identity, err := os.ReadFile(selfCgroupFile)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", selfCgroupFile, err)
	}
	return prepareRunnerCgroupAt(hostCgroupRoot, identity, validateCgroup2FD)
}

func prepareRunnerCgroupAt(root string, identity []byte, validate func(*os.File) error) (*os.File, error) {
	self, err := unifiedCgroupPath(identity)
	if err != nil {
		return nil, err
	}
	if filepath.Base(self) != "ctl" {
		return nil, fmt.Errorf("runner process cgroup %q is not the delegated ctl subgroup", self)
	}
	unitRel := filepath.Dir(self)
	if unitRel == "/" || unitRel == "." {
		return nil, fmt.Errorf("runner unit cgroup root is not isolated: %q", unitRel)
	}
	unitRoot := filepath.Join(root, strings.TrimPrefix(unitRel, "/"))
	if err := ensurePathWithin(root, unitRoot); err != nil {
		return nil, err
	}
	ctlInfo, err := os.Stat(filepath.Join(unitRoot, "ctl"))
	if err != nil || !ctlInfo.IsDir() {
		return nil, fmt.Errorf("runner ctl subgroup is unavailable")
	}
	if err := requireEmptyCgroup(unitRoot); err != nil {
		return nil, fmt.Errorf("runner unit root: %w", err)
	}

	controllers, err := os.ReadFile(filepath.Join(unitRoot, "cgroup.controllers"))
	if err != nil {
		return nil, fmt.Errorf("read delegated controllers: %w", err)
	}
	available := stringSet(controllers)
	for _, controller := range []string{"cpu", "memory"} {
		if !available[controller] {
			return nil, fmt.Errorf("runner unit does not delegate %s controller", controller)
		}
	}
	enabledData, err := os.ReadFile(filepath.Join(unitRoot, "cgroup.subtree_control"))
	if err != nil {
		return nil, fmt.Errorf("read subtree controllers: %w", err)
	}
	enabled := stringSet(enabledData)
	var commands []string
	for _, controller := range []string{"cpu", "memory"} {
		if !enabled[controller] {
			commands = append(commands, "+"+controller)
		}
	}
	if len(commands) > 0 {
		if err := writeExistingFile(filepath.Join(unitRoot, "cgroup.subtree_control"), strings.Join(commands, " ")); err != nil {
			return nil, fmt.Errorf("enable runner controllers: %w", err)
		}
	}

	vmmPath := filepath.Join(unitRoot, "vmm")
	if err := os.Mkdir(vmmPath, 0o755); err != nil && !os.IsExist(err) {
		return nil, fmt.Errorf("create vmm cgroup: %w", err)
	}
	if err := requireEmptyCgroup(vmmPath); err != nil {
		return nil, fmt.Errorf("vmm cgroup: %w", err)
	}
	fd, err := unix.Open(vmmPath, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("open vmm cgroup: %w", err)
	}
	f := os.NewFile(uintptr(fd), "runner-vmm-cgroup")
	if f == nil {
		unix.Close(fd)
		return nil, fmt.Errorf("adopt vmm cgroup descriptor")
	}
	if validate != nil {
		if err := validate(f); err != nil {
			_ = f.Close()
			return nil, err
		}
	}
	return f, nil
}

func requireEmptyCgroup(path string) error {
	procs, err := os.ReadFile(filepath.Join(path, "cgroup.procs"))
	if err != nil {
		return fmt.Errorf("read cgroup.procs: %w", err)
	}
	if len(bytes.TrimSpace(procs)) != 0 {
		return fmt.Errorf("contains existing processes")
	}
	return nil
}

func unifiedCgroupPath(data []byte) (string, error) {
	var found string
	for _, line := range bytes.Split(bytes.TrimSpace(data), []byte{'\n'}) {
		if !bytes.HasPrefix(line, []byte("0::")) {
			continue
		}
		if found != "" {
			return "", fmt.Errorf("multiple unified cgroup identities")
		}
		found = string(bytes.TrimPrefix(line, []byte("0::")))
	}
	if found == "" || !filepath.IsAbs(found) || filepath.Clean(found) != found {
		return "", fmt.Errorf("invalid unified cgroup identity %q", found)
	}
	return found, nil
}

func ensurePathWithin(root, target string) error {
	rel, err := filepath.Rel(root, target)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return fmt.Errorf("runner cgroup path %q escapes root %q", target, root)
	}
	return nil
}

func stringSet(data []byte) map[string]bool {
	out := make(map[string]bool)
	for _, field := range strings.Fields(string(data)) {
		out[field] = true
	}
	return out
}

func writeExistingFile(path, value string) error {
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.WriteString(value)
	return err
}

func validateCgroup2FD(f *os.File) error {
	var st unix.Statfs_t
	if err := unix.Fstatfs(int(f.Fd()), &st); err != nil {
		return fmt.Errorf("statfs vmm cgroup: %w", err)
	}
	if st.Type != unix.CGROUP2_SUPER_MAGIC {
		return fmt.Errorf("vmm descriptor is not on cgroup v2")
	}
	return nil
}
