package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

const (
	hostCgroupRoot = "/sys/fs/cgroup"
	selfCgroupFile = "/proc/self/cgroup"
)

func prepareRunnerCgroup(runID string) (*os.File, error) {
	return prepareRunnerCgroupAt(
		hostCgroupRoot,
		runID,
		func() ([]byte, error) { return os.ReadFile(selfCgroupFile) },
		func(path string) error { return writeExistingFile(path, strconv.Itoa(os.Getpid())) },
		validateCgroup2FD,
	)
}

func prepareRunnerCgroupAt(
	root, runID string,
	readIdentity func() ([]byte, error),
	moveSelf func(string) error,
	validate func(*os.File) error,
) (*os.File, error) {
	identity, err := readIdentity()
	if err != nil {
		return nil, fmt.Errorf("read runner cgroup identity: %w", err)
	}
	self, err := unifiedCgroupPath(identity)
	if err != nil {
		return nil, err
	}
	unitRel, alreadyPlaced, err := runnerUnitCgroup(self, runID)
	if err != nil {
		return nil, err
	}
	unitRoot := filepath.Join(root, strings.TrimPrefix(unitRel, "/"))
	if err := ensurePathWithin(root, unitRoot); err != nil {
		return nil, err
	}
	ctlPath := filepath.Join(unitRoot, "ctl")
	if alreadyPlaced {
		ctlInfo, err := os.Stat(ctlPath)
		if err != nil || !ctlInfo.IsDir() {
			return nil, fmt.Errorf("runner ctl subgroup is unavailable")
		}
	} else {
		if err := os.Mkdir(ctlPath, 0o755); err != nil {
			return nil, fmt.Errorf("create runner ctl subgroup: %w", err)
		}
		placementComplete := false
		defer func() {
			if !placementComplete {
				_ = os.Remove(ctlPath)
			}
		}()
		if err := moveSelf(filepath.Join(ctlPath, "cgroup.procs")); err != nil {
			return nil, fmt.Errorf("move runner process to ctl subgroup: %w", err)
		}
		identity, err = readIdentity()
		if err != nil {
			return nil, fmt.Errorf("re-read runner cgroup identity: %w", err)
		}
		self, err = unifiedCgroupPath(identity)
		if err != nil {
			return nil, fmt.Errorf("validate runner cgroup after ctl placement: %w", err)
		}
		want := filepath.Join(unitRel, "ctl")
		if self != want {
			return nil, fmt.Errorf("runner process cgroup after ctl placement is %q, want %q", self, want)
		}
		placementComplete = true
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

func runnerUnitCgroup(self, runID string) (string, bool, error) {
	alreadyPlaced := filepath.Base(self) == "ctl"
	unitRel := self
	if alreadyPlaced {
		unitRel = filepath.Dir(self)
	}
	if unitRel == "/" || unitRel == "." {
		return "", false, fmt.Errorf("runner unit cgroup root is not isolated: %q", unitRel)
	}
	unitName := filepath.Base(unitRel)
	instanceSuffix := "@" + runID + ".service"
	prefix := strings.TrimSuffix(unitName, instanceSuffix)
	if prefix == unitName || prefix == "" || strings.Contains(prefix, "@") {
		return "", false, fmt.Errorf("runner process cgroup %q does not match run id %q", self, runID)
	}
	runnerSlice := filepath.Dir(unitRel)
	if runnerSlice != "/sandbox.slice/sandbox-runner.slice" {
		return "", false, fmt.Errorf("runner process cgroup %q is outside sandbox-runner.slice", self)
	}
	return unitRel, alreadyPlaced, nil
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
