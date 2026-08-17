package builder

import (
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
)

// phaseCPUMaxState tracks the temporary sandboxer#112 integration workaround.
// The original value is retained as soon as a relaxation write is attempted,
// so every partial write/readback failure still drives a restore attempt.
type phaseCPUMaxState struct {
	mu       sync.Mutex
	original string
	relaxed  bool
}

// relaxCPUMaxForVCPUKick removes only the phase VMM leaf's hard CPU ceiling
// while Cloud Hypervisor performs a pause/shutdown vCPU kick. Normal phase
// execution remains constrained, and the enclosing per-Build service plus
// aggregate Builder slice remain bounded throughout. This workaround must be
// removed after sandboxer#112 provides a no-miss native kick.
func (sb *phaseSandbox) relaxCPUMaxForVCPUKick() error {
	if sb == nil || sb.p == nil || sb.p.vmmCgroup == nil || sb.p.vmmCgroup.Fd() < 3 {
		return fmt.Errorf("phase cpu.max: trusted VMM cgroup descriptor is unavailable")
	}
	if sb.p.spec == nil || sb.p.spec.Resources.Capacity.CPU <= 0 {
		return fmt.Errorf("phase cpu.max: capacity.cpu is unavailable")
	}

	sb.cpuMax.mu.Lock()
	defer sb.cpuMax.mu.Unlock()
	if sb.cpuMax.relaxed {
		return nil
	}
	if sb.cpuMax.original != "" {
		return fmt.Errorf("phase cpu.max: prior relaxation did not verify")
	}

	raw, err := readPhaseCgroupFile(sb.p.vmmCgroup, "cpu.max")
	if err != nil {
		return fmt.Errorf("phase cpu.max: read current limit: %w", err)
	}
	quota, period, unlimited, err := parsePhaseCPUMax(raw)
	if err != nil {
		return fmt.Errorf("phase cpu.max: validate current limit: %w", err)
	}
	if unlimited {
		return fmt.Errorf("phase cpu.max: current limit is already unlimited")
	}
	cpu := uint64(sb.p.spec.Resources.Capacity.CPU)
	if period > math.MaxUint64/cpu {
		return fmt.Errorf("phase cpu.max: capacity.cpu quota overflows")
	}
	expected := cpu * period
	if quota != expected {
		return fmt.Errorf("phase cpu.max: current quota %d/%d does not match capacity.cpu %d", quota, period, sb.p.spec.Resources.Capacity.CPU)
	}

	sb.cpuMax.original = fmt.Sprintf("%d %d", quota, period)
	if err := writePhaseCgroupFile(sb.p.vmmCgroup, "cpu.max", fmt.Sprintf("max %d", period)); err != nil {
		return fmt.Errorf("phase cpu.max: relax for native vCPU kick: %w", err)
	}
	readback, err := readPhaseCgroupFile(sb.p.vmmCgroup, "cpu.max")
	if err != nil {
		return fmt.Errorf("phase cpu.max: read relaxed limit: %w", err)
	}
	_, gotPeriod, gotUnlimited, err := parsePhaseCPUMax(readback)
	if err != nil {
		return fmt.Errorf("phase cpu.max: validate relaxed limit: %w", err)
	}
	if !gotUnlimited || gotPeriod != period {
		return fmt.Errorf("phase cpu.max: relaxed readback is %q, want max %d", strings.TrimSpace(string(readback)), period)
	}
	sb.cpuMax.relaxed = true
	sb.p.log.Warn("phase cpu.max temporarily relaxed for native vCPU kick", "sid", sb.sid, "issue", "sandboxer#112")
	return nil
}

// restoreCPUMaxAfterVCPUKick restores and verifies the exact numeric ceiling
// only after Cloud Hypervisor has exited. An empty original is a no-op because
// no relaxation write was attempted.
func (sb *phaseSandbox) restoreCPUMaxAfterVCPUKick() error {
	if sb == nil || sb.p == nil || sb.p.vmmCgroup == nil {
		return nil
	}
	sb.cpuMax.mu.Lock()
	defer sb.cpuMax.mu.Unlock()
	if sb.cpuMax.original == "" {
		return nil
	}
	original := sb.cpuMax.original
	if err := writePhaseCgroupFile(sb.p.vmmCgroup, "cpu.max", original); err != nil {
		return fmt.Errorf("phase cpu.max: restore numeric limit: %w", err)
	}
	readback, err := readPhaseCgroupFile(sb.p.vmmCgroup, "cpu.max")
	if err != nil {
		return fmt.Errorf("phase cpu.max: read restored limit: %w", err)
	}
	if strings.TrimSpace(string(readback)) != original {
		return fmt.Errorf("phase cpu.max: restored readback is %q, want %q", strings.TrimSpace(string(readback)), original)
	}
	sb.cpuMax.original = ""
	sb.cpuMax.relaxed = false
	sb.p.log.Info("phase cpu.max restored after native vCPU kick", "sid", sb.sid, "issue", "sandboxer#112")
	return nil
}

func parsePhaseCPUMax(raw []byte) (quota, period uint64, unlimited bool, err error) {
	fields := strings.Fields(string(raw))
	if len(fields) != 2 {
		return 0, 0, false, fmt.Errorf("expected two fields, got %q", strings.TrimSpace(string(raw)))
	}
	period, err = strconv.ParseUint(fields[1], 10, 64)
	if err != nil || period == 0 {
		return 0, 0, false, fmt.Errorf("invalid period %q", fields[1])
	}
	if fields[0] == "max" {
		return 0, period, true, nil
	}
	quota, err = strconv.ParseUint(fields[0], 10, 64)
	if err != nil || quota == 0 {
		return 0, 0, false, fmt.Errorf("invalid quota %q", fields[0])
	}
	return quota, period, false, nil
}

func phaseCgroupFile(dir *os.File, name string) (string, error) {
	if dir == nil || dir.Fd() < 3 || strings.ContainsAny(name, `/\\`) || name == "" {
		return "", fmt.Errorf("invalid trusted cgroup file")
	}
	return fmt.Sprintf("/proc/self/fd/%d/%s", dir.Fd(), name), nil
}

func readPhaseCgroupFile(dir *os.File, name string) ([]byte, error) {
	path, err := phaseCgroupFile(dir, name)
	if err != nil {
		return nil, err
	}
	return os.ReadFile(path)
}

func writePhaseCgroupFile(dir *os.File, name, value string) error {
	path, err := phaseCgroupFile(dir, name)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_TRUNC, 0)
	if err != nil {
		return err
	}
	_, writeErr := f.WriteString(value)
	closeErr := f.Close()
	if writeErr != nil {
		return writeErr
	}
	return closeErr
}
