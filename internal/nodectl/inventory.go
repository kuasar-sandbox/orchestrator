package nodectl

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
	"github.com/kuasar-sandbox/sandboxer/pkg/resource"
)

// Inventory discovers the durable liveness facts used to rebuild State. It is
// intentionally independent of state.json.
type Inventory struct {
	ControllerSocket string
	CgroupScanPaths  []string
	ManagedRunRoot   string
	Pool             Resources
	Logf             func(string, ...any)

	// processVMMCgroup is overridden only by tests. Managed production
	// identity is derived from /proc/<lease-owner>/cgroup because the runner
	// injects the node-owned VMM cgroup by FD at exec time; it is not stored in
	// the node-owned YAML.
	processVMMCgroup func(int) string
}

type LiveLease struct {
	Lease    resource.Lease
	Path     string
	OwnerPID int
}

func (i *Inventory) LeasePath(sid string) string {
	return resource.LeasePath(i.ControllerSocket, sid)
}

func (i *Inventory) defaults() {
	if i.Logf == nil {
		i.Logf = func(string, ...any) {}
	}
}

// Recover installs every live lease first, then managed legacy consumers, then
// orphan cgroups. State's cgroup index deduplicates the same consumer across
// sources.
func (i *Inventory) Recover(state *State) error {
	i.defaults()
	if err := os.MkdirAll(resource.LeaseDir(i.ControllerSocket), 0o755); err != nil {
		return fmt.Errorf("lease inventory mkdir: %w", err)
	}
	controlPIDs := make(map[int]bool)
	if err := i.scanLeases(state, controlPIDs); err != nil {
		return err
	}
	if err := i.scanManaged(state, controlPIDs); err != nil {
		return err
	}
	if err := i.scanCgroups(state, controlPIDs); err != nil {
		return err
	}
	snapshot := state.ResourceSnapshot()
	i.Logf("inventory recovered reservations=%d provisional=%d unknown=%d allocated_memory=%d",
		snapshot.ReservationCount, snapshot.ProvisionalCount, snapshot.UnknownCount,
		snapshot.Allocated.MemoryBytes)
	return nil
}

func (i *Inventory) scanLeases(state *State, controlPIDs map[int]bool) error {
	dir := resource.LeaseDir(i.ControllerSocket)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("scan leases: %w", err)
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		for {
			lease, owner, locked, err := resource.InspectLease(path)
			if err != nil {
				if locked {
					controlPIDs[owner] = true
					if installErr := i.installUnknownLease(state, path, owner, err); installErr != nil {
						return installErr
					}
					break
				}
				removed, removeErr := resource.RemoveUnlockedLease(path)
				if removeErr != nil {
					return fmt.Errorf("remove malformed stale lease %s: %w", path, removeErr)
				}
				if removed {
					break
				}
				if _, statErr := os.Lstat(path); os.IsNotExist(statErr) {
					break
				} else if statErr != nil {
					return fmt.Errorf("restat malformed lease %s: %w", path, statErr)
				}
				// The pathname changed or gained an owner between InspectLease and
				// cleanup. Re-inspect until it is either charged or safely removed.
				continue
			}
			if !locked {
				removed, removeErr := resource.RemoveUnlockedLease(path)
				if removeErr != nil {
					return fmt.Errorf("remove stale lease %s: %w", path, removeErr)
				}
				if removed {
					break
				}
				if _, statErr := os.Lstat(path); os.IsNotExist(statErr) {
					break
				} else if statErr != nil {
					return fmt.Errorf("restat stale lease %s: %w", path, statErr)
				}
				continue
			}
			controlPIDs[owner] = true
			controllerSocket, socketErr := canonicalControllerSocket(lease.ControllerSocket)
			if lease.PID != owner || socketErr != nil || controllerSocket != i.ControllerSocket ||
				entry.Name() != resource.LeaseFilename(lease.SandboxID) || !i.cgroupAllowed(lease.CgroupPath) {
				if err := i.installUnknownLease(state, path, owner, nil); err != nil {
					return err
				}
				break
			}
			lease.ControllerSocket = controllerSocket
			err = state.InstallProvisional(ProvisionalSpec{
				SandboxID: lease.SandboxID, PeerPID: owner, CgroupPath: lease.CgroupPath,
				Capacity:     Resources{MemoryBytes: lease.CapacityMemory, CPUMilli: lease.CapacityCPUMilli},
				Floor:        Resources{MemoryBytes: lease.FloorMemory, CPUMilli: lease.FloorCPUMilli},
				MemoryCharge: lease.CapacityMemory, StartupCharge: lease.CapacityMemory,
				RecoverySource: RecoveryLease, RecoveryKey: "lease:" + entry.Name(),
				LeasePath: path, ClientFeatures: lease.ClientFeatures,
			})
			if err != nil {
				return fmt.Errorf("install lease %s: %w", path, err)
			}
			break
		}
	}
	return nil
}

func (i *Inventory) installUnknownLease(state *State, path string, owner int, parseErr error) error {
	digest := sha256.Sum256([]byte(path))
	name := hex.EncodeToString(digest[:])
	if parseErr == nil {
		parseErr = errors.New("immutable lease identity validation failed")
	}
	i.Logf("live lease %s is invalid; charging full pool: %v", path, parseErr)
	if err := state.InstallProvisional(ProvisionalSpec{
		SandboxID: "unknown-lease-" + name, PeerPID: owner,
		Capacity: i.Pool, Floor: Resources{CPUMilli: i.Pool.CPUMilli},
		MemoryCharge: i.Pool.MemoryBytes, StartupCharge: i.Pool.MemoryBytes,
		RecoverySource: RecoveryUnknownLease, RecoveryKey: "unknown:" + path,
		LeasePath: path,
	}); err != nil {
		return fmt.Errorf("install invalid live lease %s: %w", path, err)
	}
	return nil
}

func (i *Inventory) scanManaged(state *State, controlPIDs map[int]bool) error {
	if i.ManagedRunRoot == "" {
		return nil
	}
	entries, err := os.ReadDir(i.ManagedRunRoot)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("scan managed run root: %w", err)
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		sid := entry.Name()
		pidPath, yamlPath, ok := i.managedPaths(sid)
		if !ok {
			continue
		}
		owner, locked, err := resource.LeaseLockOwner(pidPath)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return fmt.Errorf("inspect managed pidfile %s: %w", pidPath, err)
		}
		if !locked {
			continue
		}
		if controlPIDs[owner] {
			continue
		}
		controlPIDs[owner] = true
		pid, err := readPIDFile(pidPath)
		if err != nil || pid != owner {
			if err := i.installUnknownManaged(state, sid, owner, pidPath, "pidfile identity"); err != nil {
				return err
			}
			continue
		}
		cfg, err := rtconfig.LoadMerged([]string{yamlPath})
		if err != nil {
			if err := i.installUnknownManaged(state, sid, owner, pidPath, "managed yaml"); err != nil {
				return err
			}
			continue
		}
		controllerSocket, socketErr := canonicalControllerSocket(cfg.Resources.Control.Controller)
		if socketErr != nil || controllerSocket != i.ControllerSocket {
			if err := i.installUnknownManaged(state, sid, owner, pidPath, "managed controller socket"); err != nil {
				return err
			}
			continue
		}
		capMem, capErr := cfg.CapacityMemoryBytes()
		floorMem, floorErr := cfg.AllocatableMemoryBytes()
		startupMem, startupErr := cfg.StartupBytes()
		if capErr != nil || floorErr != nil || startupErr != nil {
			if err := i.installUnknownManaged(state, sid, owner, pidPath, "resource bounds"); err != nil {
				return err
			}
			continue
		}
		cgroupPath := i.vmmCgroup(owner)
		if cgroupPath == "" || !i.cgroupAllowed(cgroupPath) {
			if err := i.installUnknownManaged(state, sid, owner, pidPath, "vmm cgroup identity"); err != nil {
				return err
			}
			continue
		}
		if err := state.InstallProvisional(ProvisionalSpec{
			SandboxID: sid, PeerPID: owner, CgroupPath: cgroupPath,
			Capacity:     Resources{MemoryBytes: capMem, CPUMilli: cpuMilliCeil(float64(cfg.Resources.Capacity.CPU))},
			Floor:        Resources{MemoryBytes: floorMem, CPUMilli: cpuMilliCeil(cfg.Resources.Allocatable.CPU)},
			MemoryCharge: capMem, StartupCharge: capMem,
			RecoverySource: RecoveryManagedPIDFile, RecoveryKey: "pidfile:" + pidPath,
		}); err != nil {
			return err
		}
		_ = startupMem // validated against the immutable YAML; provisional startup charges capacity.
	}
	return nil
}

func (i *Inventory) installUnknownManaged(state *State, sid string, owner int, pidPath, reason string) error {
	i.Logf("live managed sandbox %s has incomplete %s; charging full pool", sid, reason)
	return state.InstallProvisional(ProvisionalSpec{
		SandboxID: sid, PeerPID: owner,
		Capacity: i.Pool, Floor: Resources{CPUMilli: i.Pool.CPUMilli},
		MemoryCharge: i.Pool.MemoryBytes, StartupCharge: i.Pool.MemoryBytes,
		RecoverySource: RecoveryUnknownManaged, RecoveryKey: "unknown-managed:" + pidPath,
	})
}

func managedVMMCgroup(pid int) string {
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/cgroup", pid))
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		if !strings.HasPrefix(line, "0::/") {
			continue
		}
		rel := strings.TrimPrefix(line, "0::")
		if filepath.Base(rel) != "ctl" {
			return ""
		}
		return filepath.Join("/sys/fs/cgroup", strings.TrimPrefix(filepath.Dir(rel), "/"), "vmm")
	}
	return ""
}

func (i *Inventory) vmmCgroup(pid int) string {
	if i.processVMMCgroup != nil {
		return i.processVMMCgroup(pid)
	}
	return managedVMMCgroup(pid)
}

func (i *Inventory) scanCgroups(state *State, controlPIDs map[int]bool) error {
	seen := make(map[string]bool)
	for _, root := range i.CgroupScanPaths {
		root = filepath.Clean(root)
		if seen[root] {
			continue
		}
		seen[root] = true
		err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil {
				if os.IsNotExist(walkErr) {
					return nil
				}
				return walkErr
			}
			if !entry.IsDir() {
				return nil
			}
			populated, err := readCgroupPopulated(path)
			if err != nil {
				if os.IsNotExist(err) {
					return nil
				}
				return err
			}
			if !populated || state.HasCgroup(path) {
				return nil
			}
			pids, err := readCgroupPIDs(path)
			if err != nil {
				if os.IsNotExist(err) {
					return nil
				}
				return err
			}
			if len(pids) == 0 {
				return nil
			}
			consumer := false
			for _, pid := range pids {
				if !controlPIDs[pid] {
					consumer = true
					break
				}
			}
			if !consumer {
				return nil
			}
			charge := readMemoryMax(path)
			if charge == 0 || charge > i.Pool.MemoryBytes {
				charge = i.Pool.MemoryBytes
			}
			hash := sha256.Sum256([]byte(path))
			sid := "orphan-cgroup-" + hex.EncodeToString(hash[:8])
			return state.InstallProvisional(ProvisionalSpec{
				SandboxID: sid, CgroupPath: path,
				Capacity: Resources{MemoryBytes: charge}, MemoryCharge: charge,
				StartupCharge: charge, RecoverySource: RecoveryCgroup,
				RecoveryKey: "cgroup:" + path,
			})
		})
		if err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("scan cgroup root %s: %w", root, err)
		}
	}
	return nil
}

func (i *Inventory) LookupLiveLease(sid string) (LiveLease, error) {
	path := i.LeasePath(sid)
	lease, owner, locked, err := resource.InspectLease(path)
	if err != nil {
		return LiveLease{}, err
	}
	if !locked {
		return LiveLease{}, fmt.Errorf("lease is not locked")
	}
	controllerSocket, socketErr := canonicalControllerSocket(lease.ControllerSocket)
	if lease.SandboxID != sid || lease.PID != owner || socketErr != nil || controllerSocket != i.ControllerSocket || !i.cgroupAllowed(lease.CgroupPath) {
		return LiveLease{}, fmt.Errorf("lease immutable identity mismatch")
	}
	lease.ControllerSocket = controllerSocket
	return LiveLease{Lease: lease, Path: path, OwnerPID: owner}, nil
}

// ValidateManaged strengthens lease authentication when the node-owned pidfile
// and YAML are present. Their absence denotes the supported direct-run mode.
func (i *Inventory) ValidateManaged(live LiveLease, peerPID int) error {
	if live.OwnerPID != peerPID {
		return fmt.Errorf("peer pid %d does not own lease (owner %d)", peerPID, live.OwnerPID)
	}
	if i.ManagedRunRoot == "" {
		return nil
	}
	pidPath, yamlPath, ok := i.managedPaths(live.Lease.SandboxID)
	if !ok {
		return fmt.Errorf("managed path escapes run root")
	}
	_, pidErr := os.Stat(pidPath)
	_, yamlErr := os.Stat(yamlPath)
	if os.IsNotExist(pidErr) && os.IsNotExist(yamlErr) {
		return nil
	}
	if pidErr != nil || yamlErr != nil {
		return fmt.Errorf("managed identity is incomplete")
	}
	pid, err := readPIDFile(pidPath)
	if err != nil || pid != peerPID {
		return fmt.Errorf("managed pidfile pid mismatch")
	}
	owner, locked, err := resource.LeaseLockOwner(pidPath)
	if err != nil || !locked || owner != peerPID {
		return fmt.Errorf("managed pidfile lock owner mismatch")
	}
	cfg, err := rtconfig.LoadMerged([]string{yamlPath})
	if err != nil {
		return fmt.Errorf("managed yaml: %w", err)
	}
	capMem, err := cfg.CapacityMemoryBytes()
	if err != nil {
		return err
	}
	floorMem, err := cfg.AllocatableMemoryBytes()
	if err != nil {
		return err
	}
	startupMem, err := cfg.StartupBytes()
	if err != nil {
		return err
	}
	l := live.Lease
	if actual := i.vmmCgroup(peerPID); actual == "" || actual != l.CgroupPath {
		return fmt.Errorf("managed process cgroup does not match lease")
	}
	controllerSocket, err := canonicalControllerSocket(cfg.Resources.Control.Controller)
	if err != nil {
		return fmt.Errorf("managed yaml controller socket: %w", err)
	}
	if controllerSocket != i.ControllerSocket || capMem != l.CapacityMemory ||
		floorMem != l.FloorMemory || startupMem != l.StartupMemory ||
		uint64(cfg.Resources.Capacity.CPU)*1000 != l.CapacityCPUMilli ||
		cpuMilliCeil(cfg.Resources.Allocatable.CPU) != l.FloorCPUMilli {
		return fmt.Errorf("managed yaml does not match lease")
	}
	return nil
}

func (i *Inventory) managedPaths(sid string) (string, string, bool) {
	if i.ManagedRunRoot == "" || sid == "" || filepath.Base(sid) != sid {
		return "", "", false
	}
	dir := filepath.Join(i.ManagedRunRoot, sid)
	rel, err := filepath.Rel(i.ManagedRunRoot, dir)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", "", false
	}
	return filepath.Join(dir, sid+".pid"), filepath.Join(dir, sid+".yaml"), true
}

func (i *Inventory) cgroupAllowed(path string) bool {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return false
	}
	for _, root := range i.CgroupScanPaths {
		root = filepath.Clean(root)
		rel, err := filepath.Rel(root, path)
		if err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			return true
		}
	}
	return false
}

func (i *Inventory) ConsumerLive(r Reservation) bool {
	if r.LeasePath != "" {
		_, locked, err := resource.LeaseLockOwner(r.LeasePath)
		if err != nil && !os.IsNotExist(err) {
			return true
		}
		if err == nil && locked {
			return true
		}
	}
	if r.RecoverySource == RecoveryManagedPIDFile || r.RecoverySource == RecoveryUnknownManaged {
		pidPath, _, ok := i.managedPaths(r.SandboxID)
		if ok {
			_, locked, err := resource.LeaseLockOwner(pidPath)
			if err != nil && !os.IsNotExist(err) {
				return true
			}
			if err == nil && locked {
				return true
			}
		}
	}
	if r.CgroupPath == "" {
		return false
	}
	populated, err := readCgroupPopulated(r.CgroupPath)
	if err != nil {
		// Disappearance is positive evidence that this consumer is gone. Any
		// other inspection failure is unknown and must retain the charge.
		return !os.IsNotExist(err)
	}
	return populated
}

func readCgroupPopulated(path string) (bool, error) {
	b, err := os.ReadFile(filepath.Join(path, "cgroup.events"))
	if err != nil {
		return false, err
	}
	s := bufio.NewScanner(strings.NewReader(string(b)))
	for s.Scan() {
		fields := strings.Fields(s.Text())
		if len(fields) == 2 && fields[0] == "populated" {
			switch fields[1] {
			case "0":
				return false, nil
			case "1":
				return true, nil
			default:
				return false, fmt.Errorf("cgroup %s has invalid populated value %q", path, fields[1])
			}
		}
	}
	if err := s.Err(); err != nil {
		return false, err
	}
	return false, fmt.Errorf("cgroup %s has no populated event", path)
}

func readCgroupPIDs(path string) ([]int, error) {
	b, err := os.ReadFile(filepath.Join(path, "cgroup.procs"))
	if err != nil {
		return nil, err
	}
	var out []int
	for _, field := range strings.Fields(string(b)) {
		pid, err := strconv.Atoi(field)
		if err != nil || pid <= 0 {
			return nil, fmt.Errorf("cgroup %s has invalid pid %q", path, field)
		}
		out = append(out, pid)
	}
	return out, nil
}

func readMemoryMax(path string) uint64 {
	b, err := os.ReadFile(filepath.Join(path, "memory.max"))
	if err != nil || strings.TrimSpace(string(b)) == "max" {
		return 0
	}
	n, _ := strconv.ParseUint(strings.TrimSpace(string(b)), 10, 64)
	return n
}

func readPIDFile(path string) (int, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(strings.TrimSpace(string(b)))
}
