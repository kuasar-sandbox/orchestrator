package nodectl

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/kuasar-sandbox/sandboxer/pkg/resource"
	"golang.org/x/sys/unix"
)

const (
	testLeaseCapacity = uint64(512 << 20)
	testLeaseFloor    = uint64(128 << 20)
	testLeaseStartup  = uint64(256 << 20)
)

func TestNodeCtlProcessHelper(t *testing.T) {
	mode := os.Getenv("NODECTL_TEST_HELPER")
	if mode == "" {
		return
	}
	switch mode {
	case "owner":
		state := makeState(4<<30, 0)
		srv := &Server{Path: os.Getenv("NODECTL_SOCKET"), State: state}
		if err := srv.Listen(); err != nil {
			t.Fatal(err)
		}
		_, _ = fmt.Fprintln(os.Stdout, "ready")
		_, _ = bufio.NewReader(os.Stdin).ReadString('\n')
		_ = srv.Stop()
	case "lease":
		socket, sid, cgroup := os.Getenv("NODECTL_SOCKET"), os.Getenv("NODECTL_SID"), os.Getenv("NODECTL_CGROUP")
		var leaseHandle *resource.LeaseHandle
		var rawLease *os.File
		if os.Getenv("NODECTL_CORRUPT") == "1" {
			if err := os.MkdirAll(resource.LeaseDir(socket), 0o755); err != nil {
				t.Fatal(err)
			}
			fd, err := unix.Open(resource.LeasePath(socket, sid), unix.O_RDWR|unix.O_CREAT|unix.O_CLOEXEC, 0o600)
			if err != nil {
				t.Fatal(err)
			}
			rawLease = os.NewFile(uintptr(fd), "corrupt-lease")
			lock := unix.Flock_t{Type: unix.F_WRLCK, Whence: 0, Start: 0, Len: 0}
			if err := unix.FcntlFlock(rawLease.Fd(), unix.F_SETLK, &lock); err != nil {
				t.Fatal(err)
			}
			_, _ = rawLease.WriteString("{not-json\n")
		} else {
			var err error
			leaseHandle, err = resource.CreateLease(resource.Lease{
				Version: resource.LeaseVersion, SandboxID: sid, PID: os.Getpid(),
				ControllerSocket: socket, CgroupPath: cgroup,
				CapacityMemory: testLeaseCapacity, CapacityCPUMilli: 1000,
				FloorMemory: testLeaseFloor, FloorCPUMilli: 500,
				StartupMemory: testLeaseStartup, ClientFeatures: []string{resource.FeatureStateSyncV1},
			})
			if err != nil {
				t.Fatal(err)
			}
		}
		var pidFD int = -1
		if pidPath := os.Getenv("NODECTL_PIDFILE"); pidPath != "" {
			fd, err := unix.Open(pidPath, unix.O_RDWR|unix.O_CREAT|unix.O_CLOEXEC, 0o600)
			if err != nil {
				t.Fatal(err)
			}
			pidFD = fd
			lock := unix.Flock_t{Type: unix.F_WRLCK, Whence: 0, Start: 0, Len: 0}
			if err := unix.FcntlFlock(uintptr(fd), unix.F_SETLK, &lock); err != nil {
				t.Fatal(err)
			}
			_ = unix.Ftruncate(fd, 0)
			_, _ = unix.Pwrite(fd, []byte(strconv.Itoa(os.Getpid())+"\n"), 0)
		}
		_, _ = fmt.Fprintln(os.Stdout, "ready")
		var client *resource.Client
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			switch scanner.Text() {
			case "sync", "syncbad":
				client = &resource.Client{SocketPath: socket}
				if err := client.Connect(); err != nil {
					_, _ = fmt.Fprintln(os.Stdout, "sync-error:"+err.Error())
					continue
				}
				applied := uint64(192 << 20)
				if scanner.Text() == "syncbad" {
					applied = 64 << 20
				}
				_, err := client.StateSync(resource.StateSyncParams{
					SandboxID: sid, AppliedAllocatableMemory: applied,
					Settled: true, CurrentRSS: 64 << 20, PreviousToken: "old-token",
				})
				if err != nil {
					_, _ = fmt.Fprintln(os.Stdout, "sync-error:"+err.Error())
				} else {
					_, _ = fmt.Fprintln(os.Stdout, "sync-ok")
				}
			case "stop":
				if client != nil {
					_ = client.Close()
				}
				if leaseHandle != nil {
					_ = leaseHandle.Close()
				}
				if rawLease != nil {
					_ = rawLease.Close()
				}
				if pidFD >= 0 {
					_ = unix.Close(pidFD)
				}
				return
			}
		}
	}
}

type helperProcess struct {
	cmd    *exec.Cmd
	stdin  *bufio.Writer
	stdout *bufio.Scanner
}

func startHelper(t *testing.T, env ...string) *helperProcess {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestNodeCtlProcessHelper$")
	cmd.Env = append(os.Environ(), env...)
	in, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	h := &helperProcess{cmd: cmd, stdin: bufio.NewWriter(in), stdout: bufio.NewScanner(out)}
	if !h.stdout.Scan() || h.stdout.Text() != "ready" {
		t.Fatalf("helper ready failed: %q", h.stdout.Text())
	}
	t.Cleanup(func() {
		if cmd.ProcessState == nil {
			h.send("stop")
			_ = cmd.Wait()
		}
	})
	return h
}

func (h *helperProcess) send(line string) {
	_, _ = h.stdin.WriteString(line + "\n")
	_ = h.stdin.Flush()
}

func (h *helperProcess) next(t *testing.T) string {
	t.Helper()
	if !h.stdout.Scan() {
		t.Fatal("helper output closed")
	}
	return h.stdout.Text()
}

func TestOwnerLockPreventsSecondControllerWithoutUnlink(t *testing.T) {
	socket := filepath.Join(t.TempDir(), "controller.sock")
	h := startHelper(t, "NODECTL_TEST_HELPER=owner", "NODECTL_SOCKET="+socket)
	before, err := os.Lstat(socket)
	if err != nil {
		t.Fatal(err)
	}
	second := &Server{Path: socket, State: makeState(4<<30, 0)}
	if err := second.Listen(); err == nil {
		t.Fatal("second controller acquired owner lock")
	}
	after, err := os.Lstat(socket)
	if err != nil {
		t.Fatalf("failed second controller removed live UDS: %v", err)
	}
	if !os.SameFile(before, after) {
		t.Fatal("live UDS was replaced")
	}
	h.send("stop")
	if err := h.cmd.Wait(); err != nil {
		t.Fatal(err)
	}
}

func writeFakeCgroup(t *testing.T, path, max string, pid int) {
	t.Helper()
	if err := os.MkdirAll(path, 0o755); err != nil {
		t.Fatal(err)
	}
	for name, value := range map[string]string{
		"cgroup.events": "populated 1\nfrozen 0\n", "cgroup.procs": fmt.Sprintf("%d\n", pid), "memory.max": max,
	} {
		if err := os.WriteFile(filepath.Join(path, name), []byte(value), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func writeFakeCgroupRoot(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(path, 0o755); err != nil {
		t.Fatal(err)
	}
	for name, value := range map[string]string{
		"cgroup.events": "populated 1\nfrozen 0\n", "cgroup.procs": "", "memory.max": "max\n",
	} {
		if err := os.WriteFile(filepath.Join(path, name), []byte(value), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func TestInventoryLiveStaleCorruptAndLeaseCgroupDedup(t *testing.T) {
	dir := t.TempDir()
	socket, root := filepath.Join(dir, "controller.sock"), filepath.Join(dir, "cgroups")
	writeFakeCgroupRoot(t, root)
	validCG := filepath.Join(root, "valid")
	writeFakeCgroup(t, validCG, strconv.FormatUint(testLeaseCapacity, 10), 424242)
	valid := startHelper(t, "NODECTL_TEST_HELPER=lease", "NODECTL_SOCKET="+socket, "NODECTL_SID=valid", "NODECTL_CGROUP="+validCG)
	corrupt := startHelper(t, "NODECTL_TEST_HELPER=lease", "NODECTL_CORRUPT=1", "NODECTL_SOCKET="+socket, "NODECTL_SID=corrupt", "NODECTL_CGROUP="+filepath.Join(root, "corrupt"))
	stale := resource.LeasePath(socket, "stale")
	if err := os.MkdirAll(filepath.Dir(stale), 0o755); err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(resource.Lease{
		Version: 1, SandboxID: "stale", PID: 999999, ControllerSocket: socket,
		CgroupPath: filepath.Join(root, "stale"), CapacityMemory: testLeaseCapacity,
		CapacityCPUMilli: 1000, FloorMemory: testLeaseFloor, FloorCPUMilli: 500, StartupMemory: testLeaseStartup,
	})
	if err := os.WriteFile(stale, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	state := NewState(4<<30, 4000, 0, 0, Watermarks{StartupFactor: .5, HighFactor: .85, LowFactor: .7, EmergencyFactor: .05})
	inventory := &Inventory{ControllerSocket: socket, CgroupScanPaths: []string{root}, Pool: state.AllocatablePool, Logf: t.Logf}
	if err := inventory.Recover(state); err != nil {
		t.Fatal(err)
	}
	snapshot := state.ResourceSnapshot()
	if snapshot.ReservationCount != 2 || snapshot.ProvisionalCount != 2 || snapshot.UnknownCount != 1 {
		t.Fatalf("snapshot = %+v", snapshot)
	}
	if want := testLeaseCapacity + state.AllocatablePool.MemoryBytes; snapshot.Allocated.MemoryBytes != want {
		t.Fatalf("allocated=%d want=%d", snapshot.Allocated.MemoryBytes, want)
	}
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Fatalf("stale lease not removed: %v", err)
	}
	valid.send("stop")
	corrupt.send("stop")
}

func TestInventoryOrphanCgroupSafeUpperBound(t *testing.T) {
	root := filepath.Join(t.TempDir(), "cgroups")
	writeFakeCgroupRoot(t, root)
	writeFakeCgroup(t, filepath.Join(root, "finite"), strconv.FormatUint(300<<20, 10), 101)
	writeFakeCgroup(t, filepath.Join(root, "unbounded"), "max", 102)
	state := NewState(2<<30, 2000, 0, 0, Watermarks{StartupFactor: .5})
	inventory := &Inventory{ControllerSocket: filepath.Join(t.TempDir(), "controller.sock"), CgroupScanPaths: []string{root}, Pool: state.AllocatablePool}
	if err := inventory.Recover(state); err != nil {
		t.Fatal(err)
	}
	snapshot := state.ResourceSnapshot()
	if snapshot.ReservationCount != 2 || snapshot.Allocated.MemoryBytes != 300<<20+state.AllocatablePool.MemoryBytes {
		t.Fatalf("orphan snapshot = %+v", snapshot)
	}
}

func TestSweeperRetainsLiveCgroupAcrossStartupAndHeartbeatTimeouts(t *testing.T) {
	root := filepath.Join(t.TempDir(), "cgroups")
	cgroup := filepath.Join(root, "consumer")
	writeFakeCgroup(t, cgroup, "536870912", 202)
	state := NewState(2<<30, 2000, 0, 0, Watermarks{StartupFactor: .5})
	installReservationForTest(t, state, Reservation{
		Token: "startup-token", SandboxID: "startup-live", CgroupPath: cgroup,
		Stage: StageStartup, StageEnteredAt: time.Now().Add(-time.Hour),
		LastHeartbeatAt: time.Now().Add(-time.Hour), AllocatableNowMem: 512 << 20,
		Capacity: Resources{MemoryBytes: 512 << 20}, RecoverySource: RecoveryCgroup,
	})
	inventory := &Inventory{ControllerSocket: filepath.Join(t.TempDir(), "controller.sock"), CgroupScanPaths: []string{root}, Pool: state.AllocatablePool}
	sweeper := &IdleSweeper{
		State: state, Admission: NewAdmissionController(AdmissionPolicy{}),
		Allocator: NewAllocator(AllocatorPolicy{}), Inventory: inventory,
		StartupTTL: time.Minute, Heartbeat: time.Second, Logf: t.Logf,
	}
	sweeper.sweep()
	if got := reservationForTest(t, state, "startup-live"); !got.StartupExpired {
		t.Fatal("live startup reservation was not marked expired")
	}
	if err := os.WriteFile(filepath.Join(cgroup, "cgroup.events"), []byte("populated 0\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	sweeper.sweep()
	if _, found := state.SnapshotSandboxResource("startup-live"); found {
		t.Fatal("dead cgroup reservation retained")
	}
}

func TestStateSyncReplacesProvisionalAndValidatesManagedIdentity(t *testing.T) {
	dir := t.TempDir()
	socket, root, runRoot := filepath.Join(dir, "controller.sock"), filepath.Join(dir, "cgroups"), filepath.Join(dir, "run")
	writeFakeCgroupRoot(t, root)
	sid, cgroup := "managed-sandbox", filepath.Join(root, "consumer")
	writeFakeCgroup(t, cgroup, strconv.FormatUint(testLeaseCapacity, 10), 333)
	managedDir := filepath.Join(runRoot, sid)
	if err := os.MkdirAll(managedDir, 0o755); err != nil {
		t.Fatal(err)
	}
	yaml := fmt.Sprintf(`resources:
  capacity: {cpu: 1, memory: 512MiB}
  allocatable: {cpu: 0.5, memory: 128MiB}
  control: {controller: %q}
  startup: {memory: 256MiB}
`, socket)
	if err := os.WriteFile(filepath.Join(managedDir, sid+".yaml"), []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	client := startHelper(t, "NODECTL_TEST_HELPER=lease", "NODECTL_SOCKET="+socket,
		"NODECTL_SID="+sid, "NODECTL_CGROUP="+cgroup, "NODECTL_PIDFILE="+filepath.Join(managedDir, sid+".pid"))
	state := NewState(4<<30, 4000, 0, 0, Watermarks{StartupFactor: .5, HighFactor: .85, LowFactor: .7, EmergencyFactor: .05})
	inventory := &Inventory{
		ControllerSocket: socket, CgroupScanPaths: []string{root}, ManagedRunRoot: runRoot,
		Pool: state.AllocatablePool, Logf: t.Logf,
		processVMMCgroup: func(int) string { return cgroup },
	}
	admission := NewAdmissionController(AdmissionPolicy{Rate: 10, Burst: 10})
	allocator := NewAllocator(AllocatorPolicy{MemoryGrantPerSecBytes: 1 << 30})
	srv := &Server{Path: socket, State: state, Admission: admission, Allocator: allocator, Inventory: inventory, Logf: t.Logf}
	if err := srv.Listen(); err != nil {
		t.Fatal(err)
	}
	before := state.ResourceSnapshot()
	if before.Allocated.MemoryBytes != testLeaseCapacity || before.ProvisionalCount != 1 {
		t.Fatalf("before sync = %+v", before)
	}
	live, err := inventory.LookupLiveLease(sid)
	if err != nil {
		t.Fatal(err)
	}
	inventory.processVMMCgroup = func(int) string { return filepath.Join(root, "different-vmm") }
	if err := inventory.ValidateManaged(live, client.cmd.Process.Pid); err == nil || !strings.Contains(err.Error(), "process cgroup") {
		t.Fatalf("managed cgroup mismatch accepted: %v", err)
	}
	inventory.processVMMCgroup = func(int) string { return cgroup }
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = srv.Serve(ctx); close(done) }()
	defer func() { cancel(); <-done }()

	// A different process cannot use the child's live lease.
	attacker := &resource.Client{SocketPath: socket}
	if err := attacker.Connect(); err != nil {
		t.Fatal(err)
	}
	if _, err := attacker.StateSync(resource.StateSyncParams{SandboxID: sid, AppliedAllocatableMemory: 192 << 20, Settled: true}); err == nil {
		t.Fatal("peer PID mismatch accepted")
	}
	_ = attacker.Close()

	client.send("sync")
	if line := client.next(t); line != "sync-ok" {
		t.Fatalf("managed sync = %s", line)
	}
	after := state.ResourceSnapshot()
	if after.Allocated.MemoryBytes != 192<<20 || after.StartupInFlight != 0 || after.ProvisionalCount != 0 || after.ReservationCount != 1 {
		t.Fatalf("after sync = %+v", after)
	}
	res := reservationForTest(t, state, sid)
	if res.RecoverySource != RecoverySynced || res.PeerPID != client.cmd.Process.Pid || res.Conn == nil {
		t.Fatalf("synced reservation = %+v", res)
	}
	client.send("stop")
}

func TestStateSyncRejectsAllocationOutsideLease(t *testing.T) {
	// Exercise the range check directly after lease/peer validation through a
	// real Unix connection in the managed test helper.
	dir := t.TempDir()
	socket, root, sid := filepath.Join(dir, "controller.sock"), filepath.Join(dir, "cgroups"), "direct-bad"
	writeFakeCgroupRoot(t, root)
	cgroup := filepath.Join(root, "consumer")
	writeFakeCgroup(t, cgroup, strconv.FormatUint(testLeaseCapacity, 10), 444)
	client := startHelper(t, "NODECTL_TEST_HELPER=lease", "NODECTL_SOCKET="+socket, "NODECTL_SID="+sid, "NODECTL_CGROUP="+cgroup)
	state := NewState(4<<30, 4000, 0, 0, Watermarks{StartupFactor: .5})
	inventory := &Inventory{ControllerSocket: socket, CgroupScanPaths: []string{root}, Pool: state.AllocatablePool}
	srv := &Server{Path: socket, State: state, Admission: NewAdmissionController(AdmissionPolicy{}), Allocator: NewAllocator(AllocatorPolicy{}), Inventory: inventory}
	if err := srv.Listen(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = srv.Serve(ctx); close(done) }()
	client.send("syncbad")
	if line := client.next(t); !strings.Contains(line, "outside floor/capacity") {
		t.Fatalf("bad sync response = %q", line)
	}
	client.send("stop")
	cancel()
	<-done
}

func TestDirectStateSyncUsesLeaseWithoutManagedFiles(t *testing.T) {
	dir := t.TempDir()
	socket, root, sid := filepath.Join(dir, "controller.sock"), filepath.Join(dir, "cgroups"), "direct-ok"
	writeFakeCgroupRoot(t, root)
	cgroup := filepath.Join(root, "consumer")
	writeFakeCgroup(t, cgroup, strconv.FormatUint(testLeaseCapacity, 10), 555)
	client := startHelper(t, "NODECTL_TEST_HELPER=lease", "NODECTL_SOCKET="+socket,
		"NODECTL_SID="+sid, "NODECTL_CGROUP="+cgroup)
	state := NewState(4<<30, 4000, 0, 0, Watermarks{StartupFactor: .5})
	inventory := &Inventory{
		ControllerSocket: socket, CgroupScanPaths: []string{root},
		ManagedRunRoot: filepath.Join(dir, "run"), Pool: state.AllocatablePool,
	}
	srv := &Server{
		Path: socket, State: state, Admission: NewAdmissionController(AdmissionPolicy{}),
		Allocator: NewAllocator(AllocatorPolicy{}), Inventory: inventory,
	}
	if err := srv.Listen(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = srv.Serve(ctx); close(done) }()
	defer func() { cancel(); <-done }()

	client.send("sync")
	if line := client.next(t); line != "sync-ok" {
		t.Fatalf("direct sync = %s", line)
	}
	res := reservationForTest(t, state, sid)
	if res.Provisional || res.RecoverySource != RecoverySynced || res.PeerPID != client.cmd.Process.Pid {
		t.Fatalf("direct synced reservation = %+v", res)
	}
	client.send("stop")
}

func TestInventoryCgroupInspectionFailureFailsClosed(t *testing.T) {
	root := filepath.Join(t.TempDir(), "cgroups")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "cgroup.events"), []byte("populated unknown\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	state := makeState(2<<30, 0)
	inventory := &Inventory{
		ControllerSocket: filepath.Join(t.TempDir(), "controller.sock"),
		CgroupScanPaths:  []string{root}, Pool: state.AllocatablePool,
	}
	if err := inventory.Recover(state); err == nil || !strings.Contains(err.Error(), "invalid populated value") {
		t.Fatalf("Recover did not fail closed on invalid cgroup inventory: %v", err)
	}

	installReservationForTest(t, state, Reservation{
		Token: "inspection-unknown", SandboxID: "inspection-unknown",
		CgroupPath: root, Stage: StageSettled, AllocatableNowMem: 512 << 20,
	})
	if !inventory.ConsumerLive(reservationForTest(t, state, "inspection-unknown")) {
		t.Fatal("unknown cgroup liveness was treated as dead")
	}
}

func TestConsumerLivenessRetainsUnknownOrChangedLeaseOwner(t *testing.T) {
	dir := t.TempDir()
	badPath := filepath.Join(dir, "bad-lease")
	if err := os.Symlink("missing-target", badPath); err != nil {
		t.Fatal(err)
	}
	inventory := &Inventory{}
	if !inventory.ConsumerLive(Reservation{LeasePath: badPath, CgroupPath: filepath.Join(dir, "missing-cgroup")}) {
		t.Fatal("lease inspection error released an unknown consumer")
	}

	socket := filepath.Join(dir, "controller.sock")
	helper := startHelper(t, "NODECTL_TEST_HELPER=lease", "NODECTL_SOCKET="+socket,
		"NODECTL_SID=owner-changed", "NODECTL_CGROUP="+filepath.Join(dir, "cgroup"))
	if !inventory.ConsumerLive(Reservation{
		LeasePath: resource.LeasePath(socket, "owner-changed"), PeerPID: helper.cmd.Process.Pid + 1,
		CgroupPath: filepath.Join(dir, "missing-cgroup"),
	}) {
		t.Fatal("live lock with changed owner released a consumer")
	}
	helper.send("stop")
}
