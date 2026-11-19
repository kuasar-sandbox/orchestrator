package nodectl

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"

	"github.com/kuasar-sandbox/sandboxer/pkg/resource"
)

func TestCacheInventoryUnknownBlocksGrantsWithoutQuotaDemand(t *testing.T) {
	for _, source := range []string{"lease", "managed", "cgroup"} {
		for _, limited := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/limited=%v", source, limited), func(t *testing.T) {
				s := makeState(8<<30, 0)
				s.Wm.StartupFactor = .5
				if limited {
					s.CachePoolCapacity = 64 << 20
				}
				token := admitCacheTest(t, s, "known", 64<<20)
				mustCacheGrant(t, s, token, 0, 8<<20, 64<<20, nil)
				dir := t.TempDir()
				root, socket := filepath.Join(dir, "cgroups"), filepath.Join(dir, "node.sock")
				writeFakeCgroupRoot(t, root)
				inventory := &Inventory{ControllerSocket: socket, CgroupScanPaths: []string{root}, Pool: s.AllocatablePool, Logf: t.Logf}
				var helper *helperProcess
				switch source {
				case "lease":
					helper = startHelper(t, "NODECTL_TEST_HELPER=lease", "NODECTL_CORRUPT=1", "NODECTL_SOCKET="+socket, "NODECTL_SID=unknown", "NODECTL_CGROUP="+filepath.Join(root, "unknown"))
				case "managed":
					inventory.ManagedRunRoot = filepath.Join(dir, "run")
					managed := filepath.Join(inventory.ManagedRunRoot, "unknown")
					if err := os.MkdirAll(managed, 0o755); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(filepath.Join(managed, "unknown.yaml"), []byte("[invalid yaml"), 0o600); err != nil {
						t.Fatal(err)
					}
					helper = startHelper(t, "NODECTL_TEST_HELPER=lease", "NODECTL_NO_LEASE=1", "NODECTL_SOCKET="+socket, "NODECTL_SID=unknown", "NODECTL_PIDFILE="+filepath.Join(managed, "unknown.pid"))
				case "cgroup":
					writeFakeCgroup(t, filepath.Join(root, "unknown"), "268435456", 999999)
				}
				if err := inventory.Recover(s); err != nil {
					t.Fatal(err)
				}
				var unknown Reservation
				for _, r := range s.ReservationViews() {
					if r.CacheRecoveryUnknown {
						unknown = r
					}
				}
				if unknown.SandboxID == "" || s.ResourceSnapshot().CacheRecoveryUnknownCount != 1 || s.reservedCache != 8<<20 {
					t.Fatalf("unknown cache not retained separately: %+v", s.ResourceSnapshot())
				}
				result := mustCacheGrant(t, s, token, 4<<20, 4<<20, 64<<20, nil)
				if result.GrantedDelta != 0 || !result.RecoveryBlocked || s.reservedCache != 4<<20 || len(s.cacheDemandSince) != 0 {
					t.Fatalf("unknown cache granted or fabricated shortage: %+v", result)
				}
				launch := LaunchAdmission{Operation: OperationCreate, Identity: "waiting"}
				if oc := s.AnalyzeLaunchWithCache("waiting", 64<<20, 4<<20, launch); oc.Status != OutcomeShortTermBlock || oc.Block != BlockedByCacheRecovery {
					t.Fatalf("cache recovery not an independent admission block: %+v", oc)
				}
				s.RecordAdmissionWaitWithCache("waiting", launch, 64<<20, 4<<20)
				if len(s.admissionCacheDemand) != 0 || len(s.PressureSnapshot().Demands) != 0 {
					t.Fatal("unknown recovery became cache quota or Guest pressure demand")
				}
				if helper != nil {
					helper.send("stop")
					if err := helper.cmd.Wait(); err != nil {
						t.Fatal(err)
					}
				} else if err := os.WriteFile(filepath.Join(root, "unknown", "cgroup.events"), []byte("populated 0\n"), 0o600); err != nil {
					t.Fatal(err)
				}
				if inventory.ConsumerLive(unknown) {
					t.Fatal("test consumer did not exit")
				}
				if _, found, err := s.RemoveIfIdentity(unknown.SandboxID, unknown.identity()); err != nil || !found {
					t.Fatalf("confirmed cleanup: %v %v", found, err)
				}
				if s.ResourceSnapshot().CacheRecoveryUnknownCount != 0 {
					t.Fatal("confirmed exit retained cache recovery barrier")
				}
				if result := mustCacheGrant(t, s, token, 4<<20, 4<<20, 64<<20, nil); result.GrantedDelta != 4<<20 || result.RecoveryBlocked {
					t.Fatalf("cleanup did not reopen independent cache grants: %+v", result)
				}
			})
		}
	}
}

func TestCacheInventoryManagedYAMLRecoversLegacyUpperBound(t *testing.T) {
	for _, lease := range []bool{false, true} {
		t.Run(fmt.Sprintf("lease=%v", lease), func(t *testing.T) {
			dir := t.TempDir()
			root, runRoot, socket := filepath.Join(dir, "cg"), filepath.Join(dir, "run"), filepath.Join(dir, "node.sock")
			sid := "legacy-cache"
			managed, cg := filepath.Join(runRoot, sid), filepath.Join(root, "vmm")
			writeFakeCgroupRoot(t, root)
			writeFakeCgroup(t, cg, strconv.FormatUint(testLeaseCapacity, 10), 999999)
			if err := os.MkdirAll(managed, 0o755); err != nil {
				t.Fatal(err)
			}
			yaml := fmt.Sprintf("resources:\n  capacity: {cpu: 1, memory: 512MiB}\n  allocatable: {cpu: 0.5, memory: 128MiB}\n  control: {controller: %q}\n  startup: {memory: 256MiB}\n  diff_cow: {cache_size: 128MiB}\n", socket)
			if err := os.WriteFile(filepath.Join(managed, sid+".yaml"), []byte(yaml), 0o600); err != nil {
				t.Fatal(err)
			}
			env := []string{"NODECTL_TEST_HELPER=lease", "NODECTL_SOCKET=" + socket, "NODECTL_SID=" + sid, "NODECTL_CGROUP=" + cg, "NODECTL_PIDFILE=" + filepath.Join(managed, sid+".pid")}
			if !lease {
				env = append(env, "NODECTL_NO_LEASE=1")
			}
			helper := startHelper(t, env...)
			s := makeState(4<<30, 0)
			s.CachePoolCapacity = 32 << 20 // Recovery must retain an over-cap maximum.
			inventory := &Inventory{ControllerSocket: socket, CgroupScanPaths: []string{root}, ManagedRunRoot: runRoot, Pool: s.AllocatablePool, processVMMCgroup: func(int) string { return cg }, Logf: t.Logf}
			if err := inventory.Recover(s); err != nil {
				t.Fatal(err)
			}
			r := reservationForTest(t, s, sid)
			if r.CacheReservation != 128<<20 || r.CacheMax != 128<<20 || r.CacheRecoveryUnknown || resource.HasFeature(r.ClientFeatures, resource.FeatureCacheBudgetV1) || s.reservedMemory != testLeaseCapacity {
				t.Fatalf("managed cache recovery lost upper bound or fabricated capability: %+v", r)
			}
			_, _, err := s.Sync(SyncSpec{SandboxID: sid, Token: "legacy-sync", PeerPID: helper.cmd.Process.Pid, CgroupPath: cg,
				Capacity: r.Capacity, ConfiguredAllocatable: r.ConfiguredAllocatable, ReservationMemory: 256 << 20,
				ClientFeatures: r.ClientFeatures, Settled: true, LeasePath: r.LeasePath})
			if err != nil {
				t.Fatal(err)
			}
			if s.reservedCache != 128<<20 || s.ResourceSnapshot().CacheRecoveryUnknownCount != 0 {
				t.Fatal("legacy StateSync without cache fields released conservative cached bytes")
			}
		})
	}
}

func TestCacheUnknownSyncEstablishesAuthenticatedUpperBound(t *testing.T) {
	for _, source := range []string{RecoveryUnknownLease, RecoveryUnknownManaged, RecoveryCgroup} {
		t.Run(source, func(t *testing.T) {
			s := makeState(4<<30, 0)
			s.CachePoolCapacity = 32 << 20
			spec := ProvisionalSpec{SandboxID: "recovering", PeerPID: 123, CgroupPath: "/cg/recovering", Capacity: Resources{MemoryBytes: 512 << 20}, ReservationMemory: 512 << 20, InitialBudget: 512 << 20, RecoverySource: source, RecoveryKey: "unknown", CacheRecoveryUnknown: true}
			if source == RecoveryUnknownLease {
				spec.SandboxID, spec.CgroupPath, spec.LeasePath = "unknown-synthetic", "", "/leases/recovering.json"
			}
			if source == RecoveryCgroup {
				spec.SandboxID, spec.PeerPID = "orphan-synthetic", 0
			}
			if err := s.InstallProvisional(spec); err != nil {
				t.Fatal(err)
			}
			sync := SyncSpec{SandboxID: "recovering", PeerPID: 123, Token: "synced", CgroupPath: "/cg/recovering", LeasePath: spec.LeasePath,
				Capacity: Resources{MemoryBytes: 512 << 20}, ReservationMemory: 128 << 20, Settled: true,
				CacheReservation: 64 << 20, CacheMax: 128 << 20, ClientFeatures: []string{resource.FeatureCacheBudgetV1, resource.CacheMaximumFeature(128 << 20)}}
			if source == RecoveryUnknownLease {
				bad := sync
				bad.LeasePath = "/leases/other.json"
				if _, _, err := s.Sync(bad); err == nil || s.cacheRecoveryUnknownCount != 1 {
					t.Fatal("PID-only match cleared unknown lease")
				}
			}
			if _, _, err := s.Sync(sync); err != nil {
				t.Fatal(err)
			}
			if s.cacheRecoveryUnknownCount != 0 || s.reservedCache != 64<<20 || s.reservedMemory != 128<<20 || len(s.bySID) != 1 {
				t.Fatalf("authenticated sync did not replace barrier independently: %+v", s.ResourceSnapshot())
			}
			if result := mustCacheGrant(t, s, "synced", 64<<20, 4<<20, 128<<20, nil); result.GrantedDelta != 0 || result.RecoveryBlocked {
				t.Fatalf("over-cap recovery confused with unknown recovery: %+v", result)
			}
		})
	}
}

func TestCacheUnknownCannotOverrideKnownGrantOrDisappearOnLegacySync(t *testing.T) {
	s := makeState(4<<30, 0)
	token := admitCacheTest(t, s, "known", 64<<20)
	mustCacheGrant(t, s, token, 0, 8<<20, 64<<20, nil)
	unknown := ProvisionalSpec{SandboxID: "orphan", CgroupPath: "/cg/recovered", Capacity: Resources{MemoryBytes: 512 << 20}, ReservationMemory: 512 << 20, InitialBudget: 512 << 20, RecoverySource: RecoveryCgroup, RecoveryKey: "orphan", CacheRecoveryUnknown: true}
	if err := s.InstallProvisional(unknown); err != nil {
		t.Fatal(err)
	}
	before := s.ResourceSnapshot()
	_, _, err := s.Sync(SyncSpec{SandboxID: "known", PeerPID: 123, Token: "replacement", CgroupPath: unknown.CgroupPath,
		Capacity: Resources{MemoryBytes: 2 << 30, CPUMilli: 1000}, ConfiguredAllocatable: Resources{MemoryBytes: 128 << 20, CPUMilli: 1000},
		ReservationMemory: 256 << 20, CacheReservation: 16 << 20, CacheMax: 64 << 20, ClientFeatures: []string{resource.FeatureCacheBudgetV1}, Settled: true})
	if err == nil || before != s.ResourceSnapshot() {
		t.Fatal("unknown duplicate authorized growth above known cache grant")
	}
	if _, _, err := s.Sync(SyncSpec{SandboxID: "legacy", PeerPID: 124, Token: "legacy-token", CgroupPath: unknown.CgroupPath, Capacity: unknown.Capacity, ReservationMemory: 256 << 20, Settled: true}); err != nil {
		t.Fatal(err)
	}
	if s.cacheRecoveryUnknownCount != 1 || !reservationByTokenForTest(t, s, "legacy-token").CacheRecoveryUnknown {
		t.Fatal("legacy StateSync silently converted unknown cache into zero")
	}
	if _, found, err := s.Release("legacy-token"); err != nil || !found || s.cacheRecoveryUnknownCount != 0 {
		t.Fatalf("confirmed Release failed to clear unknown: %v %v", found, err)
	}
}

func TestCacheRepeatedUnknownInventoryCannotDowngradeKnownReservation(t *testing.T) {
	s := makeState(4<<30, 0)
	known := ProvisionalSpec{SandboxID: "known", PeerPID: 123, CgroupPath: "/cg/known", Capacity: Resources{MemoryBytes: 512 << 20}, ReservationMemory: 512 << 20, InitialBudget: 512 << 20,
		CacheReservation: 128 << 20, CacheMax: 128 << 20, RecoverySource: RecoveryLease, RecoveryKey: "same", ClientFeatures: []string{resource.FeatureCacheBudgetV1, resource.CacheMaximumFeature(128 << 20)}}
	if err := s.InstallProvisional(known); err != nil {
		t.Fatal(err)
	}
	before := reservationForTest(t, s, "known")
	unknown := known
	unknown.CacheReservation, unknown.CacheMax, unknown.ClientFeatures, unknown.CacheRecoveryUnknown = 0, 0, nil, true
	if err := s.InstallProvisional(unknown); err != nil {
		t.Fatal(err)
	}
	if after := reservationForTest(t, s, "known"); !reflect.DeepEqual(before, after) || s.reservedCache != 128<<20 || s.cacheRecoveryUnknownCount != 0 {
		t.Fatal("weaker repeated inventory lost known cache bound or features")
	}
}

func TestCacheManagedAdmitReplayPreservesLegacyCeiling(t *testing.T) {
	for _, capable := range []bool{false, true} {
		t.Run(fmt.Sprintf("capable=%v", capable), func(t *testing.T) {
			s := makeState(4<<30, 0)
			r := ProvisionalSpec{SandboxID: "managed", PeerPID: 123, CgroupPath: "/cg/managed", Capacity: Resources{MemoryBytes: 512 << 20}, ReservationMemory: 512 << 20, InitialBudget: 512 << 20,
				CacheReservation: 128 << 20, CacheMax: 128 << 20, RecoverySource: RecoveryManagedPIDFile, RecoveryKey: "managed"}
			if err := s.InstallProvisional(r); err != nil {
				t.Fatal(err)
			}
			spec := AdmitSpec{SandboxID: r.SandboxID, PeerPID: r.PeerPID, CgroupPath: r.CgroupPath, Capacity: r.Capacity, Token: "admitted", InitialBudget: 256 << 20}
			want := uint64(128 << 20)
			if capable {
				spec.CacheMax, spec.InitialCache, spec.ClientFeatures = 128<<20, 4<<20, []string{resource.FeatureCacheBudgetV1, resource.CacheMaximumFeature(128 << 20)}
				want = 4 << 20
			}
			if !s.CanReplayAdmit(spec) {
				t.Fatal("trusted managed upper bound not replayable")
			}
			if _, _, err := s.Admit(spec); err != nil {
				t.Fatal(err)
			}
			if s.reservedCache != want || !s.CanReplayAdmit(spec) {
				t.Fatal("cache replay lost conservative ceiling or initial grant")
			}
			spec.Token = "retry"
			if _, _, err := s.Admit(spec); err != nil || s.reservedCache != want {
				t.Fatalf("ACK-lost legacy replay changed cache: %v", err)
			}
		})
	}
}
