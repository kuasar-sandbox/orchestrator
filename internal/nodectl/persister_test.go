package nodectl

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestPersisterRoundTrip(t *testing.T) {
	dir := t.TempDir()
	p := &Persister{Path: filepath.Join(dir, "state.json")}

	src := makeState(100<<30, 16<<30)
	installReservationForTest(t, src, Reservation{
		Token:             "t1",
		SandboxID:         "sb-1",
		Stage:             StageSettled,
		StageEnteredAt:    time.Now(),
		LastHeartbeatAt:   time.Now(),
		AllocatableNowMem: 256 << 20,
		Capacity:          Resources{MemoryBytes: 4 << 30, CPUMilli: 2000},
		Floor:             Resources{MemoryBytes: 128 << 20, CPUMilli: 100},
		LastReportedRSS:   96 << 20,
		LastReportAt:      time.Unix(1_800_000_000, 0).UTC(),
	})

	if err := p.Flush(src); err != nil {
		t.Fatal(err)
	}
	loaded, err := p.Load()
	if err != nil {
		t.Fatal(err)
	}
	if loaded == nil {
		t.Fatal("loaded state is nil")
	}
	if loaded.NodeBudget.MemoryBytes != src.NodeBudget.MemoryBytes {
		t.Errorf("NodeBudget mismatch: got %d, want %d",
			loaded.NodeBudget.MemoryBytes, src.NodeBudget.MemoryBytes)
	}
	r := reservationByTokenForTest(t, loaded, "t1")
	if r.SandboxID != "sb-1" || r.AllocatableNowMem != 256<<20 {
		t.Errorf("reservation fields wrong: %+v", r)
	}
	if snapshot, found := loaded.SnapshotSandboxResource("sb-1"); !found ||
		snapshot.LastReportedRSS != 96<<20 || snapshot.LastReportAt.Unix() != 1_800_000_000 {
		t.Fatalf("loaded SID index/resource snapshot = %+v found=%v", snapshot, found)
	}
}

func TestPersisterLoadMissing(t *testing.T) {
	p := &Persister{Path: filepath.Join(t.TempDir(), "missing.json")}
	loaded, err := p.Load()
	if err != nil {
		t.Errorf("loading missing file should not error, got: %v", err)
	}
	if loaded != nil {
		t.Errorf("loading missing file should return nil, got: %+v", loaded)
	}
}

func TestPersisterEmptyPath(t *testing.T) {
	p := &Persister{Path: ""}
	if err := p.Flush(makeState(1<<30, 0)); err != nil {
		t.Errorf("Flush with empty Path should be no-op, got: %v", err)
	}
	loaded, err := p.Load()
	if err != nil || loaded != nil {
		t.Errorf("Load with empty Path: got (%v, %v), want (nil, nil)", loaded, err)
	}
}

func TestPersisterAtomicWrite(t *testing.T) {
	dir := t.TempDir()
	p := &Persister{Path: filepath.Join(dir, "atomic.json")}
	s1 := makeState(100<<30, 16<<30)
	if err := p.Flush(s1); err != nil {
		t.Fatal(err)
	}
	s2 := makeState(200<<30, 16<<30)
	if err := p.Flush(s2); err != nil {
		t.Fatal(err)
	}
	loaded, err := p.Load()
	if err != nil {
		t.Fatal(err)
	}
	if loaded.NodeBudget.MemoryBytes != s2.NodeBudget.MemoryBytes {
		t.Errorf("got = %d, want %d (latest)",
			loaded.NodeBudget.MemoryBytes, s2.NodeBudget.MemoryBytes)
	}
}

func TestPersisterLoadsLegacyReservationFieldNames(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	payload := `{
  "version": 1,
  "node_budget": {"memory_bytes": 1073741824, "cpu_milli": 1000},
  "host_reserved": {"memory_bytes": 0, "cpu_milli": 0},
  "operational_margin": {"memory_bytes": 0, "cpu_milli": 0},
  "allocatable_pool": {"memory_bytes": 1073741824, "cpu_milli": 1000},
  "watermarks": {},
  "reservations": {
    "legacy-token": {
      "token": "legacy-token",
      "sandbox_id": "legacy-sid",
      "sandbox_ctl_pid": 1234,
      "cgroup_path": "/sys/fs/cgroup/legacy",
      "capacity": {"memory_bytes": 536870912, "cpu_milli": 1000},
      "floor": {"memory_bytes": 134217728, "cpu_milli": 500},
      "allocatable_now_mem": 268435456,
      "effective_startup_budget": 0,
      "stage": "settled",
      "stage_entered_at": "2026-01-01T00:00:00Z",
      "last_heartbeat_at": "2026-01-01T00:00:00Z",
      "oom_count": 0
    }
  }
}`
	if err := os.WriteFile(path, []byte(payload), 0o600); err != nil {
		t.Fatal(err)
	}
	loaded, err := (&Persister{Path: path}).Load()
	if err != nil {
		t.Fatal(err)
	}
	r := reservationByTokenForTest(t, loaded, "legacy-token")
	if r.SandboxID != "legacy-sid" || r.PeerPID != 1234 || r.CgroupPath != "/sys/fs/cgroup/legacy" {
		t.Fatalf("legacy reservation = %+v", r)
	}
}

func TestPersisterSerializesConcurrentFlushes(t *testing.T) {
	p := &Persister{Path: filepath.Join(t.TempDir(), "state.json")}
	states := []*State{makeState(100<<30, 16<<30), makeState(200<<30, 16<<30)}
	var wg sync.WaitGroup
	errs := make(chan error, 32)
	for n := 0; n < 32; n++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			errs <- p.Flush(states[index%len(states)])
		}(n)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, err := p.Load(); err != nil {
		t.Fatalf("concurrent flush left invalid snapshot: %v", err)
	}
}
