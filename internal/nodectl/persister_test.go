package nodectl

import (
	"path/filepath"
	"testing"
	"time"
)

func TestPersisterRoundTrip(t *testing.T) {
	dir := t.TempDir()
	p := &Persister{Path: filepath.Join(dir, "state.json")}

	src := makeState(100<<30, 16<<30)
	src.Lock()
	src.Reservations["t1"] = &Reservation{
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
	}
	src.Unlock()

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
	r := loaded.Reservations["t1"]
	if r == nil {
		t.Fatal("reservation t1 missing")
	}
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
