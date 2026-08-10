package nodectl

import (
	"encoding/json"
	"fmt"
	"net"
	"path/filepath"
	"sync"
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

func TestPersisterConcurrentFlushesShareOneAtomicWriter(t *testing.T) {
	p := &Persister{Path: filepath.Join(t.TempDir(), "state.json")}
	const workers = 32
	const flushesPerWorker = 20

	start := make(chan struct{})
	errs := make(chan error, workers*flushesPerWorker)
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			state := makeState(uint64(100+worker)<<30, 16<<30)
			<-start
			for flush := 0; flush < flushesPerWorker; flush++ {
				if err := p.Flush(state); err != nil {
					errs <- err
				}
			}
		}(worker)
	}
	close(start)
	wg.Wait()
	close(errs)

	for err := range errs {
		t.Fatalf("concurrent Flush failed: %v", err)
	}
	if _, err := p.Load(); err != nil {
		t.Fatalf("final state is not valid JSON: %v", err)
	}
}

func TestPersisterFlushSnapshotsConcurrentStateUpdates(t *testing.T) {
	p := &Persister{Path: filepath.Join(t.TempDir(), "state.json")}
	state := makeState(100<<30, 16<<30)
	const iterations = 200

	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < iterations; i++ {
			state.Lock()
			state.Reservations["moving"] = &Reservation{
				Token: "moving", SandboxID: "sandbox", Stage: StageSettled,
				AllocatableNowMem: uint64(i + 1),
			}
			if i%2 == 0 {
				delete(state.Reservations, "moving")
			}
			state.Unlock()
		}
	}()
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < iterations; i++ {
			if err := p.Flush(state); err != nil {
				t.Errorf("Flush failed: %v", err)
				return
			}
		}
	}()
	close(start)
	wg.Wait()
}

func TestStateSnapshotForPersistenceCopiesAllPersistedFields(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	state := NewState(1, 2, 3, 4, Watermarks{
		OperationalMarginFactor: 0.01,
		HighFactor:              0.02,
		LowFactor:               0.03,
		EmergencyFactor:         0.04,
		StartupFactor:           0.05,
	})
	state.Lock()
	state.NodeBudget = Resources{MemoryBytes: 10, CPUMilli: 11}
	state.HostReserved = Resources{MemoryBytes: 12, CPUMilli: 13}
	state.OperationalMargin = Resources{MemoryBytes: 14, CPUMilli: 15}
	state.AllocatablePool = Resources{MemoryBytes: 16, CPUMilli: 17}
	state.Version = 18
	reservation := &Reservation{
		Token:                  "token",
		SandboxID:              "sandbox",
		SandboxCtlPID:          19,
		CgroupPath:             "/cgroup/sandbox",
		Capacity:               Resources{MemoryBytes: 20, CPUMilli: 21},
		Floor:                  Resources{MemoryBytes: 22, CPUMilli: 23},
		AllocatableNowMem:      24,
		EffectiveStartupBudget: 25,
		Stage:                  StageSettled,
		StageEnteredAt:         time.Unix(26, 0).UTC(),
		LastHeartbeatAt:        time.Unix(27, 0).UTC(),
		OOMCount:               28,
		LastReportedRSS:        29,
		Conn:                   serverConn,
	}
	state.Reservations[reservation.Token] = reservation
	state.Unlock()

	wantJSON, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := state.snapshotForPersistence()
	gotJSON, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if string(gotJSON) != string(wantJSON) {
		t.Fatalf("snapshot JSON differs from source:\n got %s\nwant %s", gotJSON, wantJSON)
	}

	snapshotReservation := snapshot.Reservations[reservation.Token]
	if snapshotReservation == reservation {
		t.Fatal("snapshot retained source reservation pointer")
	}
	if snapshotReservation.Conn != nil {
		t.Fatal("snapshot retained live connection")
	}

	state.Lock()
	state.NodeBudget.MemoryBytes = 100
	reservation.SandboxID = "changed"
	delete(state.Reservations, reservation.Token)
	state.Unlock()
	if snapshot.NodeBudget.MemoryBytes != 10 {
		t.Fatalf("snapshot node budget changed with source: got %d, want 10", snapshot.NodeBudget.MemoryBytes)
	}
	if snapshot.Reservations[reservation.Token].SandboxID != "sandbox" {
		t.Fatalf("snapshot reservation changed with source: got %q, want sandbox", snapshot.Reservations[reservation.Token].SandboxID)
	}
}

func benchmarkState(reservationCount int) *State {
	state := makeState(100<<30, 16<<30)
	state.Lock()
	defer state.Unlock()
	for index := 0; index < reservationCount; index++ {
		token := fmt.Sprintf("token-%d", index)
		state.Reservations[token] = &Reservation{
			Token:                  token,
			SandboxID:              fmt.Sprintf("sandbox-%d", index),
			Capacity:               Resources{MemoryBytes: 4 << 30, CPUMilli: 2000},
			Floor:                  Resources{MemoryBytes: 128 << 20, CPUMilli: 100},
			AllocatableNowMem:      256 << 20,
			EffectiveStartupBudget: 512 << 20,
			Stage:                  StageSettled,
			StageEnteredAt:         time.Unix(int64(index), 0),
			LastHeartbeatAt:        time.Unix(int64(index+1), 0),
			LastReportedRSS:        192 << 20,
		}
	}
	return state
}

func BenchmarkStateSnapshotForPersistence(b *testing.B) {
	for _, reservationCount := range []int{0, 10, 100, 1000} {
		b.Run(fmt.Sprintf("reservations-%d", reservationCount), func(b *testing.B) {
			state := benchmarkState(reservationCount)
			b.ReportAllocs()
			for b.Loop() {
				_ = state.snapshotForPersistence()
			}
		})
	}
}

func BenchmarkPersisterFlushParallel(b *testing.B) {
	state := benchmarkState(100)
	p := &Persister{Path: filepath.Join(b.TempDir(), "state.json")}
	var once sync.Once
	var flushErr error
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := p.Flush(state); err != nil {
				once.Do(func() { flushErr = err })
				return
			}
		}
	})
	b.StopTimer()
	if flushErr != nil {
		b.Fatal(flushErr)
	}
}
