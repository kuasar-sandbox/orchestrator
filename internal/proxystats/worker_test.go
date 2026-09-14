package proxystats

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	publicconfig "github.com/kuasar-sandbox/orchestrator/config"
	"github.com/kuasar-sandbox/orchestrator/internal/proxy"
	"github.com/kuasar-sandbox/orchestrator/internal/proxyadmission"
)

type testAddr string

func (a testAddr) Network() string { return string(a) }
func (a testAddr) String() string  { return string(a) }

type testConn struct {
	closes      atomic.Int32
	closeWrites atomic.Int32
}

func (*testConn) Read([]byte) (int, error)         { return 0, io.EOF }
func (*testConn) Write(p []byte) (int, error)      { return len(p), nil }
func (c *testConn) Close() error                   { c.closes.Add(1); return nil }
func (c *testConn) CloseWrite() error              { c.closeWrites.Add(1); return nil }
func (*testConn) LocalAddr() net.Addr              { return testAddr("local") }
func (*testConn) RemoteAddr() net.Addr             { return testAddr("remote") }
func (*testConn) SetDeadline(time.Time) error      { return nil }
func (*testConn) SetReadDeadline(time.Time) error  { return nil }
func (*testConn) SetWriteDeadline(time.Time) error { return nil }

func snapshotWorker(t *testing.T, worker *WorkerStats, sandboxID string) SandboxSnapshot {
	t.Helper()
	entry := worker.lockEntry(sandboxID, false)
	if entry == nil {
		t.Fatalf("sandbox %q has no traffic entry", sandboxID)
	}
	snapshot := snapshotEntry(sandboxID, entry)
	entry.mu.Unlock()
	return snapshot
}

func TestTrafficFlowParkingEgressHalfCloseAndIdempotentClose(t *testing.T) {
	worker := NewWorkerStats()
	flow := worker.BeginParking("s1", proxy.ConnectServiceForward)
	state := snapshotWorker(t, worker, "s1").Services[string(proxy.ConnectServiceForward)]
	if state.Parking != 1 || state.Connected != 0 || state.IdleSince != nil {
		t.Fatalf("parking state = %+v", state)
	}

	base := &testConn{}
	tracked := flow.AttachBackend(base)
	state = snapshotWorker(t, worker, "s1").Services[string(proxy.ConnectServiceForward)]
	if state.Parking != 0 || state.Connected != 1 || state.IdleSince != nil {
		t.Fatalf("attached state = %+v", state)
	}
	flow.Close() // Once attached, the backend Close is authoritative.
	if state = snapshotWorker(t, worker, "s1").Services[string(proxy.ConnectServiceForward)]; state.Connected != 1 {
		t.Fatalf("flow.Close ended attached egress: %+v", state)
	}
	if err := tracked.(interface{ CloseWrite() error }).CloseWrite(); err != nil {
		t.Fatal(err)
	}
	if base.closeWrites.Load() != 1 {
		t.Fatalf("underlying CloseWrite calls = %d", base.closeWrites.Load())
	}
	if state = snapshotWorker(t, worker, "s1").Services[string(proxy.ConnectServiceForward)]; state.Connected != 1 {
		t.Fatalf("CloseWrite ended egress: %+v", state)
	}
	if err := tracked.Close(); err != nil {
		t.Fatal(err)
	}
	if err := tracked.Close(); err != nil {
		t.Fatal(err)
	}
	state = snapshotWorker(t, worker, "s1").Services[string(proxy.ConnectServiceForward)]
	if state.Parking != 0 || state.Connected != 0 || state.IdleSince == nil || state.IdleSinceBootNS == 0 {
		t.Fatalf("closed state = %+v", state)
	}
	if base.closes.Load() != 1 {
		t.Fatalf("underlying Close calls = %d, want 1", base.closes.Load())
	}
}

func TestLimitedTrafficFlowHoldsOneLeaseAcrossParkingEgressAndHalfClose(t *testing.T) {
	master, err := proxyadmission.NewMaster(2, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer master.Close()
	if err := master.BeginWorker(0, 1); err != nil {
		t.Fatal(err)
	}
	file, err := master.DupFile()
	if err != nil {
		t.Fatal(err)
	}
	admission, err := proxyadmission.OpenWorker(file, 2, 1, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer admission.Close()
	update, err := master.PrepareUpsert("s1", "identity", publicconfig.MaxInflight{Total: 1})
	if err != nil {
		t.Fatal(err)
	}
	binding := update.Binding()
	update.Commit()
	worker := NewWorkerStatsWithAdmission(admission)
	flow, err := worker.TryBeginParking("s1", proxy.ConnectServiceForward, binding)
	if err != nil {
		t.Fatal(err)
	}
	assertFull := func(stage string) {
		t.Helper()
		if _, err := worker.TryBeginParking("s1", proxy.ConnectServiceExec, binding); !errors.Is(err, proxyadmission.ErrLimitReached) {
			t.Fatalf("%s admission error=%v, want limit reached", stage, err)
		}
	}
	assertFull("parking")
	base := &testConn{}
	tracked := flow.AttachBackend(base)
	flow.Close()
	assertFull("egress")
	if err := tracked.(interface{ CloseWrite() error }).CloseWrite(); err != nil {
		t.Fatal(err)
	}
	assertFull("half-close")
	if err := tracked.Close(); err != nil {
		t.Fatal(err)
	}
	next, err := worker.TryBeginParking("s1", proxy.ConnectServiceExec, binding)
	if err != nil {
		t.Fatalf("full close did not release admission: %v", err)
	}
	next.Close()
}

func TestTrafficFlowActivationOrDialFailureEndsParking(t *testing.T) {
	worker := NewWorkerStats()
	flow := worker.BeginParking("s1", proxy.ConnectServiceE2BEnvd)
	flow.Close()
	flow.Close()
	state := snapshotWorker(t, worker, "s1").Services[string(proxy.ConnectServiceE2BEnvd)]
	if state.Parking != 0 || state.Connected != 0 || state.IdleSince == nil {
		t.Fatalf("failed flow state = %+v", state)
	}
}

func TestParkingToEgressIsAtomicUnderConcurrency(t *testing.T) {
	worker := NewWorkerStats()
	const flows = 256
	handles := make([]proxy.TrafficFlow, flows)
	for i := range handles {
		handles[i] = worker.BeginParking("s1", proxy.ConnectServiceForward)
	}
	start := make(chan struct{})
	tracked := make([]net.Conn, flows)
	var wg sync.WaitGroup
	wg.Add(flows)
	for i := range handles {
		go func(i int) {
			defer wg.Done()
			<-start
			tracked[i] = handles[i].AttachBackend(&testConn{})
		}(i)
	}
	close(start)
	for {
		state := snapshotWorker(t, worker, "s1").Services[string(proxy.ConnectServiceForward)]
		if state.Parking+state.Connected != flows {
			t.Fatalf("atomic transition exposed total=%d (parking=%d egress=%d), want %d", state.Parking+state.Connected, state.Parking, state.Connected, flows)
		}
		if state.Connected == flows {
			break
		}
	}
	wg.Wait()
	for _, conn := range tracked {
		if err := conn.Close(); err != nil {
			t.Fatal(err)
		}
	}
	state := snapshotWorker(t, worker, "s1").Services[string(proxy.ConnectServiceForward)]
	if state.Parking != 0 || state.Connected != 0 || state.IdleSince == nil {
		t.Fatalf("final concurrent state = %+v", state)
	}
}

func TestSenderMergesDirtyAbsoluteStateAcrossBackpressure(t *testing.T) {
	worker := NewWorkerStats()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	firstUpdate := make(chan struct{})
	unblock := make(chan struct{})
	updates := make(chan Frame, 4)
	var once sync.Once
	senderDone, err := worker.StartSender(ctx, "worker-0", 1, func(frame Frame) error {
		if frame.Type == TypeUpdate {
			once.Do(func() {
				close(firstUpdate)
				<-unblock
			})
			updates <- frame
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	worker.Inc("requests_total")
	select {
	case <-firstUpdate:
	case <-time.After(time.Second):
		t.Fatal("sender did not enter blocked update")
	}
	for range 50 {
		worker.Inc("requests_total")
	}
	close(unblock)
	deadline := time.After(2 * time.Second)
	for {
		select {
		case frame := <-updates:
			if frame.Counters["requests_total"] == 51 {
				cancel()
				<-senderDone
				return
			}
		case <-deadline:
			t.Fatal("sender lost the final absolute counter state")
		}
	}
}

func TestSenderMergesTrafficToLatestAbsoluteStateAcrossBackpressure(t *testing.T) {
	worker := NewWorkerStats()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	firstUpdate := make(chan struct{})
	unblock := make(chan struct{})
	updates := make(chan Frame, 8)
	var once sync.Once
	done, err := worker.StartSender(ctx, "worker-0", 1, func(frame Frame) error {
		if frame.Type == TypeUpdate {
			once.Do(func() {
				close(firstUpdate)
				<-unblock
			})
			updates <- frame
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	flow := worker.BeginParking("s1", proxy.ConnectServiceForward)
	select {
	case <-firstUpdate:
	case <-time.After(time.Second):
		t.Fatal("sender did not block on traffic update")
	}
	tracked := flow.AttachBackend(&testConn{})
	if err := tracked.Close(); err != nil {
		t.Fatal(err)
	}
	close(unblock)
	deadline := time.After(2 * time.Second)
	for {
		select {
		case frame := <-updates:
			for _, snapshot := range frame.Traffic {
				state := snapshot.Services[string(proxy.ConnectServiceForward)]
				if snapshot.SandboxID == "s1" && state.Parking == 0 && state.Connected == 0 && state.IdleSince != nil {
					cancel()
					<-done
					return
				}
			}
		case <-deadline:
			t.Fatal("sender lost final absolute traffic state")
		}
	}
}

func TestGCRouteLookupDoesNotBlockTrafficAndRechecksEntry(t *testing.T) {
	worker := NewWorkerStats()
	point := timePoint{wall: time.Now().UTC(), bootNS: 100}
	worker.now = func() timePoint { return point }
	flow := worker.BeginParking("s1", proxy.ConnectServiceForward)
	flow.Close()
	point = timePoint{wall: point.wall.Add(time.Hour), bootNS: int64(time.Hour)}

	lookupStarted := make(chan struct{})
	releaseLookup := make(chan struct{})
	gcDone := make(chan struct{})
	go func() {
		worker.gc(func(string) bool {
			close(lookupStarted)
			<-releaseLookup
			return false
		}, time.Minute)
		close(gcDone)
	}()
	select {
	case <-lookupStarted:
	case <-time.After(time.Second):
		t.Fatal("GC did not start route lookup")
	}

	parkingDone := make(chan proxy.TrafficFlow, 1)
	go func() { parkingDone <- worker.BeginParking("s1", proxy.ConnectServiceForward) }()
	var active proxy.TrafficFlow
	select {
	case active = <-parkingDone:
	case <-time.After(time.Second):
		t.Fatal("route lookup held the traffic shard lock")
	}
	close(releaseLookup)
	select {
	case <-gcDone:
	case <-time.After(time.Second):
		t.Fatal("GC did not finish")
	}
	state := snapshotWorker(t, worker, "s1").Services[string(proxy.ConnectServiceForward)]
	if state.Parking != 1 || state.Connected != 0 {
		t.Fatalf("GC removed or corrupted a concurrently reactivated entry: %+v", state)
	}
	active.Close()
}

func TestOldBatchAckPreservesRecreatedEntryDirtyState(t *testing.T) {
	worker := NewWorkerStats()
	point := timePoint{wall: time.Now().UTC(), bootNS: 100}
	worker.now = func() timePoint { return point }

	oldFlow := worker.BeginParking("s1", proxy.ConnectServiceForward)
	oldFlow.Close()
	oldBatch, ok := worker.nextBatch(1, 2)
	if !ok || len(oldBatch.frame.Traffic) != 1 {
		t.Fatalf("old pending batch = %+v, present=%v", oldBatch, ok)
	}

	point = timePoint{wall: point.wall.Add(time.Hour), bootNS: int64(time.Hour)}
	worker.gc(func(string) bool { return false }, time.Minute)
	if entry := worker.lockEntry("s1", false); entry != nil {
		entry.mu.Unlock()
		t.Fatal("GC did not remove the old idle entry")
	}

	// Recreate enough state to reuse the old entry-local revision. The old
	// implementation assigned revision 2 to both states, so acknowledging the
	// stale idle batch erased this parking update.
	first := worker.BeginParking("s1", proxy.ConnectServiceForward)
	second := worker.BeginParking("s1", proxy.ConnectServiceForward)
	worker.ack(oldBatch)

	newBatch, ok := worker.nextBatch(1, 3)
	if !ok || len(newBatch.frame.Traffic) != 1 {
		t.Fatalf("recreated entry update was cleared by old ack: %+v, present=%v", newBatch, ok)
	}
	state := newBatch.frame.Traffic[0].Services[string(proxy.ConnectServiceForward)]
	if state.Parking != 2 || state.Connected != 0 {
		t.Fatalf("recreated entry snapshot = %+v, want parking=2 egress=0", state)
	}
	first.Close()
	second.Close()
}

func BenchmarkUnlimitedAdmissionFastPath(b *testing.B) {
	b.Run("main-BeginParking", func(b *testing.B) {
		worker := NewWorkerStats()
		b.ReportAllocs()
		for range b.N {
			flow := worker.BeginParking("s1", proxy.ConnectServiceForward)
			flow.Close()
		}
	})
	b.Run("TryBeginParking-unlimited", func(b *testing.B) {
		worker := NewWorkerStats()
		b.ReportAllocs()
		for range b.N {
			flow, err := worker.TryBeginParking("s1", proxy.ConnectServiceForward, proxyadmission.Binding{})
			if err != nil {
				b.Fatal(err)
			}
			flow.Close()
		}
	})
}
