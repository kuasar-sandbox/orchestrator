package proxystats

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/metrics"
	"github.com/kuasar-sandbox/orchestrator/internal/proxy"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func readyWorker(t *testing.T, master *MasterStats, workerID string, epoch uint64) {
	t.Helper()
	if err := master.BeginWorker(workerID, epoch); err != nil {
		t.Fatal(err)
	}
	if err := master.Receive(workerID, epoch, Frame{Type: TypeHello, Version: Version, WorkerID: workerID, Epoch: epoch}); err != nil {
		t.Fatal(err)
	}
	if err := master.Receive(workerID, epoch, Frame{Type: TypeReady, Version: Version, Epoch: epoch, Sequence: 1}); err != nil {
		t.Fatal(err)
	}
}

func TestMasterAggregatesWorkersAndDuplicateAbsoluteFrames(t *testing.T) {
	mx := metrics.New()
	master := NewMasterStats(mx, []string{"w0", "w1"})
	readyWorker(t, master, "w0", 1)
	readyWorker(t, master, "w1", 1)
	update0 := Frame{
		Type: TypeUpdate, Version: Version, Epoch: 1, Sequence: 2,
		Counters: map[string]uint64{"requests_total": 3},
		Traffic: []SandboxSnapshot{{SandboxID: "s1", Services: map[string]ServiceSnapshot{
			string(proxy.ConnectServiceForward): {Parking: 1},
		}}},
	}
	update1 := Frame{
		Type: TypeUpdate, Version: Version, Epoch: 1, Sequence: 2,
		Counters: map[string]uint64{"requests_total": 4},
		Traffic: []SandboxSnapshot{{SandboxID: "s1", Services: map[string]ServiceSnapshot{
			string(proxy.ConnectServiceForward): {Egress: 2},
		}}},
	}
	if err := master.Receive("w0", 1, update0); err != nil {
		t.Fatal(err)
	}
	if err := master.Receive("w1", 1, update1); err != nil {
		t.Fatal(err)
	}
	if err := master.Receive("w1", 1, update1); err != nil { // duplicate seq is idempotent
		t.Fatal(err)
	}
	conflictingDuplicate := update1
	conflictingDuplicate.Traffic = []SandboxSnapshot{{SandboxID: "s1", Services: map[string]ServiceSnapshot{
		string(proxy.ConnectServiceForward): {Egress: 3},
	}}}
	if err := master.Receive("w1", 1, conflictingDuplicate); err == nil {
		t.Fatal("same sequence with different content was accepted")
	}
	stats, err := master.SandboxTrafficStats(context.Background(), "s1", "run-1", types.ProfileBare, types.StateRunning)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Inflight.Parking != 1 || stats.Inflight.Egress != 2 || stats.IdleSince != nil {
		t.Fatalf("aggregate stats = %+v", stats)
	}
	forward := stats.Services[string(proxy.ConnectServiceForward)]
	if forward.Parking != 1 || forward.Egress != 2 || forward.IdleSince != nil {
		t.Fatalf("forward aggregate = %+v", forward)
	}
	if exec := stats.Services[string(proxy.ConnectServiceExec)]; exec.Parking != 0 || exec.Egress != 0 || exec.IdleSince == nil {
		t.Fatalf("unused exec service = %+v", exec)
	}
	recorder := httptest.NewRecorder()
	mx.Handler().ServeHTTP(recorder, httptest.NewRequest("GET", "/metrics", nil))
	if !strings.Contains(recorder.Body.String(), "requests_total 7") {
		t.Fatalf("metrics did not aggregate absolute deltas:\n%s", recorder.Body.String())
	}

	regression := Frame{Type: TypeUpdate, Version: Version, Epoch: 1, Sequence: 3, Counters: map[string]uint64{"requests_total": 2}}
	if err := master.Receive("w0", 1, regression); err == nil {
		t.Fatal("same-epoch counter regression was accepted")
	}
	omission := Frame{Type: TypeUpdate, Version: Version, Epoch: 1, Sequence: 3, Counters: map[string]uint64{"other_total": 1}}
	if err := master.Receive("w0", 1, omission); err == nil {
		t.Fatal("partial absolute counter snapshot was accepted")
	}

	invalidMixedSnapshot := Frame{
		Type: TypeUpdate, Version: Version, Epoch: 1, Sequence: 3,
		Counters: map[string]uint64{
			"requests_total": 4,
			"overflow_total": uint64(^uint64(0)>>1) + 1,
		},
	}
	if err := master.Receive("w0", 1, invalidMixedSnapshot); err == nil {
		t.Fatal("mixed valid and overflowing counter snapshot was accepted")
	}
	master.mu.Lock()
	requests := master.workers["w0"].counters["requests_total"]
	_, retainedOverflow := master.workers["w0"].counters["overflow_total"]
	master.mu.Unlock()
	if requests != 3 || retainedOverflow {
		t.Fatalf("rejected counter snapshot partially changed contribution: requests=%d overflow=%v", requests, retainedOverflow)
	}
	recorder = httptest.NewRecorder()
	mx.Handler().ServeHTTP(recorder, httptest.NewRequest("GET", "/metrics", nil))
	if !strings.Contains(recorder.Body.String(), "requests_total 7") || strings.Contains(recorder.Body.String(), "overflow_total") {
		t.Fatalf("rejected counter snapshot partially changed metrics:\n%s", recorder.Body.String())
	}
}

func TestMasterFaultWindowExitAndReplacementReadiness(t *testing.T) {
	master := NewMasterStats(metrics.New(), []string{"w0"})
	readyWorker(t, master, "w0", 1)
	if err := master.Receive("w0", 1, Frame{
		Type: TypeUpdate, Version: Version, Epoch: 1, Sequence: 2,
		Traffic: []SandboxSnapshot{{SandboxID: "s1", Services: map[string]ServiceSnapshot{
			string(proxy.ConnectServiceForward): {Egress: 1},
		}}},
	}); err != nil {
		t.Fatal(err)
	}
	if !master.Available() {
		t.Fatal("ready worker set is unavailable")
	}
	master.StreamFault("w0", 1)
	if master.Available() {
		t.Fatal("fault window remained available")
	}
	master.WorkerExited("w0", 1)
	if master.Available() {
		t.Fatal("worker exit became available before replacement")
	}
	if err := master.BeginWorker("w0", 2); err != nil {
		t.Fatal(err)
	}
	if err := master.Receive("w0", 2, Frame{Type: TypeHello, Version: Version, WorkerID: "w0", Epoch: 2}); err != nil {
		t.Fatal(err)
	}
	if master.Available() {
		t.Fatal("hello without ready became available")
	}
	if err := master.Receive("w0", 2, Frame{Type: TypeReady, Version: Version, Epoch: 2, Sequence: 1}); err != nil {
		t.Fatal(err)
	}
	if !master.Available() {
		t.Fatal("replacement ready did not restore availability")
	}
	stats, err := master.SandboxTrafficStats(context.Background(), "s1", "run-2", types.ProfileE2B, types.StatePaused)
	if err != nil {
		t.Fatal(err)
	}
	if stats.IdleSince != nil {
		t.Fatal("paused sandbox exposed top-level idleSince")
	}
	for service, state := range stats.Services {
		if state.IdleSince == nil || state.IdleSince.After(time.Now().Add(time.Second)) {
			t.Fatalf("service %s has invalid conservative idle time: %+v", service, state)
		}
	}
	if stats.Inflight.Parking != 0 || stats.Inflight.Egress != 0 {
		t.Fatalf("exited worker contribution survived replacement: %+v", stats.Inflight)
	}
}

func TestMasterCannotReenterReadyWithinAnEpoch(t *testing.T) {
	master := NewMasterStats(metrics.New(), []string{"w0"})
	readyWorker(t, master, "w0", 1)
	readyAgain := Frame{Type: TypeReady, Version: Version, Epoch: 1, Sequence: 2}
	if err := master.Receive("w0", 1, readyAgain); err == nil {
		t.Fatal("second ready with a new sequence was accepted")
	}
	master.StreamFault("w0", 1)
	if err := master.Receive("w0", 1, readyAgain); err == nil {
		t.Fatal("faulted worker recovered without a new epoch")
	}
}

func TestMasterAdvancesIdleBaselineOnRunAndWorkerEpochChanges(t *testing.T) {
	master := NewMasterStats(metrics.New(), []string{"w0"})
	base := time.Date(2026, time.August, 12, 12, 0, 0, 0, time.UTC)
	point := timePoint{wall: base, bootNS: 100}
	master.now = func() timePoint { return point }
	master.trustSince = point
	readyWorker(t, master, "w0", 1)

	first, err := master.SandboxTrafficStats(context.Background(), "s1", "run-1", types.ProfileBare, types.StateRunning)
	if err != nil || first.IdleSince == nil || first.IdleSince.Before(base) {
		t.Fatalf("first idle baseline = %+v err=%v", first, err)
	}
	point = timePoint{wall: base.Add(time.Minute), bootNS: 200}
	second, err := master.SandboxTrafficStats(context.Background(), "s1", "run-2", types.ProfileBare, types.StateRunning)
	if err != nil || second.IdleSince == nil || second.IdleSince.Before(point.wall) {
		t.Fatalf("RunID baseline = %+v err=%v", second, err)
	}

	master.StreamFault("w0", 1)
	point = timePoint{wall: base.Add(2 * time.Minute), bootNS: 300}
	master.WorkerExited("w0", 1)
	if err := master.BeginWorker("w0", 2); err != nil {
		t.Fatal(err)
	}
	if err := master.Receive("w0", 2, Frame{Type: TypeHello, Version: Version, WorkerID: "w0", Epoch: 2}); err != nil {
		t.Fatal(err)
	}
	if err := master.Receive("w0", 2, Frame{Type: TypeReady, Version: Version, Epoch: 2, Sequence: 1}); err != nil {
		t.Fatal(err)
	}
	third, err := master.SandboxTrafficStats(context.Background(), "s1", "run-2", types.ProfileBare, types.StateRunning)
	if err != nil || third.IdleSince == nil || third.IdleSince.Before(point.wall) {
		t.Fatalf("worker epoch baseline = %+v err=%v", third, err)
	}
}

func TestMasterRunBaselineStartsOnlyWhenObservedRunning(t *testing.T) {
	master := NewMasterStats(metrics.New(), []string{"w0"})
	base := time.Date(2026, time.August, 12, 12, 0, 0, 0, time.UTC)
	point := timePoint{wall: base, bootNS: 100}
	master.now = func() timePoint { return point }
	master.trustSince = point
	readyWorker(t, master, "w0", 1)

	point = timePoint{wall: base.Add(time.Minute), bootNS: 200}
	starting, err := master.SandboxTrafficStats(context.Background(), "s1", "run-1", types.ProfileBare, types.StateStarting)
	if err != nil || starting.IdleSince != nil {
		t.Fatalf("starting stats = %+v err=%v", starting, err)
	}

	point = timePoint{wall: base.Add(2 * time.Minute), bootNS: 300}
	running, err := master.SandboxTrafficStats(context.Background(), "s1", "run-1", types.ProfileBare, types.StateRunning)
	if err != nil || running.IdleSince == nil || running.IdleSince.Before(point.wall) {
		t.Fatalf("running baseline = %+v err=%v, want >= %s", running, err, point.wall)
	}
}

func TestMasterPrunesQueryOnlyRunMarkersWithoutHoldingLockAcrossLookup(t *testing.T) {
	master := NewMasterStats(metrics.New(), []string{"w0"})
	readyWorker(t, master, "w0", 1)
	if _, err := master.SandboxTrafficStats(context.Background(), "deleted", "run-1", types.ProfileBare, types.StateRunning); err != nil {
		t.Fatal(err)
	}
	lookupEntered := make(chan struct{})
	releaseLookup := make(chan struct{})
	done := make(chan struct{})
	go func() {
		master.pruneRunMarkers(func(string) bool {
			close(lookupEntered)
			<-releaseLookup
			return false
		})
		close(done)
	}()
	<-lookupEntered
	if !master.Available() {
		t.Fatal("route lookup held the master aggregate lock")
	}
	close(releaseLookup)
	<-done
	master.mu.Lock()
	_, retained := master.runSince["deleted"]
	master.mu.Unlock()
	if retained {
		t.Fatal("query-only deleted run marker was retained")
	}
}
