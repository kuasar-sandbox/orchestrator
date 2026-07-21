package nodectl

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func preparedDigest(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}

func preparedTestController(t *testing.T, state *State, path string, queueMax int) *PreparedAdmissionController {
	t.Helper()
	policy := AdmissionPolicy{
		Rate: 100, Burst: 100, StartupTTL: time.Minute, QueueTTL: time.Minute, QueueMaxDepth: queueMax,
	}
	admission := NewAdmissionController(policy)
	admission.state = state
	controller, err := NewPreparedAdmissionController(state, admission, &Persister{Path: path}, policy)
	if err != nil {
		t.Fatal(err)
	}
	return controller
}

func preparedTestState() *State {
	return NewState(16<<30, 8000, 0, 0, Resources{}, Watermarks{
		OperationalMarginFactor: 0,
		HighFactor:              0.95,
		LowFactor:               0.80,
		EmergencyFactor:         0.05,
		StartupFactor:           1,
	})
}

func preparedTestDemand(memory uint64) SandboxAdmissionDemand {
	return SandboxAdmissionDemand{
		CapacityMemoryBytes: memory, CapacityCPU: 2,
		FloorMemoryBytes: memory, FloorCPU: 1, StartupBudgetMemory: memory,
		CgroupPath: "/sys/fs/cgroup/sandbox",
	}
}

func TestPreparedAdmissionIsDurableIdempotentAndClaimable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	state := preparedTestState()
	controller := preparedTestController(t, state, path, 4)
	digest := preparedDigest("demand-1")

	first, err := controller.PrepareAdmission("sandbox-1", digest, preparedTestDemand(1<<30))
	if err != nil || first.State != PreparedAdmitted || first.ReservationToken == "" {
		t.Fatalf("first prepare = %+v, %v", first, err)
	}
	retry, err := controller.PrepareAdmission("sandbox-1", digest, preparedTestDemand(1<<30))
	if err != nil || retry != first {
		t.Fatalf("retry = %+v, %v; want %+v", retry, err, first)
	}
	if _, err := controller.PrepareAdmission("sandbox-1", preparedDigest("different"), preparedTestDemand(1<<30)); !errors.Is(err, ErrPreparedAdmissionConflict) {
		t.Fatalf("conflicting prepare error = %v", err)
	}
	claimed, err := controller.ClaimAdmission("sandbox-1", digest)
	if err != nil || claimed.State != PreparedClaimed || claimed.ReservationToken != first.ReservationToken {
		t.Fatalf("claim = %+v, %v", claimed, err)
	}

	loaded, err := (&Persister{Path: path}).Load()
	if err != nil {
		t.Fatal(err)
	}
	record := loaded.PreparedSandboxAdmissions["sandbox-1"]
	if record == nil || record.State != PreparedClaimed || record.ReservationToken != first.ReservationToken ||
		loaded.Reservations[first.ReservationToken] == nil {
		t.Fatalf("reloaded admission = %+v reservations=%+v", record, loaded.Reservations)
	}
}

func TestPreparedAdmissionOrdinaryAdmitAttachesClaimedReservation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	state := preparedTestState()
	controller := preparedTestController(t, state, path, 4)
	demand := preparedTestDemand(1 << 30)
	// The runner cgroup does not exist until systemd starts sandbox-ctl. The
	// authenticated ordinary Admit fixes the kernel-observed path exactly once.
	demand.CgroupPath = ""
	digest := preparedDigest("transparent-attach")
	prepared, err := controller.PrepareAdmission("sandbox-attach", digest, demand)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := controller.ClaimAdmission("sandbox-attach", digest); err != nil {
		t.Fatal(err)
	}

	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()
	const cgroup = "/sys/fs/cgroup/sandbox-runner.slice/runner.service"
	server := &Server{
		State: state, Admission: controller.admission, PreparedAdmission: controller,
		Persister: controller.persister, Logf: t.Logf,
		peerCgroup: func(net.Conn) (string, error) { return cgroup, nil },
	}
	request := demand.message("sandbox-attach")
	request.CgroupPath = cgroup
	var token string
	response, handled := server.handlePreparedAdmit(serverConn, request, &token)
	if !handled || response.Status != StatusAdmitted || token != prepared.ReservationToken ||
		response.Token != prepared.ReservationToken || response.GrantedInitialAlloc != 1<<30 {
		t.Fatalf("attach response=%+v handled=%v token=%q", response, handled, token)
	}

	state.Lock()
	record := state.PreparedSandboxAdmissions["sandbox-attach"]
	reservation := state.Reservations[prepared.ReservationToken]
	if len(state.Reservations) != 1 || record.Demand.CgroupPath != cgroup ||
		reservation.CgroupPath != cgroup || reservation.Conn != serverConn {
		state.Unlock()
		t.Fatalf("attached record=%+v reservation=%+v", record, reservation)
	}
	state.Unlock()
	loaded, err := controller.persister.Load()
	if err != nil {
		t.Fatal(err)
	}
	if loaded.PreparedSandboxAdmissions["sandbox-attach"].Demand.CgroupPath != cgroup ||
		loaded.Reservations[prepared.ReservationToken].CgroupPath != cgroup {
		t.Fatalf("persisted attach=%+v", loaded.PreparedSandboxAdmissions["sandbox-attach"])
	}
}

func TestPreparedAdmissionOrdinaryAdmitFailsClosed(t *testing.T) {
	tests := []struct {
		name      string
		claim     bool
		mutate    func(*Message)
		peerGroup string
		reason    string
	}{
		{name: "not claimed", reason: "prepared_admission_not_claimed"},
		{name: "resource mismatch", claim: true, mutate: func(request *Message) { request.FloorMemoryBytes-- }, reason: "prepared_admission_mismatch"},
		{name: "cgroup not owned by peer", claim: true, peerGroup: "/sys/fs/cgroup/another.service", reason: "prepared_cgroup_unverified"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "state.json")
			state := preparedTestState()
			controller := preparedTestController(t, state, path, 4)
			demand := preparedTestDemand(1 << 30)
			demand.CgroupPath = ""
			digest := preparedDigest(test.name)
			prepared, err := controller.PrepareAdmission("sandbox-fenced", digest, demand)
			if err != nil {
				t.Fatal(err)
			}
			if test.claim {
				if _, err := controller.ClaimAdmission("sandbox-fenced", digest); err != nil {
					t.Fatal(err)
				}
			}
			serverConn, clientConn := net.Pipe()
			defer serverConn.Close()
			defer clientConn.Close()
			const requestedCgroup = "/sys/fs/cgroup/sandbox-runner.slice/runner.service"
			peerGroup := test.peerGroup
			if peerGroup == "" {
				peerGroup = requestedCgroup
			}
			server := &Server{
				State: state, Admission: controller.admission, PreparedAdmission: controller,
				Persister: controller.persister, Logf: t.Logf,
				peerCgroup: func(net.Conn) (string, error) { return peerGroup, nil },
			}
			request := demand.message("sandbox-fenced")
			request.CgroupPath = requestedCgroup
			if test.mutate != nil {
				test.mutate(request)
			}
			var token string
			response, handled := server.handlePreparedAdmit(serverConn, request, &token)
			if !handled || response.Status != StatusRejected || response.Reason != test.reason || token != "" {
				t.Fatalf("response=%+v handled=%v token=%q", response, handled, token)
			}
			state.Lock()
			reservation := state.Reservations[prepared.ReservationToken]
			if len(state.Reservations) != 1 || reservation == nil || reservation.Conn != nil {
				state.Unlock()
				t.Fatalf("rejected attach changed reservations: %+v", state.Reservations)
			}
			state.Unlock()
		})
	}
}

func TestPreparedAdmissionOrdinaryAdmitFallsThroughOnlyForUnknownSID(t *testing.T) {
	state := preparedTestState()
	controller := preparedTestController(t, state, filepath.Join(t.TempDir(), "state.json"), 4)
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()
	server := &Server{State: state, Admission: controller.admission, PreparedAdmission: controller, Persister: controller.persister, Logf: t.Logf}
	response, handled := server.handlePreparedAdmit(serverConn, preparedTestDemand(1<<30).message("standalone"), new(string))
	if handled || response != nil {
		t.Fatalf("unknown SID was captured by cluster Admission: response=%+v handled=%v", response, handled)
	}
}

func TestPreparedAdmissionQueueSurvivesRestartAndPromotesFIFO(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	state := preparedTestState()
	state.Lock()
	state.Reservations["blocker"] = &Reservation{
		Token: "blocker", SandboxID: "running", AllocatableNowMem: 15 << 30,
		Floor: Resources{MemoryBytes: 15 << 30}, Stage: StageSettled,
	}
	state.Unlock()
	controller := preparedTestController(t, state, path, 4)
	firstDigest := preparedDigest("queued-1")
	secondDigest := preparedDigest("queued-2")
	first, err := controller.PrepareAdmission("sandbox-1", firstDigest, preparedTestDemand(512<<20))
	if err != nil || first.State != PreparedQueued {
		t.Fatalf("first queued = %+v, %v", first, err)
	}
	second, err := controller.PrepareAdmission("sandbox-2", secondDigest, preparedTestDemand(512<<20))
	if err != nil || second.State != PreparedQueued {
		t.Fatalf("second queued = %+v, %v", second, err)
	}

	loaded, err := (&Persister{Path: path}).Load()
	if err != nil {
		t.Fatal(err)
	}
	loaded.Lock()
	loaded.Remove("blocker")
	if err := (&Persister{Path: path}).Flush(loaded); err != nil {
		t.Fatal(err)
	}
	loaded.Unlock()
	restarted := preparedTestController(t, loaded, path, 4)
	changed, err := restarted.PromoteQueued()
	if err != nil {
		t.Fatal(err)
	}
	if len(changed) != 2 || changed[0].SandboxID != "sandbox-1" || changed[1].SandboxID != "sandbox-2" ||
		changed[0].State != PreparedAdmitted || changed[1].State != PreparedAdmitted {
		t.Fatalf("promoted = %+v", changed)
	}
	if changed[0].ReservationToken != first.ReservationToken || changed[1].ReservationToken != second.ReservationToken {
		t.Fatalf("promotion changed durable tokens: before=(%s,%s) after=%+v",
			first.ReservationToken, second.ReservationToken, changed)
	}
}

func TestPreparedAdmissionNewWorkCannotBypassQueuedHead(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	state := preparedTestState()
	state.Lock()
	state.Reservations["blocker"] = &Reservation{
		Token: "blocker", SandboxID: "running", AllocatableNowMem: 15 << 30,
		Floor: Resources{MemoryBytes: 15 << 30}, Stage: StageSettled,
	}
	state.Unlock()
	controller := preparedTestController(t, state, path, 4)
	firstDigest := preparedDigest("fifo-head")
	secondDigest := preparedDigest("fifo-tail")
	first, err := controller.PrepareAdmission("sandbox-1", firstDigest, preparedTestDemand(512<<20))
	if err != nil || first.State != PreparedQueued {
		t.Fatalf("first prepare = %+v, %v", first, err)
	}
	state.Lock()
	state.Remove("blocker")
	state.Unlock()
	second, err := controller.PrepareAdmission("sandbox-2", secondDigest, preparedTestDemand(512<<20))
	if err != nil || second.State != PreparedQueued {
		t.Fatalf("new work bypassed queued head: first=%+v second=%+v err=%v", first, second, err)
	}
	changed, err := controller.PromoteQueued()
	if err != nil || len(changed) != 2 || changed[0].SandboxID != "sandbox-1" ||
		changed[1].SandboxID != "sandbox-2" {
		t.Fatalf("FIFO promotion = %+v, %v", changed, err)
	}
}

func TestPreparedAdmissionTokenBlockWakesAfterRefill(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	state := preparedTestState()
	policy := AdmissionPolicy{
		Rate: 20, Burst: 1, StartupTTL: time.Minute, QueueTTL: time.Minute, QueueMaxDepth: 4,
	}
	admission := NewAdmissionController(policy)
	admission.state = state
	controller, err := NewPreparedAdmissionController(state, admission, &Persister{Path: path}, policy)
	if err != nil {
		t.Fatal(err)
	}
	if first, err := controller.PrepareAdmission("sandbox-1", preparedDigest("token-1"), preparedTestDemand(1<<30)); err != nil || first.State != PreparedAdmitted {
		t.Fatalf("first admission = %+v, %v", first, err)
	}
	digest := preparedDigest("token-2")
	if queued, err := controller.PrepareAdmission("sandbox-2", digest, preparedTestDemand(1<<30)); err != nil || queued.State != PreparedQueued {
		t.Fatalf("token-blocked admission = %+v, %v", queued, err)
	}
	for {
		select {
		case <-controller.Wake():
		default:
			goto drained
		}
	}

drained:
	deadline := time.After(2 * time.Second)
	for {
		select {
		case <-controller.Wake():
			changed, err := controller.PromoteQueued()
			if err != nil {
				t.Fatal(err)
			}
			if len(changed) == 0 {
				continue
			}
			if len(changed) != 1 || changed[0].SandboxID != "sandbox-2" || changed[0].State != PreparedAdmitted {
				t.Fatalf("refill promotion = %+v", changed)
			}
			return
		case <-deadline:
			t.Fatal("token-blocked prepared admission was not woken after refill")
		}
	}
}

func TestPreparedAdmissionCapacityBlockWakesAtQueueTTL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	state := preparedTestState()
	state.Lock()
	state.Reservations["blocker"] = &Reservation{
		Token: "blocker", AllocatableNowMem: 15 << 30,
		Floor: Resources{MemoryBytes: 15 << 30}, Stage: StageSettled,
	}
	state.Unlock()
	policy := AdmissionPolicy{
		Rate: 100, Burst: 100, StartupTTL: time.Minute, QueueTTL: 40 * time.Millisecond, QueueMaxDepth: 4,
	}
	admission := NewAdmissionController(policy)
	admission.state = state
	controller, err := NewPreparedAdmissionController(state, admission, &Persister{Path: path}, policy)
	if err != nil {
		t.Fatal(err)
	}
	digest := preparedDigest("ttl-wake")
	if queued, err := controller.PrepareAdmission("sandbox-1", digest, preparedTestDemand(1<<30)); err != nil || queued.State != PreparedQueued {
		t.Fatalf("queued admission = %+v, %v", queued, err)
	}
	for {
		select {
		case <-controller.Wake():
		default:
			goto drained
		}
	}

drained:
	select {
	case <-controller.Wake():
		changed, err := controller.PromoteQueued()
		if err != nil || len(changed) != 1 || changed[0].State != PreparedRejected || changed[0].Reason != "queue_expired" {
			t.Fatalf("TTL promotion = %+v, %v", changed, err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("capacity-blocked prepared admission was not woken at queue TTL")
	}
}

func TestPreparedAdmissionPersistsQueueFullRejection(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	state := preparedTestState()
	state.Lock()
	state.Reservations["blocker"] = &Reservation{
		Token: "blocker", AllocatableNowMem: 15 << 30,
		Floor: Resources{MemoryBytes: 15 << 30}, Stage: StageSettled,
	}
	state.Unlock()
	controller := preparedTestController(t, state, path, 1)
	if first, err := controller.PrepareAdmission("sandbox-1", preparedDigest("one"), preparedTestDemand(1<<30)); err != nil || first.State != PreparedQueued {
		t.Fatalf("first = %+v, %v", first, err)
	}
	digest := preparedDigest("two")
	rejected, err := controller.PrepareAdmission("sandbox-2", digest, preparedTestDemand(1<<30))
	if err != nil || rejected.State != PreparedRejected || rejected.Reason != "queue_full" || rejected.ReservationToken != "" {
		t.Fatalf("rejected = %+v, %v", rejected, err)
	}
	retry, err := controller.PrepareAdmission("sandbox-2", digest, preparedTestDemand(1<<30))
	if err != nil || retry != rejected {
		t.Fatalf("rejected retry = %+v, %v", retry, err)
	}
}

func TestPreparedAdmissionRollsBackOnPersistenceFailure(t *testing.T) {
	state := preparedTestState()
	controller := preparedTestController(t, state, "/dev/null/state.json", 4)
	if _, err := controller.PrepareAdmission("sandbox-1", preparedDigest("demand"), preparedTestDemand(1<<30)); err == nil {
		t.Fatal("prepare succeeded without durable state")
	}
	state.Lock()
	defer state.Unlock()
	if len(state.PreparedSandboxAdmissions) != 0 || len(state.Reservations) != 0 {
		t.Fatalf("failed persistence left state: admissions=%+v reservations=%+v",
			state.PreparedSandboxAdmissions, state.Reservations)
	}
}

func TestPreparedAdmissionQueueExpiryIsDurableAndDefinitive(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	state := preparedTestState()
	state.Lock()
	state.Reservations["blocker"] = &Reservation{
		Token: "blocker", AllocatableNowMem: 15 << 30,
		Floor: Resources{MemoryBytes: 15 << 30}, Stage: StageSettled,
	}
	state.Unlock()
	controller := preparedTestController(t, state, path, 4)
	now := time.Unix(1000, 0)
	controller.clock = func() time.Time { return now }
	controller.queueTTL = time.Minute
	digest := preparedDigest("expiring")
	queued, err := controller.PrepareAdmission("sandbox-1", digest, preparedTestDemand(1<<30))
	if err != nil || queued.State != PreparedQueued {
		t.Fatalf("queued = %+v, %v", queued, err)
	}
	now = now.Add(time.Minute)
	changed, err := controller.PromoteQueued()
	if err != nil || len(changed) != 1 || changed[0].State != PreparedRejected || changed[0].Reason != "queue_expired" {
		t.Fatalf("expired = %+v, %v", changed, err)
	}
	retry, err := controller.PrepareAdmission("sandbox-1", digest, preparedTestDemand(1<<30))
	if err != nil || retry.State != PreparedRejected || retry.Reason != "queue_expired" || retry.ReservationToken != "" {
		t.Fatalf("expired retry = %+v, %v", retry, err)
	}
	loaded, err := (&Persister{Path: path}).Load()
	if err != nil {
		t.Fatal(err)
	}
	if record := loaded.PreparedSandboxAdmissions["sandbox-1"]; record == nil ||
		record.State != PreparedRejected || record.Reason != "queue_expired" {
		t.Fatalf("durable expiry = %+v", record)
	}
}

func TestPreparedAdmissionExpiryWinsWhenCapacityBecomesAvailable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	state := preparedTestState()
	state.Lock()
	state.Reservations["blocker"] = &Reservation{
		Token: "blocker", AllocatableNowMem: 15 << 30,
		Floor: Resources{MemoryBytes: 15 << 30}, Stage: StageSettled,
	}
	state.Unlock()
	controller := preparedTestController(t, state, path, 4)
	now := time.Unix(1000, 0)
	controller.clock = func() time.Time { return now }
	controller.queueTTL = time.Minute
	digest := preparedDigest("expired-with-capacity")
	queued, err := controller.PrepareAdmission("sandbox-1", digest, preparedTestDemand(1<<30))
	if err != nil || queued.State != PreparedQueued {
		t.Fatalf("queued = %+v, %v", queued, err)
	}
	state.Lock()
	state.Remove("blocker")
	state.Unlock()
	now = now.Add(time.Minute)

	changed, err := controller.PromoteQueued()
	if err != nil || len(changed) != 1 || changed[0].State != PreparedRejected || changed[0].Reason != "queue_expired" {
		t.Fatalf("expired with available capacity = %+v, %v", changed, err)
	}
	state.Lock()
	defer state.Unlock()
	if state.Lookup(queued.ReservationToken) != nil {
		t.Fatal("expired queue entry consumed capacity")
	}
}

func TestPreparedAdmissionClaimFailsClosedAfterReservationLoss(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	state := preparedTestState()
	controller := preparedTestController(t, state, path, 4)
	digest := preparedDigest("missing-reservation")
	prepared, err := controller.PrepareAdmission("sandbox-1", digest, preparedTestDemand(1<<30))
	if err != nil || prepared.State != PreparedAdmitted {
		t.Fatalf("prepared = %+v, %v", prepared, err)
	}
	state.Lock()
	state.Remove(prepared.ReservationToken)
	state.Unlock()

	claimed, err := controller.ClaimAdmission("sandbox-1", digest)
	if err != nil || claimed.State != PreparedReleased || claimed.Reason != "reservation_missing" {
		t.Fatalf("claim after reservation loss = %+v, %v", claimed, err)
	}
	loaded, err := (&Persister{Path: path}).Load()
	if err != nil {
		t.Fatal(err)
	}
	if record := loaded.PreparedSandboxAdmissions["sandbox-1"]; record == nil ||
		record.State != PreparedReleased || record.Reason != "reservation_missing" {
		t.Fatalf("durable missing-reservation fence = %+v", record)
	}
}

func TestIdleSweeperReleasesPreparedAdmissionWithReservation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	state := preparedTestState()
	controller := preparedTestController(t, state, path, 4)
	digest := preparedDigest("swept-reservation")
	prepared, err := controller.PrepareAdmission("sandbox-1", digest, preparedTestDemand(1<<30))
	if err != nil || prepared.State != PreparedAdmitted {
		t.Fatalf("prepared = %+v, %v", prepared, err)
	}
	state.Lock()
	state.Reservations[prepared.ReservationToken].StageEnteredAt = time.Now().Add(-time.Minute)
	state.Unlock()
	sweeper := &IdleSweeper{
		State: state, Admission: controller.admission,
		Allocator: NewAllocator(AllocatorPolicy{}), Persister: &Persister{Path: path},
		StartupTTL: time.Second, Logf: t.Logf,
	}
	sweeper.sweep()

	result, err := controller.GetAdmission("sandbox-1", digest)
	if err != nil || result.State != PreparedReleased || result.Reason != "reservation_expired" {
		t.Fatalf("swept admission = %+v, %v", result, err)
	}
	state.Lock()
	defer state.Unlock()
	if state.Lookup(prepared.ReservationToken) != nil {
		t.Fatal("swept reservation remains allocated")
	}
}

func TestIdleSweeperWakesPreparedQueueAfterDurableRelease(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	state := preparedTestState()
	controller := preparedTestController(t, state, path, 4)
	first, err := controller.PrepareAdmission(
		"sandbox-1", preparedDigest("sweep-owner"), preparedTestDemand(15<<30),
	)
	if err != nil || first.State != PreparedAdmitted {
		t.Fatalf("first prepare = %+v, %v", first, err)
	}
	secondDigest := preparedDigest("sweep-waiter")
	second, err := controller.PrepareAdmission("sandbox-2", secondDigest, preparedTestDemand(512<<20))
	if err != nil || second.State != PreparedQueued {
		t.Fatalf("second prepare = %+v, %v", second, err)
	}
	for {
		select {
		case <-controller.Wake():
			continue
		default:
		}
		break
	}
	state.Lock()
	state.Reservations[first.ReservationToken].StageEnteredAt = time.Now().Add(-time.Minute)
	state.Unlock()
	sweeper := &IdleSweeper{
		State: state, Admission: controller.admission, PreparedAdmission: controller,
		Allocator: NewAllocator(AllocatorPolicy{}), Persister: &Persister{Path: path},
		StartupTTL: time.Second, Logf: t.Logf,
	}
	sweeper.sweep()
	select {
	case <-controller.Wake():
	case <-time.After(time.Second):
		t.Fatal("prepared queue was not woken after swept capacity release")
	}
	changed, err := controller.PromoteQueued()
	if err != nil || len(changed) != 1 || changed[0].SandboxID != "sandbox-2" ||
		changed[0].State != PreparedAdmitted {
		t.Fatalf("promotion after sweep = %+v, %v", changed, err)
	}
}

func TestPreparedAdmissionKeepsPublishedStateOnDirectorySyncFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	state := preparedTestState()
	controller := preparedTestController(t, state, path, 4)
	controller.persister.syncParent = func(*os.File) error { return errors.New("injected directory sync failure") }
	digest := preparedDigest("published-prepare")
	result, err := controller.PrepareAdmission("sandbox-1", digest, preparedTestDemand(1<<30))
	if !FlushPublished(err) || result.State != PreparedAdmitted {
		t.Fatalf("published prepare = %+v, %v", result, err)
	}
	current, getErr := controller.GetAdmission("sandbox-1", digest)
	if getErr != nil || current != result {
		t.Fatalf("in-memory published state = %+v, %v", current, getErr)
	}
	loaded, loadErr := (&Persister{Path: path}).Load()
	if loadErr != nil || loaded.PreparedSandboxAdmissions["sandbox-1"] == nil ||
		loaded.Reservations[result.ReservationToken] == nil {
		t.Fatalf("visible published state = %+v, %v", loaded, loadErr)
	}
}

func TestIdleSweeperDoesNotReleaseCapacityWithoutDurableState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	state := preparedTestState()
	controller := preparedTestController(t, state, path, 4)
	digest := preparedDigest("failed-sweep")
	prepared, err := controller.PrepareAdmission("sandbox-1", digest, preparedTestDemand(1<<30))
	if err != nil || prepared.State != PreparedAdmitted {
		t.Fatalf("prepared = %+v, %v", prepared, err)
	}
	state.Lock()
	state.Reservations[prepared.ReservationToken].StageEnteredAt = time.Now().Add(-time.Minute)
	state.Unlock()
	sweeper := &IdleSweeper{
		State: state, Admission: controller.admission,
		Allocator: NewAllocator(AllocatorPolicy{}), Persister: &Persister{Path: "/dev/null/state.json"},
		StartupTTL: time.Second, Logf: t.Logf,
	}
	sweeper.sweep()

	result, err := controller.GetAdmission("sandbox-1", digest)
	if err != nil || result.State != PreparedAdmitted {
		t.Fatalf("failed sweep changed admission = %+v, %v", result, err)
	}
	state.Lock()
	defer state.Unlock()
	if state.Lookup(prepared.ReservationToken) == nil {
		t.Fatal("failed sweep released capacity before durable state")
	}
}

func TestPreparedAdmissionFinalizationRequiresReleasedResources(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	state := preparedTestState()
	controller := preparedTestController(t, state, path, 4)
	digest := preparedDigest("finalize")
	prepared, err := controller.PrepareAdmission("sandbox-1", digest, preparedTestDemand(1<<30))
	if err != nil || prepared.State != PreparedAdmitted {
		t.Fatalf("prepared = %+v, %v", prepared, err)
	}
	if err := controller.FinalizeAdmission("sandbox-1", digest); !errors.Is(err, ErrPreparedAdmissionState) {
		t.Fatalf("active finalization error = %v", err)
	}
	if _, err := controller.ReleaseAdmission("sandbox-1", digest, "terminal"); err != nil {
		t.Fatal(err)
	}
	if err := controller.FinalizeAdmission("sandbox-1", digest); err != nil {
		t.Fatal(err)
	}
	if _, err := controller.GetAdmission("sandbox-1", digest); !errors.Is(err, ErrPreparedAdmissionMissing) {
		t.Fatalf("finalized lookup error = %v", err)
	}
	if err := controller.FinalizeAdmission("sandbox-1", digest); err != nil {
		t.Fatalf("idempotent finalization = %v", err)
	}
	loaded, err := (&Persister{Path: path}).Load()
	if err != nil {
		t.Fatal(err)
	}
	if loaded.PreparedSandboxAdmissions["sandbox-1"] != nil || loaded.Reservations[prepared.ReservationToken] != nil {
		t.Fatalf("finalized state survived: admissions=%+v reservations=%+v",
			loaded.PreparedSandboxAdmissions, loaded.Reservations)
	}
}
