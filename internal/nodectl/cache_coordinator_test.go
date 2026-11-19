package nodectl

import (
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/kuasar-sandbox/sandboxer/pkg/resource"
)

type reclaimTestRun struct {
	token string
	run   string
	spec  AdmitSpec
}

func newReclaimTestCoordinator(s *State) (*CacheCoordinator, *time.Time) {
	now := time.Now()
	c := NewCacheCoordinator(s)
	c.clock = func() time.Time { return now }
	return c, &now
}

func admitReclaimTest(t *testing.T, c *CacheCoordinator, pool, sid string, cache uint64) reclaimTestRun {
	t.Helper()
	if pool != "" {
		if err := c.state.BindResourcePool(sid, pool); err != nil {
			t.Fatal(err)
		}
	}
	client, server := net.Pipe()
	t.Cleanup(func() { client.Close(); server.Close() })
	runID := fmt.Sprintf("%032x", len(c.members)+1)
	spec := AdmitSpec{SandboxID: sid, Token: "token-" + sid, PeerPID: 100 + len(c.members), CgroupPath: "/cg/" + sid, Conn: server,
		Capacity: Resources{MemoryBytes: 512 << 20, CPUMilli: 1000}, ConfiguredAllocatable: Resources{MemoryBytes: 128 << 20, CPUMilli: 1000}, InitialBudget: 128 << 20,
		CacheMax: 128 << 20, InitialCache: cache, ClientFeatures: []string{resource.FeatureCacheBudgetV1, resource.CacheMaximumFeature(128 << 20), resource.FeatureCacheReclaimV1, resource.CacheReclaimRunFeature(runID)}}
	if _, _, err := c.Admit(spec, &resource.CacheReclaimState{RunID: runID, Current: cache}); err != nil {
		t.Fatal(err)
	}
	return reclaimTestRun{spec.Token, runID, spec}
}

func reclaimIntent(id, obs, bytes uint64) *resource.CacheReclaimIntent {
	return &resource.CacheReclaimIntent{ID: id, Observation: obs, Bytes: bytes}
}

func updateReclaimTest(t *testing.T, c *CacheCoordinator, r reclaimTestRun, state resource.CacheReclaimState) resource.CacheReclaimResult {
	t.Helper()
	state.RunID = r.run
	if state.Done != nil {
		state.Current = state.Done.Current
	} else {
		state.Current = reservationByTokenForTest(t, c.state.forSID(r.spec.SandboxID), r.token).CacheReservation
	}
	out, err := c.Update(r.token, &state)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func completeReclaimTest(t *testing.T, c *CacheCoordinator, r reclaimTestRun, task *resource.CacheReclaimTask, current uint64, next *resource.CacheReclaimIntent) resource.CacheReclaimResult {
	t.Helper()
	return updateReclaimTest(t, c, r, resource.CacheReclaimState{LastSeq: task.Seq, Intent: next, Done: &resource.CacheReclaimDone{Task: *task, Current: current, Intent: next}})
}

func TestCacheCoordinatorOptionalPoolLowWatermark(t *testing.T) {
	for _, finite := range []bool{false, true} {
		for _, low := range []bool{false, true} {
			t.Run(fmt.Sprintf("capacity=%v/low=%v", finite, low), func(t *testing.T) {
				s := makeState(1<<30, 0)
				if finite {
					s.CachePoolCapacity = 64 << 20
				}
				if low {
					l := uint64(64 << 20)
					s.CacheLowWatermark = &l
				}
				c, _ := newReclaimTestCoordinator(s)
				r := admitReclaimTest(t, c, "", "owner", 32<<20)
				out := updateReclaimTest(t, c, r, resource.CacheReclaimState{Intent: reclaimIntent(1, 1, 16<<20)})
				if (out.Task == nil) != low {
					t.Fatalf("wrong opt-in behavior: %+v", out)
				}
				if s.reservedCache != 32<<20 || s.reservedMemory != 128<<20 {
					t.Fatal("intent spent a reservation")
				}
				if low && c.Status().HeldIntents != 1 {
					t.Fatal("held intent lost")
				}
			})
		}
	}
}

func twoPoolReclaimTest(t *testing.T, holdA bool) (*CacheCoordinator, *time.Time, reclaimTestRun, reclaimTestRun, reclaimTestRun) {
	t.Helper()
	s := twoPoolState(t)
	a, b := s.PoolStates()["numa0"], s.PoolStates()["numa1"]
	if holdA {
		l := uint64(64 << 20)
		a.CacheLowWatermark = &l
	}
	b.CachePoolCapacity = 64 << 20
	c, now := newReclaimTestCoordinator(s)
	auto := admitReclaimTest(t, c, "numa0", "auto", 32<<20)
	donor := admitReclaimTest(t, c, "numa1", "donor", 48<<20)
	borrower := admitReclaimTest(t, c, "numa1", "borrower", 16<<20)
	return c, now, auto, donor, borrower
}

func cacheDemandTest(t *testing.T, c *CacheCoordinator, r reclaimTestRun, current, delta uint64) {
	t.Helper()
	g, found, err := c.Grant(r.token, current, delta, r.spec.CacheMax, 0, nil)
	if err != nil || !found || g.GrantedDelta != 0 {
		t.Fatalf("expected finite quota wait: %+v %v %v", g, found, err)
	}
}

func TestCacheCoordinatorHeldPoolDoesNotBlockOtherPool(t *testing.T) {
	c, _, a, d, b := twoPoolReclaimTest(t, true)
	updateReclaimTest(t, c, a, resource.CacheReclaimState{Intent: reclaimIntent(1, 1, 16<<20)})
	cacheDemandTest(t, c, b, 16<<20, 8<<20)
	if out := c.Poll(d.token); out.Task == nil || out.Task.Source != resource.CacheReclaimYield {
		t.Fatalf("other pool did not yield: %+v", out)
	}
	if out := c.Poll(a.token); out.Task != nil {
		t.Fatal("B shortage reopened A")
	}
	if c.Status().HeldIntents != 1 {
		t.Fatal("A intent was discarded")
	}
}

func TestCacheCoordinatorRateWaitingAndIssuedAutoSuppressYield(t *testing.T) {
	for _, issued := range []bool{false, true} {
		t.Run(fmt.Sprintf("issued=%v", issued), func(t *testing.T) {
			c, now, a, d, b := twoPoolReclaimTest(t, false)
			if !issued {
				c.tokens = 0
				c.refill = *now
			}
			out := updateReclaimTest(t, c, a, resource.CacheReclaimState{Intent: reclaimIntent(1, 1, 8<<20)})
			if issued && out.Task == nil {
				t.Fatal("no auto authority")
			}
			cacheDemandTest(t, c, b, 16<<20, 4<<20)
			if c.Poll(d.token).Task != nil {
				t.Fatal("yield bypassed primary supply")
			}
			if issued {
				l := uint64(64 << 20)
				c.members[a.spec.SandboxID].pool.CacheLowWatermark = &l
				*now = now.Add(time.Hour)
				if c.Poll(d.token).Task != nil {
					t.Fatal("low water or timeout cancelled issued auto")
				}
			}
		})
	}
}

func TestCacheCoordinatorYieldDrainsBeforeNewAuto(t *testing.T) {
	c, _, a, d, b := twoPoolReclaimTest(t, false)
	cacheDemandTest(t, c, b, 16<<20, 12<<20)
	yield := c.Poll(d.token).Task
	if yield == nil || yield.Source != resource.CacheReclaimYield {
		t.Fatal("missing yield")
	}
	if out := updateReclaimTest(t, c, a, resource.CacheReclaimState{Intent: reclaimIntent(1, 1, 8<<20)}); out.Task != nil {
		t.Fatal("auto overlapped yield")
	}
	out := completeReclaimTest(t, c, d, yield, 48<<20-yield.MaxBytes, nil)
	if out.Task != nil {
		t.Fatal("yield continued ahead of newly schedulable auto")
	}
	if next := c.Poll(a.token).Task; next == nil || next.Source != resource.CacheReclaimAutonomous {
		t.Fatal("auto did not win next batch")
	}
}

func TestCacheCoordinatorCompletionKeepsPrimaryContinuationAtomic(t *testing.T) {
	c, _, a, d, b := twoPoolReclaimTest(t, false)
	first := updateReclaimTest(t, c, a, resource.CacheReclaimState{Intent: reclaimIntent(1, 1, 8<<20)}).Task
	cacheDemandTest(t, c, b, 16<<20, 8<<20)
	next := completeReclaimTest(t, c, a, first, 32<<20-first.MaxBytes, reclaimIntent(1, 1, 4<<20)).Task
	if next == nil || next.Source != resource.CacheReclaimAutonomous || c.Poll(d.token).Task != nil {
		t.Fatal("completion/intent gap opened yield")
	}
	// A zero attempt closes the observation; changing only ID cannot keep
	// supply alive and prevent the genuinely cache-blocked pool from yielding.
	zero := completeReclaimTest(t, c, a, next, 32<<20-first.MaxBytes, reclaimIntent(2, 1, 4<<20))
	if zero.Task != nil {
		t.Fatal("zero observation resurrected")
	}
	if task := c.Poll(d.token).Task; task == nil || task.Source != resource.CacheReclaimYield {
		t.Fatal("fruitless auto blocked supplemental yield")
	}
}

func TestCacheCoordinatorFiniteIntentAndDuplicateCompletion(t *testing.T) {
	s := makeState(1<<30, 0)
	c, _ := newReclaimTestCoordinator(s)
	r := admitReclaimTest(t, c, "", "run", 32<<20)
	in := resource.CacheReclaimState{RunID: r.run, Intent: reclaimIntent(1, 1, 4<<20)}
	first := updateReclaimTest(t, c, r, in).Task
	// Resending the original pre-authorization report cannot refill the
	// finite opportunity while its grant response is lost.
	for i := 0; i < 10; i++ {
		if task := updateReclaimTest(t, c, r, in).Task; *task != *first {
			t.Fatal("retransmit changed authority")
		}
	}
	done := resource.CacheReclaimState{RunID: r.run, Current: 28 << 20, LastSeq: first.Seq, Done: &resource.CacheReclaimDone{Task: *first, Current: 28 << 20}}
	out := updateReclaimTest(t, c, r, done)
	if out.Ack != first.Seq || out.Task != nil || out.Reservation != 28<<20 {
		t.Fatalf("finite completion: %+v", out)
	}
	g, found, err := c.Grant(r.token, 28<<20, 4<<20, 128<<20, 0, nil)
	if err != nil || !found || g.Reservation != 32<<20 {
		t.Fatalf("post-completion growth: %+v %v", g, err)
	}
	for i := 0; i < 5; i++ {
		if out := updateReclaimTest(t, c, r, done); out.Reservation != 32<<20 || out.Task != nil {
			t.Fatal("old completion deducted a later grant")
		}
	}
	if s.reservedMemory != 128<<20 {
		t.Fatal("cache changed Guest ledger")
	}
}

func TestCacheCoordinatorNodeBurstAndNoDebtAfterPause(t *testing.T) {
	s := makeState(8<<30, 0)
	l := uint64(1 << 30)
	s.CacheLowWatermark = &l
	c, now := newReclaimTestCoordinator(s)
	var runs []reclaimTestRun
	for i := 0; i < 12; i++ {
		r := admitReclaimTest(t, c, "", fmt.Sprintf("owner-%02d", i), 32<<20)
		runs = append(runs, r)
		updateReclaimTest(t, c, r, resource.CacheReclaimState{Intent: reclaimIntent(1, 1, 16<<20)})
	}
	*now = now.Add(24 * time.Hour)
	if c.Status().InFlightTasks != 0 {
		t.Fatal("low water issued authority")
	}
	s.CacheLowWatermark = nil
	c.Poll(runs[0].token)
	status := c.Status()
	if status.InFlightTasks != cacheReclaimMaxInFlight || status.InFlightBytes > cacheReclaimBurst {
		t.Fatalf("pause accumulated reclaim debt: %+v", status)
	}
	var wg sync.WaitGroup
	for _, r := range runs {
		wg.Add(1)
		go func(r reclaimTestRun) {
			defer wg.Done()
			for j := 0; j < 8; j++ {
				c.Poll(r.token)
			}
		}(r)
	}
	wg.Wait()
	if status = c.Status(); status.InFlightTasks > cacheReclaimMaxInFlight || status.InFlightBytes > cacheReclaimBurst {
		t.Fatal("concurrent polling exceeded in-flight cap")
	}
}

func syncReclaimSpec(r reclaimTestRun, current uint64, token string) SyncSpec {
	return SyncSpec{Token: token, SandboxID: r.spec.SandboxID, PeerPID: r.spec.PeerPID, CgroupPath: r.spec.CgroupPath,
		Capacity: r.spec.Capacity, ConfiguredAllocatable: r.spec.ConfiguredAllocatable, ReservationMemory: r.spec.InitialBudget,
		CacheReservation: current, CacheMax: r.spec.CacheMax, Settled: true, ClientFeatures: r.spec.ClientFeatures, Conn: r.spec.Conn}
}

func TestCacheCoordinatorLiveSyncReplaysUnseenAuthority(t *testing.T) {
	s := makeState(1<<30, 0)
	c, _ := newReclaimTestCoordinator(s)
	r := admitReclaimTest(t, c, "", "run", 32<<20)
	in := resource.CacheReclaimState{RunID: r.run, Current: 32 << 20, Intent: reclaimIntent(1, 1, 8<<20)}
	first := updateReclaimTest(t, c, r, in).Task
	_, _, recovered, err := c.Sync(syncReclaimSpec(r, 32<<20, "reconnected"), &in)
	if err != nil || recovered.Task == nil || *recovered.Task != *first {
		t.Fatalf("lost task on live reconnect: %+v %v", recovered, err)
	}
	if _, err := c.Update(r.token, &in); err == nil {
		t.Fatal("old token retained authority")
	}
	r.token = "reconnected"
	out := completeReclaimTest(t, c, r, first, 28<<20, reclaimIntent(1, 1, 4<<20))
	if out.Task == nil || out.Task.Seq != first.Seq+1 {
		t.Fatal("continuation absent")
	}
	// The completion ACK + next task response is lost. StateSync repeats the
	// old completion, preserving the next node-issued execution right.
	stale := resource.CacheReclaimState{RunID: r.run, Current: 28 << 20, LastSeq: first.Seq, Done: &resource.CacheReclaimDone{Task: *first, Current: 28 << 20, Intent: reclaimIntent(1, 1, 4<<20)}}
	_, _, replayed, err := c.Sync(syncReclaimSpec(r, 28<<20, "again"), &stale)
	if err != nil || replayed.Task == nil || *replayed.Task != *out.Task || replayed.Ack != first.Seq {
		t.Fatalf("lost continuation after duplicate completion: %+v %v", replayed, err)
	}
}

func TestCacheCoordinatorRestartWaitsForEveryParticipant(t *testing.T) {
	s := makeState(2<<30, 0)
	c, _ := newReclaimTestCoordinator(s)
	a := admitReclaimTest(t, c, "", "a", 32<<20)
	b := admitReclaimTest(t, c, "", "b", 32<<20)
	task := updateReclaimTest(t, c, a, resource.CacheReclaimState{Intent: reclaimIntent(1, 1, 8<<20)}).Task
	rebuilt := makeState(2<<30, 0)
	for _, r := range []reclaimTestRun{a, b} {
		if err := rebuilt.InstallProvisional(ProvisionalSpec{SandboxID: r.spec.SandboxID, PeerPID: r.spec.PeerPID, CgroupPath: r.spec.CgroupPath, Capacity: r.spec.Capacity,
			ConfiguredAllocatable: r.spec.ConfiguredAllocatable, ReservationMemory: r.spec.Capacity.MemoryBytes, CacheReservation: r.spec.CacheMax, CacheMax: r.spec.CacheMax, InitialBudget: r.spec.Capacity.MemoryBytes,
			RecoverySource: RecoveryLease, RecoveryKey: "lease:" + r.spec.SandboxID, ClientFeatures: r.spec.ClientFeatures}); err != nil {
			t.Fatal(err)
		}
	}
	restarted, _ := newReclaimTestCoordinator(rebuilt)
	active := resource.CacheReclaimState{RunID: a.run, Current: 32 << 20, LastSeq: task.Seq, Task: task, Intent: reclaimIntent(1, 1, 4<<20)}
	_, _, out, err := restarted.Sync(syncReclaimSpec(a, 32<<20, "a-new"), &active)
	if err != nil || out.Task == nil || *out.Task != *task {
		t.Fatalf("restart lost accepted task: %+v %v", out, err)
	}
	a.token = "a-new"
	completed := completeReclaimTest(t, restarted, a, task, 28<<20, reclaimIntent(1, 1, 4<<20))
	if completed.Task != nil || restarted.Status().RecoveryPending != 1 {
		t.Fatal("unknown participant did not freeze new permits")
	}
	_, _, _, err = restarted.Sync(syncReclaimSpec(b, 32<<20, "b-new"), &resource.CacheReclaimState{RunID: b.run, Current: 32 << 20, LastSeq: 9})
	if err != nil {
		t.Fatal(err)
	}
	if next := restarted.Poll(a.token).Task; next == nil || next.Seq <= task.Seq {
		t.Fatal("recovery failed to resume monotonic continuation")
	}
	b.token = "b-new"
	if next := updateReclaimTest(t, restarted, b, resource.CacheReclaimState{LastSeq: 9, Intent: reclaimIntent(1, 1, 4<<20)}).Task; next == nil || next.Seq <= 9 {
		t.Fatal("restart reused retained client sequence")
	}
}

func TestCacheCoordinatorInvalidCompletionIsAtomic(t *testing.T) {
	s := makeState(1<<30, 0)
	c, _ := newReclaimTestCoordinator(s)
	r := admitReclaimTest(t, c, "", "run", 32<<20)
	task := updateReclaimTest(t, c, r, resource.CacheReclaimState{Intent: reclaimIntent(1, 1, 4<<20)}).Task
	for _, current := range []uint64{24 << 20, 36 << 20} {
		_, err := c.Update(r.token, &resource.CacheReclaimState{RunID: r.run, LastSeq: task.Seq, Done: &resource.CacheReclaimDone{Task: *task, Current: current}})
		if err == nil || s.reservedCache != 32<<20 || c.Status().InFlightTasks != 1 {
			t.Fatal("invalid completion partially committed")
		}
	}
	widened := *task
	widened.MaxBytes *= 2
	if _, err := c.Update(r.token, &resource.CacheReclaimState{RunID: r.run, LastSeq: task.Seq, Task: &widened}); err == nil {
		t.Fatal("task authority expanded")
	}
	g, _, err := c.Grant(r.token, 32<<20, 16<<20, 128<<20, 0, nil)
	if err != nil || g.GrantedDelta != 0 || s.reservedCache != 32<<20 {
		t.Fatal("growth overlapped an unresolved task")
	}
}

func TestCacheCoordinatorGrowthMustBePhysicallyConfirmed(t *testing.T) {
	for _, lost := range []bool{false, true} {
		t.Run(fmt.Sprintf("lost-grow=%v", lost), func(t *testing.T) {
			s := makeState(1<<30, 0)
			c, now := newReclaimTestCoordinator(s)
			r := admitReclaimTest(t, c, "", "run", 32<<20)
			c.tokens, c.refill = 0, *now
			in := resource.CacheReclaimState{RunID: r.run, Current: 32 << 20, Intent: reclaimIntent(1, 1, 8<<20)}
			updateReclaimTest(t, c, r, in)
			grant, found, err := c.Grant(r.token, 32<<20, 32<<20, 128<<20, 0, nil)
			if err != nil || !found || grant.Reservation != 64<<20 {
				t.Fatalf("grant: %+v %v", grant, err)
			}
			c.tokens = cacheReclaimBurst
			if c.Poll(r.token).Task != nil {
				t.Fatal("heartbeat authorized reclaim of unconfirmed growth")
			}
			reservation := uint64(64 << 20)
			if lost {
				reservation = 32 << 20
			}
			_, _, out, err := c.Sync(syncReclaimSpec(r, reservation, "new-token"), &in)
			if err != nil {
				t.Fatal(err)
			}
			r.token = "new-token"
			if !lost {
				if out.Task != nil {
					t.Fatal("StateSync confused grant with physical allocation")
				}
				// The worker either finishes allocation or safely rolls back;
				// only this explicit physical confirmation unblocks authority.
				in.Current = 64 << 20
				out, err = c.Update(r.token, &in)
				if err != nil {
					t.Fatal(err)
				}
			}
			if out.Task == nil || out.Task.Target != reservation-out.Task.MaxBytes {
				t.Fatalf("wrong confirmed execution baseline: %+v", out)
			}
			completeReclaimTest(t, c, r, out.Task, reservation-out.Task.MaxBytes, nil)
		})
	}
}

func TestCacheCoordinatorPendingGrowthDoesNotBlockOtherPoolYield(t *testing.T) {
	c, now, a, d, b := twoPoolReclaimTest(t, false)
	c.tokens, c.refill = 0, *now
	intent := reclaimIntent(1, 1, 8<<20)
	updateReclaimTest(t, c, a, resource.CacheReclaimState{Intent: intent})
	grant, found, err := c.Grant(a.token, 32<<20, 32<<20, a.spec.CacheMax, 0, nil)
	if err != nil || !found || grant.Reservation != 64<<20 {
		t.Fatalf("grant: %+v %v", grant, err)
	}
	// This retained opportunity has no executable physical baseline yet.
	// A different pool must be able to resolve its real quota shortage.
	c.tokens = cacheReclaimBurst
	cacheDemandTest(t, c, b, 16<<20, 12<<20)
	yield := c.Poll(d.token).Task
	if yield == nil || yield.Source != resource.CacheReclaimYield {
		t.Fatal("unconfirmed growth blocked another pool's supplemental yield")
	}
	if c.Poll(a.token).Task != nil {
		t.Fatal("unconfirmed physical capacity received reclaim authority")
	}
	if m := c.members[a.spec.SandboxID]; !m.growthPending || m.intent == nil || *m.intent != *intent {
		t.Fatal("pending growth discarded or consumed the retained opportunity")
	}
	// Physical confirmation restores primary eligibility, but cannot overlap
	// the already-issued yield. It must win as soon as that batch completes.
	if out := updateReclaimTest(t, c, a, resource.CacheReclaimState{Intent: intent}); out.Task != nil {
		t.Fatal("confirmed autonomous supply overlapped issued yield")
	}
	if out := completeReclaimTest(t, c, d, yield, 48<<20-yield.MaxBytes, nil); out.Task != nil {
		t.Fatal("supplemental yield continued ahead of confirmed autonomous supply")
	}
	if task := c.Poll(a.token).Task; task == nil || task.Source != resource.CacheReclaimAutonomous || task.Target+task.MaxBytes != grant.Reservation {
		t.Fatalf("confirmed autonomous supply did not resume at its physical baseline: %+v", task)
	}
	if c.Poll(d.token).Task != nil {
		t.Fatal("supplemental yield overlapped resumed autonomous work")
	}
}

func TestCacheCoordinatorDisconnectedUnissuedIntentDoesNotBlockYield(t *testing.T) {
	c, now, a, d, b := twoPoolReclaimTest(t, false)
	c.tokens, c.refill = 0, *now
	updateReclaimTest(t, c, a, resource.CacheReclaimState{Intent: reclaimIntent(1, 1, 8<<20)})
	p := c.state.forSID(a.spec.SandboxID)
	p.mu.Lock()
	p.bySID[a.spec.SandboxID].Conn = nil
	p.mu.Unlock()
	c.tokens = cacheReclaimBurst
	cacheDemandTest(t, c, b, 16<<20, 4<<20)
	if task := c.Poll(d.token).Task; task == nil || task.Source != resource.CacheReclaimYield {
		t.Fatal("unissued disconnected intent blocked another pool")
	}
	if c.members[a.spec.SandboxID].intent == nil {
		t.Fatal("disconnect discarded finite opportunity")
	}
}

func TestCacheCoordinatorUnknownRecoveryBlocksAuthorityAndGrowth(t *testing.T) {
	s := makeState(2<<30, 0)
	c, _ := newReclaimTestCoordinator(s)
	r := admitReclaimTest(t, c, "", "known", 32<<20)
	if err := s.InstallProvisional(ProvisionalSpec{SandboxID: "unknown", CgroupPath: "/cg/unknown", Capacity: Resources{MemoryBytes: 128 << 20}, ReservationMemory: 128 << 20,
		RecoverySource: RecoveryCgroup, RecoveryKey: "cgroup:/cg/unknown", CacheRecoveryUnknown: true}); err != nil {
		t.Fatal(err)
	}
	out := updateReclaimTest(t, c, r, resource.CacheReclaimState{Intent: reclaimIntent(1, 1, 4<<20)})
	if out.Task != nil || c.Status().RecoveryPending != 1 {
		t.Fatal("unknown cache participant did not freeze authority")
	}
	grant, found, err := c.Grant(r.token, 32<<20, 4<<20, 128<<20, 0, nil)
	if err != nil || !found || grant.GrantedDelta != 0 || !grant.RecoveryBlocked {
		t.Fatalf("unknown recovery allowed growth: %+v %v", grant, err)
	}
	if shortage, _ := cacheShortageLocked(s, time.Now()); shortage != 0 {
		t.Fatal("recovery was reported as reclaimable quota shortage")
	}
	var unknown Reservation
	for _, v := range s.SweepSnapshot() {
		if v.SandboxID == "unknown" {
			unknown = v
		}
	}
	if _, ok, err := s.RemoveIfIdentity(unknown.SandboxID, unknown.identity()); err != nil || !ok {
		t.Fatal("failed to confirm unknown consumer exit")
	}
	if task := c.Poll(r.token).Task; task == nil {
		t.Fatal("confirmed exit did not reopen primary work")
	}
}

func TestCacheCoordinatorNewLegacyRunCannotInheritOldAuthority(t *testing.T) {
	s := makeState(1<<30, 0)
	c, _ := newReclaimTestCoordinator(s)
	r := admitReclaimTest(t, c, "", "reused", 32<<20)
	if task := updateReclaimTest(t, c, r, resource.CacheReclaimState{Intent: reclaimIntent(1, 1, 4<<20)}).Task; task == nil {
		t.Fatal("no task")
	}
	if _, found, err := s.Release(r.token); err != nil || !found {
		t.Fatal("release failed")
	}
	spec := r.spec
	spec.Token = "new-legacy-token"
	spec.ClientFeatures = []string{resource.FeatureCacheBudgetV1, resource.CacheMaximumFeature(spec.CacheMax)}
	if _, _, err := c.Admit(spec, nil); err != nil {
		t.Fatal(err)
	}
	if out := c.Poll(spec.Token); out.Task != nil || out.Ack != 0 || c.Status().InFlightTasks != 0 {
		t.Fatal("PID/SID reuse inherited a previous run's task")
	}
}

func TestCacheCoordinatorIntentDoesNotWithdrawGrowthDemand(t *testing.T) {
	c, _, a, _, b := twoPoolReclaimTest(t, true)
	cacheDemandTest(t, c, b, 16<<20, 8<<20)
	p := c.state.forSID(b.spec.SandboxID)
	before := p.cacheDemandSince[b.token]
	updateReclaimTest(t, c, b, resource.CacheReclaimState{})
	if after, ok := p.cacheDemandSince[b.token]; !ok || after != before {
		t.Fatal("pure intent update erased pending cache growth")
	}
	if c.Poll(a.token).Task != nil {
		t.Fatal("unrelated pool reopened")
	}
}

func TestCacheCoordinatorProbeYieldsPriorityAndNewObservationRevives(t *testing.T) {
	c, now, a, d, b := twoPoolReclaimTest(t, false)
	first := updateReclaimTest(t, c, a, resource.CacheReclaimState{Intent: reclaimIntent(1, 1, 4<<20)}).Task
	completeReclaimTest(t, c, a, first, 32<<20, nil)
	// A later timestamp and ID do not constitute improved reclaimable state.
	*now = now.Add(time.Second)
	if out := updateReclaimTest(t, c, a, resource.CacheReclaimState{LastSeq: first.Seq, Intent: reclaimIntent(2, 1, 4<<20)}); out.Task != nil {
		t.Fatal("time revived zero supply")
	}
	cacheDemandTest(t, c, b, 16<<20, 4<<20)
	probe := reclaimIntent(3, 1, 4<<20)
	probe.Probe = true
	if out := updateReclaimTest(t, c, a, resource.CacheReclaimState{LastSeq: first.Seq, Intent: probe}); out.Task != nil {
		t.Fatal("probe interrupted issued yield")
	}
	yield := c.Poll(d.token).Task
	completeReclaimTest(t, c, d, yield, 48<<20-yield.MaxBytes, nil)
	if task := c.Poll(a.token).Task; task == nil || task.Source != resource.CacheReclaimAutonomous {
		t.Fatal("probe did not obtain ordinary bounded authority")
	}
}
