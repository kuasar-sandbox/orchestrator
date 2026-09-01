package orch

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestLaunchGroupClaimCancelAndCleanupFence(t *testing.T) {
	var g launchGroup
	a, err := g.Claim(context.Background(), "sid", launchCreate)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g.Claim(context.Background(), "sid", launchResume); !errors.Is(err, errLaunchClaimed) {
		t.Fatalf("second Claim error = %v, want errLaunchClaimed", err)
	}

	g.Cancel("sid")
	select {
	case <-a.Context().Done():
	case <-time.After(time.Second):
		t.Fatal("Cancel did not cancel the attempt context")
	}
	if _, err := g.Claim(context.Background(), "sid", launchCreate); !errors.Is(err, errLaunchClaimed) {
		t.Fatalf("Claim after Cancel error = %v, want cleanup fence", err)
	}

	g.Finish(a, context.Canceled)
	if next, err := g.Claim(context.Background(), "sid", launchResume); err != nil {
		t.Fatalf("Claim after Finish: %v", err)
	} else {
		g.Finish(next, nil)
	}
}

func TestLaunchAttemptArtifactPrepareReplayAndFinalResult(t *testing.T) {
	var g launchGroup
	attempt, err := g.Claim(context.Background(), "sid", launchResume)
	if err != nil {
		t.Fatal(err)
	}
	attempt.SetRunID("run-1")
	summary := configsock.ArtifactPrepareSummary{
		SchemaVersion:    configsock.ArtifactPrepareSchemaVersion,
		Capacity:         configsock.ArtifactCapacity{CPU: 2, Memory: "2GiB"},
		ResolutionDigest: "digest", RequiredRefCount: 3,
	}
	if replay, err := attempt.SubmitPrepare("run-1", summary); err != nil || replay {
		t.Fatalf("first SubmitPrepare = replay %t, err %v", replay, err)
	}
	if got, err := attempt.WaitPrepare(context.Background()); err != nil || !configsock.EqualArtifactPrepareSummary(got, summary) {
		t.Fatalf("WaitPrepare = %+v, %v", got, err)
	}
	if replay, err := attempt.SubmitPrepare("run-1", summary); err != nil || !replay {
		t.Fatalf("identical SubmitPrepare = replay %t, err %v", replay, err)
	}

	waitCtx, cancelWait := context.WithCancel(context.Background())
	cancelWait()
	if _, err := attempt.WaitFinalSpec(waitCtx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled final waiter = %v", err)
	}
	// Canceling one HTTP waiter does not revoke the accepted summary.
	final := &configsock.LaunchSpec{Exec: "/sandbox-ctl", Args: []string{"run"}}
	attempt.PublishFinalSpec(final)
	for i := 0; i < 2; i++ {
		if replay, err := attempt.SubmitPrepare("run-1", summary); err != nil || !replay {
			t.Fatalf("post-final replay %d = %t, %v", i, replay, err)
		}
		got, err := attempt.WaitFinalSpec(context.Background())
		if err != nil || got.Exec != final.Exec || len(got.Args) != 1 {
			t.Fatalf("final replay %d = %+v, %v", i, got, err)
		}
		got.Args[0] = "mutated"
	}
	g.Finish(attempt, nil)
}

func TestLaunchAttemptArtifactPrepareClonesNetworkSummary(t *testing.T) {
	var g launchGroup
	attempt, err := g.Claim(context.Background(), "sid", launchResume)
	if err != nil {
		t.Fatal(err)
	}
	attempt.SetRunID("run-1")
	summary := configsock.ArtifactPrepareSummary{
		SchemaVersion:      configsock.ArtifactPrepareSchemaVersion,
		PreparedSourceKind: "sandbox",
		Network: configsock.ArtifactNetwork{
			Hostname: "sandbox.local",
			DNS:      []string{"1.1.1.1"},
		},
		ResolutionDigest: "digest",
		RequiredRefCount: 1,
	}
	summary.DiskTopology = validArtifactDiskTopology()
	dataDisk := summary.DiskTopology.Root
	dataDisk.Name = "data"
	summary.DiskTopology.Disks = []types.ArtifactDiskShape{dataDisk}
	if _, err := attempt.SubmitPrepare("run-1", summary); err != nil {
		t.Fatal(err)
	}

	// Mutating either the caller-owned request or a returned copy must not
	// change the accepted replay identity stored by the launch attempt.
	summary.Network.DNS[0] = "9.9.9.9"
	summary.DiskTopology.Disks[0].Name = "mutated"
	got, err := attempt.WaitPrepare(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Network.DNS) != 1 || got.Network.DNS[0] != "1.1.1.1" {
		t.Fatalf("accepted network DNS = %v, want immutable original", got.Network.DNS)
	}
	if len(got.DiskTopology.Disks) != 1 || got.DiskTopology.Disks[0].Name != "data" {
		t.Fatalf("accepted disk topology = %+v, want immutable original", got.DiskTopology)
	}
	got.Network.DNS[0] = "8.8.8.8"
	got.DiskTopology.Disks[0].Name = "returned-copy-mutation"
	replay := configsock.CloneArtifactPrepareSummary(got)
	replay.Network.DNS[0] = "1.1.1.1"
	replay.DiskTopology.Disks[0].Name = "data"
	if identical, err := attempt.SubmitPrepare("run-1", replay); err != nil || !identical {
		t.Fatalf("immutable replay = %t, %v", identical, err)
	}
	g.Finish(attempt, nil)
}

func TestLaunchAttemptArtifactPrepareConflictCancelsExactRun(t *testing.T) {
	var g launchGroup
	attempt, err := g.Claim(context.Background(), "sid", launchCreate)
	if err != nil {
		t.Fatal(err)
	}
	attempt.SetRunID("run-1")
	first := configsock.ArtifactPrepareSummary{SchemaVersion: 1, ResolutionDigest: "first"}
	if _, err := attempt.SubmitPrepare("run-1", first); err != nil {
		t.Fatal(err)
	}
	conflict := first
	conflict.ResolutionDigest = "different"
	if _, err := attempt.SubmitPrepare("run-1", conflict); !errors.Is(err, errArtifactPrepareConflict) {
		t.Fatalf("conflicting replay error = %v", err)
	}
	select {
	case <-attempt.Context().Done():
	case <-time.After(time.Second):
		t.Fatal("conflicting replay did not cancel launch")
	}
	if _, err := attempt.SubmitPrepare("stale-run", first); !errors.Is(err, errLaunchOwnershipLost) {
		t.Fatalf("stale exact run error = %v", err)
	}
	g.Finish(attempt, context.Canceled)
}

func TestLaunchGroupWaitHonorsCallerContext(t *testing.T) {
	var g launchGroup
	a, err := g.Claim(context.Background(), "sid", launchCreate)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	found, err := g.Wait(ctx, "sid")
	if !found || !errors.Is(err, context.Canceled) {
		t.Fatalf("Wait = found %v err %v, want true/context canceled", found, err)
	}
	if _, found := g.Lookup("sid"); !found {
		t.Fatal("caller cancellation released the attempt")
	}
	g.Finish(a, nil)
	if found, err := g.Wait(context.Background(), "missing"); found || err != nil {
		t.Fatalf("missing Wait = found %v err %v", found, err)
	}
}

func TestLaunchGroupAllowsDistinctSandboxes(t *testing.T) {
	var g launchGroup
	first, err := g.Claim(context.Background(), "first", launchCreate)
	if err != nil {
		t.Fatal(err)
	}
	second, err := g.Claim(context.Background(), "second", launchResume)
	if err != nil {
		t.Fatalf("distinct Claim: %v", err)
	}
	g.Finish(first, nil)
	g.Finish(second, nil)
}

func TestLaunchGroupStartClosesDoneAfterTerminalWork(t *testing.T) {
	var g launchGroup
	a, err := g.Claim(context.Background(), "sid", launchResume)
	if err != nil {
		t.Fatal(err)
	}
	terminal := make(chan struct{})
	cleanup := make(chan struct{})
	var terminalVisible atomic.Bool
	g.Start(a, func(context.Context, *launchAttempt) error {
		<-terminal
		terminalVisible.Store(true)
		close(cleanup)
		return errors.New("launch failed")
	})

	waited := make(chan error, 1)
	go func() {
		waited <- a.wait(context.Background())
	}()
	select {
	case <-waited:
		t.Fatal("Wait returned before terminal state and cleanup")
	default:
	}
	close(terminal)
	select {
	case err := <-waited:
		if err == nil || !terminalVisible.Load() {
			t.Fatalf("Wait error = %v, terminal visible = %v", err, terminalVisible.Load())
		}
	case <-time.After(time.Second):
		t.Fatal("Wait did not finish")
	}
	select {
	case <-cleanup:
	default:
		t.Fatal("cleanup was not complete before Wait returned")
	}
}

func TestLaunchGroupStaleFinishDoesNotDeleteSuccessor(t *testing.T) {
	var g launchGroup
	first, err := g.Claim(context.Background(), "sid", launchCreate)
	if err != nil {
		t.Fatal(err)
	}
	// Model a delayed old worker reaching Finish after a successor was installed.
	// Normal Claim fencing prevents this ordering; the pointer comparison remains
	// a defense against stale completion paths and future ownership handoffs.
	secondCtx, secondCancel := context.WithCancel(context.Background())
	second := &launchAttempt{
		sid: "sid", kind: launchResume, ctx: secondCtx, cancel: secondCancel, done: make(chan struct{}),
	}
	g.mu.Lock()
	g.m["sid"] = second
	g.mu.Unlock()
	g.Finish(first, errors.New("late finish"))
	got, found := g.Lookup("sid")
	if !found || got != second {
		t.Fatalf("late Finish removed successor: found=%v got=%p want=%p", found, got, second)
	}
	g.Finish(second, nil)
}

func TestLaunchGroupRejectsCanceledLifecycle(t *testing.T) {
	var g launchGroup
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := g.Claim(ctx, "sid", launchCreate); !errors.Is(err, context.Canceled) {
		t.Fatalf("Claim error = %v, want context canceled", err)
	}
}

func TestLaunchGroupDrainWaitsForEveryCleanup(t *testing.T) {
	var g launchGroup
	first, err := g.Claim(context.Background(), "first", launchCreate)
	if err != nil {
		t.Fatal(err)
	}
	second, err := g.Claim(context.Background(), "second", launchResume)
	if err != nil {
		t.Fatal(err)
	}
	drained := make(chan error, 1)
	go func() { drained <- g.Drain(context.Background()) }()

	select {
	case err := <-drained:
		t.Fatalf("Drain returned with active attempts: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	g.Finish(first, errors.New("first failed after cleanup"))
	select {
	case err := <-drained:
		t.Fatalf("Drain returned before all attempts finished: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	g.Finish(second, nil)
	select {
	case err := <-drained:
		if err != nil {
			t.Fatalf("Drain error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Drain did not observe all completed attempts")
	}
}

func TestLaunchGroupDrainHonorsCallerContext(t *testing.T) {
	var g launchGroup
	attempt, err := g.Claim(context.Background(), "active", launchCreate)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := g.Drain(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Drain error = %v, want context canceled", err)
	}
	if current, found := g.Lookup("active"); !found || current != attempt {
		t.Fatal("canceled Drain released the active attempt")
	}
	g.Finish(attempt, nil)
}

// TestPublishToSubscriber remains the route fan-out regression guard: an event
// reaches a live subscriber (lagging subscribers are handled by publish itself).
func TestPublishToSubscriber(t *testing.T) {
	o := &Orchestrator{subs: map[int]chan routesync.Event{}}
	ch, cancel := o.Subscribe()
	defer cancel()
	o.publish(routesync.Event{Kind: routesync.TypeDelete, SID: "x"})
	select {
	case ev := <-ch:
		if ev.SID != "x" {
			t.Fatalf("event = %+v", ev)
		}
	case <-time.After(time.Second):
		t.Fatal("subscriber did not receive event")
	}
}
