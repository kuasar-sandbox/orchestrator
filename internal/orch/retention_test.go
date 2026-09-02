package orch

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func retentionSandbox(t *testing.T, id string, deadUnix int64) *types.Sandbox {
	t.Helper()
	manifestKey := strings.Repeat("a", 64)
	sandbox := &types.Sandbox{
		ID: id, Profile: types.ProfileBare,
		TemplateID: types.TemplateID{Profile: types.ProfileBare, Kind: types.KindImg, Ref: "manifest://" + strings.Repeat("b", 64)}.String(),
		State:      types.StateDead, APISecret: deriveTestAPISecret(t, manifestKey), ManifestKey: manifestKey,
		CreatedUnix: 1, DeadUnix: deadUnix,
	}
	materializeTestSandboxCredentials(t, sandbox)
	return sandbox
}

func retentionBuild(t *testing.T, id string, state types.BuildState, finishedUnix int64, cluster bool) *types.Build {
	t.Helper()
	build := observerBuildingFixture(t, id)
	build.Status = state
	build.ExecutionClaimed = false
	build.ExecutionClaimedUnix = 0
	build.RunID = ""
	build.FinishedUnix = finishedUnix
	if state == types.BuildReady {
		build.PersistID = types.TemplateID{Profile: types.ProfileE2B, Kind: types.KindImg, Ref: "manifest://" + strings.Repeat("c", 64)}.String()
	}
	if cluster {
		build.ClusterGroup = "/retention"
	}
	return build
}

func TestReapTerminalHistoryHonorsTTLAndPublishesDeletes(t *testing.T) {
	cfg := &config.Config{
		Sandbox: config.SandboxConfig{DeadTTL: "1h"},
		Builder: config.BuilderConfig{TerminalTTL: "1h"},
	}
	o := testOrchCfg(t, cfg)
	recorder := &objectObserverRecorder{}
	o.SetExtensionObserver(recorder)
	ctx := context.Background()
	now := time.Unix(10_000, 0)

	oldDead := retentionSandbox(t, "dead-old", now.Add(-2*time.Hour).Unix())
	freshDead := retentionSandbox(t, "dead-fresh", now.Add(-30*time.Minute).Unix())
	for _, sandbox := range []*types.Sandbox{oldDead, freshDead} {
		if err := o.st.Put(ctx, sandbox); err != nil {
			t.Fatal(err)
		}
		o.cache(sandbox)
	}
	directOld := retentionBuild(t, "direct-ready-old", types.BuildReady, now.Add(-2*time.Hour).Unix(), false)
	clusterOld := retentionBuild(t, "cluster-error-old", types.BuildError, now.Add(-2*time.Hour).Unix(), true)
	clusterFresh := retentionBuild(t, "cluster-ready-fresh", types.BuildReady, now.Add(-30*time.Minute).Unix(), true)
	for _, build := range []*types.Build{directOld, clusterOld, clusterFresh} {
		if err := o.st.PutBuild(ctx, build); err != nil {
			t.Fatal(err)
		}
	}

	routeEvents, cancelRoutes := o.Subscribe()
	defer cancelRoutes()
	buildEvents, cancelBuilds := o.SubscribeBuilds()
	defer cancelBuilds()
	if err := o.reapTerminalHistory(ctx, now); err != nil {
		t.Fatal(err)
	}
	if got, err := o.st.Get(ctx, oldDead.ID); err != nil || got != nil || o.lookup(oldDead.ID) != nil {
		t.Fatalf("old dead Sandbox retained: row=%+v cache=%+v err=%v", got, o.lookup(oldDead.ID), err)
	}
	if got, err := o.st.Get(ctx, freshDead.ID); err != nil || got == nil || o.lookup(freshDead.ID) == nil {
		t.Fatalf("fresh dead Sandbox removed: row=%+v cache=%+v err=%v", got, o.lookup(freshDead.ID), err)
	}
	for _, id := range []string{directOld.BuildID, clusterOld.BuildID} {
		if got, err := o.st.GetBuild(ctx, id); err != nil || got != nil {
			t.Fatalf("old terminal Build %s retained: %+v, %v", id, got, err)
		}
	}
	if got, err := o.st.GetBuild(ctx, clusterFresh.BuildID); err != nil || got == nil {
		t.Fatalf("fresh terminal Build removed: %+v, %v", got, err)
	}

	select {
	case event := <-routeEvents:
		if event.Kind != routesync.TypeDelete || event.SID != oldDead.ID {
			t.Fatalf("Sandbox retention event = %+v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("Sandbox retention did not publish Delete")
	}
	select {
	case event := <-buildEvents:
		if event.Kind != routesync.BuildDelete || event.BuildID != clusterOld.BuildID {
			t.Fatalf("Build retention event = %+v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("cluster Build retention did not publish BuildDelete")
	}
	if got := recorder.sandboxKinds(); len(got) != 1 || got[0] != "delete" {
		t.Fatalf("Sandbox retention observer events = %v", got)
	}
	if got := recorder.buildStates(); len(got) != 2 {
		t.Fatalf("Build retention observer states = %v", got)
	}

	recorder.reset()
	if err := o.reapTerminalHistory(ctx, now); err != nil {
		t.Fatal(err)
	}
	if len(recorder.sandboxKinds()) != 0 || len(recorder.buildStates()) != 0 {
		t.Fatal("repeated terminal retention was not idempotent")
	}
}

func TestConcurrentTerminalBuildReapAndSameIDRegistrationStayOrdered(t *testing.T) {
	for iteration := 0; iteration < 12; iteration++ {
		cfg := &config.Config{
			Sandbox: config.SandboxConfig{DeadTTL: "1h"},
			Builder: config.BuilderConfig{TerminalTTL: "1h"},
		}
		o := testOrchCfg(t, cfg)
		ctx := context.Background()
		_, _, fingerprint := allowlistedBuildIdentity(t, o)
		buildID := "retention-race-" + leftPad(iteration, 2)
		cmd := clusterBuildRegisterCommand(buildID, fingerprint)
		if ack := o.HandleCommand(ctx, cmd); ack.Status != routesync.AckAccepted {
			t.Fatalf("iteration %d initial registration = %+v", iteration, ack)
		}
		build, err := o.st.GetBuild(ctx, buildID)
		if err != nil || build == nil {
			t.Fatalf("iteration %d registered Build = %+v, %v", iteration, build, err)
		}
		build.Status = types.BuildBuilding
		build.ExecutionClaimed = true
		build.ExecutionClaimedUnix = 1
		if err := o.st.PutBuild(ctx, build); err != nil {
			t.Fatal(err)
		}
		build.Status = types.BuildError
		build.Reason = "old terminal"
		if err := o.st.PutBuildTerminal(ctx, build); err != nil {
			t.Fatal(err)
		}
		now := time.Now()
		terminal, err := o.st.GetBuild(ctx, buildID)
		if err != nil || terminal == nil {
			t.Fatalf("iteration %d terminal Build = %+v, %v", iteration, terminal, err)
		}
		terminal.FinishedUnix = now.Add(-2 * time.Hour).Unix()
		if err := o.st.PutBuild(ctx, terminal); err != nil {
			t.Fatal(err)
		}
		buildEvents, cancelBuilds := o.SubscribeBuilds()

		start := make(chan struct{})
		var wg sync.WaitGroup
		var reapErr error
		var ack *routesync.CmdAck
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			reapErr = o.reapTerminalHistory(ctx, now)
		}()
		go func() {
			defer wg.Done()
			<-start
			ack = o.HandleCommand(ctx, cmd)
		}()
		close(start)
		wg.Wait()
		if reapErr != nil || ack == nil || ack.Status != routesync.AckAccepted {
			t.Fatalf("iteration %d reap=%v registration=%+v", iteration, reapErr, ack)
		}

		var events []routesync.BuildEvent
		drain := time.NewTimer(25 * time.Millisecond)
	drainLoop:
		for {
			select {
			case event := <-buildEvents:
				events = append(events, event)
			case <-drain.C:
				break drainLoop
			}
		}
		cancelBuilds()
		stored, err := o.st.GetBuild(ctx, buildID)
		if err != nil || len(events) != 2 {
			t.Fatalf("iteration %d stored=%+v events=%+v err=%v", iteration, stored, events, err)
		}
		last := events[len(events)-1]
		if stored == nil {
			if last.Kind != routesync.BuildDelete {
				t.Fatalf("iteration %d deleted row followed by stale event: %+v", iteration, events)
			}
		} else if stored.Status != types.BuildRegistered || last.Kind != routesync.BuildUpsert || last.State != string(types.BuildRegistered) {
			t.Fatalf("iteration %d reused row followed by stale event: row=%+v events=%+v", iteration, stored, events)
		}
	}
}

func leftPad(value, width int) string {
	raw := strings.Repeat("0", width) + strconv.Itoa(value)
	return raw[len(raw)-width:]
}
