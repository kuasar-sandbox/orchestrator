package store

import (
	"context"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/secretbox"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func retentionDeadSandbox(id string, deadUnix int64) *types.Sandbox {
	sandbox := sandboxInsertFixture(id, 0)
	sandbox.State = types.StateDead
	sandbox.LaunchMode = ""
	sandbox.RunID, sandbox.FloatingIP, sandbox.VswitchPort = "", "", ""
	sandbox.InnerIP, sandbox.PortMAC = "", ""
	sandbox.RunDir, sandbox.BaseDir, sandbox.EnvdUDS, sandbox.CiUDS = "", "", "", ""
	sandbox.ResumeSource = types.ResumeSource{}
	sandbox.DeadUnix = deadUnix
	return sandbox
}

func retentionTerminalBuild(id string, status types.BuildState, finishedUnix int64) *types.Build {
	build := buildTriggerFixture(id, status)
	build.RunID = ""
	build.ExecutionClaimed = false
	build.ExecutionClaimedUnix = 0
	build.EnforcementStatus = ""
	build.Phase = ""
	build.PhaseSandboxID = ""
	build.RuntimeVswitchPort = ""
	build.RuntimeFloatingIP = ""
	build.RuntimePortMAC = ""
	build.RuntimeEnvdAccessToken = ""
	build.RuntimePrepareJSON = ""
	build.ExecutionResult = nil
	build.FinishedUnix = finishedUnix
	return build
}

func TestTerminalTransitionsPersistTimestampsAtomically(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	before := time.Now().Unix()

	sandbox := sandboxInsertFixture("timestamp-dead", 0)
	if err := st.InsertSandbox(ctx, sandbox); err != nil {
		t.Fatal(err)
	}
	if changed, err := st.CommitSandboxDead(ctx, sandbox); err != nil || !changed {
		t.Fatalf("CommitSandboxDead = %t, %v", changed, err)
	}
	dead, err := st.Get(ctx, sandbox.ID)
	if err != nil || dead == nil || dead.DeadUnix < before || dead.DeadUnix > time.Now().Unix() {
		t.Fatalf("dead timestamp = %+v, %v", dead, err)
	}

	expired := buildTriggerFixture("timestamp-expired", types.BuildRegistered)
	expired.RunID = ""
	if err := st.PutBuild(ctx, expired); err != nil {
		t.Fatal(err)
	}
	if changed, err := st.ExpireBuild(ctx, expired.BuildID, types.BuildRegistered, "expired"); err != nil || !changed {
		t.Fatalf("ExpireBuild = %t, %v", changed, err)
	}
	expired, err = st.GetBuild(ctx, expired.BuildID)
	if err != nil || expired == nil || expired.Status != types.BuildError || expired.FinishedUnix < before || expired.FinishedUnix > time.Now().Unix() {
		t.Fatalf("expired Build timestamp = %+v, %v", expired, err)
	}

	finished := buildTriggerFixture("timestamp-finished", types.BuildBuilding)
	finished.ExecutionClaimed = true
	finished.ExecutionClaimedUnix = before
	if err := st.PutBuild(ctx, finished); err != nil {
		t.Fatal(err)
	}
	finished.Status = types.BuildReady
	finished.PersistID = "e2b:img:manifest://finished"
	if err := st.PutBuildTerminal(ctx, finished); err != nil {
		t.Fatal(err)
	}
	finished, err = st.GetBuild(ctx, finished.BuildID)
	if err != nil || finished == nil || finished.Status != types.BuildReady || finished.FinishedUnix < before || finished.FinishedUnix > time.Now().Unix() {
		t.Fatalf("finished Build timestamp = %+v, %v", finished, err)
	}
}

func TestTerminalRetentionBeforeAfterTTLAndOwnershipFences(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()

	for _, sandbox := range []*types.Sandbox{
		retentionDeadSandbox("dead-old", 100),
		retentionDeadSandbox("dead-fresh", 200),
	} {
		if err := st.InsertSandbox(ctx, sandbox); err != nil {
			t.Fatal(err)
		}
	}
	cleanupPending := sandboxInsertFixture("delete-cleanup-pending", 1)
	cleanupPending.State = types.StateDeleting
	cleanupPending.LaunchMode = ""
	if err := st.InsertSandbox(ctx, cleanupPending); err != nil {
		t.Fatal(err)
	}
	dead, err := st.DeadSandboxesForRetention(ctx, 150, 10)
	if err != nil || len(dead) != 1 || dead[0].ID != "dead-old" {
		t.Fatalf("dead retention candidates = %+v, %v", dead, err)
	}
	if deleted, err := st.DeleteDeadSandboxForRetention(ctx, dead[0], 150); err != nil || !deleted {
		t.Fatalf("delete old dead Sandbox = %t, %v", deleted, err)
	}
	if got, err := st.Get(ctx, "dead-old"); err != nil || got != nil {
		t.Fatalf("old dead Sandbox remained = %+v, %v", got, err)
	}
	for _, id := range []string{"dead-fresh", "delete-cleanup-pending"} {
		if got, err := st.Get(ctx, id); err != nil || got == nil {
			t.Fatalf("protected Sandbox %s = %+v, %v", id, got, err)
		}
	}

	for _, build := range []*types.Build{
		retentionTerminalBuild("ready-old", types.BuildReady, 100),
		retentionTerminalBuild("error-old", types.BuildError, 110),
		retentionTerminalBuild("ready-fresh", types.BuildReady, 200),
		retentionTerminalBuild("ready-owned", types.BuildReady, 90),
	} {
		if err := st.PutBuild(ctx, build); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := st.db.ExecContext(ctx, `UPDATE builds SET execution_claimed=1,execution_claimed_unix=1,run_id='owned-run' WHERE build_id='ready-owned'`); err != nil {
		t.Fatal(err)
	}
	builds, err := st.TerminalBuildsForRetention(ctx, 150, 10)
	if err != nil || len(builds) != 2 || builds[0].BuildID != "ready-old" || builds[1].BuildID != "error-old" {
		t.Fatalf("terminal Build candidates = %+v, %v", builds, err)
	}
	for _, build := range builds {
		if deleted, err := st.DeleteTerminalBuildForRetention(ctx, build, 150); err != nil || !deleted {
			t.Fatalf("delete terminal Build %s = %t, %v", build.BuildID, deleted, err)
		}
	}
	for _, id := range []string{"ready-fresh", "ready-owned"} {
		if got, err := st.GetBuild(ctx, id); err != nil || got == nil {
			t.Fatalf("protected Build %s = %+v, %v", id, got, err)
		}
	}
}

func TestTerminalRetentionExactCandidateAndBatchBound(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	for index := 0; index < TerminalRetentionBatchLimit+2; index++ {
		build := retentionTerminalBuild("batch-"+leftPadDecimal(index, 3), types.BuildReady, 1)
		if err := st.PutBuild(ctx, build); err != nil {
			t.Fatal(err)
		}
	}
	candidates, err := st.TerminalBuildsForRetention(ctx, 2, 0)
	if err != nil || len(candidates) != TerminalRetentionBatchLimit {
		t.Fatalf("bounded terminal batch = %d, %v", len(candidates), err)
	}
	stale := candidates[0]
	if _, err := st.db.ExecContext(ctx, `UPDATE builds SET finished_unix=3 WHERE build_id=?`, stale.BuildID); err != nil {
		t.Fatal(err)
	}
	if deleted, err := st.DeleteTerminalBuildForRetention(ctx, stale, 2); err != nil || deleted {
		t.Fatalf("stale retention candidate deleted = %t, %v", deleted, err)
	}
}

func leftPadDecimal(value, width int) string {
	raw := strings.Repeat("0", width) + strconv.Itoa(value)
	return raw[len(raw)-width:]
}

func TestTerminalRetentionSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "retention.db")
	box, err := secretbox.NewFromColonHex(strings.Repeat("a", 64))
	if err != nil {
		t.Fatal(err)
	}
	st, err := Open(path, box)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.PutBuild(context.Background(), retentionTerminalBuild("restart-ready", types.BuildReady, 10)); err != nil {
		t.Fatal(err)
	}
	if err := st.Put(context.Background(), retentionDeadSandbox("restart-dead", 10)); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	st, err = Open(path, box)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	candidates, err := st.TerminalBuildsForRetention(context.Background(), 20, 1)
	if err != nil || len(candidates) != 1 {
		t.Fatalf("restart candidates = %+v, %v", candidates, err)
	}
	if deleted, err := st.DeleteTerminalBuildForRetention(context.Background(), candidates[0], 20); err != nil || !deleted {
		t.Fatalf("restart delete = %t, %v", deleted, err)
	}
	dead, err := st.DeadSandboxesForRetention(context.Background(), 20, 1)
	if err != nil || len(dead) != 1 || dead[0].ID != "restart-dead" {
		t.Fatalf("restart dead candidates = %+v, %v", dead, err)
	}
	if deleted, err := st.DeleteDeadSandboxForRetention(context.Background(), dead[0], 20); err != nil || !deleted {
		t.Fatalf("restart dead delete = %t, %v", deleted, err)
	}
}

func TestConcurrentBuildTerminalCommitAndRetention(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	for index := 0; index < 16; index++ {
		build := buildTriggerFixture("terminal-race-"+leftPadDecimal(index, 2), types.BuildBuilding)
		build.ExecutionClaimed = true
		build.ExecutionClaimedUnix = 1
		if err := st.PutBuild(ctx, build); err != nil {
			t.Fatal(err)
		}
		terminal := *build
		terminal.Status = types.BuildError
		terminal.Reason = "race"
		start := make(chan struct{})
		var wg sync.WaitGroup
		var terminalErr, retentionErr error
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			terminalErr = st.PutBuildTerminal(ctx, &terminal)
		}()
		go func() {
			defer wg.Done()
			<-start
			candidates, err := st.TerminalBuildsForRetention(ctx, time.Now().Add(time.Hour).Unix(), 1)
			if err != nil {
				retentionErr = err
				return
			}
			for _, candidate := range candidates {
				_, retentionErr = st.DeleteTerminalBuildForRetention(ctx, candidate, time.Now().Add(time.Hour).Unix())
			}
		}()
		close(start)
		wg.Wait()
		if terminalErr != nil || retentionErr != nil {
			t.Fatalf("iteration %d terminal=%v retention=%v", index, terminalErr, retentionErr)
		}
		stored, err := st.GetBuild(ctx, build.BuildID)
		if err != nil {
			t.Fatal(err)
		}
		if stored != nil && (stored.Status != types.BuildError || stored.FinishedUnix == 0 || stored.ExecutionClaimed || stored.RunID != "") {
			t.Fatalf("iteration %d retained partial terminal row: %+v", index, stored)
		}
	}
}
