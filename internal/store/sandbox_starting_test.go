package store

import (
	"context"
	"reflect"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestStartingCASLifecyclePreservesConcurrentFields(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	sb := sandboxInsertFixture("starting-cas", 0)
	sb.State = types.StatePaused
	sb.DeadlineUnix = 100
	sb.RunID = "stale-run"
	sb.FloatingIP = "192.0.2.10"
	sb.VswitchPort = "stale-port"
	sb.InnerIP = "198.51.100.10/31"
	sb.PortMAC = "02:00:00:00:00:10"
	wantMetadata := map[string]string{"candidate": "0", "metadata": "value-0"}
	wantEnv := map[string]string{"CANDIDATE": "0", "ENV": "value-0"}
	if err := st.InsertSandbox(ctx, sb); err != nil {
		t.Fatal(err)
	}

	changed, err := st.BeginResume(ctx, sb.ID, 200)
	if err != nil || !changed {
		t.Fatalf("BeginResume = %v, %v", changed, err)
	}
	if changed, err := st.BeginResume(ctx, sb.ID, 201); err != nil || changed {
		t.Fatalf("second BeginResume = %v, %v; want CAS miss", changed, err)
	}
	got, err := st.Get(ctx, sb.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != types.StateStarting || got.DeadlineUnix != 200 || got.RunID != "" ||
		got.FloatingIP != "" || got.VswitchPort != "" || got.InnerIP != "" || got.PortMAC != "" {
		t.Fatalf("accepted resume retained stale ownership: %+v", got)
	}
	if got.SnapshotRef != sb.SnapshotRef || got.TemplateID != sb.TemplateID ||
		got.APISecret != sb.APISecret || got.ManifestKey != sb.ManifestKey ||
		!reflect.DeepEqual(got.Metadata, wantMetadata) || !reflect.DeepEqual(got.Env, wantEnv) {
		t.Fatalf("accepted resume changed immutable/portable fields: %+v", got)
	}

	resources := StartingResources{
		FloatingIP: "192.0.2.20", VswitchPort: "port-current",
		InnerIP: "198.51.100.20/31", PortMAC: "02:00:00:00:00:20",
	}
	if changed, err := st.SetStartingResources(ctx, sb.ID, resources); err != nil || !changed {
		t.Fatalf("SetStartingResources = %v, %v", changed, err)
	}
	if err := st.SetDeadline(ctx, sb.ID, 300); err != nil {
		t.Fatal(err)
	}
	if changed, err := st.BindStartingRunner(ctx, sb.ID, "run-current"); err != nil || !changed {
		t.Fatalf("BindStartingRunner = %v, %v", changed, err)
	}
	if changed, err := st.BindStartingRunner(ctx, sb.ID, "run-second"); err != nil || changed {
		t.Fatalf("second BindStartingRunner = %v, %v; want CAS miss", changed, err)
	}
	if changed, err := st.SetStartingResources(ctx, sb.ID, StartingResources{VswitchPort: "late-port"}); err != nil || changed {
		t.Fatalf("late SetStartingResources = %v, %v; want CAS miss", changed, err)
	}
	if changed, err := st.CommitStartingRunning(ctx, sb.ID, ""); err == nil || changed {
		t.Fatalf("empty CommitStartingRunning = %v, %v; want validation error", changed, err)
	}
	if changed, err := st.CommitStartingRunning(ctx, sb.ID, "run-stale"); err != nil || changed {
		t.Fatalf("stale CommitStartingRunning = %v, %v; want CAS miss", changed, err)
	}
	if changed, err := st.CommitStartingRunning(ctx, sb.ID, "run-current"); err != nil || !changed {
		t.Fatalf("CommitStartingRunning = %v, %v", changed, err)
	}
	got, err = st.Get(ctx, sb.ID)
	if err != nil || got == nil || got.State != types.StateRunning || got.RunID != "run-current" ||
		got.DeadlineUnix != 300 || got.VswitchPort != resources.VswitchPort {
		t.Fatalf("running row lost concurrent deadline/resources: %+v, %v", got, err)
	}
	if changed, err := st.RollbackStartingPaused(ctx, sb.ID, "run-current"); err != nil || changed {
		t.Fatalf("late rollback after running = %v, %v; want CAS miss", changed, err)
	}
}

func TestExactRunStartingResourcesAndNonSecretTaskIdentity(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	sb := sandboxInsertFixture("restore-exact-run", 0)
	sb.State = types.StateStarting
	sb.RunID = ""
	sb.FloatingIP, sb.VswitchPort, sb.InnerIP, sb.PortMAC = "", "", "", ""
	if err := st.InsertSandbox(ctx, sb); err != nil {
		t.Fatal(err)
	}
	if changed, err := st.BindStartingRunner(ctx, sb.ID, "run-current"); err != nil || !changed {
		t.Fatalf("BindStartingRunner = %t, %v", changed, err)
	}
	if runDir, found, err := st.StartingTaskIdentity(ctx, sb.ID, "run-stale"); err != nil || found || runDir != "" {
		t.Fatalf("stale StartingTaskIdentity = %q, %t, %v", runDir, found, err)
	}
	if runDir, found, err := st.StartingTaskIdentity(ctx, sb.ID, "run-current"); err != nil || !found || runDir != sb.RunDir {
		t.Fatalf("exact StartingTaskIdentity = %q, %t, %v", runDir, found, err)
	}
	resources := StartingResources{FloatingIP: "192.0.2.3", VswitchPort: "port-1", InnerIP: "198.51.100.2/31", PortMAC: "02:00:00:00:00:03"}
	if changed, err := st.SetStartingResourcesForRun(ctx, sb.ID, "run-stale", resources); err != nil || changed {
		t.Fatalf("stale SetStartingResourcesForRun = %t, %v", changed, err)
	}
	if changed, err := st.SetStartingResourcesForRun(ctx, sb.ID, "run-current", resources); err != nil || !changed {
		t.Fatalf("exact SetStartingResourcesForRun = %t, %v", changed, err)
	}
	if changed, err := st.SetStartingResourcesForRun(ctx, sb.ID, "run-current", StartingResources{VswitchPort: "port-2"}); err != nil || changed {
		t.Fatalf("duplicate SetStartingResourcesForRun = %t, %v", changed, err)
	}
}

func TestCommitRunningPausedFencesRunnerAndUpdatesSnapshotAtomically(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	sb := sandboxInsertFixture("pause-cas", 0)
	sb.SnapshotRef = "snapshot-old"
	if err := st.InsertSandbox(ctx, sb); err != nil {
		t.Fatal(err)
	}

	if changed, err := st.CommitRunningPaused(ctx, sb.ID, "run-stale", "snapshot-stale"); err != nil || changed {
		t.Fatalf("stale CommitRunningPaused = %v, %v; want CAS miss", changed, err)
	}
	got, err := st.Get(ctx, sb.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != types.StateRunning || got.SnapshotRef != "snapshot-old" {
		t.Fatalf("CAS miss partially changed pause: %+v", got)
	}
	if _, err := st.db.Exec(`
		CREATE TRIGGER fail_pause_commit BEFORE UPDATE OF state, snapshot_ref ON sandboxes
		BEGIN SELECT RAISE(ABORT, 'forced pause commit failure'); END`); err != nil {
		t.Fatal(err)
	}
	if changed, err := st.CommitRunningPaused(ctx, sb.ID, sb.RunID, "snapshot-failed"); err == nil || changed {
		t.Fatalf("failed CommitRunningPaused = %v, %v; want propagated error", changed, err)
	}
	got, err = st.Get(ctx, sb.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != types.StateRunning || got.SnapshotRef != "snapshot-old" {
		t.Fatalf("failed commit partially changed pause: %+v", got)
	}
	if _, err := st.db.Exec(`DROP TRIGGER fail_pause_commit`); err != nil {
		t.Fatal(err)
	}

	if changed, err := st.CommitRunningPaused(ctx, sb.ID, sb.RunID, "snapshot-current"); err != nil || !changed {
		t.Fatalf("CommitRunningPaused = %v, %v", changed, err)
	}
	got, err = st.Get(ctx, sb.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != types.StatePaused || got.SnapshotRef != "snapshot-current" || got.RunID != sb.RunID {
		t.Fatalf("committed pause = %+v", got)
	}
	if changed, err := st.CommitRunningPaused(ctx, sb.ID, sb.RunID, "snapshot-late"); err != nil || changed {
		t.Fatalf("late CommitRunningPaused = %v, %v; want CAS miss", changed, err)
	}
}

func TestStartingRollbackFencesPreAssignmentPostAssignmentAndDelete(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()

	pre := sandboxInsertFixture("rollback-pre", 1)
	pre.State = types.StateStarting
	pre.RunID = ""
	if err := st.InsertSandbox(ctx, pre); err != nil {
		t.Fatal(err)
	}
	if changed, err := st.RollbackStartingDead(ctx, pre.ID, ""); err != nil || !changed {
		t.Fatalf("pre-assignment rollback = %v, %v", changed, err)
	}
	got, err := st.Get(ctx, pre.ID)
	if err != nil || got == nil || got.State != types.StateDead || got.RunID != "" || got.VswitchPort != "" {
		t.Fatalf("pre-assignment rollback row = %+v, %v", got, err)
	}

	post := sandboxInsertFixture("rollback-post", 2)
	post.State = types.StateStarting
	post.RunID = "run-current"
	if err := st.InsertSandbox(ctx, post); err != nil {
		t.Fatal(err)
	}
	if changed, err := st.RollbackStartingPaused(ctx, post.ID, "run-stale"); err != nil || changed {
		t.Fatalf("stale post-assignment rollback = %v, %v; want CAS miss", changed, err)
	}
	if changed, err := st.RollbackStartingPaused(ctx, post.ID, "run-current"); err != nil || !changed {
		t.Fatalf("post-assignment rollback = %v, %v", changed, err)
	}
	got, err = st.Get(ctx, post.ID)
	if err != nil || got == nil || got.State != types.StatePaused || got.RunID != "" || got.VswitchPort != "" {
		t.Fatalf("post-assignment rollback row = %+v, %v", got, err)
	}

	deleted := sandboxInsertFixture("rollback-deleted", 3)
	deleted.State = types.StateStarting
	deleted.RunID = ""
	if err := st.InsertSandbox(ctx, deleted); err != nil {
		t.Fatal(err)
	}
	if err := st.Delete(ctx, deleted.ID); err != nil {
		t.Fatal(err)
	}
	checks := []struct {
		name string
		fn   func() (bool, error)
	}{
		{"resources", func() (bool, error) {
			return st.SetStartingResources(ctx, deleted.ID, StartingResources{VswitchPort: "late"})
		}},
		{"bind", func() (bool, error) { return st.BindStartingRunner(ctx, deleted.ID, "late-run") }},
		{"commit", func() (bool, error) { return st.CommitStartingRunning(ctx, deleted.ID, "late-run") }},
		{"dead rollback", func() (bool, error) { return st.RollbackStartingDead(ctx, deleted.ID, "") }},
		{"paused rollback", func() (bool, error) { return st.RollbackStartingPaused(ctx, deleted.ID, "") }},
	}
	for _, check := range checks {
		if changed, err := check.fn(); err != nil || changed {
			t.Fatalf("late %s after Delete = %v, %v; want CAS miss", check.name, changed, err)
		}
	}
}

func TestDeletePreLaunchStartingRequiresEmptyOwnership(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	clearOwnership := func(sb *types.Sandbox) {
		sb.RunID = ""
		sb.FloatingIP = ""
		sb.VswitchPort = ""
		sb.InnerIP = ""
		sb.PortMAC = ""
	}
	pre := sandboxInsertFixture("delete-pre-launch", 1)
	pre.State = types.StateStarting
	clearOwnership(pre)
	if err := st.InsertSandbox(ctx, pre); err != nil {
		t.Fatal(err)
	}
	if changed, err := st.DeletePreLaunchStarting(ctx, pre.ID); err != nil || !changed {
		t.Fatalf("DeletePreLaunchStarting = %v, %v", changed, err)
	}
	if got, err := st.Get(ctx, pre.ID); err != nil || got != nil {
		t.Fatalf("deleted pre-launch row = %+v, %v", got, err)
	}

	for _, test := range []struct {
		name   string
		mutate func(*types.Sandbox)
	}{
		{name: "runner", mutate: func(sb *types.Sandbox) { sb.RunID = "run-1" }},
		{name: "network", mutate: func(sb *types.Sandbox) { sb.VswitchPort = "port-1" }},
		{name: "running", mutate: func(sb *types.Sandbox) { sb.State = types.StateRunning }},
	} {
		t.Run(test.name, func(t *testing.T) {
			sb := sandboxInsertFixture("delete-pre-launch-"+test.name, 2)
			sb.State = types.StateStarting
			clearOwnership(sb)
			test.mutate(sb)
			if err := st.InsertSandbox(ctx, sb); err != nil {
				t.Fatal(err)
			}
			if changed, err := st.DeletePreLaunchStarting(ctx, sb.ID); err != nil || changed {
				t.Fatalf("DeletePreLaunchStarting = %v, %v", changed, err)
			}
			if got, err := st.Get(ctx, sb.ID); err != nil || got == nil {
				t.Fatalf("ownership row removed = %+v, %v", got, err)
			}
		})
	}
}

func TestListDefaultHidesStartingAndDeadButExplicitStateRemainsDiagnostic(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	for i, state := range []types.State{types.StateRunning, types.StatePaused, types.StateStarting, types.StateDead} {
		sb := sandboxInsertFixture("list-state-"+string(rune('a'+i)), i)
		sb.State = state
		if err := st.InsertSandbox(ctx, sb); err != nil {
			t.Fatal(err)
		}
	}
	rows, _, err := st.List(ctx, "", "", 10, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0].State != types.StateRunning || rows[1].State != types.StatePaused {
		t.Fatalf("default list states = %+v", rows)
	}
	for _, state := range []types.State{types.StateStarting, types.StateDead} {
		rows, _, err := st.List(ctx, string(state), "", 10, "")
		if err != nil || len(rows) != 1 || rows[0].State != state {
			t.Fatalf("explicit %s list = %+v, %v", state, rows, err)
		}
	}
}
