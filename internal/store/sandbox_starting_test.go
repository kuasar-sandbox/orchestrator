package store

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestStartingCASLifecyclePreservesConcurrentFields(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	sb := sandboxInsertFixture("starting-cas", 0)
	sb.State = types.StatePaused
	sb.LaunchMode = ""
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

	changed, err := st.BeginResume(ctx, sb.ID, 200, types.LaunchCold, sb.RunDir, sb.EnvdUDS, sb.CiUDS)
	if err != nil || changed {
		t.Fatalf("BeginResume with cleanup ownership = %v, %v; want CAS miss", changed, err)
	}
	if changed, err := st.ClearPausedRunner(ctx, sb.ID, sb.RunID); err != nil || !changed {
		t.Fatalf("ClearPausedRunner = %v, %v", changed, err)
	}
	if changed, err := st.ClearPausedNetwork(ctx, sb.ID, sb.VswitchPort); err != nil || !changed {
		t.Fatalf("ClearPausedNetwork = %v, %v", changed, err)
	}
	if changed, err := st.BeginResume(ctx, sb.ID, 200, types.LaunchCold, sb.RunDir, sb.EnvdUDS, sb.CiUDS); err != nil || changed {
		t.Fatalf("BeginResume with RunDir ownership = %v, %v; want CAS miss", changed, err)
	}
	if changed, err := st.ClearPausedRunDir(ctx, sb.ID, sb.RunDir, sb.EnvdUDS, sb.CiUDS); err != nil || !changed {
		t.Fatalf("ClearPausedRunDir = %v, %v", changed, err)
	}
	changed, err = st.BeginResume(ctx, sb.ID, 200, types.LaunchCold, sb.RunDir, sb.EnvdUDS, sb.CiUDS)
	if err != nil || !changed {
		t.Fatalf("BeginResume after ownership cleanup = %v, %v", changed, err)
	}
	if changed, err := st.BeginResume(ctx, sb.ID, 201, types.LaunchCold, sb.RunDir, sb.EnvdUDS, sb.CiUDS); err != nil || changed {
		t.Fatalf("second BeginResume = %v, %v; want CAS miss", changed, err)
	}
	got, err := st.Get(ctx, sb.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != types.StateStarting || got.DeadlineUnix != 200 || got.RunID != "" ||
		got.FloatingIP != "" || got.VswitchPort != "" || got.InnerIP != "" || got.PortMAC != "" ||
		got.RunDir != sb.RunDir || got.EnvdUDS != sb.EnvdUDS || got.CiUDS != sb.CiUDS {
		t.Fatalf("accepted resume retained stale ownership: %+v", got)
	}
	if got.ResumeSource != sb.ResumeSource || got.LaunchMode != types.LaunchCold || got.TemplateID != sb.TemplateID ||
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
	if changed, err := st.RollbackStartingPaused(ctx, sb); err != nil || changed {
		t.Fatalf("late rollback after running = %v, %v; want CAS miss", changed, err)
	}
}

func TestExactRunStartingResourcesAndNonSecretTaskIdentity(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	sb := sandboxInsertFixture("restore-exact-run", 0)
	sb.State = types.StateStarting
	sb.LaunchMode = types.LaunchCold
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

func TestResetStartingOwnershipForRecoveryPreservesAcceptedModeAndSource(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	sb := sandboxInsertFixture("recover-cold-resume", 0)
	sb.State = types.StateStarting
	sb.ResumeSource = types.ResumeSource{Kind: types.ResumeSourceSnapshot, Ref: "manifest://" + strings.Repeat("a", 64)}
	sb.LaunchMode = types.LaunchCold
	sb.RunID = "run-current"
	sb.FloatingIP = "192.0.2.20"
	sb.VswitchPort = "port-current"
	sb.InnerIP = "198.51.100.20/31"
	sb.PortMAC = "02:00:00:00:00:20"
	if err := st.InsertSandbox(ctx, sb); err != nil {
		t.Fatal(err)
	}
	if changed, err := st.ResetStartingOwnershipForRecovery(ctx, sb.ID, "run-stale"); err != nil || changed {
		t.Fatalf("stale recovery reset = %t, %v; want CAS miss", changed, err)
	}
	if changed, err := st.ResetStartingOwnershipForRecovery(ctx, sb.ID, sb.RunID); err != nil || !changed {
		t.Fatalf("exact recovery reset = %t, %v", changed, err)
	}
	got, err := st.Get(ctx, sb.ID)
	if err != nil || got == nil {
		t.Fatalf("Get = %+v, %v", got, err)
	}
	if got.State != types.StateStarting || got.ResumeSource != sb.ResumeSource || got.LaunchMode != types.LaunchCold ||
		got.RunID != "" || got.FloatingIP != "" || got.VswitchPort != "" || got.InnerIP != "" || got.PortMAC != "" {
		t.Fatalf("recovery reset changed accepted resume intent: %+v", got)
	}
}

func TestCommitRunningPausedFencesRunnerAndUpdatesResumeSourceAtomically(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	sb := sandboxInsertFixture("pause-cas", 0)
	sb.ResumeSource = types.ResumeSource{Kind: types.ResumeSourceSnapshot, Ref: "snapshot-old"}
	if err := st.InsertSandbox(ctx, sb); err != nil {
		t.Fatal(err)
	}

	staleSource := types.ResumeSource{Kind: types.ResumeSourceSnapshot, Ref: "snapshot-stale"}
	if changed, err := st.CommitRunningPaused(ctx, sb.ID, "run-stale", staleSource); err != nil || changed {
		t.Fatalf("stale CommitRunningPaused = %v, %v; want CAS miss", changed, err)
	}
	got, err := st.Get(ctx, sb.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != types.StateRunning || got.ResumeSource != sb.ResumeSource {
		t.Fatalf("CAS miss partially changed pause: %+v", got)
	}
	if _, err := st.db.Exec(`
		CREATE TRIGGER fail_pause_commit BEFORE UPDATE OF state, resume_source_kind, resume_source_ref ON sandboxes
		BEGIN SELECT RAISE(ABORT, 'forced pause commit failure'); END`); err != nil {
		t.Fatal(err)
	}
	failedSource := types.ResumeSource{Kind: types.ResumeSourceSnapshot, Ref: "snapshot-failed"}
	if changed, err := st.CommitRunningPaused(ctx, sb.ID, sb.RunID, failedSource); err == nil || changed {
		t.Fatalf("failed CommitRunningPaused = %v, %v; want propagated error", changed, err)
	}
	got, err = st.Get(ctx, sb.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != types.StateRunning || got.ResumeSource != sb.ResumeSource {
		t.Fatalf("failed commit partially changed pause: %+v", got)
	}
	if _, err := st.db.Exec(`DROP TRIGGER fail_pause_commit`); err != nil {
		t.Fatal(err)
	}

	currentSource := types.ResumeSource{Kind: types.ResumeSourceSnapshot, Ref: "snapshot-current"}
	if changed, err := st.CommitRunningPaused(ctx, sb.ID, sb.RunID, currentSource); err != nil || !changed {
		t.Fatalf("CommitRunningPaused = %v, %v", changed, err)
	}
	got, err = st.Get(ctx, sb.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != types.StatePaused || got.ResumeSource != currentSource || got.RunID != sb.RunID || got.LaunchMode != "" {
		t.Fatalf("committed pause = %+v", got)
	}
	lateSource := types.ResumeSource{Kind: types.ResumeSourceSnapshot, Ref: "snapshot-late"}
	if changed, err := st.CommitRunningPaused(ctx, sb.ID, sb.RunID, lateSource); err != nil || changed {
		t.Fatalf("late CommitRunningPaused = %v, %v; want CAS miss", changed, err)
	}
}

func TestPausedOwnershipCleanupUsesIndependentExactCAS(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	sb := sandboxInsertFixture("paused-cleanup-cas", 0)
	sb.State = types.StatePaused
	sb.LaunchMode = ""
	if err := st.InsertSandbox(ctx, sb); err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if changed, err := st.ClearPausedRunner(canceled, sb.ID, sb.RunID); err == nil || changed {
		t.Fatalf("failed runner clear = %t, %v", changed, err)
	}
	if changed, err := st.ClearPausedNetwork(canceled, sb.ID, sb.VswitchPort); err == nil || changed {
		t.Fatalf("failed network clear = %t, %v", changed, err)
	}
	if changed, err := st.ClearPausedRunDir(canceled, sb.ID, sb.RunDir, sb.EnvdUDS, sb.CiUDS); err == nil || changed {
		t.Fatalf("failed RunDir clear = %t, %v", changed, err)
	}
	unchanged, err := st.Get(ctx, sb.ID)
	if err != nil || unchanged == nil || unchanged.RunID != sb.RunID || unchanged.VswitchPort != sb.VswitchPort ||
		unchanged.FloatingIP != sb.FloatingIP || unchanged.InnerIP != sb.InnerIP || unchanged.PortMAC != sb.PortMAC ||
		unchanged.RunDir != sb.RunDir || unchanged.EnvdUDS != sb.EnvdUDS || unchanged.CiUDS != sb.CiUDS {
		t.Fatalf("failed cleanup store transition changed ownership: %+v, %v", unchanged, err)
	}

	if changed, err := st.ClearPausedRunner(ctx, sb.ID, "run-stale"); err != nil || changed {
		t.Fatalf("stale runner clear = %t, %v", changed, err)
	}
	if changed, err := st.ClearPausedNetwork(ctx, sb.ID, "port-stale"); err != nil || changed {
		t.Fatalf("stale network clear = %t, %v", changed, err)
	}
	if changed, err := st.ClearPausedRunDir(ctx, sb.ID, "run-dir-stale", sb.EnvdUDS, sb.CiUDS); err != nil || changed {
		t.Fatalf("stale RunDir clear = %t, %v", changed, err)
	}
	if changed, err := st.ClearPausedNetwork(ctx, sb.ID, sb.VswitchPort); err != nil || !changed {
		t.Fatalf("exact network clear = %t, %v", changed, err)
	}
	intermediate, err := st.Get(ctx, sb.ID)
	if err != nil || intermediate == nil {
		t.Fatalf("Get intermediate = %+v, %v", intermediate, err)
	}
	if intermediate.RunID != sb.RunID || intermediate.VswitchPort != "" || intermediate.FloatingIP != "" ||
		intermediate.InnerIP != "" || intermediate.PortMAC != "" || intermediate.ResumeSource != sb.ResumeSource {
		t.Fatalf("network clear changed independent paused state: %+v", intermediate)
	}
	if changed, err := st.ClearPausedRunner(ctx, sb.ID, sb.RunID); err != nil || !changed {
		t.Fatalf("exact runner clear = %t, %v", changed, err)
	}
	if changed, err := st.ClearPausedRunDir(ctx, sb.ID, sb.RunDir, sb.EnvdUDS, sb.CiUDS); err != nil || !changed {
		t.Fatalf("exact RunDir clear = %t, %v", changed, err)
	}
	got, err := st.Get(ctx, sb.ID)
	if err != nil || got == nil || got.State != types.StatePaused || got.RunID != "" ||
		got.RunDir != "" || got.EnvdUDS != "" || got.CiUDS != "" ||
		got.ResumeSource != sb.ResumeSource || got.LaunchMode != "" {
		t.Fatalf("cleaned paused row = %+v, %v", got, err)
	}
	if changed, err := st.ClearPausedRunner(ctx, sb.ID, sb.RunID); err != nil || changed {
		t.Fatalf("duplicate runner clear = %t, %v", changed, err)
	}
}

func TestReplacePausedResumeSourceFencesExactOriginal(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	sb := sandboxInsertFixture("promote-source-cas", 0)
	sb.State = types.StatePaused
	sb.LaunchMode = ""
	sb.RunID, sb.FloatingIP, sb.VswitchPort, sb.InnerIP, sb.PortMAC = "", "", "", "", ""
	if err := st.InsertSandbox(ctx, sb); err != nil {
		t.Fatal(err)
	}
	replacement := types.ResumeSource{Kind: types.ResumeSourceSnapshot, Ref: "manifest://" + strings.Repeat("a", 64)}
	stale := types.ResumeSource{Kind: types.ResumeSourceSnapshot, Ref: "stale.snapshot"}
	if changed, err := st.ReplacePausedResumeSource(ctx, sb.ID, stale, replacement); err != nil || changed {
		t.Fatalf("stale source replacement = %t, %v; want CAS miss", changed, err)
	}
	if changed, err := st.ReplacePausedResumeSource(ctx, sb.ID, sb.ResumeSource, replacement); err != nil || !changed {
		t.Fatalf("exact source replacement = %t, %v", changed, err)
	}
	got, err := st.Get(ctx, sb.ID)
	if err != nil || got == nil || got.State != types.StatePaused || got.ResumeSource != replacement || got.LaunchMode != "" {
		t.Fatalf("promoted paused source = %+v, %v", got, err)
	}
	if changed, err := st.ReplacePausedResumeSource(ctx, sb.ID, sb.ResumeSource, replacement); err != nil || changed {
		t.Fatalf("duplicate stale replacement = %t, %v; want CAS miss", changed, err)
	}
}

func TestStartingRollbackFencesPreAssignmentPostAssignmentAndDelete(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()

	pre := sandboxInsertFixture("rollback-pre", 1)
	pre.State = types.StateStarting
	pre.ResumeSource = types.ResumeSource{}
	pre.LaunchMode = types.LaunchImage
	pre.RunID = ""
	if err := st.InsertSandbox(ctx, pre); err != nil {
		t.Fatal(err)
	}
	stalePre := *pre
	stalePre.BaseDir += "-stale"
	if changed, err := st.RollbackStartingDead(ctx, &stalePre); err != nil || changed {
		t.Fatalf("stale pre-assignment rollback = %v, %v; want exact-owner CAS miss", changed, err)
	}
	if changed, err := st.RollbackStartingDead(ctx, pre); err != nil || !changed {
		t.Fatalf("pre-assignment rollback = %v, %v", changed, err)
	}
	got, err := st.Get(ctx, pre.ID)
	if err != nil || got == nil || got.State != types.StateDead || got.RunID != "" || got.VswitchPort != "" {
		t.Fatalf("pre-assignment rollback row = %+v, %v", got, err)
	}

	post := sandboxInsertFixture("rollback-post", 2)
	post.State = types.StateStarting
	post.LaunchMode = types.LaunchCold
	post.RunID = "run-current"
	if err := st.InsertSandbox(ctx, post); err != nil {
		t.Fatal(err)
	}
	stalePost := *post
	stalePost.RunID = "run-stale"
	if changed, err := st.RollbackStartingPaused(ctx, &stalePost); err != nil || changed {
		t.Fatalf("stale post-assignment rollback = %v, %v; want CAS miss", changed, err)
	}
	if changed, err := st.RollbackStartingPaused(ctx, post); err != nil || !changed {
		t.Fatalf("post-assignment rollback = %v, %v", changed, err)
	}
	got, err = st.Get(ctx, post.ID)
	if err != nil || got == nil || got.State != types.StatePaused || got.RunID != "" || got.VswitchPort != "" ||
		got.RunDir != "" || got.EnvdUDS != "" || got.CiUDS != "" {
		t.Fatalf("post-assignment rollback row = %+v, %v", got, err)
	}

	deleted := sandboxInsertFixture("rollback-deleted", 3)
	deleted.State = types.StateStarting
	deleted.ResumeSource = types.ResumeSource{}
	deleted.LaunchMode = types.LaunchImage
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
		{"dead rollback", func() (bool, error) { return st.RollbackStartingDead(ctx, deleted) }},
		{"paused rollback", func() (bool, error) { return st.RollbackStartingPaused(ctx, deleted) }},
	}
	for _, check := range checks {
		if changed, err := check.fn(); err != nil || changed {
			t.Fatalf("late %s after Delete = %v, %v; want CAS miss", check.name, changed, err)
		}
	}
}

func TestStartingRollbackRejectsWrongLifecycleKind(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()

	fresh := sandboxInsertFixture("rollback-fresh-as-paused", 4)
	fresh.State = types.StateStarting
	fresh.ResumeSource = types.ResumeSource{}
	fresh.LaunchMode = types.LaunchImage
	fresh.RunID = "fresh-run"
	if err := st.InsertSandbox(ctx, fresh); err != nil {
		t.Fatal(err)
	}
	if changed, err := st.RollbackStartingPaused(ctx, fresh); err != nil || changed {
		t.Fatalf("fresh rollback to paused = %v, %v; want lifecycle-kind CAS miss", changed, err)
	}

	resume := sandboxInsertFixture("rollback-resume-as-dead", 5)
	resume.State = types.StateStarting
	resume.LaunchMode = types.LaunchMemory
	resume.RunID = "resume-run"
	if err := st.InsertSandbox(ctx, resume); err != nil {
		t.Fatal(err)
	}
	if changed, err := st.RollbackStartingDead(ctx, resume); err != nil || changed {
		t.Fatalf("resume rollback to dead = %v, %v; want lifecycle-kind CAS miss", changed, err)
	}

	for _, id := range []string{fresh.ID, resume.ID} {
		got, err := st.Get(ctx, id)
		if err != nil || got == nil || got.State != types.StateStarting || got.LaunchMode == "" {
			t.Fatalf("rejected rollback changed %s: %+v, %v", id, got, err)
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
	pre.ResumeSource = types.ResumeSource{}
	pre.LaunchMode = types.LaunchImage
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
		{name: "resume", mutate: func(sb *types.Sandbox) {
			sb.ResumeSource = types.ResumeSource{Kind: types.ResumeSourceSandbox, Ref: "paused.sandbox"}
			sb.LaunchMode = types.LaunchCold
		}},
		{name: "running", mutate: func(sb *types.Sandbox) { sb.State, sb.LaunchMode = types.StateRunning, "" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			sb := sandboxInsertFixture("delete-pre-launch-"+test.name, 2)
			sb.State = types.StateStarting
			sb.ResumeSource = types.ResumeSource{}
			sb.LaunchMode = types.LaunchImage
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

func TestListDefaultHidesInternalStatesButExplicitStateRemainsDiagnostic(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	for i, state := range []types.State{
		types.StateRunning, types.StatePaused, types.StateStarting, types.StateDeleting, types.StateDead,
	} {
		sb := sandboxInsertFixture("list-state-"+string(rune('a'+i)), i)
		sb.State = state
		if state == types.StateStarting {
			sb.LaunchMode = types.LaunchCold
		} else if state == types.StateDead {
			sb.RunDir, sb.BaseDir, sb.RunID, sb.EnvdUDS, sb.CiUDS = "", "", "", "", ""
			sb.FloatingIP, sb.VswitchPort, sb.InnerIP, sb.PortMAC = "", "", "", ""
			sb.ResumeSource = types.ResumeSource{}
		}
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
	for _, state := range []types.State{types.StateStarting, types.StateDeleting, types.StateDead} {
		rows, _, err := st.List(ctx, string(state), "", 10, "")
		if err != nil || len(rows) != 1 || rows[0].State != state {
			t.Fatalf("explicit %s list = %+v, %v", state, rows, err)
		}
	}
}

func TestSandboxDeletingTransitionPreservesExactCleanupOwnership(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	sb := sandboxInsertFixture("delete-durable-owner", 0)
	sb.State = types.StateStarting
	sb.LaunchMode = types.LaunchCold
	if err := st.InsertSandbox(ctx, sb); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.Exec(`
		CREATE TRIGGER fail_begin_sandbox_delete
		BEFORE UPDATE OF state ON sandboxes
		WHEN OLD.id='delete-durable-owner'
		BEGIN SELECT RAISE(ABORT, 'forced delete transition failure'); END`); err != nil {
		t.Fatal(err)
	}
	if changed, err := st.BeginSandboxDelete(ctx, sb); err == nil || changed {
		t.Fatalf("forced BeginSandboxDelete = %t, %v", changed, err)
	}
	unchanged, err := st.Get(ctx, sb.ID)
	if err != nil || unchanged == nil || unchanged.State != sb.State || unchanged.RunID != sb.RunID ||
		unchanged.VswitchPort != sb.VswitchPort || unchanged.RunDir != sb.RunDir || unchanged.BaseDir != sb.BaseDir {
		t.Fatalf("failed delete transition changed exact owner: %+v, %v", unchanged, err)
	}
	if _, err := st.db.Exec(`DROP TRIGGER fail_begin_sandbox_delete`); err != nil {
		t.Fatal(err)
	}

	stale := *sb
	stale.ResumeSource.Ref = "stale-source"
	if changed, err := st.BeginSandboxDelete(ctx, &stale); err != nil || changed {
		t.Fatalf("stale BeginSandboxDelete = %t, %v", changed, err)
	}
	if changed, err := st.BeginSandboxDelete(ctx, sb); err != nil || !changed {
		t.Fatalf("BeginSandboxDelete = %t, %v", changed, err)
	}
	deleting, err := st.Get(ctx, sb.ID)
	if err != nil || deleting == nil {
		t.Fatalf("deleting row = %+v, %v", deleting, err)
	}
	if deleting.State != types.StateDeleting || deleting.LaunchMode != "" ||
		deleting.RunID != sb.RunID || deleting.VswitchPort != sb.VswitchPort ||
		deleting.RunDir != sb.RunDir || deleting.BaseDir != sb.BaseDir || deleting.ResumeSource != sb.ResumeSource {
		t.Fatalf("delete transition lost cleanup ownership: %+v", deleting)
	}
	if changed, err := st.BeginSandboxDelete(ctx, deleting); err != nil || !changed {
		t.Fatalf("repeated BeginSandboxDelete = %t, %v", changed, err)
	}
	staleDeleting := *deleting
	staleDeleting.BaseDir += "-stale"
	if changed, err := st.DeleteFinalizedSandbox(ctx, &staleDeleting); err != nil || changed {
		t.Fatalf("stale DeleteFinalizedSandbox = %t, %v", changed, err)
	}
	if _, err := st.db.Exec(`
		CREATE TRIGGER fail_finalize_sandbox_delete
		BEFORE DELETE ON sandboxes
		WHEN OLD.id='delete-durable-owner'
		BEGIN SELECT RAISE(ABORT, 'forced delete finalization failure'); END`); err != nil {
		t.Fatal(err)
	}
	if changed, err := st.DeleteFinalizedSandbox(ctx, deleting); err == nil || changed {
		t.Fatalf("forced DeleteFinalizedSandbox = %t, %v", changed, err)
	}
	if retained, err := st.Get(ctx, sb.ID); err != nil || retained == nil || retained.State != types.StateDeleting ||
		retained.RunID != sb.RunID || retained.VswitchPort != sb.VswitchPort || retained.RunDir != sb.RunDir || retained.BaseDir != sb.BaseDir {
		t.Fatalf("failed hard delete lost exact owner: %+v, %v", retained, err)
	}
	if _, err := st.db.Exec(`DROP TRIGGER fail_finalize_sandbox_delete`); err != nil {
		t.Fatal(err)
	}
	if changed, err := st.DeleteFinalizedSandbox(ctx, deleting); err != nil || !changed {
		t.Fatalf("DeleteFinalizedSandbox = %t, %v", changed, err)
	}
	if got, err := st.Get(ctx, sb.ID); err != nil || got != nil {
		t.Fatalf("hard-deleted row = %+v, %v", got, err)
	}
}

func TestSandboxDeleteTransitionFailureLeavesOriginalOwner(t *testing.T) {
	st := testStore(t)
	sb := sandboxInsertFixture("delete-transition-failure", 0)
	if err := st.InsertSandbox(context.Background(), sb); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if changed, err := st.BeginSandboxDelete(ctx, sb); err == nil || changed {
		t.Fatalf("canceled BeginSandboxDelete = %t, %v", changed, err)
	}
	got, err := st.Get(context.Background(), sb.ID)
	if err != nil || got == nil || got.State != types.StateRunning || got.RunID != sb.RunID ||
		got.VswitchPort != sb.VswitchPort || got.RunDir != sb.RunDir || got.BaseDir != sb.BaseDir {
		t.Fatalf("failed delete transition changed owner: %+v, %v", got, err)
	}
}

func TestCommitSandboxDeadAtomicallyClearsAllLocalOwnership(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	sb := sandboxInsertFixture("dead-cleanup-commit", 0)
	if err := st.InsertSandbox(ctx, sb); err != nil {
		t.Fatal(err)
	}
	stale := *sb
	stale.VswitchPort = "stale-port"
	if changed, err := st.CommitSandboxDead(ctx, &stale); err != nil || changed {
		t.Fatalf("stale CommitSandboxDead = %t, %v", changed, err)
	}
	if changed, err := st.CommitSandboxDead(ctx, sb); err != nil || !changed {
		t.Fatalf("CommitSandboxDead = %t, %v", changed, err)
	}
	dead, err := st.Get(ctx, sb.ID)
	if err != nil || dead == nil || dead.State != types.StateDead {
		t.Fatalf("dead row = %+v, %v", dead, err)
	}
	if dead.RunID != "" || dead.VswitchPort != "" || dead.FloatingIP != "" || dead.InnerIP != "" || dead.PortMAC != "" ||
		dead.RunDir != "" || dead.BaseDir != "" || dead.EnvdUDS != "" || dead.CiUDS != "" || !dead.ResumeSource.Empty() {
		t.Fatalf("dead row retained local ownership: %+v", dead)
	}
}

func TestDeadSandboxRejectsCleanupOwnership(t *testing.T) {
	st := testStore(t)
	for _, field := range []string{"runner", "port", "run-dir", "base-dir", "artifact"} {
		t.Run(field, func(t *testing.T) {
			sb := sandboxInsertFixture("dead-owner-"+field, 2)
			sb.State = types.StateDead
			sb.LaunchMode = ""
			sb.RunDir, sb.BaseDir, sb.RunID, sb.EnvdUDS, sb.CiUDS = "", "", "", "", ""
			sb.FloatingIP, sb.VswitchPort, sb.InnerIP, sb.PortMAC = "", "", "", ""
			sb.ResumeSource = types.ResumeSource{}
			switch field {
			case "runner":
				sb.RunID = "owned-run"
			case "port":
				sb.VswitchPort = "owned-port"
			case "run-dir":
				sb.RunDir = "/owned/run"
			case "base-dir":
				sb.BaseDir = "/owned/base"
			case "artifact":
				sb.ResumeSource = types.ResumeSource{Kind: types.ResumeSourceSnapshot, Ref: "owned.snapshot"}
			}
			if err := st.InsertSandbox(context.Background(), sb); err == nil {
				t.Fatal("dead sandbox accepted cleanup ownership")
			}
		})
	}
}
