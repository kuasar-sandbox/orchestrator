package orch

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	conductorextension "github.com/kuasar-sandbox/orchestrator/app/conductor/extension"
	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/nodectl"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"golang.org/x/sys/unix"
)

func attachPressure(t *testing.T, o *Orchestrator) *nodectl.State {
	t.Helper()
	s := nodectl.NewState(4<<30, 4000, 0, 0, nodectl.Watermarks{LowFactor: .7, HighFactor: .85, EmergencyFactor: .05, StartupFactor: .5})
	a := nodectl.NewAdmissionController(nodectl.AdmissionPolicy{Rate: 100, Burst: 100, QueueTTL: time.Minute, QueueMaxDepth: 32})
	if err := o.ConfigureMemoryPressure(context.Background(), s, a, nodectl.DefaultPressurePolicy()); err != nil {
		t.Fatal(err)
	}
	return s
}

func resourcePausedFixture(t *testing.T) (*Orchestrator, *types.Sandbox, string, *nodectl.State, string) {
	t.Helper()
	o, sb, key, _, _, args := newCheckpointPauseFixture(t, checkpointOrchestratorConfig(t, config.CheckpointLocal), "")
	sb.State = types.StatePaused
	sb.PauseReason = types.PauseReasonResource
	sb.ResourceObligation = true
	sb.PressureVersion = 7
	sb.PressureSinceUnixNano = time.Now().Add(-time.Minute).UnixNano()
	sb.ResumeSource = types.ResumeSource{Kind: types.ResumeSourceSnapshot, Ref: checkpointSnapshotRef, SandboxRef: checkpointSandboxRef}
	if err := o.st.Put(context.Background(), sb); err != nil {
		t.Fatal(err)
	}
	o.cache(sb)
	s := attachPressure(t, o)
	sb, err := o.st.Get(context.Background(), sb.ID)
	if err != nil {
		t.Fatal(err)
	}
	return o, sb, key, s, args
}

func TestResourcePauseAdoptionPreservesIdentityAndPublishesAllSubscribers(t *testing.T) {
	o, sb, key, s, args := resourcePausedFixture(t)
	_, _, err := s.Admit(nodectl.AdmitSpec{SandboxID: sb.ID, Token: "old-run", Capacity: nodectl.Resources{MemoryBytes: 1 << 30}, InitialBudget: 512 << 20})
	if err != nil {
		t.Fatal(err)
	}
	first, unsub1 := o.Subscribe()
	defer unsub1()
	second, unsub2 := o.Subscribe()
	defer unsub2()
	if err := o.Pause(context.Background(), sb.ID, key, orchSnapshotCapture(sandboxcfg.SnapshotPolicy{})); err != nil {
		t.Fatal(err)
	}
	got, err := o.st.Get(context.Background(), sb.ID)
	if err != nil {
		t.Fatal(err)
	}
	want := cloneSandbox(sb)
	want.ResourceObligation = false
	want.PauseReason = types.PauseReasonExplicit
	want.PressureVersion++
	if !reflect.DeepEqual(got, want) {
		t.Fatal("adoption changed fields beyond reason, obligation and version")
	}
	for _, events := range []<-chan routesync.Event{first, second} {
		select {
		case e := <-events:
			if e.Kind != routesync.TypeUpsert || e.Route.State != string(types.StatePaused) || e.Route.PauseReason != types.PauseReasonExplicit || e.Route.ResourceObligation || e.Route.PressureVersion != 8 {
				t.Fatalf("same-state adoption event=%+v", e)
			}
		default:
			t.Fatal("subscriber missed same-state adoption")
		}
	}
	if _, err := os.Stat(args); !os.IsNotExist(err) {
		t.Fatalf("adoption captured again: %v", err)
	}
	if p := s.PressureSnapshot(); p.Pending != 0 || p.Cleanup != 1 || p.Zone != nodectl.ZoneCritical || p.ReservedMemory != 512<<20 {
		t.Fatalf("adoption released old ownership: %+v", p)
	}
	s.ObserveObligation(sb.ID, 7, "paused", false)
	if p := s.PressureSnapshot(); p.Pending != 0 {
		t.Fatalf("stale event resurrected Q: %+v", p)
	}
	if err := o.Pause(context.Background(), sb.ID, key, orchSnapshotCapture(sandboxcfg.SnapshotPolicy{})); !errors.Is(err, api.ErrAlreadyPaused) {
		t.Fatalf("ordinary repeat=%v", err)
	}
	// Reload from the same durable rows before service/admission. Historical
	// resource pressure keeps cleanup charged but cannot restore an obligation.
	restarted := attachPressure(t, o)
	if p := restarted.PressureSnapshot(); p.Pending != 0 || p.Cleanup != 1 || p.Zone != nodectl.ZoneCritical {
		t.Fatalf("restart changed adoption: %+v", p)
	}
	got.LaunchMode = types.LaunchMemory
	if a := launchAdmission(got); a.Operation != nodectl.OperationResume || !a.SavedSource {
		t.Fatalf("adoption lost source capability or kept exemption: %+v", a)
	}
	if err := o.checkResourceLaunch(got); !errors.Is(err, api.ErrResourceUnavailable) {
		t.Fatalf("adopted snapshot bypassed critical: %v", err)
	}
}

func TestResourcePauseAdoptionRejectsUnauthenticatedAndCaptureSpecificRequests(t *testing.T) {
	for _, tc := range []struct {
		name, key string
		request   sandboxcfg.CaptureRequest
		want      error
	}{
		{"wrong-owner", "wrong", orchSnapshotCapture(sandboxcfg.SnapshotPolicy{}), api.ErrNotFound},
		{"disk-capture", "", sandboxcfg.CaptureRequest{Kind: types.CaptureSandbox}, api.ErrBadRequest},
		{"explicit-action", "", orchSnapshotCapture(sandboxcfg.SnapshotPolicy{DropCaches: orchCheckpointBool(true)}), api.ErrBadRequest},
		{"explicit-false", "", orchSnapshotCapture(sandboxcfg.SnapshotPolicy{MergeRef: orchCheckpointBool(false)}), api.ErrBadRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o, sb, key, s, _ := resourcePausedFixture(t)
			if tc.key != "" {
				key = tc.key
			}
			if err := o.Pause(context.Background(), sb.ID, key, tc.request); !errors.Is(err, tc.want) {
				t.Fatalf("Pause=%v want %v", err, tc.want)
			}
			got, _ := o.st.Get(context.Background(), sb.ID)
			if !reflect.DeepEqual(got, sb) || s.PressureSnapshot().Pending != 1 {
				t.Fatal("rejected adoption changed obligation")
			}
		})
	}
}

func TestResourcePauseAdoptionHookRevalidatesReasonAndSource(t *testing.T) {
	for _, change := range []string{"reason", "source", "reject"} {
		t.Run(change, func(t *testing.T) {
			o, sb, key, s, _ := resourcePausedFixture(t)
			o.SetExtensionHooks(sandboxHookFunc(func(ctx context.Context, operation *conductorextension.SandboxOperation) error {
				unlock := o.lifecycle.Lock(sb.ID)
				defer unlock() // Hook must run with the fence dropped.
				if change == "reject" {
					return fmtRejected("test policy")
				}
				current, err := o.st.Get(ctx, sb.ID)
				if err != nil {
					return err
				}
				if change == "reason" {
					current.ResourceObligation = false
					current.PauseReason = types.PauseReasonExplicit
				} else {
					current.ResumeSource.Ref = "file://other.snapshot@digest:" + strings.Repeat("c", 64)
				}
				current.PressureVersion++
				if err := o.st.Put(ctx, current); err != nil {
					return err
				}
				o.cache(current)
				o.observePressureSandbox(current)
				return nil
			}), nil)
			err := o.Pause(context.Background(), sb.ID, key, orchSnapshotCapture(sandboxcfg.SnapshotPolicy{}))
			want := api.ErrSandboxChanged
			if change == "reject" {
				want = conductorextension.ErrRejected
			}
			if !errors.Is(err, want) {
				t.Fatalf("stale/rejected hook=%v", err)
			}
			if change != "reason" && s.PressureSnapshot().Pending != 1 {
				t.Fatal("hook discharged current obligation")
			}
		})
	}
}

func TestNodeCreateAdmissionIsSourceIndependent(t *testing.T) {
	for _, zone := range []nodectl.Zone{nodectl.ZoneGreen, nodectl.ZoneYellow, nodectl.ZoneRed, nodectl.ZoneCritical} {
		for _, origin := range []string{"direct", "command"} {
			t.Run(string(zone)+"/"+origin, func(t *testing.T) {
				lc := &countingLauncher{}
				o, ctx := newAsyncConnectTestOrchestrator(t, &config.Config{}, lc)
				s := attachPressure(t, o)
				if err := s.ConfigurePressure(nodectl.DefaultPressurePolicy(), nodectl.PressureRecord{Zone: zone, Version: 1}); err != nil {
					t.Fatal(err)
				}
				allowed := zone == nodectl.ZoneGreen || zone == nodectl.ZoneYellow
				var id string
				if origin == "direct" {
					created, err := o.Create(ctx, createRequestFixture(t, o, "3"))
					if allowed {
						if err != nil {
							t.Fatal(err)
						}
						id = created.ID
					} else if !errors.Is(err, api.ErrResourceUnavailable) {
						t.Fatalf("create rejection=%v", err)
					}
				} else {
					_, _, fp := allowlistedBuildIdentity(t, o)
					cmd := clusterCreateCommand(fp, "pressure-create")
					ack := o.HandleCommand(ctx, cmd)
					if allowed {
						if ack.Status != routesync.AckAccepted {
							t.Fatalf("create ack=%+v", ack)
						}
						id = cmd.SID
					} else if ack.Status != routesync.AckRejected || ack.HTTPStatus != 503 {
						t.Fatalf("create rejection=%+v", ack)
					}
				}
				if allowed {
					waitForSandbox(t, o, ctx, id, func(sb *types.Sandbox) bool { return sb.State == types.StateRunning }, "source-independent create")
				} else if lc.starts.Load() != 0 {
					t.Fatal("rejected create started runner")
				}
			})
		}
	}
}

func TestPressureLaunchClassificationIsLifecycleOwned(t *testing.T) {
	source := types.ResumeSource{Kind: types.ResumeSourceSnapshot, Ref: checkpointSnapshotRef, SandboxRef: checkpointSandboxRef}
	for _, tc := range []struct {
		name       string
		source     types.ResumeSource
		mode       types.LaunchMode
		obligation bool
		want       nodectl.Operation
		saved      bool
	}{
		{"snapshot-template-create", types.ResumeSource{}, types.LaunchMemory, false, nodectl.OperationCreate, false},
		{"ordinary-memory", source, types.LaunchMemory, false, nodectl.OperationResume, true},
		{"resource-memory", source, types.LaunchMemory, true, nodectl.OperationRecovery, true},
		{"explicit-cold", source, types.LaunchCold, true, nodectl.OperationResume, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sb := &types.Sandbox{ResumeSource: tc.source, LaunchMode: tc.mode, ResourceObligation: tc.obligation, PauseReason: types.PauseReasonResource}
			a := launchAdmission(sb)
			if a.Operation != tc.want || a.SavedSource != tc.saved {
				t.Fatalf("classification=%+v", a)
			}
		})
	}
}

func TestPressureSelectionUsesContinuousAgeAndSoftProtection(t *testing.T) {
	now := time.Now()
	rows := []*types.Sandbox{
		{ID: "old-created-new-run", CreatedUnix: 1, State: types.StateRunning, RunningSinceUnixNano: now.Add(-time.Second).UnixNano()},
		{ID: "longest-run", CreatedUnix: 99, State: types.StateRunning, RunningSinceUnixNano: now.Add(-time.Minute).UnixNano()},
		{ID: "starting", State: types.StateStarting, RunningSinceUnixNano: 1},
	}
	eligible := func(string) bool { return true }
	p := nodectl.PressureSnapshot{}
	if got := pressurePauseCandidate(rows, p, now, nil, 30*time.Second, eligible); got == nil || got.ID != "longest-run" {
		t.Fatal("did not choose longest continuous run")
	}
	p.Beneficiary = "longest-run"
	if got := pressurePauseCandidate(rows, p, now, nil, 30*time.Second, eligible); got != nil {
		t.Fatal("ignored beneficiary or soft run window")
	}
	p.Zone = nodectl.ZoneCritical
	p.PauseEligible = true
	if got := pressurePauseCandidate(rows, p, now, nil, 30*time.Second, eligible); got != nil {
		t.Fatal("critical pressure bypassed the minimum run window")
	}
	now = now.Add(30 * time.Second)
	if got := pressurePauseCandidate(rows, p, now, nil, 30*time.Second, eligible); got == nil || got.ID != "old-created-new-run" {
		t.Fatal("eligible candidate remained protected after its run window")
	}
	if got := pressurePauseCandidate(rows, p, now, map[string]time.Time{"old-created-new-run": now.Add(time.Second)}, 0, eligible); got != nil {
		t.Fatal("failed candidate ignored backoff")
	}
}

func pressureCaptureFixture(t *testing.T) (*Orchestrator, *types.Sandbox, *nodectl.State, *countingLauncher, *checkpointVS, string) {
	t.Helper()
	o, sb, _, lc, vs, args := newCheckpointPauseFixture(t, checkpointOrchestratorConfig(t, config.CheckpointLocal), "")
	if err := os.MkdirAll(sb.BaseDir, 0700); err != nil {
		t.Fatal(err)
	}
	s := attachPressure(t, o)
	p := nodectl.DefaultPressurePolicy()
	p.PauseAfterRounds = 1
	if err := s.ConfigurePressure(p, nodectl.PressureRecord{Zone: nodectl.ZoneCritical, Version: 1}); err != nil {
		t.Fatal(err)
	}
	_, _, err := s.Admit(nodectl.AdmitSpec{SandboxID: sb.ID, Token: "live", Capacity: nodectl.Resources{MemoryBytes: 1 << 30}, InitialBudget: 512 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = s.SetSettled("live", 0, time.Now()); err != nil {
		t.Fatal(err)
	}
	// A complete saved-source budget can fit this pool only after the live
	// charge is released. Model its authoritative, eligible admission wait,
	// rather than injecting whole-host availability into the node controller.
	s.RecordAdmissionWait("saved-waiter", nodectl.LaunchAdmission{Operation: nodectl.OperationRecovery, SavedSource: true, Identity: "saved-waiter:1"}, s.AllocatablePool.MemoryBytes)
	if p := s.PressureSnapshot(); !p.PauseEligible || p.Beneficiary != "saved-waiter" || p.ReservedMemory != 512<<20 {
		t.Fatalf("fixture did not establish a real pool-budget wait: %+v", p)
	}
	return o, sb, s, lc, vs, args
}

func TestPressurePauseUsesExistingCaptureAndConfirmedRelease(t *testing.T) {
	o, sb, s, lc, vs, args := pressureCaptureFixture(t)
	started, release := filepath.Join(t.TempDir(), "started"), filepath.Join(t.TempDir(), "release")
	t.Setenv("CHECKPOINT_STARTED_FILE", started)
	t.Setenv("CHECKPOINT_RELEASE_FILE", release)
	defer os.WriteFile(release, nil, 0600)
	done := make(chan error, 1)
	go func() { done <- o.pauseForPressure(context.Background(), sb) }()
	waitForCheckpointFile(t, started)
	// Capture I/O cannot block the global resource lock or claim release.
	if p := s.PressureSnapshot(); p.Capturing != 1 || p.Pending != 1 || p.ReservedMemory != 512<<20 {
		t.Fatalf("in-flight capture=%+v", p)
	}
	if err := os.WriteFile(release, nil, 0600); err != nil {
		t.Fatal(err)
	}
	if err := waitCheckpointResult(t, done); err != nil {
		t.Fatal(err)
	}
	stored, err := o.st.Get(context.Background(), sb.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.State != types.StatePaused || !stored.ResourceObligation || stored.PauseReason != types.PauseReasonResource || !stored.ResumeSource.Valid() || pausedCleanupPending(stored) {
		t.Fatal("current Pause did not commit resource source and clean ownership")
	}
	if lc.stops.Load() != 1 || vs.detaches.Load() != 1 || readCheckpointArgs(t, args)[0] != "snapshot" {
		t.Fatal("resource pause bypassed full snapshot/stop/detach")
	}
	if p := s.PressureSnapshot(); p.Paused != 1 || p.Cleanup != 1 || p.ReservedMemory != 512<<20 {
		t.Fatalf("logical Pause claimed physical release: %+v", p)
	}
	if _, _, err = s.Release("live"); err != nil {
		t.Fatal(err)
	}
	if p := s.PressureSnapshot(); p.Pending != 1 || p.Cleanup != 0 || p.ReservedMemory != 0 || p.Zone != nodectl.ZoneCritical {
		t.Fatalf("confirmed release lost obligation: %+v", p)
	}
}

func TestPressureCaptureFailureKeepsRunningCharge(t *testing.T) {
	o, sb, s, lc, vs, _ := pressureCaptureFixture(t)
	t.Setenv("CHECKPOINT_FAIL", "1")
	if err := o.pauseForPressure(context.Background(), sb); err == nil {
		t.Fatal("failed capture succeeded")
	}
	current, err := o.st.Get(context.Background(), sb.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.State != types.StateRunning || current.ResourceObligation || current.ResumeSource.Valid() || lc.stops.Load() != 0 || vs.detaches.Load() != 0 {
		t.Fatal("capture failure changed ownership")
	}
	if p := s.PressureSnapshot(); p.Pending != 0 || p.ReservedMemory != 512<<20 {
		t.Fatalf("failed capture released resources: %+v", p)
	}
}

func TestPressureCleanupOnlyUpdatesAndTerminalDeleteConverge(t *testing.T) {
	o, sb, _, s, _ := resourcePausedFixture(t)
	sb.ResourceObligation = false
	sb.PauseReason = types.PauseReasonExplicit
	sb.PressureVersion++
	sb.RunID = ""
	sb.VswitchPort = ""
	o.observePressureSandbox(sb)
	if s.PressureSnapshot().Cleanup != 1 {
		t.Fatal("RunDir cleanup owner was lost")
	}
	clean := cloneSandbox(sb)
	clean.RunDir = ""
	o.recordPausedCleanupProgress(clean, "", "", sb.RunDir)
	if s.PressureSnapshot().Cleanup != 0 {
		t.Fatal("RunDir-only cleanup left critical barrier")
	}
	deleted := cloneSandbox(sb)
	deleted.State = types.StateDeleting
	deleted.PressureVersion++
	o.observePressureSandbox(deleted)
	if s.PressureSnapshot().Cleanup != 1 {
		t.Fatal("delete accepted before cleanup cleared barrier")
	}
	o.observeSandboxDelete(deleted)
	if s.PressureSnapshot().Cleanup != 0 {
		t.Fatal("confirmed hard delete retained cleanup owner")
	}
}

// Use another process: F_GETLK does not report locks owned by its caller.
func TestPressureRunnerIdentityHelper(t *testing.T) {
	path := os.Getenv("PRESSURE_TEST_PIDFILE")
	if path == "" {
		return
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	lock := unix.Flock_t{Type: unix.F_WRLCK, Whence: 0, Start: 0, Len: 0}
	if err = unix.FcntlFlock(f.Fd(), unix.F_SETLK, &lock); err != nil {
		t.Fatal(err)
	}
	if _, err = fmt.Fprintln(f, os.Getpid()); err != nil {
		t.Fatal(err)
	}
	fmt.Println("ready")
	_, _ = io.Copy(io.Discard, os.Stdin)
}

func TestResourceLaunchLookupRequiresCurrentLiveRunner(t *testing.T) {
	o, sb, _, _, _ := resourcePausedFixture(t)
	sb.State = types.StateStarting
	sb.LaunchMode = types.LaunchMemory
	if err := o.st.Put(context.Background(), sb); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(sb.RunDir, 0700); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestPressureRunnerIdentityHelper$")
	cmd.Env = append(os.Environ(), "PRESSURE_TEST_PIDFILE="+sb.PidFile())
	in, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { in.Close(); cmd.Wait() }()
	line, err := bufio.NewReader(out).ReadString('\n')
	if err != nil || line != "ready\n" {
		t.Fatalf("runner identity helper: %v", err)
	}
	a, err := o.ResourceLaunchAdmission(sb.ID, cmd.Process.Pid)
	if err != nil || a.Operation != nodectl.OperationRecovery || !a.SavedSource || !a.Accepted {
		t.Fatalf("trusted launch=%+v %v", a, err)
	}
	if _, err = o.ResourceLaunchAdmission(sb.ID, os.Getpid()); err == nil {
		t.Fatal("another process obtained recovery authority")
	}
	in.Close()
	if err = cmd.Wait(); err != nil {
		t.Fatal(err)
	}
	if _, err = o.ResourceLaunchAdmission(sb.ID, cmd.Process.Pid); err == nil {
		t.Fatal("stale pidfile obtained recovery authority")
	}
}

func TestPressureUnattendedRecoveryFailureRetainsSourceAndQ(t *testing.T) {
	cfg := &config.Config{}
	lc := &countingLauncher{artifactPrepareErr: errors.New("saved snapshot unavailable")}
	o, ctx := newAsyncConnectTestOrchestrator(t, cfg, lc)
	sb := &types.Sandbox{ID: "unattended", Profile: types.ProfileBare, TemplateID: types.TemplateID{Profile: types.ProfileBare, Kind: types.KindImg, Ref: "manifest://" + strings.Repeat("d", 64)}.String(), State: types.StatePaused, ResumeSource: types.ResumeSource{Kind: types.ResumeSourceSnapshot, Ref: checkpointSnapshotRef, SandboxRef: checkpointSandboxRef}, ResourceObligation: true, PauseReason: types.PauseReasonResource, PressureVersion: 7, PressureSinceUnixNano: time.Now().UnixNano(), ManifestKey: strings.Repeat("f", 64), APISecret: deriveTestAPISecret(t, strings.Repeat("f", 64)), BaseDir: filepath.Join(cfg.Paths.BaseRoot, "sandboxes", "unattended")}
	materializeTestSandboxCredentials(t, sb)
	if err := o.st.Put(ctx, sb); err != nil {
		t.Fatal(err)
	}
	o.cache(sb)
	s := attachPressure(t, o)
	policy := nodectl.DefaultPressurePolicy()
	policy.Interval = 10 * time.Millisecond
	o.memoryPressure.Load().policy = policy
	if err := s.ConfigurePressure(policy, nodectl.PressureRecord{Zone: nodectl.ZoneCritical, Version: 1}); err != nil {
		t.Fatal(err)
	}
	workerCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { defer close(done); o.RunMemoryPressure(workerCtx) }()
	defer func() { cancel(); <-done }()
	current := waitForSandbox(t, o, ctx, sb.ID, func(current *types.Sandbox) bool {
		return current.State == types.StatePaused && current.PressureVersion > 7
	}, "unattended failed recovery rollback")
	if lc.starts.Load() != 1 || !current.ResourceObligation || current.ResumeSource != sb.ResumeSource || s.PressureSnapshot().Pending != 1 {
		t.Fatal("background retry lost source/obligation or started concurrent runners")
	}
	if current.DeadlineUnix != sb.DeadlineUnix {
		t.Fatal("background recovery changed deadline")
	}
}

func TestPressurePauseRejectsRAMBackedCheckpointMount(t *testing.T) {
	var fs unix.Statfs_t
	if err := unix.Statfs("/dev/shm", &fs); err != nil || fs.Type != unix.TMPFS_MAGIC {
		t.Skip("host has no tmpfs fixture")
	}
	o, sb, s, lc, vs, args := pressureCaptureFixture(t)
	if err := os.Symlink("/dev/shm", filepath.Join(sb.BaseDir, "checkpoint")); err != nil {
		t.Fatal(err)
	}
	if err := o.pauseForPressure(context.Background(), sb); err == nil || !strings.Contains(err.Error(), "disk-backed") {
		t.Fatalf("RAM-backed checkpoint accepted: %v", err)
	}
	if _, err := os.Stat(args); !os.IsNotExist(err) {
		t.Fatal("RAM-backed storage reached capture")
	}
	if lc.stops.Load() != 0 || vs.detaches.Load() != 0 || s.PressureSnapshot().Pending != 0 {
		t.Fatal("storage rejection changed lifecycle")
	}
}

func TestPressureUnattendedRecoveryWithUnrelatedGrants(t *testing.T) {
	cfg := &config.Config{}
	lc := &countingLauncher{artifactPrepareErr: errors.New("saved snapshot unavailable")}
	o, ctx := newAsyncConnectTestOrchestrator(t, cfg, lc)
	sb := &types.Sandbox{ID: "unattended", Profile: types.ProfileBare, TemplateID: types.TemplateID{Profile: types.ProfileBare, Kind: types.KindImg, Ref: "manifest://" + strings.Repeat("d", 64)}.String(), State: types.StatePaused, ResumeSource: types.ResumeSource{Kind: types.ResumeSourceSnapshot, Ref: checkpointSnapshotRef, SandboxRef: checkpointSandboxRef}, ResourceObligation: true, PauseReason: types.PauseReasonResource, PressureVersion: 7, PressureSinceUnixNano: time.Now().UnixNano(), ManifestKey: strings.Repeat("f", 64), APISecret: deriveTestAPISecret(t, strings.Repeat("f", 64)), BaseDir: filepath.Join(cfg.Paths.BaseRoot, "sandboxes", "unattended")}
	materializeTestSandboxCredentials(t, sb)
	if err := o.st.Put(ctx, sb); err != nil {
		t.Fatal(err)
	}
	o.cache(sb)
	s := attachPressure(t, o)
	policy := nodectl.DefaultPressurePolicy()
	policy.Interval = 250 * time.Millisecond
	o.memoryPressure.Load().policy = policy
	if err := s.ConfigurePressure(policy, nodectl.PressureRecord{Zone: nodectl.ZoneCritical, Version: 1}); err != nil {
		t.Fatal(err)
	}
	// Keep ample headroom but change another legitimate consumer's Budget
	// much more often than the pressure scan. This must not block recovery.
	if _, _, err := s.Admit(nodectl.AdmitSpec{SandboxID: "busy", Token: "busy", Capacity: nodectl.Resources{MemoryBytes: 1 << 30}, InitialBudget: 512 << 20}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.SetSettled("busy", 0, time.Now()); err != nil {
		t.Fatal(err)
	}
	allocator := nodectl.NewAllocator(nodectl.AllocatorPolicy{MemoryGrantPerSecBytes: 4 << 30, MinGrantStep: 64 << 20, MaxGrantStep: 64 << 20})
	workerCtx, cancel := context.WithCancel(ctx)
	churnDone, workerDone := make(chan struct{}), make(chan struct{})
	churnErr := make(chan error, 1)
	go func() {
		defer close(churnDone)
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		current := uint64(512 << 20)
		for {
			select {
			case <-workerCtx.Done():
				return
			case <-ticker.C:
			}
			baseline, delta := current, uint64(64<<20)
			if current > 512<<20 {
				baseline, delta = 512<<20, 0
			}
			got, found, err := s.ReconcileAndGrant("busy", baseline, delta, nodectl.UrgencyNormal, allocator)
			if err != nil || !found {
				churnErr <- fmt.Errorf("churn found=%v: %w", found, err)
				return
			}
			current = got.Reservation.ReservationMemory
		}
	}()
	startedAt := time.Now()
	go func() { defer close(workerDone); o.RunMemoryPressure(workerCtx) }()
	defer func() { cancel(); <-workerDone; <-churnDone }()
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for lc.starts.Load() == 0 {
		select {
		case err := <-churnErr:
			t.Fatal(err)
		case <-deadline.C:
			p := s.PressureSnapshot()
			t.Fatalf("background recovery starved by unrelated grants despite more than 3GiB available: starts=%d stable=%v Q=%d R=%d P=%d", lc.starts.Load(), p.HeadroomStable, p.Pending, p.ReservedMemory, p.PoolMemory)
		case <-ticker.C:
		}
	}
	if time.Since(startedAt) < policy.Interval {
		t.Fatal("background recovery skipped its initial pacing interval")
	}
}

func TestPressureRecoveryPacesSuccessiveWaiters(t *testing.T) {
	cfg := &config.Config{}
	started := make(chan struct{}, 4)
	lc := &countingLauncher{artifactPrepareErr: errors.New("saved snapshot unavailable"), started: started}
	o, ctx := newAsyncConnectTestOrchestrator(t, cfg, lc)
	for _, id := range []string{"paced-first", "paced-second"} {
		sb := &types.Sandbox{
			ID: id, Profile: types.ProfileBare,
			TemplateID:         types.TemplateID{Profile: types.ProfileBare, Kind: types.KindImg, Ref: "manifest://" + strings.Repeat("d", 64)}.String(),
			State:              types.StatePaused,
			ResumeSource:       types.ResumeSource{Kind: types.ResumeSourceSnapshot, Ref: checkpointSnapshotRef, SandboxRef: checkpointSandboxRef},
			ResourceObligation: true, PauseReason: types.PauseReasonResource, PressureVersion: 7,
			PressureSinceUnixNano: time.Now().UnixNano(),
			ManifestKey:           strings.Repeat("f", 64), APISecret: deriveTestAPISecret(t, strings.Repeat("f", 64)),
			BaseDir: filepath.Join(cfg.Paths.BaseRoot, "sandboxes", id),
		}
		materializeTestSandboxCredentials(t, sb)
		if err := o.st.Put(ctx, sb); err != nil {
			t.Fatal(err)
		}
		o.cache(sb)
	}
	s := attachPressure(t, o)
	policy := nodectl.DefaultPressurePolicy()
	policy.Interval = 200 * time.Millisecond
	o.memoryPressure.Load().policy = policy
	if err := s.ConfigurePressure(policy, nodectl.PressureRecord{Zone: nodectl.ZoneCritical, Version: 1}); err != nil {
		t.Fatal(err)
	}
	workerCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { defer close(done); o.RunMemoryPressure(workerCtx) }()
	defer func() { cancel(); <-done }()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("first unattended recovery did not start")
	}
	// A completed/failed attempt may wake the loop immediately. It must not
	// release the next waiter without the coordinator's own settling period.
	select {
	case <-started:
		t.Fatal("successive background recoveries were not paced")
	case <-time.After(policy.Interval / 2):
	}
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("next unattended recovery remained blocked")
	}
	if p := s.PressureSnapshot(); p.Pending != 2 || p.Zone != nodectl.ZoneCritical {
		t.Fatalf("failed preparations lost their recovery obligations: %+v", p)
	}
}

func TestPressureBackgroundRecoveryClaimCanSelectDemandOwnerAfterOlderCandidate(t *testing.T) {
	s := nodectl.NewState(4<<30, 4000, 0, 0, nodectl.Watermarks{LowFactor: .7, HighFactor: .85, EmergencyFactor: .05, StartupFactor: .5})
	// The coordinator scans oldest A before B. B's own failed recovery demand
	// must block A but must not prevent reaching and claiming B.
	accepted := nodectl.LaunchAdmission{Operation: nodectl.OperationRecovery, SavedSource: true, Accepted: true, Identity: "filler"}
	if _, _, err := s.Admit(nodectl.AdmitSpec{SandboxID: "filler", Token: "filler", Capacity: nodectl.Resources{MemoryBytes: 4 << 30}, InitialBudget: 3800 << 20, Admission: &accepted}); err != nil {
		t.Fatal(err)
	}
	recoveryB := nodectl.LaunchAdmission{Operation: nodectl.OperationRecovery, SavedSource: true, Accepted: true, Identity: "run-b"}
	s.RecordAdmissionWait("b", recoveryB, 512<<20)
	if s.TryClaimBackgroundRecovery("a") {
		t.Fatal("older A bypassed B's pending recovery demand")
	}
	if !s.TryClaimBackgroundRecovery("b") {
		t.Fatal("coordinator could not continue to demand-owning candidate B")
	}
	s.ReleaseBackgroundRecovery("b")
}
