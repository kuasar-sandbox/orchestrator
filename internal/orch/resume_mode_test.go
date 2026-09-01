package orch

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/keys"
	"github.com/kuasar-sandbox/orchestrator/internal/proxy"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestConnectResumeModeMatrix(t *testing.T) {
	tests := []struct {
		name       string
		sourceKind types.ResumeSourceKind
		memory     *bool
		wantMode   types.LaunchMode
		wantErr    error
	}{
		{name: "snapshot auto", sourceKind: types.ResumeSourceSnapshot, wantMode: types.LaunchMemory},
		{name: "snapshot memory", sourceKind: types.ResumeSourceSnapshot, memory: resumeMemoryOption(true), wantMode: types.LaunchMemory},
		{name: "snapshot cold", sourceKind: types.ResumeSourceSnapshot, memory: resumeMemoryOption(false), wantMode: types.LaunchCold},
		{name: "sandbox auto", sourceKind: types.ResumeSourceSandbox, wantMode: types.LaunchCold},
		{name: "sandbox cold", sourceKind: types.ResumeSourceSandbox, memory: resumeMemoryOption(false), wantMode: types.LaunchCold},
		{name: "sandbox memory unavailable", sourceKind: types.ResumeSourceSandbox, memory: resumeMemoryOption(true), wantErr: types.ErrMemoryUnavailable},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newResumeModeFixture(t, test.sourceKind, true)
			accepted, err := fixture.o.Connect(fixture.ctx, fixture.sb.ID, fixture.apiKey, "", api.ConnectOptions{Memory: test.memory})
			if test.wantErr != nil {
				if !errors.Is(err, test.wantErr) {
					t.Fatalf("Connect error = %v, want %v", err, test.wantErr)
				}
				stored, getErr := fixture.o.st.Get(fixture.ctx, fixture.sb.ID)
				if getErr != nil || stored == nil || stored.State != types.StatePaused ||
					stored.ResumeSource != fixture.sb.ResumeSource || stored.LaunchMode != "" {
					t.Fatalf("rejected resume changed durable state: %+v, %v", stored, getErr)
				}
				if got := fixture.launcher.starts.Load(); got != 0 {
					t.Fatalf("rejected resume launched %d units", got)
				}
				return
			}
			if err != nil || accepted == nil || accepted.State != types.StateStarting || accepted.LaunchMode != test.wantMode {
				t.Fatalf("Connect = %+v, %v; want starting/%s", accepted, err, test.wantMode)
			}
			stored, err := fixture.o.st.Get(fixture.ctx, fixture.sb.ID)
			if err != nil || stored == nil || stored.State != types.StateStarting ||
				stored.LaunchMode != test.wantMode || stored.ResumeSource != fixture.sb.ResumeSource {
				t.Fatalf("durable accepted resume = %+v, %v", stored, err)
			}

			close(fixture.startGate)
			select {
			case got := <-fixture.launchModes:
				if got != string(test.wantMode) {
					t.Fatalf("task launch mode = %q, want %q", got, test.wantMode)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("task did not receive the accepted launch mode")
			}
			running := waitForSandbox(t, fixture.o, fixture.ctx, fixture.sb.ID, func(sb *types.Sandbox) bool {
				return sb.State == types.StateRunning
			}, "running after mode-selected resume")
			if running.LaunchMode != "" || running.ResumeSource != fixture.sb.ResumeSource {
				t.Fatalf("running lifecycle fields = mode %q source %+v", running.LaunchMode, running.ResumeSource)
			}
		})
	}
}

func TestStartingResumeJoinsOnlyCompatibleMode(t *testing.T) {
	fixture := newResumeModeFixture(t, types.ResumeSourceSnapshot, true)
	accepted, err := fixture.o.Connect(fixture.ctx, fixture.sb.ID, fixture.apiKey, "", api.ConnectOptions{Memory: resumeMemoryOption(false)})
	if err != nil || accepted == nil || accepted.State != types.StateStarting || accepted.LaunchMode != types.LaunchCold {
		t.Fatalf("first Connect = %+v, %v", accepted, err)
	}
	waitForLauncherStart(t, fixture.started)

	for _, options := range []api.ConnectOptions{
		{},
		{Memory: resumeMemoryOption(false)},
	} {
		joined, err := fixture.o.Connect(fixture.ctx, fixture.sb.ID, fixture.apiKey, "", options)
		if err != nil || joined == nil || joined.State != types.StateStarting || joined.LaunchMode != types.LaunchCold {
			t.Fatalf("compatible Connect = %+v, %v", joined, err)
		}
	}
	if _, err := fixture.o.Connect(fixture.ctx, fixture.sb.ID, fixture.apiKey, "", api.ConnectOptions{Memory: resumeMemoryOption(true)}); !errors.Is(err, types.ErrLaunchModeConflict) {
		t.Fatalf("conflicting Connect error = %v, want ErrLaunchModeConflict", err)
	}
	stored, err := fixture.o.st.Get(fixture.ctx, fixture.sb.ID)
	if err != nil || stored == nil || stored.State != types.StateStarting || stored.LaunchMode != types.LaunchCold {
		t.Fatalf("conflict changed accepted mode: %+v, %v", stored, err)
	}
	if got := fixture.launcher.starts.Load(); got != 1 {
		t.Fatalf("launcher starts before release = %d, want 1", got)
	}

	close(fixture.startGate)
	waitForSandbox(t, fixture.o, fixture.ctx, fixture.sb.ID, func(sb *types.Sandbox) bool {
		return sb.State == types.StateRunning
	}, "running after compatible joins")
	if got := fixture.launcher.starts.Load(); got != 1 {
		t.Fatalf("launcher starts = %d, want one shared attempt", got)
	}
}

func TestRunningConnectMemoryDoesNotRestart(t *testing.T) {
	cfg := &config.Config{}
	lc := &countingLauncher{}
	o, ctx := newAsyncConnectTestOrchestrator(t, cfg, lc)
	manifestKey := strings.Repeat("a", 64)
	_, apiKey := defaultTestCredentials(t, manifestKey)
	sb := &types.Sandbox{
		ID: "running-memory-option", Profile: types.ProfileBare,
		TemplateID:   types.TemplateID{Profile: types.ProfileBare, Kind: types.KindImg, Ref: "manifest://" + strings.Repeat("b", 64)}.String(),
		State:        types.StateRunning,
		ResumeSource: types.ResumeSource{Kind: types.ResumeSourceSnapshot, Ref: "manifest://" + strings.Repeat("c", 64)},
		APISecret:    deriveTestAPISecret(t, manifestKey), ManifestKey: manifestKey,
		RunDir: filepath.Join(cfg.Paths.RunRoot, "running-memory-option"), BaseDir: filepath.Join(cfg.Paths.BaseRoot, "running-memory-option"),
		CreatedUnix: 1, DeadlineUnix: 100,
	}
	materializeTestSandboxCredentials(t, sb)
	if err := o.st.Put(ctx, sb); err != nil {
		t.Fatal(err)
	}

	connected, err := o.Connect(ctx, sb.ID, apiKey, "", api.ConnectOptions{Memory: resumeMemoryOption(false)})
	if err != nil || connected == nil || connected.State != types.StateRunning {
		t.Fatalf("Connect running = %+v, %v", connected, err)
	}
	stored, err := o.st.Get(ctx, sb.ID)
	if err != nil || stored == nil || stored.State != types.StateRunning || stored.LaunchMode != "" {
		t.Fatalf("running row changed = %+v, %v", stored, err)
	}
	if got := lc.starts.Load(); got != 0 {
		t.Fatalf("running Connect launched %d units", got)
	}
}

func TestSnapshotColdFailureRollsBackOriginalSnapshot(t *testing.T) {
	cfg := &config.Config{}
	connectGate := make(chan struct{})
	launchModes := make(chan string, 1)
	lc := &countingLauncher{
		connectGate:         connectGate,
		artifactLaunchModes: launchModes,
		readinessWire:       []byte("ready\ncontrol_ready\n"),
	}
	o, ctx := newAsyncConnectTestOrchestrator(t, cfg, lc)
	manifestKey := strings.Repeat("d", 64)
	_, apiKey := defaultTestCredentials(t, manifestKey)
	source := types.ResumeSource{Kind: types.ResumeSourceSnapshot, Ref: "manifest://" + strings.Repeat("e", 64)}
	sb := &types.Sandbox{
		ID: "snapshot-cold-failure", Profile: types.ProfileBare,
		TemplateID: types.TemplateID{Profile: types.ProfileBare, Kind: types.KindImg, Ref: "manifest://" + strings.Repeat("f", 64)}.String(),
		State:      types.StatePaused, ResumeSource: source,
		APISecret: deriveTestAPISecret(t, manifestKey), ManifestKey: manifestKey,
		RunDir: filepath.Join(cfg.Paths.RunRoot, "snapshot-cold-failure"), BaseDir: filepath.Join(cfg.Paths.BaseRoot, "snapshot-cold-failure"), CreatedUnix: 1,
	}
	materializeTestSandboxCredentials(t, sb)
	if err := o.st.Put(ctx, sb); err != nil {
		t.Fatal(err)
	}

	accepted, err := o.Connect(ctx, sb.ID, apiKey, "", api.ConnectOptions{Memory: resumeMemoryOption(false)})
	if err != nil || accepted == nil || accepted.State != types.StateStarting || accepted.LaunchMode != types.LaunchCold {
		t.Fatalf("Connect = %+v, %v", accepted, err)
	}
	attempt, found := o.launches.Lookup(sb.ID)
	if !found {
		t.Fatal("accepted resume has no launch owner")
	}
	close(connectGate)
	select {
	case got := <-launchModes:
		if got != string(types.LaunchCold) {
			t.Fatalf("task launch mode = %q, want cold", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("task did not receive cold mode")
	}
	if err := attempt.wait(ctx); err == nil {
		t.Fatal("malformed readiness unexpectedly succeeded")
	}
	paused := waitForSandbox(t, o, ctx, sb.ID, func(current *types.Sandbox) bool {
		return current.State == types.StatePaused
	}, "paused after failed snapshot cold resume")
	if paused.ResumeSource != source || paused.LaunchMode != "" {
		t.Fatalf("failed cold resume rewrote source/mode: %+v", paused)
	}
}

func TestAutoResumeTriggersUseDurableSourceKind(t *testing.T) {
	for _, source := range []struct {
		name string
		kind types.ResumeSourceKind
		want types.LaunchMode
	}{
		{name: "Sandbox E", kind: types.ResumeSourceSandbox, want: types.LaunchCold},
		{name: "Snapshot S", kind: types.ResumeSourceSnapshot, want: types.LaunchMemory},
	} {
		t.Run(source.name, func(t *testing.T) {
			t.Run("external ordinary and exec Wake", func(t *testing.T) {
				fixture := newResumeModeFixture(t, source.kind, true)
				fixture.o.OnWake(fixture.ctx, fixture.sb.ID)
				assertAcceptedAutoLaunch(t, fixture, source.want)
				finishAcceptedAutoLaunch(t, fixture, source.want)
			})

			t.Run("internal ordinary route", func(t *testing.T) {
				fixture := newResumeModeFixture(t, source.kind, true)
				target := proxy.LegacyTarget(8080)
				binding, found, err := fixture.o.LookupRoute(fixture.ctx, fixture.sb.ID, target)
				if err != nil || !found {
					t.Fatalf("LookupRoute = %+v, %v, %v", binding, found, err)
				}
				type result struct {
					route proxy.Route
					found bool
					err   error
				}
				done := make(chan result, 1)
				go func() {
					route, found, err := fixture.o.ActivateRoute(fixture.ctx, binding)
					done <- result{route: route, found: found, err: err}
				}()
				waitForLauncherStart(t, fixture.started)
				assertAcceptedAutoLaunch(t, fixture, source.want)
				close(fixture.startGate)
				assertArtifactLaunchMode(t, fixture.launchModes, source.want)
				select {
				case got := <-done:
					if got.err != nil || !got.found || got.route.Kind != proxy.KindTCP {
						t.Fatalf("ActivateRoute = %+v, %v, %v", got.route, got.found, got.err)
					}
				case <-time.After(2 * time.Second):
					t.Fatal("authorized route did not remain parked through launch")
				}
			})

			t.Run("native exec", func(t *testing.T) {
				fixture := newResumeModeFixture(t, source.kind, true)
				identity, found, err := fixture.o.LookupExec(fixture.ctx, fixture.sb.ID)
				if err != nil || !found {
					t.Fatalf("LookupExec = %+v, %v, %v", identity, found, err)
				}
				type result struct {
					identity proxy.ExecIdentity
					found    bool
					err      error
				}
				done := make(chan result, 1)
				go func() {
					ready, found, err := fixture.o.ActivateExec(fixture.ctx, fixture.sb.ID, identity)
					done <- result{identity: ready, found: found, err: err}
				}()
				waitForLauncherStart(t, fixture.started)
				assertAcceptedAutoLaunch(t, fixture, source.want)
				close(fixture.startGate)
				assertArtifactLaunchMode(t, fixture.launchModes, source.want)
				select {
				case got := <-done:
					if got.err != nil || !got.found || got.identity != identity {
						t.Fatalf("ActivateExec = %+v, %v, %v", got.identity, got.found, got.err)
					}
				case <-time.After(2 * time.Second):
					t.Fatal("authorized exec did not remain parked through launch")
				}
			})

			t.Run("exec session", func(t *testing.T) {
				fixture := newResumeModeFixture(t, source.kind, true)
				token, err := fixture.o.ExecSession(fixture.ctx, fixture.sb.ID, fixture.apiKey, "", 30, nil)
				if err != nil || token == "" {
					t.Fatalf("ExecSession token=%q err=%v", token, err)
				}
				if err := keys.VerifyExecAccessToken(token, fixture.sb.ServiceSecret, fixture.sb.StableID(), time.Now()); err != nil {
					t.Fatalf("ExecSession token: %v", err)
				}
				assertAcceptedAutoLaunch(t, fixture, source.want)
				finishAcceptedAutoLaunch(t, fixture, source.want)
			})
		})
	}
}

func assertAcceptedAutoLaunch(t *testing.T, fixture resumeModeFixture, want types.LaunchMode) {
	t.Helper()
	stored, err := fixture.o.st.Get(fixture.ctx, fixture.sb.ID)
	if err != nil || stored == nil || stored.State != types.StateStarting || stored.LaunchMode != want ||
		stored.ResumeSource != fixture.sb.ResumeSource {
		t.Fatalf("accepted auto resume = %+v, %v; want starting/%s from %+v", stored, err, want, fixture.sb.ResumeSource)
	}
}

func finishAcceptedAutoLaunch(t *testing.T, fixture resumeModeFixture, want types.LaunchMode) {
	t.Helper()
	waitForLauncherStart(t, fixture.started)
	close(fixture.startGate)
	assertArtifactLaunchMode(t, fixture.launchModes, want)
	waitForSandbox(t, fixture.o, fixture.ctx, fixture.sb.ID, func(sb *types.Sandbox) bool {
		return sb.State == types.StateRunning
	}, "running after automatic resume")
}

func assertArtifactLaunchMode(t *testing.T, modes <-chan string, want types.LaunchMode) {
	t.Helper()
	select {
	case got := <-modes:
		if got != string(want) {
			t.Fatalf("task launch mode = %q, want %q", got, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("task did not receive the accepted launch mode")
	}
}

type resumeModeFixture struct {
	o           *Orchestrator
	ctx         context.Context
	sb          *types.Sandbox
	apiKey      string
	launcher    *countingLauncher
	started     <-chan struct{}
	startGate   chan struct{}
	launchModes <-chan string
}

func newResumeModeFixture(t *testing.T, sourceKind types.ResumeSourceKind, blockStart bool) resumeModeFixture {
	t.Helper()
	cfg := &config.Config{}
	cfg.Sandbox.TimeoutSec = 900
	started := make(chan struct{}, 4)
	startGate := make(chan struct{})
	launchModes := make(chan string, 1)
	lc := &countingLauncher{started: started, artifactLaunchModes: launchModes}
	if blockStart {
		lc.startGate = startGate
	} else {
		close(startGate)
	}
	o, ctx := newAsyncConnectTestOrchestrator(t, cfg, lc)

	manifestKey := strings.Repeat("6", 64)
	_, apiKey := defaultTestCredentials(t, manifestKey)
	extension := ".snapshot"
	if sourceKind == types.ResumeSourceSandbox {
		extension = ".sandbox"
	}
	sb := &types.Sandbox{
		ID: "resume-mode-target", Profile: types.ProfileBare,
		TemplateID: types.TemplateID{Profile: types.ProfileBare, Kind: types.KindImg, Ref: "manifest://" + strings.Repeat("7", 64)}.String(),
		State:      types.StatePaused,
		ResumeSource: types.ResumeSource{
			Kind: sourceKind,
			Ref:  "file://" + strings.Repeat("8", 64) + extension + "@location:resume-mode",
		},
		APISecret: deriveTestAPISecret(t, manifestKey), ManifestKey: manifestKey,
		RunDir: filepath.Join(cfg.Paths.RunRoot, "resume-mode-target"), BaseDir: filepath.Join(cfg.Paths.BaseRoot, "resume-mode-target"), CreatedUnix: 1,
	}
	materializeTestSandboxCredentials(t, sb)
	if err := o.st.Put(ctx, sb); err != nil {
		t.Fatal(err)
	}
	return resumeModeFixture{
		o: o, ctx: ctx, sb: sb, apiKey: apiKey, launcher: lc,
		started: started, startGate: startGate, launchModes: launchModes,
	}
}

func resumeMemoryOption(value bool) *bool { return &value }
