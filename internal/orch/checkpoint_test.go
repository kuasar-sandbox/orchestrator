package orch

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/nodepath"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/orchestrator/internal/vswitch"
)

const checkpointSnapshotRef = "file://produced.snapshot@digest:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
const checkpointSandboxRef = "file://associated.sandbox@digest:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

func orchCheckpointBool(value bool) *bool { return &value }

func orchSnapshotCapture(policy sandboxcfg.SnapshotPolicy) sandboxcfg.CaptureRequest {
	return sandboxcfg.CaptureRequest{Kind: types.CaptureSnapshot, SnapshotPolicy: policy}
}

func TestResolveSnapshotPolicyPrecedence(t *testing.T) {
	tests := []struct {
		name     string
		node     sandboxcfg.SnapshotPolicy
		metadata string
		action   sandboxcfg.SnapshotPolicy
		want     sandboxcfg.SnapshotPolicy
	}{
		{
			name: "node only",
			node: sandboxcfg.SnapshotPolicy{
				MergeRef: orchCheckpointBool(true), DropCaches: orchCheckpointBool(false),
			},
			want: sandboxcfg.SnapshotPolicy{
				MergeRef: orchCheckpointBool(true), DropCaches: orchCheckpointBool(false),
			},
		},
		{
			name:     "metadata overrides node",
			node:     sandboxcfg.SnapshotPolicy{MergeRef: orchCheckpointBool(true), DropCaches: orchCheckpointBool(true)},
			metadata: `{"merge_ref":false,"drop_caches":false}`,
			want:     sandboxcfg.SnapshotPolicy{MergeRef: orchCheckpointBool(false), DropCaches: orchCheckpointBool(false)},
		},
		{
			name:     "action overrides metadata",
			node:     sandboxcfg.SnapshotPolicy{MergeRef: orchCheckpointBool(true), DropCaches: orchCheckpointBool(true)},
			metadata: `{"merge_ref":false,"drop_caches":false}`,
			action:   sandboxcfg.SnapshotPolicy{MergeRef: orchCheckpointBool(true), DropCaches: orchCheckpointBool(true)},
			want:     sandboxcfg.SnapshotPolicy{MergeRef: orchCheckpointBool(true), DropCaches: orchCheckpointBool(true)},
		},
		{
			name:     "fields come from different layers",
			node:     sandboxcfg.SnapshotPolicy{MergeRef: orchCheckpointBool(true)},
			metadata: `{"drop_caches":false}`,
			action:   sandboxcfg.SnapshotPolicy{MergeRef: orchCheckpointBool(false)},
			want:     sandboxcfg.SnapshotPolicy{MergeRef: orchCheckpointBool(false), DropCaches: orchCheckpointBool(false)},
		},
		{name: "all unset"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := checkpointOrchestratorConfig(t, config.CheckpointLocal)
			cfg.Checkpoint.MergeRef = tc.node.MergeRef
			cfg.Checkpoint.DropCaches = tc.node.DropCaches
			o := testOrchCfg(t, cfg)
			metadata := map[string]string{}
			if tc.metadata != "" {
				metadata[sandboxcfg.NsCheckpoint] = tc.metadata
			}
			got, err := o.resolveSnapshotPolicy(metadata, tc.action)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("effective policy = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestPauseCheckpointModeAndPolicyArgv(t *testing.T) {
	tests := []struct {
		name     string
		mode     string
		node     sandboxcfg.SnapshotPolicy
		metadata string
		action   sandboxcfg.SnapshotPolicy
		wantTail []string
	}{
		{name: "local all unset", mode: config.CheckpointLocal},
		{name: "bundle node values", mode: config.CheckpointBundle,
			node:     sandboxcfg.SnapshotPolicy{MergeRef: orchCheckpointBool(false), DropCaches: orchCheckpointBool(true)},
			wantTail: []string{"--merge-ref=false", "--drop-caches=true"}},
		{name: "local metadata and action layered", mode: config.CheckpointLocal,
			node:     sandboxcfg.SnapshotPolicy{MergeRef: orchCheckpointBool(true), DropCaches: orchCheckpointBool(true)},
			metadata: `{"merge_ref":false}`, action: sandboxcfg.SnapshotPolicy{DropCaches: orchCheckpointBool(false)},
			wantTail: []string{"--merge-ref=false", "--drop-caches=false"}},
		{name: "bundle one explicit field", mode: config.CheckpointBundle, action: sandboxcfg.SnapshotPolicy{DropCaches: orchCheckpointBool(false)},
			wantTail: []string{"--drop-caches=false"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := checkpointOrchestratorConfig(t, tc.mode)
			cfg.Checkpoint.MergeRef = tc.node.MergeRef
			cfg.Checkpoint.DropCaches = tc.node.DropCaches
			o, sb, apiKey, launcher, vs, argsPath := newCheckpointPauseFixture(t, cfg, tc.metadata)
			if err := o.Pause(context.Background(), sb.ID, apiKey, orchSnapshotCapture(tc.action)); err != nil {
				t.Fatal(err)
			}
			checkpointDir := filepath.Join(sb.BaseDir, "checkpoint")
			want := []string{"snapshot", "--json", "--path-id", sb.ID, "--output", checkpointDir, "--mode", tc.mode, "--run-root", nodepath.SandboxRunRoot(cfg.Paths.RunRoot)}
			want = append(want, tc.wantTail...)
			if got := readCheckpointArgs(t, argsPath); !reflect.DeepEqual(got, want) {
				t.Fatalf("snapshot argv = %#v, want %#v", got, want)
			}
			stored, err := o.st.Get(context.Background(), sb.ID)
			if err != nil {
				t.Fatal(err)
			}
			if stored.State != types.StatePaused || stored.ResumeSource != (types.ResumeSource{SandboxRef: checkpointSandboxRef,
				Kind: types.ResumeSourceSnapshot,
				Ref:  checkpointSnapshotRef,
			}) {
				t.Fatalf("paused sandbox = %+v", stored)
			}
			if launcher.stops.Load() != 1 || vs.detaches.Load() != 1 {
				t.Fatalf("stop/detach = %d/%d, want 1/1", launcher.stops.Load(), vs.detaches.Load())
			}
		})
	}
}

func TestPauseSandboxCaptureAndTTLAutoPauseSelection(t *testing.T) {
	for _, mode := range []string{config.CheckpointLocal, config.CheckpointBundle} {
		t.Run(mode+" explicit sandbox", func(t *testing.T) {
			cfg := checkpointOrchestratorConfig(t, mode)
			o, sb, apiKey, launcher, vs, argsPath := newCheckpointPauseFixture(t, cfg, "")
			if err := o.Pause(context.Background(), sb.ID, apiKey, sandboxcfg.CaptureRequest{Kind: types.CaptureSandbox}); err != nil {
				t.Fatal(err)
			}
			checkpointDir := filepath.Join(sb.BaseDir, "checkpoint")
			want := []string{"export", "--json", "--path-id", sb.ID, "--output", checkpointDir, "--mode", mode, "--run-root", nodepath.SandboxRunRoot(cfg.Paths.RunRoot)}
			if got := readCheckpointArgs(t, argsPath); !reflect.DeepEqual(got, want) {
				t.Fatalf("export argv = %#v, want %#v", got, want)
			}
			stored, err := o.st.Get(context.Background(), sb.ID)
			if err != nil || stored == nil || stored.State != types.StatePaused || stored.ResumeSource != (types.ResumeSource{
				Kind: types.ResumeSourceSandbox, Ref: checkpointSandboxRef,
			}) || stored.RunID != "" || stored.VswitchPort != "" {
				t.Fatalf("paused Sandbox E = %+v, %v", stored, err)
			}
			if launcher.stops.Load() != 1 || vs.detaches.Load() != 1 {
				t.Fatalf("stop/detach = %d/%d", launcher.stops.Load(), vs.detaches.Load())
			}
		})

		t.Run(mode+" ttl sandbox", func(t *testing.T) {
			cfg := checkpointOrchestratorConfig(t, mode)
			o, sb, _, _, _, argsPath := newCheckpointPauseFixture(t, cfg, `{"merge_ref":true,"drop_caches":true}`)
			if err := o.st.Delete(context.Background(), sb.ID); err != nil {
				t.Fatal(err)
			}
			sb.AutoPauseMemory = false
			if err := o.st.Put(context.Background(), sb); err != nil {
				t.Fatal(err)
			}
			o.cache(sb)
			if err := o.pauseSandbox(context.Background(), sb); err != nil {
				t.Fatal(err)
			}
			args := readCheckpointArgs(t, argsPath)
			if len(args) == 0 || args[0] != "export" || containsString(args, "--merge-ref=true") || containsString(args, "--drop-caches=true") {
				t.Fatalf("TTL Sandbox E argv = %#v", args)
			}
			stored, err := o.st.Get(context.Background(), sb.ID)
			if err != nil || stored == nil || stored.ResumeSource.Kind != types.ResumeSourceSandbox {
				t.Fatalf("TTL Sandbox E row = %+v, %v", stored, err)
			}
		})
	}
}

func TestExplicitPauseDefaultsToSnapshotIndependentlyOfAutoPauseMemory(t *testing.T) {
	cfg := checkpointOrchestratorConfig(t, config.CheckpointLocal)
	o, sb, apiKey, _, _, argsPath := newCheckpointPauseFixture(t, cfg, "")
	if err := o.st.Delete(context.Background(), sb.ID); err != nil {
		t.Fatal(err)
	}
	sb.AutoPauseMemory = false
	if err := o.st.Put(context.Background(), sb); err != nil {
		t.Fatal(err)
	}
	o.cache(sb)
	if err := o.Pause(context.Background(), sb.ID, apiKey, orchSnapshotCapture(sandboxcfg.SnapshotPolicy{})); err != nil {
		t.Fatal(err)
	}
	if args := readCheckpointArgs(t, argsPath); len(args) == 0 || args[0] != "snapshot" {
		t.Fatalf("explicit default Pause argv = %#v, want Snapshot S", args)
	}
	stored, err := o.st.Get(context.Background(), sb.ID)
	if err != nil || stored == nil || stored.ResumeSource.Kind != types.ResumeSourceSnapshot {
		t.Fatalf("explicit default Pause row = %+v, %v", stored, err)
	}
}

func TestAutoPauseUsesMetadataOverNodePolicy(t *testing.T) {
	cfg := checkpointOrchestratorConfig(t, config.CheckpointLocal)
	cfg.Checkpoint.MergeRef = orchCheckpointBool(true)
	cfg.Checkpoint.DropCaches = orchCheckpointBool(false)
	o, sb, _, _, _, argsPath := newCheckpointPauseFixture(t, cfg, `{"merge_ref":false}`)
	if err := o.pauseSandbox(context.Background(), sb); err != nil {
		t.Fatal(err)
	}
	wantTail := []string{"--merge-ref=false", "--drop-caches=false"}
	args := readCheckpointArgs(t, argsPath)
	if len(args) < len(wantTail) || !reflect.DeepEqual(args[len(args)-len(wantTail):], wantTail) {
		t.Fatalf("auto-pause argv = %#v, want tail %#v", args, wantTail)
	}
}

func TestPausePolicyValidationHasNoSideEffects(t *testing.T) {
	tests := []struct {
		name     string
		mode     string
		metadata string
		auto     bool
	}{
		{name: "malformed historical metadata", mode: config.CheckpointLocal, metadata: `{"merge_ref":"false"}`},
		{name: "bundle malformed auto-pause metadata", mode: config.CheckpointBundle, metadata: `{"drop_caches":"false"}`, auto: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := checkpointOrchestratorConfig(t, tc.mode)
			o, sb, apiKey, launcher, vs, argsPath := newCheckpointPauseFixture(t, cfg, tc.metadata)
			var err error
			if tc.auto {
				err = o.pauseSandbox(context.Background(), sb)
			} else {
				err = o.Pause(context.Background(), sb.ID, apiKey, orchSnapshotCapture(sandboxcfg.SnapshotPolicy{}))
			}
			if !errors.Is(err, api.ErrBadRequest) {
				t.Fatalf("Pause error = %v, want ErrBadRequest", err)
			}
			if _, statErr := os.Stat(argsPath); !os.IsNotExist(statErr) {
				t.Fatalf("snapshot command ran before validation: stat error=%v", statErr)
			}
			stored, getErr := o.st.Get(context.Background(), sb.ID)
			if getErr != nil {
				t.Fatal(getErr)
			}
			if stored.State != types.StateRunning || stored.ResumeSource != (types.ResumeSource{}) {
				t.Fatalf("sandbox changed after rejected Pause: %+v", stored)
			}
			if launcher.stops.Load() != 0 || vs.detaches.Load() != 0 {
				t.Fatalf("rejected Pause stop/detach = %d/%d", launcher.stops.Load(), vs.detaches.Load())
			}
		})
	}
}

func TestSnapshotFailureLeavesSandboxRunning(t *testing.T) {
	cfg := checkpointOrchestratorConfig(t, config.CheckpointLocal)
	o, sb, apiKey, launcher, vs, _ := newCheckpointPauseFixture(t, cfg, "")
	t.Setenv("CHECKPOINT_FAIL", "1")
	if err := o.Pause(context.Background(), sb.ID, apiKey, orchSnapshotCapture(sandboxcfg.SnapshotPolicy{})); err == nil {
		t.Fatal("Pause succeeded despite snapshot failure")
	}
	stored, err := o.st.Get(context.Background(), sb.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.State != types.StateRunning || stored.ResumeSource != (types.ResumeSource{}) {
		t.Fatalf("snapshot failure changed sandbox: %+v", stored)
	}
	if launcher.stops.Load() != 0 || vs.detaches.Load() != 0 {
		t.Fatalf("snapshot failure stop/detach = %d/%d", launcher.stops.Load(), vs.detaches.Load())
	}
}

func TestSandboxExportFailureLeavesRunningOwnershipUnchanged(t *testing.T) {
	cfg := checkpointOrchestratorConfig(t, config.CheckpointBundle)
	o, sb, apiKey, launcher, vs, _ := newCheckpointPauseFixture(t, cfg, "")
	t.Setenv("CHECKPOINT_FAIL", "1")
	request := sandboxcfg.CaptureRequest{Kind: types.CaptureSandbox}
	if err := o.Pause(context.Background(), sb.ID, apiKey, request); err == nil {
		t.Fatal("Pause succeeded despite Sandbox export failure")
	}
	stored, err := o.st.Get(context.Background(), sb.ID)
	if err != nil || stored == nil || stored.State != types.StateRunning || stored.ResumeSource != (types.ResumeSource{}) ||
		stored.RunID != sb.RunID || stored.VswitchPort != sb.VswitchPort {
		t.Fatalf("export failure changed running ownership: %+v, %v", stored, err)
	}
	if launcher.stops.Load() != 0 || vs.detaches.Load() != 0 {
		t.Fatalf("export failure stop/detach = %d/%d", launcher.stops.Load(), vs.detaches.Load())
	}
}

func TestAcceptedPauseSurvivesCancellationAndDrainsAtShutdown(t *testing.T) {
	cfg := checkpointOrchestratorConfig(t, config.CheckpointLocal)
	o, sb, apiKey, launcher, vs, _ := newCheckpointPauseFixture(t, cfg, "")

	serviceCtx, stopService := context.WithCancel(context.Background())
	o.SetLifecycleContext(serviceCtx)
	requestCtx, cancelRequest := context.WithCancel(context.Background())
	started := filepath.Join(t.TempDir(), "started")
	release := filepath.Join(t.TempDir(), "release")
	t.Setenv("CHECKPOINT_STARTED_FILE", started)
	t.Setenv("CHECKPOINT_RELEASE_FILE", release)
	t.Cleanup(func() { _ = os.WriteFile(release, nil, 0o600) })

	pauseDone := make(chan error, 1)
	go func() {
		pauseDone <- o.Pause(requestCtx, sb.ID, apiKey, orchSnapshotCapture(sandboxcfg.SnapshotPolicy{}))
	}()
	waitForCheckpointFile(t, started)

	// Neither the disconnected caller nor shutdown admission cancellation may
	// kill a snapshot client after the runtime has accepted its request.
	cancelRequest()
	stopService()
	drainDone := make(chan error, 1)
	go func() { drainDone <- o.DrainPauses(context.Background()) }()
	assertCheckpointBlocked(t, pauseDone, "pause returned before snapshot completion")
	assertCheckpointBlocked(t, drainDone, "shutdown drain returned before accepted pause completion")

	if err := os.WriteFile(release, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := waitCheckpointResult(t, pauseDone); err != nil {
		t.Fatalf("Pause after cancellation: %v", err)
	}
	if err := waitCheckpointResult(t, drainDone); err != nil {
		t.Fatalf("DrainPauses: %v", err)
	}

	stored, err := o.st.Get(context.Background(), sb.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.State != types.StatePaused || stored.ResumeSource != (types.ResumeSource{SandboxRef: checkpointSandboxRef,
		Kind: types.ResumeSourceSnapshot,
		Ref:  checkpointSnapshotRef,
	}) {
		t.Fatalf("pause after cancellation was not committed: %+v", stored)
	}
	if launcher.stops.Load() != 1 || vs.detaches.Load() != 1 {
		t.Fatalf("stop/detach = %d/%d, want 1/1", launcher.stops.Load(), vs.detaches.Load())
	}
}

func TestAcceptedPauseCancellationFencesImmediateConnectAndExecActivation(t *testing.T) {
	cfg := checkpointOrchestratorConfig(t, config.CheckpointLocal)
	shortDir := shortOrchestratorTestDir(t)
	cfg.Paths.RunRoot = filepath.Join(shortDir, "run")
	cfg.Paths.BaseRoot = filepath.Join(shortDir, "base")
	installCheckpointSandboxCtl(t)
	stopEntered := make(chan struct{}, 1)
	stopGate := make(chan struct{}, 1)
	t.Cleanup(func() {
		select {
		case stopGate <- struct{}{}:
		default:
		}
	})
	launcher := &countingLauncher{stopEntered: stopEntered, stopGate: stopGate}
	o, lifecycleCtx := newAsyncConnectTestOrchestrator(t, cfg, launcher)

	manifestKey := strings.Repeat("5", 64)
	apiSecret, apiKey := defaultTestCredentials(t, manifestKey)
	sb := &types.Sandbox{
		ID: "pause-connect-fence", Profile: types.ProfileBare,
		TemplateID: types.TemplateID{
			Profile: types.ProfileBare, Kind: types.KindImg,
			Ref: "manifest://" + strings.Repeat("6", 64),
		}.String(),
		State: types.StateRunning, RunID: "pause-connect-old-run", VswitchPort: "pause-connect-old-port",
		APISecret: apiSecret, ManifestKey: manifestKey,
		RunDir:      nodepath.SandboxRunDir(cfg.Paths.RunRoot, "pause-connect-fence"),
		BaseDir:     nodepath.SandboxBaseDir(cfg.Paths.BaseRoot, "pause-connect-fence"),
		CreatedUnix: 1,
	}
	materializeTestSandboxCredentials(t, sb)
	if err := o.st.Put(lifecycleCtx, sb); err != nil {
		t.Fatal(err)
	}
	o.runs.restore(sb.RunID, instanceUnit(cfg.Units.RunnerPoolConfigs()[0].Unit, sb.RunID))
	o.cache(sb)

	requestCtx, cancelRequest := context.WithCancel(context.Background())
	started := filepath.Join(t.TempDir(), "started")
	release := filepath.Join(t.TempDir(), "release")
	t.Setenv("CHECKPOINT_STARTED_FILE", started)
	t.Setenv("CHECKPOINT_RELEASE_FILE", release)
	t.Cleanup(func() { _ = os.WriteFile(release, nil, 0o600) })

	pauseDone := make(chan error, 1)
	go func() {
		pauseDone <- o.Pause(requestCtx, sb.ID, apiKey, orchSnapshotCapture(sandboxcfg.SnapshotPolicy{}))
	}()
	waitForCheckpointFile(t, started)
	cancelRequest()
	if err := os.WriteFile(release, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	select {
	case <-stopEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("accepted pause did not reach post-commit runner cleanup")
	}
	stored, err := o.st.Get(lifecycleCtx, sb.ID)
	if err != nil || stored == nil || stored.State != types.StatePaused {
		t.Fatalf("sandbox at blocked cleanup = %+v, %v; want durable paused", stored, err)
	}

	type connectResult struct {
		sb  *types.Sandbox
		err error
	}
	connectDone := make(chan connectResult, 1)
	go func() {
		connected, connectErr := o.Connect(lifecycleCtx, sb.ID, apiKey, "", api.ConnectOptions{})
		connectDone <- connectResult{sb: connected, err: connectErr}
	}()
	select {
	case result := <-connectDone:
		t.Fatalf("Connect escaped pause cleanup fence: %+v, %v", result.sb, result.err)
	case <-time.After(50 * time.Millisecond):
	}
	stopGate <- struct{}{}
	if err := waitCheckpointResult(t, pauseDone); err != nil {
		t.Fatalf("Pause after cancellation: %v", err)
	}

	var connected connectResult
	select {
	case connected = <-connectDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Connect did not continue after pause cleanup")
	}
	if connected.err != nil || connected.sb == nil ||
		(connected.sb.State != types.StateStarting && connected.sb.State != types.StateRunning) {
		t.Fatalf("Connect after canceled Pause = %+v, %v", connected.sb, connected.err)
	}

	waitForSandbox(t, o, lifecycleCtx, sb.ID, func(current *types.Sandbox) bool {
		return current.State == types.StateRunning
	}, "running after Connect")
	stored, err = o.st.Get(lifecycleCtx, sb.ID)
	if err != nil || stored == nil || stored.State != types.StateRunning {
		t.Fatalf("sandbox after Connect = %+v, %v; want running", stored, err)
	}
}

func waitForCheckpointFile(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(path); err == nil {
			return
		} else if !os.IsNotExist(err) {
			t.Fatal(err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", path)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func assertCheckpointBlocked(t *testing.T, done <-chan error, message string) {
	t.Helper()
	select {
	case err := <-done:
		t.Fatalf("%s: %v", message, err)
	case <-time.After(100 * time.Millisecond):
	}
}

func waitCheckpointResult(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for checkpoint operation")
		return nil
	}
}

func TestCreateRejectsSnapshotPolicyBeforeLaunchSideEffects(t *testing.T) {
	tests := []struct {
		name     string
		mode     string
		metadata string
	}{
		{name: "local malformed", mode: config.CheckpointLocal, metadata: `{"merge_ref":0}`},
		{name: "bundle malformed", mode: config.CheckpointBundle, metadata: `{"drop_caches":"false"}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := checkpointOrchestratorConfig(t, tc.mode)
			o := testOrchCfg(t, cfg)
			launcher := &countingLauncher{}
			vs := &checkpointVS{}
			o.lc, o.vs = launcher, vs
			manifestKey := strings.Repeat("4", 64)
			apiSecret, apiKey := defaultTestCredentials(t, manifestKey)
			if _, _, _, err := o.AddKeyPair(context.Background(), manifestKey, apiSecret, "test", 0, ""); err != nil {
				t.Fatal(err)
			}
			_, err := o.Create(context.Background(), api.CreateReq{
				APIKey: apiKey,
				TemplateID: types.TemplateID{
					Profile: types.ProfileBare, Kind: types.KindImg, Ref: "manifest://" + strings.Repeat("5", 64),
				}.String(),
				Metadata: map[string]string{sandboxcfg.NsCheckpoint: tc.metadata},
			})
			if !errors.Is(err, api.ErrBadRequest) {
				t.Fatalf("Create error = %v, want ErrBadRequest", err)
			}
			if launcher.starts.Load() != 0 || vs.attaches.Load() != 0 {
				t.Fatalf("invalid Create launch/attach = %d/%d", launcher.starts.Load(), vs.attaches.Load())
			}
		})
	}
}

func TestSnapshotPolicyLogValues(t *testing.T) {
	if checkpointPolicyValue(nil) != "default" || checkpointPolicyValue(orchCheckpointBool(true)) != "true" || checkpointPolicyValue(orchCheckpointBool(false)) != "false" {
		t.Fatal("checkpoint policy log values do not distinguish default/true/false")
	}
}

func checkpointOrchestratorConfig(t *testing.T, mode string) *config.Config {
	t.Helper()
	dir := t.TempDir()
	return &config.Config{
		Checkpoint: config.CheckpointConfig{Mode: mode},
		Paths:      config.PathsConfig{RunRoot: filepath.Join(dir, "run"), BaseRoot: filepath.Join(dir, "base")},
		Units:      config.UnitsConfig{Runner: "sandbox-runner@.service"},
	}
}

func newCheckpointPauseFixture(t *testing.T, cfg *config.Config, metadataRaw string) (*Orchestrator, *types.Sandbox, string, *countingLauncher, *checkpointVS, string) {
	t.Helper()
	argsPath := installCheckpointSandboxCtl(t)
	o := testOrchCfg(t, cfg)
	launcher := &countingLauncher{}
	vs := &checkpointVS{}
	o.lc, o.vs = launcher, vs
	manifestKey := strings.Repeat("1", 64)
	apiSecret, apiKey := defaultTestCredentials(t, manifestKey)
	sb := &types.Sandbox{
		ID: "checkpoint-sandbox", Profile: types.ProfileBare,
		TemplateID: types.TemplateID{Profile: types.ProfileBare, Kind: types.KindImg, Ref: "manifest://" + strings.Repeat("2", 64)}.String(),
		State:      types.StateRunning, RunID: "checkpoint-run", VswitchPort: "checkpoint-port",
		RunDir:          nodepath.SandboxRunDir(cfg.Paths.RunRoot, "checkpoint-sandbox"),
		BaseDir:         nodepath.SandboxBaseDir(cfg.Paths.BaseRoot, "checkpoint-sandbox"),
		AutoPauseMemory: true,
		APISecret:       apiSecret, ManifestKey: manifestKey, CreatedUnix: 1,
	}
	if metadataRaw != "" {
		sb.Metadata = map[string]string{sandboxcfg.NsCheckpoint: metadataRaw}
	}
	materializeTestSandboxCredentials(t, sb)
	if err := o.st.Put(context.Background(), sb); err != nil {
		t.Fatal(err)
	}
	o.runs.restore(sb.RunID, instanceUnit(cfg.Units.RunnerPoolConfigs()[0].Unit, sb.RunID))
	o.cache(sb)
	return o, sb, apiKey, launcher, vs, argsPath
}

func installCheckpointSandboxCtl(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	argsPath := filepath.Join(dir, "args")
	script := `#!/bin/sh
printf '%s\n' "$@" > "$CHECKPOINT_ARGS_FILE"
if [ "${CHECKPOINT_FAIL:-}" = "1" ]; then
  echo "forced snapshot failure" >&2
  exit 1
fi
if [ -n "${CHECKPOINT_STARTED_FILE:-}" ]; then
  : > "$CHECKPOINT_STARTED_FILE"
  while [ ! -e "$CHECKPOINT_RELEASE_FILE" ]; do
    sleep 0.01
  done
fi
if [ -n "${CHECKPOINT_STDOUT:-}" ]; then
  printf '%s\n' "$CHECKPOINT_STDOUT"
elif [ "$1" = "snapshot" ]; then
  printf '%s\n' '{"snapshotRef":"file://produced.snapshot@digest:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","sandboxRef":"file://associated.sandbox@digest:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","removedRefs":[]}'
else
  printf '%s\n' '{"sandboxRef":"file://associated.sandbox@digest:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","removedRefs":[]}'
fi
`
	if err := os.WriteFile(filepath.Join(dir, config.BinSandboxCtl), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "node-ctl"), []byte("#!/bin/sh\n[ \"$1\" = checkpoint-cleanup ] || exit 2\nexit 0\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("CHECKPOINT_ARGS_FILE", argsPath)
	t.Setenv("CHECKPOINT_FAIL", "")
	t.Setenv("CHECKPOINT_STARTED_FILE", "")
	t.Setenv("CHECKPOINT_RELEASE_FILE", "")
	return argsPath
}

func readCheckpointArgs(t *testing.T, path string) []string {
	t.Helper()
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return strings.Split(strings.TrimSuffix(string(body), "\n"), "\n")
}

type checkpointVS struct {
	attaches atomic.Int64
	detaches atomic.Int64
}

func (v *checkpointVS) Attach(context.Context, vswitch.AttachReq) (*vswitch.Port, error) {
	v.attaches.Add(1)
	return &vswitch.Port{Port: "checkpoint-port", FloatingIP: "169.254.1.2", MAC: "02:00:00:00:00:01", InnerIP: "169.254.1.1"}, nil
}

func (v *checkpointVS) Detach(context.Context, string) error {
	v.detaches.Add(1)
	return nil
}

func (*checkpointVS) TapFD(port string) vswitch.TapFD {
	return vswitch.TapFD{Exec: []string{"true", port}}
}
