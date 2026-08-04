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

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/orchestrator/internal/vswitch"
)

func orchCheckpointBool(value bool) *bool { return &value }

func TestResolveCheckpointPolicyPrecedence(t *testing.T) {
	tests := []struct {
		name     string
		node     sandboxcfg.CheckpointPolicy
		metadata string
		action   sandboxcfg.CheckpointPolicy
		want     sandboxcfg.CheckpointPolicy
	}{
		{
			name: "node only",
			node: sandboxcfg.CheckpointPolicy{
				MergeRef: orchCheckpointBool(true), DropCaches: orchCheckpointBool(false),
			},
			want: sandboxcfg.CheckpointPolicy{
				MergeRef: orchCheckpointBool(true), DropCaches: orchCheckpointBool(false),
			},
		},
		{
			name:     "metadata overrides node",
			node:     sandboxcfg.CheckpointPolicy{MergeRef: orchCheckpointBool(true), DropCaches: orchCheckpointBool(true)},
			metadata: `{"merge_ref":false,"drop_caches":false}`,
			want:     sandboxcfg.CheckpointPolicy{MergeRef: orchCheckpointBool(false), DropCaches: orchCheckpointBool(false)},
		},
		{
			name:     "action overrides metadata",
			node:     sandboxcfg.CheckpointPolicy{MergeRef: orchCheckpointBool(true), DropCaches: orchCheckpointBool(true)},
			metadata: `{"merge_ref":false,"drop_caches":false}`,
			action:   sandboxcfg.CheckpointPolicy{MergeRef: orchCheckpointBool(true), DropCaches: orchCheckpointBool(true)},
			want:     sandboxcfg.CheckpointPolicy{MergeRef: orchCheckpointBool(true), DropCaches: orchCheckpointBool(true)},
		},
		{
			name:     "fields come from different layers",
			node:     sandboxcfg.CheckpointPolicy{MergeRef: orchCheckpointBool(true)},
			metadata: `{"drop_caches":false}`,
			action:   sandboxcfg.CheckpointPolicy{MergeRef: orchCheckpointBool(false)},
			want:     sandboxcfg.CheckpointPolicy{MergeRef: orchCheckpointBool(false), DropCaches: orchCheckpointBool(false)},
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
			got, err := o.resolveCheckpointPolicy(metadata, tc.action)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("effective policy = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestPauseLocalCheckpointPolicyArgv(t *testing.T) {
	tests := []struct {
		name     string
		node     sandboxcfg.CheckpointPolicy
		metadata string
		action   sandboxcfg.CheckpointPolicy
		wantTail []string
	}{
		{name: "all unset"},
		{name: "node values",
			node:     sandboxcfg.CheckpointPolicy{MergeRef: orchCheckpointBool(false), DropCaches: orchCheckpointBool(true)},
			wantTail: []string{"--merge-ref=false", "--drop-caches=true"}},
		{name: "metadata and action layered",
			node:     sandboxcfg.CheckpointPolicy{MergeRef: orchCheckpointBool(true), DropCaches: orchCheckpointBool(true)},
			metadata: `{"merge_ref":false}`, action: sandboxcfg.CheckpointPolicy{DropCaches: orchCheckpointBool(false)},
			wantTail: []string{"--merge-ref=false", "--drop-caches=false"}},
		{name: "one explicit field", action: sandboxcfg.CheckpointPolicy{DropCaches: orchCheckpointBool(false)},
			wantTail: []string{"--drop-caches=false"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := checkpointOrchestratorConfig(t, config.CheckpointLocal)
			cfg.Checkpoint.MergeRef = tc.node.MergeRef
			cfg.Checkpoint.DropCaches = tc.node.DropCaches
			o, sb, apiKey, launcher, vs, argsPath := newCheckpointPauseFixture(t, cfg, tc.metadata)
			if err := o.Pause(context.Background(), sb.ID, apiKey, tc.action); err != nil {
				t.Fatal(err)
			}
			want := []string{"snapshot", "--sandbox-id", sb.ID, "--output", filepath.Join(cfg.Checkpoint.LocalDir, sb.ID), "--run-root", cfg.Paths.RunRoot}
			want = append(want, tc.wantTail...)
			if got := readCheckpointArgs(t, argsPath); !reflect.DeepEqual(got, want) {
				t.Fatalf("snapshot argv = %#v, want %#v", got, want)
			}
			stored, err := o.st.Get(context.Background(), sb.ID)
			if err != nil {
				t.Fatal(err)
			}
			if stored.State != types.StatePaused || stored.SnapshotRef != filepath.Join(cfg.Checkpoint.LocalDir, sb.ID, sb.ID+".snapshot") {
				t.Fatalf("paused sandbox = %+v", stored)
			}
			if launcher.stops.Load() != 1 || vs.detaches.Load() != 1 {
				t.Fatalf("stop/detach = %d/%d, want 1/1", launcher.stops.Load(), vs.detaches.Load())
			}
		})
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

func TestRemoteCheckpointRoutesRemainUnchanged(t *testing.T) {
	tests := []struct {
		name   string
		parent string
		want   func(*config.Config, *types.Sandbox) []string
		ref    func(*config.Config, *types.Sandbox) string
	}{
		{
			name: "direct upload",
			want: func(cfg *config.Config, sb *types.Sandbox) []string {
				return []string{"snapshot", "--sandbox-id", sb.ID, "--upload", "--run-root", cfg.Paths.RunRoot}
			},
			ref: func(_ *config.Config, _ *types.Sandbox) string {
				return "manifest://" + strings.Repeat("a", 64)
			},
		},
		{
			name: "ref location parent still captures locally", parent: "file:///mnt/checkpoints",
			want: func(cfg *config.Config, sb *types.Sandbox) []string {
				return []string{"snapshot", "--sandbox-id", sb.ID, "--output", filepath.Join(cfg.Checkpoint.LocalDir, sb.ID), "--run-root", cfg.Paths.RunRoot}
			},
			ref: func(cfg *config.Config, sb *types.Sandbox) string {
				return filepath.Join(cfg.Checkpoint.LocalDir, sb.ID, sb.ID+".snapshot")
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := checkpointOrchestratorConfig(t, config.CheckpointRemote)
			cfg.Checkpoint.Remote.RefLocationParent = tc.parent
			o, sb, apiKey, _, _, argsPath := newCheckpointPauseFixture(t, cfg, "")
			if err := o.Pause(context.Background(), sb.ID, apiKey, sandboxcfg.CheckpointPolicy{}); err != nil {
				t.Fatal(err)
			}
			if got, want := readCheckpointArgs(t, argsPath), tc.want(cfg, sb); !reflect.DeepEqual(got, want) {
				t.Fatalf("remote compatibility argv = %#v, want %#v", got, want)
			}
			stored, err := o.st.Get(context.Background(), sb.ID)
			if err != nil {
				t.Fatal(err)
			}
			if stored.SnapshotRef != tc.ref(cfg, sb) {
				t.Fatalf("snapshot ref = %q, want %q", stored.SnapshotRef, tc.ref(cfg, sb))
			}
		})
	}
}

func TestPausePolicyValidationHasNoSideEffects(t *testing.T) {
	tests := []struct {
		name     string
		mode     string
		metadata string
		action   sandboxcfg.CheckpointPolicy
		auto     bool
	}{
		{name: "malformed historical metadata", mode: config.CheckpointLocal, metadata: `{"merge_ref":"false"}`},
		{name: "remote explicit action", mode: config.CheckpointRemote, action: sandboxcfg.CheckpointPolicy{MergeRef: orchCheckpointBool(false)}},
		{name: "remote metadata", mode: config.CheckpointRemote, metadata: `{"drop_caches":false}`},
		{name: "remote auto-pause metadata", mode: config.CheckpointRemote, metadata: `{"merge_ref":true}`, auto: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := checkpointOrchestratorConfig(t, tc.mode)
			o, sb, apiKey, launcher, vs, argsPath := newCheckpointPauseFixture(t, cfg, tc.metadata)
			queued := o.newResumeRequest(sb.ID)
			defer o.releaseResumeRequest(queued)
			var err error
			if tc.auto {
				err = o.pauseSandbox(context.Background(), sb)
			} else {
				err = o.Pause(context.Background(), sb.ID, apiKey, tc.action)
			}
			if !errors.Is(err, api.ErrBadRequest) {
				t.Fatalf("Pause error = %v, want ErrBadRequest", err)
			}
			if !o.resumeRequestValid(queued) {
				t.Fatal("policy validation canceled a queued resume request")
			}
			if _, statErr := os.Stat(argsPath); !os.IsNotExist(statErr) {
				t.Fatalf("snapshot command ran before validation: stat error=%v", statErr)
			}
			stored, getErr := o.st.Get(context.Background(), sb.ID)
			if getErr != nil {
				t.Fatal(getErr)
			}
			if stored.State != types.StateRunning || stored.SnapshotRef != "" {
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
	if err := o.Pause(context.Background(), sb.ID, apiKey, sandboxcfg.CheckpointPolicy{}); err == nil {
		t.Fatal("Pause succeeded despite snapshot failure")
	}
	stored, err := o.st.Get(context.Background(), sb.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.State != types.StateRunning || stored.SnapshotRef != "" {
		t.Fatalf("snapshot failure changed sandbox: %+v", stored)
	}
	if launcher.stops.Load() != 0 || vs.detaches.Load() != 0 {
		t.Fatalf("snapshot failure stop/detach = %d/%d", launcher.stops.Load(), vs.detaches.Load())
	}
}

func TestCreateRejectsCheckpointPolicyBeforeLaunchSideEffects(t *testing.T) {
	tests := []struct {
		name     string
		mode     string
		metadata string
	}{
		{name: "malformed", mode: config.CheckpointLocal, metadata: `{"merge_ref":0}`},
		{name: "remote conflict", mode: config.CheckpointRemote, metadata: `{"merge_ref":false}`},
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

func TestCheckpointPolicyLogValues(t *testing.T) {
	if checkpointPolicyValue(nil) != "default" || checkpointPolicyValue(orchCheckpointBool(true)) != "true" || checkpointPolicyValue(orchCheckpointBool(false)) != "false" {
		t.Fatal("checkpoint policy log values do not distinguish default/true/false")
	}
}

func checkpointOrchestratorConfig(t *testing.T, mode string) *config.Config {
	t.Helper()
	dir := t.TempDir()
	return &config.Config{
		Checkpoint: config.CheckpointConfig{Mode: mode, LocalDir: filepath.Join(dir, "checkpoints")},
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
		APISecret: apiSecret, ManifestKey: manifestKey, CreatedUnix: 1,
	}
	if metadataRaw != "" {
		sb.Metadata = map[string]string{sandboxcfg.NsCheckpoint: metadataRaw}
	}
	materializeTestSandboxCredentials(t, sb)
	if err := o.st.Put(context.Background(), sb); err != nil {
		t.Fatal(err)
	}
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
printf '%s\n' "${CHECKPOINT_STDOUT:-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa}"
`
	if err := os.WriteFile(filepath.Join(dir, config.BinSandboxCtl), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("CHECKPOINT_ARGS_FILE", argsPath)
	t.Setenv("CHECKPOINT_FAIL", "")
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
