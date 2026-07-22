package orch

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/apikey"
	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestCreateRejectsInvalidEffectiveRestoreConfigBeforeSideEffects(t *testing.T) {
	root := t.TempDir()
	cfg := &config.Config{}
	cfg.Paths.RunRoot = filepath.Join(root, "run")
	cfg.Paths.BaseRoot = filepath.Join(root, "base")
	o := testOrchCfg(t, cfg)
	ctx := context.Background()
	apiKey, manifestKey, _ := allowlistedBuildIdentity(t, o)

	persistID := "bare-img-" + strings.Repeat("a", 64)
	if err := o.st.PutBuild(ctx, &types.Build{
		BuildID: "invalid-restore-template", TemplateID: "transient-invalid-restore",
		PersistID: persistID, ManifestKey: manifestKey, Profile: types.ProfileBare,
		Kind: types.KindImg, Status: types.BuildReady,
		Metadata: map[string]string{sandboxcfg.NsRestore: `{"prefetch":"disk"}`},
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := o.Create(ctx, api.CreateReq{TemplateID: persistID, APIKey: apiKey}); !errors.Is(err, api.ErrBadRequest) {
		t.Fatalf("Create error = %v, want ErrBadRequest", err)
	}
	for _, path := range []string{cfg.Paths.RunRoot, cfg.Paths.BaseRoot} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("invalid create produced filesystem side effect %s: %v", path, err)
		}
	}
	rows, _, err := o.st.List(ctx, "", "", 10, "")
	if err != nil || len(rows) != 0 {
		t.Fatalf("invalid create persisted sandboxes: rows=%+v err=%v", rows, err)
	}
}

func TestCreateRestoreConfigCreateOverridesTemplate(t *testing.T) {
	root := t.TempDir()
	cfg := &config.Config{}
	cfg.Paths.RunRoot = filepath.Join(root, "run")
	cfg.Paths.BaseRoot = filepath.Join(root, "base")
	cfg.Sandbox.Network.Bare.InnerIP = "169.254.1.1/31"
	o := testOrchCfg(t, cfg)
	lc := &countingLauncher{orch: o}
	o.lc = lc
	o.vs = stubVS{}
	o.runnerPool = newRunPool(runKindSandbox, 0, cfg.Units.PoolWaitDuration(), cfg.Paths.RunRoot, lc, o.runnerUnit, o.log)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := o.StartRunPools(ctx); err != nil {
		t.Fatal(err)
	}
	apiKey, manifestKey, _ := allowlistedBuildIdentity(t, o)

	persistID := "bare-img-" + strings.Repeat("b", 64)
	if err := o.st.PutBuild(ctx, &types.Build{
		BuildID: "restore-template", TemplateID: "transient-restore",
		PersistID: persistID, ManifestKey: manifestKey, Profile: types.ProfileBare,
		Kind: types.KindImg, Status: types.BuildReady,
		Metadata: map[string]string{sandboxcfg.NsRestore: `{"prefetch":"memory"}`},
	}); err != nil {
		t.Fatal(err)
	}

	inherited, err := o.Create(ctx, api.CreateReq{TemplateID: persistID, APIKey: apiKey})
	if err != nil {
		t.Fatalf("Create with template default: %v", err)
	}
	if got := inherited.Metadata[sandboxcfg.NsRestore]; got != `{"prefetch":"memory"}` {
		t.Fatalf("effective restore metadata = %q, want inherited template memory", got)
	}

	sb, err := o.Create(ctx, api.CreateReq{
		TemplateID: persistID, APIKey: apiKey,
		Metadata: map[string]string{sandboxcfg.NsRestore: `{"prefetch":"off"}`},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if got := sb.Metadata[sandboxcfg.NsRestore]; got != `{"prefetch":"off"}` {
		t.Fatalf("effective restore metadata = %q, want create-time off", got)
	}
	stored, err := o.st.Get(ctx, sb.ID)
	if err != nil || stored == nil || stored.Metadata[sandboxcfg.NsRestore] != `{"prefetch":"off"}` {
		t.Fatalf("persisted sandbox restore metadata = %+v, err=%v", stored, err)
	}
}

func TestBuildRestoreMetadataValidationAndTriggerOverride(t *testing.T) {
	o := testOrch(t)
	ctx := context.Background()
	apiKey, _, _ := allowlistedBuildIdentity(t, o)

	if _, err := o.RegisterBuild(ctx, apiKey, api.RegisterSpec{
		Profile:  types.ProfileBare,
		Metadata: map[string]string{sandboxcfg.NsRestore: `{"prefetch":"disk"}`},
	}); !errors.Is(err, api.ErrBadRequest) {
		t.Fatalf("RegisterBuild invalid restore error = %v, want ErrBadRequest", err)
	}
	registered, err := o.st.BuildsByStatus(ctx, types.BuildRegistered)
	if err != nil || len(registered) != 0 {
		t.Fatalf("invalid register persisted builds: builds=%+v err=%v", registered, err)
	}

	b, err := o.RegisterBuild(ctx, apiKey, api.RegisterSpec{
		Profile:  types.ProfileBare,
		Metadata: map[string]string{sandboxcfg.NsRestore: `{"prefetch":"memory"}`},
	})
	if err != nil {
		t.Fatal(err)
	}
	badTrigger := api.TriggerSpec{
		FromImage: "registry.test/base:latest",
		Metadata:  map[string]string{sandboxcfg.NsRestore: `{"prefetch":"disk"}`},
	}
	if err := o.TriggerBuild(ctx, apiKey, b.TemplateID, b.BuildID, badTrigger, api.BuildAuth{}); !errors.Is(err, api.ErrBadRequest) {
		t.Fatalf("TriggerBuild invalid restore error = %v, want ErrBadRequest", err)
	}
	stored, err := o.st.GetBuild(ctx, b.BuildID)
	if err != nil || stored == nil {
		t.Fatal(err)
	}
	if stored.Status != types.BuildRegistered || stored.Metadata[sandboxcfg.NsRestore] != `{"prefetch":"memory"}` {
		t.Fatalf("invalid trigger changed persisted build: %+v", stored)
	}

	if err := o.TriggerBuild(ctx, apiKey, b.TemplateID, b.BuildID, api.TriggerSpec{
		FromImage: "registry.test/base:latest",
		Metadata:  map[string]string{sandboxcfg.NsRestore: `{"prefetch":"off"}`},
	}, api.BuildAuth{}); err != nil {
		t.Fatalf("TriggerBuild: %v", err)
	}
	stored, err = o.st.GetBuild(ctx, b.BuildID)
	if err != nil || stored == nil || stored.Status != types.BuildWaiting || stored.Metadata[sandboxcfg.NsRestore] != `{"prefetch":"off"}` {
		t.Fatalf("trigger override was not persisted: build=%+v err=%v", stored, err)
	}
}

func TestImportRejectsInvalidRestoreMetadataBeforePersisting(t *testing.T) {
	o := testOrch(t)
	ctx := context.Background()
	mk := strings.Repeat("e", 64)
	rawMK, err := hex.DecodeString(mk)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(SandboxToken{
		V: sandboxTokenVersion, TemplateID: "bare-snp-" + strings.Repeat("c", 64),
		SnapshotRef: "manifest://" + strings.Repeat("d", 64), Profile: string(types.ProfileBare),
		MKFingerprint: hex.EncodeToString(apikey.Fingerprint(rawMK)),
		Metadata:      map[string]string{sandboxcfg.NsRestore: `{"prefetch":"disk"}`},
	})
	if err != nil {
		t.Fatal(err)
	}
	token := base64.StdEncoding.EncodeToString(raw)
	if _, err := o.importSandboxWithKey(ctx, mk, token); !errors.Is(err, api.ErrBadRequest) {
		t.Fatalf("import error = %v, want ErrBadRequest", err)
	}
	rows, _, err := o.st.List(ctx, "", "", 10, "")
	if err != nil || len(rows) != 0 {
		t.Fatalf("invalid import persisted sandboxes: rows=%+v err=%v", rows, err)
	}
}

func TestPausePreservesRestoreMetadata(t *testing.T) {
	root := t.TempDir()
	cfg := &config.Config{}
	cfg.Checkpoint.Mode = config.CheckpointLocal
	cfg.Checkpoint.LocalDir = filepath.Join(root, "checkpoints")
	cfg.Paths.RunRoot = filepath.Join(root, "run")
	o := testOrchCfg(t, cfg)
	o.vs = stubVS{}
	ctx := context.Background()
	apiKey, manifestKey, _ := allowlistedBuildIdentity(t, o)
	installPromoteStub(t, "") // a successful sandbox-ctl snapshot test double

	sb := &types.Sandbox{
		ID: "pause-restore-metadata", TemplateID: "bare-img-" + strings.Repeat("a", 64),
		State: types.StateRunning, ManifestKey: manifestKey,
		Metadata: map[string]string{sandboxcfg.NsRestore: `{"prefetch":"memory"}`},
	}
	if err := o.st.Put(ctx, sb); err != nil {
		t.Fatal(err)
	}
	if err := o.Pause(ctx, sb.ID, apiKey); err != nil {
		t.Fatalf("Pause: %v", err)
	}
	stored, err := o.st.Get(ctx, sb.ID)
	if err != nil || stored == nil || stored.State != types.StatePaused {
		t.Fatalf("paused sandbox = %+v, err=%v", stored, err)
	}
	if stored.Metadata[sandboxcfg.NsRestore] != `{"prefetch":"memory"}` {
		t.Fatalf("pause lost restore metadata: %+v", stored.Metadata)
	}
}

func TestClusterCreateRejectsInvalidRestoreConfigSynchronously(t *testing.T) {
	o := testOrch(t)
	cmd := &routesync.Command{
		CmdID: "invalid-restore", Kind: routesync.CmdCreate, SID: "sandbox-invalid-restore",
		TemplateRef: "bare-img-" + strings.Repeat("f", 64),
		Config:      map[string]string{sandboxcfg.NsRestore: `{"prefetch":"disk"}`},
	}
	ack := o.HandleCommand(context.Background(), cmd)
	if ack.Status != routesync.AckRejected || !strings.Contains(ack.Reason, api.ErrBadRequest.Error()) {
		t.Fatalf("cluster ack = %+v, want synchronous rejected bad request", ack)
	}
	if _, claimed := o.clusterCreates[cmd.SID]; claimed {
		t.Fatal("rejected cluster create claimed an async launch slot")
	}
}

func TestClusterBuildRegisterRejectsInvalidRestoreBeforePersisting(t *testing.T) {
	o := testOrch(t)
	ctx := context.Background()
	_, _, fingerprint := allowlistedBuildIdentity(t, o)
	cmd := &routesync.Command{
		BuildID: "cluster-build-invalid-restore", TemplateRef: "transient-cluster-build",
		Profile: string(types.ProfileBare), KeyFingerprint: fingerprint,
		Config: map[string]string{sandboxcfg.NsRestore: `{"prefetch":"disk"}`},
	}
	if err := o.registerClusterBuild(ctx, cmd); !errors.Is(err, api.ErrBadRequest) {
		t.Fatalf("registerClusterBuild error = %v, want ErrBadRequest", err)
	}
	stored, err := o.st.GetBuild(ctx, cmd.BuildID)
	if err != nil || stored != nil {
		t.Fatalf("invalid cluster build was persisted: build=%+v err=%v", stored, err)
	}
	if _, found := o.clusterBuilds[cmd.BuildID]; found {
		t.Fatal("invalid cluster build retained transient credentials")
	}
	select {
	case event := <-o.buildEvents:
		t.Fatalf("invalid cluster build published state: %+v", event)
	default:
	}
}
