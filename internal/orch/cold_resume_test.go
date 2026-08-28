package orch

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/proxy"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// coldResumeSandbox persists a paused sandbox with a self-consistent
// credential pair and returns it together with the minted API key.
func coldResumeSandbox(t *testing.T, o *Orchestrator, sid string) (*types.Sandbox, string) {
	t.Helper()
	manifestKey := strings.Repeat("3", 64)
	apiSecret, apiKey := defaultTestCredentials(t, manifestKey)
	sb := &types.Sandbox{
		ID: sid, Profile: types.ProfileBare, State: types.StatePaused,
		FloatingIP: "192.0.2.10", APISecret: apiSecret, ManifestKey: manifestKey,
		SnapshotRef: "/checkpoints/" + sid + "/" + sid + ".sandbox", ResumeKind: types.ResumeSandbox,
		TemplateID: types.TemplateID{Profile: types.ProfileBare, Kind: types.KindImg, Ref: "manifest://" + strings.Repeat("a", 64)}.String(),
	}
	materializeTestSandboxCredentials(t, sb)
	if err := o.st.Put(context.Background(), sb); err != nil {
		t.Fatal(err)
	}
	return sb, apiKey
}

// In-process router: a paused sandbox with a Sandbox artifact (E) resume
// source is a cold start owned by explicit Connect — data-plane and exec
// activation must fail with proxy.ErrColdSandbox instead of auto-booting
// (sandboxer#143 §1.3), matching the external proxyshm worker behavior.
func TestInProcessRouterNeverWakesColdSandboxSource(t *testing.T) {
	o := testOrch(t)
	sb, _ := coldResumeSandbox(t, o, "cold-paused")
	target := proxy.LegacyTarget(8080)

	binding, found, err := o.LookupRoute(context.Background(), sb.ID, target)
	if err != nil || !found {
		t.Fatalf("LookupRoute = %+v, %v, %v", binding, found, err)
	}
	if _, _, err := o.ActivateRoute(context.Background(), binding); !errors.Is(err, proxy.ErrColdSandbox) {
		t.Fatalf("ActivateRoute err = %v, want ErrColdSandbox", err)
	}
	if _, _, err := o.ActivateExec(context.Background(), sb.ID, execIdentity(sb)); !errors.Is(err, proxy.ErrColdSandbox) {
		t.Fatalf("ActivateExec err = %v, want ErrColdSandbox", err)
	}
	// The cold rejection must not have started a resume: durable state stays
	// paused with the E ref intact.
	stored, err := o.st.Get(context.Background(), sb.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.State != types.StatePaused || stored.ResumeKind != types.ResumeSandbox || stored.RunID != "" {
		t.Fatalf("cold rejection changed durable state: %+v", stored)
	}

	// Snapshot-kind paused sandboxes keep the historical auto-resume.
	sb.ResumeKind = types.ResumeSnapshot
	sb.SnapshotRef = "/checkpoints/" + sb.ID + "/" + sb.ID + ".snapshot"
	if err := o.st.Put(context.Background(), sb); err != nil {
		t.Fatal(err)
	}
	o.mutateCached(sb.ID, func(cached *types.Sandbox) {
		cached.ResumeKind = types.ResumeSnapshot
		cached.SnapshotRef = sb.SnapshotRef
	})
	binding, found, err = o.LookupRoute(context.Background(), sb.ID, target)
	if err != nil || !found {
		t.Fatalf("LookupRoute(snapshot) = %+v, %v, %v", binding, found, err)
	}
	if _, _, err := o.ActivateRoute(context.Background(), binding); errors.Is(err, proxy.ErrColdSandbox) {
		t.Fatal("snapshot-kind paused route rejected as cold")
	}
}

// A persisted disk-only preference is only executable in local checkpoint
// mode; create rejects it for other modes instead of leaving the reaper
// failing on an unsupported pause forever.
func TestValidateCreateCheckpointModeRejectsDiskOnlyOutsideLocalMode(t *testing.T) {
	metadata := map[string]string{
		sandboxcfg.NsCheckpoint: `{"memory":false}`,
	}
	bundleCfg := checkpointOrchestratorConfig(t, config.CheckpointBundle)
	if err := (&Orchestrator{cfg: bundleCfg}).validateCreateCheckpointMode(metadata); err == nil ||
		!strings.Contains(err.Error(), "memory=false requires checkpoint.mode=local") {
		t.Fatalf("bundle-mode disk-only policy error = %v", err)
	}

	localCfg := checkpointOrchestratorConfig(t, config.CheckpointLocal)
	if err := (&Orchestrator{cfg: localCfg}).validateCreateCheckpointMode(metadata); err != nil {
		t.Fatalf("local-mode disk-only policy rejected: %v", err)
	}
}

// Export and template conversion of a disk-only paused sandbox are rejected:
// its node-bound Sandbox artifact E has no memory to publish, and importing
// the published ref would silently restore it as a memory snapshot.
func TestExportSandboxRejectsSandboxKindSource(t *testing.T) {
	o := migrationOrchestrator(t, t.TempDir(), []byte("runtime"))
	sb, apiKey := coldResumeSandbox(t, o, "export-cold")
	for _, toTemplate := range []bool{false, true} {
		if _, err := o.ExportSandbox(context.Background(), apiKey, sb.ID, toTemplate, false); err == nil ||
			!strings.Contains(err.Error(), "node-bound Sandbox artifact") {
			t.Fatalf("ExportSandbox(toTemplate=%v, sandbox-kind) err = %v, want node-bound artifact rejection", toTemplate, err)
		}
	}
}
