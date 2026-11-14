package orch

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/migrationtoken"
	"github.com/kuasar-sandbox/orchestrator/internal/strictjson"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/orchestrator/internal/workspace"
	"github.com/kuasar-sandbox/orchestrator/internal/workspacedisk"
	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
)

// Workspace operations can outlive a client disconnect while a child or owner
// finishes I/O. Keep the database alive and cancel their work on node shutdown.
func (o *Orchestrator) workspaceOperation(ctx context.Context) (context.Context, func(), error) {
	lifecycle := o.launchContext()
	finish, err := o.acceptedOps.Begin(lifecycle)
	if err != nil {
		return ctx, nil, api.ErrWorkspaceUnavailable
	}
	child, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(lifecycle, cancel)
	return child, func() { stop(); cancel(); finish() }, nil
}

func (o *Orchestrator) ExportWorkspace(ctx context.Context, id, apiKey, name string) (string, error) {
	ctx, finish, err := o.workspaceOperation(ctx)
	if err != nil {
		return "", err
	}
	defer finish()
	// Keep both the SID lifecycle fence and applicable on-disk ownership grant
	// until publication completes. Resume, Reset, deletion and checkpoint GC
	// cannot release a selected version or change its subtree underneath us.
	guard, release, err := o.lockLifecycleMutation(ctx, id)
	if err != nil {
		return "", err
	}
	defer release()
	sb, err := o.st.Get(guard, id)
	if err != nil {
		return "", err
	}
	if !ownsSandbox(sb, apiKey) {
		return "", api.ErrNotFound
	}
	if !workspaceDeclared(sb, name) && (len(sb.WorkspacePlan) > 0 || sb.Metadata[workspace.Namespace] == "") {
		return "", api.ErrNotFound
	}
	if sb.State != types.StatePaused {
		return "", api.ErrWorkspaceConflict
	}
	result, err := o.workspaceArchiveOperation(guard, sb, "export", name)
	if err != nil {
		return "", err
	}
	if result.Disk == nil {
		return "", api.ErrWorkspaceUnavailable
	}
	return migrationtoken.SealWorkspace(migrationtoken.KeyMaterial{APISecret: sb.APISecret, ManifestKey: sb.ManifestKey}, *result.Disk)
}

func (o *Orchestrator) workspaceArchiveOperation(ctx context.Context, sb *types.Sandbox, operation, name string) (workspacedisk.Result, error) {
	req := workspacedisk.Request{Operation: operation, Name: name, ManifestConfig: o.cfg.ManifestConfig, RefLocationParent: o.cfg.Checkpoint.Remote.RefLocationParent, Publication: sb.StableID(), Source: sb.ResumeSource.Ref}
	if !sb.ResumeSource.Valid() {
		return workspacedisk.Result{}, api.ErrWorkspaceUnavailable
	}
	if sb.ResumeSource.Kind == types.ResumeSourceSnapshot {
		req.Source = sb.ResumeSource.SandboxRef
	}
	if !types.IsPortableRef(req.Source) {
		dir, err := o.ownedLocalArtifactDir(sb, sb.ResumeSource)
		if err != nil {
			return workspacedisk.Result{}, err
		}
		if g := sourceGrant(ctx); g == nil || g.file == nil {
			return workspacedisk.Result{}, api.ErrWorkspaceUnavailable
		}
		req.SourceDir = dir
		if filepath.IsAbs(req.Source) {
			req.Source = (manifest.Ref{Scheme: manifest.RefSchemeFile, Path: req.Source}).String()
		}
	}
	return o.callWorkspaceStorage(ctx, sb.ManifestKey, req)
}

func (o *Orchestrator) callWorkspaceStorage(ctx context.Context, key string, req workspacedisk.Request) (workspacedisk.Result, error) {
	var result workspacedisk.Result
	raw, err := json.Marshal(req)
	if err != nil {
		return result, err
	}
	cmd := exec.CommandContext(ctx, o.executables.OrchestratorCtl(), "workspace-storage")
	cmd.Env = append(os.Environ(), manifest.CustomerKeyEnv+"="+key)
	inheritSourceOperation(ctx, cmd)
	output := &snapshotCheckOutput{limit: rtconfig.MaxPortableConfigBytes}
	if err := runSnapshotRuntimeHelper(ctx, cmd, raw, output, io.Discard); err != nil || output.truncated {
		if ctx.Err() != nil {
			return result, ctx.Err()
		}
		return result, api.ErrWorkspaceUnavailable
	}
	if err := strictjson.Decode(output.Bytes(), &result); err != nil {
		return result, api.ErrWorkspaceUnavailable
	}
	switch result.Error {
	case "":
		return result, nil
	case "not_found":
		return result, api.ErrNotFound
	case "conflict":
		return result, api.ErrWorkspaceConflict
	case "permission":
		return result, api.ErrNotAllowed
	default:
		return result, api.ErrWorkspaceUnavailable
	}
}
