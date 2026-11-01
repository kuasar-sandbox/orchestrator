package orch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"

	"github.com/kuasar-sandbox/orchestrator/internal/nodepath"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/sandboxer/pkg/artifact"
)

// cleanupCheckpoint runs under the SID lifecycle lock, after runner fencing,
// while RunDir still durably owns paused cleanup. Artifact interpretation stays
// in the tenant-key-bound tool process, outside conductor.
func (o *Orchestrator) cleanupCheckpoint(ctx context.Context, sb *types.Sandbox) error {
	_, err := o.inspectOrCleanupCheckpoint(ctx, sb, false)
	return err
}

func (o *Orchestrator) inspectOrCleanupCheckpoint(ctx context.Context, sb *types.Sandbox, inspect bool) ([]artifact.DiffCheckpointRef, error) {
	source := sb.ResumeSource
	if source.Empty() || types.IsPortableRef(source.Ref) {
		return nil, nil
	}
	current, err := o.st.Get(ctx, sb.ID)
	if err != nil {
		return nil, err
	}
	if current == nil || current.InstanceID != sb.InstanceID || current.State != sb.State || (!inspect && current.State != types.StatePaused) || current.ResumeSource != sb.ResumeSource || current.RunDir != sb.RunDir {
		return nil, fmt.Errorf("orch: checkpoint source ownership changed for %s", sb.ID)
	}
	dir, err := o.ownedLocalArtifactDir(sb, source)
	if err != nil {
		return nil, err
	}
	// An export can retain this source while publishing outside the lock. Do not
	// wait for its completion here: publication needs this lock to finalize.
	ctx, release, err := o.trySourceOperation(ctx, sb)
	if err != nil {
		return nil, err
	}
	defer release()
	// Legacy/recovered rows may have no checkpoint directory. There are then no
	// candidates to remove; an I/O or identity failure is not equivalent.
	if _, err := os.Lstat(dir); os.IsNotExist(err) && !inspect {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	args := []string{"checkpoint-cleanup", "--sandbox-id", sb.ID, "--path-id", sb.ID,
		"--base-root", nodepath.SandboxBaseRoot(o.cfg.Paths.BaseRoot), "--manifest-config", o.cfg.ManifestConfig}
	e := source.Ref
	if source.Kind == types.ResumeSourceSnapshot {
		e = source.SandboxRef
		args = append(args, "--restore", source.Ref)
	}
	args = append(args, "--from", e)
	retained, err := o.st.Checkpoint(ctx, sb.ID)
	if err != nil {
		return nil, err
	}
	if retained != nil {
		pair := artifact.CheckpointSource{SandboxRef: retained.Source.Ref}
		if retained.Source.Kind == types.ResumeSourceSnapshot {
			pair.SnapshotRef, pair.SandboxRef = retained.Source.Ref, retained.Source.SandboxRef
		}
		encoded, err := json.Marshal([]artifact.CheckpointSource{pair})
		if err != nil {
			return nil, err
		}
		args = append(args, "--retain", string(encoded))
	}
	args = append(args, "--ref-location-parent", o.cfg.Checkpoint.Remote.RefLocationParent)
	if inspect {
		args = append(args, "--list-versions")
	}
	cmd := exec.CommandContext(ctx, o.executables.OrchestratorCtl(), args...)
	inheritSourceOperation(ctx, cmd)
	cmd.Env = append(os.Environ(), "MANIFEST_KEY="+sb.ManifestKey)
	var stderr, stdout bytes.Buffer
	cmd.Stderr = &stderr
	cmd.Stdout = &stdout
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("orch: checkpoint inspection/cleanup %s: %w: %s", sb.ID, err, stderr.String())
	}
	var refs []artifact.DiffCheckpointRef
	if inspect {
		if err := json.Unmarshal(stdout.Bytes(), &refs); err != nil {
			return nil, err
		}
	}
	return refs, nil
}
