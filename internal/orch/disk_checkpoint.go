package orch

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/sandboxer/pkg/ctl"
)

type diskCheckpointReleaseWork struct{ revision uint64 }

type diskCheckpointSelection struct {
	runID, baseDir, instanceID string
	createdUnix                int64
	refs                       []ctl.DiffCheckpointRef
	listed                     bool
}

// requestDiskCheckpointRelease is triggered by successful launch, actual export
// completion, and startup adoption, never by every reaper tick. Failures cannot
// roll back the already committed running sandbox or publication result.
func (o *Orchestrator) requestDiskCheckpointRelease(sid string) {
	lifecycleCtx := o.launchContext()
	finish, err := o.acceptedOps.Begin(lifecycleCtx)
	if err != nil {
		return
	}
	o.diskReleaseMu.Lock()
	if o.diskReleaseActive == nil {
		o.diskReleaseActive = make(map[string]*diskCheckpointReleaseWork)
	}
	if work := o.diskReleaseActive[sid]; work != nil {
		work.revision++
		o.diskReleaseMu.Unlock()
		finish()
		return
	}
	work := &diskCheckpointReleaseWork{revision: 1}
	o.diskReleaseActive[sid] = work
	o.diskReleaseMu.Unlock()
	go func() {
		defer finish()
		defer func() {
			o.diskReleaseMu.Lock()
			if o.diskReleaseActive[sid] == work {
				delete(o.diskReleaseActive, sid)
			}
			o.diskReleaseMu.Unlock()
		}()
		revision := uint64(1)
		selection := &diskCheckpointSelection{}
		delay := launchCleanupRetryMin
		for lifecycleCtx.Err() == nil {
			ctx, cancel := context.WithTimeout(context.WithoutCancel(lifecycleCtx), 5*time.Second)
			err := o.releaseDiskCheckpointsOnce(ctx, sid, selection)
			cancel()
			if err != nil {
				o.log.Warn("disk checkpoint release incomplete; retrying", "sid", sid, "retry_in", delay, "err", err)
				if !waitSandboxCleanupRetry(lifecycleCtx, delay) {
					return
				}
				delay = nextLaunchCleanupRetry(delay)
				continue
			}
			o.diskReleaseMu.Lock()
			if work.revision == revision {
				delete(o.diskReleaseActive, sid)
				o.diskReleaseMu.Unlock()
				return
			}
			revision = work.revision
			o.diskReleaseMu.Unlock()
			// Only an independent trigger starts a new selection. A partial
			// or uncertain Delete retries the exact prior list, even when
			// another trigger arrived while that deletion was in flight.
			selection = &diskCheckpointSelection{}
			delay = launchCleanupRetryMin
		}
	}()
}

// The lifecycle lock orders List/Delete with capture and terminal admission.
// Runtime owns DiffStore interpretation; conductor only forwards complete,
// immutable {DiffID, ID} selections, preserving active bases and memory ancestry.
func (o *Orchestrator) releaseDiskCheckpointsOnce(ctx context.Context, sid string, selected *diskCheckpointSelection) error {
	unlock := o.lifecycle.Lock(sid)
	defer unlock()
	sb, err := o.st.Get(ctx, sid)
	if err != nil {
		return err
	}
	if sb == nil || sb.State != types.StateRunning || sb.ExecutionResult != nil || (sb.RunID != "" && sb.DeadRunID == sb.RunID) {
		return nil
	}
	if sb.BaseDir == "" || sb.RunDir == "" || sb.RunID == "" {
		return nil
	}
	if err := o.validateSandboxCleanupPaths(sb); err != nil {
		return err
	}
	if selected.listed && (selected.instanceID != sb.InstanceID || selected.runID != sb.RunID || selected.baseDir != sb.BaseDir || selected.createdUnix != sb.CreatedUnix) {
		return nil // A new launch must supply its own release trigger.
	}
	ctx, release, err := o.trySourceOperation(ctx, sb)
	if err != nil {
		return err
	}
	defer release()
	socket := filepath.Join(sb.RunDir, "ctl.sock")
	if !selected.listed {
		refs, err := ctl.ListCheckpoints(ctx, socket)
		if errors.Is(err, ctl.ErrCheckpointUnsupported) {
			// Explicitly unsupported runtimes need no online version cleanup.
			// Finish this check; independent triggers still recheck capability.
			return nil
		}
		if err != nil {
			return fmt.Errorf("list disk checkpoints: %w", err)
		}
		retained, err := o.st.Checkpoints(ctx, sid)
		if err != nil {
			return err
		}
		// ResumeSource remains parent-owned even after a successful launch. Its
		// named versions must survive runner Close, rollback and future Reset.
		protected, err := o.inspectOrCleanupCheckpoint(ctx, sb, true)
		if err != nil {
			return err
		}
		keep := map[ctl.DiffCheckpointRef]bool{}
		for _, ref := range protected {
			keep[ctl.CheckpointRef{DiffID: ref.DiffID, MemoryID: ref.MemoryID, ID: ref.ID}] = true
		}
		named := make(map[string]bool, len(retained))
		for _, checkpoint := range retained {
			named[checkpoint.ID] = true
		}
		for _, ref := range refs {
			if !keep[ref] && !named[ref.ID] {
				selected.refs = append(selected.refs, ref)
			}
		}
		selected.runID, selected.baseDir, selected.createdUnix = sb.RunID, sb.BaseDir, sb.CreatedUnix
		selected.instanceID = sb.InstanceID
		selected.listed = true
	}
	sources := ctl.CheckpointSources{Retain: []ctl.CheckpointSource{}}
	set := func(source types.ResumeSource) ctl.CheckpointSource {
		out := ctl.CheckpointSource{SandboxRef: source.Ref}
		if source.Kind == types.ResumeSourceSnapshot {
			out.SnapshotRef, out.SandboxRef = source.Ref, source.SandboxRef
		}
		return out
	}
	primary := set(sb.ResumeSource)
	sources.SnapshotRef, sources.SandboxRef = primary.SnapshotRef, primary.SandboxRef
	retained, err := o.st.Checkpoints(ctx, sid)
	if err != nil {
		return err
	}
	for _, capture := range retained {
		sources.Retain = append(sources.Retain, set(capture.Source))
	}
	if err := ctl.DeleteCheckpoints(ctx, socket, "", selected.refs, sources); err != nil {
		return fmt.Errorf("delete selected disk checkpoints: %w", err)
	}
	return nil
}
