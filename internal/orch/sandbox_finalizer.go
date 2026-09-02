package orch

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/kuasar-sandbox/orchestrator/internal/nodepath"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/orchestrator/internal/vswitch"
)

// acceptSandboxDeleteLocked commits the only durable delete acceptance point.
// The caller holds the sandbox lifecycle lock. The local cache is withdrawn
// after the transition and Range excludes deleting rows. The terminal route
// event is intentionally delayed until the common finalizer has removed every
// exact local owner and hard-deleted the row.
func (o *Orchestrator) acceptSandboxDeleteLocked(ctx context.Context, sb *types.Sandbox) (bool, error) {
	if sb == nil {
		return false, nil
	}
	changed, err := o.st.BeginSandboxDelete(ctx, sb)
	if err != nil {
		return false, err
	}
	if !changed {
		return false, fmt.Errorf("orch: sandbox %s changed before delete acceptance", sb.ID)
	}

	o.launches.Cancel(sb.ID)
	o.clearDeadlineIntent(sb.ID)
	o.uncache(sb.ID)
	o.startSandboxDeleteFinalizer(sb.ID)
	return true, nil
}

// startSandboxDeleteFinalizer coalesces repeated direct/cluster Delete calls.
// Once deleting is durable, cleanup outlives the request context and retries
// until success. DrainSandboxDeletes keeps process dependencies open for these
// accepted finalizers during orderly shutdown; a crash is recovered from the
// unchanged deleting row by ReconcileSandboxes.
func (o *Orchestrator) startSandboxDeleteFinalizer(sid string) {
	finish, err := o.deleteOps.Begin(context.Background())
	if err != nil {
		// Admission can be closed only during shutdown. The deleting row remains
		// the restart owner, so declining a new goroutine cannot lose cleanup.
		return
	}

	o.deleteMu.Lock()
	if o.deleteActive == nil {
		o.deleteActive = make(map[string]struct{})
	}
	if _, exists := o.deleteActive[sid]; exists {
		o.deleteMu.Unlock()
		finish()
		return
	}
	o.deleteActive[sid] = struct{}{}
	o.deleteMu.Unlock()

	go func() {
		defer finish()
		delay := launchCleanupRetryMin
		for {
			if err := o.finalizeSandboxDeleteOnce(context.Background(), sid); err != nil {
				o.log.Error("sandbox delete cleanup incomplete; retrying",
					"sid", sid, "retry_in", delay, "err", err)
				waitLaunchCleanupRetry(delay)
				delay = nextLaunchCleanupRetry(delay)
				continue
			}

			retired, err := o.retireSandboxDeleteWorker(sid)
			if err != nil {
				o.log.Error("sandbox delete worker retirement failed; retrying",
					"sid", sid, "retry_in", delay, "err", err)
				waitLaunchCleanupRetry(delay)
				delay = nextLaunchCleanupRetry(delay)
				continue
			}
			if retired {
				return
			}
			// A successor with the same node-local SandboxID reached deleting
			// after the previous row was hard-deleted but before this worker
			// retired. Keep the coalesced worker and finalize the new exact row.
			delay = launchCleanupRetryMin
		}
	}()
}

// retireSandboxDeleteWorker closes the same-ID successor handoff race. The
// lifecycle lock covers the final durable read and deleteActive removal: a
// later Delete either leaves a deleting row for this worker to consume or sees
// the map entry removed and starts a new worker.
func (o *Orchestrator) retireSandboxDeleteWorker(sid string) (bool, error) {
	unlock := o.lifecycle.Lock(sid)
	defer unlock()

	current, err := o.st.Get(context.Background(), sid)
	if err != nil {
		return false, err
	}
	if current != nil && current.State == types.StateDeleting {
		return false, nil
	}
	o.deleteMu.Lock()
	delete(o.deleteActive, sid)
	o.deleteMu.Unlock()
	return true, nil
}

// DrainSandboxDeletes prevents new finalizer goroutines and waits for every
// already-accepted deleting row to reach its hard-delete commit.
func (o *Orchestrator) DrainSandboxDeletes(ctx context.Context) error {
	return o.deleteOps.Drain(ctx)
}

// finalizeSandboxDeleteOnce performs one complete, ordered finalizer pass. It
// deliberately does not clear per-step fields: the unchanged deleting row is
// the compact durable retry record required after any failure or crash.
func (o *Orchestrator) finalizeSandboxDeleteOnce(ctx context.Context, sid string) error {
	o.launches.Cancel(sid)
	if found, err := o.launches.Wait(ctx, sid); found && err != nil && ctx.Err() != nil {
		return ctx.Err()
	}

	unlock := o.lifecycle.Lock(sid)
	defer unlock()

	sb, err := o.st.Get(ctx, sid)
	if err != nil {
		return err
	}
	if sb == nil || sb.State != types.StateDeleting {
		return nil
	}
	if err := o.validateSandboxCleanupPaths(sb); err != nil {
		return err
	}

	cleanupCtx, cancel := cleanupContext()
	defer cancel()
	if sb.RunID != "" {
		if err := o.fenceSandboxRunner(cleanupCtx, sb.RunID); err != nil {
			return err
		}
	}
	if sb.VswitchPort != "" {
		if err := o.detachSandboxPort(cleanupCtx, sb.VswitchPort); err != nil {
			return err
		}
	}
	removeRunDir := o.removeSandboxRunDir
	if removeRunDir == nil {
		removeRunDir = os.RemoveAll
	}
	if sb.RunDir != "" {
		if err := removeRunDir(sb.RunDir); err != nil {
			return fmt.Errorf("remove sandbox RunDir %s: %w", sb.RunDir, err)
		}
	}
	removeBaseDir := o.removeSandboxBaseDir
	if removeBaseDir == nil {
		removeBaseDir = os.RemoveAll
	}
	if sb.BaseDir != "" {
		if err := removeBaseDir(sb.BaseDir); err != nil {
			return fmt.Errorf("remove sandbox BaseDir %s: %w", sb.BaseDir, err)
		}
	}

	deleted, err := o.st.DeleteFinalizedSandbox(cleanupCtx, sb)
	if err != nil {
		return err
	}
	if !deleted {
		return fmt.Errorf("orch: sandbox %s deleting ownership changed before hard delete", sid)
	}
	if sb.VswitchPort != "" {
		o.networkAllocationMu.Lock()
		delete(o.detachedPortsPending, sb.VswitchPort)
		o.networkAllocationMu.Unlock()
	}
	o.clearDeadlineIntent(sid)
	o.uncache(sid)
	o.publishDelete(sid)
	o.observeSandboxDelete(sb)
	return nil
}

func (o *Orchestrator) validateSandboxCleanupPaths(sb *types.Sandbox) error {
	if sb == nil || !types.ValidLocalSandboxID(sb.ID) {
		return errors.New("orch: sandbox cleanup requires a valid sandbox identity")
	}
	if sb.VswitchPort == "" && (sb.FloatingIP != "" || sb.InnerIP != "" || sb.PortMAC != "") {
		return fmt.Errorf("orch: sandbox %s has network ownership without an exact VSwitch port", sb.ID)
	}
	if !sandboxHasLocalOwnership(sb) {
		// A fully cleaned dead history row may itself be explicitly deleted.
		return nil
	}
	wantRunDir := nodepath.SandboxRunDir(o.cfg.Paths.RunRoot, sb.ID)
	wantBaseDir := nodepath.SandboxBaseDir(o.cfg.Paths.BaseRoot, sb.ID)
	if sb.RunDir != "" && sb.RunDir != wantRunDir {
		return fmt.Errorf("orch: sandbox %s cleanup RunDir %q does not match canonical %q",
			sb.ID, sb.RunDir, wantRunDir)
	}
	if sb.BaseDir != wantBaseDir {
		return fmt.Errorf("orch: sandbox %s cleanup BaseDir %q does not match canonical %q",
			sb.ID, sb.BaseDir, wantBaseDir)
	}
	if sb.RunDir == "" && (sb.RunID != "" || sb.VswitchPort != "" || sb.FloatingIP != "" ||
		sb.InnerIP != "" || sb.PortMAC != "" || sb.EnvdUDS != "" || sb.CiUDS != "") {
		return fmt.Errorf("orch: sandbox %s has runtime ownership without a RunDir", sb.ID)
	}
	if sb.EnvdUDS != "" && sb.EnvdUDS != filepath.Join(wantRunDir, "envd.sock") {
		return fmt.Errorf("orch: sandbox %s envd UDS %q is outside canonical RunDir", sb.ID, sb.EnvdUDS)
	}
	if sb.CiUDS != "" && sb.CiUDS != filepath.Join(wantRunDir, "ci.sock") {
		return fmt.Errorf("orch: sandbox %s ci UDS %q is outside canonical RunDir", sb.ID, sb.CiUDS)
	}
	return nil
}

func sandboxHasLocalOwnership(sb *types.Sandbox) bool {
	return sb != nil && (sb.RunID != "" || sb.VswitchPort != "" || sb.FloatingIP != "" ||
		sb.InnerIP != "" || sb.PortMAC != "" || sb.RunDir != "" || sb.BaseDir != "" ||
		sb.EnvdUDS != "" || sb.CiUDS != "" || !sb.ResumeSource.Empty())
}

// fenceSandboxRunner proves the exact unit can no longer execute before any
// directory is removed. An already missing/inactive unit is an equivalent Stop
// result, but ResetFailed and the final liveness readback remain mandatory.
func (o *Orchestrator) fenceSandboxRunner(ctx context.Context, runID string) error {
	unit := o.runnerUnit(runID)
	if err := o.lc.Stop(ctx, unit); err != nil {
		active, listErr := o.sandboxUnitActive(ctx, unit)
		if listErr != nil {
			return errors.Join(fmt.Errorf("stop %s: %w", unit, err), listErr)
		}
		if active {
			return fmt.Errorf("stop %s: %w", unit, err)
		}
	}
	if err := o.lc.ResetFailed(ctx, unit); err != nil {
		return fmt.Errorf("reset %s: %w", unit, err)
	}
	active, err := o.sandboxUnitActive(ctx, unit)
	if err != nil {
		return err
	}
	if active {
		return fmt.Errorf("sandbox runner unit %s remained active after stop/reset", unit)
	}
	return nil
}

func (o *Orchestrator) sandboxUnitActive(ctx context.Context, unit string) (bool, error) {
	units, err := o.lc.List(ctx, unit)
	if err != nil {
		return false, fmt.Errorf("list sandbox runner unit %s: %w", unit, err)
	}
	for _, current := range units {
		if current.Name == unit && builderUnitMayHaveProcesses(current.ActiveState) {
			return true, nil
		}
	}
	return false, nil
}

// detachSandboxPort keeps allocation excluded until the deleting row is gone.
// Without this fence, a filesystem/store failure after Detach could let a retry
// detach a connector slot that had already been reassigned to another owner.
func (o *Orchestrator) detachSandboxPort(ctx context.Context, port string) error {
	o.networkAllocationMu.Lock()
	defer o.networkAllocationMu.Unlock()
	if err := o.vs.Detach(ctx, port); err != nil && !errors.Is(err, vswitch.ErrPortNotAttached) {
		return fmt.Errorf("detach sandbox port %s: %w", port, err)
	}
	if o.detachedPortsPending == nil {
		o.detachedPortsPending = make(map[string]struct{})
	}
	o.detachedPortsPending[port] = struct{}{}
	return nil
}

func (o *Orchestrator) releaseDetachedPortFence(port string) {
	if port == "" {
		return
	}
	o.networkAllocationMu.Lock()
	delete(o.detachedPortsPending, port)
	o.networkAllocationMu.Unlock()
}
