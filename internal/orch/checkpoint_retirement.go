package orch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/nodectl"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/sandboxer/pkg/artifact"
	"github.com/kuasar-sandbox/sandboxer/pkg/ctl"
	"github.com/kuasar-sandbox/sandboxer/pkg/memorycheckpoint"
)

var errRetirementPending = errors.New("checkpoint runtime retirement is pending")
var errCaptureOutcomePending = errors.New("checkpoint capture outcome requires reconciliation")
var errCaptureNotStarted = errors.New("checkpoint capture command was not started")

func retirementIsPending(err error) bool {
	return errors.Is(err, errRetirementPending) || errors.Is(err, errCaptureOutcomePending) || errors.Is(err, api.ErrPreviousMemorySavePending)
}

// ConfigureMemoryPressure publishes the controller before Listen restores its
// inventory. During that interval, absence in State is not release evidence.
// Keep the exact capture/owner available for the existing lifecycle retry.
func (o *Orchestrator) retirementResourceState() (*nodectl.State, error) {
	p := o.memoryPressure.Load()
	if p != nil {
		if p.state.InventoryReady() {
			return p.state, nil
		}
		return nil, fmt.Errorf("%w: resource controller inventory is not ready", errRetirementPending)
	}
	if o.cfg.ResourceListen != nil && o.cfg.ResourceListen.Enabled {
		return nil, fmt.Errorf("%w: resource controller is unavailable", errRetirementPending)
	}
	return nil, nil
}

func (o *Orchestrator) ensureRetiredMemoryReleased(ctx context.Context, sid string) error {
	if o.st == nil {
		return nil // A host without durable ownership cannot issue a permit.
	}
	rows, err := o.st.RunPoolOwnerships(ctx)
	if err != nil {
		return err
	}
	for _, m := range rows {
		if m.TaskID != sid || m.RetirementOperationID == "" {
			continue
		}
		record, err := o.recoverRunCapture(ctx, m)
		if err != nil {
			return err
		}
		if record == nil || !record.RAMReleased {
			if record != nil && record.Aborted {
				continue
			}
			o.startSandboxRunEndCheck(m.RunID)
			return api.ErrPreviousMemorySavePending
		}
		// The record lives under BaseDir. Finish the exact old ownership before
		// deleting that directory, otherwise a retry would lose its proof.
		if err := o.releaseRetiredRun(ctx, m, record); err != nil {
			return err
		}
	}
	return nil
}

// This exact-run check also protects indirect cleanup paths such as a failed
// launch or pool reassignment. A successor can still be stopped while an older
// run of the same SID holds frozen RAM.
func (o *Orchestrator) ensureRunnerMayFence(ctx context.Context, runID string) error {
	if runID == "" || o.st == nil {
		return nil
	}
	m, found, err := o.st.RunPoolOwnership(ctx, runID)
	if err != nil || !found || m.RetirementOperationID == "" {
		return err
	}
	record, err := o.readRunCapture(ctx, m)
	if err != nil {
		return err
	}
	if record == nil || !record.Aborted && !record.RAMReleased {
		return api.ErrPreviousMemorySavePending
	}
	return nil
}

func (o *Orchestrator) releaseRetiredRun(ctx context.Context, m store.RunPoolOwnership, record *artifact.CaptureRecord) error {
	if record == nil || !record.RAMReleased {
		return api.ErrPreviousMemorySavePending
	}
	state, err := o.retirementResourceState()
	if err != nil {
		return err
	}
	if state != nil {
		if err := state.ReconcileRetainedMemory(m.TaskID); err != nil {
			return err
		}
		if retained, ok := state.RetainedMemory(m.TaskID); ok && retained.MemoryID == record.MemoryID && retained.Epoch == record.Epoch {
			// The controller must persist settlement while its capture proof is
			// still readable. A later hard delete may remove that proof.
			return api.ErrPreviousMemorySavePending
		}
	}
	if err := o.fenceSandboxRunner(ctx, m.RunID); err != nil {
		return err
	}
	if !m.ParentRetired {
		if _, err := o.mutateRetirement(ctx, m.RunID, func(current *store.RunPoolOwnership) error {
			if current.RetirementOperationID != m.RetirementOperationID {
				return errors.New("retired owner changed during release")
			}
			current.ParentRetired = true
			return nil
		}); err != nil {
			return err
		}
	}
	if err := memorycheckpoint.RemoveHolderDirectory(*record); err != nil {
		// RAM and its resource charge are already gone, and the exact old
		// unit is fenced. Keep only this directory cleanup responsibility;
		// it cannot block controller startup or re-create a RAM obligation.
		return errors.Join(errRetirementPending, err)
	}
	return o.runs.forget(m.RunID)
}

func (o *Orchestrator) discardRetiredMemoryForDelete(ctx context.Context, sb *types.Sandbox) error {
	rows, err := o.st.RunPoolOwnerships(ctx)
	if err != nil {
		return err
	}
	var retiring []store.RunPoolOwnership
	for _, m := range rows {
		if m.TaskID == sb.ID && m.RetirementOperationID != "" {
			retiring = append(retiring, m)
		}
	}
	if len(retiring) == 0 {
		return nil
	}
	g := sourceGrant(ctx)
	if sb.State != types.StateDeleting || g == nil || g.sid != sb.ID || g.instanceID != sb.InstanceID || g.baseDir != sb.BaseDir {
		return errors.New("discarding checkpoint memory requires deleting source ownership")
	}
	// Fence the new runtime as well as old holders: a resumed VM may still own
	// an immutable predecessor memfd. No proof is published from disconnection.
	fenced := map[string]bool{}
	if sb.RunID != "" {
		if err := o.fenceSandboxRunnerUnit(ctx, sb.RunID); err != nil {
			return err
		}
		fenced[sb.RunID] = true
	}
	for _, m := range retiring {
		if !fenced[m.RunID] {
			if err := o.fenceSandboxRunnerUnit(ctx, m.RunID); err != nil {
				return err
			}
			fenced[m.RunID] = true
		}
	}
	for _, m := range retiring {
		producer := artifact.CurrentRun{SandboxID: m.TaskID, InstanceID: m.RetirementInstanceID, RunID: m.RunID}
		dir := filepath.Join(m.RetirementBaseDir, "checkpoint")
		record, err := artifact.DiscardCheckpointCapture(ctx, dir, producer, m.RetirementCheckpointID)
		if errors.Is(err, os.ErrNotExist) {
			if err := o.discardAbsentCapture(ctx, m, dir); err != nil {
				return err
			}
			continue
		}
		if err != nil {
			return errors.Join(errRetirementPending, err)
		}
		if err := o.releaseRetiredRun(ctx, m, record); err != nil {
			return err
		}
	}
	return nil
}

func (o *Orchestrator) discardAbsentCapture(ctx context.Context, m store.RunPoolOwnership, dir string) error {
	pin, err := memorycheckpoint.ExclusiveDirectory(dir)
	if err != nil {
		return errors.Join(errRetirementPending, err)
	}
	defer pin.Close()
	// All exact units are fenced and no new reader can attach under the source
	// grant. Recheck absence after taking the reader fence, then require the
	// resource controller to confirm there is no prepared/holding receipt.
	record, err := o.readRunCapture(ctx, m)
	if err != nil {
		return err
	}
	if record != nil {
		return errCaptureOutcomePending
	}
	state, err := o.retirementResourceState()
	if err != nil {
		return err
	}
	if state != nil {
		if _, pending := state.RetainedMemory(m.TaskID); pending {
			return api.ErrPreviousMemorySavePending
		}
	}
	return o.runs.forget(m.RunID)
}

func retirementCommandEnvironment(permit *ctl.HostRetirementPermit) ([]string, error) {
	const prefix = "SANDBOX_CHECKPOINT_RETIREMENT="
	env := make([]string, 0, len(os.Environ())+1)
	for _, value := range os.Environ() {
		if !strings.HasPrefix(value, prefix) {
			env = append(env, value)
		}
	}
	if permit != nil {
		raw, err := json.Marshal(permit)
		if err != nil {
			return nil, err
		}
		env = append(env, prefix+string(raw))
	}
	return env, nil
}

func runRetirementEvent(m store.RunPoolOwnership) configsock.RunRetirementEvent {
	return configsock.RunRetirementEvent{OperationID: m.RetirementOperationID, SandboxID: m.TaskID,
		InstanceID: m.RetirementInstanceID, RunID: m.RunID, CheckpointID: m.RetirementCheckpointID,
		MemoryID: m.MemoryID, Epoch: m.MemoryEpoch}
}

func (o *Orchestrator) readRunCapture(ctx context.Context, m store.RunPoolOwnership) (*artifact.CaptureRecord, error) {
	if m.RetirementOperationID == "" || m.RetirementCheckpointID == "" || m.RetirementBaseDir == "" {
		return nil, nil
	}
	run := artifact.CurrentRun{SandboxID: m.TaskID, InstanceID: m.RetirementInstanceID, RunID: m.RunID}
	var attempt []string
	if m.MemoryID != "" {
		attempt = []string{m.MemoryID}
	}
	record, err := artifact.ReadCheckpointCapture(ctx, filepath.Join(m.RetirementBaseDir, "checkpoint"), run, m.RetirementCheckpointID, attempt...)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, errors.Join(errCaptureOutcomePending, err)
	}
	if record == nil {
		return nil, nil
	}
	if record.HostOperationID != m.RetirementOperationID {
		return nil, errors.Join(errCaptureOutcomePending, errors.New("capture host operation identity mismatch"))
	}
	if m.MemoryID != "" && (record.MemoryID != m.MemoryID || record.Epoch != m.MemoryEpoch) {
		return nil, errors.Join(errCaptureOutcomePending, errors.New("capture memory generation changed"))
	}
	return record, nil
}

// Recover only from the SDK's exact writer/PID/RAM-pin proof. In particular,
// neither a missing service nor an exited process alone may release ownership.
// All proof locks are nonblocking, including when a lifecycle caller owns SID.
func (o *Orchestrator) recoverRunCapture(ctx context.Context, m store.RunPoolOwnership) (*artifact.CaptureRecord, error) {
	record, err := o.readRunCapture(ctx, m)
	if err != nil || record == nil || record.Committed && !record.RuntimeRetired || record.RAMReleased || record.Aborted || record.Discarded {
		return record, err
	}
	path, err := memorycheckpoint.RecordPath(filepath.Join(m.RetirementBaseDir, "checkpoint"), m.RetirementCheckpointID, record.MemoryID)
	if err != nil {
		return nil, err
	}
	recovered, err := memorycheckpoint.RecoverMemoryRelease(ctx, path, *record)
	if err != nil {
		return nil, err
	}
	record = &recovered.Record
	if recovered.ProducerExited && !record.Aborted && !record.FileReady && !record.RAMReleased {
		// Existing readers can still own frozen RAM. Keep the physical obligation
		// retryable without advertising this source as a successful new capture.
		return record, errors.Join(api.ErrPreviousMemorySavePending,
			fmt.Errorf("%w: checkpoint %s memory %s", memorycheckpoint.ErrMemoryUnavailable, record.CheckpointID, record.MemoryID))
	}
	return record, nil
}

// mutateRetirement serializes with full run retirement and reads the latest
// durable row before updating it. In particular a delayed prepare/commit never
// overwrites a parent's already-persisted ACK.
func (o *Orchestrator) mutateRetirement(ctx context.Context, runID string, mutate func(*store.RunPoolOwnership) error) (store.RunPoolOwnership, error) {
	o.runs.retireMu.Lock()
	defer o.runs.retireMu.Unlock()
	m, found, err := o.st.RunPoolOwnership(ctx, runID)
	if err != nil {
		return m, err
	}
	if !found {
		return m, fmt.Errorf("run %s has no durable owner", runID)
	}
	if err := mutate(&m); err != nil {
		return m, err
	}
	if err := o.st.SaveRunPoolOwnership(ctx, m); err != nil {
		return m, err
	}
	o.runs.mu.Lock()
	if o.runs.metadata == nil {
		o.runs.metadata = map[string]store.RunPoolOwnership{}
	}
	o.runs.metadata[runID] = m
	o.runs.mu.Unlock()
	return m, nil
}

func (o *Orchestrator) prepareCheckpointRetirement(ctx context.Context, sb *types.Sandbox, checkpointID string) (*ctl.HostRetirementPermit, error) {
	if plan := captureAdmissionFor(ctx, sb); plan != nil && plan.checked && plan.retentionSupported != nil && !*plan.retentionSupported {
		return nil, nil
	}
	if sb == nil || sb.InstanceID == "" || sb.RunID == "" || checkpointID == "" {
		return nil, nil
	}
	o.runSessionsMu.Lock()
	session := o.runSessions[runSessionKey{kind: runKindSandbox, runID: sb.RunID}]
	known, supported := session != nil, session != nil && session.holderRetirement
	o.runSessionsMu.Unlock()
	if !known {
		if o.allowLegacyAssignmentWithoutRunSession {
			return nil, nil
		}
		return nil, errRetirementPending
	}
	if !supported {
		return nil, nil
	}
	// Establish the stable checkpoint directory before the host intent. Even a
	// crash before command dispatch can then take its reader lock during delete.
	if err := os.MkdirAll(filepath.Join(sb.BaseDir, "checkpoint"), 0700); err != nil {
		return nil, err
	}
	permit := ctl.HostRetirementPermit{Version: 1, SandboxID: sb.ID, InstanceID: sb.InstanceID, RunID: sb.RunID, CheckpointID: checkpointID, OperationID: checkpointID}
	_, err := o.mutateRetirement(ctx, sb.RunID, func(m *store.RunPoolOwnership) error {
		if m.Kind != runKindSandbox || (m.TaskID != "" && m.TaskID != sb.ID) {
			return errors.New("runner retirement owner mismatch")
		}
		if m.RetirementOperationID != "" && m.RetirementOperationID != permit.OperationID {
			return errRetirementPending
		}
		m.TaskID = sb.ID
		m.RetirementOperationID, m.RetirementCheckpointID = permit.OperationID, checkpointID
		m.RetirementInstanceID, m.RetirementBaseDir = sb.InstanceID, sb.BaseDir
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &permit, nil
}

func (o *Orchestrator) abortCheckpointRetirement(ctx context.Context, sb *types.Sandbox, permit ctl.HostRetirementPermit) error {
	state, err := o.retirementResourceState()
	if err != nil {
		return err
	}
	if state != nil {
		if err := state.ReconcileRetainedMemory(sb.ID); err != nil {
			return err
		}
		if _, pending := state.RetainedMemory(sb.ID); pending {
			return api.ErrPreviousMemorySavePending
		}
	}
	_, err = o.mutateRetirement(ctx, sb.RunID, func(m *store.RunPoolOwnership) error {
		if m.TaskID != sb.ID || m.RetirementOperationID != permit.OperationID || m.Role != "" {
			return errors.New("cannot abort committed runner retirement")
		}
		m.RetirementOperationID, m.RetirementCheckpointID, m.RetirementInstanceID = "", "", ""
		m.RetirementBaseDir = ""
		m.MemoryID, m.MemoryEpoch, m.RetainCheckpoint, m.ParentRetired = "", 0, false, false
		return nil
	})
	return err
}

func captureSource(record *artifact.CaptureRecord) (types.ResumeSource, error) {
	if record != nil && record.Lost() {
		return types.ResumeSource{}, fmt.Errorf("%w: checkpoint %s memory %s", memorycheckpoint.ErrMemoryLost, record.CheckpointID, record.MemoryID)
	}
	if record == nil || !record.Committed || !record.RuntimeRetired || record.Discarded {
		return types.ResumeSource{}, errCaptureOutcomePending
	}
	source := types.ResumeSource{Kind: types.ResumeSourceSnapshot, Ref: record.SnapshotRef, SandboxRef: record.SandboxRef}
	if !source.Valid() {
		return types.ResumeSource{}, errors.Join(errCaptureOutcomePending, errors.New("committed capture has invalid source"))
	}
	return source, nil
}

// recoverCaptureOutcome handles the producer/CLI acknowledgement gap. Only an
// explicit rejection, an aborted record, or a completed synchronous capture may
// erase the prepared host intent. EOF cannot revive an irreversibly retired VM.
func (o *Orchestrator) recoverCaptureOutcome(ctx context.Context, sb *types.Sandbox, permit *ctl.HostRetirementPermit, source types.ResumeSource, captureErr error) (types.ResumeSource, error) {
	if permit == nil {
		return source, captureErr
	}
	m, found, err := o.st.RunPoolOwnership(ctx, sb.RunID)
	if err != nil || !found {
		return types.ResumeSource{}, errors.Join(errCaptureOutcomePending, captureErr, err)
	}
	record, err := o.recoverRunCapture(ctx, m)
	if err != nil {
		return types.ResumeSource{}, errors.Join(errCaptureOutcomePending, captureErr, err)
	}
	if record != nil && record.Committed {
		if !record.RuntimeRetired {
			record, err = o.awaitCapturedRuntime(ctx, m, record)
			if err != nil {
				return types.ResumeSource{}, errors.Join(errCaptureOutcomePending, captureErr, err)
			}
			record, err = o.recoverRunCapture(ctx, m)
			if err != nil {
				return types.ResumeSource{}, errors.Join(errCaptureOutcomePending, captureErr, err)
			}
		}
		fixed, err := captureSource(record)
		if err != nil {
			return types.ResumeSource{}, errors.Join(errCaptureOutcomePending, captureErr, err)
		}
		if captureErr == nil && source != fixed {
			return types.ResumeSource{}, errors.Join(errCaptureOutcomePending, errors.New("capture response differs from committed source"))
		}
		return fixed, nil
	}
	var exitErr *exec.ExitError
	rejected := errors.Is(captureErr, errCaptureNotStarted) || errors.As(captureErr, &exitErr) && exitErr.ExitCode() == 79
	if captureErr == nil && record == nil || record != nil && record.Aborted || rejected {
		if err := o.abortCheckpointRetirement(ctx, sb, *permit); err != nil {
			return types.ResumeSource{}, errors.Join(errCaptureOutcomePending, captureErr, err)
		}
		return source, captureErr
	}
	return types.ResumeSource{}, errors.Join(errCaptureOutcomePending, captureErr)
}

func (o *Orchestrator) awaitCapturedRuntime(ctx context.Context, m store.RunPoolOwnership, record *artifact.CaptureRecord) (*artifact.CaptureRecord, error) {
	// Same bounded cleanup budget as paused ownership. The durable producer
	// record and existing retry worker retain responsibility after this waiter.
	waitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	for record != nil && record.Committed && !record.RuntimeRetired {
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-timer.C:
		case <-waitCtx.Done():
			timer.Stop()
			return nil, waitCtx.Err()
		}
		var err error
		record, err = o.readRunCapture(waitCtx, m)
		if err != nil {
			return nil, err
		}
	}
	if record == nil || !record.Committed || !record.RuntimeRetired {
		return nil, errCaptureOutcomePending
	}
	return record, nil
}

func (o *Orchestrator) capturedHolderOwnership(ctx context.Context, sb *types.Sandbox) (*store.RunPoolOwnership, error) {
	m, found, err := o.st.RunPoolOwnership(ctx, sb.RunID)
	if err != nil || !found || m.RetirementOperationID == "" {
		return nil, err
	}
	record, err := o.recoverRunCapture(ctx, m)
	if err != nil {
		return nil, err
	}
	if _, err := captureSource(record); err != nil {
		return nil, err
	}
	m.Role, m.MemoryID, m.MemoryEpoch = store.RunRoleMemoryHolder, record.MemoryID, record.Epoch
	return &m, nil
}

func (o *Orchestrator) commitPausedCapture(ctx context.Context, sb *types.Sandbox, source types.ResumeSource, checkpointID string) (bool, error) {
	o.runs.retireMu.Lock()
	defer o.runs.retireMu.Unlock()
	holder, err := o.capturedHolderOwnership(ctx, sb)
	if err != nil {
		return false, err
	}
	changed, err := o.st.CommitPausedWithRetirement(ctx, sb.ID, sb.RunID, source, checkpointID, holder)
	if err != nil || !changed || holder == nil {
		return changed, err
	}
	o.runs.mu.Lock()
	if o.runs.metadata == nil {
		o.runs.metadata = map[string]store.RunPoolOwnership{}
	}
	o.runs.metadata[sb.RunID] = *holder
	o.runs.mu.Unlock()
	return true, nil
}

// RunRetirementRole is called by the authenticated parent. It deliberately
// takes neither SID nor source locks: capture owns them while waiting for ACK.
func (o *Orchestrator) RunRetirementRole(ctx context.Context, kind, runID string, ack *configsock.RunRetirementEvent) (configsock.RunRoleResponse, error) {
	if kind != runKindSandbox {
		return configsock.RunRoleResponse{}, errors.New("only sandbox runners may retire as holders")
	}
	m, found, err := o.st.RunPoolOwnership(ctx, runID)
	if err != nil {
		return configsock.RunRoleResponse{}, err
	}
	if !found || m.RetirementOperationID == "" {
		if ack != nil {
			return configsock.RunRoleResponse{}, errors.New("unknown retirement event")
		}
		return configsock.RunRoleResponse{}, nil
	}
	out := configsock.RunRoleResponse{Pending: true}
	if m.Role == store.RunRoleMemoryHolder {
		event := runRetirementEvent(m)
		out.Retirement, out.Acknowledged = &event, m.ParentRetired
	}
	if ack == nil {
		return out, nil
	}
	if out.Retirement == nil || *out.Retirement != *ack {
		return out, errors.New("runner retirement acknowledgement changed")
	}
	firstAck := false
	m, err = o.mutateRetirement(ctx, runID, func(current *store.RunPoolOwnership) error {
		if current.Role != store.RunRoleMemoryHolder || runRetirementEvent(*current) != *ack {
			return errors.New("runner retirement identity changed")
		}
		firstAck = !current.ParentRetired
		current.ParentRetired = true
		return nil
	})
	if err != nil {
		return out, err
	}
	if firstAck {
		o.log.Info("checkpoint parent retirement acknowledged", "sid", ack.SandboxID,
			"run_id", runID, "memory_id", ack.MemoryID, "epoch", ack.Epoch)
	}
	o.runSessionsMu.Lock()
	if o.retirementWake != nil {
		close(o.retirementWake)
	}
	o.retirementWake = make(chan struct{})
	o.runSessionsMu.Unlock()
	o.startSandboxRunEndCheck(runID)
	if err := o.retireHolderExecution(m); err != nil {
		return out, err
	}
	out.Acknowledged = true
	return out, nil
}

// The frozen holder retains its durable unit identity and source-pool memory
// charge, but no longer owns an execution placement. RetireExecution matches
// the exact old run, so replay cannot remove a successor's placement or budget.
// A prepared resource receipt still protects B until CommitRetained converts
// it to R; keep paused cleanup pending if that commit has not completed yet.
func (o *Orchestrator) retireHolderExecution(m store.RunPoolOwnership) error {
	if m.Role != store.RunRoleMemoryHolder || !m.ParentRetired || m.TaskID == "" || m.RunID == "" {
		return errRetirementPending
	}
	state, err := o.retirementResourceState()
	if err != nil {
		return err
	}
	if state != nil {
		if err := state.RetireExecution(m.TaskID, m.RunID); err != nil {
			return errors.Join(errRetirementPending, err)
		}
	}
	o.runs.releaseRuntimeSlot(m.RunID)
	return nil
}

func (o *Orchestrator) awaitParentRetirement(ctx context.Context, m store.RunPoolOwnership) error {
	for {
		o.runSessionsMu.Lock()
		if o.retirementWake == nil {
			o.retirementWake = make(chan struct{})
		}
		wake := o.retirementWake
		session := o.runSessions[runSessionKey{kind: runKindSandbox, runID: m.RunID}]
		if session != nil && session.holderRetirement {
			select {
			case session.retirements <- runRetirementEvent(m):
			default:
			}
		}
		o.runSessionsMu.Unlock()
		current, found, err := o.st.RunPoolOwnership(ctx, m.RunID)
		if err != nil {
			return err
		}
		if !found || runRetirementEvent(current) != runRetirementEvent(m) {
			return errors.New("runner retirement owner changed")
		}
		if current.ParentRetired {
			return o.retireHolderExecution(current)
		}
		if session == nil || !session.holderRetirement {
			return errRetirementPending
		}
		select {
		case <-wake:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (o *Orchestrator) commitCheckpointRetirement(ctx context.Context, sb *types.Sandbox, permit ctl.HostRetirementPermit, capture artifact.CaptureRecord) error {
	if !capture.Committed || !capture.RuntimeRetired || capture.HostOperationID != permit.OperationID || capture.CheckpointID != permit.CheckpointID || capture.Producer.SandboxID != sb.ID || capture.Producer.InstanceID != sb.InstanceID || capture.Producer.RunID != sb.RunID {
		return errCaptureOutcomePending
	}
	m, found, err := o.st.RunPoolOwnership(ctx, sb.RunID)
	if err != nil {
		return err
	}
	if !found || m.Role != store.RunRoleMemoryHolder || m.MemoryID != capture.MemoryID || m.MemoryEpoch != capture.Epoch {
		return errors.New("missing committed holder role")
	}
	return o.awaitParentRetirement(ctx, m)
}

func (o *Orchestrator) cleanupRetiredRunner(ctx context.Context, sb *types.Sandbox) (bool, error) {
	m, found, err := o.st.RunPoolOwnership(ctx, sb.RunID)
	if err != nil {
		return false, err
	}
	if !found || m.RetirementOperationID == "" {
		return false, nil
	}
	record, err := o.readRunCapture(ctx, m)
	if err != nil {
		return true, err
	}
	if record == nil || !record.Committed || !record.RuntimeRetired || m.Role != store.RunRoleMemoryHolder {
		return true, errCaptureOutcomePending
	}
	permit := ctl.HostRetirementPermit{Version: 1, SandboxID: sb.ID, InstanceID: sb.InstanceID, RunID: sb.RunID, CheckpointID: m.RetirementCheckpointID, OperationID: m.RetirementOperationID}
	if err := o.commitCheckpointRetirement(ctx, sb, permit, *record); err != nil {
		return true, err
	}
	o.startSandboxRunEndCheck(sb.RunID)
	return true, nil
}

// Uses the existing bounded run-end worker/retry queue. Holder disconnection is
// never interpreted as ordinary runtime failure or physical memory release.
func (o *Orchestrator) reconcileRetiredRun(ctx context.Context, observed store.RunPoolOwnership) error {
	current, found, err := o.st.RunPoolOwnership(ctx, observed.RunID)
	if err != nil || !found {
		return err
	}
	if current.RetirementOperationID == "" {
		return nil
	}
	record, err := o.recoverRunCapture(ctx, current)
	if err != nil {
		return err
	}
	if record == nil {
		return o.reconcileUnstartedCapture(ctx, current)
	}
	sandbox, err := o.st.Get(ctx, current.TaskID)
	if err != nil {
		return err
	}
	if record.Aborted {
		unlock := o.lifecycle.Lock(current.TaskID)
		defer unlock()
		// A killed preparation cannot perform the SDK's ordinary private
		// directory cleanup. Keep its exact owner until this cleanup succeeds;
		// never follow or remove the successor's canonical RunDir.
		if err := memorycheckpoint.RemoveHolderDirectory(*record); err != nil {
			return errors.Join(errRetirementPending, err)
		}
		producer := &types.Sandbox{ID: current.TaskID, RunID: current.RunID}
		if err := o.abortCheckpointRetirement(ctx, producer, ctl.HostRetirementPermit{OperationID: current.RetirementOperationID}); err != nil {
			return err
		}
		sb, err := o.st.Get(ctx, current.TaskID)
		if err != nil {
			return err
		}
		if sb != nil && sb.InstanceID == current.RetirementInstanceID && sb.RunID == current.RunID && sb.State == types.StatePausing {
			running := cloneSandbox(sb)
			running.State = types.StateRunning
			o.rollbackPause(ctx, running)
		}
		// Recheck the ordinary runner after dropping its retirement intent. It
		// may have exited while its parent waited for the unknown capture role.
		return errRetirementPending
	}
	if sandbox != nil && sandbox.State == types.StatePausing && sandbox.RunID == current.RunID && sandbox.InstanceID == current.RetirementInstanceID {
		unlock := o.lifecycle.Lock(current.TaskID)
		defer unlock()
		sb, err := o.st.Get(ctx, current.TaskID)
		if err != nil {
			return err
		}
		if sb == nil || sb.InstanceID != current.RetirementInstanceID || sb.RunID != current.RunID || sb.State != types.StatePausing {
			return errCaptureOutcomePending
		}
		if record.Lost() {
			// Retirement is irreversible. Keep the last usable historical source
			// and finish this exact run as dead, never as an unusable paused S.
			o.log.Warn("checkpoint memory lost after runtime retirement", "sid", sb.ID,
				"run_id", current.RunID, "memory_id", record.MemoryID, "epoch", record.Epoch)
			return o.finalizeRunningSandboxDeadLocked(ctx, sb)
		}
		source, err := captureSource(record)
		if err != nil {
			return err
		}
		checkpointID := ""
		if current.RetainCheckpoint {
			checkpointID = current.RetirementCheckpointID
		}
		changed, err := o.commitPausedCapture(ctx, sb, source, checkpointID)
		if err != nil {
			return err
		}
		if !changed {
			return errCaptureOutcomePending
		}
		paused, err := o.st.Get(ctx, sb.ID)
		if err != nil {
			return err
		}
		o.cache(paused)
		o.publishUpsert(paused)
		o.observeSandboxUpsert(paused)
		o.startPausedCleanupRetry(sb.ID)
		return errRetirementPending
	}
	if current.Role != store.RunRoleMemoryHolder {
		if !record.Lost() {
			if _, err := captureSource(record); err != nil {
				return err
			}
		}
		// A delete may have superseded the unknown pause outcome. Preserve the
		// committed holder identity without resurrecting the deleting sandbox.
		current, err = o.mutateRetirement(ctx, current.RunID, func(m *store.RunPoolOwnership) error {
			if m.RetirementOperationID != record.HostOperationID {
				return errors.New("capture operation changed during recovery")
			}
			m.Role, m.MemoryID, m.MemoryEpoch = store.RunRoleMemoryHolder, record.MemoryID, record.Epoch
			return nil
		})
		if err != nil {
			return err
		}
	}
	if current.ParentRetired {
		if err := o.retireHolderExecution(current); err != nil {
			return err
		}
	}
	if err := o.releaseRetiredRun(ctx, current, record); err != nil {
		return err
	}
	// A fast Pause cannot collect while its holder pins the directory. The
	// existing run-end worker gets a real collection opportunity once RAM and
	// the holder are gone, even when ordinary RunDir ownership is already clear.
	o.cleanupCheckpointAfterMemorySave(ctx, current)
	return nil
}

// Called only from the unlocked, completed-retirement branch above. A cleanup
// failure owns only garbage under BaseDir; it must not resurrect a retired run
// or retain its resource charge. Running successors keep their active refs.
func (o *Orchestrator) cleanupCheckpointAfterMemorySave(ctx context.Context, retired store.RunPoolOwnership) {
	unlock := o.lifecycle.Lock(retired.TaskID)
	defer unlock()
	sb, err := o.st.Get(ctx, retired.TaskID)
	if err == nil && sb != nil && sb.State == types.StatePaused && sb.InstanceID == retired.RetirementInstanceID && sb.BaseDir == retired.RetirementBaseDir {
		err = o.cleanupCheckpoint(ctx, sb)
	}
	if err != nil {
		o.log.Warn("checkpoint garbage collection after memory save deferred", "sid", retired.TaskID, "err", err)
	}
}

// An intent can survive a conductor crash before the capture command reaches
// PrepareHolder. Absence alone is not a release fact: fence preparation and
// the exact producer while holding the SID/source authority, then recheck.
func (o *Orchestrator) reconcileUnstartedCapture(ctx context.Context, observed store.RunPoolOwnership) error {
	unlock := o.lifecycle.Lock(observed.TaskID)
	defer unlock()
	current, found, err := o.st.RunPoolOwnership(ctx, observed.RunID)
	if err != nil {
		return err
	}
	if !found || current.RetirementOperationID == "" {
		return nil
	}
	if !sameUnstartedCapture(current, observed) {
		return errCaptureOutcomePending
	}
	sb, err := o.st.Get(ctx, current.TaskID)
	if err != nil {
		return err
	}
	if sb == nil || sb.State != types.StatePausing || sb.RunID != current.RunID ||
		sb.InstanceID != current.RetirementInstanceID || sb.BaseDir != current.RetirementBaseDir {
		return errCaptureOutcomePending
	}
	ctx, release, err := o.trySourceOperation(ctx, sb)
	if err != nil {
		return errors.Join(errRetirementPending, err)
	}
	defer release()
	preparation, err := memorycheckpoint.LockPreparation(filepath.Join(current.RetirementBaseDir, "checkpoint"), current.RetirementCheckpointID)
	if err != nil {
		return errors.Join(errCaptureOutcomePending, err)
	}
	defer preparation.Close()
	checkAbsent := func() error {
		record, err := o.readRunCapture(ctx, current)
		if err != nil {
			return err
		}
		if record != nil {
			return errCaptureOutcomePending
		}
		state, err := o.retirementResourceState()
		if err != nil {
			return err
		}
		if state != nil {
			if _, pending := state.RetainedMemory(current.TaskID); pending {
				return api.ErrPreviousMemorySavePending
			}
		}
		return nil
	}
	if err := checkAbsent(); err != nil {
		return err
	}
	// The preparation lock excludes a delayed holder transition. This is the
	// only non-delete path allowed to fence an intent without a capture fact.
	if err := o.fenceSandboxRunnerUnit(ctx, current.RunID); err != nil {
		return errors.Join(errRetirementPending, err)
	}
	latest, found, err := o.st.RunPoolOwnership(ctx, current.RunID)
	if err != nil {
		return err
	}
	if !found || !sameUnstartedCapture(latest, current) {
		return errCaptureOutcomePending
	}
	if err := checkAbsent(); err != nil {
		return err
	}
	if err := o.abortCheckpointRetirement(ctx, sb, ctl.HostRetirementPermit{OperationID: current.RetirementOperationID}); err != nil {
		return err
	}
	return o.finalizeRunningSandboxDeadLocked(ctx, sb)
}

func sameUnstartedCapture(a, b store.RunPoolOwnership) bool {
	return a.Role == "" && b.Role == "" && a.MemoryID == "" && b.MemoryID == "" && a.MemoryEpoch == 0 && b.MemoryEpoch == 0 &&
		!a.ParentRetired && !b.ParentRetired && a.Kind == runKindSandbox && b.Kind == runKindSandbox &&
		a.RunID == b.RunID && a.TaskID == b.TaskID && a.RetirementOperationID == b.RetirementOperationID &&
		a.RetirementCheckpointID == b.RetirementCheckpointID && a.RetirementInstanceID == b.RetirementInstanceID &&
		a.RetirementBaseDir == b.RetirementBaseDir
}
