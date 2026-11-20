package orch

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/sandboxer/pkg/ctl"
)

// lockPauseAdmission leaves a pending runtime running. Neither its lifecycle
// lock nor the cross-process source grant is retained during the cancellable
// wait. A request that waited for one run may never pause its successor.
func (o *Orchestrator) lockPauseAdmission(ctx context.Context, sid string, request sandboxcfg.CaptureRequest, validate func(*types.Sandbox) error) (context.Context, func(), *types.Sandbox, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	var expected *types.Sandbox
	for {
		guardCtx, unlock, err := o.lockLifecycleMutation(ctx, sid)
		if err != nil {
			return ctx, nil, nil, err
		}
		sb, err := o.st.Get(guardCtx, sid)
		if err == nil && validate != nil {
			err = validate(sb)
		}
		if err == nil && expected != nil && (sb == nil || sb.ID != expected.ID || sb.InstanceID != expected.InstanceID || sb.RunID != expected.RunID || sb.State != types.StateRunning) {
			err = api.ErrPauseRunChanged
		}
		if err != nil {
			unlock()
			return ctx, nil, nil, err
		}
		if sb == nil || sb.State != types.StateRunning {
			return guardCtx, unlock, sb, nil
		}
		pending, err := o.previousMemorySavePending(guardCtx, sb, true)
		if err != nil || !pending {
			if err != nil {
				unlock()
				return ctx, nil, nil, err
			}
			return guardCtx, unlock, sb, nil
		}
		expected = cloneSandbox(sb)
		unlock()
		if request.NoWaitForPreviousMemorySave {
			return ctx, nil, nil, api.ErrPreviousMemorySavePending
		}
		if _, err := o.previousMemorySavePending(ctx, expected, false); err != nil {
			return ctx, nil, nil, err
		}
	}
}

func (o *Orchestrator) previousMemorySavePending(ctx context.Context, sb *types.Sandbox, noWait bool) (bool, error) {
	if sb == nil {
		return false, nil
	}
	// Pre-checkpoint legacy rows cannot own a new managed holder. A retained
	// resource receipt still fences them, including a restored older base.
	if sb.InstanceID == "" || sb.RunID == "" {
		return o.retainedMemoryPending(ctx, sb.ID)
	}
	read := o.captureStateReader
	if read == nil {
		read = ctl.ReadCaptureState
	}
	if noWait {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
	}
	state, err := read(ctx, filepath.Join(sb.RunDir, "ctl.sock"), ctl.CaptureStateQuery{
		SandboxID: sb.ID, InstanceID: sb.InstanceID, RunID: sb.RunID,
		NoWaitForPreviousMemorySave: noWait,
	})
	if errors.Is(err, ctl.ErrCaptureStateUnsupported) {
		pending, pendingErr := o.retainedMemoryPending(ctx, sb.ID)
		if pendingErr != nil {
			return false, pendingErr
		}
		if pending {
			return true, api.ErrPreviousMemorySavePending
		}
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if state.SandboxID != sb.ID || state.InstanceID != sb.InstanceID || state.RunID != sb.RunID {
		return false, api.ErrPauseRunChanged
	}
	return state.Pending, nil
}

func (o *Orchestrator) retainedMemoryPending(ctx context.Context, sid string) (bool, error) {
	state, err := o.retirementResourceState()
	if err != nil {
		return true, err
	}
	if state != nil {
		if _, ok := state.RetainedMemory(sid); ok {
			return true, nil
		}
	}
	rows, err := o.st.RunPoolOwnerships(ctx)
	if err != nil {
		return false, err
	}
	for _, row := range rows {
		if row.TaskID != sid || row.RetirementOperationID == "" {
			continue
		}
		record, err := o.readRunCapture(ctx, row)
		if err != nil || record == nil || !record.Aborted && !record.RAMReleased {
			// Unknown is not ready. Leave the durable owner for reconciliation.
			return true, nil
		}
	}
	return false, nil
}

// A new runtime must explicitly understand the SID-wide predecessor obligation
// before it becomes publicly running. This also covers static deployments and
// restoring an older base with a downgraded runtime executable.
func (o *Orchestrator) requirePreviousMemorySaveSupport(ctx context.Context, sb *types.Sandbox) error {
	pending, err := o.retainedMemoryPending(ctx, sb.ID)
	if err != nil || !pending {
		return err
	}
	read := o.captureStateReader
	if read == nil {
		read = ctl.ReadCaptureState
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	state, err := read(ctx, filepath.Join(sb.RunDir, "ctl.sock"), ctl.CaptureStateQuery{
		SandboxID: sb.ID, InstanceID: sb.InstanceID, RunID: sb.RunID, NoWaitForPreviousMemorySave: true,
	})
	if err != nil {
		return fmt.Errorf("resume requires previous memory save support: %w", err)
	}
	if state.SandboxID != sb.ID || state.InstanceID != sb.InstanceID || state.RunID != sb.RunID {
		return api.ErrPauseRunChanged
	}
	return nil
}
