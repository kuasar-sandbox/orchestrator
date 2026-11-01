package orch

import (
	"context"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func (o *Orchestrator) DeleteCheckpoint(ctx context.Context, apiKey, sid, checkpointID string) error {
	ctx, unlock, err := o.lockLifecycleMutation(ctx, sid)
	if err != nil {
		return err
	}
	defer unlock()
	sb, err := o.st.Get(ctx, sid)
	if err != nil {
		return err
	}
	if !ownsSandbox(sb, apiKey) {
		return api.ErrNotFound
	}
	changed, err := o.st.DeleteCheckpoint(ctx, sid, checkpointID)
	if err != nil {
		return err
	}
	if !changed {
		return api.ErrNotFound
	}
	o.cleanupConsumedCheckpoint(ctx, sb)
	return nil
}

// Logical consumption releases the quota. Physical artifacts remain owned by
// the sandbox and are collected online or by its next offline cleanup. Its
// current ResumeSource may still require the same files after consumption.
func (o *Orchestrator) cleanupConsumedCheckpoint(ctx context.Context, sb *types.Sandbox) {
	if sb.State == types.StatePaused {
		if err := o.cleanupCheckpoint(ctx, sb); err != nil {
			o.log.Warn("consumed checkpoint cleanup deferred", "sid", sb.ID, "err", err)
		}
	}
	o.requestDiskCheckpointRelease(sb.ID)
}
