package orch

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// reapTerminalHistory admits expired Sandbox parents to their deleting finalizer
// and deletes owner-free Build history in bounded batches. SQLite is the lifecycle
// authority; management projection remains until the exact parent delete commits.
func (o *Orchestrator) reapTerminalHistory(ctx context.Context, now time.Time) error {
	var errs []error
	if err := o.reapRequestedBuilds(ctx); err != nil {
		errs = append(errs, err)
	}
	if ttl := o.cfg.Sandbox.DeadTTLDur(); ttl <= 0 {
		errs = append(errs, errors.New("sandbox.dead_ttl is not a positive duration"))
	} else if err := o.reapDeadSandboxes(ctx, now.Add(-ttl).Unix()); err != nil {
		errs = append(errs, err)
	}
	// Rebuild work from durable responsibility, including a dead commit whose
	// publication or worker admission was interrupted. Never invent a new ID.
	if err := o.st.RangeByState(ctx, types.StateDead, func(sb *types.Sandbox) error {
		if sb.ResetState == "pending" {
			o.startSandboxReset(sb, false)
		}
		return nil
	}); err != nil {
		errs = append(errs, err)
	}
	if ttl := o.cfg.Builder.TerminalTTLDur(); ttl <= 0 {
		errs = append(errs, errors.New("builder.terminal_ttl is not a positive duration"))
	} else if err := o.reapTerminalBuilds(ctx, now.Add(-ttl).Unix()); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

func (o *Orchestrator) reapDeadSandboxes(ctx context.Context, cutoffUnix int64) error {
	candidates, err := o.st.DeadSandboxesForRetention(ctx, cutoffUnix, store.TerminalRetentionBatchLimit)
	if err != nil {
		return err
	}
	var errs []error
	for _, sandbox := range candidates {
		unlock := o.lifecycle.Lock(sandbox.ID)
		guard, release, deleteErr := o.trySourceOperation(ctx, sandbox)
		deleted := false
		if deleteErr == nil {
			deleted, deleteErr = o.st.DeleteDeadSandboxForRetention(guard, sandbox, cutoffUnix)
			release()
		}
		if deleteErr != nil {
			errs = append(errs, deleteErr)
		} else if deleted {
			o.uncache(sandbox.ID)
			if current, err := o.st.Get(ctx, sandbox.ID); err == nil && current != nil {
				o.publishUpsert(current)
			}
			o.startSandboxDeleteFinalizer(sandbox.ID)
		}
		unlock()
	}
	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("reap dead Sandbox history: %w", err)
	}
	return nil
}

func (o *Orchestrator) reapTerminalBuilds(ctx context.Context, cutoffUnix int64) error {
	candidates, err := o.st.TerminalBuildsForRetention(ctx, cutoffUnix, store.TerminalRetentionBatchLimit)
	if err != nil {
		return err
	}
	var errs []error
	for _, build := range candidates {
		unlock := o.buildRetention.Lock(build.BuildID)
		unlockEvent := o.lockBuildEvent(build.BuildID)
		deleted, deleteErr := o.st.DeleteTerminalBuildForRetention(ctx, build, cutoffUnix)
		if deleteErr != nil {
			errs = append(errs, deleteErr)
		} else if deleted {
			o.clusterBuildMu.Lock()
			delete(o.clusterBuilds, build.BuildID)
			o.clusterBuildMu.Unlock()
			o.publishBuildDelete(build)
			o.observeBuildRemove(build)
		}
		unlockEventFence(unlockEvent)
		unlock()
	}
	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("reap terminal Build history: %w", err)
	}
	return nil
}
