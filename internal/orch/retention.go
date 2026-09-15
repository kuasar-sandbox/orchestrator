package orch

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/store"
)

// reapTerminalHistory deletes one bounded batch of owner-free terminal rows.
// SQLite is the node lifecycle authority; live projections are notifications
// emitted only after the exact durable delete commits.
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
		deleted, deleteErr := o.st.DeleteDeadSandboxForRetention(ctx, sandbox, cutoffUnix)
		if deleteErr != nil {
			errs = append(errs, deleteErr)
		} else if deleted {
			o.uncache(sandbox.ID)
			o.publishDelete(sandbox.ID)
			o.observeSandboxDelete(sandbox)
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
