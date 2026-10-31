package orch

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// The caller holds the SID lifecycle lock. Wake is queued behind this entire
// operation, including atomic publication of a retained checkpoint ID.
func (o *Orchestrator) preparePause(ctx context.Context, sb *types.Sandbox, ifIdle bool) (retErr error) {
	if o.pauseBarriers == nil {
		return api.ErrProxyUnavailable
	}
	barrier, err := o.pauseBarriers.BeginProxyPauseBarrier()
	if err != nil {
		return errors.Join(api.ErrProxyUnavailable, err)
	}
	defer barrier.Cancel()
	pending := cloneSandbox(sb)
	pending.State = types.StatePausing
	o.pauseMu.Lock()
	if o.pausePreparing == nil {
		o.pausePreparing = make(map[string]*types.Sandbox)
	}
	o.pausePreparing[sb.ID] = pending
	o.pauseMu.Unlock()
	defer func() {
		o.pauseMu.Lock()
		delete(o.pausePreparing, sb.ID)
		o.pauseMu.Unlock()
		if retErr != nil {
			current, err := o.st.Get(context.WithoutCancel(ctx), sb.ID)
			if err == nil && current != nil {
				o.publishUpsert(current)
			}
		}
	}()
	// This ordinary upsert also covers new/reconnecting subscriptions. It is
	// followed by the synchronous worker sample on each existing ordered stream.
	o.publishUpsert(pending)
	o.fanout(routesync.Event{Kind: routesync.TypePauseBarrier, BarrierID: barrier.ID(), Route: o.routeEntry(pending)})
	waitCtx, cancel := context.WithTimeout(ctx, o.cfg.ParkTimeoutDur())
	defer cancel()
	if err := barrier.Wait(waitCtx); err != nil {
		return errors.Join(api.ErrProxyUnavailable, err)
	}
	if err := waitCtx.Err(); err != nil {
		return errors.Join(api.ErrProxyUnavailable, err)
	}
	activity := barrier.Activity()
	o.log.Info("pause proxy activity", "sid", sb.ID, "if_idle", ifIdle, "activity", activity.Activity)
	if ifIdle && !activity.Idle() {
		return api.ErrSandboxBusy
	}
	if err := barrier.Commit(); err != nil {
		return errors.Join(api.ErrProxyUnavailable, err)
	}
	changed, err := o.st.SetPausing(ctx, sb.ID, sb.RunID, true)
	if err != nil {
		return err
	}
	if !changed {
		return fmt.Errorf("%w: pause runner changed", api.ErrSandboxChanged)
	}
	o.cache(pending)
	o.observeSandboxUpsert(pending)
	return nil
}

func (o *Orchestrator) rollbackPause(ctx context.Context, sb *types.Sandbox) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	changed, err := o.st.SetPausing(ctx, sb.ID, sb.RunID, false)
	if err != nil {
		o.log.Warn("pause rollback incomplete", "sid", sb.ID, "err", err)
		return
	}
	if !changed {
		return
	}
	o.cache(sb)
	o.publishUpsert(sb)
	o.observeSandboxUpsert(sb)
}
