package orch

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func (o *Orchestrator) CancelBuild(ctx context.Context, apiKey, templateID, buildID string) (api.BuildActionResult, error) {
	if err := validateBuildIDRequest(buildID); err != nil {
		return api.BuildActionResult{}, err
	}
	return o.requestBuildAction(ctx, apiKey, templateID, buildID, false, true, false)
}

func (o *Orchestrator) DeleteBuild(ctx context.Context, apiKey, templateID string, options api.DeleteBuildOptions) (api.BuildActionResult, error) {
	return o.requestBuildAction(ctx, apiKey, templateID, "", true, options.Cancel, false)
}

func (o *Orchestrator) requestBuildAction(ctx context.Context, apiKey, templateID, buildID string, remove, cancel, admin bool) (api.BuildActionResult, error) {
	var result api.BuildActionResult
	if err := types.ValidateTransientID(templateID); err != nil {
		return result, fmt.Errorf("%w: %v", api.ErrBadRequest, err)
	}
	expected, err := o.st.GetBuildByTemplateID(ctx, templateID)
	if err != nil {
		return result, err
	}
	if expected == nil || (!admin && !ownsBuild(expected, apiKey)) || (buildID != "" && buildID != expected.BuildID) {
		return result, api.ErrNotFound
	}
	unlockRetention := o.buildRetention.Lock(expected.BuildID)
	defer unlockRetention()
	unlock := o.lockBuildEvent(expected.BuildID)
	defer unlock()
	build, deleted, pending, err := o.st.RequestBuildAction(ctx, expected, remove, cancel, time.Now())
	if errors.Is(err, store.ErrBuildDeleteConflict) {
		return result, api.ErrBuildOwned
	}
	if err != nil {
		return result, err
	}
	if build == nil {
		return result, api.ErrNotFound
	}
	result = api.BuildActionResult{BuildID: build.BuildID, TemplateID: build.TemplateID, Pending: pending}
	if deleted {
		o.finishBuildDeletion(build)
	} else {
		o.publishCommittedBuild(build)
		if build.Status == types.BuildError {
			o.observeBuildRemove(build)
		} else {
			o.observeBuildUpsert(build)
		}
		// The request only signals the sole owner. It never detaches/removes
		// resources concurrently with that owner's prepare/attach goroutine.
		o.pendMu.Lock()
		if owner := o.pend[build.BuildID]; owner != nil && owner.templateID == build.TemplateID && owner.cancelExecution != nil {
			owner.cancelExecution()
		}
		o.pendMu.Unlock()
	}
	o.refreshBuildAdmissionGauges(context.Background())
	o.buildCapacityChanged()
	return result, nil
}

// Called under the Build event fence, after the database hard-delete commit.
func (o *Orchestrator) finishBuildDeletion(build *types.Build) {
	o.clusterBuildMu.Lock()
	delete(o.clusterBuilds, build.BuildID)
	o.clusterBuildMu.Unlock()
	o.publishBuildDelete(build)
	o.observeBuildRemove(build)
}

func (o *Orchestrator) buildCapacityChanged() {
	select {
	case o.buildWake <- struct{}{}:
	default:
	}
	select {
	case o.buildUsageWake <- struct{}{}:
	default:
	}
}

func (o *Orchestrator) HeartbeatChanges() <-chan struct{} { return o.buildUsageWake }

func (o *Orchestrator) reapRequestedBuilds(ctx context.Context) error {
	builds, err := o.st.BuildsPendingDeletion(ctx, store.TerminalRetentionBatchLimit)
	if err != nil {
		return err
	}
	var errs []error
	for _, build := range builds {
		unlock := o.buildRetention.Lock(build.BuildID)
		event := o.lockBuildEvent(build.BuildID)
		deleted, err := o.st.DeleteRequestedBuild(ctx, build)
		if err != nil {
			errs = append(errs, err)
		} else if deleted {
			o.finishBuildDeletion(build)
		}
		event()
		unlock()
	}
	return errors.Join(errs...)
}

func (o *Orchestrator) releaseBuildOwner(build *types.Build, owner *pendingBuild) {
	o.pendMu.Lock()
	if o.pend[build.BuildID] == owner {
		delete(o.pend, build.BuildID)
	}
	if owner.cancelExecution != nil {
		owner.cancelExecution()
	}
	if owner.done != nil {
		close(owner.done)
	}
	o.pendMu.Unlock()
}

func (o *Orchestrator) CancelBuildAdmin(ctx context.Context, buildID string) (api.BuildActionResult, error) {
	if err := validateBuildIDRequest(buildID); err != nil {
		return api.BuildActionResult{}, err
	}
	build, err := o.st.GetBuild(ctx, buildID)
	if err != nil {
		return api.BuildActionResult{}, err
	}
	if build == nil {
		return api.BuildActionResult{}, api.ErrNotFound
	}
	return o.requestBuildAction(ctx, "", build.TemplateID, buildID, false, true, true)
}

func (o *Orchestrator) DeleteBuildAdmin(ctx context.Context, templateID string, options api.DeleteBuildOptions) (api.BuildActionResult, error) {
	return o.requestBuildAction(ctx, "", templateID, "", true, options.Cancel, true)
}

// The original execution owner joins this bounded stop before reclaiming host
// resources. A failed stop leaves normal cleanup/recovery responsible for retry.
func (o *Orchestrator) stopBuildOnCancellation(ctx context.Context, buildID, unit string) func() {
	done := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		defer close(done)
		if err := o.stopBuilderUnit(unit); err != nil {
			o.log.Warn("cancel builder unit; cleanup will retry", "bid", buildID, "err", err)
		}
	})
	return func() {
		if !stop() {
			<-done
		}
	}
}
