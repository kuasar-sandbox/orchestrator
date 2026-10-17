package proxy

import (
	"context"

	"github.com/kuasar-sandbox/orchestrator/internal/proxyapp"
)

// RestartID identifies an accepted operation across process-image replacement.
type RestartID = proxyapp.RestartID
type Status = proxyapp.Status
type InstanceStatus = proxyapp.InstanceStatus
type WorkerStatus = proxyapp.WorkerStatus
type RestartStatus = proxyapp.RestartStatus

var (
	ErrNotReady          = proxyapp.ErrNotReady
	ErrStopped           = proxyapp.ErrStopped
	ErrRestartInProgress = proxyapp.ErrRestartInProgress
	ErrIncompatible      = proxyapp.ErrIncompatible
	ErrForcedClose       = proxyapp.ErrForcedClose
)

// WaitReady waits for this instance's initial routes, local resources and every
// configured worker's data/MMDS acceptance. Cancellation only ends this wait.
func (a *App) WaitReady(ctx context.Context) error {
	if err := a.requireInitialized(); err != nil {
		return err
	}
	return a.service.WaitReady(ctx)
}

// Status is a read-only snapshot of this instance, the service and its operation.
func (a *App) Status() Status {
	if a == nil || a.service == nil {
		return Status{InstanceStatus: InstanceStatus{Phase: "starting", Reasons: []string{"not_started"}}}
	}
	return a.service.Status()
}

// Shutdown stops the whole service, including its temporary replacement. The
// first call fixes the configured budget; zero leaves this explicit graceful
// drain without a deadline. Later calls do not extend it. A caller
// timeout ends only that wait. ErrForcedClose distinguishes forced completion.
// A business handler must not wait indefinitely for its own drain to finish.
func (a *App) Shutdown(ctx context.Context) error {
	if err := a.requireInitialized(); err != nil {
		return err
	}
	return a.service.Shutdown(ctx)
}

// InplaceRestart accepts an original-PID replacement operation independently of
// the inplace_restart signal setting. Cancellation before acceptance aborts the
// request. A positive shutdown_timeout is required for either trigger. After
// acceptance the engine continues. Observe Status for completion: the
// original Go caller stack does not survive exec. Concurrent operations return
// ErrRestartInProgress; candidates and workers cannot start nested operations.
func (a *App) InplaceRestart(ctx context.Context) (RestartID, error) {
	if err := a.requireInitialized(); err != nil {
		return "", err
	}
	return a.service.InplaceRestart(ctx)
}
