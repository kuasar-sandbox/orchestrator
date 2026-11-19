package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	nativeusage "github.com/kuasar-sandbox/sandboxer/pkg/usage"
)

// Owned by the parent wait loop. No goroutine may read the shared metrics paths
// after retired becomes true; the terminal view is captured before ACK.
type parentRetirement struct {
	socket, sid, runID string
	metrics            *runnerMetrics
	stopSampling       func()
	log                *slog.Logger
	retired            bool
	event              configsock.RunRetirementEvent
	terminal           *nativeusage.View
	published          bool
}

func newParentRetirement(socket, sid, runID string, metrics *runnerMetrics, stop func(), log *slog.Logger) *parentRetirement {
	return &parentRetirement{socket: socket, sid: sid, runID: runID, metrics: metrics, stopSampling: stop, log: log}
}

func (p *parentRetirement) accept(event configsock.RunRetirementEvent) {
	if !event.Valid() || event.SandboxID != p.sid || event.RunID != p.runID || p.retired && p.event != event {
		if p.log != nil {
			p.log.Error("runner retirement identity mismatch", "run_id", p.runID)
		}
		return
	}
	if !p.retired {
		p.stopSampling()
		if p.metrics != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
			view, err := p.metrics.readUsage(ctx, true)
			cancel()
			if err == nil {
				p.terminal = &view
			} else if p.log != nil {
				p.log.Debug("retired runtime terminal usage unavailable", "run_id", p.runID, "err", err)
			}
		}
		p.event, p.retired = event, true
	}
	if err := acknowledgeRetirementWithRetry(p.socket, event); err != nil && p.log != nil {
		// Keep waiting for the holder; returning from its systemd parent here
		// would kill it merely because the controller's ACK response was lost.
		p.log.Warn("runner retirement acknowledgement pending", "run_id", p.runID, "err", err)
	}
	if !p.published && p.terminal != nil {
		p.published = true
		ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
		p.metrics.emitUsageView(ctx, *p.terminal, true)
		cancel()
	}
}

func acknowledgeRetirementWithRetry(socket string, event configsock.RunRetirementEvent) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var last error
	for attempt := 0; attempt < 5; attempt++ {
		callCtx, callCancel := context.WithTimeout(ctx, 5*time.Second)
		last = configsock.AcknowledgeRunRetirement(callCtx, socket, "sandbox", event)
		callCancel()
		if last == nil || !configsock.IsRetryableError(last) {
			return last
		}
		timer := time.NewTimer(time.Duration(1<<attempt) * 100 * time.Millisecond)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return errors.Join(ctx.Err(), last)
		}
	}
	return last
}

// Child exit and capture completion may be observed in either order. Query the
// exact durable host role before using the ordinary final-read/result path.
func (p *parentRetirement) resolveExitedRole() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for {
		callCtx, callCancel := context.WithTimeout(ctx, 5*time.Second)
		role, err := configsock.ReadRunRole(callCtx, p.socket, "sandbox", p.runID)
		callCancel()
		if errors.Is(err, configsock.ErrRunRetirementUnsupported) {
			return nil
		}
		if err == nil {
			if role.Retirement != nil {
				p.accept(*role.Retirement)
				if !p.retired {
					return errors.New("invalid durable holder identity")
				}
				return nil
			}
			if !role.Pending {
				return nil
			}
		} else if !configsock.IsRetryableError(err) {
			return err
		}
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("runner exit role remains unknown: %w", ctx.Err())
		}
	}
}
