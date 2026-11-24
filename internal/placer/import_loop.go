package placer

import (
	"context"
	"time"
)

// StartNodeView is the read-only bootstrap for a one-shot apply.
func (s *Service) StartNodeView(ctx context.Context) {
	s.linksMu.Lock()
	defer s.linksMu.Unlock()
	if s.startedCtx == nil {
		s.startedCtx = ctx
		if len(s.watchLinks) > 0 {
			s.startWatchLinkLocked(ctx)
		}
	}
}

func (s *Service) reconcileGroups(ctx context.Context) {
	tick := time.NewTicker(s.reconcileInterval())
	defer tick.Stop()
	ready := time.NewTicker(placerReadyPollInterval)
	defer ready.Stop()
	wasReady := false
	run := func() {
		_ = s.importAll(ctx, false, func(result ImportResult) {
			if result.Error != "" {
				s.log.Warn("placer: Group import", "source", result.SourceID, "node", result.NodeID,
					"group", result.Group, "status", result.Status, "err", result.Error)
			}
		})
	}
	if s.Ready() {
		wasReady = true
		run()
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-ready.C:
			nowReady := s.Ready()
			if nowReady && !wasReady {
				run()
			}
			wasReady = nowReady
		case <-s.nodeChanges:
			if s.Ready() {
				wasReady = true
				run()
			}
		case <-tick.C:
			wasReady = s.Ready()
			if wasReady {
				run()
			}
		}
	}
}
