package controlplane

import (
	"context"
	"sync"
	"time"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/coordinator"
	"github.com/kuasar-sandbox/orchestrator/internal/session"
)

type launchRateLimiter struct {
	mu     sync.Mutex
	rate   float64
	burst  float64
	tokens float64
	last   time.Time
	now    func() time.Time
}

func newLaunchRateLimiter(totalRate, totalBurst, members int) *launchRateLimiter {
	if members <= 0 {
		members = 1
	}
	burst := float64(totalBurst) / float64(members)
	if burst < 1 {
		burst = 1
	}
	now := time.Now
	return &launchRateLimiter{
		rate: float64(totalRate) / float64(members), burst: burst, tokens: burst, last: now(), now: now,
	}
}

func (l *launchRateLimiter) allow() bool {
	if l == nil {
		return false
	}
	now := l.now()
	l.mu.Lock()
	defer l.mu.Unlock()
	if elapsed := now.Sub(l.last).Seconds(); elapsed > 0 {
		l.tokens = min(l.burst, l.tokens+elapsed*l.rate)
		l.last = now
	}
	if l.tokens < 1 {
		return false
	}
	l.tokens--
	return true
}

type rateLimitedDispatcher struct {
	next    coordinator.Dispatcher
	sandbox *launchRateLimiter
	build   *launchRateLimiter
}

func (d rateLimitedDispatcher) AdmitAndDispatch(
	ctx context.Context,
	command session.DispatchCommand,
) (session.DispatchReply, error) {
	limiter := d.sandbox
	if command.Kind == clusterstate.ExecutionKindBuild {
		limiter = d.build
	}
	if !limiter.allow() {
		return session.DispatchReply{
			Outcome: clusterstate.DispatchSessionMoved,
			Reason:  "cluster launch rate is temporarily exhausted before dispatch",
		}, nil
	}
	return d.next.AdmitAndDispatch(ctx, command)
}
