package orch

import (
	"context"
	"sync"
)

// acceptedOperationGroup tracks lifecycle operations that cannot be safely
// abandoned once their runtime request has been accepted. Drain closes
// admission before waiting, so no operation can race in after shutdown has
// observed an empty group.
type acceptedOperationGroup struct {
	mu      sync.Mutex
	active  int
	stopped bool
	idle    chan struct{}
}

func (g *acceptedOperationGroup) Begin(admissionCtx context.Context) (func(), error) {
	if admissionCtx == nil {
		admissionCtx = context.Background()
	}

	g.mu.Lock()
	defer g.mu.Unlock()
	if g.stopped {
		return nil, context.Canceled
	}
	if err := admissionCtx.Err(); err != nil {
		return nil, err
	}
	if g.active == 0 {
		g.idle = make(chan struct{})
	}
	g.active++

	var once sync.Once
	return func() {
		once.Do(g.finish)
	}, nil
}

func (g *acceptedOperationGroup) finish() {
	g.mu.Lock()
	g.active--
	if g.active == 0 {
		close(g.idle)
	}
	g.mu.Unlock()
}

func (g *acceptedOperationGroup) Drain(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	g.mu.Lock()
	g.stopped = true
	if g.active == 0 {
		g.mu.Unlock()
		return nil
	}
	idle := g.idle
	g.mu.Unlock()

	select {
	case <-idle:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
