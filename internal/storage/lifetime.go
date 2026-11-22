package storage

import (
	"context"
	"errors"
	"sync"
)

// Lifetime prevents new work during Close and cancels/waits for all work that
// already owns the handle. Drivers destroy their connection only afterwards.
type Lifetime struct {
	mu     sync.Mutex
	ctx    context.Context
	cancel context.CancelFunc
	closed bool
	work   sync.WaitGroup
}

func NewLifetime() *Lifetime {
	ctx, cancel := context.WithCancel(context.Background())
	return &Lifetime{ctx: ctx, cancel: cancel}
}
func (l *Lifetime) Enter(ctx context.Context) (context.Context, func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return nil, nil, &Unavailable{Cause: errors.New("engine closed")}
	}
	l.work.Add(1)
	l.mu.Unlock()
	child, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(l.ctx, cancel)
	var once sync.Once
	return child, func() { once.Do(func() { stop(); cancel(); l.work.Done() }) }, nil
}
func (l *Lifetime) Stop() {
	l.mu.Lock()
	l.closed = true
	l.mu.Unlock()
	l.cancel()
}
func (l *Lifetime) Close() {
	l.Stop()
	l.work.Wait()
}
