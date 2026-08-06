package orch

import (
	"context"
	"errors"
	"sync"
	"time"
)

type launchKind string

const (
	launchCreate launchKind = "create"
	launchResume launchKind = "resume"
)

var (
	errLaunchClaimed       = errors.New("orchestrator: sandbox launch already claimed")
	errLaunchOwnershipLost = errors.New("orchestrator: sandbox launch ownership lost")
)

// launchAttempt is the process-local owner of one accepted create or resume.
// Its entry remains in launchGroup until the worker has completed all terminal
// state and local resource cleanup. Cancel therefore fences new claims without
// pretending that cleanup has already finished.
type launchAttempt struct {
	sid    string
	kind   launchKind
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}

	mu         sync.Mutex
	started    bool
	runID      string
	err        error
	acceptedAt time.Time

	// cleanup serializes Kill/Delete with rollback and retains per-resource
	// successes so the two paths never race duplicate Stop/Detach operations.
	cleanupMu sync.Mutex
	cleanup   *launchCleanupProgress

	finishOnce sync.Once
}

func (a *launchAttempt) Context() context.Context { return a.ctx }
func (a *launchAttempt) SID() string              { return a.sid }
func (a *launchAttempt) Kind() launchKind         { return a.kind }

func (a *launchAttempt) SetRunID(runID string) {
	a.mu.Lock()
	a.runID = runID
	a.mu.Unlock()
}

func (a *launchAttempt) RunID() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.runID
}

func (a *launchAttempt) SetAcceptedAt(at time.Time) {
	a.mu.Lock()
	a.acceptedAt = at
	a.mu.Unlock()
}

func (a *launchAttempt) AcceptedAt() time.Time {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.acceptedAt
}

func (a *launchAttempt) result() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.err
}

func (a *launchAttempt) wait(ctx context.Context) error {
	select {
	case <-a.done:
		return a.result()
	case <-ctx.Done():
		return ctx.Err()
	}
}

// launchGroup is the sole process-local launch ownership registry. Lifecycle
// locks serialize durable transitions; this group separately guarantees that a
// SID cannot own two concurrent create/resume workers.
type launchGroup struct {
	mu sync.Mutex
	m  map[string]*launchAttempt
}

func (g *launchGroup) Claim(parent context.Context, sid string, kind launchKind) (*launchAttempt, error) {
	if parent == nil {
		parent = context.Background()
	}
	if err := parent.Err(); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(parent)
	a := &launchAttempt{sid: sid, kind: kind, ctx: ctx, cancel: cancel, done: make(chan struct{})}

	g.mu.Lock()
	defer g.mu.Unlock()
	if g.m == nil {
		g.m = make(map[string]*launchAttempt)
	}
	if _, exists := g.m[sid]; exists {
		cancel()
		return nil, errLaunchClaimed
	}
	if err := parent.Err(); err != nil {
		cancel()
		return nil, err
	}
	g.m[sid] = a
	return a, nil
}

func (g *launchGroup) Start(a *launchAttempt, fn func(context.Context, *launchAttempt) error) {
	if a == nil {
		return
	}
	a.mu.Lock()
	if a.started {
		a.mu.Unlock()
		return
	}
	a.started = true
	a.mu.Unlock()
	go func() {
		var err error
		if fn != nil {
			err = fn(a.ctx, a)
		}
		g.Finish(a, err)
	}()
}

func (g *launchGroup) Lookup(sid string) (*launchAttempt, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	a, ok := g.m[sid]
	return a, ok
}

func (g *launchGroup) Wait(ctx context.Context, sid string) (found bool, err error) {
	a, ok := g.Lookup(sid)
	if !ok {
		return false, nil
	}
	return true, a.wait(ctx)
}

func (g *launchGroup) Cancel(sid string) {
	a, ok := g.Lookup(sid)
	if ok {
		a.cancel()
	}
}

// Drain waits until every claimed launch has completed terminal publication
// and cleanup. Callers must first cancel the lifecycle root so no new attempt
// can be admitted while shutdown is draining the current set.
func (g *launchGroup) Drain(ctx context.Context) error {
	for {
		g.mu.Lock()
		attempts := make([]*launchAttempt, 0, len(g.m))
		for _, attempt := range g.m {
			attempts = append(attempts, attempt)
		}
		g.mu.Unlock()
		if len(attempts) == 0 {
			return nil
		}
		for _, attempt := range attempts {
			if err := attempt.wait(ctx); err != nil && ctx.Err() != nil {
				return ctx.Err()
			}
		}
	}
}

// Finish publishes the result and removes only the exact attempt pointer. The
// worker calling Finish must already have committed/published its authoritative
// terminal state and completed resource cleanup; close(done) is deliberately
// last so every awakened waiter can re-read that state.
func (g *launchGroup) Finish(a *launchAttempt, err error) {
	if a == nil {
		return
	}
	a.finishOnce.Do(func() {
		a.mu.Lock()
		a.err = err
		a.mu.Unlock()

		g.mu.Lock()
		if g.m[a.sid] == a {
			delete(g.m, a.sid)
		}
		g.mu.Unlock()

		a.cancel()
		close(a.done)
	})
}
