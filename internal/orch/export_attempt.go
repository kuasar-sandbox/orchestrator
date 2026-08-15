package orch

import (
	"context"
	"sync"
)

type exportAttemptState uint8

const (
	exportPublishing exportAttemptState = iota
	exportPreempted
	exportDetached
	exportFinalizing
)

// exportAttempt is the exact process-local owner of one sandbox export. Source
// state remains durable in the sandbox row; this record only coordinates the
// publish/finalize boundary with a concurrently accepted resume.
type exportAttempt struct {
	sid          string
	sourceRef    string
	toTemplate   bool
	state        exportAttemptState
	cancelUpload context.CancelCauseFunc
	done         chan struct{}
}

type exportAttemptGroup struct {
	mu     sync.Mutex
	active map[string]*exportAttempt
}

func (g *exportAttemptGroup) Begin(sid, sourceRef string, toTemplate bool, cancelUpload context.CancelCauseFunc) (*exportAttempt, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.active == nil {
		g.active = make(map[string]*exportAttempt)
	}
	if g.active[sid] != nil {
		return nil, false
	}
	attempt := &exportAttempt{
		sid:          sid,
		sourceRef:    sourceRef,
		toTemplate:   toTemplate,
		state:        exportPublishing,
		cancelUpload: cancelUpload,
		done:         make(chan struct{}),
	}
	g.active[sid] = attempt
	return attempt, true
}

// ActiveDone returns the completion fence for the current export. Callers that
// mutate source lifecycle release the lifecycle lock before waiting on it.
func (g *exportAttemptGroup) ActiveDone(sid string) <-chan struct{} {
	g.mu.Lock()
	defer g.mu.Unlock()
	if attempt := g.active[sid]; attempt != nil {
		return attempt.done
	}
	return nil
}

func (g *exportAttemptGroup) State(attempt *exportAttempt) exportAttemptState {
	g.mu.Lock()
	defer g.mu.Unlock()
	if attempt == nil || g.active[attempt.sid] != attempt {
		return exportPreempted
	}
	return attempt.state
}

// ResumeAccepted runs only after BeginResume durably changed paused to
// starting under the per-SID lifecycle lock. KMT export is no longer valid and
// its upload is canceled; template publication stays useful but is detached
// from all source finalization.
func (g *exportAttemptGroup) ResumeAccepted(sid string, cause error) (exportAttemptState, bool) {
	var cancel context.CancelCauseFunc
	state := exportPublishing
	found := false
	g.mu.Lock()
	attempt := g.active[sid]
	if attempt != nil && attempt.state == exportPublishing {
		found = true
		if attempt.toTemplate {
			attempt.state = exportDetached
		} else {
			attempt.state = exportPreempted
			cancel = attempt.cancelUpload
		}
		state = attempt.state
	}
	g.mu.Unlock()
	if cancel != nil {
		cancel(cause)
	}
	return state, found
}

// BeginFinalize is called while holding the per-SID lifecycle lock. Only an
// unmodified publishing attempt may become the source finalizer.
func (g *exportAttemptGroup) BeginFinalize(attempt *exportAttempt) exportAttemptState {
	g.mu.Lock()
	defer g.mu.Unlock()
	if attempt == nil || g.active[attempt.sid] != attempt {
		return exportPreempted
	}
	if attempt.state == exportPublishing {
		attempt.state = exportFinalizing
	}
	return attempt.state
}

func (g *exportAttemptGroup) Finish(attempt *exportAttempt) {
	if attempt == nil {
		return
	}
	g.mu.Lock()
	if g.active[attempt.sid] == attempt {
		delete(g.active, attempt.sid)
		close(attempt.done)
	}
	g.mu.Unlock()
}

// lockLifecycleMutation preserves the previous behavior for source lifecycle
// mutations: they remain serialized behind an active export. Resume admission
// and internal launch commits deliberately use lifecycle.Lock directly so they
// can overtake or complete a detached template upload.
func (o *Orchestrator) lockLifecycleMutation(ctx context.Context, sid string) (func(), error) {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		unlock := o.lifecycle.Lock(sid)
		done := o.exports.ActiveDone(sid)
		if done == nil {
			return unlock, nil
		}
		unlock()
		select {
		case <-done:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}
