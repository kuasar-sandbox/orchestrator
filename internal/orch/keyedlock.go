package orch

import (
	"context"
	"errors"
	"sync"
)

var errResumeFenced = errors.New("orchestrator: resume request fenced")

// keyedLockGroup serializes distinct lifecycle mutations for one sandbox while
// allowing unrelated sandboxes to proceed independently. Entries are retained
// only while an owner or waiter references them.
type keyedLockGroup struct {
	mu    sync.Mutex
	locks map[string]*keyedLock
}

type keyedLock struct {
	mu   sync.Mutex
	refs int
}

type resumeRequestState struct {
	next     uint64
	canceled uint64
	refs     int
}

type resumeRequest struct {
	sid   string
	seq   uint64
	state *resumeRequestState
}

func (g *keyedLockGroup) Lock(key string) func() {
	g.mu.Lock()
	if g.locks == nil {
		g.locks = make(map[string]*keyedLock)
	}
	lock := g.locks[key]
	if lock == nil {
		lock = &keyedLock{}
		g.locks[key] = lock
	}
	lock.refs++
	g.mu.Unlock()

	lock.mu.Lock()
	return func() {
		lock.mu.Unlock()
		g.mu.Lock()
		lock.refs--
		if lock.refs == 0 && g.locks[key] == lock {
			delete(g.locks, key)
		}
		g.mu.Unlock()
	}
}

func (o *Orchestrator) markDeadlineIntent(sid string) {
	o.deadlineIntentMu.Lock()
	o.deadlineIntents[sid] = struct{}{}
	o.deadlineIntentMu.Unlock()
}

func (o *Orchestrator) hasDeadlineIntent(sid string) bool {
	o.deadlineIntentMu.Lock()
	_, ok := o.deadlineIntents[sid]
	o.deadlineIntentMu.Unlock()
	return ok
}

func (o *Orchestrator) clearDeadlineIntent(sid string) {
	o.deadlineIntentMu.Lock()
	delete(o.deadlineIntents, sid)
	o.deadlineIntentMu.Unlock()
}

func (o *Orchestrator) newResumeRequest(sid string) resumeRequest {
	o.resumeRequestMu.Lock()
	if o.resumeRequests == nil {
		o.resumeRequests = make(map[string]*resumeRequestState)
	}
	state := o.resumeRequests[sid]
	if state == nil {
		state = &resumeRequestState{}
		o.resumeRequests[sid] = state
	}
	state.next++
	state.refs++
	request := resumeRequest{sid: sid, seq: state.next, state: state}
	o.resumeRequestMu.Unlock()
	return request
}

func (o *Orchestrator) releaseResumeRequest(request resumeRequest) {
	o.resumeRequestMu.Lock()
	request.state.refs--
	if request.state.refs == 0 && o.resumeRequests[request.sid] == request.state {
		delete(o.resumeRequests, request.sid)
	}
	o.resumeRequestMu.Unlock()
}

func (o *Orchestrator) resumeRequestValid(request resumeRequest) bool {
	o.resumeRequestMu.Lock()
	valid := o.resumeRequests[request.sid] == request.state && request.seq > request.state.canceled
	o.resumeRequestMu.Unlock()
	return valid
}

// cancelResumeRequests fences every resume whose request was registered before
// this lifecycle mutation. A later data-plane wake obtains a larger sequence and
// remains eligible to resume the sandbox.
func (o *Orchestrator) cancelResumeRequests(sid string) {
	o.resumeRequestMu.Lock()
	if state := o.resumeRequests[sid]; state != nil {
		state.canceled = state.next
	}
	o.resumeRequestMu.Unlock()
}

// resumeSandbox collapses concurrent wakeups, then runs the actual resume under
// the same per-sandbox lifecycle boundary used by delete and deadline updates.
// A deadline explicitly set while paused is preserved exactly once; ordinary
// data-plane wakeups continue to re-arm the node default TTL.
func (o *Orchestrator) resumeSandbox(ctx context.Context, sid string) error {
	request := o.newResumeRequest(sid)
	defer o.releaseResumeRequest(request)
	return o.resumeSandboxRequest(ctx, request)
}

func (o *Orchestrator) resumeSandboxRequest(ctx context.Context, request resumeRequest) error {
	for {
		err := o.sf.Do(request.sid, func() error {
			unlock := o.lifecycle.Lock(request.sid)
			defer unlock()
			if !o.resumeRequestValid(request) {
				return errResumeFenced
			}

			preserveDeadline := o.hasDeadlineIntent(request.sid)
			if err := o.resumeIfPaused(ctx, request.sid, preserveDeadline); err != nil {
				return err
			}
			o.clearDeadlineIntent(request.sid)
			return nil
		})
		if !errors.Is(err, errResumeFenced) {
			return err
		}
		if !o.resumeRequestValid(request) {
			return nil
		}
		// This valid request joined an older, fenced single-flight. Retry after
		// that flight has left the map so the post-fence request can run.
	}
}
