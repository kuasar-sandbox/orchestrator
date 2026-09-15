package orch

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/launcher"
	"github.com/kuasar-sandbox/orchestrator/internal/nodepath"
)

type runPool struct {
	kind        string
	size        int
	waitTimeout time.Duration
	runRoot     string
	lc          launcher.Launcher
	unitName    func(string) string
	log         *slog.Logger

	consumeCh       chan *runConsumeReq
	waitCh          chan *runWaitReq
	startDoneCh     chan runStartDone
	controlCh       chan runControlReq
	consumeCancelCh chan *runConsumeReq
	waitCancelCh    chan *runWaitReq
	done            chan struct{}
}

type runConsumeReq struct {
	taskID     string
	ctx        context.Context
	commit     func(runID string) error
	resp       chan runConsumeResp
	stopCancel func() bool
	// startAttempts is the finite wave of already in-flight or demand-created
	// units that may satisfy this request. Replacements created after a failed
	// wave can still win the race and become idle, but they do not extend the
	// request forever when systemd cannot start any builder unit.
	startAttempts map[string]struct{}
	lastStartErr  error
}

type runConsumeResp struct {
	runID string
	err   error
}

type runWaitReq struct {
	runID      string
	ctx        context.Context
	resp       chan runWaitResp
	stopCancel func() bool
}

type runWaitResp struct {
	taskID string
	ok     bool
	err    error
}

type runStartDone struct {
	runID string
	err   error
}

type runControlReq struct {
	op    string
	runID string
}

type idleRun struct {
	runID string
	req   *runWaitReq
}

func newRunPool(kind string, size int, waitTimeout time.Duration, runRoot string, lc launcher.Launcher, unitName func(string) string, log *slog.Logger) *runPool {
	return &runPool{
		kind: kind, size: size, waitTimeout: waitTimeout, runRoot: runRoot, lc: lc, unitName: unitName, log: log,
		consumeCh:       make(chan *runConsumeReq),
		waitCh:          make(chan *runWaitReq),
		startDoneCh:     make(chan runStartDone),
		controlCh:       make(chan runControlReq),
		consumeCancelCh: make(chan *runConsumeReq, 128),
		waitCancelCh:    make(chan *runWaitReq, 128),
		done:            make(chan struct{}),
	}
}

func (p *runPool) runPidFile(runID string) string {
	return nodepath.RunnerPID(p.runRoot, runID)
}

func (p *runPool) Start(ctx context.Context) error {
	if err := os.MkdirAll(nodepath.RunnerRoot(p.runRoot), 0o700); err != nil {
		return fmt.Errorf("run pool: create pidfile directory: %w", err)
	}
	go p.controlLoop(ctx)
	go p.loop(ctx)
	return nil
}

func (p *runPool) Assign(ctx context.Context, taskID string, commit func(runID string) error) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	req := &runConsumeReq{taskID: taskID, ctx: ctx, commit: commit, resp: make(chan runConsumeResp, 1)}
	select {
	case p.consumeCh <- req:
	case <-ctx.Done():
		return "", ctx.Err()
	case <-p.done:
		return "", fmt.Errorf("run pool: stopped")
	}
	// Once enqueued, the pool loop is the sole arbiter. In particular, caller
	// cancellation cannot turn a successfully committed handoff into an error.
	res := <-req.resp
	return res.runID, res.err
}

func (p *runPool) WaitAssignment(ctx context.Context, runID string) (string, bool, error) {
	if err := ctx.Err(); err != nil {
		return "", false, err
	}
	req := &runWaitReq{runID: runID, ctx: ctx, resp: make(chan runWaitResp, 1)}
	select {
	case p.waitCh <- req:
	case <-ctx.Done():
		return "", false, ctx.Err()
	case <-p.done:
		return "", false, fmt.Errorf("run pool: stopped")
	}
	res := <-req.resp
	return res.taskID, res.ok, res.err
}

func (p *runPool) loop(ctx context.Context) {
	defer close(p.done)
	type startingRun struct {
		started time.Time
	}
	starting := map[string]startingRun{}
	var idle []idleRun
	var pending []*runConsumeReq
	var startControls, stopControls []runControlReq
	tick := time.NewTicker(200 * time.Millisecond)
	defer tick.Stop()
	queueControl := func(req runControlReq) {
		if req.op == "stop" {
			stopControls = append(stopControls, req)
			return
		}
		startControls = append(startControls, req)
	}
	replyConsume := func(req *runConsumeReq, resp runConsumeResp) {
		if req.stopCancel != nil {
			req.stopCancel()
		}
		req.resp <- resp
	}
	replyWait := func(req *runWaitReq, resp runWaitResp) {
		if req.stopCancel != nil {
			req.stopCancel()
		}
		req.resp <- resp
	}

	ensure := func() []string {
		need := p.size + len(pending) - len(idle) - len(starting)
		created := make([]string, 0, max(need, 0))
		for ; need > 0; need-- {
			runID, err := newRunID(p.kind)
			if err != nil {
				p.log.Error("run pool: new run id", "kind", p.kind, "err", err)
				return created
			}
			starting[runID] = startingRun{}
			created = append(created, runID)
			queueControl(runControlReq{op: "start", runID: runID})
		}
		return created
	}
	addStartAttempts := func(reqs []*runConsumeReq, runIDs []string) {
		for _, req := range reqs {
			if req.startAttempts == nil {
				continue
			}
			for _, runID := range runIDs {
				req.startAttempts[runID] = struct{}{}
			}
		}
	}
	initializeStartAttempts := func(req *runConsumeReq) {
		req.startAttempts = make(map[string]struct{}, len(starting))
		for runID := range starting {
			req.startAttempts[runID] = struct{}{}
		}
		if len(req.startAttempts) == 0 {
			req.lastStartErr = fmt.Errorf("run pool: no unit start attempt available")
		}
	}
	removeStartAttempt := func(runID string, startErr error) {
		for _, req := range pending {
			if req.startAttempts == nil {
				continue
			}
			if _, tracked := req.startAttempts[runID]; !tracked {
				continue
			}
			delete(req.startAttempts, runID)
			if startErr != nil {
				req.lastStartErr = startErr
			}
		}
	}
	failExhausted := func() {
		for i := len(pending) - 1; i >= 0; i-- {
			req := pending[i]
			if req.startAttempts == nil || len(req.startAttempts) != 0 {
				continue
			}
			err := req.lastStartErr
			if err == nil {
				err = fmt.Errorf("run pool: all unit start attempts exited before assignment")
			}
			pending = slices.Delete(pending, i, i+1)
			replyConsume(req, runConsumeResp{err: err})
		}
	}

	prune := func() {
		now := time.Now()
		for runID, st := range starting {
			if st.started.IsZero() || now.Sub(st.started) <= p.waitTimeout {
				continue
			}
			delete(starting, runID)
			p.log.Warn("run pool: wait assignment timeout", "kind", p.kind, "run_id", runID)
			removeStartAttempt(runID, fmt.Errorf("run pool: run %s exceeded wait timeout", runID))
			queueControl(runControlReq{op: "stop", runID: runID})
		}
		failExhausted()
		idle = slices.DeleteFunc(idle, func(w idleRun) bool {
			if w.req.ctx.Err() == nil {
				return false
			}
			replyWait(w.req, runWaitResp{err: w.req.ctx.Err()})
			queueControl(runControlReq{op: "stop", runID: w.runID})
			return true
		})
		pending = slices.DeleteFunc(pending, func(req *runConsumeReq) bool {
			if req.ctx.Err() == nil {
				return false
			}
			replyConsume(req, runConsumeResp{err: req.ctx.Err()})
			return true
		})
	}

	assign := func() {
		for len(idle) > 0 && len(pending) > 0 {
			w := idle[0]
			idle = idle[1:]
			if w.req.ctx.Err() != nil {
				replyWait(w.req, runWaitResp{err: w.req.ctx.Err()})
				queueControl(runControlReq{op: "stop", runID: w.runID})
				continue
			}
			var req *runConsumeReq
			for len(pending) > 0 {
				req = pending[0]
				pending = pending[1:]
				if req.ctx.Err() == nil {
					break
				}
				replyConsume(req, runConsumeResp{err: req.ctx.Err()})
				req = nil
			}
			if req == nil {
				idle = append([]idleRun{w}, idle...)
				return
			}
			if err := req.ctx.Err(); err != nil {
				replyConsume(req, runConsumeResp{err: err})
				idle = append([]idleRun{w}, idle...)
				continue
			}
			if err := req.commit(w.runID); err != nil {
				replyConsume(req, runConsumeResp{err: err})
				if p.kind == runKindBuild {
					// Build preparation can be cancelled after touching this unit.
					// Its original owner must fence the exact unit before releasing
					// its claim; never lend that unit to another Build meanwhile.
					replyWait(w.req, runWaitResp{err: err})
					queueControl(runControlReq{op: "stop", runID: w.runID})
				} else {
					idle = append([]idleRun{w}, idle...)
				}
				continue
			}
			replyWait(w.req, runWaitResp{taskID: req.taskID, ok: true})
			replyConsume(req, runConsumeResp{runID: w.runID})
		}
	}

	trimIdle := func() {
		for len(idle) > p.size {
			w := idle[len(idle)-1]
			idle = idle[:len(idle)-1]
			replyWait(w.req, runWaitResp{err: fmt.Errorf("run pool: idle capacity retired")})
			queueControl(runControlReq{op: "stop", runID: w.runID})
		}
	}

	ensure()
	for {
		for len(startControls) > 0 {
			if _, ok := starting[startControls[0].runID]; ok {
				break
			}
			startControls = startControls[1:]
		}
		var controlOut chan runControlReq
		var control runControlReq
		controlIsStop := false
		switch {
		case len(stopControls) > 0:
			controlOut, control, controlIsStop = p.controlCh, stopControls[0], true
		case len(startControls) > 0:
			controlOut, control = p.controlCh, startControls[0]
		}
		select {
		case <-ctx.Done():
			for _, w := range idle {
				replyWait(w.req, runWaitResp{err: ctx.Err()})
			}
			for _, req := range pending {
				replyConsume(req, runConsumeResp{err: ctx.Err()})
			}
			return
		case controlOut <- control:
			if controlIsStop {
				stopControls = stopControls[1:]
				continue
			}
			startControls = startControls[1:]
			if st, ok := starting[control.runID]; ok && st.started.IsZero() {
				st.started = time.Now()
				starting[control.runID] = st
			}
		case req := <-p.consumeCh:
			req.stopCancel = context.AfterFunc(req.ctx, func() {
				select {
				case p.consumeCancelCh <- req:
				case <-p.done:
				}
			})
			if req.ctx.Err() != nil {
				replyConsume(req, runConsumeResp{err: req.ctx.Err()})
				continue
			}
			pending = append(pending, req)
			assign()
			trimIdle()
			created := ensure()
			// Existing requests may use starts created by newly arrived demand,
			// while this request takes one finite snapshot of the resulting wave.
			addStartAttempts(pending, created)
			if slices.Contains(pending, req) {
				initializeStartAttempts(req)
				failExhausted()
			}
		case req := <-p.waitCh:
			req.stopCancel = context.AfterFunc(req.ctx, func() {
				select {
				case p.waitCancelCh <- req:
				case <-p.done:
				}
			})
			st, ok := starting[req.runID]
			if !ok {
				replyWait(req, runWaitResp{ok: false, err: fmt.Errorf("run %s is not starting", req.runID)})
				continue
			}
			if !st.started.IsZero() && time.Since(st.started) > p.waitTimeout {
				delete(starting, req.runID)
				replyWait(req, runWaitResp{err: fmt.Errorf("run %s exceeded wait timeout", req.runID)})
				removeStartAttempt(req.runID, fmt.Errorf("run pool: run %s exceeded wait timeout", req.runID))
				failExhausted()
				queueControl(runControlReq{op: "stop", runID: req.runID})
				ensure()
				continue
			}
			delete(starting, req.runID)
			if req.ctx.Err() != nil {
				replyWait(req, runWaitResp{err: req.ctx.Err()})
				removeStartAttempt(req.runID, req.ctx.Err())
				failExhausted()
				queueControl(runControlReq{op: "stop", runID: req.runID})
				ensure()
				continue
			}
			idle = append(idle, idleRun{runID: req.runID, req: req})
			assign()
			// Give the ready worker to the oldest request before retiring this
			// start from the finite waves of all remaining requests.
			removeStartAttempt(req.runID, nil)
			failExhausted()
			trimIdle()
			ensure()
		case req := <-p.consumeCancelCh:
			for i, pendingReq := range pending {
				if pendingReq != req {
					continue
				}
				pending = slices.Delete(pending, i, i+1)
				replyConsume(req, runConsumeResp{err: req.ctx.Err()})
				break
			}
			trimIdle()
			ensure()
		case req := <-p.waitCancelCh:
			for i, waiting := range idle {
				if waiting.req != req {
					continue
				}
				idle = slices.Delete(idle, i, i+1)
				replyWait(req, runWaitResp{err: req.ctx.Err()})
				queueControl(runControlReq{op: "stop", runID: waiting.runID})
				break
			}
			ensure()
		case done := <-p.startDoneCh:
			st, ok := starting[done.runID]
			if !ok {
				continue
			}
			if done.err != nil {
				delete(starting, done.runID)
				p.log.Warn("run pool: start failed", "kind", p.kind, "run_id", done.runID, "err", done.err)
				startErr := fmt.Errorf("run pool: start %s: %w", p.unitName(done.runID), done.err)
				removeStartAttempt(done.runID, startErr)
				failExhausted()
				queueControl(runControlReq{op: "stop", runID: done.runID})
				// Keep replenishing the configured pool for future requests, but
				// do not add replacements to an existing request's finite wave.
				ensure()
			} else {
				if st.started.IsZero() {
					st.started = time.Now()
				}
				starting[done.runID] = st
			}
		case <-tick.C:
			prune()
			assign()
			trimIdle()
			ensure()
		}
	}
}

func (p *runPool) controlLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case req := <-p.controlCh:
			unit := p.unitName(req.runID)
			switch req.op {
			case "start":
				startCtx, cancel := context.WithTimeout(ctx, p.waitTimeout)
				err := p.lc.Start(startCtx, unit)
				cancel()
				select {
				case p.startDoneCh <- runStartDone{runID: req.runID, err: err}:
				case <-ctx.Done():
					return
				}
			case "stop":
				_ = p.lc.Stop(context.Background(), unit)
				_ = p.lc.ResetFailed(context.Background(), unit)
			default:
				p.log.Warn("run pool: unknown unit op", "kind", p.kind, "op", req.op, "run_id", req.runID)
			}
		}
	}
}
