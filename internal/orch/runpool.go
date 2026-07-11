package orch

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/launcher"
)

type runPool struct {
	kind        string
	size        int
	waitTimeout time.Duration
	runRoot     string
	lc          launcher.Launcher
	unitName    func(string) string
	log         *slog.Logger

	consumeCh   chan *runConsumeReq
	waitCh      chan *runWaitReq
	startDoneCh chan runStartDone
	controlCh   chan runControlReq
}

type runConsumeReq struct {
	taskID string
	ctx    context.Context
	commit func(runID string) error
	resp   chan runConsumeResp
}

type runConsumeResp struct {
	runID string
	err   error
}

type runWaitReq struct {
	runID string
	ctx   context.Context
	resp  chan runWaitResp
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
	ctx   context.Context
	resp  chan runWaitResp
}

func newRunPool(kind string, size int, waitTimeout time.Duration, runRoot string, lc launcher.Launcher, unitName func(string) string, log *slog.Logger) *runPool {
	return &runPool{
		kind: kind, size: size, waitTimeout: waitTimeout, runRoot: runRoot, lc: lc, unitName: unitName, log: log,
		consumeCh:   make(chan *runConsumeReq),
		waitCh:      make(chan *runWaitReq),
		startDoneCh: make(chan runStartDone),
		controlCh:   make(chan runControlReq),
	}
}

func (p *runPool) runPidFile(runID string) string {
	return filepath.Join(p.runRoot, "runs", runID+".pid")
}

func (p *runPool) Start(ctx context.Context) error {
	if err := os.MkdirAll(filepath.Join(p.runRoot, "runs"), 0o700); err != nil {
		return fmt.Errorf("run pool: create pidfile directory: %w", err)
	}
	go p.controlLoop(ctx)
	go p.loop(ctx)
	return nil
}

func (p *runPool) Assign(ctx context.Context, taskID string, commit func(runID string) error) (string, error) {
	req := &runConsumeReq{taskID: taskID, ctx: ctx, commit: commit, resp: make(chan runConsumeResp, 1)}
	select {
	case p.consumeCh <- req:
	case <-ctx.Done():
		return "", ctx.Err()
	}
	select {
	case res := <-req.resp:
		return res.runID, res.err
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

func (p *runPool) WaitAssignment(ctx context.Context, runID string) (string, bool, error) {
	req := &runWaitReq{runID: runID, ctx: ctx, resp: make(chan runWaitResp, 1)}
	select {
	case p.waitCh <- req:
	case <-ctx.Done():
		return "", false, ctx.Err()
	}
	select {
	case res := <-req.resp:
		return res.taskID, res.ok, res.err
	case <-ctx.Done():
		return "", false, ctx.Err()
	}
}

func (p *runPool) loop(ctx context.Context) {
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

	ensure := func() {
		need := p.size + len(pending) - len(idle) - len(starting)
		for ; need > 0; need-- {
			runID, err := newRunID(p.kind)
			if err != nil {
				p.log.Error("run pool: new run id", "kind", p.kind, "err", err)
				return
			}
			starting[runID] = startingRun{}
			queueControl(runControlReq{op: "start", runID: runID})
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
			queueControl(runControlReq{op: "stop", runID: runID})
		}
		idle = slices.DeleteFunc(idle, func(w idleRun) bool {
			if w.ctx.Err() == nil {
				return false
			}
			queueControl(runControlReq{op: "stop", runID: w.runID})
			return true
		})
		pending = slices.DeleteFunc(pending, func(req *runConsumeReq) bool {
			if req.ctx.Err() == nil {
				return false
			}
			req.resp <- runConsumeResp{err: req.ctx.Err()}
			return true
		})
	}

	assign := func() {
		for len(idle) > 0 && len(pending) > 0 {
			w := idle[0]
			idle = idle[1:]
			if w.ctx.Err() != nil {
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
				req.resp <- runConsumeResp{err: req.ctx.Err()}
				req = nil
			}
			if req == nil {
				idle = append([]idleRun{w}, idle...)
				return
			}
			if err := req.commit(w.runID); err != nil {
				req.resp <- runConsumeResp{err: err}
				idle = append([]idleRun{w}, idle...)
				continue
			}
			w.resp <- runWaitResp{taskID: req.taskID, ok: true}
			req.resp <- runConsumeResp{runID: w.runID}
		}
	}

	trimIdle := func() {
		for len(idle) > p.size {
			w := idle[len(idle)-1]
			idle = idle[:len(idle)-1]
			w.resp <- runWaitResp{err: fmt.Errorf("run pool: idle capacity retired")}
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
				w.resp <- runWaitResp{err: ctx.Err()}
			}
			for _, req := range pending {
				req.resp <- runConsumeResp{err: ctx.Err()}
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
			if req.ctx.Err() != nil {
				req.resp <- runConsumeResp{err: req.ctx.Err()}
				continue
			}
			pending = append(pending, req)
			assign()
			trimIdle()
			ensure()
		case req := <-p.waitCh:
			st, ok := starting[req.runID]
			if !ok {
				req.resp <- runWaitResp{ok: false, err: fmt.Errorf("run %s is not starting", req.runID)}
				continue
			}
			if !st.started.IsZero() && time.Since(st.started) > p.waitTimeout {
				delete(starting, req.runID)
				req.resp <- runWaitResp{err: fmt.Errorf("run %s exceeded wait timeout", req.runID)}
				queueControl(runControlReq{op: "stop", runID: req.runID})
				ensure()
				continue
			}
			delete(starting, req.runID)
			if req.ctx.Err() != nil {
				req.resp <- runWaitResp{err: req.ctx.Err()}
				queueControl(runControlReq{op: "stop", runID: req.runID})
				ensure()
				continue
			}
			idle = append(idle, idleRun{runID: req.runID, ctx: req.ctx, resp: req.resp})
			assign()
			trimIdle()
			ensure()
		case done := <-p.startDoneCh:
			st, ok := starting[done.runID]
			if !ok {
				continue
			}
			if done.err != nil {
				delete(starting, done.runID)
				p.log.Warn("run pool: start failed", "kind", p.kind, "run_id", done.runID, "err", done.err)
				queueControl(runControlReq{op: "stop", runID: done.runID})
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
